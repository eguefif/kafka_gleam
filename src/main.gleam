import gleam/bytes_tree.{type BytesTree}
import gleam/io
import gleam/result
import gleam/string

import gleam/erlang/process
import gleam/option.{None}
import glisten.{Packet}

import kafka/internals/create_response.{build_response}
import kafka/internals/process_request.{process_request}

pub fn main() {
  io.println("Logs from your program will appear here!")

  let server =
    glisten.new(fn(_conn) { #(Nil, None) }, handler)
    |> glisten.start(9092)

  case server {
    Ok(_) -> process.sleep_forever()
    Error(err) -> {
      io.println("Failed to start server" <> string.inspect(err))
      Nil
    }
  }
}

fn handler(state, msg, conn) {
  io.println("Received message!")
  let response = case msg {
    Packet(msg) -> {
      msg
      |> process_request
      |> result.try(build_response)
      |> result.unwrap(get_error_response())
    }
    _ -> get_error_response()
  }
  send_response(conn, response)
  glisten.continue(state)
}

fn send_response(
  conn: glisten.Connection(user_message),
  response: BytesTree,
) -> Nil {
  case glisten.send(conn, response) {
    Ok(Nil) -> io.println("Response sent")
    Error(_) -> io.println("Error")
  }
}

fn get_error_response() -> BytesTree {
  bytes_tree.from_bit_array(<<>>)
}
