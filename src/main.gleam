import gleam/bytes_tree.{type BytesTree}
import gleam/io
import gleam/result
import gleam/string

import gleam/erlang/process
import gleam/option.{None}
import glisten.{Packet}

import internals/api_version_response.{get_api_version_response}
import internals/kpacket.{type KPacket, HeaderV2, Request}
import internals/process_request.{process_request}

// TODO: refactor structure.
// Put bytes read and write primites in a module with documentation

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
      |> result.map(build_response)
      |> result.unwrap(get_error_response())
    }
    _ -> get_error_response()
  }
  send_response(conn, response)
  glisten.continue(state)
}

fn build_response(request: KPacket) -> BytesTree {
  let assert Request(_, header, _) = request
  case header {
    HeaderV2(api_key, ..) -> {
      handle_header_v2(api_key, request)
    }
    _ -> get_not_implemented_api_key()
  }
}

fn handle_header_v2(api_key: Int, request: KPacket) -> BytesTree {
  let response = case api_key {
    18 -> get_api_version_response(request)
    _ -> Ok(get_not_implemented_api_key())
  }
  case response {
    Ok(response) -> response
    Error(Nil) -> get_error_response()
  }
}

fn get_not_implemented_api_key() -> BytesTree {
  bytes_tree.from_bit_array(<<>>)
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
