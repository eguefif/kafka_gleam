//// Main module.
//// It provides a handler function that process, create a response and send it
//// This module is based on two other modules:
//// * process_request
//// * create_response
//// They both creates packet using the headers module and their respective body modules.

import gleam/bytes_tree.{type BytesTree}
import gleam/io
import gleam/result
import glisten.{Packet}
import kafka/internals/create_response.{build_response}
import kafka/internals/process_request.{process_request}

pub fn handler(state, msg, conn) {
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
