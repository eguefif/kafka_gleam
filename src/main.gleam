import gleam/bytes_tree.{type BytesTree}
import gleam/io

import gleam/erlang/process
import gleam/option.{None}
import glisten.{Packet}

import internals/api_version_response.{get_api_version_response}
import internals/kpacket.{type KPacket, HeaderV2}
import internals/process_request.{process_request}

pub fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  io.println("Logs from your program will appear here!")

  let assert Ok(_) =
    glisten.new(fn(_conn) { #(Nil, None) }, fn(state, msg, conn) {
      io.println("Received message!")
      let assert Packet(msg) = msg
      let assert Ok(request) = process_request(msg)
      let response = build_response(request)
      send_response(conn, response)
      glisten.continue(state)
    })
    |> glisten.start(9092)

  process.sleep_forever()
}

fn build_response(request: KPacket) -> BytesTree {
  case request {
    HeaderV2(..) -> {
      handle_header_v2(request)
    }
    _ -> get_not_implemented_api_key()
  }
}

fn handle_header_v2(request: KPacket) -> BytesTree {
  let assert HeaderV2(_, request_api_key, ..) = request
  case request_api_key {
    18 -> get_api_version_response(request)
    _ -> get_not_implemented_api_key()
  }
}

fn get_not_implemented_api_key() -> BytesTree {
  echo "test"
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
