import gleam/io

import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/option.{None}
import glisten.{Packet}

pub type Message {
  HeaderV1(
    size: Int,
    request_api_key: Int,
    request_api_version: Int,
    correlation_id: Int,
  )
}

pub fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  io.println("Logs from your program will appear here!")

  // TODO: Uncomment the code below to pass the first stage

  let assert Ok(_) =
    glisten.new(fn(_conn) { #(Nil, None) }, fn(state, msg, conn) {
      io.println("Received message!")
      let assert Packet(msg) = msg
      let assert Ok(message) = process_message(msg)
      send_response(conn, message)
      glisten.continue(state)
    })
    |> glisten.start(9092)

  process.sleep_forever()
}

fn process_message(bytes: BitArray) -> Result(Message, Nil) {
  case bytes {
    <<
      size:32,
      request_api_key:16,
      request_api_version:16,
      correlation_id:32,
      _:bits,
    >> -> {
      Ok(HeaderV1(size, request_api_key, request_api_version, correlation_id))
    }
    _ -> Error(Nil)
  }
}

fn send_response(conn: glisten.Connection(user_message), rcv_message: Message) {
  let to_send_message = case rcv_message {
    HeaderV1(_size, _request_api_key, request_api_version, correlation_id) -> {
      case request_api_version {
        1 | 2 | 3 | 4 -> {
          bytes_tree.append(
            bytes_tree.new(),
            bit_array.append(<<64:size(32)>>, <<correlation_id:size(32)>>),
          )
        }
        _ -> {
          bytes_tree.append(
            bytes_tree.new(),
            bit_array.append(<<64:32>>, <<correlation_id:32, 35:16>>),
          )
        }
      }
    }
  }

  case glisten.send(conn, to_send_message) {
    Ok(Nil) -> io.println("Response sent")
    Error(_) -> io.println("Error")
  }
}
