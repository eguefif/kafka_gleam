import gleam/io

import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/option.{None}
import glisten.{Packet}

import internals/api_version_response.{get_api_version_response}
import internals/process_request.{type Request, HeaderV2, process_message}

pub fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  io.println("Logs from your program will appear here!")

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

fn send_response(
  conn: glisten.Connection(user_message),
  rcv_message: Request,
) -> Nil {
  let to_send_message = case rcv_message {
    HeaderV2(
      _size,
      _request_api_key,
      request_api_version,
      correlation_id,
      _body,
    ) -> {
      case request_api_version {
        1 | 2 | 3 | 4 -> {
          bytes_tree.append(
            bytes_tree.new(),
            get_api_version_response(correlation_id),
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
