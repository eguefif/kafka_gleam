import gleam/io

import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/option.{None}
import glisten.{Packet}

pub fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  io.println("Logs from your program will appear here!")

  // TODO: Uncomment the code below to pass the first stage

  let assert Ok(_) =
    glisten.new(fn(_conn) { #(Nil, None) }, fn(state, msg, conn) {
      io.println("Received message!")
      let assert Packet(msg) = msg
      let assert Ok(correlation_id) = process_message(msg)
      send_response(conn, correlation_id)
      glisten.continue(state)
    })
    |> glisten.start(9092)

  process.sleep_forever()
}

fn process_message(bytes: BitArray) -> Result(Int, Nil) {
  case bytes {
    <<_:32, _:16, _:16, correlation_id:32, _:bits>> -> Ok(correlation_id)
    _ -> Error(Nil)
  }
}

fn send_response(conn: glisten.Connection(user_message), correlation_id: Int) {
  let message =
    bytes_tree.append(
      bytes_tree.new(),
      bit_array.append(<<64:size(32)>>, <<correlation_id:size(32)>>),
    )
  case glisten.send(conn, message) {
    Ok(Nil) -> io.println("Response sent")
    Error(_) -> io.println("Error")
  }
}
