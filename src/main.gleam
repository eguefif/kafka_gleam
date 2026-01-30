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
      process_message(msg)
      send_response(conn)
      glisten.continue(state)
    })
    |> glisten.start(9092)

  process.sleep_forever()
}

fn process_message(bytes: BitArray) {
  case bit_array.to_string(bytes) {
    Ok(message) -> io.println("New message: " <> message)
    Error(Nil) -> io.println_error("Impossible to transform bytes into string")
  }
}

fn send_response(conn: glisten.Connection(user_message)) {
  let message =
    bytes_tree.append(
      bytes_tree.new(),
      bit_array.append(<<32:size(32)>>, <<7:size(32)>>),
    )
  case glisten.send(conn, message) {
    Ok(Nil) -> io.println("Response sent")
    Error(_) -> io.println("Error")
  }
}
