import gleam/io
import gleam/string

import gleam/erlang/process
import gleam/option.{None}
import glisten

import kafka/internals/packet_handler.{handler}

// TODO: write functions documentation if necessary

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
