import gleam/bit_array
import gleam/result
import gleam/string
import kafka/primitives/number.{
  encode_varint, read_bytes, read_i16n, read_varint,
}

pub fn read_compact_string(bytes: BitArray) -> Result(#(String, BitArray), Nil) {
  use #(string_size, rest) <- result.try(read_varint(bytes))
  use #(raw_str, rest) <- result.try(read_bytes(rest, string_size - 1))
  use str <- result.try(bit_array.to_string(raw_str))
  Ok(#(str, rest))
}

pub fn read_nullable_string(bytes: BitArray) -> Result(#(String, BitArray), Nil) {
  use #(size, rest) <- result.try(read_i16n(bytes))
  use #(raw_str, rest) <- result.try(read_bytes(rest, size))
  use str <- result.try(bit_array.to_string(raw_str))
  Ok(#(str, rest))
}

pub fn compact_nullable_string_to_bytes(str: String) -> Result(BitArray, Nil) {
  let len = string.length(str) + 1
  use varint <- result.try(encode_varint(len))
  Ok(<<varint:bits, str:utf8>>)
}

pub fn encode_uuidv4(_topic_id: String) -> Result(BitArray, Nil) {
  // TODO: Impl uuidv4 encoding
  Ok(<<0:size({ 16 * 8 })>>)
}
