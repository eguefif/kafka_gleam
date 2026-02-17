/// This modules contains primivites to encode and decode Kafka arrays
import gleam/io
import gleam/list
import gleam/result
import gleam/string
import kafka/primitives/number.{encode_varint, read_varint}

pub fn compact_array_to_bytes(
  values: List(value),
  serialize_func: fn(value) -> Result(BitArray, Nil),
) -> Result(BitArray, Nil) {
  use dumps <- result.try(compact_array_to_bytes_loop(values, serialize_func))
  use list_size <- result.try(encode_varint(list.length(values) + 1))
  Ok(<<list_size:bits, dumps:bits>>)
}

fn compact_array_to_bytes_loop(
  list: List(value),
  serialize_func: fn(value) -> Result(BitArray, Nil),
) -> Result(BitArray, Nil) {
  case list {
    [first, ..rest] -> {
      use dumps <- result.try(compact_array_to_bytes_loop(rest, serialize_func))
      use serialized_value <- result.try(serialize_func(first))
      io.println(string.inspect(dumps))
      Ok(<<serialized_value:bits, dumps:bits>>)
    }
    [] -> Ok(<<>>)
  }
}

pub fn read_compact_array(
  bytes: BitArray,
  parsing_func: fn(BitArray) -> Result(#(value, BitArray), Nil),
) -> Result(#(List(value), BitArray), Nil) {
  use #(size, rest) <- result.try(read_varint(bytes))
  read_compact_array_loop(rest, size, parsing_func)
}

fn read_compact_array_loop(
  bytes: BitArray,
  size: Int,
  parsing_func: fn(BitArray) -> Result(#(value, BitArray), Nil),
) -> Result(#(List(value), BitArray), Nil) {
  case size {
    1 -> Ok(#([], bytes))
    _ -> {
      use #(array_item, rest) <- result.try(parsing_func(bytes))
      use #(array, rest) <- result.try(read_compact_array_loop(
        rest,
        size - 1,
        parsing_func,
      ))
      Ok(#([array_item, ..array], rest))
    }
  }
}
