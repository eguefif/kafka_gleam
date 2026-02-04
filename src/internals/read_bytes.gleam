import gleam/bit_array
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import gleam/string

// TODO: organize into different module depending on the type:
// * number
// * list: string + array
// Write documentation 

pub fn try_read_i8(bytes) -> Result(#(Int, BitArray), Nil) {
  case bytes {
    <<num:int-big-size(8), rest:bits>> -> Ok(#(num, rest))
    _ -> Error(Nil)
  }
}

pub fn try_read_i16(bytes) -> Result(#(Int, BitArray), Nil) {
  case bytes {
    <<num:int-big-size(16), rest:bits>> -> Ok(#(num, rest))
    _ -> Error(Nil)
  }
}

pub fn try_read_i32(bytes) -> Result(#(Int, BitArray), Nil) {
  case bytes {
    <<num:int-big-size(32), rest:bits>> -> Ok(#(num, rest))
    _ -> Error(Nil)
  }
}

pub fn try_read_bytes(
  bytes: BitArray,
  size: Int,
) -> Result(#(BitArray, BitArray), Nil) {
  case bytes {
    <<buffer:bytes-size(size), rest:bits>> -> Ok(#(buffer, rest))
    _ -> Error(Nil)
  }
}

pub fn try_read_int_bytes(
  bytes: BitArray,
  size: Int,
) -> Result(#(Int, BitArray), Nil) {
  case bytes {
    <<num:int-big-size(size), rest:bits>> -> Ok(#(num, rest))
    _ -> Error(Nil)
  }
}

pub fn try_read_compact_string(
  bytes: BitArray,
) -> Result(#(String, BitArray), Nil) {
  use #(string_size, rest) <- result.try(read_varint(bytes))
  use #(raw_str, rest) <- result.try(try_read_bytes(rest, string_size - 1))
  use str <- result.try(bit_array.to_string(raw_str))
  Ok(#(str, rest))
}

pub fn read_varint(bytes: BitArray) -> Result(#(Int, BitArray), Nil) {
  use #(varint_bytes, rest) <- result.try(read_varint_acc(bytes))
  let varint_size = bit_array.bit_size(varint_bytes)
  case varint_bytes {
    <<varint:int-big-size({ varint_size })>> -> Ok(#(varint, rest))
    _ -> Error(Nil)
  }
}

fn read_varint_acc(bytes: BitArray) -> Result(#(BitArray, BitArray), Nil) {
  case bytes {
    <<1:1, number:7, next_rest:bits>> -> {
      use #(acc, rest) <- result.try(read_varint_acc(next_rest))
      Ok(#(<<acc:bits, number:7>>, <<rest:bits>>))
    }
    <<0:1, number:7, rest:bits>> -> {
      Ok(#(<<number:7>>, <<rest:bits>>))
    }
    _ -> Error(Nil)
  }
}

pub fn try_read_nullable_string(
  bytes: BitArray,
) -> Result(#(String, BitArray), Nil) {
  use #(size, rest) <- result.try(try_read_i16(bytes))
  use #(raw_str, rest) <- result.try(try_read_bytes(rest, size))
  use str <- result.try(bit_array.to_string(raw_str))
  Ok(#(str, rest))
}

pub fn encode_varint(num: Int) -> Result(BitArray, Nil) {
  case num >= 0b1000_0000 {
    True -> {
      use buffer <- result.try(encode_varint(int.bitwise_shift_right(num, 7)))
      let num = int.bitwise_or(num, 0b1000_0000)
      Ok(<<1:1, num:int-size(7), buffer:bits>>)
    }
    False -> {
      Ok(<<0:1, num:int-big-size(7)>>)
    }
  }
}

pub fn compact_nullable_string_to_bytes(str: String) -> Result(BitArray, Nil) {
  let len = string.length(str) + 1
  use varint <- result.try(encode_varint(len))
  Ok(<<varint:bits, str:utf8>>)
}

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
