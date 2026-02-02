import gleam/bit_array
import gleam/result

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

pub fn try_read_bytes(
  bytes: BitArray,
  size: Int,
) -> Result(#(BitArray, BitArray), Nil) {
  case bytes {
    <<buffer:bytes-size(size), rest:bits>> -> Ok(#(buffer, rest))
    _ -> Error(Nil)
  }
}

pub fn try_read_compact_string(
  bytes: BitArray,
) -> Result(#(String, BitArray), Nil) {
  let #(string_size, rest) = read_varint(bytes)
  use #(raw_str, rest) <- result.try(try_read_bytes(rest, string_size - 1))
  use str <- result.try(bit_array.to_string(raw_str))
  Ok(#(str, rest))
}

pub fn read_varint(bytes: BitArray) -> #(Int, BitArray) {
  let #(varint, size) = read_varint_acc(bytes, 0)
  let assert <<varint:int-big-signed-size(size)>> = varint
  let varint_size = size / 7
  let assert <<_:size(varint_size * 8), rest:bits>> = bytes
  #(varint, rest)
}

fn read_varint_acc(bytes: BitArray, size: Int) -> #(BitArray, Int) {
  case bytes {
    <<1:1, number:7, rest:bits>> -> {
      let #(acc, size) = read_varint_acc(rest, size + 7)
      #(<<acc:bits, number:7>>, size)
    }
    <<0:1, number:7, _:bits>> -> {
      #(<<number:7>>, size + 7)
    }
    _ -> {
      panic as "Should never get there when decoding unsigned varint"
    }
  }
}
