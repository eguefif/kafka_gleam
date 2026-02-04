import gleam/bit_array
import gleeunit
import internals/read_bytes.{
  encode_varint, read_varint, try_read_compact_string, try_read_nullable_string,
}

pub fn main() {
  gleeunit.main()
}

pub fn read_varint_1_byte_test() {
  let n = 0b00000010
  let assert Ok(#(varint, _)) = read_varint(<<n:big-8>>)
  assert varint == 2
}

pub fn rest_varint_1_bytes_4_test() {
  let n = 0x04
  let assert Ok(#(varint, _)) = read_varint(<<n:big-8>>)
  assert varint == 4
}

pub fn read_varint_2_byte_test() {
  let n = 0b10010110_00000001
  let assert Ok(#(varint, _)) = read_varint(<<n:big-16>>)
  assert varint == 150
}

pub fn read_varint_2_byte_with_remaining_test() {
  let n = 0b10010110_00000001
  let assert Ok(#(varint, rest)) = read_varint(<<n:big-16, 12:int>>)
  assert rest == <<12:int>>
  assert varint == 150
}

pub fn read_compact_string_test() {
  let size = 0b0000_1101
  let assert Ok(#(str, _)) =
    try_read_compact_string(<<
      size:8,
      bit_array.from_string("Hello, World"):bits,
    >>)

  assert str == "Hello, World"
}

pub fn read_nullable_string_test() {
  let size = 0b00000000_0000_1100

  let assert Ok(#(str, _)) =
    try_read_nullable_string(<<
      size:16,
      bit_array.from_string("Hello, World"):bits,
    >>)

  assert str == "Hello, World"
}

pub fn encode_decode_varint_test() {
  let assert Ok(result) = encode_varint(150)
  let assert Ok(#(result, _)) = read_varint(result)
  assert result == 150
}

pub fn encode_decode_varint2_test() {
  let assert Ok(result) = encode_varint(1_123_150)
  let assert Ok(#(result, _)) = read_varint(result)
  assert result == 1_123_150
}
