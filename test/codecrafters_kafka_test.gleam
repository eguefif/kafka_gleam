import gleam/bit_array
import gleeunit
import internals/process_request

pub fn main() {
  gleeunit.main()
}

pub fn read_varint_1_byte_test() {
  let n = 0b00000010
  let #(varint, _) = process_request.read_varint(<<n:big-8>>)
  assert varint == 2
}

pub fn rest_varint_1_bytes_4() {
  let n = 0x04
  let #(varint, _) = process_request.read_varint(<<n:big-8>>)
  assert varint == 4
}

pub fn read_varint_2_byte_test() {
  let n = 0b10010110_00000001
  let #(varint, _) = process_request.read_varint(<<n:big-16>>)
  assert varint == 150
}

pub fn read_varint_2_byte_with_remaining_test() {
  let n = 0b10010110_00000001
  let #(varint, rest) = process_request.read_varint(<<n:big-16, 12:int>>)
  assert rest == <<12:int>>
  assert varint == 150
}

pub fn read_compact_string_test() {
  let size = 0b0000_1100
  let assert #(Ok(str), _) =
    process_request.read_compact_string(<<
      size:8,
      bit_array.from_string("Hello, World"):bits,
    >>)

  assert str == "Hello, World"
}
