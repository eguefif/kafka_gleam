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

pub fn read_varint_2_byte_test() {
  let n = 0b10010110_00000001
  let #(varint, _) = process_request.read_varint(<<n:big-16>>)
  assert varint == 150
}
