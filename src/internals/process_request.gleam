import gleam/bit_array
import gleam/result
import internals/kpacket.{type Body, type KPacket, ApiRequestV4, HeaderV2}
import internals/read_bytes.{
  try_read_bytes, try_read_compact_string, try_read_i16, try_read_i8,
}

pub fn process_request(bytes: BitArray) -> Result(KPacket, Nil) {
  use #(size, request_api_key, request_api_version, correlation_id, rest) <- result.try(
    parse_header(bytes),
  )
  use #(client_id, rest) <- result.try(read_nullable_string(rest))
  use #(tagged_fields, rest) <- result.try(try_read_i8(rest))
  use body <- result.try(get_body(request_api_key, rest))
  Ok(HeaderV2(
    size:,
    request_api_key:,
    request_api_version:,
    correlation_id:,
    client_id:,
    tagged_fields:,
    body:,
  ))
}

fn parse_header(bytes: BitArray) -> Result(#(Int, Int, Int, Int, BitArray), Nil) {
  case bytes {
    <<
      size:32,
      request_api_key:16,
      request_api_version:16,
      correlation_id:32,
      rest:bits,
    >> ->
      Ok(#(size, request_api_key, request_api_version, correlation_id, rest))
    _ -> Error(Nil)
  }
}

fn get_body(request_api_key: Int, bytes: BitArray) -> Result(Body, Nil) {
  case request_api_key {
    18 -> try_get_api_version_body(bytes)
    _ -> Error(Nil)
  }
}

fn try_get_api_version_body(bytes: BitArray) -> Result(Body, Nil) {
  use #(client_software_name, rest) <- result.try(try_read_compact_string(bytes))
  use #(client_software_version, _) <- result.try(try_read_compact_string(rest))

  Ok(ApiRequestV4(client_software_name:, client_software_version:))
}

fn read_nullable_string(bytes: BitArray) -> Result(#(String, BitArray), Nil) {
  use #(size, rest) <- result.try(try_read_i16(bytes))
  use #(raw_str, rest) <- result.try(try_read_bytes(rest, size))
  use str <- result.try(bit_array.to_string(raw_str))
  Ok(#(str, rest))
}
