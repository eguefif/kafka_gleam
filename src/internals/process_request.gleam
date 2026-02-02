import gleam/result
import internals/kpacket.{
  type Body, type Header, type KPacket, ApiRequestV4, HeaderV2, Request,
}
import internals/read_bytes.{
  try_read_compact_string, try_read_i32, try_read_i8, try_read_nullable_string,
}

pub fn process_request(bytes: BitArray) -> Result(KPacket, Nil) {
  use #(size, rest) <- result.try(try_read_i32(bytes))
  use #(header, api_key, rest) <- result.try(get_header(rest))
  use body <- result.try(get_body(api_key, rest))
  Ok(Request(size:, header:, body:))
}

fn get_header(bytes: BitArray) -> Result(#(Header, Int, BitArray), Nil) {
  use #(request_api_key, request_api_version, correlation_id, rest) <- result.try(
    parse_header(bytes),
  )
  use #(client_id, rest) <- result.try(try_read_nullable_string(rest))
  use #(tagged_fields, rest) <- result.try(try_read_i8(rest))
  Ok(#(
    HeaderV2(
      request_api_key,
      request_api_version,
      correlation_id,
      client_id,
      tagged_fields,
    ),
    request_api_key,
    rest,
  ))
}

fn parse_header(bytes: BitArray) -> Result(#(Int, Int, Int, BitArray), Nil) {
  case bytes {
    <<request_api_key:16, request_api_version:16, correlation_id:32, rest:bits>> ->
      Ok(#(request_api_key, request_api_version, correlation_id, rest))
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
