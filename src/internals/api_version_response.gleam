import gleam/bit_array
import gleam/bytes_tree.{type BytesTree}
import internals/kpacket.{
  type Body, type KPacket, ApiResponseV4, ApiVersion, HeaderV0, HeaderV2, None,
  ResponseError,
}

pub fn get_api_version_response(request: KPacket) -> BytesTree {
  let assert HeaderV0(_, _, correlation_id) = get_header(request)

  let body = get_body(request)
  let response =
    kpacket.to_bitarray(HeaderV0(
      body:,
      error: 0,
      correlation_id: correlation_id,
    ))
  let response_size = bit_array.byte_size(response)
  bytes_tree.from_bit_array(<<response_size:size(32), response:bits>>)
}

fn get_header(request: KPacket) -> KPacket {
  let assert HeaderV2(_, _, _, _, _, correlation_id, ..) = request

  HeaderV0(correlation_id:, error: 0, body: None)
}

pub fn get_body(request: KPacket) -> Body {
  let assert HeaderV2(_, _, _, _, request_api_version, ..) = request
  case request_api_version {
    1 | 2 | 3 | 4 -> {
      get_body_api_key()
    }
    _ -> {
      ResponseError(code: 35)
    }
  }
}

fn get_body_api_key() -> Body {
  let api_keys = [ApiVersion(start: 0, end: 4, tag_buffer: 0)]
  ApiResponseV4(api_keys:, throttle: 0, tag_buffer: 0)
}
