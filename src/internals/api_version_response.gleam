import gleam/bit_array
import gleam/bytes_tree.{type BytesTree}
import internals/kpacket.{
  type Body, type KPacket, ApiResponseV4, HeaderV2, None, ResponseError,
}

pub fn get_api_version_response(request: KPacket) -> BytesTree {
  let header = get_header(request)
  let body = get_body(request)
  let response = kpacket.to_bitarray(HeaderV2(..header, body:))
  let response_size = bit_array.byte_size(response)
  bytes_tree.from_bit_array(<<response_size:size(32), response:bits>>)
}

pub fn get_body(request: KPacket) -> Body {
  let HeaderV2(_, _, request_api_version, ..) = request
  case request_api_version {
    1 | 2 | 3 | 4 -> {
      get_body_api_key(request)
    }
    _ -> {
      ResponseError(code: 35)
    }
  }
}

fn get_body_api_key(_request: KPacket) -> Body {
  None
}

fn get_header(request: KPacket) -> KPacket {
  let HeaderV2(_, request_api_key, request_api_version, correlation_id, _) =
    request

  HeaderV2(
    size: 0,
    request_api_key:,
    request_api_version:,
    correlation_id:,
    body: None,
  )
}
