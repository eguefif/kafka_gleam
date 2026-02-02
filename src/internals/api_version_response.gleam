import gleam/bit_array
import gleam/bytes_tree.{type BytesTree}
import gleam/result
import internals/kpacket.{
  type Body, type Header, type KPacket, ApiResponseV4, ApiVersion,
  DescribeTopicPartition, HeaderV0, HeaderV2, Request, Response, ResponseError,
}

pub fn get_api_version_response(request: KPacket) -> Result(BytesTree, Nil) {
  let assert Request(_, request_header, _) = request
  use header <- result.try(get_header(request_header))
  let body = get_body(request_header)
  let response = kpacket.to_bitarray(Response(header:, body:))
  let response_size = bit_array.byte_size(response)
  Ok(bytes_tree.from_bit_array(<<response_size:size(32), response:bits>>))
}

fn get_header(header: Header) -> Result(Header, Nil) {
  case header {
    HeaderV2(_, _, correlation_id, ..) -> Ok(HeaderV0(correlation_id:))
    _ -> Error(Nil)
  }
}

pub fn get_body(header: Header) {
  let assert HeaderV2(_, request_api_version, ..) = header
  case request_api_version {
    0 | 1 | 2 | 3 | 4 -> {
      get_body_api_key()
    }
    _ -> {
      ResponseError(code: 35)
    }
  }
}

fn get_body_api_key() -> Body {
  let api_keys = [
    ApiVersion(start: 0, end: 4, tag_buffer: 0),
    DescribeTopicPartition(start: 0, end: 0, tag_buffer: 0),
  ]
  ApiResponseV4(api_keys:, throttle: 0, tag_buffer: 0)
}
