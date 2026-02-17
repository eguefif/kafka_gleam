//// Api key handlers
//// This api is used by the client to learn about all the api and their versions
//// that the server provides

import gleam/bytes_tree.{type BytesTree}
import gleam/result
import kafka/internals/headers.{type Header, HeaderV2, Hv0, get_header}
import kafka/internals/request.{type TRequest, Request}
import kafka/internals/response.{
  type ResponseBody, ApiVersion, ApiVersionResponseV4, DescribeTopicPartition,
  ResponseError, craft_bytes_response,
}

pub fn get_api_version_response(request: TRequest) -> Result(BytesTree, Nil) {
  let Request(_, request_header, _) = request
  use header <- result.try(get_header(request_header, Hv0))
  let body = get_body(request_header)
  craft_bytes_response(header, body)
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

fn get_body_api_key() -> ResponseBody {
  let api_keys = [
    ApiVersion(start: 0, end: 4, tag_buffer: 0),
    DescribeTopicPartition(start: 0, end: 0, tag_buffer: 0),
  ]
  ApiVersionResponseV4(api_keys:, throttle: 0, tag_buffer: 0)
}
