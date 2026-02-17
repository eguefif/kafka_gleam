import gleam/bit_array
import gleam/bytes_tree.{type BytesTree}
import gleam/result
import kafka/internals/headers.{
  type Header, HeaderV0, HeaderV1, HeaderV2, Hv0, Hv1,
}
import kafka/internals/request.{
  type RequestBody, type RequestComponent, type TRequest, DescribeTopicRequestV0,
  Request, RequestTopic,
}
import kafka/internals/response.{
  type ResponseBody, type ResponseComponent, ApiVersion, ApiVersionResponseV4,
  DescribeTopicPartition, DescribeTopicResponseV0, ResponseError, ResponseTopic,
}

// TODO: extract each API function into their own module. This module should be a dispatch

pub fn build_response(request: TRequest) -> Result(BytesTree, Nil) {
  let Request(_, header, _) = request
  case header {
    HeaderV2(18, ..) -> {
      get_api_version_response(request)
    }
    HeaderV2(75, ..) -> {
      get_describe_topic_response(request)
    }
    _ -> Ok(get_not_implemented_api_key())
  }
}

fn get_not_implemented_api_key() -> BytesTree {
  bytes_tree.from_bit_array(<<>>)
}

pub fn get_api_version_response(request: TRequest) -> Result(BytesTree, Nil) {
  let Request(_, request_header, _) = request
  use header <- result.try(get_header(request_header, Hv0))
  let body = get_body(request_header)
  craft_bytes_response(header, body)
}

fn craft_bytes_response(
  header: Header,
  body: ResponseBody,
) -> Result(BytesTree, Nil) {
  use response <- result.try(response.to_bitarray(header, body))
  let response_size = bit_array.byte_size(response)
  Ok(bytes_tree.from_bit_array(<<response_size:size(32), response:bits>>))
}

fn get_header(
  header: Header,
  header_response_type: Header,
) -> Result(Header, Nil) {
  let assert HeaderV2(_, _, correlation_id, ..) = header
  case header_response_type {
    Hv0 -> Ok(HeaderV0(correlation_id:))
    Hv1 -> Ok(HeaderV1(correlation_id:, tag_buffer: 0))
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

fn get_body_api_key() -> ResponseBody {
  let api_keys = [
    ApiVersion(start: 0, end: 4, tag_buffer: 0),
    DescribeTopicPartition(start: 0, end: 0, tag_buffer: 0),
  ]
  ApiVersionResponseV4(api_keys:, throttle: 0, tag_buffer: 0)
}

fn get_describe_topic_response(request: TRequest) -> Result(BytesTree, Nil) {
  let Request(_, header, body) = request
  use header <- result.try(get_header(header, Hv1))
  let body = get_describe_topic_body(body)
  craft_bytes_response(header, body)
}

fn get_describe_topic_body(body: RequestBody) -> ResponseBody {
  let assert DescribeTopicRequestV0(topics, ..) = body
  DescribeTopicResponseV0(
    throttle_time: 0,
    topics: get_topics(topics),
    next_cursor: 255,
    tag_field: 0,
  )
}

fn get_topics(topics: List(RequestComponent)) -> List(ResponseComponent) {
  case topics {
    [first, ..rest] -> {
      let response_topics = get_topics(rest)
      [get_one_topic_response(first), ..response_topics]
    }
    [] -> []
  }
}

fn get_one_topic_response(topic: RequestComponent) -> ResponseComponent {
  let assert RequestTopic(_, name) = topic
  ResponseTopic(
    error_code: 3,
    name:,
    topic_id: "00000000-0000-0000-0000-000000000000",
    is_internal: False,
    partitions: 1,
    topic_authorized_operations: 0,
    tag_field: 0,
  )
}
