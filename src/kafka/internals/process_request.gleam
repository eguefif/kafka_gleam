//// Request module entry point.
//// This module provides on public function: process_request
//// It uses the headers and the request_body modules to parse the request
//// and create the Request record that will be used by create_response

import gleam/result
import kafka/internals/headers.{type Header, HeaderV2}

import kafka/internals/request.{
  type RequestBody, type RequestComponent, type TRequest, ApiVersionRequestV4,
  Cursor, DescribeTopicRequestV0, Request, RequestTopic,
}
import kafka/primitives/array.{read_compact_array}
import kafka/primitives/number.{read_i32, read_i8}
import kafka/primitives/str.{read_compact_string, read_nullable_string}

pub fn process_request(bytes: BitArray) -> Result(TRequest, Nil) {
  use #(size, rest) <- result.try(read_i32(bytes))
  use #(header, api_key, rest) <- result.try(get_header(rest))
  use body <- result.try(get_body(api_key, rest))
  Ok(Request(size:, header:, body:))
}

fn get_header(bytes: BitArray) -> Result(#(Header, Int, BitArray), Nil) {
  use #(request_api_key, request_api_version, correlation_id, rest) <- result.try(
    parse_header(bytes),
  )
  use #(client_id, rest) <- result.try(read_nullable_string(rest))
  use #(tagged_fields, rest) <- result.try(read_i8(rest))
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

fn get_body(request_api_key: Int, bytes: BitArray) -> Result(RequestBody, Nil) {
  case request_api_key {
    18 -> get_api_version_body(bytes)
    75 -> get_describe_topic_body(bytes)
    _ -> Error(Nil)
  }
}

fn get_api_version_body(bytes: BitArray) -> Result(RequestBody, Nil) {
  use #(client_software_name, rest) <- result.try(read_compact_string(bytes))
  use #(client_software_version, _) <- result.try(read_compact_string(rest))

  Ok(ApiVersionRequestV4(client_software_name:, client_software_version:))
}

fn get_describe_topic_body(bytes: BitArray) -> Result(RequestBody, Nil) {
  use #(topics, rest) <- result.try(read_compact_array(bytes, read_topic))
  use #(response_partition_limit, rest) <- result.try(read_i32(rest))
  use #(cursor, rest) <- result.try(read_cursor(rest))
  use #(tagged_field, _) <- result.try(read_i8(rest))
  Ok(DescribeTopicRequestV0(
    topics:,
    response_partition_limit:,
    cursor:,
    tagged_field:,
  ))
}

fn read_topic(bytes: BitArray) -> Result(#(RequestComponent, BitArray), Nil) {
  use #(name, rest) <- result.try(read_compact_string(bytes))
  use #(tagged_field, rest) <- result.try(read_i8(rest))
  Ok(#(RequestTopic(tagged_field:, name:), rest))
}

fn read_cursor(bytes) -> Result(#(RequestComponent, BitArray), Nil) {
  //use #(topic_name, rest) <- result.try(try_read_compact_string(bytes))
  use #(partition_index, rest) <- result.try(read_i8(bytes))
  Ok(#(Cursor(partition_index:), rest))
}
