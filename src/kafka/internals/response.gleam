//// This module contains all the Response related type and record along with their bytes encoding functions

import gleam/bit_array
import gleam/bytes_tree.{type BytesTree}
import gleam/list
import gleam/result
import kafka/internals/headers.{type Header, HeaderV0, HeaderV1}
import kafka/primitives/array.{compact_array_to_bytes}
import kafka/primitives/number.{encode_bool}
import kafka/primitives/str.{compact_nullable_string_to_bytes, encode_uuidv4}

pub type Response {
  Response(header: Header, body: ResponseBody)
}

pub fn craft_bytes_response(
  header: Header,
  body: ResponseBody,
) -> Result(BytesTree, Nil) {
  use response <- result.try(to_bitarray(header, body))
  let response_size = bit_array.byte_size(response)
  Ok(bytes_tree.from_bit_array(<<response_size:size(32), response:bits>>))
}

pub type ResponseBody {
  ApiVersionRequestV4(
    client_software_name: String,
    client_software_version: String,
  )
  ApiVersionResponseV4(api_keys: List(ApiKeys), throttle: Int, tag_buffer: Int)

  DescribeTopicRequestV0(
    topics: List(ResponseComponent),
    response_partition_limit: Int,
    cursor: ResponseComponent,
    tagged_field: Int,
  )
  DescribeTopicResponseV0(
    throttle_time: Int,
    topics: List(ResponseComponent),
    next_cursor: Int,
    tag_field: Int,
  )

  ResponseError(code: Int)
  None
}

pub type ResponseComponent {
  RequestTopic(tagged_field: Int, name: String)
  ResponseTopic(
    error_code: Int,
    name: String,
    topic_id: String,
    is_internal: Bool,
    partitions: Int,
    topic_authorized_operations: Int,
    tag_field: Int,
  )
  Partition(
    error_code: Int,
    parition_index: Int,
    leader_id: Int,
    leader_epoch: Int,
    isr_nodes: Int,
    eligible_leader_replicas: Int,
    last_know_elr: Int,
    offline_replicas: Int,
  )
  NextCursor(topic_name: String, partition_index: Int)
  //Cursor(topic_name: String, partition_index: Int)
  Cursor(partition_index: Int)
}

pub type ApiKeys {
  ApiVersion(start: Int, end: Int, tag_buffer: Int)
  DescribeTopicPartition(start: Int, end: Int, tag_buffer: Int)
}

pub fn to_bitarray(header: Header, body: ResponseBody) -> Result(BitArray, Nil) {
  let header = header_to_bitarray(header)
  use body <- result.try(body_to_bitarray(body))
  Ok(<<header:bits, body:bits>>)
}

fn header_to_bitarray(header: Header) -> BitArray {
  case header {
    HeaderV0(correlation_id) -> <<correlation_id:int-big-size(32)>>
    HeaderV1(correlation_id, tag_buffer) -> <<
      correlation_id:int-big-size(32),
      tag_buffer:int-big-size(8),
    >>
    _ -> {
      <<>>
    }
  }
}

fn body_to_bitarray(body: ResponseBody) -> Result(BitArray, Nil) {
  case body {
    ApiVersionResponseV4(api_keys, throttle, tag_buffer) ->
      Ok(<<
        0:int-big-size(16),
        { list.length(api_keys) + 1 }:int-big-size(8),
        api_keys_list_to_bitarray(api_keys):bits,
        throttle:int-big-size(32),
        tag_buffer:int-big-size(8),
      >>)
    DescribeTopicResponseV0(..) -> describe_topic_response_to_bytes(body)
    ResponseError(code) -> Ok(<<code:int-big-size(16)>>)
    _ -> Ok(<<>>)
  }
}

fn api_keys_list_to_bitarray(api_keys: List(ApiKeys)) -> BitArray {
  case api_keys {
    [first, ..rest] -> <<
      api_key_to_bitarray(first):bits,
      api_keys_list_to_bitarray(rest):bits,
    >>
    [] -> <<>>
  }
}

fn api_key_to_bitarray(api_key: ApiKeys) -> BitArray {
  case api_key {
    ApiVersion(start, end, tag_buffer) -> <<
      18:16,
      start:int-big-size(16),
      end:int-big-size(16),
      tag_buffer:int-big-size(8),
    >>

    DescribeTopicPartition(start, end, tag_buffer) -> <<
      75:16,
      start:int-big-size(16),
      end:int-big-size(16),
      tag_buffer:int-big-size(8),
    >>
  }
}

pub fn describe_topic_response_to_bytes(
  response: ResponseBody,
) -> Result(BitArray, Nil) {
  let assert DescribeTopicResponseV0(
    throttle_time,
    topics,
    next_cursor,
    tag_field,
  ) = response
  let throttle_time = <<throttle_time:int-big-size(32)>>
  use topics <- result.try(compact_array_to_bytes(
    topics,
    response_topic_to_bytes,
  ))
  let next_cursor = <<next_cursor:int-big-size(8)>>
  let tag_field = <<tag_field:int-big-size(8)>>
  Ok(<<throttle_time:bits, topics:bits, next_cursor:bits, tag_field:bits>>)
}

fn response_topic_to_bytes(topic: ResponseComponent) -> Result(BitArray, Nil) {
  let assert ResponseTopic(
    error_code,
    name,
    topic_id,
    is_internal,
    partitions,
    topic_authorized_operations,
    tag_field,
  ) = topic

  use topic_id <- result.try(encode_uuidv4(topic_id))
  use topic_name <- result.try(compact_nullable_string_to_bytes(name))
  Ok(<<
    error_code:int-big-size(16),
    topic_name:bits,
    topic_id:bits,
    encode_bool(is_internal):bits,
    partitions:int-big-size(8),
    topic_authorized_operations:int-big-size(32),
    tag_field:int-big-size(8),
  >>)
}
