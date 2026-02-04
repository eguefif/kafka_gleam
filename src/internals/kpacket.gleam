import gleam/bit_array
import gleam/list
import gleam/result
import gleam/string
import internals/read_bytes.{encode_varint}

pub type KPacket {
  Request(size: Int, header: Header, body: Body)
  Response(header: Header, body: Body)
}

pub type Header {
  HeaderV0(correlation_id: Int)
  HeaderV1(correlation_id: Int, tag_buffer: Int)
  HeaderV2(
    request_api_key: Int,
    request_api_version: Int,
    correlation_id: Int,
    client_id: String,
    tagged_fields: Int,
  )
}

pub type Body {
  ApiVersionRequestV4(
    client_software_name: String,
    client_software_version: String,
  )
  ApiVersionResponseV4(api_keys: List(ApiKeys), throttle: Int, tag_buffer: Int)

  DescribeTopicRequestV0(
    topics: List(PacketComponent),
    response_partition_limit: Int,
    cursor: PacketComponent,
    tagged_field: Int,
  )
  DescribeTopicResponseV0(
    throttle_time: Int,
    topics: List(PacketComponent),
    next_cursor: Int,
    tag_field: Int,
  )

  ResponseError(code: Int)
  None
}

pub type PacketComponent {
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

pub fn to_bitarray(packet: KPacket) -> BitArray {
  let assert Response(header, body) = packet
  let header = header_to_bitarray(header)
  let body = body_to_bitarray(body)
  <<header:bits, body:bits>>
}

fn header_to_bitarray(header: Header) -> BitArray {
  case header {
    HeaderV0(correlation_id) -> <<correlation_id:int-big-size(32)>>
    _ -> {
      <<>>
    }
  }
}

fn body_to_bitarray(body: Body) -> BitArray {
  case body {
    ApiVersionResponseV4(api_keys, throttle, tag_buffer) -> <<
      0:int-big-size(16),
      { list.length(api_keys) + 1 }:int-big-size(8),
      api_keys_list_to_bitarray(api_keys):bits,
      throttle:int-big-size(32),
      tag_buffer:int-big-size(8),
    >>
    ResponseError(code) -> <<code:int-big-size(16)>>
    _ -> <<>>
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

// To bytes function write:
// compact_array_to_bytes
// response_topic_bo_bytes
// will compact array partition with nothing for now.

pub fn compact_array_to_bytes(
  values: List(value),
  serialize_func: fn(value) -> Result(BitArray, Nil),
) -> Result(BitArray, Nil) {
  use dumps <- result.try(compact_array_to_bytes_loop(values, serialize_func))
  use list_size <- result.try(encode_varint(list.length(values) + 1))
  Ok(<<list_size:bits, dumps:bits>>)
}

fn compact_array_to_bytes_loop(
  list: List(value),
  serialize_func: fn(value) -> Result(BitArray, Nil),
) -> Result(BitArray, Nil) {
  case list {
    [first, ..rest] -> {
      use dumps <- result.try(compact_array_to_bytes(rest, serialize_func))
      use serialized_value <- result.try(serialize_func(first))
      Ok(<<serialized_value:bits, dumps:bits>>)
    }
    [] -> Ok(<<>>)
  }
}

fn response_topic_to_bytes(topic: PacketComponent) -> Result(BitArray, Nil) {
  let assert ResponseTopic(
    error_code,
    name,
    topic_id,
    is_internal,
    partitions,
    topic_authorized_operations,
    tag_field,
  ) = topic

  let topic_id = bit_array.from_string(topic_id)
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

fn encode_bool(bool: Bool) -> BitArray {
  case bool {
    True -> <<1:size(1)>>
    False -> <<0:size(1)>>
  }
}

fn compact_nullable_string_to_bytes(str: String) -> Result(BitArray, Nil) {
  let len = string.length(str) + 1
  use varint <- result.try(encode_varint(len))
  Ok(<<varint:bits, str:utf8>>)
}
