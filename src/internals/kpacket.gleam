import gleam/list

pub type KPacket {
  Request(size: Int, header: Header, body: Body)
  Response(header: Header, body: Body)
}

pub type Header {
  HeaderV0(correlation_id: Int)
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

  ResponseError(code: Int)
  None
}

pub type PacketComponent {
  Topic(tagged_field: Int, name: String)
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
