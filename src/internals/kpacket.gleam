import gleam/list

pub type KPacket {
  HeaderV0(body: Body, correlation_id: Int)
  HeaderV2(
    body: Body,
    size: Int,
    request_api_key: Int,
    request_api_version: Int,
    correlation_id: Int,
    client_id: String,
    tagged_fields: Int,
  )
}

pub type Body {
  ApiRequestV4(client_software_name: String, client_software_version: String)
  ApiResponseV4(api_keys: List(ApiKeys), throttle: Int, tag_buffer: Int)
  ResponseError(code: Int)
  None
}

pub type ApiKeys {
  ApiVersion(start: Int, end: Int, tag_buffer: Int)
}

pub fn to_bitarray(packet: KPacket) -> BitArray {
  let header = header_to_bitarray(packet)
  let body = body_to_bitarray(packet.body)
  <<header:bits, body:bits>>
}

fn header_to_bitarray(header: KPacket) -> BitArray {
  case header {
    HeaderV0(_, correlation_id) -> <<correlation_id:int-big-size(32)>>
    _ -> {
      <<>>
    }
  }
}

fn body_to_bitarray(body: Body) -> BitArray {
  case body {
    ApiResponseV4(api_keys, throttle, tag_buffer) -> <<
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
  }
}
