pub type KPacket {
  HeaderV2(
    size: Int,
    request_api_key: Int,
    request_api_version: Int,
    correlation_id: Int,
    body: Body,
  )
}

pub type Body {
  ApiRequestV4(client_software_name: String, client_software_version: String)
  ApiResponseV4(api_keys: List(ApiKeys), throttle: Int)
  ResponseError(code: Int)
  None
}

pub type ApiKeys {
  ApiVersion(start: Int, end: Int)
}

pub fn to_bitarray(_packet: KPacket) -> BitArray {
  <<>>
}
