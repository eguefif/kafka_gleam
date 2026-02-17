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
  Hv0
  Hv1
  Hv2
}
