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

pub fn get_header(
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
