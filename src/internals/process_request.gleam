import gleam/result
import internals/kpacket.{
  type Body, type Header, type KPacket, type PacketComponent,
  ApiVersionRequestV4, Cursor, DescribeTopicRequestV0, HeaderV2, Request,
  RequestTopic,
}
import internals/read_bytes.{
  read_varint, try_read_compact_string, try_read_i32, try_read_i8,
  try_read_nullable_string,
}

pub fn process_request(bytes: BitArray) -> Result(KPacket, Nil) {
  use #(size, rest) <- result.try(try_read_i32(bytes))
  use #(header, api_key, rest) <- result.try(get_header(rest))
  use body <- result.try(get_body(api_key, rest))
  Ok(Request(size:, header:, body:))
}

fn get_header(bytes: BitArray) -> Result(#(Header, Int, BitArray), Nil) {
  use #(request_api_key, request_api_version, correlation_id, rest) <- result.try(
    parse_header(bytes),
  )
  use #(client_id, rest) <- result.try(try_read_nullable_string(rest))
  use #(tagged_fields, rest) <- result.try(try_read_i8(rest))
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

fn get_body(request_api_key: Int, bytes: BitArray) -> Result(Body, Nil) {
  case request_api_key {
    18 -> try_get_api_version_body(bytes)
    75 -> try_get_describe_topic_body(bytes)
    _ -> Error(Nil)
  }
}

fn try_get_api_version_body(bytes: BitArray) -> Result(Body, Nil) {
  use #(client_software_name, rest) <- result.try(try_read_compact_string(bytes))
  use #(client_software_version, _) <- result.try(try_read_compact_string(rest))

  Ok(ApiVersionRequestV4(client_software_name:, client_software_version:))
}

fn try_get_describe_topic_body(bytes: BitArray) -> Result(Body, Nil) {
  use #(topics, rest) <- result.try(try_read_compact_array(
    bytes,
    try_read_topic,
  ))
  use #(response_partition_limit, rest) <- result.try(try_read_i32(rest))
  use #(cursor, rest) <- result.try(try_read_cursor(rest))
  use #(tagged_field, _) <- result.try(try_read_i8(rest))
  Ok(DescribeTopicRequestV0(
    topics:,
    response_partition_limit:,
    cursor:,
    tagged_field:,
  ))
}

fn try_read_compact_array(
  bytes: BitArray,
  parsing_func: fn(BitArray) -> Result(#(value, BitArray), Nil),
) -> Result(#(List(value), BitArray), Nil) {
  use #(size, rest) <- result.try(read_varint(bytes))
  read_compact_array_loop(rest, size, parsing_func)
}

fn try_read_topic(bytes: BitArray) -> Result(#(PacketComponent, BitArray), Nil) {
  use #(name, rest) <- result.try(try_read_compact_string(bytes))
  use #(tagged_field, rest) <- result.try(try_read_i8(rest))
  Ok(#(RequestTopic(tagged_field:, name:), rest))
}

fn read_compact_array_loop(
  bytes: BitArray,
  size: Int,
  parsing_func: fn(BitArray) -> Result(#(value, BitArray), Nil),
) -> Result(#(List(value), BitArray), Nil) {
  case size {
    1 -> Ok(#([], bytes))
    _ -> {
      use #(array_item, rest) <- result.try(parsing_func(bytes))
      use #(array, rest) <- result.try(read_compact_array_loop(
        rest,
        size - 1,
        parsing_func,
      ))
      Ok(#([array_item, ..array], rest))
    }
  }
}

fn try_read_cursor(bytes) -> Result(#(PacketComponent, BitArray), Nil) {
  //use #(topic_name, rest) <- result.try(try_read_compact_string(bytes))
  use #(partition_index, rest) <- result.try(try_read_i8(bytes))
  Ok(#(Cursor(partition_index:), rest))
}
