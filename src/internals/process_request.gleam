import gleam/bit_array
import gleam/result
import internals/kpacket.{type Body, type KPacket, ApiRequestV4, HeaderV2, None}

pub fn process_request(bytes: BitArray) -> Result(KPacket, Nil) {
  case bytes {
    <<
      size:32,
      request_api_key:16,
      request_api_version:16,
      correlation_id:32,
      rest:bits,
    >> -> {
      let assert #(Ok(client_id), rest) = read_nullable_string(rest)
      let assert <<tagged_fields:8, rest:bits>> = rest
      let body = case request_api_key {
        18 ->
          result.unwrap(
            get_body(rest),
            ApiRequestV4(
              client_software_name: "Hello",
              client_software_version: "World",
            ),
          )
        _ -> None
      }
      Ok(HeaderV2(
        size:,
        request_api_key:,
        request_api_version:,
        correlation_id:,
        client_id:,
        tagged_fields:,
        error: 0,
        body:,
      ))
    }
    _ -> Error(Nil)
  }
}

fn get_body(bytes: BitArray) -> Result(Body, Nil) {
  let assert #(Ok(client_software_name), rest) = read_compact_string(bytes)
  let assert #(Ok(client_software_version), _) = read_compact_string(rest)

  Ok(ApiRequestV4(client_software_name:, client_software_version:))
}

fn read_nullable_string(bytes: BitArray) -> #(Result(String, Nil), BitArray) {
  let assert <<size:int-big-size(16), rest:bits>> = bytes

  let assert <<raw_str:bytes-size(size), rest:bits>> = rest
  #(bit_array.to_string(raw_str), rest)
}

pub fn read_compact_string(bytes: BitArray) -> #(Result(String, Nil), BitArray) {
  let #(string_size, rest) = read_varint(bytes)
  let assert <<raw_str:bytes-size(string_size - 1), rest:bits>> = rest
  let str = bit_array.to_string(raw_str)
  #(str, rest)
}

pub fn read_varint(bytes: BitArray) -> #(Int, BitArray) {
  let #(varint, size) = read_varint_acc(bytes, 0)
  let assert <<varint:int-big-signed-size(size)>> = varint
  let varint_size = size / 7
  let assert <<_:size(varint_size * 8), rest:bits>> = bytes
  #(varint, rest)
}

fn read_varint_acc(bytes: BitArray, size: Int) -> #(BitArray, Int) {
  case bytes {
    <<1:1, number:7, rest:bits>> -> {
      let #(acc, size) = read_varint_acc(rest, size + 7)
      #(<<acc:bits, number:7>>, size)
    }
    <<0:1, number:7, _:bits>> -> {
      #(<<number:7>>, size + 7)
    }
    _ -> {
      panic as "Should never get there when decoding unsigned varint"
    }
  }
}
