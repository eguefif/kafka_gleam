import gleam/bit_array
import gleam/int
import gleam/result

pub type Request {
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
}

pub fn process_message(bytes: BitArray) -> Result(Request, Nil) {
  case bytes {
    <<
      size:32,
      request_api_key:16,
      request_api_version:16,
      correlation_id:32,
      rest:bits,
    >> -> {
      let body = case request_api_key {
        18 ->
          result.unwrap(
            get_body(rest),
            ApiRequestV4(
              client_software_name: todo,
              client_software_version: todo,
            ),
          )
        _ -> todo
      }
      Ok(HeaderV2(
        size,
        request_api_key,
        request_api_version,
        correlation_id,
        body,
      ))
    }
    _ -> Error(Nil)
  }
}

fn get_body(_bytes: BitArray) -> Result(Body, Nil) {
  Ok(ApiRequestV4(
    client_software_name: "hello",
    client_software_version: "world",
  ))
  //let (client_software_name, rest) = read_compact_string(bytes)
  //let (client_software_name, rest) = read_compact_string(rest)
}

//fn read_compact_string(bytes: BitArray) -> Tupple(Result(String, Nil), BitArray) {
//  let Tupple(string_size, rest) = read_varint(bytes)
//  let assert <<string_bytes:bytes, rest:bits>> =
//    rest(Ok(bit_array.to_string(string_bytes), rest))
//}

pub fn read_varint(bytes: BitArray) -> #(Int, BitArray) {
  let #(varint, size) = read_varint_acc(bytes, 0)
  let assert <<varint:int-big-signed-size(size)>> = varint
  let remaining_bits_size = size / 7
  let assert <<_:size(remaining_bits_size), rest:bits>> = bytes
  #(varint, rest)
}

fn read_varint_acc(bytes: BitArray, size: Int) -> #(BitArray, Int) {
  case bytes {
    <<1:1, number:7, rest:bits>> -> {
      let #(acc, size) = read_varint_acc(rest, size + 7)
      #(<<acc:bits, number:7>>, size)
    }
    <<0:1, number:7>> -> {
      #(<<number:7>>, size + 7)
    }
    _ -> {
      #(<<>>, size)
    }
  }
}
