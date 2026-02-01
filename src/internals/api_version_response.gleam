import gleam/bit_array

pub fn get_api_version_response(correlation_id) {
  bit_array.append(<<64:size(32)>>, <<correlation_id:size(32), 0:16>>)
}

pub fn get_body() {
  <<>>
}
