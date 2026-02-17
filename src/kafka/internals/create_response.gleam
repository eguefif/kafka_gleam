//// This module create the response
//// It read the RequestHeader to dispatch the request to apis handler.
//// Each api's handlers have their own modules.

import gleam/bytes_tree.{type BytesTree}
import kafka/internals/headers.{HeaderV2}
import kafka/internals/request.{type TRequest, Request}

import kafka/internals/apis/key_api.{get_api_version_response}
import kafka/internals/apis/topic_api.{get_describe_topic_response}

pub fn build_response(request: TRequest) -> Result(BytesTree, Nil) {
  let Request(_, header, _) = request
  case header {
    HeaderV2(18, ..) -> {
      get_api_version_response(request)
    }
    HeaderV2(75, ..) -> {
      get_describe_topic_response(request)
    }
    _ -> Ok(get_not_implemented_api_key())
  }
}

fn get_not_implemented_api_key() -> BytesTree {
  bytes_tree.from_bit_array(<<>>)
}
