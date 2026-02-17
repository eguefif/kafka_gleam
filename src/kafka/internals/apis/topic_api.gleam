//// Topic response handler

import gleam/bytes_tree.{type BytesTree}
import gleam/result
import kafka/internals/headers.{Hv1, get_header}
import kafka/internals/request.{
  type RequestBody, type RequestComponent, type TRequest, DescribeTopicRequestV0,
  Request, RequestTopic,
}
import kafka/internals/response.{
  type ResponseBody, type ResponseComponent, DescribeTopicResponseV0,
  ResponseTopic, craft_bytes_response,
}

pub fn get_describe_topic_response(request: TRequest) -> Result(BytesTree, Nil) {
  let Request(_, header, body) = request
  use header <- result.try(get_header(header, Hv1))
  let body = get_describe_topic_body(body)
  craft_bytes_response(header, body)
}

fn get_describe_topic_body(body: RequestBody) -> ResponseBody {
  let assert DescribeTopicRequestV0(topics, ..) = body
  DescribeTopicResponseV0(
    throttle_time: 0,
    topics: get_topics(topics),
    next_cursor: 255,
    tag_field: 0,
  )
}

fn get_topics(topics: List(RequestComponent)) -> List(ResponseComponent) {
  case topics {
    [first, ..rest] -> {
      let response_topics = get_topics(rest)
      [get_one_topic_response(first), ..response_topics]
    }
    [] -> []
  }
}

fn get_one_topic_response(topic: RequestComponent) -> ResponseComponent {
  let assert RequestTopic(_, name) = topic
  ResponseTopic(
    error_code: 3,
    name:,
    topic_id: "00000000-0000-0000-0000-000000000000",
    is_internal: False,
    partitions: 1,
    topic_authorized_operations: 0,
    tag_field: 0,
  )
}
