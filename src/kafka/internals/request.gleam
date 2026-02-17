import kafka/internals/headers.{type Header}

pub type TRequest {
  Request(size: Int, header: Header, body: RequestBody)
}

pub type RequestBody {
  ApiVersionRequestV4(
    client_software_name: String,
    client_software_version: String,
  )

  DescribeTopicRequestV0(
    topics: List(RequestComponent),
    response_partition_limit: Int,
    cursor: RequestComponent,
    tagged_field: Int,
  )
}

pub type RequestComponent {
  RequestTopic(tagged_field: Int, name: String)
  Partition(
    error_code: Int,
    parition_index: Int,
    leader_id: Int,
    leader_epoch: Int,
    isr_nodes: Int,
    eligible_leader_replicas: Int,
    last_know_elr: Int,
    offline_replicas: Int,
  )
  NextCursor(topic_name: String, partition_index: Int)
  //Cursor(topic_name: String, partition_index: Int)
  Cursor(partition_index: Int)
}

pub type ApiKeys {
  ApiVersion(start: Int, end: Int, tag_buffer: Int)
  DescribeTopicPartition(start: Int, end: Int, tag_buffer: Int)
}
