# Kafka-connect-http
# ==================

# TODO
# ====
- task: make a generics version for `(|Kv)ParsedResponse`
  progress:

- task: Add Schemas for key and value's records on `SchemedKvSourceRecordMapper`
  details:
    - config keys:
      - http.response.record.key.schema
      - http.response.record.value.schema


# Done tasks
# ==========
- task: add configurable task for including paging and metadata into SourceRecords

- task: Use `RequestInput` as data-model for template engine
  progress: *done*
  details:
    - replace `TemplateModel` class

- task: Change HttpResponseParser for parse() returns `Response` instead of List<SourceRecord>
  progress: *done*
  details:
    - [x] change interface
    - [x] make an adapter for old response parser interface
    - [x] use paging and metadata from JacksonRecordParser

- task: Extend data passed to HttpRequestFactory
  progress: done
  details:
    - inmutable class for `Request` data-model
- task: new updating for paging and metadata on SourceTask
  progress: *done*
  details:
    - paging: update if at least one new record is commited
    - metadata: update at each HttpResponseParser.parse()



# Connector Task
# =============================================================================
HttpSourceTask:
  poll:
    desc: function that polls source task for new records should return source records
    notes:
      - var unseenRecords: new records (after sorting and filtering)
      - ConfirmationWindow: is a template class, receives its type at its constructor
    potential-changes:
      - records = responseRecords (clarify they come from http response)
      - unseenRecords = newRecords (using terms from Kafka Connect API)
    steps:
      - throttler.throttle(offset.getTimestamp().orElseGet(Instant::now))
      - HttpRequest request = requestFactory.createRequest(offset)
      - HttpResponse response = execute(request)
      - List<SourceRecord> records = responseParser.parse(response);
      - List<SourceRecord> unseenRecords = recordSorter.sort(records).stream()
                .filter(recordFilterFactory.create(offset))
                .collect(toList())
      - confirmationWindow = new ConfirmationWindow<>(extractOffsets(unseenRecords))
      - return unseenRecords

  throttler:
    type: TimerThrottler
    default: none
    config_key: none
    constructor:
      arguments:
        - timer: { type: Timer, default: AdaptableIntervalTimer, config_key: http.timer }

  requestFactory:
    type: HttpRequestFactory
    default: TemplateHttpRequestFactory
    config_key: http.request.factory

  requestExecutor:
    type: HttpClient
    default: OkHttpClient
    config_key: http.client

  responseParser:
    type: HttpResponseParser
    default: PolicyHttpResponseParser
    config_key: http.response.parser
    members:
      - parse: { signature: (HttpResponse response) -> List<SourceRecord> }
      - configure: { signature: (Map<String, ?> map) -> void }

  recordSorter:
    type: SourceRecordSorter
    default: OrderDirectionSourceRecordSorter
    config_key: http.record.sorter

  recordFilterFactory:
    type: SourceRecordFilterFactory
    default: OffsetRecordFilterFactory
    config_key: http.record.filter.factory


# Response Parser
# =============================================================================
Data flow:
  Object: Http response
  Conversions: [ HttpResponse, KvParsedResponse, ParsedResponse ]
  Converters:
  - KvParsedResponseHttpResponseParser.parse(HttpResponse) -> KvParsedResponse
  - KvHttpResponseParser.map(KvParsedResponse) -> ParsedResponse

HttpResponseParser:
  type: interface
  members:
    parse: { signature: parse(HttpResponse response) -> List<SourceRecord> }

PolicyHttpResponseParser:
  implements: HttpResponseParser
  members:
    delegate:
      default: KvHttpResponseParser
      config_key: http.response.policy.parser
    policy:
      default: StatusCodeHttpResponsePolicy
      config_key: http.response.policy

KvHttpResponseParser:
  implements: HttpResponseParser
  potential-changes:
    - change KvRecordHttpResponseParser to KvRecordParser
    - change KvSourceRecordMapper to KvToSourceRecordMapper
  members:
    recordParser:
      members:
        parse: { signature: (HttpResponse) -> List<KvRecord> }
      type: KvRecordHttpResponseParser
      default: JacksonKvRecordHttpResponseParser
      config_key: http.response.record.parser
    recordMapper:
      members:
        parse: { signature: (KvRecord) -> SourceRecord }
      type: KvSourceRecordMapper
      default: SchemedKvSourceRecordMapper
      config_key: http.response.record.mapper

## KvRecord(s) Parser
KvRecordHttpResponseParser:
  type: interface
  members:
    parse: { signature: parse(HttpResponse response) -> List<KvRecord> }

JacksonKvRecordHttpResponseParser:
  implements: KvRecordHttpResponseParser
  members:
    parse:
      signature: (HttpResponse response) -> List<KvRecord>
    map:
      signature: (JacksonRecord record) -> KvRecord
    responseParser:
      type: JacksonResponseRecordParser
    timestampParser:
      type: TimestampParser
      default: EpochMillisOrDelegateTimestampParser
      config_key: http.response.record.timestamp.parser

## JacksonRecord(s) Parser
JacksonResponseRecordParser:
  notes: uses one of its members' Configurable implementation,
  recordParser with config type JacksonRecordParserConfig
  potential-changes: removal. JacksonRecord is necessary?
  members:
    getRecords:
      signature: (byte[] body) -> Stream<JacksonRecord>
    recordParser:
      type: JacksonRecordParser
    recordsPointer:
      type: JsonPointer
      default: /
      config_key: http.response.list.pointer
      config_class: JacksonRecordParserConfig
    serializer:
      type: JacksonSerializer

## JsonNodeMap Parser
#  Getters for a deserialized json object (com.fasterxml.jackson.databind.JsonNode)
#  Use pointers for identifying key, value, timestamp, an offset elements. Offset
#  can have several elements
JacksonRecordParser:
  members:
    getOffset:
      signature: (JsonNode node) -> Map<String, Object>
      notes: Object is deserialized as string
    getValue:  { signature: (JsonNode node) -> String }
    getTimestamp:
      signature: (JsonNode node) -> Optional<String>
      status: deprecated
    getKey:
      signature: (JsonNode node) -> Optional<String>
      status: deprecated
    keyPointer:
      type: List<JsonPointer>
      default: ""
      config_key: http.response.record.key.pointer
    valuePointer:
      type: JsonPointer
      default: /
      config_key: http.response.record.pointer
    offsetPointers:
      type: Map<String, JsonPointer>
      default: ""
      config_key: http.response.record.offset.pointer
    timestampPointer:
      type: Optional<JsonPointer>
      default: ""
      config_key: http.response.record.timestamp.pointer

## JsonNode Parser
JacksonSerializer:
  members:
    deserialize: { signature: (byte[] body) -> JsonNode }
    serialize:   { signature: (JsonNode) -> String }
    getObjectAt: { signature: (JsonNode) -> JsonNode }
    getArrayAt:  { signature: (JsonNode, JsonPointer) -> Stream<JsonNode> }


# Record Sorter
# =============================================================================
SourceRecordSorter:
  type: interface
  members:
    sort:
      signature: (List<SourceRecord> records) -> List<SourceRecord>

OrderDirectionSourceRecordSorter:
  members:
    sort:
      signature: (List<SourceRecord> records) -> List<SourceRecord>


# Record Filter
# =============================================================================
SourceRecordFilterFactory:
  type: interface
  members:
    create:
      signature: (Offset offset) -> Predicate<SourceRecord>

OffsetRecordFilterFactory:
  members:
    create:
      signature: (Offset offset) -> Predicate<SourceRecord>
