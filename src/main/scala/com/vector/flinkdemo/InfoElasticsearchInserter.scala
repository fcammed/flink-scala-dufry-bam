package com.vector.flinkdemo

// ElasticSearch
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.apache.flink.api.common.functions.RuntimeContext

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.`type`.{TypeFactory, MapType}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.`type`.TypeReference

object InfoElasticsearchInserter {}

final class InfoElasticsearchInserter(index: String, mappingType: String) extends ElasticsearchSinkFunction[String] {

    def process(element: String,
                ctx: RuntimeContext,
                indexer: RequestIndexer): Unit = {
	  val json = readHashMap(element)
      val rqst: IndexRequest = Requests.indexRequest
        .index(index)
        .`type`(mappingType)
        .source(json)
      indexer.add(rqst)
    }
	
	
	
	def readHashMap(jsonString: String): java.util.HashMap[String, Any] = {		
		val json = new java.util.HashMap[String, Any]
		val mapper: ObjectMapper = new ObjectMapper()
		val iterator = mapper.readTree(jsonString).fields()
		while(iterator.hasNext) {
			val entry = iterator.next()
			val value: JsonNode = entry.getValue()
			value.getNodeType() match {
				case JsonNodeType.NUMBER => (if (value.toString().contains(".")) {
														json.put(s"${entry.getKey}",value.asDouble)
												} else {
														json.put(s"${entry.getKey}",value.asInt)
												})
				case _ 		=> (json.put(s"${entry.getKey}",value.asText))
				}
			}
		json
	}
  }