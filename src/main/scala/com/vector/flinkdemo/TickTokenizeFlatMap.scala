package com.vector.flinkdemo

import java.util.StringTokenizer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer

//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.connectors.twitter.TwitterSource
//import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData

object TickTokenizeFlatMap {}

class TickTokenizeFlatMap extends FlatMapFunction[String, (String, Int)] {
    lazy val jsonParser = new ObjectMapper()

    override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
      // deserialize JSON from twitter source
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      val hasText = jsonNode.has("cmd")

      (hasText, jsonNode) match {
        case (true, node) => {
          val tokens = new ListBuffer[(String, Int)]()
          val tokenizer = new StringTokenizer(node.get("cmd").asText())

          while (tokenizer.hasMoreTokens) {
            val token = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase()
            if (token.nonEmpty)out.collect((token, 1))
          }
        }
        case _ =>
      }
    }
}