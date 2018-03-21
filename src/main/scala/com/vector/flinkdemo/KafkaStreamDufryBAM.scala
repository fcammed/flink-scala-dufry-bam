package com.vector.flinkdemo

/*When using Flink's Scala DataSet API it is necessary to add the following import to your code: import   org.apache.flink.api.scala._.
When using Flink's Scala DataStream API you have to import import org.apache.flink.streaming.api.scala._.
The reason is that the package object contains a function which generates the missing TypeInformation instances.
*/ 


import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink


//import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//import org.apache.flink.api.common.functions.RuntimeContext


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.DataStream
//import org.apache.flink.streaming.api.scala.KeyedStream

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011 //con
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011 // FlinkKafkaProducer08 no era compatible con el cliente consumer de la 11

//CEP
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.PatternStream
import org.apache.flink.cep.scala.pattern.Pattern

//import org.apache.flink.streaming.util.serialization.SimpleStringSchema; DEPRECATED !!!!
import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.api.common.functions.FlatMapFunction
//
//import org.apache.flink.api.common.io._
//import org.apache.flink.api.common.typeinfo._

//import org.apache.flink.streaming.api._

//import com.vector.flinkdemo.InfoElasticsearchInserter._
//import org.example.EventsDufry._
//import com.vector.flinkdemo.TickTokenizeFlatMap._

import java.util.concurrent.TimeUnit
import java.util.Properties
//import java.util.stream.Stream
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date


//import scala.util.parsing.json.JSONObject
//import scala.util.parsing.json.JSON
//import java.net.URLEncoder
import java.net.InetSocketAddress
import java.net.InetAddress
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/**
 * Implements the "DufryBAM" program that do Pattern matching
 * over some sample data
 * Send the response over Kafka Topic
 */
object KafkaStreamDufryBAM {
	//import DufryEvents._
	val window = Time.of(10, TimeUnit.SECONDS)  //por si queremos agreados dentro de una ventana de tiempo
	val pattern = "1"
	type Tokenizado  	= (String, Int)
	type Evento 		= String
	type Alerta 		= String
	val formatter: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
	
	
	def main(args: Array[String]) {
	if (args.length != 0) {
      System.err.println("USAGE:\nKafkaStreamDufryBAM <parammetros?>")
      return
    }

	val properties = new Properties()
		properties.setProperty("bootstrap.servers", "localhost:9092")
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "localhost:2181")
		properties.setProperty("group.id", "flink_dufry")
		
	val env = StreamExecutionEnvironment.getExecutionEnvironment
	//por si queremos calculos en función del EventTime
	env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		
	
	val myConsumer = new FlinkKafkaConsumer011[String]("tick", new SimpleStringSchema, properties)
	val stream: DataStream[String] = env.addSource(myConsumer)
		.assignTimestampsAndWatermarks(new IngestionTimeExtractor[String]())
				
				
	val myProducer = new FlinkKafkaProducer011[String](
        "localhost:9092",         // broker list
        "pattern-event",               // target topic
        new SimpleStringSchema)   // serialization schema
		

	val streamjson = stream
		.flatMap(new TickTokenizeFlatMap) //convierte un JSON Text en Collector cuya //Key es el campo cmd:
		.keyBy(0)
		.timeWindow(window)
		.sum(1)
		.map(x => generateJson(x))
	
	streamjson
		.addSink(myProducer).name("Kafka_Reduce")

	//CEP
	// Version que funcionaba generando un String
	def createAlert( li: scala.collection.Map[String, Iterable[String]]): String = {
		var tipo = ""
		var mensaje = ""
		if(li.contains("start1")) {
			tipo = "INFO"
			mensaje = "Ciclo de Caja Normal"
		} else if(li.contains("start2")){
			tipo = "ERROR"
			mensaje = "Cierre sin Apertura"
		} else if(li.contains("start3")){
			tipo = "ERROR"
			mensaje = "Apertura no esperada"
		} else if(li.contains("start4")){
			tipo = "ERROR"
			mensaje = "Ciclo sin Cierre"
		}
		
		val obj = Map("Alerta" -> tipo,
					"cmd" -> mensaje,
					//"ts" -> System.currentTimeMillis())
					"ts" -> formatter.format(new Date(System.currentTimeMillis())) )
		scala.util.parsing.json.JSONObject(obj).toString()
		}
				
	val streamEventos: DataStream[String] = stream
		.map(x => readJson("cmd", x))

//---------------------------------------------------------------------------------------------------	
	val  errorPattern1 = definePatron(1)
	val MonitoringEvent1: PatternStream[String] = CEP.pattern(
												streamEventos,	
												errorPattern1: Pattern[String,  _ <: String] )
	val alarmstream1: DataStream[String] = MonitoringEvent1.select(createAlert(_))
	alarmstream1
		.addSink(myProducer).name("Kafka_INFO")	
//---------------------------------------------------------------------------------------------------	
	val  errorPattern2: Pattern[String, String] = definePatron(2)
	val MonitoringEvent2: PatternStream[String] = CEP.pattern(
												streamEventos,
												errorPattern2: Pattern[String,  _ <: String] )
	val alarmstream2: DataStream[String] = MonitoringEvent2.select(createAlert(_))
	alarmstream2
		.addSink(myProducer).name("Kafka_ERROR")
//---------------------------------------------------------------------------------------------------
	val  errorPattern3: Pattern[String, String] = definePatron(3)
	val MonitoringEvent3: PatternStream[String] = CEP.pattern(
												streamEventos,
												errorPattern3: Pattern[String,  _ <: String] )
	val alarmstream3: DataStream[String] = MonitoringEvent3.select(createAlert(_))
	
	alarmstream3
		.addSink(myProducer).name("Kafka_ERROR")
//---------------------------------------------------------------------------------------------------
	val  errorPattern4: Pattern[String, String] = definePatron(4)
	val MonitoringEvent4: PatternStream[String] = CEP.pattern(
												streamEventos,
												errorPattern4: Pattern[String,  _ <: String] )
	val alarmstream4: DataStream[String] = MonitoringEvent4.select(createAlert(_))
	
	alarmstream4
		.addSink(myProducer).name("Kafka_ERROR")
// ElasticSearch ------------------------------------------------------------------------------
	val config = new java.util.HashMap[String, String]
	config.put("bulk.flush.max.actions", "1")
	config.put("cluster.name", "elasticsearch")
	
	val transportAddresses = new java.util.ArrayList[InetSocketAddress]
	transportAddresses.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300))
	
	streamjson.addSink ( new ElasticsearchSink(config, transportAddresses, new InfoElasticsearchInserter("ticket","ticket"))).name("ElasticSearch ticket")
	
	alarmstream1.union( alarmstream2, alarmstream3, alarmstream4).addSink ( new ElasticsearchSink(config, transportAddresses, new InfoElasticsearchInserter("alarma","alarma"))).name("ElasticSearch alarm")
    
	env.execute("Scala Dufry BAM Example")
	
  }
 
def definePatron(x: Int): Pattern[String, String] = {
  x match 	{
	  case 1 => {
				Pattern
					.begin[String]("start1")
						.where(txt => txt.equals("Apertura"))
					.next("middle1")
						.where(txt => txt.equals("Ticket")).oneOrMore.consecutive
					.next("end2")
						.where(txt => txt.equals("Cierre"))
					// .followedBy("end")
						// .where(txt => txt.equals("Cierre"))
					.within(Time.seconds(120))
				}
	  case 2 => {
				Pattern
					.begin[String]("start2")
						.where(txt => txt.equals("Cierre"))
					.notNext("end")
						.where(txt => txt.equals("Apertura"))
					.within(Time.seconds(120))  
				}
	  case 3 => {
				Pattern
					.begin[String]("start3")
						.where(txt => txt.equals("Apertura"))
					.next("end")
						.where(txt => txt.equals("Apertura"))
					.within(Time.seconds(120))  
				}
	  case 4 => {
				Pattern
					.begin[String]("start4")
						.where(txt => txt.equals("Ticket"))
					.next("end")
						.where(txt => txt.equals("Apertura"))
					.within(Time.seconds(120))  
				}
			}
		
  }

 
  def generateJson(x: (String, Int)): String = {
    val obj = Map("evento" -> x._1,
				"cmd" -> x._2,
				"ts" -> formatter.format(new Date(System.currentTimeMillis())) )
	scala.util.parsing.json.JSONObject(obj).toString()
  }
  
  def readJson(attr: String, jsonString: String): String = {
	val mapper: ObjectMapper = new ObjectMapper()
	val jsonNode: JsonNode = mapper.readTree(jsonString)
	jsonNode.get(attr).asText()
	}

}