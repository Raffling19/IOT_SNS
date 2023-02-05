import java.time.Duration
import java.util.Properties


import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.avro.data.Json
import java.time.Duration
import com.eclipsesource
import com.fasterxml.jackson.annotation.JsonValue
import play.api.libs
import com.eclipsesource

object Main extends App {

  import org.apache.kafka.streams.scala.serialization.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.kstream.{TimeWindows, Windowed, SlidingWindows}


  println("Preparing")
  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "sns-scala-application")
    val bootstrapServers = if (args.length > 0) args(0) else "iot-kafka-0.iot-kafka-headless.iot:9092,iot-kafka-1.iot-kafka-headless.iot:9092,iot-kafka-2.iot-kafka-headless.iot:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }
  val builder = new StreamsBuilder()
  val json_inside: KStream[String, String] = builder.stream[String, String]("snsinside")
  val json_outside: KStream[String, String] = builder.stream[String, String]("snsoutside")
  //val textLines: KStream[String, String] = builder.stream[String, String]("raffling-wordcount")

 
  def dump[T](value:T):T = {
    println(value)
    value
  }

  def dumpKV[K,V](k:K,v:V):(K,V) = {
    println(s"key=$k, value=$v")
    (k,v)
  }
  // {"digPir": 0,"lightInside": 12,"lightInsideLux": 0.53,"uptime": 1609.00,"red": 0,"green": 0,"blue": 0,"cct": 0,"lux": 0}
  case class SensorData(digPir: Int, lightInside: Int, lightInsideLux: Double, uptime: Double, red: Int, green: Int, blue: Int, cct: Int, lux: Int)
  //{"lightOutside": 4,"lightOutsideLux": 0.24,"uptime": 1.00}
  case class SensorDataOutside(lightOutside: Int, lightOutsideLux: Double, uptime: Double)

  def dataMaster_inside(k:String,v:String):(String,String) ={

    val cleanJsonString = v.replace("\u0000", "").replace("\u0002","")
    val json = libs.json.Json.parse(cleanJsonString)

    var inside_light_on = 0
    var shells_down_or_dark_outside = 0

    val data = SensorData(
      (json \ "digPir").as[Int],
      (json \ "lightInside").as[Int],
      (json \ "lightInsideLux").as[Double],
      (json \ "uptime").as[Double],
      (json \ "red").as[Int],
      (json \ "green").as[Int],
      (json \ "blue").as[Int],
      (json \ "cct").as[Int],
      (json \ "lux").as[Int]
    ) 

    if (data.lightInside >= 400) {
      inside_light_on = 1
    }

    if (data.cct == 0) {
      shells_down_or_dark_outside = 1
    }
    val out = s"""{"inside_light_on": $inside_light_on, "shells_down_or_dark_outside": $shells_down_or_dark_outside}"""
    (k,out)
  }

  def dataMaster_outside(k:String,v:String):(String,String) ={

    var light_ouside_enough = 0
    val cleanJsonString = v.replace("\u0000", "").replace("\u0002","")
    val json = libs.json.Json.parse(cleanJsonString)

    val data = SensorDataOutside(
      (json \ "lightOutside").as[Int],
      (json \ "lightOutsideLux").as[Double],
      (json \ "uptime").as[Double]
    ) 

    if (data.lightOutside >= 250) {
      light_ouside_enough = 1
    }

    val out = s"""{"light_ouside_enough":$light_ouside_enough }"""
    (k,out)
  }

  var lightI = 0
  println("Starting")
  val out_outside: KStream[String,String] = json_outside.map((a,b)=>dataMaster_outside(a,b))
  out_outside.to("sns_ouside_info")

  val out_inside: KStream[String,String] = json_inside.map((a,b)=>dataMaster_inside(a,b))
  out_inside.to("sns_inside_info")

  //json_inside.to("snsinside_is_someone_inside")
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  // Always (and unconditionally) clean local state prior to starting the processing topology.
  // We opt for this unconditional call here because this will make it easier for you to play around with the example
  // when resetting the application for doing a re-run (via the Application Reset Tool,
  // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
  //
  // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
  // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
  // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
  // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
  // See `ApplicationResetExample.java` for a production-like example.
  streams.cleanUp()

  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

}