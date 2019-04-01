import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object twitterConsumer {

  val conf = new SparkConf().setAppName("twitter")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


  def main (args: Array[String]): Unit = {

    val host = "localhost:2181"
    val group = "test-consumer-group"
    val twitter_topics = "TwitterCapstone"
    val number_of_threads = 5
    val sparkConf = new SparkConf().setAppName("twitterConsumer").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sc = ssc.sparkContext




    sc.setLogLevel("OFF")

    ssc.checkpoint("checkpoint")

    val topicMap = twitter_topics.split(",").map((_, number_of_threads.toInt)).toMap
    val tweets = KafkaUtils.createStream(ssc, host, group, topicMap).map(_._2)

    //making JSON schema
    val jsonSchema = new StructType()
      .add("created_at", StringType)
      .add("text", StringType)
      .add("screen_name", StringType)
      .add("followers_count", StringType)
      .add("friend_count", StringType)
      .add("location", StringType)

    //applying schema to tweets

    case class twitterData (created_at: String, text: String, screen_name: String, followers_count: String,
                            friend_count: String, location: String)


    var df_tweets:DataFrame = null

    tweets.foreachRDD {
      rdd => if (df_tweets != null) {
        df_tweets = df_tweets.unionAll(rdd.toDF) // combine previous dataframe
      } else {
        df_tweets = rdd.toDF() // create new dataframe
      }
    }

    val twitterDS = df_tweets.as[twitterData]

    //val twitterDS = tweets.toDF("id","device").as[twitterData]
    val jsDF = twitterDS.select($"created_at", get_json_object($"json",
    "$.text").alias("text"),

      get_json_object($"json",
        "$.screen_name").alias("screen_name"),
      get_json_object($"json",
        "$.followers_count").alias("followers_count"),
      get_json_object($"json",
        "$.friend_count").alias("friend_count"),
      get_json_object($"json",
        "$.location").alias("location"))

    //tweets.print()
    jsDF.show()
    ssc.start()
    ssc.awaitTermination()


  }

}
