import com.github.catalystcode.fortis.spark.streaming.rss.RSSInputDStream
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object SentimentAnalyzer {
  def getSentiment(sentiment: Int): String = sentiment match {
    case x if x == 0 || x == 1 => "Negative"
    case 2 => "Neutral"
    case x if x == 3 || x == 4 => "Positive"
  }

  def main(args: Array[String]): Unit = {
    val durationSeconds = 30
    val urlCSV = "https://queryfeed.net/twitter?token=5bfec0d2-4657-4d2a-98d0-69f3584dc3b3&" +
      "q=%23" + args(0) + "&title-type=tweet-text-full&order-by=recent&geocode=&" +
      "omit-direct=on&omit-retweets=on"
    val urls = urlCSV.split(",")

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SentimentAnalyzer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    //val model = new StreamingLinearRegressionWithSGD()
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
    import spark.sqlContext.implicits._

    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds, readTimeout = 10000)

    stream.foreachRDD(rdd => {
      val ds = Utils.remover.transform(rdd.toDS()
        .drop(
          "source", "uri", "links", "content", "description",
          "enclosures", "publishedDate", "updatedDate", "authors"
        )
        .map(row => (row.getString(0).replaceAll("\t|\n| {8}", ""), Utils.tweetPreprocessing(row.getString(0))))
      ).drop("_2")
        .withColumn("sentiment", lit(-1))
        .map(row => {
          import scala.collection.JavaConverters._
          val pipeline = Utils.getOrCreateSentimentPipeline()
          val annotation = pipeline.process(row.getString(0))
          val tree = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
            .asScala
            .head
            .get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
          (row.getString(0), row.getSeq[String](1), getSentiment(RNNCoreAnnotations.getPredictedClass(tree)))
        })
        .withColumnRenamed("_1", "tweet")
        .withColumnRenamed("_2", "preprocessed")
        .withColumnRenamed("_3", "sentiment")
      //ds.show(false)
      ds.select("tweet", "sentiment").write.mode("append").format("com.databricks.spark.csv").save("myFile.csv")
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
