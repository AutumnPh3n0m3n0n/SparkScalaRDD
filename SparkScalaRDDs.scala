import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._



object Main {

  case class eventTable(patientID: String, eventID: String, description: String, time: String, value: Double)
  case class mortalityTable(patientID: String, time: String, label: Int)

  def main(args:Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Bigdata for Bigguys").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val eventsdata = sc.textFile("toInput/events.csv")
    val mortalitydata = sc.textFile("toInput/mortality.csv")

    val eventSchema = StructType(List(
      StructField("patientID", StringType, true),
      StructField("eventID", StringType, true),
      StructField("description", StringType, true),
      StructField("time", StringType, true),
      StructField("value", FloatType, true)
    )
    )

    val eventDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").option("treatEmptyValuesAsNulls", "true").schema(eventSchema).load("toInput/events.csv")

    val eventsDF = eventDF.toDF()

    val mortalityRDD = mortalitydata.map{row => row.split(",")}.map{cols => mortalityTable(cols(0),cols(1),cols(2).toInt)}

    val mortalityDF = mortalityRDD.toDF()

    mortalityDF.show()
    eventsDF.show()

  }

}