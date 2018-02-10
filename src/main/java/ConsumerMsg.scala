/**
  * Created by f.touopi.touopi on 1/9/2017.
  */
import java.sql.DriverManager._
import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark._
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}
import java.util

case  class Vote(candidateId:Long,voter:String)


class JDBCSink(url: String,user:String,pwd:String) extends ForeachWriter[Vote]{
  var driver="com.mysql.jdbc.Driver"
  var connection:Connection =_
  var statement:Statement=_

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection=DriverManager.getConnection(url,user,pwd)
    statement=connection.createStatement()
    true
  }

  override def process(value: Vote): Unit = {
    val query:String="INSERT INTO votes(candidate_id) " +
      "VALUES("+value.candidateId+")"
    statement.executeUpdate(query)
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close();
  }
}

object ConsumerMsg {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark-Kafka-Integration")
      .master("local[*]")
      .getOrCreate()
    // Subscribe to 1 topic
    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "vote-topic")
      .option("startingOffsets", "latest")
      .load()

//val df1=kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    val df1=kafka.selectExpr("split(value,';')[0] as candidateId","split(value,';')[1] as voter")

    val df2=df1.withColumn("candidateId", df1.col("candidateId").cast(sql.types.LongType))
      //.withWatermark("timestamp", "10 minutes")
   // val dfVote=df1.groupBy("cand").count().select("cand","count")
    import spark.implicits._
    val ds: Dataset[Vote]=df2.as[Vote]
     //val dsVote=ds.select("cand","count")
    ds.printSchema()

 /*   ds.writeStream
     // .outputMode("complete")
    .format("console")
    .option("truncate","false")
    .start()
    .awaitTermination()*/
    var url="jdbc:mysql://localhost:3306/votedb"
    var user="root"
    var pwd="admin"
  val writeJdbc=new JDBCSink(url,user,pwd)
   ds.writeStream
      .format("jdbc")
      .foreach(writeJdbc)
      .start()
      .awaitTermination()

  }
}

