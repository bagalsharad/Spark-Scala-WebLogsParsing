
//https://sreejithrpillai.wordpress.com/2017/02/19/log-analyzer-example-using-spark-and-scala/ 

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD


case class LogSchema(address: String,
    datetime: String,
    action: Option[String])
    
    class TransformMapper extends Logging {
  
        def transform(events: RDD[LogSchema]) ={
          val e = events.map(x => (x.datetime,1)).reduceByKey(_ + _)
          e.saveAsTextFile("/Users/sharadbagal/BigData/data/logs_data/logoutput")
        }
}

object MapRawData extends Serializable with Logging {
  def mapRawLine (line: String): Option[LogSchema] ={
    try{
      val fields = line.split(",", -1).map(_.trim)
      Some(
        LogSchema(
          address = fields(0),
          datetime = fields(1).substring(13, 15),
          action = if (fields(2).length > 2) Some(fields(2)) else None
        )
      )
      
    }catch{
    case e: Exception =>
        log.warn(s"Unable to parse line: $line")
        None
    }
  }
}
