/**
 * An example for parsing common-crawl wat data files
 * using Spark.
 *
 * This particular script counts the number of crawls and groups them by
 * date. The results are stored as tuples containing (date, count).
 *
 * Example:
 *  (2014-09-05, 101231)
 *
 * Author: Dennis J. McWherter, Jr.
 */

package org.microsonic.spark.cc

import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map

// NOTE: Should launch using spark-submit 
object DateFrequency
{
  def main(args: Array[String]): Unit = {
    var dataFile = "hdfs:///common-crawl/CC-MAIN-20140820021320-00000-ip-10-180-136-8.ec2.internal.warc.wat.gz"
    var outFile = "hdfs:///hist-res"
    if(args.length > 0) {
      dataFile = args(0)
      if(args.length > 1) {
        outFile = args(1)
      }
    }
    val conf = new SparkConf().setAppName("DateFreq")
    val sc = new SparkContext(conf)
    val file = sc.textFile(dataFile)
    val wat = file.filter(line => line.startsWith("{")).map(line => parse(line))
    wat.cache
    val data = for {
      JObject(outer) <- wat
      JField("Envelope", JObject(env)) <- outer 
      JField("WARC-Header-Metadata", JObject(meta)) <- env 
      JField("WARC-Date", JString(date)) <- meta 
    } yield (date.split("T")(0), 1)
    val result = data.reduceByKey(_+_)
    result.saveAsTextFile("hdfs:///hist-res")
    println("Dates found: " + result.count)
    sc.stop()
  }
}

