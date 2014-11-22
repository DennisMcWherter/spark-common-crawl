/**
 * An example for parsing common-crawl wat data files
 * using Spark.
 *
 * This script arranges number of searches based on web URI and date.
 * See notes below. This is not written very effectively.
 *
 * Author: Dennis J. McWherter, Jr.
 */

package org.microsonic.spark.cc

import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.Map

// NOTE: Should launch using spark-submit 
// I would not recommend this particular class. It does not make effective use of RDDs
// and will likely suffer performance impacts as a result (read: there ARE better ways to do this)
object PageFrequency
{
  def addToMap(m: Map[String,Map[String,Int]], kv:(String,String)): Map[String,Map[String,Int]] = {
    val uri = kv._1
    val date = kv._2
    if(m.contains(uri)) {
      // We have seen this uri
      if(m(uri).contains(date)) {
        m(uri)(date) += 1
      } else {
        m(uri)(date) = 1
      }
    } else {
      m(uri) = Map[String,Int]()
    }
    m
  }

  def mergeMaps(m:Map[String,Map[String,Int]], p:Map[String,Map[String,Int]]): Map[String,Map[String,Int]] = {
    p.foreach {
      case (k:String,v:Map[String,Int]) => {
        if(!m.contains(k)) {
          m(k) = v
        } else {
          val dates = m(k)
          v.foreach {
            case (k:String,v:Int) => {
              if(!dates.contains(k)) {
                dates(k) = v
              } else {
                dates(k) += v
              }
            }
          }
        }
      }
    }
    m
  }

  def main(args: Array[String]): Unit = {
    var dataFile = "hdfs:///common-crawl/CC-MAIN-20140820021320-00000-ip-10-180-136-8.ec2.internal.warc.wat.gz"
    if(args.length > 0) {
      dataFile = args(0)
    }
    val conf = new SparkConf().setAppName("PageFreq")
    val sc = new SparkContext(conf)
    val file = sc.textFile(dataFile)
    val wat = file.filter(line => line.startsWith("{")).map(line => parse(line))
    wat.cache
    val data = for {
      JObject(outer) <- wat
      JField("Envelope", JObject(env)) <- outer 
      JField("WARC-Header-Metadata", JObject(meta)) <- env 
      JField("WARC-Date", JString(date)) <- meta 
      JField("WARC-Target-URI", JString(uri)) <- meta
    } yield (uri, date.split("T")(0))
    val result = data.aggregate(Map[String,Map[String,Int]]())(addToMap, mergeMaps)
    println("Found: " + result.size + " uris")
    val resultSeq = result.map {
      case (k:String, v:Map[String,Int]) => {
        (k, v.map(kv => (kv._1, kv._2))(collection.breakOut): List[(String,Int)])
      }
    }(collection.breakOut): List[(String, List[(String,Int)])]
    val resRDD = sc.makeRDD(resultSeq)
    resRDD.saveAsTextFile("hdfs:///hist-res/test")
    sc.stop()
  }
}

