package org.apache.spark.examples

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Executes a roll up-style query against Apache logs.
 *  
 * Usage: LogQuery [logFile]
 */
object AirLine {
  val airlineEx = List(
    """2008,12,13,6,1007,847,1149,1010""".stripMargin.lines.mkString,
    """2008,11,13,6,1110,1103,1413,1418""".stripMargin.lines.mkString
  )

  val l1 = List("k1,v11", "k2,v12", "k3,v13")
  val l2 = List("k1,v21", "k1,v24", "k3,v23")

  //  val exampleApacheLogs = List(
//    """10.10.10.10 - "FRED" [18/Jan/2013:17:56:07 +1100] "GET http://images.com/2013/Generic.jpg
//      | HTTP/1.1" 304 315 "http://referall.com/" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1;
//      | GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR
//      | 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR
//      | 3.5.30729; Release=ARP)" "UD-1" - "image/jpeg" "whatever" 0.350 "-" - "" 265 923 934 ""
//      | 62.24.11.25 images.com 1358492167 - Whatup""".stripMargin.lines.mkString,
//    """10.10.10.10 - "FRED" [18/Jan/2013:18:02:37 +1100] "GET http://images.com/2013/Generic.jpg
//      | HTTP/1.1" 304 306 "http:/referall.com" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1;
//      | GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR
//      | 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR
//      | 3.5.30729; Release=ARP)" "UD-1" - "image/jpeg" "whatever" 0.352 "-" - "" 256 977 988 ""
//      | 0 73.23.2.15 images.com 1358492557 - Whatup""".stripMargin.lines.mkString
//  )

  def main(args: Array[String]) {
    val stTime = System.currentTimeMillis()
    val sparkConf = new SparkConf().setAppName("Air Line").setMaster("local[4]")
      .set("spark.driver.maxResultSize", "3g")
    val sc = new SparkContext(sparkConf)

//    airLineRegex(args, sc)
//    airLineSql(sc)

    //
    sparkReducesideJoin(sc)

    sc.stop()
    val spTime = System.currentTimeMillis()
    println("-------- total time : "+ (spTime - stTime)/1000 )
  }

  def airLineSql (sc: SparkContext): Unit = {
//    bank-full.csv  seperator = ;
//    2008.csv  (big file) seperator = ,
    val hadoopLoc = "hdfs://127.0.0.1:9000/user/ken/"

    val file = "2008.csv"
    val sep = ","

    val f = sc.textFile(hadoopLoc + file).map(_.split(sep))
      .filter(s => !s(0).equals("Year")).map(s => (s(16), s(17))).distinct().cache()

    println("total airline: " + f.count())
  }

  def airLineRegex (args: Array[String], sc: SparkContext): Unit = {
    val dataSet =
      if (args.length == 1) sc.textFile(args(0)) else sc.parallelize(airlineEx)
    // scalastyle:off
    //    val apacheLogRegex =
    //      """^([\d.]+) (\S+) (\S+) \[([\w\d:/]+\s[+\-]\d{4})\] "(.+?)" (\d{3}) ([\d\-]+) "([^"]+)" "([^"]+)".*""".r
    val airlineReg =
      """^(\d{4}),(\d{1,2}),(\d{1,2}),(\d{1,2}),(\d{3,4}),(\d{3,4}),(\d{3,4}),(\d{3,4})""".r
    // scalastyle:on
    /** Tracks the total query count and number of aggregate bytes for a particular group. */
    class Stats(val count: Int, val numBytes: Int) extends Serializable {
      def merge(other: Stats) = new Stats(count + other.count, numBytes + other.numBytes)
      override def toString = "bytes=%s\tn=%s".format(numBytes, count)
    }

    def extractKey(line: String): Unit = {
      airlineReg.findFirstIn(line) match {
        case Some(airlineReg(year,month,dayofMonth,dayOfWeek,depTime,cRSDepTime,arrTime,cRSArrTime)) =>
          val date = s"$year/$month/$dayofMonth"
          println(s"$date : " + depTime)
        case _ => ""
      }
    }

    //    def extractStats(line: String): Stats = {
    //      airlineReg.findFirstIn(line) match {
    //        case Some(airlineReg(ip, _, user, dateTime, query, status, bytes, referer, ua)) =>
    //          new Stats(1, bytes.toInt)
    //        case _ => new Stats(1, 0)
    //      }
    //    }
    println("---------start airline")
    dataSet.map(line => (extractKey(line))).collect()

  }

  def sparkReducesideJoin (sc: SparkContext): Unit = {
    val table1 = sc.parallelize(l1)
    val table2 = sc.parallelize(l2)

    //table1 and table 2 are both very large
    val pairs1 = table1.map { x =>
      val pos = x.indexOf(',')
      (x.substring(0, pos), x.substring(pos + 1))
    }

    val pairs2 = table2.map { x =>
      val pos = x.indexOf(',')
      (x.substring(0, pos), x.substring(pos + 1))
    }

    val result = pairs1.join(pairs2)
    result.collect().foreach(println)
//    val output = "d:/wordcount-" + System.currentTimeMillis();
//    result.saveAsTextFile(output) //save result to local file or HDFS

  }

}
