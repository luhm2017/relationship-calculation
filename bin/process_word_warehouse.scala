/**
  * 处理词库的脚本
  * Created by root on 16-10-11.
  */

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

val hc = new HiveContext(sc)
import hc.implicits._
// business_recommendation.core_nature_dictionary
sc.textFile("file:/home/yanghb/warehouse/brand_use/data/dictionary/CoreNatureDictionary.mini.txt").map{
  line =>
    val l = line.replaceAll("\t"," ")
    val s = l.indexOf(" ")
    val word = l.substring(0,s)
    val nature = l.substring(s+1)
    (word,nature)
}.toDF("word","nature").write.mode(SaveMode.Overwrite).saveAsTable("business_recommendation.core_nature_dictionary")

//business_recommendation.custom_dictionary
sc.textFile("file:/home/yanghb/warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt").map{
  line =>
    val l = line.replaceAll("\t"," ")
    val s = l.indexOf(" ")
    val word = l.substring(0,s)
    val nature = l.substring(s+1)
    (word,nature)
}.toDF("word","nature").write.mode(SaveMode.Overwrite).saveAsTable("business_recommendation.custom_dictionary")