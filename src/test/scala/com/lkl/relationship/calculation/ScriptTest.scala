package com.lkl.relationship.calculation

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.hankcs.hanlp.dictionary.CoreDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.junit.Test

import scala.collection.JavaConversions._

/**
  * Created by root on 16-10-9.
  */
@Test
class ScriptTest {

  def testProcBrand(): Unit ={
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val hanPrt = "[\\u4E00-\\u9FA5]+".r
    val unHanPrt = "[^\\u4E00-\\u9FA5]+".r
    import hc.implicits._
    val non_brand_word = Set("IT","双赢","前海","新能源","三方","精准","深港","华强","福田", "诚信")
    val not_spilt = Set("e代驾")
    val df = hc.sql("select * from business_recommendation.brand").flatMap{
      row =>
        val brand_id = row.getAs[Long]("id")
        val company_name = row.getAs[String]("company_name")
        val brand_name = row.getAs[String]("name")
        if(!not_spilt.contains(brand_name)){
          (hanPrt.findAllMatchIn(brand_name).map(_.group(0)) ++ unHanPrt.findAllMatchIn(brand_name).map(_.group(0))).map(b => (brand_id,company_name,b, CoreDictionary.contains(b)))
        }else{
          Seq((brand_id,company_name,brand_name, CoreDictionary.contains(brand_name)))
        }
    }.filter(x => x._3.length>1 && !non_brand_word.contains(x._3) && !x._3.matches("^\\d+$") && !x._3.matches("^[\\da-zA-Z]{2}$")).
      groupBy(_._3).filter(_._2.map(x => x._1).toArray.distinct.length == 1).flatMap(x=>x._2).distinct().coalesce(1)
    df.toDF("brand_id","company_name","brand_name", "is_normal_word").write.mode(SaveMode.Overwrite).saveAsTable("business_recommendation.proc_brand")
    println(Nature.nx.toString == "nx")
  }

  def testWordWarehouseScript(): Unit ={
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
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

  }


  def testSegCompanyProfile(): Unit ={
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    import hc.implicits._
    hc.sql("select id, company_name, description from business_recommendation.company_profile").mapPartitions{
      rows =>
        rows.map{
          row =>
            val id = row.getAs[Long]("id")
            val company_name = row.getAs[String]("company_name")
            val desc = row.getAs[String]("description")
            (id, company_name, if(company_name == null) null else StandardTokenizer.segment(company_name).map(_.word), if(desc == null) null else StandardTokenizer.segment(desc).map(_.word))
        }
    }.toDF("id","company_name","company_name_words","desc_words").write.
      mode(SaveMode.Overwrite).saveAsTable("business_recommendation.company_profile_seg")
  }

  @Test
  def testGetBaseBrand(): Unit ={
    def cleanBaseBrand(company_name:String, b:String) = {
      val short_brand_name = Set("华为","宝洁","鞍钢","宝钢","鲁花","奥迪","邦德","森马","德芙","厨邦")
      HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
      HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
      StandardTokenizer.SEGMENT.enableCustomDictionary(false).enableTranslatedNameRecognize(false)
      try{
        if(b.length < 2) ""
        else if(b.length > 2) b
        else if((CoreDictionary.get(b).nature.toSet intersect Set(Nature.nz, Nature.ntc)).nonEmpty || short_brand_name.contains(b)) b
        else if(company_name.contains(b)){
          val start_index = company_name.indexOf(b) + b.length
          val company_name_tail = company_name.substring(start_index)
          val tail_words = StandardTokenizer.segment(company_name_tail)
          if(tail_words.size() != 0){
            val tail_word = tail_words.get(0)
            if(tail_word.nature != Nature.w){
              b + tail_word.word
            }else{
              ""
            }
          }else{
            ""
          }
        }
        else ""
      }catch {
        case e:Exception =>
          ""
      }
    }
    println(cleanBaseBrand("完美(中国)有限公司","完美"))
  }

  @Test
  def test(): Unit ={
    val line = "西游记\tnz\t5\tn\t1"
    val l = line.replaceAll("\t"," ")
    val s = l.indexOf(" ")
    println(l.substring(0,s))
    println(l.substring(s+1))
  }
}
