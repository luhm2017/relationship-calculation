/*
package com.lkl.relationship.calculation

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import com.hankcs.hanlp.dictionary.CustomDictionary
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

/**
  * Created by root on 16-10-9.
  */
object SparkDataProcessing {

  def recogniseSellCompany(hc:HiveContext, partitionsNum:Int): Unit ={
    import hc.implicits._
    // 短文本的关键字匹配可以比较宽松，用在company_name 和desc 这两个属性的描述比较规范化
    val saleReg = "销售|营销|经销|专卖店|贸易|代理|商行|维修|商店|经营|售后服务|批发".r
    // 长文本的匹配关键字要比较严格，用在intro的匹配中
    val longSentenceSaleReg = "主营|销售|经销|贸易|代理|维修|经营|售后服务|批发|战略合作|合作伙伴".r
    hc.sql("select id, reg_code, company_name, description, company_intro, products from business_recommendation.company_profile").mapPartitions{
      rows =>
        val bf_rows = rows.toBuffer
        HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
        HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
        HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
        StandardTokenizer.SEGMENT.enableNameRecognize(false).enableTranslatedNameRecognize(false)
        insertBrandWord()
        // 短文本取品牌词， 部分歧义品牌词不太可能在短文本中代表别的含义，所以可以使用有歧义的品牌词, 适用 company_name , products, description
        insertAmbiguityBrandName()
        // 切company_name 品牌
        val sortTextBrands = bf_rows.map{
          row =>
            val id = row.getAs[Long]("id")
            val reg_code = row.getAs[String]("reg_code")

            val company_name:String = {
              val company_name_raw = row.getAs[String]("company_name")
              if(company_name_raw != null) company_name_raw.replaceAll("\\(.*?\\)|（.*?）|【.*?】","") else null
            }
            val desc:String = {
              val desc_raw = row.getAs[String]("description")
              if(desc_raw != null) desc_raw.replaceAll(s"\\([^\\(\\)]*?($saleReg)[^\\(\\)]*?\\)|（[^（）]*?($saleReg)[^（）]*?）|【[^【】]*?($saleReg)[^【】]*?】","") else null
            }
            val desc_abstract = new StringBuilder()

            val products = row.getAs[String]("products")
            val products_words = if(products != null) StandardTokenizer.segment(products).toBuffer else mutable.Buffer[Term]()

            CustomDictionary.insertIntoTrie("统一","bn 1000")
            // 判断品牌经销公司逻辑
            // 1. 销售的产品有品牌信息的，为品牌经销公司
            val sell_brands_in_product = CalculationUtil.recogniseBrand(products_words)
            // 3. 公司名为经销型公司的，经营范围中所有提及的品牌为都放进销售品牌的sell_brands中，否则只将经营范围desc中提及的销售品牌放进sell_brands
            val sell_brands_in_desc = mutable.Buffer[String]()
            if(saleReg.findFirstMatchIn(company_name).nonEmpty){
              val name_words = if(company_name != null) StandardTokenizer.segment(company_name).toBuffer else mutable.Buffer[Term]()
              // 以下几行代码主要为了处理"深圳市安顺康机动车驾驶员培训有限公司华为报名处" 这种把"华为"作为品牌词切出来的情况
              if(name_words.count(w => w.nature == Nature.nis && w.word != "专卖店") > 1){
                val i = name_words.indexWhere(_.nature == Nature.nis) + 1
                name_words.remove(i,name_words.size-i)
              }
              if(name_words.count(w => w.nature == Nature.bn) == 1){
                sell_brands_in_desc ++= CalculationUtil.recogniseBrand(name_words)
              }
              if(desc != null){
                val desc_words = StandardTokenizer.segment(desc)
                sell_brands_in_desc ++= CalculationUtil.recogniseBrand(desc_words)
              }
              CustomDictionary.remove("统一")
            }else{
              CustomDictionary.remove("统一")
              if(desc != null){
                val desc_sentences = desc.split("；|。|;|！|!|？|\\?| |,|，")
                for(sentence<-desc_sentences){
                  if(saleReg.findFirstMatchIn(sentence).nonEmpty){
                    val desc_words = StandardTokenizer.segment(sentence)
                    val brands = CalculationUtil.recogniseBrand(desc_words)
                    if(brands.nonEmpty){
                      sell_brands_in_desc ++= brands
                      desc_abstract.append(sentence).append("。")
                    }
                  }
                }
              }
            }
            (id,reg_code,company_name, desc_abstract.toString(), products, sell_brands_in_product.distinct, sell_brands_in_desc.distinct)
          }

        // 长文本文本取品牌词， 不适用歧义品牌词，因为长文本很容易出现歧义词，所以去掉歧义词再进行切词。适用 company_intro
        removeAmbiguityBrandName()
        val longTextBrands = bf_rows.map{
          row =>
            val id = row.getAs[Long]("id")
            val reg_code = row.getAs[String]("reg_code")

            val intro:String = {
              val intro_raw = row.getAs[String]("company_intro")
              if(intro_raw != null) intro_raw.replaceAll(s"\\([^\\(\\)]*?($saleReg)[^\\(\\)]*?\\)|（[^（）]*?($saleReg)[^（）]*?）|【[^【】]*?($saleReg)[^【】]*?】|<[^\u4E00-\u9FA5]*?>","") else null
            }
            val intro_abstract = new StringBuilder()
            // 2. company_intro 中提到销售的句子有品牌信息的，为对应的品牌经销公司
            val sell_brands_in_intro = mutable.Buffer[String]()
            if(intro != null){
              val intro_sentences = intro.split("；|。|;|！|!|？|\\?| |,|，")
              for(sentence<-intro_sentences){
                if(longSentenceSaleReg.findFirstMatchIn(sentence).nonEmpty){
                  val intro_words = StandardTokenizer.segment(sentence)
                  val brands = CalculationUtil.recogniseBrand(intro_words)
                  if(brands.nonEmpty){
                    sell_brands_in_intro ++= brands
                    intro_abstract.append(sentence).append("。")
                  }
                }
              }
            }
            (id,reg_code ,intro_abstract.toString(), sell_brands_in_intro.distinct)
        }

        (sortTextBrands zip longTextBrands).map{
          case((id, reg_code, company_name, desc_abstract, products, sell_brands_in_product, sell_brands_in_desc),(_,_,intro_abstract,sell_brands_in_intro)) =>
            (id,reg_code,company_name,desc_abstract,products,intro_abstract,(sell_brands_in_product++sell_brands_in_desc++sell_brands_in_intro).distinct,
              sell_brands_in_product, sell_brands_in_intro, sell_brands_in_desc)
        }.toIterator
    }.filter(_._7.nonEmpty).
      toDF("id", "reg_code", "company_name", "desc_abstract", "products", "intro_abstract", "sell_brands", "sell_brands_in_product", "sell_brands_in_intro", "sell_brands_in_desc").
      repartition(partitionsNum).write.mode(SaveMode.Overwrite).saveAsTable("business_recommendation.company_sell_brand")
  }
  /**
    * 对的所有公司名进行切词
    *
    * @param hc
    * @param partitionsNum
    */
  def segCompanyProfile(hc:HiveContext, partitionsNum:Int): Unit ={
    import hc.implicits._
    hc.sql("select company from creditloan.s_c_loan_order").mapPartitions{
      rows =>
        rows.map{
          row =>
            val company_name = row.getAs[String]("company_name")
            (company_name,if(company_name == null) null else StandardTokenizer.segment(company_name).filter(_.nature != Nature.w).map(_.toString))
        }
    }.toDF("id","company_name","name_words","desc_words").repartition(partitionsNum).write.
      mode(SaveMode.Overwrite).saveAsTable("business_recommendation.company_profile_seg")
  }

  /**
    * 在公司名中切出品牌词
    * @param hc
    * @param partitionsNum
    */
  def segBrandWord(hc:HiveContext, partitionsNum:Int): Unit ={
    import hc.implicits._
    hc.sql("select company from creditloan.s_c_loan_order").mapPartitions{
      rows =>
        //用户自定义词典路径
        HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
        //核心词典路径
        HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
        //2元语法词典路径
        HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
        StandardTokenizer.SEGMENT.enableNameRecognize(false).enableTranslatedNameRecognize(false)
        insertBrandWord()
        rows.map{
          row =>
            val id = row.getAs[Long]("id")
            val reg_code = row.getAs[String]("reg_code")
            val company_name:String = {
              val company_name_raw = row.getAs[String]("company_name")
              if(company_name_raw != null) company_name_raw.replaceAll("\\(.*?\\)|（.*?）|【.*?】","") else null
            }
            val name_words = if(company_name != null) StandardTokenizer.segment(company_name).toBuffer else mutable.Buffer[Term]()
            // 以下几行代码主要为了处理"深圳市安顺康机动车驾驶员培训有限公司华为报名处" 这种把"华为"作为品牌词切出来的情况
            if(name_words.count(w => w.nature == Nature.nis && w.word != "专卖店") > 1){
              val i = name_words.indexWhere(_.nature == Nature.nis) + 1
              name_words.remove(i,name_words.size-i)
            }

            val desc_words = {
              val desc_raw = row.getAs[String]("description")
              if(desc_raw != null) StandardTokenizer.segment(desc_raw).toBuffer else mutable.Buffer[Term]()
            }

            val products_words = {
              val products_raw = row.getAs[String]("products")
              if(products_raw != null) StandardTokenizer.segment(products_raw).toBuffer else mutable.Buffer[Term]()
            }

            val intro_words = {
              val company_intro = row.getAs[String]("company_intro")
              if(company_intro != null) StandardTokenizer.segment(company_intro).toBuffer else mutable.Buffer[Term]()
            }

            val brand_in_name = CalculationUtil.recogniseBrand(name_words)
            val brand_in_desc = CalculationUtil.recogniseBrand(desc_words)
            val brand_in_product = CalculationUtil.recogniseBrand(products_words)
            val brand_in_intro = CalculationUtil.recogniseBrand(intro_words)

            (id,reg_code,company_name,
              if(name_words.isEmpty) null else name_words.map(_.toString),
              if(desc_words.isEmpty) null else desc_words.map(_.toString),
              if(products_words.isEmpty) null else products_words.map(_.toString),
              if(intro_words.isEmpty) null else intro_words.map(_.toString),
              brand_in_name.distinct, brand_in_desc.distinct, brand_in_product.distinct, brand_in_intro.distinct)
        }
    }.filter(t => (t._11 ++ t._10 ++ t._9 ++ t._8).nonEmpty).
      toDF("id", "reg_code", "company_name", "name_words", "desc_words", "products_words", "intro_words", "brand_in_name", "brand_in_desc", "brand_in_product", "brand_in_intro").
      repartition(partitionsNum).write.mode(SaveMode.Overwrite).saveAsTable("business_recommendation.company_brand_seg")
  }

  /**
    * 获取公司名和品牌词有歧义的公司特征名, 例如有品牌"格力" 获取 公司名"格力亚"
    *
    * @param hc
    */
  def cutCompanyNameKeyWord(hc: HiveContext): Unit ={
    import hc.implicits._
    val natureSet = Set(Nature.ns,Nature.n,Nature.nis,Nature.v,Nature.vn,Nature.mq, Nature.z, Nature.m,Nature.b,Nature.j, Nature.a, Nature.f)
    hc.sql("select id, company_name from business_recommendation.company_profile where company_name is not null").mapPartitions{
      rows =>
        HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
        insertBrandWord()
        CustomDictionary.add("统一", "bn 1000")
        StandardTokenizer.SEGMENT.enableNameRecognize(false)
        rows.map{
          row =>
            val id = row.getAs[Long]("id")
            val company_name_raw = row.getAs[String]("company_name")
            val company_name = company_name_raw.replaceAll("\\(.*\\)"," ").replaceAll("（.*）"," ")
            val words = StandardTokenizer.segment(company_name)
            val deque = new util.LinkedList(words)
            var continue = true
            // 正向去词
            while(continue){
              if(deque.isEmpty){
                continue = false
              }else{
                val w = deque.peek()
                if(w.word.length>1 && w.nature != Nature.bn || w.nature == Nature.w){
                  deque.removeFirst()
                }else{
                  continue = false
                }
              }
            }
            continue = true
            // 反向去词
            while(continue){
              if(deque.isEmpty){
                continue = false
              }else{
                val w = deque.peekLast()
                if(w.word.length>1 && w.nature != Nature.bn || w.nature == Nature.w){
                  deque.removeLast()
                }else{
                  continue = false
                }
              }
            }
            // 只组合有一个品牌词和多个单字词的情况
            if(deque.count(_.word.length > 1)>1 || deque.count(_.nature == Nature.bn)!=1){
              deque.clear()
            }
            (id, company_name_raw, words.map(_.toString), deque.map(_.toString), deque.map(_.word).mkString(""))
        }
    }.filter(_._4.nonEmpty).toDF("id", "company_name", "name_words", "key_words", "id_word")
      .write.mode(SaveMode.Overwrite).saveAsTable("business_recommendation.company_name_seg")
  }

  // 这些品牌词带空格，不能放到txt中直接初始化到词典中，只能在使用的时候手动加入
  private def insertBrandWord(): Unit ={
    val in = getClass.getClassLoader.getResourceAsStream("brand_name_have_space.txt")
    val br = Source.fromInputStream(in)
    for(brand_name <- br.getLines()){
      CustomDictionary.insertIntoTrie(brand_name, "bn 1000")
    }
    br.close()
    in.close()
  }

  private def insertAmbiguityBrandName(): Unit ={
    val in = getClass.getClassLoader.getResourceAsStream("brand_name_have_ambiguity.txt")
    val br = Source.fromInputStream(in)
    for(brand_name <- br.getLines()){
      CustomDictionary.insertIntoTrie(brand_name, "bn 1000")
    }
    br.close()
    in.close()
  }

  private def removeAmbiguityBrandName(): Unit ={
    val in = getClass.getClassLoader.getResourceAsStream("brand_name_have_ambiguity.txt")
    val br = Source.fromInputStream(in)
    for(brand_name <- br.getLines()){
      CustomDictionary.remove(brand_name)
    }
    br.close()
    in.close()
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("com.lkl.relationship.calculation.SparkDataProcessing")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress","true")
    sparkConf.set("spark.hadoop.mapred.output.compress","true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")

    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    args(0) match {
      case "segBrandWord" =>
        val partitionsNum = args(1).toInt
        segBrandWord(hc,partitionsNum)
      case "cutCompanyNameKeyWord" =>
        cutCompanyNameKeyWord(hc)
      case "segCompanyProfile" =>
        val partitionsNum = args(1).toInt
        segCompanyProfile(hc,partitionsNum)
      case "recogniseSellCompany" =>
        val partitionsNum = args(1).toInt
        recogniseSellCompany(hc,partitionsNum)
    }
  }
}
*/
