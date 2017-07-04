package com.lkl.relationship.calculation

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.junit.Test
import CalculationUtil._
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.{SparkConf, SparkContext}
import  com.hankcs.hanlp.corpus.io.IOUtil
import scala.collection.JavaConversions._

/**
  * Created by root on 16-10-20.
  * */
@Test
class CalculationUtilTest {

  @Test
  def testRecogniseBrand(): Unit ={
    /*HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
    HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
    HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
    StandardTokenizer.SEGMENT.enableNameRecognize(false).enableTranslatedNameRecognize(false)
    val words = StandardTokenizer.segment("苏州三全万丰电子科技有限公司深圳分公司")
    println(words)*/
    //println(recogniseBrand(words))
    //println(StandardTokenizer.segment("河南车宝行汽车销售有限公司成立于2015年4月，位于三全路中州大道一米阳光，附近有风雅颂、琥珀名城等小区及柳林等城中村，住宿便利。"))
    /*HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
    //HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
    HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
    val wordbf = StandardTokenizer.segment("通辽市巨宝门业有限公司")
    println(wordbf)
    val termList = HanLP.segment("商品和服务");
    println(termList)*/
    HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
    //HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
    //HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
    StandardTokenizer.SEGMENT.enableNameRecognize(false).enableTranslatedNameRecognize(false)
    println("通辽市巨宝门业有限公司")
    println(HanLP.segment("通辽市巨宝门业有限公司"))
    println("河南车宝行汽车销售有限公司")
    println(HanLP.segment("河南车宝行汽车销售有限公司"))
    println("苏州三全万丰电子科技有限公司深圳分公司")
    println(HanLP.segment("苏州三全万丰电子科技有限公司深圳分公司"))
    println("柳州德姿形象设计有限公司")
    println(HanLP.segment("柳州德姿形象设计有限公司"))
    println("广西壮族自治区|柳州市|柳江县基隆开发区祥和南路5号")
    println(HanLP.segment("广西壮族自治区|柳州市|柳江县基隆开发区祥和南路5号"))
    println("中国电信股份有限公司建始分公司")
    println(HanLP.segment("中国电信股份有限公司建始分公司"))
    println("广东省|深圳市|宝安区观澜街道章阁村延燕公寓1113房")
    println(HanLP.segment("广东省|深圳市|宝安区观澜街道章阁村延燕公寓1113房"))
    println("富泰华工业深圳有限公司")
    println(HanLP.segment("富泰华工业深圳有限公司"))

  }
}

