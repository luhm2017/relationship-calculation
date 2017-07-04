package com.lkl.relationship.calculation

import java.io.{BufferedReader, InputStreamReader}
import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.hankcs.hanlp.dictionary.{CoreDictionary, CustomDictionary}
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.junit.Test

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source
/**
  * Created by root on 16-10-8.
  */
@Test
class DataProcessingTest {

  @Test
  def testCustomDictionary() {
    CustomDictionary.add("Kuan's Living", "nz 1")
    println(StandardTokenizer.segment("Kuan's Living系列产品的批发，零售及家用空调的安装、维修；普通货运（道路运输经营许可证有效期至2011年7月1日）").map(_.word).filter(_.length>1))
    println(CustomDictionary.contains("Kuan's Living"))
  }

  @Test
  def testGetWordSpeech(): Unit ={
    HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
    HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
    HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
    StandardTokenizer.SEGMENT.enableNameRecognize(false).enableTranslatedNameRecognize(false)
    println(CustomDictionary.get("满堂红"))
    println(CoreDictionary.get("满堂红"))
  }

  @Test
  def testViterbiSeg(): Unit ={
    HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
    HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
    HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
    StandardTokenizer.SEGMENT.enableNameRecognize(false).enableTranslatedNameRecognize(false)
    println(StandardTokenizer.segment("丘萨克"))
    println(StandardTokenizer.segment("2008年雪润全国三十余家经销商举行第一次例会，共同分享了雪润产品带给大家以及顾客内在美的改变"))
//        println(StandardTokenizer.segment("绵阳市新兴瑞格中央空调销售有限责任公司格力中央空调专卖店"))
  }

  @Test
  def testCutCompanyNameKeyWord(): Unit ={
    HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
    val wordbf = StandardTokenizer.segment("通辽市巨宝门业有限公司")
    val deque = new util.LinkedList(wordbf)
    var continue = true
    // 正向去词
    while(continue){
      if(deque.isEmpty){
        continue = false
      }else{
        val w = deque.peek()
        if(w.word.length>1 && Set(Nature.ns,Nature.n,Nature.nis,Nature.v,Nature.vn, Nature.mq).contains(w.nature) || w.nature == Nature.w){
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
        if(w.word.length>1 && Set(Nature.ns,Nature.n,Nature.nis,Nature.v,Nature.vn, Nature.mq).contains(w.nature) || w.nature == Nature.w){
          deque.removeLast()
        }else{
          continue = false
        }
      }
    }
    if(deque.filter(w => /*w.nature == Nature.bn ||*/ w.nature == Nature.nz /*|| w.nature == Nature.cn*/).count(_.word.length > 1)>1){
      deque.clear()
    }
    println(wordbf)
    println(deque)
    println(deque.map(_.toString))
  }

  @Test
  def testSegBrandWord(): Unit ={
    HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
    val name_words = {
      val company_name = "格力亚空调销售有限公司"
      if(company_name != null) StandardTokenizer.segment(company_name).toBuffer else mutable.Buffer[Term]()
    }
    val desc_words = {
      val desc = "本公司销售格力亚空调等设备"
      if(desc != null) StandardTokenizer.segment(desc).toBuffer else mutable.Buffer[Term]()
    }

    val brand_words = mutable.Buffer[String]()
    val all_words = name_words ++ mutable.Buffer(new Term(" ",Nature.w)) ++ desc_words
    for((w,i) <- all_words.zipWithIndex){
      var is_brand_word = true
      if(i>0){
        val pre_w = all_words.get(i-1)
        if(pre_w.length() == 1 && pre_w.nature != Nature.w){
          is_brand_word = false
        }
      }
      if(i < all_words.size - 1){
        val pos_w = all_words.get(i+1)
        if(pos_w.length() == 1 && pos_w.nature != Nature.w){
          is_brand_word = false
        }
      }
      if(is_brand_word){
        brand_words += w.word
      }
    }
    println(name_words)
    println(desc_words)
    println(brand_words)
  }

  @Test
  def testTemp(): Unit ={
    HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt")
    val name_words = StandardTokenizer.segment("深圳市国贸物业管理有限公司华为科研中心停车场").toBuffer
    println(name_words.zipWithIndex)
    val i = name_words.indexWhere(_.nature == Nature.nis) + 1
    println(i)
    name_words.remove(i,name_words.size-i)
    println(name_words)
  }

}
