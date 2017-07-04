package com.lkl.relationship.calculation

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.io.IOUtil
import com.hankcs.hanlp.dictionary.{CoreDictionary, CustomDictionary}
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.junit.Test
/**
  * Created by root on 16-10-26.
  */
@Test
class DictionaryTest {

  @Test
  def testBrandDictionary(): Unit ={
    ///IOUtil.isResourceParam = true
    // 注意品牌词不能是单个字，否则会报错
    HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt",
      "warehouse/brand_use/data/dictionary/custom/上海地名.txt ns", "warehouse/brand_use/data/dictionary/custom/全国地名大全.txt ns",
      "warehouse/brand_use/data/dictionary/custom/基础品牌词库.txt bn", "warehouse/brand_use/data/dictionary/custom/扩充品牌词.txt bn",
      "warehouse/brand_use/data/dictionary/custom/深圳地名.txt ns", "warehouse/brand_use/data/dictionary/custom/公司名_非品牌词.txt cn")
    HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
    HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
    StandardTokenizer.SEGMENT.enableCustomDictionary(false).enableTranslatedNameRecognize(false)
    println(CustomDictionary.get("和"))
    println(CoreDictionary.get("和"))
//    println(StandardTokenizer.segment("阿尔卑斯"))
//    println(StandardTokenizer.segment("长春通胜医药经销有限公司"))
//    println(StandardTokenizer.segment("北京种字林贸易有限公司位于北京市朝阳朝阳区北京市朝阳区麦子店街38号北京三全公寓101,交通便利，我们属于外资企业"))
  }
}
