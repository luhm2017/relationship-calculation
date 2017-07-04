/*
package com.lkl.relationship.calculation

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.io.IOUtil
import com.hankcs.hanlp.dictionary.{CoreDictionary, CustomDictionary}
import com.hankcs.hanlp.tokenizer.StandardTokenizer

/**
  * 这个类用来生成词库的缓存文件，放到resources，每次词库修改，修改这个类，执行一次，再将生成的bat文件放到resources中
  * Created by root on 16-11-21.
  */
object CreateDictionaryBat {

  def main(args: Array[String]): Unit = {
    IOUtil.isResourceParam = false
    // 注意品牌词不能是单个字，否则会报错
    HanLP.Config.CustomDictionaryPath = Array[String]("warehouse/brand_use/data/dictionary/custom/CustomDictionary.txt",
      "warehouse/brand_use/data/dictionary/custom/上海地名.txt ns", "warehouse/brand_use/data/dictionary/custom/全国地名大全.txt ns",
      "warehouse/brand_use/data/dictionary/custom/基础品牌词库.txt bn", "warehouse/brand_use/data/dictionary/custom/扩充品牌词.txt bn",
      "warehouse/brand_use/data/dictionary/custom/深圳地名.txt ns", "warehouse/brand_use/data/dictionary/custom/公司名_非品牌词.txt cn")
    HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
    HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
    StandardTokenizer.SEGMENT.enableCustomDictionary(false).enableTranslatedNameRecognize(false)
    println(CustomDictionary.get("宝源"))
    println(CoreDictionary.get("宝源"))
    println(StandardTokenizer.segment("阿尔卑斯"))
  }
}
*/
