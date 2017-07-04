package com.lkl.relationship.calculation

import com.hankcs.hanlp.corpus.tag.Nature
import com.hankcs.hanlp.seg.common.Term

import scala.collection.mutable

/**
  * Created by root on 16-10-14.
  */
object CalculationUtil {
  private val escape_word = Set("等","的","是")
  private val escape_nature = Set(Nature.w,Nature.c,Nature.cc,Nature.ul,Nature.ule,Nature.ulian,Nature.uv,
    Nature.uz,Nature.p,Nature.pba,Nature.pbei,Nature.u,Nature.ud,Nature.ude1,Nature.ude2,Nature.ude3,Nature.udeng)
  /**
    * 从一句话已分词的句子中识别出品牌词
    * @param words
    */
  def recogniseBrand(words:Seq[Term]) = {
    val brand_words = mutable.Buffer[String]()
    //根据一句话中，切分出的品牌词前后词情况，判断一个切出的品牌词是否真的是品牌词，判断依据如下：
    // 1. 前后词是单个字的情况，则不是品牌词
    // 2. 前后字是音译名的情况，则不是品牌词
    for((w,i) <- words.zipWithIndex){
      var is_brand_word = true
      // 暂时先不处理英文品牌名的情况
      if("^[A-Za-z]+$".r.findFirstMatchIn(w.word).isEmpty){
        if(i>0){
          val pre_w = words(i-1)
          if(pre_w.length() == 1 && (!escape_nature.contains(pre_w.nature) || !escape_word.contains(pre_w.word)) || pre_w.nature == Nature.nrf){
            is_brand_word = false
          }
        }
        if(i < words.length - 1){
          val pos_w = words(i+1)
          if(pos_w.length() == 1 && (!escape_nature.contains(pos_w.nature) || !escape_word.contains(pos_w.word)) || pos_w.nature == Nature.nrf){
            is_brand_word = false
          }
        }
      }
      if(is_brand_word){
        brand_words += w.word
      }
    }
    brand_words
  }
}
