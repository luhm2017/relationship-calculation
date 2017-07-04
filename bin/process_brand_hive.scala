/**
  * 切割brand表中的数据，令品牌名中的中英文分开。
  * spark-shell --jars /home/yanghb/lib/hanlp-abundant-word-1.3.1.jar
  * Created by root on 16-10-8.
  */

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import com.hankcs.hanlp.corpus.tag.Nature
import com.hankcs.hanlp.dictionary.CoreDictionary
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

val hc = new HiveContext(sc)
val hanPrt = "[\\u4E00-\\u9FA5·]+".r
val unHanPrt = "[^\\u4E00-\\u9FA5]+".r
import hc.implicits._
val non_brand_word = Set("双赢","前海","新能源","三方","精准","深港","华强","福田", "诚信","卡拉", "再保险","高端","网络通信","鞋柜","厨柜","门业","百强","佳格","安瑞",
  "中港","众合","挺拔","国贸","海珠","大王","江南","赛格","长岭","电子","大使","踏浪",
  "财经","神农","亲亲","中华","天助","天明","黎明","盼盼","艺龙","梅林","云顶","青木","绿牌","大众","众星","安卓","开元","黑猫","蓝光",
  "普天","三环","川崎","中捷","鹏程","龙翔","木林森","罗氏","富士","百家","丰田","万达","中信","天安","汇丰","中南","万通","光明","华晨",
  "正大","信诚","华瑞","腾龙","蓝海","旭日","神舟","力天","国泰","三丰","科创","欧亚","华商","兴华","鸿达","中银","天安","华丰","恒安",
  "新港","万向","恒兴","国信","三和","顺通","华源","永发","金泰","求是","奇美","信诺","华星","鼎力","泰达","美亚","盈科","利华","永利",
  "华龙","富华","中汇","安泰","同方","鸿基","金盾","万方","天元","星宇","华威","创想","嘉德","新力","力天","天虹","新源","陶氏","大昌",
  "天逸","龙翔","翔宇","恒盛","天健","广汇","富康","德宝","富邦","德宝","聚能","赛特","建华","星辉","皇明","华天","瑞华","扬子","大和",
  "美林","恒顺","裕隆","一帆","北新","天瑞","北新","宝利","国安","通达","星源","通联","明达","丰华","天翔","金牛","百川","华旗","海盛",
  "广信","颐和","天华","龙马","新联","耀华","天源","冠华","维维","东盛","维维","快钱","工美","旭辉","维达","隆兴","欣欣","合纵","东星",
  "海华","环亚","亨通","浩宇","德源","星海","沁园","富田","银星","蓝盾","瑞银","百盛","龙翔","安科","惠丰","飞马","红塔","中富","大丰",
  "万顺","天逸","红塔","同享","京海","高桥","万福","鹏飞","倍特","德赛","香雪","泛美","统一","德赛","三鼎","金福","儒商","金杜","晶华",
  "泰禾","五羊","河海","新飞","双汇","美达","众志","国正","实达","奔马","一帆","百大","东联","格格","易宝","双清","天悦","汉方","大成",
  "吉星","凯丰","金港","雅宝","黑龙","国强","盘锦","金花","爱乐","元丰","凯乐","恒康","明发","东盛","国丰","汇能","禾田","旭东","银桥",
  "创生","劲霸","鼎兴","舜天","兄弟","燕莎","东田","泰昌","中州","家宝","飞云","石基","塔牌","瑞泽","冠宇","中辰","恒欣","居易","金球",
  "美特","纳川","春江","北疆","红狮","天圣","永吉","振兴","三洲","露露","宝光","电通","千寻","惠康","倍特","金锣","亚星","长信","天兴",
  "东昌","中体","怡园","青龙","金兰","回天","双安","敖东","风光","咸亨","飞航","白象","鸿博","荣源","雄风","玉华","新马","健民","绿宝",
  "长丰","伊泰","翰海","辅仁","海螺","熊猫","美庐","双良","西王","双鸽","颐中","宏光","洽洽","云贵","蓝白","乘风","统一","富春","星锐",
  "萧氏","超凡","创生","超大","金雕","武陵","越美","修正","顺峰","星辰","佳缘","荣宝","双枪","道光","白猫","振兴","太子","天雨","御马",
  "凌霄","松竹","飞燕","成山","立博","闽山","正奥","日丰","双马","大洋","荣科","久立","萨博","互助","名邦","四海","三木","兰陵","悟道",
  "国风","合力","韶钢","真美","南台","国脉","怡莲","小刘","工正","摩天","禹王","涛涛","金鸽","福娃","利水","千红","红旗","锡柴","恒星",
  "辽海","豹王","巨峰","正章","朗逸","周氏","双雄","重山","创新","裕民","德昌","丹丹","延长","文通","联环","湖湘","星箭","故宫","涛涛",
  "劲力","西湖","大午","香香","天康","扶正","香江","火星人","晶晶","宝源","花园酒店","小宝贝","泛亚","标准","天音","零距离","四通",
  "中海","住友","兴发","信安","汇川","嘉华","富达","普惠","派克","祥龙","思源","海航","金正","张裕","九洲","力创","华语","富通","宏远",
  "威龙","海润","乐家","波导","极速","嘉美","诺华","天利","天力","云峰","广达","德龙","帝豪","永和","天合","天祥","博远","德意","物美",
  "开泰","王氏","双环","美图","云龙","雨润","春兰","新湖","延中")
val internet_brand = Set("京东","腾讯","百度","网易","搜狐","谷歌","途牛","新浪")
val non_brand_word_en = Set("wifi","100%","KTV","every","PLC","BBS","MOS","CCTV","CBD","ipad","iPhone","show","Head",
  "Amazon","ebay","Google","Android","iPod","sohu","sina","JAVA");
val not_spilt = Set("e代驾","1号店","21购物中心","ST钢构","AA租车","KC皮草","家装e站","百V厨柜","建业JY","58同城","100%感觉","SOR祛痘",
  "Ole'精品超市","TESCO乐购","伊利QQ星","坦克TANK","凯波CABLE","十铨TEAM","泛亚Fanya","好主人Care")
val short_brand_name = Set("华为","宝洁","鞍钢","宝钢","鲁花","奥迪","邦德","森马","德芙","厨邦")
val short_brand_name_en = Set("IBM")
val brand_rdd = hc.sql("select * from business_recommendation.brand").flatMap{
  row =>
    HanLP.Config.CoreDictionaryPath = "data/dictionary/CoreNatureDictionary.txt"
    HanLP.Config.BiGramDictionaryPath = "data/dictionary/CoreNatureDictionary.ngram.txt"
    StandardTokenizer.SEGMENT.enableCustomDictionary(false).enableTranslatedNameRecognize(false)
    val brand_id = row.getAs[Long]("id")
    val company_name = row.getAs[String]("company_name")
    val brand_name = row.getAs[String]("name")
    var is_change = false
    if(!not_spilt.contains(brand_name)){
      (hanPrt.findAllMatchIn(brand_name).map(_.group(0)).filter(b => !(non_brand_word union internet_brand).contains(b)).map{
        b =>
          val brand_in_name = company_name.contains(b)
          try{
            if(b.length < 2) ""
            else if(b.length > 2) b
            else if(((CoreDictionary.get(b).nature.toSet intersect Set(Nature.nz, Nature.ntc)).nonEmpty && brand_in_name) || short_brand_name.contains(b)) b
            else if(brand_in_name){
              val start_index = company_name.indexOf(b) + b.length
              val company_name_tail = company_name.substring(start_index)
              val tail_words = StandardTokenizer.segment(company_name_tail)
              if(tail_words.size() != 0){
                val tail_word = tail_words.get(0)
                if(tail_word.nature != Nature.w){
                  is_change = true
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
      }.filter(_ != "") ++ unHanPrt.findAllMatchIn(brand_name).map(_.group(0)).filter(b => (b.length>3 || short_brand_name_en.contains(b)) && !non_brand_word_en.contains(b))).
        map(b => (brand_id,company_name,b, CoreDictionary.contains(b),is_change))
    }else{
      Seq((brand_id,company_name,brand_name, CoreDictionary.contains(brand_name), false))
    }
}.filter(x => !non_brand_word.contains(x._3) && !x._3.matches("^\\d+$") && !x._3.matches("^[\\da-zA-Z]{2}$")).
  groupBy(_._3).filter(_._2.map(x => x._2).toArray.distinct.length <= 2).flatMap(x=>x._2).distinct().coalesce(1)

// 用来做补充用的
val sub_brand = hc.sql("select brand_id, company_name, brand_supplement_name brand_name, false is_normal_word, true is_change from business_recommendation.brand_supplement").map{
  row =>{
    val brand_id = row.getAs[Long]("brand_id")
    val company_name = row.getAs[String]("company_name")
    val brand_name = row.getAs[String]("brand_name")
    val is_normal_word = row.getAs[Boolean]("is_normal_word")
    val is_change = row.getAs[Boolean]("is_change")
    (brand_id, company_name, brand_name, is_normal_word, is_change)
  }
}
(brand_rdd union sub_brand).distinct().coalesce(1).toDF("brand_id","company_name","brand_name", "is_normal_word", "is_change").write.mode(SaveMode.Overwrite).saveAsTable("business_recommendation.proc_brand2")
