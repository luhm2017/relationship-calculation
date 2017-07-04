# relationship-calculation
图谱相关计算代码，SparkDataProcessing 中大部分代码主要先切词，再从切出的词中识别品牌词。

### 传输指令代码
    scp -P 50022 ~/IdeaProjects/enterprise-relationship/relationship-calculation/target/relationship-calculation-1.0-SNAPSHOT.jar yanghb@172.17.0.39:/home/yanghb/test
    scp -P 50022 .m2/repository/com/hankcs/hanlp/abundant-word-1.3.1/hanlp-abundant-word-1.3.1.jar yanghb@172.17.0.39:/home/yanghb/lib
    scp /home/yanghb/test/relationship-calculation-1.0-SNAPSHOT.jar yanghb@172.17.32.224:/home/yanghb/test/
    scp /home/yanghb/lib/hanlp-abundant-word-1.3.1.jar yanghb@172.17.32.224:/home/yanghb/lib/

### SparkDataProcessing.segBrandWord
    $SPARK_HOME/bin/spark-submit --master yarn --num-executors 5 --executor-memory 3G --driver-memory 1G \
    --jars /home/yanghb/lib/hanlp-abundant-word-1.3.1.jar \
    --class com.fxiaoke.relationship.calculation.SparkDataProcessing /home/yanghb/test/relationship-calculation-1.0-SNAPSHOT.jar \
    segBrandWord 50
    
### SparkDataProcessing.cutCompanyNameKeyWord
    $SPARK_HOME/bin/spark-submit --master yarn --num-executors 5 --executor-memory 3G --driver-memory 1G \
    --jars /home/yanghb/lib/hanlp-abundant-word-1.3.1.jar \
    --class com.fxiaoke.relationship.calculation.SparkDataProcessing /home/yanghb/test/relationship-calculation-1.0-SNAPSHOT.jar \
    cutCompanyNameKeyWord 50

### SparkDataProcessing.segCompanyProfile
    $SPARK_HOME/bin/spark-submit --master yarn --num-executors 5 --executor-memory 3G --driver-memory 1G \
    --jars /home/yanghb/lib/hanlp-abundant-word-1.3.1.jar \
    --class com.fxiaoke.relationship.calculation.SparkDataProcessing /home/yanghb/test/relationship-calculation-1.0-SNAPSHOT.jar \
    segCompanyProfile 50 

### SparkDataProcessing.recogniseSellCompany
    $SPARK_HOME/bin/spark-submit --master yarn --num-executors 5 --executor-memory 3G --driver-memory 1G \
    --jars /home/yanghb/lib/hanlp-abundant-word-1.3.1.jar \
    --class com.fxiaoke.relationship.calculation.SparkDataProcessing /home/yanghb/test/relationship-calculation-1.0-SNAPSHOT.jar \
    recogniseSellCompany 50 

## 品牌识别未处理问题
1. 常用词和品牌词区分，未处理
例如，识别"老板" 为品牌词异或非品牌词

2. "***公司华为服务区" 这种情况会切出"华为"品牌词，这种情况要处理

## warehouse说明
warehouse用来放自定义词库，在 DictionaryTest 中执行对应的test后生成的缓存文件需要方法，src/main/resources 中才能生效。

1. brand_name_have_ambiguity.txt
这个文件表示容易产生歧义的品牌词，这些品牌词在公司名中出现很可能是品牌词，但是在company_intro 和 desc中出现就不一定了。

2. brand_name_high_ambiguity.txt
这个文件中的品牌词是高度有歧义的，不管中company_name 中出现还是在company_intro 或 desc 中出现，都很难识别是品牌词。

3. 公司名_非品牌词.txt
这个文件中的公司名含有品牌词库中的品牌词，但是又是和品牌词库中对应的品牌完全没有关系的公司名。

4. 品牌词_带空格.txt
这个文件中的词带空格，所以不能直接使用 CustomDictionaryPath 加入词库，需要用 CustomDictionary.insertIntoTrie 加入词库。

5. 基础品牌词库.txt
最初来自 select distinct brand_name from business_recommendation.proc_brand2 where brand_name not regexp ' ' order by brand_name
基础品牌词需要进行自定义增减，每次词库改变时主要修改这个库

6. 扩充品牌词.txt
process_brand_hive.scala 中无法生成的品牌词