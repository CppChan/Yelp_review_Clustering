import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TFIDF {

  def main(args: Array[String]): Unit = {
    var start_time = new Date().getTime
    val conf = new SparkConf().setAppName("response_count").setMaster("local[*]")
    @transient
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate

    var raw = sc.textFile("Data/yelp_reviews_clustering_small.txt")
    var feature = "W"
    var N = 5
    var iterations = 20

    def word2count(review:Array[String], word_map:Map[String, Int], Size:Int): Array[Double] ={
      var count: Array[Double] = new Array[Double](Size)
      for(word <- review){
        var word_index = word_map(word)
        count(word_index)+=1.0
      }
      count
    }

    def word2TF(review:Array[String], word_map:Map[String, Int], Size:Int): Array[Double] ={
      var count: Array[Double] = new Array[Double](Size)
      for(word <- review){
        var word_index = word_map(word)
        count(word_index)+=1.0
      }
      var mostfreq = count.max
      var res = count.map(x=> x / mostfreq)
      res
    }
    var array = Array[String]("aa","bb","aa")
    var immutableMap = Map("aa" -> 0, "bb" -> 1)
    var Size = 2

    def word2IDF(unique_word:(String,Int), doc_word_num:List[Array[Int]], Size:Int):(Any,Double) ={
      var app_count = 0
      for(i <- 0 until doc_word_num.length){
        var _doc = doc_word_num(i).toSet
        if (_doc.contains(unique_word._2)){app_count+=1}
      }
      (unique_word._2, Math.log(Size.toDouble/app_count.toDouble))
    }

    var word = raw.map(row => row.split(" "))
    var word_list = word.collect().toList
    var doc_num = word_list.size
    var ret = Set.empty[String]
    var allword = word.flatMap(x=>x).collect().toSet
    var allword_size = allword.size
    var word_index = allword.zipWithIndex.map{case(v,i) => (v, i)}
    var word_map = word_index.toMap
    var doc_word_num = word.map(doc=>{
      var num = doc.map(x=> word_map(x)).toSet.toArray
      num
    }).collect().toList

    var TF = word.map(review=>{word2TF(review, word_map, allword.size)}).collect().toList//((tf,tf...)....)
//    TF.foreach(x=>x.foreach(println))
    var IDF = word_index.map(unique_word=>{word2IDF(unique_word, doc_word_num, doc_num)}).toMap//((word_int, idf)...)
    IDF.foreach(println)
    var TFIDF = TF.map(review=>{
      for (i <- 0 until review.length){
        review(i)=review(i)*IDF(i)
      }
      review
    })
    //    word_count.foreach(println)



//    var all_word = word.flatMap(x=>x).collect()
//    var all_word_count = word2count(all_word, word_map, allword.size)



  }
}
