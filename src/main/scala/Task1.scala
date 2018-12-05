import java.io.PrintWriter
import java.util.Date
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable.ListBuffer


object Task1{
  def main(args: Array[String]): Unit = {
    var start_time = new Date().getTime
    val conf = new SparkConf().setAppName("response_count").setMaster("local[*]")
    @transient
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate

    var method = args(1)
//    var raw = sc.textFile("Data/yelp_reviews_clustering_small.txt")
    var raw = sc.textFile(args(0))
    var N = args(2).toInt
    var iterations = args(3).toInt

    def comp_avg(member:List[(Int, Array[Double])], group_size: Int, dim: Int): Array[Double]={
      var mem = member.map(x=>x._2)//((001),(110)....)
      var sum = new Array[Double](dim)
      for(m<-mem){
        for(i <- 0 until m.size){
          sum(i)+=m(i)
        }
      }
      for(s <- 0 until sum.size){
        sum(s) = (sum(s).toDouble)/(group_size.toDouble)
      }
      sum
    }


    def distance(vect:Array[Double], cent:Map[Array[Double], Int]): Int={
      var res = -1
      var mindif = 10000000.0
      for(c<-cent){
        var dif = 0.0
        var centroid = c._1
        for(i <- 0 until centroid.size){
          //          print(math.sqrt((centroid(i)-vect(i))*(centroid(i)-vect(i))).toFloat)
          dif+= (centroid(i)-vect(i))*(centroid(i)-vect(i))
        }
        dif = math.sqrt(dif)
        if(dif<mindif) {
          mindif = dif
          res = c._2
        }
      }
      res
    }

    def kmeans(vect: List[Array[Double]], cent:Map[Array[Double], Int], dim:Int): Map[Int,Array[Double]]={
      var vec_group = vect.map(vec=>{
        var nearest = distance(vec,cent)
        (nearest,vec)
      })
      var group = vec_group.groupBy(x=>x._1)
      var new_group_cent = group.map(x=>{
        var group_num = x._1
        var member = x._2 // ((001),(110)....)
        var group_size = member.size
        var new_cent = comp_avg(member, group_size, dim)
        (group_num, new_cent)
      })
      new_group_cent
    }

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
    var allword_list = allword.toList
    var word_index = allword.zipWithIndex.map{case(v,i) => (v, i)}
    var word_map = word_index.toMap
    var doc_word_num = word.map(doc=>{
      var num = doc.map(x=> word_map(x)).toSet.toArray
      num
    }).collect().toList


    var centroid = Map[Array[Double],Int]()
    var _word = sc.emptyRDD[Array[Double]]

    //kmeans
      if (method == "W"){
        var word_count = word.map(review=>{word2count(review, word_map, allword.size)})// ((0,0,0,1,0,0,2),(0,0,0,1,0,0,2),(0,0,0,1,0,0,2))
            //    word_count.foreach(println)
        print(allword_size)
        var cent = word_count.takeSample(withReplacement=false, num = N, seed = 20181031).zipWithIndex.map{case(v,i) => (v, i)}.toMap //(((001310),0),....)
//        word_count.foreach(println)

        for(i <- 0 until iterations){
          var new_cent = kmeans(word_count.collect().toList, cent, allword_size)
          cent = new_cent.map(x=>(x._2, x._1))
        }
//        cent.map(x=>(x._1.toList, x._2)).foreach(println)
        centroid = cent
        _word = word_count
      }


    //tfidf

    if (method == "T"){
      var TF = word.map(review=>{word2TF(review, word_map, allword.size)})//((tf,tf...)....)
//      val normalizer = new Normalizer()
//      var TF_v = TF.map(a => Vectors.dense(a))
//      var _TF = normalizer.transform(TF_v).map(x=>x.toArray)

      var IDF = word_index.map(unique_word=>{word2IDF(unique_word, doc_word_num, doc_num)}).toMap//((word_int, idf)...)
//      IDF.foreach(println)
      var TFIDF = TF.map(review=>{
        for (i <- 0 until review.length){
          review(i)=review(i)*IDF(i)
        }
        review
      })

//      val TFIDF_v = TFIDF.map(a => Vectors.dense(a))
//      val TFIDF_n = normalizer.transform(TFIDF_v).map(x=>x.toArray)

      var cent = TFIDF.takeSample(withReplacement=false, num = N, seed = 20181031).zipWithIndex.map{case(v,i) => (v, i)}.toMap //(((001310),0),....)
      var _TFIDF = TFIDF.collect().toList

      for(i <- 0 until iterations){
        var new_cent = kmeans(_TFIDF, cent, allword_size)
        cent = new_cent.map(x=>(x._2, x._1))
      }
      centroid = cent
      _word = TFIDF
    }



    var cent_map = centroid.map(x=>(x._2,x._1))
    var word_group = _word.map(doc=>{(distance(doc, centroid), doc)})//((nearest, doc),(...))
    var group_error = word_group.groupBy(x=>x._1).map(doc_group=>{
      var member = doc_group._2.map(x=>x._2)
      var size = member.toList.length
      var cent = cent_map(doc_group._1)
      var err = 0.0
      var sqrt_err = 0.0
      for(m <- member){
        var dif = 0.0
        for(i <- 0 until cent.length){
          dif+= (cent(i)-m(i))*(cent(i)-m(i))
        }
        err+=dif
        sqrt_err+=dif
      }
      (doc_group._1, (err,sqrt_err,size))
    })

    var group_sqrterr = group_error.map(x=>(x._1, x._2._2)).collect().toList
    group_sqrterr.foreach(println)//((num, err),...)
    var wsse = group_error.map(x=>x._2._1).sum()
    println("wsse:"+wsse)

    var fre_term = centroid.map(doc=>{
      var group_num = doc._2
      var cent = doc._1
      var cent_index = cent.zipWithIndex.map{case(v,i) => (v, i)}
      var top_10 = cent_index.sortBy(x=>x._1).slice(cent_index.length-10, cent_index.length)
      var term = top_10.map(x=>allword_list(x._2))
      (group_num, term)
    }).toList// ((num,[term]),..)
    fre_term.take(1).foreach(println)
    fre_term.foreach(println)

    var group_size = group_error.map(x=>(x._1, x._2._3)).collect().toList
    group_size.foreach(println)

    //json
    case class Cluster(id:Int, size:Int, error: Double, term: List[String])
    case class All(algorithm:String, WSSE: Double, cluster: List[Cluster])

    var clusters = new ListBuffer[Cluster]()
    for(i <- 0 until N){
      var temp = Cluster(i, group_size(i)._2,group_sqrterr(i)._2,fre_term(i)._2.toList)
      clusters.append(temp)
    }
    var clusters_ = clusters.toList

    var alg = ""
    if (method == "W"){alg = "K-means"}
    else{alg = "TF-IDF" }

    val all = All(alg, wsse, clusters_)

    val json =
      (("algorithm" -> all.algorithm) ~
      ("WSSE" -> all.WSSE) ~
      ("clusters" ->
        all.cluster.map { c =>
          ("id" -> c.id) ~ ("size" -> c.size) ~ ("error" -> c.error) ~ ("terms" -> c.term)}))

    var res = pretty(render(json))

    var out_path = ""
    if(method=="W"){
      out_path = "Xijia_Chen_Cluster_small_W_5_20.json"
    }else{
      out_path = "Xijia_Chen_Cluster_small_T_5_20.json"
    }
    val writer = new PrintWriter(out_path)
    writer.write(res)
    writer.close()


  }


}
