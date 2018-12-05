import java.io.PrintWriter
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.feature.Normalizer
import org.json4s.jackson.JsonMethods.{pretty, render}

import scala.collection.mutable.ListBuffer
import org.json4s.JsonDSL._


object Task2 {
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
      (unique_word._2, Math.log10(Size.toDouble/app_count.toDouble))
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

    var word = raw.map(row => row.split(" "))
    var word_list = word.collect().toList
    var doc_num = word_list.size
    var ret = Set.empty[String]
    var allword = word.flatMap(x => x).collect().toSet
    var allword_size = allword.size
    var allword_list = allword.toList
    var word_index = allword.zipWithIndex.map { case (v, i) => (v, i) }
    var word_map = word_index.toMap
    var doc_word_num = word.map(doc => {
      var num = doc.map(x => word_map(x)).toSet.toArray
      num
    }).collect().toList

//    var word_count = word.map(review => {
//      word2count(review, word_map, allword.size)
//    }) // ((0,0,0,1,0,0,2),(0,0,0,1,0,0,2),(0,0,0,1,0,0,2))

    var TF = word.map(review=>{word2TF(review, word_map, allword.size)})//((tf,tf...)....)
//    val normalizer = new Normalizer()
//    var TF_v = TF.map(a => Vectors.dense(a))
//    var _TF = normalizer.transform(TF_v).map(x=>x.toArray)

    var IDF = word_index.map(unique_word=>{word2IDF(unique_word, doc_word_num, doc_num)}).toMap//((word_int, idf)...)
    //      IDF.foreach(println)
    var TFIDF = TF.map(review=>{
      for (i <- 0 until review.length){
        review(i)=review(i)*IDF(i)
      }
      review
    })

    var _TFIDF = TFIDF.collect().toList


    var vec_count = TFIDF.map(x => Vectors.dense(x))
    var wsse = 0.0
    var centroid = Map[Array[Double],Int]()
    var word_group_b = new ListBuffer[(Int, Array[Double])]()

    if (method == "K") {
//      var numClusters = 8
//      var numIterations = 20
      var clusters = KMeans.train(vec_count, N, iterations, "k-means||", 42)
//      val clusters = KMeans.train(vec_count, N, iterations, 42)
      wsse = clusters.computeCost(vec_count)
//      println("Within Set Sum of Squared Errors = " + WSSE)
      var cent = clusters.clusterCenters.map(x=>x.toArray).toList.zipWithIndex.map{case(v,i) => (v, i)}.toMap
      centroid = cent
      var group_num = clusters.predict(vec_count).collect().toList
      var TFIDF_list = TFIDF.collect().toList
      var w_group = new ListBuffer[(Int, Array[Double])]()
      for(i <- 0 until TFIDF_list.length){
        word_group_b.append((group_num(i),TFIDF_list(i)))
      }

    }
//
    if (method == "B") {
      val bkm = new BisectingKMeans().setK(N).setSeed(42).setMaxIterations(iterations)
      val model = bkm.run(vec_count)
      wsse = model.computeCost(vec_count)
      println(s"Compute Cost: ${model.computeCost(vec_count)}")
      model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
        println(s"Cluster Center ${idx}: ${center}")
      }
      var cent = model.clusterCenters.map(x=>x.toArray).toList.zipWithIndex.map{case(v,i) => (v, i)}.toMap
      centroid = cent
      var group_num = model.predict(vec_count).collect().toList
      var TFIDF_list = TFIDF.collect().toList
      var w_group = new ListBuffer[(Int, Array[Double])]()
      for(i <- 0 until TFIDF_list.length){
        word_group_b.append((group_num(i),TFIDF_list(i)))
      }
    }

    var word_group = sc.parallelize(word_group_b.toList)
    var cent_map = centroid.map(x=>(x._2,x._1))
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
//    var wsse = group_error.map(x=>x._2._1).sum()
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
    if (method == "K"){alg = "K-means"}
    else{alg = "Bisecting K-Means" }

    val all = All(alg, wsse, clusters_)

    val json =
      (("algorithm" -> all.algorithm) ~
        ("WSSE" -> all.WSSE) ~
        ("clusters" ->
          all.cluster.map { c =>
            ("id" -> c.id) ~ ("size" -> c.size) ~ ("error" -> c.error) ~ ("terms" -> c.term)}))

    var out_path = ""
    if(method=="K"){
      out_path = "Xijia_Chen_Cluster_small_K_8_20.json"
    }else{
      out_path = "Xijia_Chen_Cluster_small_B_8_20.json"
    }
    var res = pretty(render(json))
    val writer = new PrintWriter(out_path)
    writer.write(res)
    writer.close()
  }

}
