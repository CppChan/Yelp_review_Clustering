import java.util.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Task1_kv {
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
//    def comp_avg(member:List[(Int, Array[Float])], group_size: Int, dim: Int): Array[Float]={
//      var mem = member.map(x=>x._2)//((001),(110)....)
//      var sum = new Array[Float](dim)
//      for(m<-mem){
//        for(i <- 0 until m.size){
//          sum(i)+=m(i)
//        }
//      }
//      for(s <- 0 until sum.size){
//        sum(s) = (sum(s).toFloat)/(group_size.toFloat)
//      }
//      sum
//    }
    def comp_avg(member:List[(Int, scala.collection.mutable.Map[Int, Double])], group_size: Int, dim: Int): scala.collection.mutable.Map[Int, Double]={
      var new_cent: scala.collection.mutable.Map[Int, Double] = scala.collection.mutable.Map()
      var mem = member.map(x=>x._2)
      for(i<- 0 until dim){
        var dim_sum = 0.0
        for (m<-mem){
          if (m.contains(i)){dim_sum+=m(i)}
        }
        if(dim_sum>0.0){
          new_cent += (i->(dim_sum/group_size))
        }
      }
      new_cent
    }


    //    def distance(vect:Array[Float], cent:Map[Array[Float], Int]): Int={
//      var res = -1
//      var mindif = 10000000.0
//      for(c<-cent){
//        var dif = 0.0f
//        var centroid = c._1
//        for(i <- 0 until centroid.size){
//          //          print(math.sqrt((centroid(i)-vect(i))*(centroid(i)-vect(i))).toFloat)
//          dif+= math.sqrt((centroid(i)-vect(i))*(centroid(i)-vect(i))).toFloat
//        }
//        if(dif<mindif) {
//          mindif = dif
//          res = c._2
//        }
//      }
//      res
//    }

    def e_dist(vect:scala.collection.mutable.Map[Int, Double], cent:scala.collection.mutable.Map[Int, Double]): Double={
      var dif = 0.0
      for(k<-vect.keySet){
        if(cent.contains(k)){
          dif+= (cent(k)-vect(k))*(cent(k)-vect(k))
          cent.remove(k)
        }else{
          dif+=vect(k)*vect(k)
        }
      }
      for(k<-cent.keySet){
        dif+=cent(k)*cent(k)
      }
      dif
    }
      def find_nearest(vect:scala.collection.mutable.Map[Int, Double], cent:Array[(scala.collection.mutable.Map[Int, Double],Int)]): Int={
        var res = -1
        var mindif = 100000000000.0
        for(c<-cent){
          var centroid = c._1//Map[Int, Int]
          var dif = e_dist(vect, centroid)
          if(dif<mindif) {
            mindif = dif
            res = c._2
          }
        }
        res
      }

//    def kmeans(vect: List[scala.collection.mutable.Map[Int, Int]], cent:Map[Array[Float], Int], dim:Int): Map[Int,Array[Float]]={
//      var vec_group = vect.map(vec=>{
//        var nearest = distance(vec,cent)
//        (nearest,vec)
//      })
//      var group = vec_group.groupBy(x=>x._1)
//      var new_group_cent = group.map(x=>{
//        var group_num = x._1
//        var member = x._2 // ((001),(110)....)
//        var group_size = member.size
//        var new_cent = comp_avg(member, group_size, dim)
//        (group_num, new_cent)
//      })
//      new_group_cent
//    }
    def kmeans(vect: List[scala.collection.mutable.Map[Int, Double]], cent:Array[(scala.collection.mutable.Map[Int, Double],Int)], dim:Int): Array[(Int, scala.collection.mutable.Map[Int, Double])]={
      var vec_group = vect.map(vec=>{
        var nearest = find_nearest(vec,cent)
        (nearest,vec)
      })
      var group = vec_group.groupBy(x=>x._1)
      var new_group_cent = group.map(x=>{
        var group_num = x._1
        var member = x._2 // ((group_num, 001),(group_num, 110)....)
        var group_size = member.size
        var new_cent = comp_avg(member, group_size, dim)
        (group_num, new_cent)
      })
      new_group_cent.toArray
    }
    def word2count(review:Array[String], word_map:Map[String, Int], Size:Int): scala.collection.mutable.Map[Int, Double] ={
      var count: scala.collection.mutable.Map[Int, Double] = scala.collection.mutable.Map()
      for(word <- review){
        var word_index = word_map(word)
        if(!count.contains(word_index)){count += (word_index->1.0) }
        else{count(word_index)=count(word_index)+1.0}
      }
      count
    }

    var word = raw.map(row => row.split(" "))
    var ret = Set.empty[String]
    var allword = word.flatMap(x=>x).collect().toSet
    var allword_size = allword.size
    var word_map = allword.zipWithIndex.map{case(v,i) => (v, i)}.toMap
    var word_count = word.map(review=>{word2count(review, word_map, allword.size)})// ((0,0,0,1,0,0,2),(0,0,0,1,0,0,2),(0,0,0,1,0,0,2))
//    word_count.foreach(println)
    print(allword_size)

    var all_word = word.flatMap(x=>x).collect()
    var all_word_count = word2count(all_word, word_map, allword.size)

    //kmeans
    var cent = word_count.takeSample(withReplacement=false, num = N).zipWithIndex.map{case(v,i) => (v, i)} //(((001310),0),....)
//    word_count.foreach(println)
//    word_count = word_count.collect().toList
//    word_count.foreach(println)
    for(i <- 0 until iterations){
      var new_cent = kmeans(word_count.collect().toList, cent, allword_size)
      cent = new_cent.map(x=>(x._2, x._1))
    }
    cent.foreach(println)
    val immutableMap = Map(Array(1.0,0.5) -> 0, Array(2.0,0.2) -> 1)

  }
}
