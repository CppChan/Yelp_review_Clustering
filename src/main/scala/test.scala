import scala.util.parsing.json.JSONObject
import scala.util.parsing.json.JSONArray
import org.json4s._
import org.json4s.jackson.JsonMethods._


object test {

  def main(args: Array[String]): Unit = {
//    def distance(vect:Array[Float], cent:Map[Array[Float], Int]): Int={
//      var res = -1
//      var mindif = 10000000.0
//      for(c<-cent){
//        var dif = 0.0f
//        var centroid = c._1
//        for(i <- 0 until centroid.size){
////          print(math.sqrt((centroid(i)-vect(i))*(centroid(i)-vect(i))).toFloat)
//          dif+= math.sqrt((centroid(i)-vect(i))*(centroid(i)-vect(i))).toFloat
//        }
//        if(dif<mindif) {
//          mindif = dif
//          res = c._2
//        }
//      }
//      res
//    }
//    var test = Array[Float](1.0f,2.0f)
//    val immutableMap = Map(Array(1.0f,0.5f) -> 0, Array(2.0f,0.2f) -> 1)
//    var res = distance(test, immutableMap)
//    print(res)

//    var map_arr = new Array[scala.collection.mutable.Map[Int, Double]]

//    val colors:Map[String,Object]  = Map("red" -> "123456",
//      "azure" -> "789789")
//
//    val random = new util.Random
//
//    val cypher = """CREATE ( me:User { user_id: {user_id} } )"""
//    val oneThousand = JSONArray.apply(
//      List.fill(1000)(
//        JSONObject.apply(
//          Map("statement" -> cypher,
//            "parameters" -> JSONObject.apply(
//              Map("user_id" -> random.nextInt())))
//        )
//      )
//    )
//    val statements = JSONObject.apply(Map("statements" -> oneThousand))
//
//
//    import java.io.PrintWriter
//    val out = new PrintWriter("myDictionary.json")
//    out.println(statements.toString())
//    out.close()
    var cent = Array[Int](4,2,6)
    var cent_index = cent.zipWithIndex.map{case(v,i) => (v, i)}
    cent_index.foreach(println)
    var top_2 = cent_index.sortBy(x=>x._1).slice(cent_index.length-2, cent_index.length)
    top_2.foreach(println)

  }

}
