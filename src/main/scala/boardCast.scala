import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author shkstart 
 * @create 2020-12-06 15:17 
 */
object boardCast {
  def main(args: Array[String]): Unit = {
    //创建 SparkConf对象, 并设置 App名字
    val conf = new SparkConf().setAppName("boardCast").setMaster("local[*]")
    // 2. 创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(
      List(("a",1),("b",2),("c",3))
    )

    val rdd2 = sc.makeRDD(
      List(("a",1),("b",2),("c",3))
    )

    val  list = List(("a",4),("b",5),("c",6),("d",7))

    val broadcast = sc.broadcast(list)

    val mapRdd = rdd1.map{
      case (k,v) =>
        var otherv = 0
        /*Breaks.breakable{

          for ((k1,v1) <- list){
            if (k1 ==k){
              otherv = v1
              Breaks.break()
            }

          }


        }*/


          for ((k1,v1) <- broadcast.value){
            if (k1 ==k){
              otherv = v1
            }

          }
        (k,(v,otherv))

    }

    mapRdd.foreach(println)

    /*val rdd2 = sc.makeRDD(
      List(("a",4),("b",5),("c",6))
    )

    val joinrdd = rdd1.join(rdd2)

    joinrdd.foreach(println)*/
    sc.stop()
  }
}

















