package cse512

import org.apache.spark.sql.SparkSession
import scala.math.pow
import scala.math.sqrt

object SpatialQuery extends App{

      /*
       * ST_Contains : Check if the given point (x, y) is in the given rectangle
       * @params: queryRectangle:String   -   Diagonal point coordinates of the rectangle
       *          pointString:String      -   Query point coordinates
       * @return: Boolean True or False
       */
      def ST_Contains(queryRectangle:String, pointString:String): Boolean = {

          // If the inputs are empty, return false
          if (queryRectangle == null || queryRectangle.isEmpty() ||
                    pointString == null || pointString.isEmpty())
              return false

          val rectangleArr = queryRectangle.split(",").toList.map(e => e.toDouble)
          val pointArr = pointString.split(",").toList.map(e => e.toDouble)


          if (pointArr(0) >= rectangleArr(0) && pointArr(0) <= rectangleArr(2) &&
                    pointArr(1) >= rectangleArr(1) && pointArr(1) <= rectangleArr(3))
              return true
          else if (pointArr(0) >= rectangleArr(2) && pointArr(0) <= rectangleArr(0) &&
                    pointArr(1) >= rectangleArr(3) && pointArr(1) <= rectangleArr(1))
              return true
          else
              return false
      }


  def ST_Within(point1: String, point2: String, distance: Double): Boolean = {
    if (distance <= 0 || point1 == null || point2 == null) {
      return  false
    }
    val point1Arr = point1.split(",").toList.map(e => e.toDouble)
    val point2Arr = point2.split(",").toList.map(e => e.toDouble)
    val dist = euclideanDistance(point1Arr(0), point1Arr(1), point2Arr(0), point2Arr(1))
    return dist <= distance
  }

  def euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    return sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
                      ST_Contains(queryRectangle, pointString)
              })

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
                    ST_Contains(queryRectangle, pointString)
            })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
