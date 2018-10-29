package inc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.Data


object SSSPBase {
  def loadCSV(sql:SQLContext, filepath: String, hasHeader: Boolean, inferSchema: Boolean) : DataFrame = {
    val df = sql.read
      .format("com.databricks.spark.csv")
      .option("header", hasHeader.toString) // Use first line of all files as header
      .option("inferSchema", inferSchema.toString)
      .load(filepath);
    df
  }

  def main(args: Array[String]): Unit = {
    val filename = args(0)
    val output = args(1)
    val log = args(2)
    val changesFile =args(3)
    //val iterStr = args(4)
    val srcStr = args(4)

    FileLogger.open(log)



    try {
      val start = System.currentTimeMillis()

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      val conf = new SparkConf().setAppName("ssspinc-loop-shortestpath")
      conf.set("spark.scheduler.mode", "FAIR")
      conf.set("spark.memory.fraction", "0.6")
      conf.set("spark.eventLog.enabled", "true")
      val home = sys.env("HOME")
      conf.set("spark.eventLog.dir", home + "/spark/logs")
      FileLogger.println(conf.getAll.mkString("\n"))
      val sc = new SparkContext(conf)
      FileLogger.println("AppId:" + sc.applicationId)
      sc.setLogLevel("ERROR")
      val sqlC = new SQLContext(sc)
      //val changes = FileHandler.loadInput(filename)
      //val uritype = metaFiles(0).uriType
      // val vFile = metaFiles(0).uri
      //val eFile = metaFiles(0).uri
      //FileLogger.println("Graph: " + eFile)

      //val metaFilesC = FileHandler.loadInput(modificationFile)
      //val changesDF = Data.loadTSV(sc, filename) //, false, false)




      FileLogger.println("graph: " + filename)
      FileLogger.println("changes: " + changesFile)
      println("graph: "+ filename)
      println("changes: " + changesFile)


      print("here ============================= ")
      val changesDF = Data.loadCSV_S(sc, changesFile)
      val fedges : RDD[Edge[(Double,Long)]] = changesDF.map(r=> Edge(r.apply(0).asInstanceOf[String].toLong, r.apply(1).asInstanceOf[String].toLong,
        (r.apply(2).asInstanceOf[String].toDouble, r.apply(3).asInstanceOf[String].toLong)))
      val iteration = 100
      //val sourceVertex = srcStr.toLong
      var currentTimestamp = 0L
      var baseTime = 0L
      var snapshotDuration = 1000000000L / 1 //10*365*24*3600L
      var startSnapshot = baseTime + 0 * snapshotDuration

      //val baseEdges = fedges.filter(e => e.attr._2 < startSnapshot && e.attr._1 > 0 && e.srcId != e.dstId)
      //var origGraph = Graph.fromEdges(baseEdges, defaultValue = 1).mapEdges(e=>1.0)
      var origGraph = GraphLoader.edgeListFile(sc, filename ).mapEdges(e=>1.0)
      var highdeg = origGraph.outDegrees.reduce((v1,v2)=>if (v1._2 >= v2._2) v1 else v2)
      val sourceVertex = highdeg._1
      FileLogger.println("Source: " + sourceVertex)
      FileLogger.println("High out deg node : " + highdeg._1 + " with degree " + highdeg._2)
      FileLogger.println("Number of vertices in orig graph :" +origGraph.vertices.count())
      FileLogger.println("Number of edges in orig graph :" +origGraph.edges.count())

      //var vertexRDD = sc.parallelize[(Long,(Boolean,Double, Long))](Array((sourceVertex,(false, 0.0, sourceVertex))))
      //var edgeRDD = sc.parallelize[(Long,Long)](Array((sourceVertex,sourceVertex))).map(e=>Edge(e._1,e._2, 1.0))
      //var sourceVertex = highdeg._1
      var found = true
      //var graph = Graph(vertexRDD, edgeRDD)
      val gmod = new SSSPCorrection(sourceVertex)
      val ginit = gmod.initVattr(origGraph)
      var graph = gmod.run(ginit, false, iteration)
      //FileLogger.println("Number of edges in sssp tree: " + ssspTree.edges.count())
      //fedges.take(10).foreach(e=>FileLogger.println(e.dstId, e.srcId, e.attr._1, e.attr._2))
      var numSnapshots = 0
      while (found) {
        val startRound =  System.currentTimeMillis();
        numSnapshots = numSnapshots + 1
        val snapshotEdges = fedges.filter(e => e.attr._2 >= startSnapshot && e.attr._2 < (startSnapshot + snapshotDuration) && e.srcId != e.dstId)
        val aEdges = snapshotEdges.filter(e => e.attr._1 > 0).map(e => Edge(e.srcId, e.dstId, 1.0)).cache()
        val dEdges = snapshotEdges.filter(e => e.attr._1 < 0).map(e => Edge(e.srcId, e.dstId, 1.0)).cache()

        val numAddedEdges = aEdges.count()
        val numDelEdges = dEdges.count()

        FileLogger.println("Snapshot: " + numSnapshots + " Duration: " +  startSnapshot + " to " + (startSnapshot+snapshotDuration) )
        FileLogger.println("Number of added edges:" + numAddedEdges )
        FileLogger.println("Number of deleted edges:" + numDelEdges )

        if (numAddedEdges + numDelEdges > 0) {
          found = true
        } else{
          found = false
        }

        if (found) {


          var firstRunStop = System.currentTimeMillis();
          val deleteSet = sc.broadcast(dEdges.collect().map(e => (e.srcId, e.dstId)).toSet)
          val remainingEdges = graph.edges.filter(e => if (deleteSet.value.contains((e.srcId, e.dstId))) false else true)



          //val allEdges = augmentedSSSPTree.edges.union(aEdges).distinct()

          val addSet = aEdges.map(e => (e.srcId, e.dstId)).collect().toSet
          val addSetB = sc.broadcast(addSet)
          val notDuplicate: RDD[Edge[Double]] = remainingEdges.filter { e => !addSetB.value.contains((e.srcId, e.dstId)) }
          val uEdges: RDD[Edge[Double]] = notDuplicate.union(aEdges)

          val graphRaw = Graph.fromEdges(uEdges, defaultValue = 1)
          //graphRaw.cache()
          val ginit = gmod.initVattr(graphRaw)
          ginit.cache()

          val prevGraph = graph
          graph = gmod.run(ginit, false, iteration)


          prevGraph.vertices.unpersist(false)
          prevGraph.unpersist(false)
          //ginit.unpersist(false)
          addSetB.unpersist(false)
          deleteSet.unpersist(false)
        }
        startSnapshot = startSnapshot + snapshotDuration

        val stopRound =  System.currentTimeMillis();
        FileLogger.println("Round done in " + ((stopRound - startRound) / 1000.0) + " sec" );
      }


      val distRDD = graph.vertices.map{ case (v,a)=> (v, if (a._2>1000.0) 0 else a._2) }
      val totalDistance = distRDD.map{case (vid,d)=>d}.reduce(_+_)
      FileLogger.println("Total Distance after changes = " + totalDistance)

      val stop = System.currentTimeMillis();
      FileLogger.println("SSSPseq successfully done in " + ((stop - start) / 1000.0) + " sec" );
    }
    catch {
      case e: Exception => FileLogger.println(e)
    } finally {
      FileLogger.close();
    }
  }
}
