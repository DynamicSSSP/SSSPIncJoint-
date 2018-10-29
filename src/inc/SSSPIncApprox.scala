package inc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.Data



object SSSPIncApprox {
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
      sc.setCheckpointDir("/tmp/spark-checkpoints")
      sc.setLogLevel("ERROR")
      FileLogger.println("AppId:" + sc.applicationId)

      val sqlC = new SQLContext(sc)
      //val graphFile = FileHandler.loadInput(filename)
      //val uritype = metaFiles(0).uriType
      // val vFile = metaFiles(0).uri
      //val eFile = metaFiles(0).uri
      //FileLogger.println("Graph: " + eFile)

      val debug = false

      //val metaFilesC = FileHandler.loadInput(modificationFile)
      //val changesDF = Data.loadTSV(sc, filename) //, false, false)




      FileLogger.println("graph: " + filename)
      FileLogger.println("changes: " + changesFile)
      println("graph: "+ filename)
      println("changes: " + changesFile)


      print("here ============================= ")
      val changesDF = Data.loadCSV_S(sc, changesFile)
      val fedges : RDD[Edge[(Double,Long)]] = changesDF.map(r=> Edge(r.apply(0).toLong, r.apply(1).toLong, (r.apply(2).toDouble, r.apply(3).toLong)))
      val iteration = 100
      //val sourceVertex = srcStr.toLong
      var currentTimestamp = 0L
      var baseTime = 0L
      var snapshotDuration = 1000000000L / 1 //10*365*24*3600L
      var startSnapshot = baseTime + 0 * snapshotDuration

      //val baseEdges = fedges.filter(e => e.attr._2 < startSnapshot && e.attr._1 > 0 && e.srcId != e.dstId)
      //var origGraph = Graph.fromEdges(baseEdges, defaultValue = 1).mapEdges(e=>1.0)
      val origGraph = GraphLoader.edgeListFile(sc, filename ).mapEdges(e=>1.0)
      //val origGraph = first/.partitionBy(partitionStrategy = PartitionStrategy.RandomVertexCut)
      //origGraph.edges.count()
      //origGraph.vertices.count()
      //first.unpersistVertices(false)
      //first.edges.unpersist(false)
      var highdeg = origGraph.outDegrees.reduce((v1,v2)=>if (v1._2 >= v2._2) v1 else v2)
      //val sourceVertex = 3
      val sourceVertex = highdeg._1
      FileLogger.println("Source: " + sourceVertex)
      //FileLogger.println("High out deg node : " + highdeg._1 + " with degree " + highdeg._2)
      FileLogger.println("Number of vertices in orig graph :" +origGraph.vertices.count())
      FileLogger.println("Number of edges in orig graph :" +origGraph.edges.count())

      //var vertexRDD = sc.parallelize[(Long,(Boolean,Double, Long))](Array((sourceVertex,(false, 0.0, sourceVertex))))
      //var edgeRDD = sc.parallelize[(Long,Long)](Array((sourceVertex,sourceVertex))).map(e=>Edge(e._1,e._2, 1.0))
      var found = true
      //var graph = Graph(vertexRDD, edgeRDD)
      val gmod = new GraphModEff(sourceVertex)
      val ginvalidate = new SSSPInvalidate()

      var graph = gmod.initVattr(origGraph)
      var ssspGraph = gmod.run(graph, false, iteration)
         // .cache()

      origGraph.unpersistVertices(false)
      origGraph.edges.unpersist(false)
      origGraph.unpersist(false)

      graph.unpersistVertices(false)
      graph.edges.unpersist(false)
      graph.unpersist(false)
      //var ssspTree = ssspGraph.subgraph(epred = et => et.srcId == et.dstAttr._3)
      //var edgeRDD = ssspGraph.edges.map(e=>Edge(e.srcId, e.dstId, e.attr))
      //FileLogger.println("Number of edges in sssp tree: " + ssspTree.edges.count())

      //var ginit = gmod.initVattr(graph)
      //fedges.take(10).foreach(e=>FileLogger.println(e.dstId, e.srcId, e.attr._1, e.attr._2))
      var numSnapshots = 0
      while (found) {
        val startRound =  System.currentTimeMillis();
        numSnapshots = numSnapshots + 1
        val snapshotEdges = fedges.filter(e => e.attr._2 >= startSnapshot && e.attr._2 < (startSnapshot + snapshotDuration) && e.dstId != e.srcId)
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
        var sigInvalidate = false

        var updatedVertices : VertexRDD[(Boolean, Double, Long)] = null
        var firstRunStop = System.currentTimeMillis();
        if (found) {
          val deleteSet = sc.broadcast(dEdges.collect().map(e => (e.srcId, e.dstId)).toSet)
//          if (numDelEdges == 0) {
//            updatedVertices = ssspGraph.vertices
//          }
//          else {


            var invalStart = System.currentTimeMillis();
//            val invalidVer = ssspGraph.triplets.filter(et => et.srcId == et.dstAttr._3 && deleteSet.value.contains(et.srcId, et.dstId))
//            //val treeEdges = ssspGraph.triplets.filter(et => et.srcId == et.dstAttr._3).map(et=>Edge(et.srcId, et.dstId, et.attr))
//            val invalidVerId = invalidVer.map(et => et.dstId).collect()
//            var invalStop = System.currentTimeMillis();
//
//            FileLogger.println("Number effective vertices for delete:" + invalidVerId.length)
//            FileLogger.println("Collecting affected vertices in " + (invalStop - invalStart) / 1000.0)
////            if (invalidVerId.length > 0) {
//              val invalidVerB = sc.broadcast(invalidVerId.toSet)
//              val nSSSPTree = ssspGraph.mapVertices { case (vid, vattr) =>
//                if (invalidVerB.value.contains(vid))
//                  (true, // invalidate,
//                    Double.MaxValue,//vattr._2, //shortest path
//                    Long.MaxValue// vattr._3 //parent
//                  )
//                else
//                  (false,
//                    vattr._2,
//                    vattr._3
//                  )
//              }
              //nSSSPTree.vertices.count()
              //val gnew = Graph.fromEdge(nSSSPTree.edges)



            ssspGraph = ssspGraph.mapVertices { case (vid, vattr) =>
              //val add = addAffectedB.value.contains(vid)
              val del = deleteSet.value.contains(vattr._3, vid)
              if (del) (true, Double.MaxValue, Long.MaxValue)
              else
                (false, vattr._2, vattr._3)
            }


            //ssspGraph.outerJoinVertices(invalidVer)
              val totalVeritces = 100000000L //ssspGraph.vertices.count()
              //val (ginval, terminate) = ginvalidate.run(nSSSPTree, false, totalVeritces)
              updatedVertices = ssspGraph.vertices
              //sigInvalidate = terminate
              //ginval.edges.unpersist(false)

              //nSSSPTree.unpersistVertices(false)
//              nSSSPTree.edges.unpersist(false)
              //nSSSPTree.unpersist(false)
            ssspGraph.unpersistVertices(false)
            ssspGraph.edges.unpersist(false)
            ssspGraph.unpersist(false)

              var deleteStop = System.currentTimeMillis();
              FileLogger.println("Delete time: " + ((deleteStop - firstRunStop) / 1000.0))
//            } else {
//              updatedVertices = ssspGraph.vertices
//            }
//          }
            val startAugment = System.currentTimeMillis();
            FileLogger.println("Running SSSP ...")


//            val deleteStop = System.currentTimeMillis();

            val addSet = aEdges.map(e => (e.srcId, e.dstId)).collect().toSet
            val addSetB = sc.broadcast(addSet)
            val remainingEdges = ssspGraph.edges.filter(e => if (deleteSet.value.contains((e.srcId, e.dstId))) false else true)
            val notDuplicate: RDD[Edge[Double]] = remainingEdges.filter { e => !addSetB.value.contains((e.srcId, e.dstId)) }
            val uEdges: RDD[Edge[Double]] = notDuplicate.union(aEdges)

            //uEdges.count() // to remove
            remainingEdges.unpersist(false)
            notDuplicate.unpersist(false)

            //1.4
            val graphRaw = Graph.fromEdges(uEdges, defaultValue = 1)

            val ngraph = graphRaw.outerJoinVertices(updatedVertices) { case (vid, value, o) => o.getOrElse((false, Double.MaxValue, Long.MaxValue)) }
            ngraph.cache()

            updatedVertices.unpersist(false)
            //ngraph.cache()
            //ngraph.vertices.count()
            val ssspGraphPrev = ssspGraph
            ssspGraph = gmod.run(ngraph).cache()

            uEdges.unpersist(false)
            ssspGraphPrev.unpersistVertices(false)
            ssspGraphPrev.edges.unpersist(false)
            ssspGraphPrev.unpersist(false)

            graphRaw.unpersistVertices(false)
            graphRaw.edges.unpersist(false)
            graphRaw.unpersist(false)
            ngraph.unpersistVertices(false)
            ngraph.edges.unpersist(false)
            ngraph.unpersist(false)
            //ssspTree = ssspGraph.subgraph(epred = et => et.srcId == et.dstAttr._3)
            //invalidVerB.unpersist(false)
            //invalidateVerIdB.unpersist(false)
            addSetB.unpersist(false)
            //            a_srcVerticesB.unpersist(false)
            deleteSet.unpersist(false)

            val addStop = System.currentTimeMillis()
            FileLogger.println("Add time: " + (addStop - deleteStop)/1000.0 )

        }


        startSnapshot = startSnapshot + snapshotDuration
        val stopRound =  System.currentTimeMillis();
        FileLogger.println("Round done in " + ((stopRound - startRound) / 1000.0) + " sec" );
        //ssspGraph.vertices.localCheckpoint()
        //ssspGraph.edges.localCheckpoint()
      }
      val distRDD = ssspGraph.vertices.map{ case (v,a)=> (v, if (a._2>1000.0) 0 else a._2) }
      val totalDistance = distRDD.map{case (vid,d)=>d}.reduce(_+_)
      FileLogger.println("Total Distance after changes = " + totalDistance)

      val stop = System.currentTimeMillis();
      FileLogger.println("SSSPApprox successfully done in " + ((stop - start) / 1000.0) + " sec" );
    }
    catch {
      case e: Exception => FileLogger.println(e)
    } finally {
      FileLogger.close();
    }
  }
}
