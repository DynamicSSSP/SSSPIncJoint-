package inc

import org.apache.spark.graphx._

import scala.reflect.ClassTag


class JointPropagate(sourceVertex: VertexId) extends java.io.Serializable {

  type Vattr = (Boolean, Double, Long) //affected Nodes, Shortest Path, Parent

  private def mergeMsgs(path1: (VertexId, Double, Boolean), path2: (VertexId, Double,  Boolean)): (VertexId, Double, Boolean) = {
//    val addAffected = path1._3 || path2._3
//    val count = path1._3 + path2._3
    val delAffected = path1._3 || path2._3
    if (path1._2 < path2._2) (path1._1, path1._2, delAffected) else (path2._1, path2._2, delAffected)
  }

  def vertexProgram(id: VertexId, attr: Vattr, path: (VertexId, Double, Boolean)): Vattr = {
//    val msgAddAffected = path._3
    val msgDelAffected = path._3
//    val verAddAffected = attr._1._1
    val verDelAffected = attr._1
    val parent = path._1
    val distance = path._2

    if (id == sourceVertex) (false, 0.0, sourceVertex)
//    else {
//      if (verDelAffected||verAddAffected) ((msgAddAffected, if (verDelAffected) false else msgDelAffected), distance, parent)
//      else ((msgAddAffected, msgDelAffected), attr._2, attr._3)
//    }
    else {
      if (msgDelAffected){
        (true, Double.MaxValue, Long.MaxValue)
      } else if (attr._2 > path._2) {
        (false, path._2, path._1)
      } else {
        (false, attr._2, attr._3)
      }
    }

  }


  def sendMessage(edge: EdgeTriplet[Vattr, Double]): Iterator[(VertexId, (VertexId,Double, Boolean))] = {
    val dstDelAffected = edge.dstAttr._1
//    val dstAddAffected = edge.dstAttr._1._1

    val srcDelAffected = edge.srcAttr._1
//    val srcAddAffected = edge.srcAttr._1._1

    val myChild = edge.dstAttr._3 == edge.srcId
    val myParent = edge.srcAttr._3 == edge.dstId

    if (srcDelAffected) {
      if (dstDelAffected) {
        Iterator.empty
      } else if (myChild) {
        Iterator((edge.dstId, (edge.srcId, Double.MaxValue, true)))
      } else {
        Iterator.empty
      }

    } else if (dstDelAffected){
      if (!myParent){
        Iterator((edge.dstId, (edge.srcId, edge.srcAttr._2 + edge.attr, false)))}
      else {
        Iterator.empty
      }
    }

    else if (edge.srcAttr._2 + edge.attr < edge.dstAttr._2) {
      Iterator((edge.dstId, (edge.srcId, edge.srcAttr._2 + edge.attr, false)))
    }
    else {
      Iterator.empty
    }


  }

  private def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
          g: Graph[VD, ED],
          mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
          reduceFunc: (A, A) => A) : VertexRDD[A] = {
    def sendMsg(ctx: EdgeContext[VD, ED, A]) {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }
    g.aggregateMessages(sendMsg, reduceFunc, TripletFields.All)
  }



  def run(graph: Graph[Vattr, Double], dbg: Boolean = false, maxIterations: Int = 10) : Graph[Vattr, Double] = {
    val initStart = System.currentTimeMillis();
    var g = graph
    var prevG: Graph[Vattr, Double] = null
    var i: Int = 0
    //1.4
    var messages = mapReduceTriplets(g, sendMessage, mergeMsgs).cache()
    var activeMessages = messages.count()
    var prevMessages = messages
    val initStop = System.currentTimeMillis();
    //var updateCount = g.vertices.filter{case (vid, vattr)=>vattr._1}.count()

    var updateCount = 1L
    FileLogger.println("Number of active messages: " + activeMessages + ", time to compute:  " + ((initStop-initStart)/ 1000.0) )
   // FileLogger.println(s"Number of udpated vertices: $updateCount ")
    while (activeMessages > 0) {
      var itStart = System.currentTimeMillis();
      //val newg = g.joinVertices(messages)(vertexProgram)
      //newg.vertices.localCheckpoint()


      prevG = g
//      val newv = g.vertices.innerJoin(messages)(vertexProgram)
//      g = g.outerJoinVertices(newv){ (vid, old, newOpt)=>newOpt.getOrElse(old)}

      g = g.joinVertices(messages)(vertexProgram)

      //g.vertices.count()
      //g.cache()

      //FileLogger.println("after vertices checkpoint")
      prevMessages = messages
      //1.6


      messages = mapReduceTriplets(g, sendMessage, mergeMsgs).cache()
      activeMessages = messages.count()

//      val allmessages = g.vertices.map{case (vid, vattr)=>vattr._1._1}.reduce(_+_)

      //FileLogger.println(messages.toDebugString)
      //if (activeMessages < 5){
      //  messages.collect().foreach(v=>FileLogger.println(v._1 + " <- " + v._2._2 + " from " + v._2._1 ))
      //}
      //newv.unpersist(false)
      prevMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      prevG.unpersist(blocking = false)

      i += 1
      var itStop = System.currentTimeMillis();
      FileLogger.println("Number of active messages: " + activeMessages + ", time to compute:  " + ((itStop-itStart)/ 1000.0))
      //FileLogger.println(s"Number of udpated vertices: $updateCount ")

    }
    return g
  }
}

