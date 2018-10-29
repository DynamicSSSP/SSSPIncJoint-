package inc

import org.apache.spark.graphx._

import scala.reflect.ClassTag


class SSSPCorrection(sourceVertex: VertexId) extends java.io.Serializable {

  type Vattr = (Boolean, Double, Long) //affected Nodes, Shortest Path, Parent

  private def mergeMsgs(path1: (VertexId, Double), path2: (VertexId, Double)): (VertexId, Double) = {
    if (path1._2 < path2._2) path1 else path2
  }

  def vertexProgram(id: VertexId, attr: Vattr, path: (VertexId,Double)): Vattr = {
    if (id == sourceVertex) (false, 0.0, sourceVertex)
    else {if (path._2 < attr._2) (false, path._2, path._1) else
    (false, attr._2, attr._3)}
  }


  def sendMessage(edge: EdgeTriplet[Vattr, Double]): Iterator[(VertexId, (VertexId,Double))] = {
    if (edge.srcAttr._2 + edge.attr < edge.dstAttr._2 )
      Iterator((edge.dstId, (edge.srcId, edge.srcAttr._2 + edge.attr)))
    else {
      Iterator.empty
    }
  }


 /* def sendMessageFirst(edge: EdgeTriplet[Vattr, Double]): Iterator[(VertexId, (VertexId,Double))] = {
    if (edge.srcAttr._1 && edge.srcAttr._2 + edge.attr < edge.dstAttr._2 )
      Iterator((edge.dstId, (edge.srcId, edge.srcAttr._2 + edge.attr)))
    else {
      Iterator.empty
    }
  }
*/

  def initVattr(gr: Graph[Int, Double]): Graph[Vattr, Double] = {
    val initVertexMsg = (false, Double.MaxValue, Long.MaxValue)
    val initVertexMsgSource =  (false, 0.0, sourceVertex)
    val setVertexAttr = (vid: VertexId, vdata: Int) => if (vid == sourceVertex) initVertexMsgSource else initVertexMsg
    gr.mapVertices(setVertexAttr)
  }

  private def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
                                                                          g: Graph[VD, ED],
                                                                          mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                                          reduceFunc: (A, A) => A)
  : VertexRDD[A] = {
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
    FileLogger.println("Number of active messages: " + activeMessages + ", time to compute:  " + ((initStop-initStart)/ 1000.0) )
    while (activeMessages > 0) {
      var itStart = System.currentTimeMillis();
      //val newg = g.joinVertices(messages)(vertexProgram)
      //newg.vertices.localCheckpoint()


      prevG = g
      g = g.joinVertices(messages)(vertexProgram)

//      val newv = g.vertices.innerJoin(messages)(vertexProgram)
//      g = g.outerJoinVertices(newv){ (vid, old, newOpt)=>newOpt.getOrElse(old)}
      //g.cache()

      //FileLogger.println("after vertices checkpoint")
      prevMessages = messages
      //1.6
      messages = mapReduceTriplets(g, sendMessage, mergeMsgs).cache()
      activeMessages = messages.count()
      //FileLogger.println(messages.toDebugString)
      //if (activeMessages < 5){
      //  messages.collect().foreach(v=>FileLogger.println(v._1 + " <- " + v._2._2 + " from " + v._2._1 ))
      //}
//      newv.unpersist(false)
      prevMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      prevG.unpersist(blocking = false)

      i += 1
      var itStop = System.currentTimeMillis();
      FileLogger.println("Number of active messages: " + activeMessages + ", time to compute:  " + ((itStop-itStart)/ 1000.0) )

    }
    return g
  }
}

