package inc

import org.apache.spark.graphx._

import scala.reflect.ClassTag


class SSSPInvalidate() extends java.io.Serializable {

  type Vattr = (Boolean, Double, Long) //affected Nodes, Shortest Path, Old Shortest Path, Parent

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

  def sendMsg(edge: EdgeTriplet[Vattr, Double]): Iterator[(VertexId, Boolean)] = {
    if (edge.dstAttr._3 == edge.srcId) {

      //my child
      if (edge.srcAttr._1) {
        Iterator((edge.dstId,true))
      }else{
        Iterator.empty
      }
    } else{
      Iterator.empty
    }
  }
  private def mergeMsg( v1: Boolean, v2: Boolean): Boolean = {
    return v1 || v2
  }

  def vertexProgram(id: VertexId, attr: Vattr, msg:Boolean): Vattr = {
    if (msg) {
      (true, Double.MaxValue, Long.MaxValue)
    }else{
      (false, attr._2,  attr._3)
    }
  }


  def run(graph: Graph[Vattr, Double], dbg: Boolean = false, totalVer: Long): (Graph[Vattr, Double], Boolean) = {
    val initStart = System.currentTimeMillis();
    var g = graph

    var prevG: Graph[Vattr, Double] = null
    var i: Int = 0


    var messages = mapReduceTriplets(g, sendMsg, mergeMsg)
    var activeMessages = messages.count()
    var prevMessages = messages
    val initStop = System.currentTimeMillis();
    FileLogger.println("Number of active messages: " + activeMessages + ", time to compute:  " + ((initStop-initStart)/ 1000.0) )
    var totalMessage = 0L
    var terminate = false
    while (activeMessages > 0 && !terminate) {
      val itStart = System.currentTimeMillis();
      prevG = g
      prevMessages = messages


//      val newv = g.vertices.innerJoin(messages)(vertexProgram)
//      g = g.outerJoinVertices(newv){ (vid, old, newOpt)=>newOpt.getOrElse(old)}
//      g.cache()


      g = g.joinVertices(messages)(vertexProgram)


      messages = mapReduceTriplets(g, sendMsg, mergeMsg).cache()
      activeMessages = messages.count()
      totalMessage += activeMessages

      if (totalMessage > (totalVer / 100)) {
        terminate = true
      }

      prevMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)

      i += 1
      val itStop = System.currentTimeMillis();
      FileLogger.println("Number of active messages: " + activeMessages + ", time to compute:  " + ((itStop-itStart)/ 1000.0) )
    }
    return (g, terminate)
  }
}

