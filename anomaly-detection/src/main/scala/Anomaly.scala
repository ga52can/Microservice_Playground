import scala.collection.mutable.ListBuffer
import scala.tools.ant.sabbus.Break

class Anomaly(val spanId: String, val TraceId: String, val begin: Long, val end: Long, val endpointIdentifier: String, val errorDescriptor: String, val parentId: String) {

  var children: ListBuffer[Anomaly] = ListBuffer()
  var parent: Anomaly = null

  /**
   * Assumes the anomalies are inserted into the anomaly of a trace with
   * the smalles begin sorted by begin in ascending order
   */
  def insert(anomaly: Anomaly): Unit = {
    var inserted = false
    for (child <- children) {
      if (child.parentId==this.spanId||(anomaly.begin > child.begin && anomaly.end <= child.end)||anomaly.begin >= child.begin && anomaly.end < child.end) {//TODO: Sortierung bei http -> mvc - leicht versetzte Zeitfenster aber sinngemäßg http ruf mvc auf - durch manipulation der ID (z.B. x.toString+"mvc") und entsprechend bei insert behandeln
        if (!inserted) {
        child.insert(anomaly)
        inserted = true}
      }
    }
    if (!inserted) {
      children.append(anomaly)
      anomaly.parent = this
//      println("Inserted " + anomaly.endpointIdentifier+" into "+this.endpointIdentifier)
      
    }
  }

  def hasChildren(): Boolean = {
    !(children.size == 0)
  }

  def getLeaves(): ListBuffer[Anomaly] = {
//    println("Calling getLeaves for:" +this.endpointIdentifier)
    var collectedLeaves: ListBuffer[Anomaly] = ListBuffer()
    if (hasChildren()) {
      for (child <- children) {
        for (anomaly <- child.getLeaves()) {
          collectedLeaves.append(anomaly)
        }
      }
    } else {
      collectedLeaves.append(this)
//      println(this.endpointIdentifier+" is a leave")
    }

    collectedLeaves

  }
  
  def getCalling(): ListBuffer[Anomaly] = {
    val callingBuffer: ListBuffer[Anomaly] = ListBuffer()
    if(parent!=null){
      callingBuffer.append(parent)
      for(calling <- parent.getCalling()){
        callingBuffer.append(calling)
      }
    }
    
    callingBuffer
    
  }
  
  def printLeaves():String = {
    var leaves = "["
    
    
    for(leave <- getLeaves()){
      if(!(leaves.last == '[')){
      leaves = leaves+", "
    }
      leaves = leaves + leave.endpointIdentifier
    }
    
    leaves = leaves + "]"
    leaves
  }
  
    def printCalling():String = {
    var callingSpans = "["
    
    
    for(calling <- getCalling().reverse){
      if(!(callingSpans.last == '[')){
      callingSpans = callingSpans+", "
    }
      callingSpans = callingSpans +calling.endpointIdentifier
    }
    
    callingSpans = callingSpans + "]"
    callingSpans
  }
  
    
    def printCallingSQL():String = {
    var callingSpans = "["
    
    
    for(calling <- getCalling().reverse){
      if(!(callingSpans.last == '[')){
      callingSpans = callingSpans+", "
    }
      callingSpans = callingSpans + "\""+calling.endpointIdentifier+"\""
    }
    
    callingSpans = callingSpans + "]"
    callingSpans
  }


}