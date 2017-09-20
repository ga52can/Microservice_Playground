import scala.collection.mutable.ListBuffer
import scala.tools.ant.sabbus.Break

class Anomaly(val spanId: String, val traceId: String, val begin: Long, val end: Long, val endpointIdentifier: String, val errorDescriptor: String, val parentId: String) {

  var children: ListBuffer[Anomaly] = ListBuffer()
  var parent: Anomaly = null

  /**
   * Assumes the anomalies are inserted into the anomaly of a trace with
   * the earliest begin sorted by begin in ascending order
   */
  def insert(anomaly: Anomaly): Unit = {
    var inserted = false
    for (child <- children) {
      if (anomaly.parentId==this.spanId 
          ||(anomaly.begin > child.begin && anomaly.end <= child.end)
          ||anomaly.begin >= child.begin && anomaly.end < child.end) {
        if (!inserted) {
        child.insert(anomaly)
        inserted = true}
      }
    }
    if (!inserted) {
      children.append(anomaly)
      anomaly.parent = this
    }
  }

  def hasChildren(): Boolean = {
    !(children.size == 0)
  }

  def getRootCauses(): ListBuffer[Anomaly] = {
    //TODO: Traces of uncaught exceptions work the other way round
//    println("Calling getLeaves for:" +this.endpointIdentifier)
    var collectedRootCauses: ListBuffer[Anomaly] = ListBuffer()
    if (hasChildren()) {
      for (child <- children) {
        for (anomaly <- child.getRootCauses()) {
          collectedRootCauses.append(anomaly)
        }
      }
    } else {
      collectedRootCauses.append(this)
//      println(this.endpointIdentifier+" is a leave")
    }

    collectedRootCauses

  }
  
  def getWarnings(): ListBuffer[Anomaly] = {
    //TODO: Traces of uncaught exceptions work the other way round
    val warningBuffer: ListBuffer[Anomaly] = ListBuffer()
    if(parent!=null){
      warningBuffer.append(parent)
      for(warning <- parent.getWarnings()){
        warningBuffer.append(warning)
      }
    }
    
    warningBuffer
    
  }
  
  def printRootCauses():String = {
    var rootCauses = "["
    
    
    for(leave <- getRootCauses()){
      if(!(rootCauses.last == '[')){
      rootCauses = rootCauses+", "
    }
      rootCauses = rootCauses + leave.endpointIdentifier
    }
    
    rootCauses = rootCauses + "]"
    rootCauses
  }
  
    def printWarnings():String = {
    
      
      var warningSpans = "["
    
    
    for(calling <- getWarnings().reverse){
      if(!(warningSpans.last == '[')){
      warningSpans = warningSpans+", "
    }
      warningSpans = warningSpans +calling.endpointIdentifier
    }
    
    warningSpans = warningSpans + "]"
    warningSpans
  }
  
    
    def printWarningSQL():String = {
    var warningSpans = "["
    
    
    for(calling <- getWarnings().reverse){
      if(!(warningSpans.last == '[')){
      warningSpans = warningSpans+", "
    }
      warningSpans = warningSpans + "\""+calling.endpointIdentifier+"\""
    }
    
    warningSpans = warningSpans + "]"
    warningSpans
  }


}