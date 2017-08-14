import scala.collection.mutable.ListBuffer


object AnomalyTest {
  def main(args: Array[String]) {
    //    val anomaly1 = new Anomaly("-4861775942820964367", "-4861775942820964367", 1502201991593L, 1502201991734L, "zuul-service@ignoredIP:9000/travelcompanion-mobility-service/getroutes:::null", "splittedKMeans")
    //    val anomaly2 = new Anomaly("-13245866539313486", "4861775942820964367", 1502201991594L, 1502201991731L, "zuul-service@ignoredIP:9000/travelcompanion-mobility-service/getroutes:::POST", "splittedKMEans")
    //    val anomaly3 = new Anomaly("5200877534396065616", "-4861775942820964367", 1502201991597L, 1502201991731L, "travelcompanion-mobility-service@ignoredIP:6002/travelcompanion-mobility-service/getroutes:::TravelCompanionControllergetRoutes", "splittedKMeans")
    //    val anomaly4 = new Anomaly("5051187549457163288", "-4861775942820964367", 1502201991600L, 1502201991712L, "maps-helper-service@ignoredIP:7000/distance:::MapsControllerfindDistance", "splittedKMeans")

    //    val anomaly5 = new Anomaly("-7316700837415977788", "-7316700837415977788",1502201799255L, 1502201800646L, "zuul-service@ignoredIP:9000/business-core-service/businesses/list:::null", "splittedKMeans")
    //    val anomaly6 = new Anomaly("4273401834980503605", "-7316700837415977788", 1502201799280L, 1502201800638L, "zuul-service@ignoredIP:9000/business-core-service/businesses/list:::GET", "splittedKMEans")
    //    val anomaly7 = new Anomaly("-6242790120112369912", "-7316700837415977788",1502201800164L, 1502201800626L, "business-core-service@ignoredIP:5000/business-core-service/businesses/list:::BusinessListControllergetServices", "splittedKMeans")
    //    

    //    {"endpointIdentifier":"business-core-service@ignoredIP:5000/business-core-service/businesses/list:::BusinessListControllergetServices", "spanId":"-6242790120112369912", "traceId":"-7316700837415977788", "anomalyDescriptor":"splittedKMeans", "begin":"1502201800164", "end":"1502201800626"}
    //    {"endpointIdentifier":"zuul-service@ignoredIP:9000/business-core-service/businesses/list:::GET", "spanId":"4273401834980503605", "traceId":"-7316700837415977788", "anomalyDescriptor":"splittedKMeans", "begin":"1502201799280", "end":"1502201800638"}
    //    {"endpointIdentifier":"zuul-service@ignoredIP:9000/business-core-service/businesses/list:::null", "spanId":"-7316700837415977788", "traceId":"-7316700837415977788", "anomalyDescriptor":"splittedKMeans", "begin":"1502201799255", "end":"1502201800646"}

    //{"endpointIdentifier":"travelcompanion-mobility-service@ignoredIP:6002/getroutes:::GET", "spanId":"-5618816118468433521", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201802489", "end":"1502201802949"} - cs:1502201802489 cr:1502201802949 | sr:1502201802680 ss:1502201802898
    //{"endpointIdentifier":"travelcompanion-mobility-service@ignoredIP:6002/getroutes:::GET", "spanId":"-7540391266273840727", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201802952", "end":"1502201803396"} - cs:1502201802952 cr:1502201803396 | sr:1502201803161 ss:1502201803161
    //{"endpointIdentifier":"travelcompanion-mobility-service@ignoredIP:6002/travelcompanion-mobility-service/getroutes:::TravelCompanionControllergetRoutes", "spanId":"-4800790700074471792", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201801304", "end":"1502201803436"}
    //{"endpointIdentifier":"deutschebahn-mobility-service@ignoredIP:6003/getroutes:::DeutscheBahnControllerfindDistance", "spanId":"4327059873365883944", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":1502201803157, "end":"1502201803388"}
    //{"endpointIdentifier":"drivenow-mobility-service@ignoredIP:6001/getroutes:::DriveNowControllerfindDistance", "spanId":"7440824012979721880", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201802677", "end":"1502201802914"}
    //{"endpointIdentifier":"maps-helper-service@ignoredIP:7000/distance:::MapsControllerfindDistance", "spanId":"-2145659704869729536", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201802107", "end":"1502201802484"}
    //{"endpointIdentifier":"travelcompanion-mobility-service@ignoredIP:6002/distance:::GET", "spanId":"-75932635999707936", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201801639", "end":"1502201802476"} - sr:1502201802112 ss:1502201802465 | cs: 1502201801648 cr: 1502201802476
    //{"endpointIdentifier":"zuul-service@ignoredIP:9000/travelcompanion-mobility-service/getroutes:::null", "spanId":"-1791998925882270103", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201800666", "end":"1502201803444"} - sr:1502201800666 ss:1502201803443
    //{"endpointIdentifier":"zuul-service@ignoredIP:9000/travelcompanion-mobility-service/getroutes:::POST", "spanId":"7595257767668260794", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201800669", "end":"1502201803425"} - sr:1502201801306 ss:1502201803420 |  cs: 1502201800669 cs: 1502201800758 cr: 1502201803425 

    //{"endpointIdentifier":"travelcompanion-mobility-GET", "spanId":"-5618816118468433521", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201802489", "end":"1502201802949"}
    //{"endpointIdentifier":"travelcompanion-mobility-GET", "spanId":"-7540391266273840727", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201802952", "end":"1502201803396"}
    //{"endpointIdentifier":"travelcompanion-mobility-TravelCompanionControllergetRoutes", "spanId":"-4800790700074471792", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201801304", "end":"1502201803436"}
    //{"endpointIdentifier":"deutschebahn-mobility-DeutscheBahnControllerfindDistance", "spanId":"4327059873365883944", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201803157", "end":"1502201803388"}
    //{"endpointIdentifier":"drivenow-mobility-DriveNowControllerfindDistance", "spanId":"7440824012979721880", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201802677", "end":"1502201802914"}
    //{"endpointIdentifier":"maps-helper-MapsControllerfindDistance", "spanId":"-2145659704869729536", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201802107", "end":"1502201802484"}
    //{"endpointIdentifier":"travelcompanion-mobility-GET", "spanId":"-75932635999707936", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201801639", "end":"1502201802476"}
    //{"endpointIdentifier":"zuul-null", "spanId":"-1791998925882270103", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201800666", "end":"1502201803444"}
    //{"endpointIdentifier":"zuul-POST", "spanId":"7595257767668260794", "traceId":"-1791998925882270103", "anomalyDescriptor":"splittedKMeans", "begin":"1502201800669", "end":"1502201803425"}

//    val a1 = new Anomaly("-5618816118468433521", "-1791998925882270103", 1502201802489L, 1502201802949L, "travelcompanion-mobility-GET1", "splittedKMeans")
//    val a2 = new Anomaly("-7540391266273840727", "-1791998925882270103", 1502201802952L, 1502201803396L, "travelcompanion-mobility-GET2", "splittedKMeans")
//    val a3 = new Anomaly("-4800790700074471792", "-1791998925882270103", 1502201801304L, 1502201803436L, "travelcompanion-mobility-TravelCompanionControllergetRoutes", "splittedKMeans")
//    val a4 = new Anomaly("4327059873365883944", "-1791998925882270103",  1502201803157L, 1502201803388L, "deutschebahn-mobility-DeutscheBahnControllerfindDistance", "splittedKMeans")
//    val a5 = new Anomaly("7440824012979721880", "-1791998925882270103",  1502201802677L, 1502201802914L, "drivenow-mobility-DriveNowControllerfindDistance", "splittedKMeans")
//    val a6 = new Anomaly("-2145659704869729536", "-1791998925882270103", 1502201802107L, 1502201802484L, "maps-helper-MapsControllerfindDistance", "splittedKMeans")
//    val a7 = new Anomaly("-75932635999707936", "-1791998925882270103",   1502201801639L, 1502201802476L, "travelcompanion-mobility-GET3", "splittedKMeans")
//    val a8 = new Anomaly("-1791998925882270103", "-1791998925882270103", 1502201800666L, 1502201803444L, "zuul-null", "splittedKMeans")
//    val a9 = new Anomaly("7595257767668260794", "-1791998925882270103",  1502201800669L, 1502201803425L, "zuul-POST", "splittedKMeans")
//
//    
//    
//    
//    var anomalyList: ListBuffer[Anomaly] = new ListBuffer()
//    anomalyList.append(a1)
//    anomalyList.append(a2)
//    anomalyList.append(a3)
//    anomalyList.append(a4)
//    anomalyList.append(a5)
//    anomalyList.append(a6)
//    anomalyList.append(a7)
//    anomalyList.append(a8)
//    anomalyList.append(a9)
//    
//    
//    anomalyList = anomalyList.sortWith((x,y) => (x.begin<y.begin||(x.begin==y.begin&&x.end>y.end)))
//    
//    for(a<-anomalyList){
//      println((a.endpointIdentifier, a.begin, a.end))
//    }
//    
//    var anomaly: Anomaly = null
//      for(a <- anomalyList){
//        if(anomaly==null){
//          anomaly = a
//        }else{
//          anomaly.insert(a)
//        }
//      }
//      anomaly
//      
//          println("---Leave Identifier---")
//    println(anomaly.printLeaves())
//
//    println("---Leave: [Calling*]---")
//    for (leave <- anomaly.getLeaves()){
//      println(leave.endpointIdentifier + ": " + leave.printCalling())}
//    
//    val anomaly5 = new Anomaly("-7316700837415977788", "-7316700837415977788", 1502201799255L, 1502201800646L, "zuul-null", "splittedKMeans")
//    val anomaly6 = new Anomaly("4273401834980503605", "-7316700837415977788", 1502201799280L, 1502201800638L, "zuul-GET", "splittedKMEans")
//    val anomaly7 = new Anomaly("-6242790120112369912", "-7316700837415977788", 1502201800164L, 1502201800626L, "business-core-BusinessListControllergetServices", "splittedKMeans")
//
//    val anomaly1 = new Anomaly("-4861775942820964367", "-4861775942820964367", 1502201991593L, 1502201991734L, "zuul-null", "splittedKMeans")
//    val anomaly2 = new Anomaly("-13245866539313486", "-4861775942820964367", 1502201991594L, 1502201991731L, "zuul-POST", "splittedKMEans")
//    val anomaly3 = new Anomaly("5200877534396065616", "-4861775942820964367", 1502201991597L, 1502201991731L, "travelcompanion-TravelCompanionControllergetRoutes", "splittedKMeans")
//    val anomaly4 = new Anomaly("5051187549457163288", "-4861775942820964367", 1502201991600L, 1502201991712L, "maps-helper-service-MapsControllerfindDistance", "splittedKMeans")
//
//    val anomalies = Array(anomaly6, anomaly7)
//    for (anomaly <- anomalies) {
//      anomaly5.insert(anomaly)
//    }
//
//    println("---Leave Identifier---")
//    println(anomaly5.printLeaves())
//
//    println("---Leave: [Calling*]---")
//    for (leave <- anomaly5.getLeaves()){
//      println(leave.endpointIdentifier + ": " + leave.printCalling())}
  }
}