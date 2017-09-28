var express = require('express'); // Web Framework
var serveStatic = require('serve-static');
var app = express();
var mysql = require('mysql');
var path = require('path');


var qs = require('querystring');

 var con=mysql.createPool({

host:'localhost',
 user:'root',
 password:'root',
 database:'anomalies'

});

app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});



// Start server and listen on http://localhost:8081/
var server = app.listen(8081, function () {
    var host = server.address().address
    var port = server.address().port

    console.log("app listening at http://%s:%s", host, port)
});

app.get('/anomalies', function (req, res) {
    con.query('SELECT * FROM anomalies.anomalies WHERE timestamp > NOW() - INTERVAL 60 MINUTE', function(err,rows){

if(err)
  {
  res.json(err);
  }
  else{
  res.json(rows);
  }
  });
 })

 app.get('/anomalies/:timeWindowMinutes/', function (req, res) {
     con.query('SELECT * FROM anomalies.anomalies WHERE timestamp > NOW() - INTERVAL '+req.params.timeWindowMinutes+' MINUTE', function(err,rows){

 if(err)
   {
   res.json(err);
   }
   else{
   res.json(rows);
   }
   });
  })

app.post("/reportAnomaly", function (req, res) {


	if (req.method == 'POST') {
        var body = '';

        req.on('data', function (data) {
            body += data;

            // Too much POST data, kill the connection!
            // 1e6 === 1 * Math.pow(10, 6) === 1 * 1000000 ~~~ 1MB
            if (body.length > 1e6)
                req.connection.destroy();
        });

        req.on('end', function () {
            var post = qs.parse(body);
				    con.query("INSERT INTO anomalies.anomalies (endpoint, information) VALUES ('"+post.endpoint+"','"+post.information+"');", function(err,rows){
						if(err)
						  {
						  res.json(err);
						  }
						  else{
						  res.json(rows);
						  }
						  });
				});
			}
});





app.use(express.static('vis'));
