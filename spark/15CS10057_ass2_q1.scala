import java.util.TimeZone
import java.text.SimpleDateFormat
//import  java.sql.Timestamp

var f=sc.textFile("ghtorrent-logs.txt.gz")
f=f.filter(x=>x.size>0)
var fmap=f.map(line=>line.split(", | -- ",4))

fmap=fmap.map(line=> if(line.size==4)Array(line(0),line(1),line(2))++line(3).split(": ",2) else line)

case class my_schema (debug_level:String,timestamp:java.util.Date,download_id:String,retrieval_stage:String,rest:String)//


def mapping_fn(x:Array[String])={
	TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
	val s=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX") //for conversion of string to timestamp format
	if(x.size==5){
		try
			my_schema(x(0),s.parse(x(1)),x(2),x(3),x(4))
		catch{
			case e:Exception=>my_schema(x(0),null,x(2),x(3),x(4))
		}
	}	
	else if(x.size==4){
		try
			my_schema(x(0),s.parse(x(1)),x(2),x(3),null)
		catch{
			case e:Exception=>my_schema(x(0),null,x(2),x(3),null)
		}	
	}
	else if(x.size==3){
		try
			my_schema(x(0),s.parse(x(1)),x(2),null,null)
		catch{
			case e:Exception=>my_schema(x(0),null,x(2),null,null)
		}	
	}
	else if(x.size==2){
		try
			my_schema(x(0),s.parse(x(1)),null,null,null)
		catch{
			case e:Exception=>my_schema(x(0),null,null,null,null)
		}
	}
	else{
		my_schema(x(0),null,null,null,null)
	}
}

def mapping_fn_2(x:my_schema):String={
	var containsURL=x.rest.contains("github.com/repos/")
	try{
		if(!containsURL){
			var a=x.rest.split(" ")
			val repo=a(a.indexOf("Repo")+1)
			return repo
		}
		else{
			//removing html parameters: ?per_page=200.....
			var a=x.rest.split("github.com/repos/")(1)
			a=a.split("\\?")(0)
			var b=a.split("/")
			if(b.size>1){
				val repo=b(0)+"/"+b(1)
				return repo
			}
			else if(b.size==1){
				val repo=b(0)
				return repo
			}
		}
		return null
	}
	catch{
		case e:Exception=>return null
	}
}

TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
var s=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX") //for conversion of string to timestamp format

var myRDD=fmap.map(x=>try{my_schema(x(0),s.parse(x(1)),x(2),x(3),x(4))}catch{case e:Exception=>my_schema(null,null,null,null,null)})


println("number of lines: ",myRDD.count())
println("number of WARN messages: ",myRDD.filter(x=>x.debug_level=="WARN").count())
println("RDD rows with retrieval_stage=api_client: ",myRDD.filter(x=>x.retrieval_stage=="api_client.rb").count())
println("number of repositories processed with retrieval_stage= api_client: ", myRDD.filter(x=>x.retrieval_stage=="api_client.rb" && ((x.rest.contains("Repo")) || (x.rest.contains("repos")))).count())


myRDD=myRDD.filter(x=>x.rest!=null && x.timestamp!=null)
var y=myRDD.filter(x=>x.retrieval_stage=="api_client.rb" && (x.rest.contains("https://")||(x.rest.contains("http://"))))
var z=y.groupBy(x=>x.download_id)
var a=z.map(x=>(x._1,x._2.size))
println("clients with most http requests: ",a.max()(Ordering[Int].on(x=>x._2)))

// for failed https requests
y=myRDD.filter(x=>x.retrieval_stage=="api_client.rb" && (x.rest.contains("https://")||(x.rest.contains("http://"))) &&(x.rest.contains("Failed request")))
z=y.groupBy(x=>x.download_id)
a=z.map(x=>(x._1,x._2.size))
println("clients with most FAILED http requests: ",a.max()(Ordering[Int].on(x=>x._2)))

// for active hour
val act_hrs=myRDD.filter(x=>x.timestamp!=null).map(x=>x.timestamp.getHours()).map(x=>(x,1)).reduceByKey(_ + _).collect()
println("Most active hours: ",act_hrs)


// for active repo:
// finding the subset of the RDD that contain a reference to a repo
var y=myRDD.filter(x=>(((x.rest.contains("http://")||x.rest.contains("https://"))&& x.rest.contains("github.com/repos")) ||x.rest.contains("Repo")))
var repos=y.map(x=>mapping_fn_2(x)).filter(x=>x!=null)
var repo_counts=repos.map(x=>(x,1)).reduceByKey(_ + _)
val act_repo=repo_counts.max()(Ordering[Int].on(x=>x._2))
println("Most active repo: ",act_repo)


