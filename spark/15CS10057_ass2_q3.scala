import java.util.TimeZone
import java.text.SimpleDateFormat

var f=sc.textFile("ghtorrent-logs.txt.gz")
var f2=sc.textFile("important-repos.csv")

f=f.filter(x=>x.size>0)
f2=f2.filter(x=>x.size>0)

var fmap=f.map(line=>line.split(", | -- ",4))
var fmap2=f2.map(line=>line.split(","))

fmap=fmap.map(line=> if(line.size==4)Array(line(0),line(1),line(2))++line(3).split(": ",2) else line)

case class my_schema (debug_level:String,timestamp:java.util.Date,download_id:String,retrieval_stage:String,rest:String)//
case class schema2 (id:String,url:String,owner_id:String,name:String,language:String,created_at:java.util.Date,forked_from:String,deleted:String,updated_at:java.util.Date)

// input: rows containing reference to some repo
def mapping_fn(x:String):String={
	var containsURL=x.contains("github.com/repos/")
	try{
		if(!containsURL){
			var a=x.split(" ")
			val repo=a(a.indexOf("Repo")+1)
			return repo
		}
		else{
			//removing html parameters: ?per_page=200.....
			var a=x.split("github.com/repos/")(1)
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
var s2=new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss") //for conversion of string to timestamp format
var myRDD=fmap.map(x=>try{my_schema(x(0),s.parse(x(1)),x(2),x(3),x(4))}catch{case e:Exception=>my_schema(null,null,null,null,null)})
var RDD2=fmap2.map(x=>try{schema2(x(0),x(1),x(2),x(3),x(4),s2.parse(x(5)),x(6),x(7),s2.parse(x(8)))}catch{case e:Exception=>schema2(null,null,null,null,null,null,null,null,null)})


// removing all invalid records altogether	
myRDD=myRDD.filter(x=>x.rest!=null && x.timestamp!=null)
RDD2=RDD2.filter(x=>x.id!=null)
var y=myRDD.filter(x=>(((x.rest.contains("http://")||x.rest.contains("https://"))&& x.rest.contains("github.com/repos")) ||x.rest.contains("Repo")))

var key_myRDD=y.map(x=>(mapping_fn(x.rest),x))
var key_RDD2=RDD2.map(x=>(mapping_fn(x.url),x))


//joining the 2 RDDs
var joined_RDD=key_RDD2.join(key_myRDD)
var count1=joined_RDD.count()
var count2=joined_RDD.filter(x=>(x._2)._2.rest.contains("Failed")).count()