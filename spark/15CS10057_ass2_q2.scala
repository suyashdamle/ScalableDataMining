import java.util.TimeZone
import java.text.SimpleDateFormat

var f=sc.textFile("ghtorrent-logs.txt.gz")
f=f.filter(x=>x.size>0)
var fmap=f.map(line=>line.split(", | -- ",4))

fmap=fmap.map(line=> if(line.size==4)Array(line(0),line(1),line(2))++line(3).split(": ",2) else line)

case class my_schema (debug_level:String,timestamp:java.util.Date,download_id:String,retrieval_stage:String,rest:String)//


// input: rows containing reference to some repo
def mapping_fn(x:my_schema):String={
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

// removing all invalid records altogether	
myRDD=myRDD.filter(x=>x.rest!=null && x.timestamp!=null)

// finding the subset of the RDD that contain a reference to a repo
var y=myRDD.filter(x=>(((x.rest.contains("http://")||x.rest.contains("https://"))&& x.rest.contains("github.com/repos")) ||x.rest.contains("Repo")))
var repos=y.map(x=>mapping_fn(x)).filter(x=>x!=null)
var repo_counts=repos.map(x=>(x,1)).reduceByKey(_ + _)
val act_repo=repo_counts.max()(Ordering[Int].on(x=>x._2))

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// function for inverted_index:
def invertedIndex(x: String,rdd:org.apache.spark.rdd.RDD[my_schema]) = {rdd.groupBy(r=>x match {
	case "debug_level" => r.debug_level
	case "timestamp" => r.timestamp
	case "download_id" => r.download_id
	case "retrieval_stage" => r.retrieval_stage
	})
}

// trying to use the more efficient aggregateByKey operation for inverted_index
import collection.mutable.HashSet
def invertedIndex2(x: String,rdd:org.apache.spark.rdd.RDD[my_schema])={
	var kv=rdd.map(r=>x match{
		case "debug_level" => (r.debug_level,r)
		case "timestamp" => (r.timestamp,r)
		case "download_id" => (r.download_id,r)
		case "retrieval_stage" => (r.retrieval_stage ,r)
	})
	kv.take(10).foreach(println)
	val initialSet = HashSet.empty[my_schema]
	val addToSet = (s: HashSet[my_schema], v: my_schema) => s += v
	val mergePartitionSets = (p1: HashSet[my_schema], p2: HashSet[my_schema]) => p1 ++= p2
	val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
	//uniqueByKey.first()
	uniqueByKey
}

//b
var rdd2=y.filter(x=>x.download_id=="ghtorrent-22")
var n_repo=rdd2.map(x=>mapping_fn(x)).filter(x=>x!=null).map(x=>(x,1)).reduceByKey(_+_).count()


//c 
var rdd3=invertedIndex("download_id",y).filter(x=>x._1=="ghtorrent-22")
//var n_repo=(rdd3.first()._2).map(x=>mapping_fn(x)).filter(x=>x!=null).map(x=>(x,1)).reduceByKey(_+_).count()