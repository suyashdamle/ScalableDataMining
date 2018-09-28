import java.io.*; 
import java.util.StringTokenizer; 
import java.util.Vector;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class matrix_mult_1 { 
	public static void main(String[] args) throws Exception { 
		int SIZE=68746;
		BufferedReader input = new BufferedReader (new InputStreamReader (System.in));
		System.out.println("Enter the size of the matrices. (for N X N) > ");
		String line=input.readLine();
		SIZE=Integer.parseInt(line);
		Configuration conf = new Configuration(); 
		conf.set("n",Integer.toString(SIZE));
		Job job = Job.getInstance(conf, "word count"); 
		job.setJarByClass(matrix_mult_1.class); 
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class); 
		job.setReducerClass(IntSumReducer.class); 
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class); 
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
class IntSumReducer extends Reducer<Text,Text,Text,Text> { 
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{ 
		int sum = 0;
		int n=Integer.parseInt(context.getConfiguration().get("n"));
		int[] arrA= new int[n];
		int[] arrB= new int[n];
		for(int i=0;i<n;i++){
			arrB[i]=arrA[i]=0;												// setting default value of elements to be zero
		} 
		for (Text val : values) { 
			String[] strVals=(val.toString()).split(",");
			if (strVals.length < 3){
				System.out.println("FOUND EXCEPTIONAL VALUE: "+val.toString());
				continue;
			}
			if (strVals[0].equals("A")){
				arrA[Integer.parseInt(strVals[1])]=Integer.parseInt(strVals[2]);
			}
			else{
				arrB[Integer.parseInt(strVals[1])]=Integer.parseInt(strVals[2]);	
			}
		}
		for(int i=0;i<n;i++){
			sum+=(arrA[i]*arrB[i]);
		}
		if (sum==0){
			return;
		}
		String r=key.toString().split(",")[0];
		String c=key.toString().split(",")[1];
		context.write(null,new Text(r+"\t"+c+"\t"+Integer.toString(sum))); 
	}
}
class TokenizerMapper extends Mapper<Object, Text, Text, Text>{ 
	private final static IntWritable one = new IntWritable(1); 
	private Text word = new Text(); 
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
		if(value.toString().split("\t").length<3)return;//assuming to be a bank line
		int r=Integer.parseInt((value.toString()).split("\t")[0]);
		int c=Integer.parseInt((value.toString()).split("\t")[1]);
		int v=Integer.parseInt((value.toString()).split("\t")[2]);
		int n=Integer.parseInt(context.getConfiguration().get("n"));
		Text outputKey=new Text();
		Text outputVal=new Text();

		// for first matrix - sending row of A to all cols of B
		for(int i=0;i<n;i++){
			outputKey.set(r+","+i);
			outputVal.set("A,"+c+","+v);
			context.write(outputKey,outputVal);
		}		

		// for the second matriix - sending col of B to all rows of A
		for(int i=0;i<n;i++){
			outputKey.set(i+","+c);
			outputVal.set("B,"+r+","+v);
			context.write(outputKey,outputVal);
		}		
 
	}
}