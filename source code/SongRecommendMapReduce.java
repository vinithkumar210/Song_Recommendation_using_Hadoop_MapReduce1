
package Cloud.FinalProject;
  
import java.io.IOException;  
import java.util.*;  
import java.io.*;
  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.conf.*;  
import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapred.*;  
import org.apache.hadoop.util.*;  
import org.apache.hadoop.fs.FileSystem;
import java.nio.ByteBuffer;
import java.math.*;




public class songRecommandMapReduce { 

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text,Text,Text> {  
      //private final static IntWritable one = new IntWritable(1);  
      private DoubleWritable num = new DoubleWritable();  
      private Text num1 = new Text();
	  private String n = "1";
	  
      public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {  
        String line = value.toString(); 
		//String[] tokens= line.split(",");
		String[] tokens = new String[7];
		tokens= line.split(",");
		String Artist = tokens[0].toString().replace("/"," ").trim();
		String SongName = tokens[1].toString().replace("/"," ").trim();
		String SentimentScore = tokens[2].toString().replace(" ","").trim();
		String JoyScore = tokens[3].toString().replace(" ","").trim();
		String SorrowScore = tokens[4].toString().replace(" ","").trim();
		String AngryScore = tokens[5].toString().replace(" ","").trim();
		String SurpriseScore = tokens[6].toString().replace(" ","").trim();
		
		String Keyword = tokens[7].toString();
		
		String[] keywords = Keyword.split(" ");
		// System.out.print(keywords[0]);
		
    output.collect(new Text("positiveornegative"+SentimentScore), new Text(Artist+"_"+SongName));  
		output.collect(new Text("Joy"+JoyScore), new Text(Artist+"_"+SongName));  
		output.collect(new Text("Sorrow"+SorrowScore), new Text(Artist+"_"+SongName));  
		output.collect(new Text("Angry"+AngryScore), new Text(Artist+"_"+SongName));  
		output.collect(new Text("Surprise"+SurpriseScore), new Text(Artist+"_"+SongName));  
		
		
		for(int index = 1; index < keywords.length; index++)//第一個是空  導致也收集了
		{
			String word = keywords[index].toString().trim().toLowerCase();
			output.collect(new Text(word), new Text(Artist+"_"+SongName));  
		}

      }  
    }  
  
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> { 
	  public void reduce(Text key, Iterator<Text> values,OutputCollector<Text,Text> output, Reporter reporter) throws IOException {  
	  
	  String value = "";
	  String allValue = "";
	  //  System.out.print(key+",");
      while (values.hasNext()) {
		 value = values.next().toString();
		 allValue += value+"/";
      } 
	  // allValue = allValue.replace(" ","");
	  allValue = allValue.replace("//","/");
	  allValue = allValue.toLowerCase();
	  allValue = allValue.replace("'"," ");
	  allValue = allValue.replace("\""," ");
	  allValue = allValue.replace("‘"," ");
	  allValue = allValue.replace("“"," ");
	  allValue = allValue.replace("’"," ");
	  allValue = allValue.replace("”"," ");
		allValue = allValue.replace("."," ");
		// allValue = allValue.replaceAll("-"," ");
	  allValue = allValue.toString().trim();
	  String label = key+"";
	//   label = label.replaceAll(" ","");
	  label = label+",";
	  label = label.replace(",,",",");
	  // label = label.toLowerCase();
	  label = label.replace("'"," ");
	  label = label.replace("\""," ");
	  label = label.replace("‘"," ");
	  label = label.replace("“"," ");
	  label = label.replace("’"," ");
	  label = label.replace("”"," ");
		label = label.replace("."," ");
	  // label = label.toString().trim();
		// System.out.print(label);
	  output.collect(new Text(label),new Text(allValue)); 
	// String value="";
	// String allValue="";
	// while(values.hasNext()){
	// 	value = values.next().toString();
	// 	value.replace(" ","");
	// 	allValue+=value+"/";
	// }
	// output.collect(new Text(key),new Text(allValue));
    }
	
	}
  
    public static void main(String[] args) throws Exception {  
      JobConf conf = new JobConf(songRecommandMapReduce.class);  
      conf.setJobName("song-Recommand");  
  
      conf.setOutputKeyClass(Text.class);  
      conf.setOutputValueClass(Text.class);  
  
      conf.setMapperClass(Map.class);  
      conf.setCombinerClass(Reduce.class);  
      conf.setReducerClass(Reduce.class);  
  
      conf.setInputFormat(TextInputFormat.class);  
      conf.setOutputFormat(TextOutputFormat.class);  
  
	  Path path1=new Path(args[1]);
      FileInputFormat.setInputPaths(conf, new Path(args[0]));  
      FileOutputFormat.setOutputPath(conf, path1);  

	  try{
        FileSystem dfs=FileSystem.get(path1.toUri(),conf);
        if(dfs.exists(path1)){
          dfs.delete(path1,true);
        }
        JobClient.runJob(conf);  
      }catch(Exception ex){
        ex.printStackTrace();
      }
    }  
 }  