import java.io.IOException;
import java.lang.Integer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.curator.utils.DefaultZookeeperFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WHVisitors {
	
	public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }
	
    public static class WHTestMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString();
        	String delims = ",";
        	String[]tokens = line.split(delims);
            context.write(new Text(tokens[0]+" "+tokens[1]+","+tokens[2]+","+tokens[11]), new IntWritable(1));
        }
    }

    public static class WHTestReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    public static class TopWHMap extends Mapper <Text, Text, NullWritable, TextArrayWritable>{

    	SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm");
    	//private TreeSet<Pair<Date, String>> nameDateMap = new TreeSet<Pair<Date, String>>();
    	private TreeMap<String, String> nameDateMap = new TreeMap<String, String>();
    	//Date date = new Date();
    	@Override
    	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
    		String line = key.toString();
    		String delims = ",";
        	String[]tokens = line.split(delims);
        	String name = tokens[0]+" "+tokens[1];
        	String date = tokens[2];
        	/*try {
				date = format.parse(tokens[2]);
			} catch (ParseException e) {
				e.printStackTrace();
			}*/
        nameDateMap.put(name, date);
        date = null;	
    	}
    	
    	@Override
		protected void cleanup(Mapper<Text, Text, NullWritable, TextArrayWritable>.Context context)
				throws IOException, InterruptedException {
    		
    		for (String key : nameDateMap.keySet()) {
                String[] strings = {key.toString(), nameDateMap.get(key).toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
		}
    }
    
    private static class TopWHReduce extends Reducer<NullWritable, TextArrayWritable, Text, Text>{

    	private TreeMap<String, String> nameDateMap = new TreeMap<String, String>();
    	SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm");
    	
		@Override
		protected void reduce(NullWritable key, Iterable<TextArrayWritable> values,
				Reducer<NullWritable, TextArrayWritable, Text, Text>.Context context) throws IOException, InterruptedException{
			//Date date = new Date();
			for (TextArrayWritable val : values){
				Text[]line = (Text[]) val.toArray();
				
				String name = line[0].toString();
				String date = line[1].toString();
				/*try {
					date = format.parse(line[1].toString());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				
				nameDateMap.put(name, date);
				context.write(new Text(name), new Text(date.toString()));
			}
/*			List <Date> daty = new ArrayList<Date>();
			for (String k : nameDateMap.keySet()){
			
				daty.add(nameDateMap.get(k));
				Date dateMin = Collections.min(daty);
				Date dateMax = Collections.max(daty);
				context.write(new Text(k), new Text(dateMin.toString()+", "+dateMax.toString()));
				Iterator<Date> i = daty.iterator();
				while(i.hasNext()){
					Date d = i.next();
					i.remove();
				}
			}*/
		}
    }
    

    public static void main(String[] args) throws Exception {
    	//The purpose of the driver is to orchestrate the jobs.
    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(conf);
    	Path tmpPath = new Path("tmp");
    	fs.delete(tmpPath, true);
    	/* Job job = Job.getInstance(new Configuration(), "WH");
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);

         job.setMapperClass(WHTestMap.class);
         job.setReducerClass(WHTestReduce.class);

         FileInputFormat.setInputPaths(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));

         job.setJarByClass(WHVisitors.class);
         System.exit(job.waitForCompletion(true) ? 0 : 1);*/
        Job jobA = Job.getInstance(conf, "WHTest");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(WHTestMap.class);
        jobA.setReducerClass(WHTestReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(WHVisitors.class);
        jobA.waitForCompletion(true);
        
        Job jobB = Job.getInstance(conf, "Top WH");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);
        
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);
        
        jobB.setMapperClass(TopWHMap.class);
        jobB.setReducerClass(TopWHReduce.class);
        jobB.setNumReduceTasks(1);
        
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));
        
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
        
        jobB.setJarByClass(WHVisitors.class);
        
        System.exit(jobB.waitForCompletion(true) ? 0 : 1); //Submit the job to the cluster and wait for it to finish
    }
}
    
    class Pair<A extends Comparable<? super A>,
    B extends Comparable<? super B>>
    implements Comparable<Pair<A, B>> {

public final A first;
public final B second;

public Pair(A first, B second) {
    this.first = first;
    this.second = second;
}

public static <A extends Comparable<? super A>,
        B extends Comparable<? super B>>
Pair<A, B> of(A first, B second) {
    return new Pair<A, B>(first, second);
}


public int compareTo(Pair<A, B> o) {
    int cmp = o == null ? 1 : (this.first).compareTo(o.first);
    return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
}

@Override
public int hashCode() {
    return 31 * hashcode(first) + hashcode(second);
}

private static int hashcode(Object o) {
    return o == null ? 0 : o.hashCode();
}

@Override
public boolean equals(Object obj) {
    if (!(obj instanceof Pair))
        return false;
    if (this == obj)
        return true;
    return equal(first, ((Pair<?, ?>) obj).first)
            && equal(second, ((Pair<?, ?>) obj).second);
}

private boolean equal(Object o1, Object o2) {
    return o1 == o2 || (o1 != null && o1.equals(o2));
}

@Override
public String toString() {
    return "(" + first + ", " + second + ')';
}
}
