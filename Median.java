
// Mean.java
// CPSC 3620 Assignment 4
// Dr. Linh Ngo
// William Beasley

// Calculates the median of the genre ratings from movies.dat and x00[0-9]


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.*;
import java.io.*;


public class Median {
	public static class Map extends Mapper<LongWritable ,Text, Text, FloatWritable>{
		
		private HashMap<Integer, String> matchMap = new HashMap<Integer, String>();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			String[] columnValues = line.split("::");
			
			int movieid = Integer.parseInt(columnValues[1]);
			float rating = Float.parseFloat(columnValues[2]);
			
			String genre = matchMap.get(new Integer(movieid));
			String genreValues[] = genre.split("\\|");

			for(int i = 0; i < genreValues.length; i++){
				context.write(new Text(genreValues[i]), new FloatWritable(rating));
			}			
		}
						
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			String param = conf.get("matchfile");
			InputStream is = new FileInputStream(param);
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			
			String line = null;
			while((line = br.readLine()) != null){
				String[] splitter = line.split("::");
				Integer temp = new Integer(Integer.parseInt(splitter[0]));
				matchMap.put(temp, splitter[2]);
			}
			
			is.close();
			isr.close();
			br.close();	
		}
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		
		HashMap<String, Float[]> rateMap = new HashMap<String, Float[]>();
		HashMap<String, Vector<Float>> medianMap = new HashMap<String, Vector<Float>>();
		float[] stats = new float[5];
		
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
					
			String curgenre = key.toString();
			for(FloatWritable val: values){				
				
				if(rateMap.containsKey(curgenre)){
					Float med = new Float(val.get());
					medianMap.get(curgenre).add(med);
					
					Float[] temp = rateMap.get(curgenre);
					float currentrating = val.get();
					float t0 = temp[0].floatValue();
					temp[0] = new Float(t0 + currentrating);
					float t1 = temp[1].floatValue();
					temp[1] = new Float(t1 + 1);
					float t2 = temp[2].floatValue();
					temp[2] = new Float(t2 + ((currentrating - t2) / temp[1].floatValue()));
					float t3 = temp[3].floatValue();
					temp[3] = new Float(t3 + ((currentrating - t2)*(currentrating - temp[2].floatValue())));
					rateMap.put(curgenre, temp);
				}
				else{
					Vector<Float> a = new Vector<Float>();
					Float med = new Float(val.get());
					a.add(med);
					medianMap.put(curgenre, a);
					
					float currentrating = val.get();
					Float[] temp = new Float[4];
					temp[0] = new Float(currentrating); /* sum */
					temp[1] = new Float(1); /* count */
					temp[2] = new Float(val.get()); /* m(k) */
					temp[3] = new Float(0); /* s(k) */
					
					rateMap.put(curgenre, temp);
				}
			}
						
			/* median */
			float median = 0;
			int vecsize = medianMap.get(curgenre).size();
			Collections.sort(medianMap.get(curgenre));
			int medi = vecsize/2;
			if(vecsize % 2 == 0){
				float upper = medianMap.get(curgenre).get(medi).floatValue();
				float lower = medianMap.get(curgenre).get(medi-1).floatValue();
				median = (upper + lower) / 2;
			}
			else{
				median = medianMap.get(curgenre).get(medi).floatValue();
			}
			
			context.write(key, new FloatWritable(median));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		
        Configuration conf = new Configuration();
        conf.set("matchfile", "./movies.dat");

        Job job = new Job(conf, "MapReduce");
        job.setJarByClass(Median.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        for(int i = 0; i < 10; ++i){
        	FileInputFormat.addInputPath(job, new Path("x00"+i));
        }
        FileInputFormat.addInputPath(job, new Path("x010"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

	}
}
