
//      qsub -I
//      chmod 755 runMR.sh 
//      ./runMR.sh Mean /newscratch/lngo/dataset/rating/ out

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


public class Mean {
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
			
			float mean = rateMap.get(curgenre)[0].floatValue() / rateMap.get(curgenre)[1].floatValue();
					
			context.write(key, new FloatWritable(mean));
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		
        Configuration conf = new Configuration();
        conf.set("matchfile", "./movies.dat");

        Job job = new Job(conf, "MapReduce");
        job.setJarByClass(Mean.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("x000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

	}
}
