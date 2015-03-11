
//      qsub -I
//      chmod 755 runMR.sh 
//      ./runMR.sh MapReduce /newscratch/lngo/dataset/rating/ out



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

/* Main Problem with this program
 * The values from the Mapper are not being passed to the Reducer
 */

public class StDev {
	public static class Map extends Mapper<LongWritable ,Text, Text, FloatWritable>{
		/* matchMap is a hashmap used find a movie's genre from its movieid
		 * movieMap is a hashmap a genre and a (genre, rating) pair
		 */
		private HashMap<Integer, String> matchMap = new HashMap<Integer, String>();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			/* splits the line and stores each column into an array of strings */
			String line = value.toString();
			String[] columnValues = line.split("::");
			
			/* obtains movieid and rating from their respective columns */
			int movieid = Integer.parseInt(columnValues[1]);
			float rating = Float.parseFloat(columnValues[2]);
			
			/* obtains appropriate genre for the given movieid */
			String genre = matchMap.get(new Integer(movieid));
			String genreValues[] = genre.split("\\|");

			for(int i = 0; i < genreValues.length; i++){
				context.write(new Text(genreValues[i]), new FloatWritable(rating));
			}			
		}
		
		
		/* the setup function runs only once before the mapping process starts
		 * it opens the movies.dat file and creates a hashmap of the movieids and
		 * their corresponding genres
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			String param = conf.get("matchfile");
			InputStream is = new FileInputStream(param);
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			
			/* reads each line, splits it and hashes id and matching genre */
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
		/* rateMap is a hashmap of (genre, statistics)
		 * the algorithm implemented here allows us to keep track of both the mean
		 * and standard deviation as a stream
		 */
		HashMap<String, Float[]> rateMap = new HashMap<String, Float[]>();
		HashMap<String, Vector<Float>> medianMap = new HashMap<String, Vector<Float>>();
		float[] stats = new float[5];
		
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
			/* this is the part where the problems occur, when key and value are received from the Mapper, they are
			 * different / wrong. However, they are correct in the mapper.
			 */		
			String curgenre = key.toString();
			for(FloatWritable val: values){				
				/* loops through every genre and updates the statistics: 
				 * mean is calculated by adding to the sum every iteration
				 * standard deviation uses the following formula:
				 * to start: m(k) = 0, s(1) = 0
				 * m(k) = m(k-1) + (x(k) - m(k-1)) / k
				 * s(k) = s(k-1) + (x(k) - m(k-1)*(x(k)-m(k))
				 * Finally, stdev = sqrt(s(k) / (k-1))
				 * 
				 * The float array is organized as follows:
				 * floatarray[0] = calculates the sum for the mean (add x everytime)
				 * floatarray[1] = keeps track of count (add 1 everytime)
				 * floatarray[2] = keeps track of m(k)
				 * floatarray[3] = keeps track of s(k)
				 */
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
			
			/* compute the final results and output them */
			float mean = rateMap.get(curgenre)[0].floatValue() / rateMap.get(curgenre)[1].floatValue();
			/* standard deviation */
			float std = 0;
			if(rateMap.get(curgenre)[1] < 2){
				std = rateMap.get(curgenre)[0].floatValue();
			}
			else{
				float total = rateMap.get(curgenre)[3].floatValue();
				float div = total / (rateMap.get(curgenre)[1] - 1);
				std = (float)Math.sqrt((double)div);

			}
			context.write(key, new FloatWritable(std));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		/* movies.dat could be replaced by an args[x] is necessary */
        Configuration conf = new Configuration();
        conf.set("matchfile", "./movies.dat");

        /* creates new job from configuration */
        Job job = new Job(conf, "MapReduce");
        job.setJarByClass(StDev.class);
        
        /* defines the outputs from the Mapper */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        /* defines the outputs from the Job */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        /* defines the Mapper and Reduce classes */
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        /* defines the input and output formats for the Job */
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* defines the input and output files for the job */
        FileInputFormat.addInputPath(job, new Path("x000"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

	}
}
