
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

public class UserID{
	public static class Map extends Mapper<LongWritable ,Text, Text, Text>{
		
//		private HashMap<Integer, String> matchMap = new HashMap<Integer, String>();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			String[] columnValues = line.split("::");
						
			int userid = Integer.parseInt(columnValues[0]);
			String movieid = columnValues[1];
			context.write(new Text(userid), new Text(movieid));			
		}
		
/*		@Override
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
		}*/
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
	
		private HashMap<Integer, String> matchMap = new HashMap<Integer, String>();
		String maxUsrID = null;
		String maxUsrGenre = null;
		int maxRates = 0;
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<String, Integer> numRatings = new HashMap<String, Integer>();

			int totalRatings = 0;
			String usrID = key.toString();
			String genre = null;
			Integer numNew = null;
			
			for(Text val: values){
				/* get movie id and pick out most popular genre then pick user with highest rating */
				String curVal = val.toString();
				genre = matchMap.get(Integer.parseInt(curVal));

				if(numRatings.containsKey(genre)){
					int num = numRatings.get(genre).intValue();	
					numNew = new Integer(num + 1);
				}
				else{
					numNew = new Integer(1);
				}
				numRatings.put(genre, numNew);

				totalRatings += 1;
			}

			Iterator it = numRatings.entrySet().iterator();
			String maxGenre = genre;
			int maxRatings = numRatings.get(genre).intValue();
			while(it.hasNext()){
				Map.Entry pairs = (Map.Entry)it.next();
				if(pairs.getValue().intValue > maxRatings){
					maxGenre = pairs.getKey();
					maxRatings = pairs.getValue().intValue();
				}
			}

			if(totalRatings > maxRates){
				maxUsrID = usrID;
				maxUsrGenre = maxGenre;
				maxRates = totalRatings;			
			}
		}

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
	
	public static void main(String[] args) throws Exception{
		
		/* movies.dat could be replaced by an args[x] is necessary */
        Configuration conf = new Configuration();
        conf.set("matchfile", "./movies.dat");

        /* creates new job from configuration */
        Job job = new Job(conf, "MapReduce");
        job.setJarByClass(UserID.class);
        
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
