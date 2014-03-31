/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SixDegree {
	// The mapper class, the key is the compositeKey pair (airline, month)
	public static class FlightMapper extends
			Mapper<Object, Text, Text, Text> {


		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//System.out.println(value.toString());
			context.write(new Text("1"), value);

		}
	}

	// This reduce task will generate the airline and average
	// arrdelay for each month
	public static class FlightReducer extends
			Reducer<Text, Text, NullWritable, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			HashMap<Pair<String, String>, Integer> edge_dict = new HashMap<Pair<String, String>, Integer>();
			// compute every month's sum of the arrdelay and the count of them
			for (Text val : values) {
				String[] edge = val.toString().split(",");
				if (edge.length != 2)
					continue;
				Pair<String, String> pairkey;
				if (Integer.parseInt(edge[0]) < Integer.parseInt(edge[1])) {
					pairkey = Pair.make(edge[1], edge[0]);
				}
				else	
					pairkey = Pair.make(edge[0], edge[1]);
				
				if (edge_dict.containsKey(pairkey))
					edge_dict.put(pairkey, edge_dict.get(pairkey) + 1);
				else
					edge_dict.put(pairkey, 1);
			}
			for (Entry<Pair<String, String>, Integer> entry : edge_dict.entrySet()) {
				//System.out.println(entry.getValue());
				if (entry.getValue() == 2) {
					context.write(NullWritable.get(), new Text(entry.getKey().a + "," + new Text(entry.getKey().b)));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(SixDegree.class);
		job.setMapperClass(FlightMapper.class);
		job.setReducerClass(FlightReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
