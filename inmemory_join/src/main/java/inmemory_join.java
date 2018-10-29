import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class inmemory_join {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, String[]> user_map = new HashMap<String, String[]>();
        private ArrayList<String> friend_list1 = new ArrayList<>();
        String user_value = "";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String user1 = context.getConfiguration().get("user_1");
            String user2 = context.getConfiguration().get("user_2");

            String[] users = value.toString().split("\t");
            String user_id = users[0];


            if (user_id.equals(user1) || user_id.equals(user2)) {
                if (friend_list1.isEmpty()) {
                    friend_list1.addAll(Arrays.asList(users[1].split(",")));
                } else {
                    String[] user_friends = users[1].split(",");
                    for (String friend : user_friends) {
                        if (friend_list1.contains(friend)) {
                            user_value = user_value + (user_map.get(friend)[0] + ":" + user_map.get(friend)[1] + ", ");
                        }
                    }
                }
                if (!user_value.equals(""))
                    context.write(new Text(user1 + "," + user2), new Text("[" + user_value.substring(0, user_value.length() - 2) + "]"));
            }
        }


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //read data to memory on the mapper.
            Configuration conf = context.getConfiguration();
            Path part = new Path(conf.get("user_data"));//Location of file in HDFS
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            for (FileStatus status : fss) {
                Path pt = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    //put (word, wordid) in the HashMap variable
                    String[] temp = new String[2];
                    temp[0] = arr[1];
                    temp[1] = arr[5];
                    user_map.put(arr[0], temp);
                    line = br.readLine();
                }
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 5) {
            System.err.println("Usage: inmemory_join <user_1> <user_2> <in_path> <user_data_path> <user_data_output_path>");
            //inmemory_join 10 30 soc_file.txt userdata.txt output_path
            System.exit(2);
        }

        conf.set("user_1", otherArgs[0]);
        conf.set("user_2", otherArgs[1]);
        conf.set("user_data", otherArgs[3]);

        // create a job with name "inmemory_join"
        Job job = new Job(conf, "inmemory_join");
        job.setJarByClass(inmemory_join.class);
        job.setMapperClass(Map.class);


        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}