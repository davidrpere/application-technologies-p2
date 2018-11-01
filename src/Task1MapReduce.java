import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1MapReduce {

    public static class Task1MapperInputFile
            extends Mapper<Object, Text, IntWritable, Text>{

        private IntWritable decade = new IntWritable();
        private Text word_appearances_year = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\\s+");

            if(value.getLength() > 3)
            {
                int decade_read = Integer.parseInt(values[1]) / 10;
                if(decade_read > 179){
                    decade.set(decade_read);
                    word_appearances_year.set(values[0] + "\t" + values[2]);
                    context.write(decade, word_appearances_year);
                }
            }
        }
    }
    public static class Task1ReducerToDecades
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text highest_text_data = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            AtomicInteger highest_score = new AtomicInteger();
            AtomicReference<String> highest_string = new AtomicReference<>("");
            TreeMap<String, Integer> word_appearances = new TreeMap<>();
            for( Text val : values )
            {
                String[] value = val.toString().split("\\s+");
                String word = value[0];
                int appearances = Integer.parseInt(value[1]);
                if(word_appearances.containsKey(word))
                {
                    int aggregated = word_appearances.get(word);
                    word_appearances.replace(word, aggregated+appearances);
                    if(aggregated+appearances > highest_score.get())
                    {
                        highest_score.set(aggregated + appearances);
                        highest_string.set(word);
                    }
                }else{
                    word_appearances.put(word, appearances);
                    if(appearances > highest_score.get())
                    {
                        highest_score.set(appearances);
                        highest_string.set(word);
                    }
                }
            }

            decade.set(Integer.parseInt(key.toString()));
            highest_text_data.set(highest_string + "\t" + highest_score);
            context.write(decade, highest_text_data);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task 1");
        job.setJarByClass(Task1MapReduce.class);
        job.setMapperClass(Task1MapperInputFile.class);
        job.setReducerClass(Task1ReducerToDecades.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
