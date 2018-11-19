import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Task1MapReduce {

    /**
     * Mapper class.
     */
    public static class Task1MapperInputFile
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text word_appearances_year = new Text();

        /**
         * Map function.
         * @param key Key fed to the mapper.
         * @param value Value fed to the mapper.
         * @param context Context of the job.
         * @throws IOException Exception handling for IO errors.
         * @throws InterruptedException Exception handling for interrupts (i.e. CTRL + C)
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            // Lines are split by regex \\s+, which means <tab>
            String line = value.toString();
            String[] values = line.split("\\s+");

            // Sanity of the input is checked. We're expecting 4 values.
            // We'll not take into account entries which contain _ as these are metadata.
            if (values.length > 3 && !values[0].contains("_"))
            {
                // Calculate the decade to which this entry belongs.
                int decade_read = Integer.parseInt(values[1]) / 10;
                if(decade_read > 179)
                {
                    // If decade is above 179 (i.e. 1800 onwards), map the values.
                    decade.set(decade_read*10);
                    word_appearances_year.set(values[0] + "\t" + values[2]);

                    // <Key,Value> pairs are fed as <Decade, "word<tab>number_of_appearances">
                    context.write(decade, word_appearances_year);
                }
            }
        }
    }

    /**
     * Combiner class. Different from the Reducer class as we can't assume we have all the records for a given
     * word in a decade.
     */
    public static class Task1CombinerToDecades
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text highest_text_data = new Text();

        /**
         * Reduce method.
         * @param key Key fed to the reducer.
         * @param values Values fed to the reducer.
         * @param context Context fed to the reducer.
         * @throws IOException Exception handling for IO errors.
         * @throws InterruptedException Exception handling for interrupts (i.e. CTRL + C)
         */
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // We'll use a HashMap as it's light and has convenient methods for keeping our code simple.
            HashMap<String, Integer> word_appearances = new HashMap<>();

            for (Text val : values) {

                // For each value for a given key (Each word<tab>apps for our given decade)
                String[] value = val.toString().split("\\s+");
                String word = value[0];
                int appearances = Integer.parseInt(value[1]);

                // We check if it's already in our HashMap.
                if (word_appearances.containsKey(word))
                {
                    // If it is, we update the appearances count.
                    int aggregated = word_appearances.get(word);
                    word_appearances.replace(word, aggregated + appearances);
                } else {
                    // If it isn't, we put it in.
                    word_appearances.put(word, appearances);
                }
            }

            // For each word in our HashMap...
            for (Map.Entry entry : word_appearances.entrySet())
            {
                decade.set(Integer.parseInt(key.toString()));
                highest_text_data.set(entry.getKey() + "\t" + entry.getValue());

                // We'll output decade, word and the sum of it's appearances.
                context.write(decade,highest_text_data);
            }
        }
    }

    /**
     * Reducer class.
     */
    public static class Task1ReducerToDecades
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text highest_text_data = new Text();

        /**
         * Reduce method.
         * @param key Key fed to the reducer.
         * @param values Values fed to the reducer.
         * @param context Context fed to the reducer.
         * @throws IOException Exception handling for IO errors.
         * @throws InterruptedException Exception handling for interrupts (i.e. CTRL + C)
         */
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // We'll maintain a record of the highest repetitions and the corresponding 1gram.
            int highest_score = 0;
            String highest_string = "";

            // We'll again use a HashMap for convenience and speed.
            HashMap<String, Integer> word_appearances = new HashMap<>();

            for (Text val : values)
            {
                // For each value for a given key (Each word<tab>apps for our given decade)
                String[] value = val.toString().split("\\s+");
                String word = value[0];
                int appearances = Integer.parseInt(value[1]);

                // We check if it's already in our HashMap.
                if (word_appearances.containsKey(word))
                {
                    // If it is, we update the appearances count.
                    int aggregated = word_appearances.get(word);
                    word_appearances.replace(word, aggregated + appearances);
                    if (aggregated + appearances > highest_score)
                    {
                        // And if it sets a new record, we replace our current one.
                        highest_score = (aggregated + appearances);
                        highest_string = (word);
                    }
                } else {
                    // If it isn't, we put it in.
                    word_appearances.put(word, appearances);
                    if (appearances > highest_score)
                    {
                        // And if it sets a new record, we replace our current one.
                        highest_score = appearances;
                        highest_string = word;
                    }
                }
            }

            // We output the most repeated 1gram and its appearances for the present decade.
            decade.set(Integer.parseInt(key.toString()));
            highest_text_data.set(highest_string + "\t" + highest_score);

            // Only one record will be written per decade.
            context.write(decade, highest_text_data);
        }
    }

    public static void main(String[] args) throws Exception {
        // Create a new configuration for the Hadoop job.
        Configuration conf = new Configuration();

        // Create a new job for the configuration instance, and give it a name.
        Job job = Job.getInstance(conf, "Task 1");

        // Select which JAR is going to be executed.
        job.setJarByClass(Task1MapReduce.class);

        // Select our mapper class.
        job.setMapperClass(Task1MapperInputFile.class);

        // Select our combiner class.
        job.setCombinerClass(Task1CombinerToDecades.class);

        // Select our reducer class.
        job.setReducerClass(Task1ReducerToDecades.class);

        // Configure the output <key,value> types for the mapper class.
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Configure the output <key,value> types for the reducer class.
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Select our input files. As task 1 requires, we'll pick an 1 gram for the given letter.
        // Input format is plain text file so no further action is required.
        FileInputFormat.addInputPath(job, new Path("input_1gram/googlebooks-spa-all-1gram-20120701-"+args[0]));

        // Select our output path. We'll use output_task1 folder. It can't exist prior to executing the job.
        FileOutputFormat.setOutputPath(job, new Path("output_task1/"));

        // Wait for conclusion and finish.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
