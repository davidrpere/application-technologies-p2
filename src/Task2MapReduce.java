import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.Normalizer;
import java.util.*;

public class Task2MapReduce {

    /**
     * First mapper class.
     */
    public static class Task2MapperInputFile
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
        ) throws IOException, InterruptedException {

            // Lines are split by regex \\s+, which means <tab>
            String line = value.toString();
            String[] values = line.split("\\s+");

            // Sanity of the input is checked. We're expecting 4 values.
            // We'll not take into account entries which contain _ as these are metadata.
            if (values.length > 3 && !values[0].contains("_"))
            {
                // Calculate the decade to which this entry belongs
                int decade_read = Integer.parseInt(values[1]) / 10;
                if (decade_read > 189 && decade_read < 200)
                {

                    // If decade is above 189 (i.e. 1900 onwards) and below 200 (2000), map the values.
                    decade.set(decade_read * 10);
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
    public static class Task2CombinerInputFile
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

            // We'll maintain a record of the highest repetitions and the corresponding ngram.
            int highest_score = 0;
            String highest_string = "";

            // We'll again use a HashMap for convenience and speed.
            HashMap<String, Integer> word_appearances = new HashMap<>();

            for (Text val : values) {

                // For each value for a given key (Each word<tab>apps for our given decade)
                String[] value = val.toString().split("\\s+");
                String word = value[0];
                int appearances = Integer.parseInt(value[1]);

                // We check if it's already in our HashMap.
                if (word_appearances.containsKey(word)) {

                    // If it is, we update the appearances count.
                    int aggregated = word_appearances.get(word);
                    word_appearances.replace(word, aggregated + appearances);
                    if (aggregated + appearances > highest_score) {

                        // And if it sets a new record, we replace our current one.
                        highest_score = (aggregated + appearances);
                        highest_string = (word);
                    }
                } else {

                    // If it isn't, we put it in.
                    word_appearances.put(word, appearances);
                    if (appearances > highest_score) {

                        // And if it sets a new record, we replace our current one.
                        highest_score = appearances;
                        highest_string = word;
                    }
                }
            }

            // We output the most repeated ngram and its appearances for the present decade.
            decade.set(Integer.parseInt(key.toString()));
            highest_text_data.set(highest_string + "\t" + highest_score);

            // Only one record will be written per decade.
            context.write(decade, highest_text_data);
        }
    }

    /**
     * Second mapper class.
     */
    public static class Task2OutputMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text word_appearances_year = new Text();

        /**
         * Reduce method.
         * @param key Key fed to the mapper.
         * @param value Values fed to the mapper.
         * @param context Context fed to the mapper.
         * @throws IOException Exception handling for IO errors.
         * @throws InterruptedException Exception handling for interrupts (i.e. CTRL + C)
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            // Lines are split by regex \\s+, which means <tab>
            String line = value.toString();
            String[] values = line.split("\\s+");

            // Sanity of the input is checked. We're expecting 5 values.
            // We'll not take into account entries which contain _ as these are metadata.
            if (values.length > 4 && !values[0].contains("_") && !values[1].contains("_"))
            {
                // Calculate the decade to which this entry belongs.
                int decade_read = Integer.parseInt(values[2]) / 10;
                if (decade_read > 189 && decade_read < 200)
                {
                    // If decade is above 189 (i.e. 1900 onwards) and below 200 (2000), map the values.
                    decade.set(decade_read * 10);

                    // Get from the job configuration context the most common 1-gram for this decade.
                    String interest = context.getConfiguration().get(decade.toString());

                    // IF the 1-gram was longer than one letter...
                    if (interest.length() > 1)
                    {
                        // If...
                        // the first char of the first word of the mapper entry 2-gram equals the first char of the 1-gram
                        // AND
                        // the first char of the second word of the mapper entry 2-gram equals the second char of the 1-gram
                        // AND
                        // the first word of the mapper entry 2-gram equals the 1-gram
                        if (Character.toLowerCase(values[0].charAt(0)) == Character.toLowerCase(interest.charAt(0))
                                && Character.toLowerCase(values[0].charAt(1)) == Character.toLowerCase(interest.charAt(1))
                                && values[0].equalsIgnoreCase(interest))
                        {
                            // <Key,Value> pairs are fed as <Decade, "word1<tab>word2<tab>number_of_appearances">
                            word_appearances_year.set(values[0] + "\t" + values[1] + "\t" + values[3]);
                            context.write(decade, word_appearances_year);
                        }
                        // Else if the 1 gram was only one letter...
                    } else if(interest.length() == 1){
                        // If...
                        // the first char of the first word of the mapper entry 2-gram equals the first char of the 1-gram
                        // AND
                        // the first word of the mapper entry 2-gram equals the 1-gram
                        if (Character.toLowerCase(values[0].charAt(0)) == Character.toLowerCase(interest.charAt(0))) {
                            // <Key,Value> pairs are fed as <Decade, "word1<tab>word2<tab>number_of_appearances">
                            word_appearances_year.set(values[0] + "\t" + values[1] + "\t" + values[3]);
                            context.write(decade, word_appearances_year);
                        }
                    }
                    // Otherwise, the 2-gram we're reading is not relevant for this decade.
                    // i.e:
                    // Most common unigram for 1910 decade was "animal"
                    // We're reading "a dormir 1914"
                }
            }
        }

    }

    /**
     * First reducer class.
     */
    public static class Task2ReducerToDecades
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

            // We'll maintain a record of the highest repetitions and the corresponding ngram.
            int highest_score = 0;
            String highest_string = "";

            // We'll again use a HashMap for convenience and speed.
            HashMap<String, Integer> word_appearances = new HashMap<>();

            for (Text val : values) {

                // For each value for a given key (Each word1<tab>apps for our given decade)
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
                        highest_score = aggregated + appearances;
                        highest_string = word;
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


    /**
     * Second reducer class.
     */
    public static class Task2BigramReducerToDecades
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

            // We'll maintain a record of the highest repetitions and the corresponding 2gram.
            int highest_score = 0;
            String highest_string = "";

            // We'll again use a HashMap for convenience and speed.
            HashMap<String, Integer> word_appearances = new HashMap<>();

            for (Text val : values)
            {
                // For each value for a given key (Each word1<tab>word2<tab>apps for our given decade)
                String[] value = val.toString().split("\\s+");
                String bigram = value[0] + "\t" + value[1];
                int appearances = Integer.parseInt(value[2]);

                // We check if it's already in our HashMap.
                if (word_appearances.containsKey(bigram))
                {
                    // If it is, we update the appearances count.
                    int aggregated = word_appearances.get(bigram);
                    word_appearances.replace(bigram, aggregated + appearances);
                    if (aggregated + appearances > highest_score)
                    {
                        // And if it sets a new record, we replace our current one.
                        highest_score = aggregated + appearances;
                        highest_string = bigram;
                    }
                } else {
                    // If it isn't, we put it in.
                    word_appearances.put(bigram, appearances);
                    if (appearances > highest_score)
                    {
                        // And if it sets a new record, we replace our current one.
                        highest_score = appearances;
                        highest_string = bigram;
                    }
                }
            }

            // We output the most repeated 2gram and its appearances for the present decade.
            decade.set(Integer.parseInt(key.toString()));
            highest_text_data.set(highest_string + "\t" + highest_score);

            // Only one record will be written per decade.
            context.write(decade, highest_text_data);
        }
    }

    /**
     * Method for removing tildes from strings.
     * @param s String to clear tildes from.
     * @return Resulting string.
     */
    public static String stripAccents(String s) {
        s = Normalizer.normalize(s, Normalizer.Form.NFD);
        s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        return s;
    }

    public static void main(String[] args) throws Exception {

        // Create a new configuration for the Hadoop job.
        Configuration conf = new Configuration();

        // Create a new job for the configuration instance, and give it a name.
        Job job = Job.getInstance(conf, "Task 2, part I");

        // Select which JAR is going to be executed.
        job.setJarByClass(Task2MapReduce.class);

        // Select our mapper class. This is the first mapper. It's the same from Task 1.
        job.setMapperClass(Task2MapperInputFile.class);

        // Select our combiner class. This is the first combiner. It's the same from Task 1.
        job.setCombinerClass(Task2CombinerInputFile.class);

        // Select our reducer class. This is the first reducer. It's the same from Task 1.
        job.setReducerClass(Task2ReducerToDecades.class);

        // Configure the output <key,value> types for the mapper class.
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Configure the output <key,value> types for the reducer class.
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        // Select our input files. As task 1 requires, we'll pick an 1 gram for the given letter.
        // Input format is plain text file so no further action is required.
        FileInputFormat.addInputPath(job, new Path("input_1gram/googlebooks-spa-all-1gram-20120701-"+args[0]));

        // Select our output path. We'll use out1 folder. It can't exist prior to executing the job.
        FileOutputFormat.setOutputPath(job, new Path("output_task2", "out1"));

        // Wait for conclusion.
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        // Now we must retrieve the output of the first Map-Reduce job.
        // We'll use a list of Strings and a HashMap for storing file paths and decade-1gram relations.
        HashMap<Integer, String> hmap = new HashMap<Integer, String>();
        List<String> filePaths = new ArrayList<>();

        // Create an auxiliary configuration to retrieve a file from HDFS.
        Configuration conf_aux = new Configuration();
        conf_aux.addResource(new Path("/opt/hadoop3/etc/hadoop/core-site.xml"));
        conf_aux.addResource(new Path("/opt/hadoop3/etc/hadoop/hdfs-site.xml"));

        // Path must be equal to the output Path of the first job.
        Path path = new Path("output_task2/out1/part-r-00000");
        FileSystem fs = path.getFileSystem(conf_aux);

        // Open the resulting file.
        BufferedReader inputStream = new BufferedReader(new InputStreamReader(fs.open(path)));

        String lineJustFetched = null;
        String[] wordsArray = null;
        while (true) {
            lineJustFetched = inputStream.readLine();

            // Loop while there are lines left to read.
            if (lineJustFetched == null) {
                break;
            } else {

                // Split input by <tab> and identify fields.
                wordsArray = lineJustFetched.split("\t");

                String word = wordsArray[1];
                String word_clean = stripAccents(wordsArray[1]);

                // Put in our map <Decade, Word>
                hmap.put(Integer.parseInt(wordsArray[0]), word);
                String path2 = "";

                // Generate the path to the corresponding 2gram file based on curren 1gram result.
                if (word.length() > 1) {
                    path2 = "input_2gram/googlebooks-spa-all-2gram-20120701-" + Character.toLowerCase(word_clean.charAt(0)) + Character.toLowerCase(word_clean.charAt(1));
                } else {
                    path2 = "input_2gram/googlebooks-spa-all-2gram-20120701-" + Character.toLowerCase(word_clean.charAt(0)) + "_";
                }

                // If it's not yet in our list, add it.
                if (!filePaths.contains(path2))
                    filePaths.add(path2);
            }
        }

        // Close the file.
        fs.close();

        // Create a new configuration for the Hadoop job.
        Configuration conf2 = new Configuration();

        // Set configuration <Key,Value> pairs for setting <Decade,1gram> results from first part.
        // These will be retrieved by the second mapper.
        for (Map.Entry<Integer, String> entry : hmap.entrySet()) {
            Integer key = entry.getKey();
            String value = entry.getValue();

            // Write to conf2 the <Key,Value> pairs.
            conf2.set(key.toString(), value);
        }

        // Create a new job for the configuration instance, and give it a name.
        Job job2 = Job.getInstance(conf2, "Task 2, part II");

        // Select which JAR is going to be executed.
        job2.setJarByClass(Task2MapReduce.class);

        // Select our mapper class.
        job2.setMapperClass(Task2OutputMapper.class);

        // Select our reducer class.
        job2.setReducerClass(Task2BigramReducerToDecades.class);

        // Configure the output <key,value> types for the mapper class.
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        // Configure the output <key,value> types for the reducer class.
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        // Set our input files, based in the ones we composed earlier.
        for (String pathX : filePaths) {
            MultipleInputs.addInputPath(job2, new Path(pathX), TextInputFormat.class);
        }

        // Select our output path. We'll use output_task2/out2.
        FileOutputFormat.setOutputPath(job2, new Path("output_task2", "out2"));


        // Wait for job completion.
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}
