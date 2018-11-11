import org.apache.hadoop.conf.Configuration;
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
import java.io.FileReader;
import java.io.IOException;
import java.text.Normalizer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Task2MapReduce {

    public static class Task2MapperInputFile
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text word_appearances_year = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\\s+");

            if (values.length > 3) {
                int decade_read = Integer.parseInt(values[1]) / 10;
                if (decade_read > 189 && decade_read < 200) {
                    decade.set(decade_read * 10);
                    word_appearances_year.set(values[0] + "\t" + values[2]);
                    context.write(decade, word_appearances_year);
                }
            }
        }
    }

    public static class Task2OutputMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text word_appearances_year = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\\s+");

            if (values.length > 4) {
                int decade_read = Integer.parseInt(values[2]) / 10;
                if (decade_read > 189 && decade_read < 200) {
                    decade.set(decade_read * 10);
                    String interest = context.getConfiguration().get(decade.toString());
                    if(interest.length()>1){
                        if(Character.toLowerCase(values[0].charAt(0)) == Character.toLowerCase(interest.charAt(0))
                                && Character.toLowerCase(values[1].charAt(0)) == Character.toLowerCase(interest.charAt(1)))
                        {
                            word_appearances_year.set(values[0] + "\t" + values[1] + values[3]);
                            context.write(decade, word_appearances_year);
                        }
                    }else{
                        if(Character.toLowerCase(values[0].charAt(0)) == Character.toLowerCase(interest.charAt(0)))
                        {
                            word_appearances_year.set(values[0] + "\t" + values[1] + "\t" + values[3]);
                            context.write(decade, word_appearances_year);
                        }
                    }
                }
            }
        }

    }

    public static class Task2ReducerToDecades
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text highest_text_data = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            AtomicInteger highest_score = new AtomicInteger();
            AtomicReference<String> highest_string = new AtomicReference<>("");
            TreeMap<String, Integer> word_appearances = new TreeMap<>();
            for (Text val : values) {
                String[] value = val.toString().split("\\s+");
                String word = value[0];
                int appearances = Integer.parseInt(value[1]);
                if (word_appearances.containsKey(word)) {
                    int aggregated = word_appearances.get(word);
                    word_appearances.replace(word, aggregated + appearances);
                    if (aggregated + appearances > highest_score.get()) {
                        highest_score.set(aggregated + appearances);
                        highest_string.set(word);
                    }
                } else {
                    word_appearances.put(word, appearances);
                    if (appearances > highest_score.get()) {
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


    public static class Task2BigramReducerToDecades
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private IntWritable decade = new IntWritable();
        private Text highest_text_data = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            AtomicInteger highest_score = new AtomicInteger();
            AtomicReference<String> highest_string = new AtomicReference<>("");
            TreeMap<String, Integer> word_appearances = new TreeMap<>();
            for (Text val : values) {
                String[] value = val.toString().split("\\s+");
                String bigram = value[0] + "\t" + value[1];
                int appearances = Integer.parseInt(value[2]);
                if (word_appearances.containsKey(bigram)) {
                    int aggregated = word_appearances.get(bigram);
                    word_appearances.replace(bigram, aggregated + appearances);
                    if (aggregated + appearances > highest_score.get()) {
                        highest_score.set(aggregated + appearances);
                        highest_string.set(bigram);
                    }
                } else {
                    word_appearances.put(bigram, appearances);
                    if (appearances > highest_score.get()) {
                        highest_score.set(appearances);
                        highest_string.set(bigram);
                    }
                }
            }

            decade.set(Integer.parseInt(key.toString()));
            highest_text_data.set(highest_string + "\t" + highest_score);
            context.write(decade, highest_text_data);
        }
    }

    public static String stripAccents(String s) {
        s = Normalizer.normalize(s, Normalizer.Form.NFD);
        s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        return s;
    }

    public static void main(String[] args) throws Exception {

        Path out = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Unigrams per decade");
        job.setJarByClass(Task2MapReduce.class);
        job.setMapperClass(Task2MapperInputFile.class);
        job.setReducerClass(Task2ReducerToDecades.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(out, "out1"));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        HashMap<Integer, String> hmap = new HashMap<Integer, String>();
        List<String> filePaths = new ArrayList<>();
        try {
            BufferedReader buf = new BufferedReader(new FileReader("output" + "/out1/part-r-00000"));
            ArrayList<String> words = new ArrayList<>();
            String lineJustFetched = null;
            String[] wordsArray = null;
            while (true) {
                lineJustFetched = buf.readLine();
                if (lineJustFetched == null) {
                    break;
                } else {
                    words.add(lineJustFetched);
                    wordsArray = lineJustFetched.split("\t");

                    String word = wordsArray[1];
                    String word_clean = stripAccents(wordsArray[1]);
                    hmap.put(Integer.parseInt(wordsArray[0]), word);
                    String path = "";
                    if (word.length() > 1) {
                        path = "input_2gram/googlebooks-spa-all-2gram-20120701-" + word_clean.charAt(0) + word_clean.charAt(1);
                    } else {
                        path = "input_2gram/googlebooks-spa-all-2gram-20120701-" + word_clean.charAt(0) + "_";
                    }
                    if (!filePaths.contains(path))
                        filePaths.add(path);
                }
            }
            buf.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        Configuration conf2 = new Configuration();
        for (Map.Entry<Integer, String> entry : hmap.entrySet()) {
            Integer key = entry.getKey();
            String value = entry.getValue();
            conf2.set(key.toString(), value);
        }

        Job job2 = Job.getInstance(conf2, "Bigrams per decade");
        job2.setJarByClass(Task2MapReduce.class);
        job2.setMapperClass(Task2OutputMapper.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setReducerClass(Task2BigramReducerToDecades.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        for (String path : filePaths) {
            MultipleInputs.addInputPath(job2, new Path(path), TextInputFormat.class);
        }

//        FileInputFormat.addInputPath(job2, new Path(out, "input_2gram"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

    }
}
