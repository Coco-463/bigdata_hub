package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.*;

public class HighFrequencyWords {

    private static Set<String> stopWords = new HashSet<>();

    // Mapper 类
    public static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获取 HDFS 文件系统
            Configuration configuration = context.getConfiguration();
            FileSystem fs = FileSystem.get(configuration);
            Path stopWordsPath = new Path("hdfs://h01:9000/input/stop-word-list.txt"); // 修改为你的 HDFS 路径

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");
            if (columns.length > 1) {
                String headline = columns[1].toLowerCase().replaceAll("[^a-zA-Z ]", " "); // 清理标点符号
                String[] words = headline.split("\\s+");
                for (String w : words) {
                    if (!stopWords.contains(w) && !w.isEmpty()) {
                        word.set(w);
                        context.write(word, ONE);
                    }
                }
            }
        }
    }

    // Reducer 类
    public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> wordCountMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            wordCountMap.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 按出现次数排序
            List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(wordCountMap.entrySet());
            sortedEntries.sort((a, b) -> b.getValue().compareTo(a.getValue())); // 降序排序

            int rank = 1;
            for (Map.Entry<String, Integer> entry : sortedEntries) {
                if (rank > 100) break; // 只输出前100个
                context.write(new Text(rank + " : " + entry.getKey() + " , " + entry.getValue()), null);
                rank++;
            }
        }
    }

    // Driver 主类
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "high frequency words");

        job.setJarByClass(HighFrequencyWords.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
