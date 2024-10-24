package com.example;

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
import java.util.*;

public class StockCount {

    // Mapper 类
    public static class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private Text stockCode = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");
            if (columns.length == 4 && !columns[3].equals("stock")) {
                stockCode.set(columns[3].trim());
                context.write(stockCode, ONE);
            }
        }
    }

    // Reducer 类
    public static class StockReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<Integer, List<String>> occurrencesMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable count : values) {
                total += count.get();
            }
            occurrencesMap.computeIfAbsent(total, k -> new ArrayList<>()).add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Integer, List<String>>> sortedEntries = new ArrayList<>(occurrencesMap.entrySet());
            sortedEntries.sort((a, b) -> b.getKey().compareTo(a.getKey())); // 按次数降序排序
            
            int rank = 1;
            for (Map.Entry<Integer, List<String>> entry : sortedEntries) {
                int count = entry.getKey();
                List<String> stocks = entry.getValue();
                for (String stock : stocks) {
                    context.write(new Text(rank + " : " + stock + " , " + count), null);
                    rank++;
                }
            }
        }
    }

    // Driver 主类
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "stock count");

        job.setJarByClass(StockCount.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
