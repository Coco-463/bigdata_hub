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
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class ActiveDay {

    public static class ActiveDayMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text userId = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");

            if (columns.length < 9 || columns[0].equals("user_id")) {
                return; 
            }

            try {
                userId.set(columns[0]);
                double directPurchaseAmt = Double.parseDouble(columns[5]);
                double totalRedeemAmt = Double.parseDouble(columns[8]);

                // 判断活跃条件
                if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
                    context.write(userId, one); 
                }
            } catch (NumberFormatException e) {
                System.err.println("Error parsing numeric values in line: " + value.toString());
            }
        }
    }

    public static class ActiveDayReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> userActiveDaysMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int activeDays = 0;
            for (IntWritable val : values) {
                activeDays += val.get();
            }
            userActiveDaysMap.put(key.toString(), activeDays); // 存储用户ID和活跃天数
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 将 Map 中的用户数据按活跃天数降序排序
            ArrayList<Map.Entry<String, Integer>> sortedList = new ArrayList<>(userActiveDaysMap.entrySet());
            Collections.sort(sortedList, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue()); 
                }
            });

            // 输出排序后的结果
            for (Map.Entry<String, Integer> entry : sortedList) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Active Day Count and Sort");
        job.setJarByClass(ActiveDay.class);
        job.setMapperClass(ActiveDayMapper.class);
        job.setReducerClass(ActiveDayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
