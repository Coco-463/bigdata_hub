package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DayAverage {

    public static class DayAverageMapper extends Mapper<Object, Text, Text, Text> {
        private Text weekday = new Text();
        private Text flowAmounts = new Text();
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        private final String[] weekDays = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");
            if (columns.length != 2) {
                return; 
            }

            try {
                String dateStr = columns[0];
                Date date = dateFormat.parse(dateStr);
                
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);
                int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK); // 获取星期几
                String weekDayStr = weekDays[dayOfWeek - 1]; 
                
                weekday.set(weekDayStr);
                flowAmounts.set(columns[1]);
                
                context.write(weekday, flowAmounts); 
            } catch (ParseException e) {
                System.err.println("Error parsing date in line: " + value.toString());
            }
        }
    }

    public static class DayAverageReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, double[]> weekdayMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalInflow = 0;
            double totalOutflow = 0;
            int count = 0;

            for (Text val : values) {
                String[] amounts = val.toString().split(",");
                if (amounts.length == 2) {
                    try {
                        double inflow = Double.parseDouble(amounts[0]);
                        double outflow = Double.parseDouble(amounts[1]);
                        totalInflow += inflow;
                        totalOutflow += outflow;
                        count++;
                    } catch (NumberFormatException e) {
                        System.err.println("Error parsing inflow/outflow values in line: " + val.toString());
                    }
                }
            }

            if (count > 0) {
                double avgInflow = totalInflow / count;
                double avgOutflow = totalOutflow / count;
                weekdayMap.put(key.toString(), new double[]{avgInflow, avgOutflow});
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, double[]>> sortedList = new ArrayList<>(weekdayMap.entrySet());// 对 weekdayMap 按平均资金流入量降序排序
            sortedList.sort(new Comparator<Map.Entry<String, double[]>>() {
                @Override
                public int compare(Map.Entry<String, double[]> o1, Map.Entry<String, double[]> o2) {
                    return Double.compare(o2.getValue()[0], o1.getValue()[0]); // 按资金流入量降序排序
                }
            });

            // 输出排序后的结果
            for (Map.Entry<String, double[]> entry : sortedList) {
                String weekday = entry.getKey();
                double avgInflow = entry.getValue()[0];
                double avgOutflow = entry.getValue()[1];
                context.write(new Text(weekday), new Text(String.format("%.0f,%.0f", avgInflow, avgOutflow)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Day Average Calculation and Sort");
        job.setJarByClass(DayAverage.class);
        job.setMapperClass(DayAverageMapper.class);
        job.setReducerClass(DayAverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
