package com.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InterestDayAverage {

    public static class InterestMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private IntWritable intervalKey = new IntWritable();
        private DoubleWritable flowValue = new DoubleWritable();
        private static final int INFLOW_COLUMN = 3;
        private static final int OUTFLOW_COLUMN = 4;
        private static final int YIELD_COLUMN = 2;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            
            try {
                double yield = Double.parseDouble(fields[YIELD_COLUMN]);
                double inflow = Double.parseDouble(fields[INFLOW_COLUMN]);
                double outflow = Double.parseDouble(fields[OUTFLOW_COLUMN]);

                int interval = getInterval(yield);
                intervalKey.set(interval);

                flowValue.set(inflow);
                context.write(intervalKey, new DoubleWritable(inflow));

                flowValue.set(-outflow); 
                context.write(intervalKey, flowValue);

            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            }
        }

        private int getInterval(double yield) {
            if (yield >= 4 && yield < 4.5) return 1;
            else if (yield >= 4.5 && yield < 5) return 2;
            else if (yield >= 5 && yield < 5.5) return 3;
            else if (yield >= 5.5 && yield < 6) return 4;
            else return 5;
        }
    }

    public static class InterestReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double totalInflow = 0;
            double totalOutflow = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                double flow = val.get();
                if (flow > 0) {
                    totalInflow += flow;
                } else {
                    totalOutflow += -flow; 
                }
                count++;
            }

            if (count > 0) {
                double avgInflow = totalInflow / count;
                double avgOutflow = totalOutflow / count;
                context.write(key, new Text(String.format("%.0f,%.0f", avgInflow, avgOutflow)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Interest Day Average");
        job.setJarByClass(InterestDayAverage.class);
        job.setMapperClass(InterestMapper.class);
        job.setReducerClass(InterestReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

