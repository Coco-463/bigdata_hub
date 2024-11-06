package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FundsInOut {

    public static class Amounts implements Writable {
        private double purchase = 0.0;
        private double redeem = 0.0;

        // 无参构造函数
        public Amounts() {}

        public void set(double purchase, double redeem) {
            this.purchase = purchase;
            this.redeem = redeem;
        }

        public double getPurchase() {
            return purchase;
        }

        public double getRedeem() {
            return redeem;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(purchase);
            out.writeDouble(redeem);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            purchase = in.readDouble();
            redeem = in.readDouble();
        }

        @Override
        public String toString() {
            return purchase + "," + redeem;
        }
    }

    public static class FundsMapper extends Mapper<LongWritable, Text, Text, Amounts> {
        private Text date = new Text();
        private Amounts amounts = new Amounts();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length >= 9) {
                date.set(fields[1]); // report_date
                double purchase = parseDouble(fields[4]); // total_purchase_amt
                double redeem = parseDouble(fields[8]); // total_redeem_amt

                amounts = new Amounts(); // 每次使用新对象
                amounts.set(purchase, redeem);
                context.write(date, amounts);
            }
        }

        private double parseDouble(String value) {
            if (value == null || value.trim().isEmpty()) {
                return 0.0;
            }
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return 0.0; // 如果解析失败，返回 0.0
            }
        }
    }

    public static class FundsReducer extends Reducer<Text, Amounts, Text, Text> {
        private double totalPurchase;
        private double totalRedeem;

        @Override
        protected void reduce(Text key, Iterable<Amounts> values, Context context) throws IOException, InterruptedException {
            totalPurchase = 0.0;
            totalRedeem = 0.0;

            for (Amounts amounts : values) {
                totalPurchase += amounts.getPurchase();
                totalRedeem += amounts.getRedeem();
            }

            context.write(key, new Text(totalPurchase + "," + totalRedeem));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: FundsInOut <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Funds In Out");
        job.setJarByClass(FundsInOut.class);
        job.setMapperClass(FundsMapper.class);
        job.setReducerClass(FundsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Amounts.class);
        job.setMapOutputValueClass(Amounts.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
