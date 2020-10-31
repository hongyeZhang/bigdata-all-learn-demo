package com.zhq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author: ZHQ
 * @Date: 2020/10/30
 */
public class WordCount {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        // conf.set("mapreduce.app-submission.cross-platform", "true");
        // Windows上开发必须配置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:/hadoop/hadoop-3.1.4");
        // 必须加载hadoop.dll动态链接库
        System.load("D:/hadoop/hadoop-3.1.4/bin/hadoop.dll");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        String args_0 = "hdfs://master:9000/input/payment.txt";
        String args_1 = "hdfs://master:9000/out/payment-java/";
        // 输入路径
        FileInputFormat.addInputPath(job, new Path(args_0));
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(args_1));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

