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
import java.util.SortedSet;
import java.util.TreeSet;

public class TopN {

    public static class CustomMap extends Mapper<Object, Text, Text, IntWritable> {
        private Text data = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            data.set(value.toString().split(",")[1]);
            context.write(data, new IntWritable(1));
        }
    }

    public static class CustomReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private SortedSet<Integer> set = new TreeSet<>();
        Text key = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            set.add(Integer.valueOf(key.toString()));
            if (set.size() > context.getConfiguration().getInt("TopN", 10))
                set.remove(set.first());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int i = 0;
            for (Integer num : set) {
                i++;
                key.set(String.valueOf(num));
                context.write(key, new IntWritable(i));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        conf.setInt("TopN", 5);
        // Windows上开发必须配置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:/hadoop/hadoop-3.1.4");
        // 必须加载hadoop.dll动态链接库
        System.load("D:/hadoop/hadoop-3.1.4/bin/hadoop.dll");
        Job job = Job.getInstance(conf, "TopN");
        job.setJarByClass(TopN.class);
        job.setMapperClass(CustomMap.class);
        job.setCombinerClass(CustomReducer.class);
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        String args_0 = "hdfs://master:9000/input/topn.txt";
        String args_1 = "hdfs://master:9000/out/topn-out/";
        // 输入路径
        FileInputFormat.addInputPath(job, new Path(args_0));
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(args_1));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
