import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class Map extends Mapper<Object,Text,Text,Text> {
        public void map(Object key, Text value,Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> F = new HashMap();
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName().split("\\.")[0];
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String term = itr.nextToken();
                if(F.containsKey(term)){
                    F.put(term, F.get(term)+1);
                } else {
                    F.put(term, 1);
                }
            }

            for(String term: F.keySet()) {
                context.write(new Text(term+","+fileName), new Text(F.get(term)+""));
            }
        }
    }


    public static class MyPartitioner extends Partitioner<Text,Text > {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String term = key.toString().split(",")[0]; //<term, docid>=>term
            return (term.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>
    {
        String termPre = "";
        String filenamePre = "";
        Integer samefilesum = 0;
        Integer totalsum = 0;
        Integer filesnum = 1;
        StringBuilder P = new StringBuilder();

        @Override
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            while (itr.hasNext()) {
                String term = key.toString().split(",")[0];
                String filename = key.toString().split(",")[1];
                if (!term.equals(termPre) && !termPre.equals("")) {
                    //遇到一个新的词，把缓存的上一个filename和samefilesum加入P
                    P = P.append(filenamePre + ":" +samefilesum+";");
                    totalsum += samefilesum;
                    samefilesum = new Integer(itr.next().toString());
                    filenamePre = filename;
                    //把上一个词和对应的payload加入context
                    context.write(new Text(termPre), new Text(String.format("%.2f,   ", 1.0*totalsum/filesnum) + P.toString()));
                    P = new StringBuilder();
                    totalsum = 0;
                    filesnum = 1;
                } else {
                    if(filenamePre.equals("")){  //第一个filename，初始化一下samefilesum和filenamePre
                        samefilesum = new Integer(itr.next().toString());
                        filenamePre = filename;
                    } else if(filename.equals(filenamePre)){  //如果是一个filename，那么累加
                        samefilesum = samefilesum + new Integer(itr.next().toString());
                    } else {  //遇到一个新的filename，统计上一个filename的词频，更新filenamePre和samefilesum
                        filesnum += 1;
                        P = P.append(filenamePre + ":" +samefilesum+";");
                        totalsum += samefilesum;
                        samefilesum = new Integer(itr.next().toString());
                        filenamePre = filename;

                    }
                }
                termPre = term;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //遇到一个新的词，把缓存的上一个filename和samefilesum加入P
            P = P.append(filenamePre + ":" +samefilesum+";");
            totalsum += samefilesum;
            //把上一个词和对应的payload加入context
            context.write(new Text(termPre), new Text(String.format("%.2f",1.0*totalsum/filesnum) + P.toString()));
            super.cleanup(context);
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();//配置对象

//    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//    if(otherArgs.length != 2){
//      System.err.println(otherArgs.length);
//      System.err.print("Usage: Lab3.jar <in> <out>\n");
//      System.exit(2);
//    }

        Job job = new Job(conf,"InvertedIndex");//新建job
        job.setJarByClass(InvertedIndex.class);//job类

        job.setMapperClass(Map.class);//map设置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(MyPartitioner.class);

        job.setReducerClass(Reduce.class);//reduce设置
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));//路径设置
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//    FileInputFormat.addInputPath(job, new Path(args[0]));//路径设置
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}