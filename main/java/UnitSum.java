import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * UnitSum takes the last job UnitMultiplication's output, <distNode>\t<subPageRank>, as input and sum them up to
 * get the final PageRank for each node in the graph.
 */
public class UnitSum {

    /**
     * PassMapper reads the input and passes it as key-value pair (distNode: subPageRank) to the reducer.
     */
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           String[] pageAndSubPR = value.toString().split("\t");
            double subPR = Double.parseDouble(pageAndSubPR[1]);
            context.write(new Text(pageAndSubPR[0]), new DoubleWritable(subPR));
        }
    }

    /**
     * BetaMapper takes the previous PageRank file as input, and pass key-value pairs (node: previous pagerank *
     * (1 - damping)) to the reducer for final pagerank computation.
     */
    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        float damping;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            damping = conf.getFloat("damping", 0.85f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pageRank = value.toString().split("\t");
            double betaRank = Double.parseDouble(pageRank[1]) * (1 - damping);
            context.write(new Text(pageRank[0]), new DoubleWritable(betaRank));
        }
    }

    /**
     * SumReducer sum up all the subPageRanks and previous pagerank * (1 - damping) to generate the final PageRank for
     * the distNode (key).
     *
     */
    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            for (DoubleWritable value: values) {
                sum += value.get();
            }
            DecimalFormat df = new DecimalFormat("#.000000");
            sum = Double.valueOf(df.format(sum));
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("damping", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);

        ChainMapper.addMapper(job, PassMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);
        ChainMapper.addMapper(job, BetaMapper.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf);

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BetaMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
