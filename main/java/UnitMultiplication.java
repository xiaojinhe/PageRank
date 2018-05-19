import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * UnitMultiplication uses two mappers to parse the two inputs, transitionMatrix and pageRank0, into key-value pairs,
 * and uses a reducer to do the matrix unit cell multiplication to get the subPageRank unit for each page, and generate
 * the key-value pairs (distNode: subPageRank unit from one source node) as output for later job.
 */
public class UnitMultiplication {

    /**
     * TransitionMapper takes transitionMatrix file as input.
     * Input format for transition matrix:
     * <sourceNode>\t<distNode1>,<distNode2>,<distNode3>...
     *
     * Output format: transition matrix unit with the format: <sourceNode>\t<distNode=weightPercentage>
     */
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fromAndTo = value.toString().trim().split("\t");
            if (fromAndTo.length < 2 || fromAndTo[1].trim().equals("")) {
                return;
            }

            String[] distNodes = fromAndTo[1].split(",");
            double prob = (double) 1 / distNodes.length;
            for (String s : distNodes) {
                context.write(new Text(fromAndTo[0]), new Text(s + "=" + prob));
            }
        }
    }

    /**
     * PRMapper takes pageRank0 generated from last job as input, and write the key-value pair to the reducer.
     * Input format: <sourceNode>\t<pagerank0>
     */
    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] pageAndPR = value.toString().trim().split("\t");
            context.write(new Text(pageAndPR[0]), new Text(pageAndPR[1]));
        }
    }

    /**
     * MultiplicationReducer do the matrix unit multiplication for each node, and generate output as key-value pair:
     * distNode: subPageRank unit from one source node, for next job to sum-up all the subPageRank unit for the distNode.
     *
     * Damping factor can also be passed in or used the default value of 0.85.
     *
     * input key = sourceNode
     * input value = <distNode1=weightPercentage1, distNode2=weightPercentage2, ..., pageRank>
     */
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        float damping;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            damping = conf.getFloat("damping", 0.85f);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> distNodeAndProb = new ArrayList<String>();
            double pageRank = 0;
            for (Text value : values) {
                String s = value.toString();
                if (s.contains("=")) {
                    distNodeAndProb.add(s);
                } else {
                    pageRank = Double.parseDouble(s);
                }
            }

            for (String s : distNodeAndProb) {
                String[] nodeAndProb = s.split("=");
                double prob = Double.parseDouble(nodeAndProb[1]);
                double subPR = prob * pageRank * damping;
                context.write(new Text(nodeAndProb[0]), new Text(String.valueOf(subPR)));
            }
        }
    }

    /**
     * Main method to set up the job.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("damping", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
