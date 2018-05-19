import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * PageRelationParser takes input format: <sourceNode> \t <distNode> (represents an edge from sourceNode to distNode)
 * in the graph, and parse it into two files: transitionMatrix and pageRank0, which will be the input files for
 * PageRanking iteration in the later two jobs.
 *
 * Datasets can be downloaded from http://snap.stanford.edu/data/#web.
 */
public class PageRelationParser {

    /**
     * PageRelationParser mapper will parse every line of the input network graph and create key-value pairs that
     * map the source page to its outgoing pages.
     * At the same time, we set each node's initial pagerank as 1 and output it as pagerank0.
     *
     * Input format: <sourceNode> \t <distNode>; which represents an edge from sourceNode to distNode.
     *
     * We also need to ignore comment lines that start with '#'.
     */
    public static class PageRelationMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.charAt(0) != '#') {
                String[] edge = value.toString().trim().split("\t");
                String source = edge[0];
                String dist = edge[1];
                context.write(new Text(source), new Text(dist));
            }
        }
    }

    /**
     * PageRelationParser reducer will generate two output files with defined names and save them into two defined
     * directories respectively, using MultipleOutputs class.
     *
     * PageRelationParser reducer will put each source node and all its outgoing nodes together as a key-values
     * pair. This output is called transition matrix.
     *
     * Output format for transition matrix:
     * <sourceNode>\t<distNode1>,<distNode2>,<distNode3>...
     *
     * And it will generate another output the initial pagerank0. The initial pagerank for each page is the same, it
     * can be set to 1 or 1 / total unique nodes in the graph.
     *
     * Output format for pagerank0:
     * <sourceNode>\t<pagerank0>
     */
    public static class PageRelationReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs out;

        @Override
        public void setup(Context context) {
            out = new MultipleOutputs(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            StringBuilder transition = new StringBuilder();

            boolean flag = false;
            for (Text value : values) {
                if (flag) {
                    transition.append(",");
                }
                transition.append(value.toString());
                flag = true;
            }
            double pr0 = (double) 1;
            out.write("transition", key, new Text(transition.toString()), "transition/transMatrix");
            out.write("pagerank0", key, new Text(String.valueOf(pr0)), "pagerank0/pr");
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }

    /**
     * Main method to set up the PageRelationParser job.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRelationParser.class);

        job.setMapperClass(PageRelationMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        MultipleOutputs.addNamedOutput(job, "transition", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "pagerank0", TextOutputFormat.class, Text.class, Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setReducerClass(PageRelationReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
