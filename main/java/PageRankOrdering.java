import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageRankOrdering {

    /**
     * The class takes the final PageRank file after iterations and order it in descending order (mapper only), by
     * setting sort comparator class to DescendingKeyComparator.class.
     *
     * Input format: <node>\t<pageRank>
     *
     * Output format: <pageRank>\t<node> in descending order
     */
    public static class PageRankOrderingMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pair = value.toString().trim().split("\t");
            String page = pair[0];
            double pageRank = Double.parseDouble(pair[1]);
            context.write(new DoubleWritable(pageRank), new Text(page));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRankOrdering.class);

        job.setMapperClass(PageRankOrderingMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setSortComparatorClass(DescendingKeyComparator.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

