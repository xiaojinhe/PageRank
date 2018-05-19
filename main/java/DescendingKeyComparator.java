import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * DescendingKeyComparator is used to generate final PageRank in descending order. It overrides the compare method.
 */
public class DescendingKeyComparator extends WritableComparator {

    protected DescendingKeyComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable d1 = (DoubleWritable) w1;
        DoubleWritable d2 = (DoubleWritable) w2;
        return -1 * d1.compareTo(d2);
    }
}
