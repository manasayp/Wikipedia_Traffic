import org.apache.hadoop.io.LongWritable;

public class sortComparator extends WritableComparator {

 protected sortComparator() {
  super(LongWritable.class, true);
  // TODO Auto-generated constructor stub
 }

 @Override
 public int compare(WritableComparable o1, WritableComparable o2) {
  LongWritable k1 = (LongWritable) o1;
  LongWritable k2 = (LongWritable) o2;
  int cmp = k1.compareTo(k2);
  return -1 * cmp;
 }

}