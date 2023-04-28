import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TipsCleaningReducer
    extends Reducer<NullWritable, Text, NullWritable, Text> {
  
  public void reduce(NullWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    context.write(key, value);
  }
}
