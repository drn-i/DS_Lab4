package edu.hassan;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0, min = Double.MAX_VALUE, max = Double.MIN_VALUE, sumOfSquares = 0;
        int count = 0;

        for (DoubleWritable val : values) {
            double price = val.get();
            sum += price;
            min = Math.min(min, price);
            max = Math.max(max, price);
            sumOfSquares += price * price;
            count++;
        }
        context.write(new Text("Count"), new DoubleWritable(count));
        context.write(new Text("Sum"), new DoubleWritable(sum));
        context.write(new Text("Min"), new DoubleWritable(min));
        context.write(new Text("Max"), new DoubleWritable(max));
        context.write(new Text("SumOfSquares"), new DoubleWritable(sumOfSquares));
    }
}
