package edu.hassan;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private double sum = 0, min = Double.MAX_VALUE, max = Double.MIN_VALUE, sumOfSquares = 0;
    private int count = 0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context){

        for (DoubleWritable val : values) {
            double value = val.get();

            switch (key.toString()) {
                case "Count":
                    count += (int) value;
                    break;
                case "Sum":
                    sum += value;
                    break;
                case "Min":
                    min = Math.min(min, value);
                    break;
                case "Max":
                    max = Math.max(max, value);
                    break;
                case "SumOfSquares":
                    sumOfSquares += value;
                    break;
            }
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double mean = sum / count;
        double variance = (sumOfSquares / count) - (mean * mean);
        double stdDev = Math.sqrt(variance);

        context.write(new Text("Count"), new DoubleWritable(count));
        context.write(new Text("Sum"), new DoubleWritable(sum));
        context.write(new Text("Min"), new DoubleWritable(min));
        context.write(new Text("Max"), new DoubleWritable(max));
        context.write(new Text("Mean"), new DoubleWritable(mean));
        context.write(new Text("Standard Deviation"), new DoubleWritable(stdDev));
    }
}

