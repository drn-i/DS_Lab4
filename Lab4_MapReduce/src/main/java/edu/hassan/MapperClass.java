package edu.hassan;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final static Text word = new Text("price");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double price = Double.parseDouble(value.toString().trim());
            context.write(word, new DoubleWritable(price));

    }
}