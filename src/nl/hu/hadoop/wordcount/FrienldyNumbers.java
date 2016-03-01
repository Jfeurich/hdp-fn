package nl.hu.hadoop.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class FrienldyNumbers {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(FrienldyNumbers.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FriendlyNumberMapper.class);
		job.setReducerClass(FriendlyNumberReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
	}
}

/*
*  voor ieder getal uit de lijst, tel alle delers van het getal bij elkaar op
*  sla de getallen bij elkaar op in het formaat delersom + getal
*  voor alle getallen waarbij de som aan elkaar gelijk is, check of getal == delersom
*
*  map: in plaats van het getal, is de som de key. de value is 1 van de bevriende getallen.
*  reduce: controleer of voor een key: key -val1 == val2
* */
class FriendlyNumberMapper extends Mapper<LongWritable,Text,Integer,Integer> {
    public void map(LongWritable Key, String value, Context context) throws IOException, InterruptedException {
        String[] getallen = value.split("\\r?\\n");
        for (String s : getallen){
            context.write(diviserSum(Integer.parseInt(s)),Integer.parseInt(s));
    }
    public Integer diviserSum(Integer input){
        int maxD = (int)Math.sqrt(input);
        int sum=1;
        for(int i = 2; i <= maxD; i++)
        {
            if(input % i == 0)
            {
                int d = input/i;
                if(d!=i)
                    sum = sum+i;
            }
        }
        return sum;
    }
}
class FriendlyNumberReducer extends Reducer<Integer,Integer,Integer,String> {
    public void reduce(Integer key, Iterable<Integer> values, Context context) throws IOException,InterruptedException {
        Iterator<Integer> getValues = values.iterator();
        while(getValues.hasNext()){
            int value = getValues.next();
            int value2 = getValues.next();
            if ((key - value) == value2) {
                context.write(new IntWritable(value), new IntWritable(value2));
            }
        }
    }

}

