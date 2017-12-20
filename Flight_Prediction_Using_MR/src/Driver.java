import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author Ankita
 */
public class Driver {

    public static void predictionJob(Configuration conf, String input){
        Job j = Job.getInstance(conf,"Prediction Job");
        j.setJarByClass(ActiveAirportAirlines.class);
        j.setMapperClass(ActiveAirportAirlines.ActiveAirportAirlinesMapper.class);
        j.setPartitionerClass(ActiveAirportAirlines.ActivePartitioner.class);
        j.setCombinerClass(ActiveAirportAirlines.ActiveAirportAirlinesReducer.class);
        j.setReducerClass(ActiveAirportAirlines.ActiveAirportAirlinesReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        j.setInputFormatClass(NonSplitableTextInputFormat.class);
        j.getConfiguration().set("input",input);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1] + "/phase1output"));
        j.waitForCompletion(true);
    }

    public static void main(String[] args){
        Configuration conf = new Configuration();
        String input = "(2001, 09, 11, DEN, DCA)";
        predictionJob(conf,input);
    }
}
