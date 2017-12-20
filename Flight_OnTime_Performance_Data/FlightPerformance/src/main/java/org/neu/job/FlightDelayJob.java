package org.neu.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.neu.combiner.FlightDelayCombiner;
import org.neu.data.FlightDataWritable;
import org.neu.data.FlightDelayCompositeKey;
import org.neu.mapper.FlightDelayMapper;
import org.neu.reducer.FlightDelayReducer;

/**
 * FlightDelayJob: fetch the flight information, and output the most busy airport/airline with delay time.
 * @author Ankita
 */
public class FlightDelayJob extends Configured implements Tool {

  private static String OUTPUT_SEPARATOR = "mapreduce.output.textoutputformat.separator";

  @Override
  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "FlightDelayJob");
    job.setJarByClass(this.getClass());
    job.getConfiguration().set(OUTPUT_SEPARATOR, ",");

    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3] + "/flightDelay"));

    MultipleOutputs.addNamedOutput(job, "flightDelayAirportData", TextOutputFormat.class,
        FlightDelayCompositeKey.class, FloatWritable.class);
    MultipleOutputs.addNamedOutput(job, "flightDelayAirlineData", TextOutputFormat.class,
        FlightDelayCompositeKey.class, FloatWritable.class);
    MultipleOutputs.addNamedOutput(job, "mostBusyAirportData", TextOutputFormat.class, Text.class,
        Text.class);
    MultipleOutputs.addNamedOutput(job, "mostBusyAirlineData", TextOutputFormat.class, Text.class,
        Text.class);

    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

    job.setMapperClass(FlightDelayMapper.class);
    job.setCombinerClass(FlightDelayCombiner.class);
    job.setReducerClass(FlightDelayReducer.class);
    job.setNumReduceTasks(1);

    job.setMapOutputKeyClass(FlightDelayCompositeKey.class);
    job.setMapOutputValueClass(FlightDataWritable.class);

    job.setOutputKeyClass(FlightDelayCompositeKey.class);
    job.setOutputValueClass(FlightDataWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
