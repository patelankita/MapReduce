package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * FlightDataWritable: the pair values of output of map, includes delay and count
 * @author Ankita
 */
public class FlightDataWritable implements Writable {
	// the value part of map output pair <key, value>

	FloatWritable delay;
	IntWritable count;

	public FlightDataWritable() {
		this.delay = new FloatWritable();
		this.count = new IntWritable();
	}

	public FlightDataWritable(FloatWritable delay, IntWritable count) {

		this.delay = delay;
		this.count = count;
	}

	public FlightDataWritable(Float delay, Integer count) {
		this.delay = new FloatWritable(delay);
		this.count = new IntWritable(count);
	}

	public FloatWritable getDelay() {
		return delay;
	}

	public void setDelay(FloatWritable delay) {
		this.delay = delay;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		delay.write(dataOutput);
		count.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		delay.readFields(dataInput);
		count.readFields(dataInput);
	}

	@Override
	public String toString() {
		return delay.toString() + "," + count.toString();
	}
}
