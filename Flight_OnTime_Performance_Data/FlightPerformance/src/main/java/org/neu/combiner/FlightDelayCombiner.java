package org.neu.combiner;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightDelayCompositeKey;
import org.neu.data.FlightDataWritable;

/**
 * FlightDelayCombiner: combine the number of flight and total delay time for same key
 * @author Ankita
 */
public class FlightDelayCombiner extends
Reducer<FlightDelayCompositeKey, FlightDataWritable, FlightDelayCompositeKey, FlightDataWritable> {

	@Override
	public void reduce(FlightDelayCompositeKey key, Iterable<FlightDataWritable> values,
			Context context)
					throws IOException, InterruptedException {

		int count = 0;
		float totalDelay = 0;
		for (FlightDataWritable value : values) {
			totalDelay += value.getDelay().get();
			count += value.getCount().get();
		}
		context.write(key, new FlightDataWritable(totalDelay, count));
	}

}