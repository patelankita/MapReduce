package org.neu.reducer;

import static org.neu.FlightPerformance.TOP_K_COUNT_CONF_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.neu.data.FlightCodeCountKeyPair;
import org.neu.data.FlightDataWritable;
import org.neu.data.FlightDelayCompositeKey;

/**
 * FlightDelayReducer: Reducer class:For the same key, calculate the total delay time and count of flight, 
 * and store the new data into map; Then according to the flight count, sort the map, filter top K flight count,
 * output the result.
 * @author Ankita
 */
public class FlightDelayReducer extends
    Reducer<FlightDelayCompositeKey, FlightDataWritable, FlightDelayCompositeKey, FlightDataWritable> {

  private static int topKCount; // number of top airline/airport.
  private MultipleOutputs mos;
  /*airportFlightCount: holds global flight counts for airport: k->aaCode, v->Count*/
  private Map<Integer, Integer> airportFlightCount = new HashMap<>();
  /*airlineFlightCount: holds global flight counts for airline: k->aaCode, v->Count*/
  private Map<Integer, Integer> airlineFlightCount = new HashMap<>();
  private SortedSet<FlightCodeCountKeyPair> airportFlightCountSorted = new TreeSet<>();
  private SortedSet<FlightCodeCountKeyPair> airlineFlightCountSorted = new TreeSet<>();
  /*reducedValueMap: Local Map to hold reduced records for each <YEAR,MONTH,RECORD_TYPE,CODE>*/
  private Map<FlightDelayCompositeKey, FloatWritable> reducedValueMap = new HashMap<>();
  /*mostBusyMap: Local Map to hold List of Most Busy Airport/Airline.
  k -> recordType, V->List of Most Busy Airport/Airline*/
  private Map<Integer, List<Integer>> mostBusyMap = new HashMap<>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    topKCount = context.getConfiguration().getInt(TOP_K_COUNT_CONF_KEY, 5);
    mos = new MultipleOutputs<>(context);
  }

  @Override
  public void reduce(FlightDelayCompositeKey key, Iterable<FlightDataWritable> values,
      Context context)
      throws IOException, InterruptedException {
    int count = 0;
    float totalDelay = 0;
    FlightDelayCompositeKey newKey = new FlightDelayCompositeKey(key.getYear().get(),
        key.getMonth().get(), key.getAaCode().get(), key.getAaName().toString(),
        key.getRecordType().get());
    //Actual reduce operation
    for (FlightDataWritable value : values) {
      totalDelay += value.getDelay().get();
      count += value.getCount().get();
    }
    // Storing reduced record to local map
    reducedValueMap.put(newKey, new FloatWritable(totalDelay / count));
    // Storing flight counts
    increaseFlightCount(key, count);
  }

  /**
   * IncreaseFlightCount stores global flight counts for airlines/airports
   */
  private void increaseFlightCount(FlightDelayCompositeKey key, int count) {
    if (key.getRecordType().get() == 1) {
      airportFlightCount.put(key.getAaCode().get(),
          airportFlightCount.getOrDefault(key.getAaCode().get(), 0) + count);
    } else {
      airlineFlightCount.put(key.getAaCode().get(),
          airlineFlightCount.getOrDefault(key.getAaCode().get(), 0) + count);
    }
  }

  @Override
  /**
   * After reduce, sort the result by value, filter top K busy airpot/airline and output the result
   */
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // sort airportFlightCount,airlineFlightCount
    sortCountMaps();
    // write top 5 most busy airport/airline to file
    writeMostBusy();
    // write filtered records (filtered on most busy) to file
    writeFilteredRecords();
    mos.close();
  }

  /**
   * writeFilteredRecords filter local stored reduced records by mostBusy Airline/Airport
   * The output is written to file
   */
  private void writeFilteredRecords() throws IOException, InterruptedException {
    for (Map.Entry<FlightDelayCompositeKey, FloatWritable> entry : reducedValueMap.entrySet()) {
      List<Integer> busyList = mostBusyMap.get(entry.getKey().getRecordType().get());
      if (busyList.contains(entry.getKey().getAaCode().get())) {
        writeDelayData(entry);
      }
    }
  }

  /**
   * Writes Filtered Records to individual Output files.
   */
  private void writeDelayData(Entry<FlightDelayCompositeKey, FloatWritable> entry)
      throws IOException, InterruptedException {
    if (1 == entry.getKey().getRecordType().get()) {
      mos.write("flightDelayAirportData", entry.getKey(), entry.getValue());
    } else {
      mos.write("flightDelayAirlineData", entry.getKey(), entry.getValue());
    }
  }

  /**
   * Sort map by their value
   */
  private void sortCountMaps() {
    for (Map.Entry<Integer, Integer> entry : airportFlightCount.entrySet()) {
      airportFlightCountSorted.add(new FlightCodeCountKeyPair(entry.getKey(), entry.getValue()));
    }
    for (Map.Entry<Integer, Integer> entry : airlineFlightCount.entrySet()) {
      airlineFlightCountSorted.add(new FlightCodeCountKeyPair(entry.getKey(), entry.getValue()));
    }
  }


  /**
   * Write most busy aiport and airline data separately
   */
  private void writeMostBusy() throws IOException, InterruptedException {
    writeSortedSet(airportFlightCountSorted, 1, "mostBusyAirportData");
    writeSortedSet(airlineFlightCountSorted, 2, "mostBusyAirlineData");
  }

  /**
   * Write top k busy aiport and airline data 
   */
  private void writeSortedSet(SortedSet<FlightCodeCountKeyPair> sortedSet, int recordType,
      String outputFile)
      throws IOException, InterruptedException {
    int recordCount = 0;
    for (FlightCodeCountKeyPair entry : sortedSet) {
      if (++recordCount <= topKCount) {
        List<Integer> busyList = mostBusyMap
            .computeIfAbsent(recordType, k -> new ArrayList<>());
        busyList.add(entry.getAaCode());
        mostBusyMap.put(recordType, busyList);
        mos.write(outputFile, new Text(String.valueOf(recordType)),
            new Text(String.valueOf(entry.getAaCode())));
      } else {
        break;
      }
    }
  }
}
