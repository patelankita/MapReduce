package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * FlightDelayCompositeKey: CompositeKey, includes year, month, aaCode, aaName, recodeType of the map output
 * @author Ankita
 */

public class FlightDelayCompositeKey implements WritableComparable<FlightDelayCompositeKey> {
  // key of the map output

  private IntWritable year;
  private IntWritable month;
  private IntWritable aaCode;//airport/airline code
  private Text aaName;//airport/airline name
  private IntWritable recordType; //1-Airport , 2-Airline

  public FlightDelayCompositeKey() {
    this.month = new IntWritable();
    this.year = new IntWritable();
    this.aaCode = new IntWritable();
    this.aaName = new Text();
    this.recordType = new IntWritable();
  }

  public FlightDelayCompositeKey(IntWritable year, IntWritable month, IntWritable aaCode,
      Text aaName, IntWritable recordType) {
    this.year = year;
    this.month = month;
    this.aaCode = aaCode;
    this.aaName = aaName;
    this.recordType = recordType;
  }

  public FlightDelayCompositeKey(String year, String month, String aaCode, String aaName,
      int recordType) {
    this(new IntWritable(Integer.valueOf(year)), new IntWritable(Integer.valueOf(month)),
        new IntWritable(Integer.valueOf(aaCode)), new Text(aaName),
        new IntWritable(recordType));
  }

  public FlightDelayCompositeKey(Integer year, Integer month, Integer aaCode, String aaName,
      Integer recordType) {
    this(new IntWritable(year), new IntWritable(month),
        new IntWritable(aaCode), new Text(aaName),
        new IntWritable(recordType));
  }

  public static int compare(FlightDelayCompositeKey a, FlightDelayCompositeKey b) {
    return a.compareTo(b);
  }


  @Override
  public void write(DataOutput dataOutput) throws IOException {
    year.write(dataOutput);
    month.write(dataOutput);
    aaCode.write(dataOutput);
    aaName.write(dataOutput);
    recordType.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    year.readFields(dataInput);
    month.readFields(dataInput);
    aaCode.readFields(dataInput);
    aaName.readFields(dataInput);
    recordType.readFields(dataInput);
  }

  @Override
  public int compareTo(FlightDelayCompositeKey key) {
    int code = this.year.compareTo(key.getYear());
    if (code == 0) {
      code = this.month.compareTo(key.getMonth());
      if (code == 0) {
        code = this.recordType.compareTo(key.getRecordType());
        if (code == 0) {
          code = this.getAaCode().compareTo(key.getAaCode());
        }
      }
    }
    return code;
  }


  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FlightDelayCompositeKey) {
      if (obj == this) {
        return true;
      }
      FlightDelayCompositeKey that = (FlightDelayCompositeKey) obj;
      return this.year.equals(that.getYear())
          && this.month.equals(that.getMonth())
          && this.recordType.equals(that.getRecordType())
          && this.aaCode.equals(that.getAaCode());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(year.hashCode())
        .append(month.hashCode())
        .append(recordType.hashCode())
        .append(aaCode.hashCode())
        .toHashCode();
  }

  @Override
  public String toString() {
    return year.toString()
        + "," + month.toString()
        + "," + aaCode.toString()
        + "," + aaName.toString()
        + "," + recordType.toString();
  }

  public Text getAaName() {
    return aaName;
  }

  public void setAaName(Text aaName) {
    this.aaName = aaName;
  }

  public IntWritable getRecordType() {
    return recordType;
  }

  public void setRecordType(IntWritable recordType) {
    this.recordType = recordType;
  }

  public IntWritable getMonth() {
    return month;
  }

  public void setMonth(IntWritable month) {
    this.month = month;
  }

  public IntWritable getYear() {
    return year;
  }

  public void setYear(IntWritable year) {
    this.year = year;
  }

  public IntWritable getAaCode() {
    return aaCode;
  }

  public void setAaCode(IntWritable aaCode) {
    this.aaCode = aaCode;
  }

}
