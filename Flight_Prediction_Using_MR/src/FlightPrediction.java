import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;

/**
 * @author Ushang Thakker
 */
public class FlightPrediction {

    public static class FlightPredictionMapper extends Mapper<Object, Text, Text, Text> {

        private static HashMap<String,HashMap<Long,FlightDataWritable>> src = new HashMap<>();
        private static HashMap<String,HashMap<Long,FlightDataWritable>> dest = new HashMap<>();

        private CSVParser csvParser = new CSVParser(',', '"');
        private static String[] input;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            input = context.getConfiguration().get("input").split(",");
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] inVal = csvParser.parseLine(value.toString());
            if(isRecordValidAndRequired(inVal)){    //Record is clean and needed
                if(inVal[14].equals(3))     //src
                    addToHashMap(inVal,src,14);
                else                       // dest
                    addToHashMap(inVal,dest,23);
            }
        }


        public static void addToHashMap(String[] record, HashMap<String,HashMap<Long,FlightDataWritable>> target,
                                        int keyToCheck){
            HashMap<Long,FlightDataWritable> temp = new HashMap<>();

            long deptTime = getTimeInMillis(record,29);
            long arrTime = getTimeInMillis(record,40);

            FlightDataWritable ft = new FlightDataWritable();
            ft.setCarrier(new Text(record[8]));
            ft.setDest(new Text(record[23]));
            ft.setSrc(new Text(record[14]));
            ft.setArrivalTime(new LongWritable(arrTime));
            ft.setDeptTime(new LongWritable(deptTime));
            double delay = 0;
            if(Double.parseDouble(record[42]) > 0)
                delay = Double.parseDouble(record[42]);

            ft.setDelay(new DoubleWritable(delay));

            if(Integer.parseInt(record[47]) == 1)       //Flight is cancelled
                ft.setIsCancelled(new BooleanWritable(true));
            else
                ft.setIsCancelled(new BooleanWritable(false));

            if(keyToCheck == 14){
                //need destination and arrival time

            }
            else{
                // need the source and dept time
            }

            if(target.containsKey(record[keyToCheck])){
                temp = target.get(record[keyToCheck]);
            }
            else{

            }
        }


        public static long getTimeInMillis(String[] record, int key){
            String time = record[key];
            int month = Integer.parseInt(record[2]);
            int year = Integer.parseInt(record[0]);
            int day = Integer.parseInt(record[3]);
            int mins = Integer.parseInt(time.substring(time.length()-2));
            int hrs  = Integer.parseInt(time.substring(0,time.length()-2));
            Calendar c = Calendar.getInstance();
            c.set(year,month,day,hrs,mins);
            return c.getTimeInMillis();
        }

        /**
         * Checks if the record is required(non empty) and valid(non zero and number
         * @param record The record that needs to be checked
         * @return boolean True if the record is valid & required else false
         */
        public static boolean isRecordValidAndRequired(String[] record){

            if (record == null || record.length == 0) //If row is empty don't do anything
                return false;

            if(record[0].equals(input[0]))  //If year is equal to prediction year return false
                return false;

            if(record[14].equals(input[3]) && record[23].equals(input[4]))   //If its a direct flight
                return false;

            if(record[14].equals(input[3]) || record[23].equals(input[4]))
                if(checkIfNonZero(record) && checkIfNotEmpty(record) && timezoneCheck(record)) //Now check its validity
                    return true;
            return false;
        }

        /**
         * Checks if the timezone is proper ie basically checks timezone%60 is 0
         * @param record The record that needs to be checked
         * @return boolean True if the timezone in the record is valid
         */
        public static boolean timezoneCheck(String[] record){
            try {
                // timezone = CRS_ARR_TIME - CRS_DEP_TIME - CRS_ELAPSED_TIME
                int timeZone = Integer.parseInt(record[40]) - Integer.parseInt(record[29]) - Integer.parseInt(record[50]);
                if(timeZone % 60 == 0)
                    return true;
            }
            catch(Exception e){
                return false;
            }
            return false;
        }

        /**
         * Checks if the fields(Origin,Destination,CityName,State,StateName) of the record are not empty
         * @param record The record that needs to be checked
         * @return boolean True if the fields are nonempty else false
         */
        public static boolean checkIfNotEmpty(String[] record){
            //Origin - 14, Destination- 23,  CityName - 15 & 24, State - 16 & 25, StateName - 18 & 27 should not be empty
            if(record[14].isEmpty() || record[23].isEmpty() || record[15].isEmpty() || record[24].isEmpty() ||
                    record[16].isEmpty() || record[25].isEmpty() || record[18].isEmpty() || record[27].isEmpty() ||
                    record[2].isEmpty())
                return false;
            return true;
        }

        /**
         * Checks if the fields(AirportID,AirportSeqID,CityMarketID,StateFips,Wac,CRS_ARR_TIME,CRS_DEP_TIME) are non zero
         * and the validty of month and year field
         * @param record The record that needs to be checked
         * @return boolean True if the fields are nonzero else false
         */
        public static boolean checkIfNonZero(String[] record){
            try{
                /**AirportID - 11 & 20,  AirportSeqID - 12 & 21, CityMarketID - 13 & 22, StateFips - 17 & 26 , Wac - 19 & 28
                 * should be larger than 0, CRS_ARR_TIME - 40 & CRS_DEP_TIME - 29 should not be 0
                 **/
                if(Integer.parseInt(record[11])>0 && Integer.parseInt(record[20])>0 &&
                        Integer.parseInt(record[12])>0 && Integer.parseInt(record[21])>0 &&
                        Integer.parseInt(record[13])>0 && Integer.parseInt(record[22])>0 &&
                        Integer.parseInt(record[17])>0 && Integer.parseInt(record[26])>0 &&
                        Integer.parseInt(record[19])>0 && Integer.parseInt(record[28])>0 &&
                        Integer.parseInt(record[40])!=0 && Integer.parseInt(record[29])!=0 &&
                        Integer.parseInt(record[2])>0 && Integer.parseInt(record[2])<13 &&   // Check for month correctness
                        Integer.parseInt(record[2])>0 && Integer.parseInt(record[2])<32 &&   // Check for day correctness
                        Integer.parseInt(record[0])>=1989 && Integer.parseInt(record[0])<=2017){  // Check for year correctness
                    return true;
                }
            }
            catch(Exception e){
                return false;
            }
            return false;
        }


    }
}
