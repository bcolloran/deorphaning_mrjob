package test;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.io.Text;

public class NaiveMultiOutputFormat
    extends MultipleTextOutputFormat<Text, Text> {
 
    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        String date = key.toString().split(",")[0].replace("\"","");
        return date + ".csv";
    }
}
