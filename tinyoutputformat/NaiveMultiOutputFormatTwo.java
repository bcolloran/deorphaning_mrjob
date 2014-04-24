package test;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

public class NaiveMultiOutputFormatTwo
    extends MultipleTextOutputFormat<Text, Text> {
 
    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        String fileName = key.toString().split("\\|")[0].replace("\"","");
        return fileName+ "/"+name + ".csv";
    }

    @Override
    protected Text generateActualKey(Text key, Text value) {
    // return value;
    // key.setValue( key.toString().split("|")[1].replace("\"","") );
    String keyStr = key.toString().split("\\|")[1].replace("\"","");
    return new Text( keyStr );
    }
}
