import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TipsCleaningMapper
    extends Mapper<LongWritable, Text, NullWritable, Text> {
  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
   
    try {
    	JSONParser parser = new JSONParser(); 
    	Object obj = parser.parse(value.toString());
    	JSONObject jsonObject = (JSONObject) obj;
    	String user_id = (String) jsonObject.get("user_id");
    	String business_id = (String) jsonObject.get("business_id");
    	String text_field= (String) jsonObject.get("text");
    	text_field = text_field.replaceAll("[^a-zA-Z0-9\\s]", "");
    	String date_field = (String) jsonObject.get("date");
   	//SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    	//Date date = dt.parse(date_field);
    	//SimpleDateFormat dt_new = new SimpleDateFormat("yyyy-MM-dd");
    	//String date_field_result = dt_new.format(date);
    	Long compliment_count = (Long) jsonObject.get("compliment_count");
    	String compliment_count_field= Long.toString(compliment_count);
	String result = user_id+","+business_id+","+text_field+","+date_field+","+compliment_count_field;
	//+compliment_count_field;
    	context.write(NullWritable.get(), new Text(result));
    } catch (ParseException e) {
	    e.printStackTrace();
	}
   
  }
}

