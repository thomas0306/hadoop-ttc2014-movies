package com.thomas0306;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransformMapper extends Mapper<LongWritable, Text, Text, Text> {
  private Text objectKey = new Text();
  private Text payload = new Text();

  private Pattern instancePattern = Pattern.compile("^(actor|actress|movie)\\d{1,6}\\s:\\s(Actor|Actress|Movie)$");
  private Pattern attributePattern = Pattern.compile("^(actor|actress|movie)\\d{1,6}\\..+\\s=\\s(\".+\"|\\d\\.\\d)$");
  private Pattern linkPattern = Pattern.compile("^(actor|atress)\\d{1,6}\\s:\\smovie\\d{1,6}\\.persons$");

  public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    // movie46959 : Movie
    // actor2659 : Actor
    // actress1231 : Actress

    // movie46959.title = "100 Musicians"
    // movie46959.rating = 0.0
    // actor2659.name = "Bako, Peter"

    // actor5241 : movie46959.persons
    
    if (instancePattern.matcher(line).matches()) {
      objectKey.set(line.substring(0, line.indexOf(' ')));
      payload.set("instance");
    } else if (attributePattern.matcher(line).matches()) {
      objectKey.set(line.substring(0, line.indexOf('.')));
      String attribute = line.split(" = ")[0].split("\\.")[1];
      payload.set(attribute + "=" + line.split(" = ")[1]);
    } else if (linkPattern.matcher(line).matches()) {
      objectKey.set(line.split(" : ")[1].split("\\.")[0]);
      payload.set("link" + ":" + line.split(" : ")[0]);
    } else {
      objectKey.set("Invalid");
    }

    context.write(objectKey, payload);
  }
}
