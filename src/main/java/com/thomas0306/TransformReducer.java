package com.thomas0306;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TransformReducer extends Reducer<Text, Text, Text, Text> {
  public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String newLine = "";
    for(Text text: values) {
      newLine += " " + text.toString();
    }
    context.write(key, new Text(newLine));
  }
}
