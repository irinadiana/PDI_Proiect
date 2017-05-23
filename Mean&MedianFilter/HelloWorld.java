package org.hipi.examples;
import java.util.Arrays;
import java.io.FileOutputStream;
import java.io.IOException;

import org.hipi.image.*;
import org.hipi.image.io.*;
import org.hipi.imagebundle.mapreduce.HibInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HelloWorld extends Configured implements Tool {

  public static class HelloWorldMapper extends Mapper<HipiImageHeader, FloatImage, Text, FloatImage> {

    public void map(HipiImageHeader key, FloatImage value, Context context) 
        throws IOException, InterruptedException {

      String filename = key.getAllMetaData().get("filename");
      System.out.println("*****DEBUG: filename = " + filename);

        // Get dimensions of image
        int w = value.getWidth();
        int h = value.getHeight();

        // Get pointer to image data
        float[] valData = value.getData();
        float[] meanFilterData = new float[valData.length];
        float[] medianFilterData = new float[valData.length];

        // Traverse image pixel data in raster-scan order and copy to meanFilterData and  medianFilterData
        for (int j = 0; j < h; j++) {
          for (int i = 0; i < w; i++) {
            meanFilterData[(j*w+i)*3+0] = valData[(j*w+i)*3+0]; // R
            meanFilterData[(j*w+i)*3+1] = valData[(j*w+i)*3+1]; // G
            meanFilterData[(j*w+i)*3+2] = valData[(j*w+i)*3+2]; // B

            medianFilterData[(j*w+i)*3+0] = valData[(j*w+i)*3+0]; // R
            medianFilterData[(j*w+i)*3+1] = valData[(j*w+i)*3+1]; // G
            medianFilterData[(j*w+i)*3+2] = valData[(j*w+i)*3+2]; // B
          }
        }

        for (int j = 1; j < h - 1; j++) {
          for (int i = 1; i < w - 1; i++) {
            // Fill data for mean filder
            meanFilterData[(j*w+i)*3+0] = (valData[(j*w+i)*3+0] + valData[((j-1)*w+i-1)*3+0] + valData[((j-1)*w+i)*3+0] + valData[((j-1)*w+i+1)*3+0] +
                                      valData[(j*w+i-1)*3+0] + valData[(j*w+i+1)*3+0] + valData[((j+1)*w+i-1)*3+0] + valData[((j+1)*w+i)*3+0] + valData[((j+1)*w+i+1)*3+0]) / 9;
            meanFilterData[(j*w+i)*3+1] = (valData[(j*w+i)*3+1] + valData[((j-1)*w+i-1)*3+1] + valData[((j-1)*w+i)*3+1] + valData[((j-1)*w+i+1)*3+1] +
                                      valData[((j*w)+i-1)*3+1] + valData[((j*w)+i+1)*3+1] + valData[((j+1)*w+i-1)*3+1] + valData[((j+1)*w+i)*3+1] + valData[((j+1)*w+i+1)*3+1]) / 9;
            meanFilterData[(j*w+i)*3+2] = (valData[(j*w+i)*3+2] + valData[((j-1)*w+i-1)*3+2] + valData[((j-1)*w+i)*3+2] + valData[((j-1)*w+i+1)*3+2] +
                                      valData[(j*w+i-1)*3+2] + valData[(j*w+i+1)*3+2] + valData[((j+1)*w+i-1)*3+2] + valData[((j+1)*w+i)*3+2] + valData[((j+1)*w+i+1)*3+2]) / 9;

            // Fill data for median filder
            float[] array = new float[10];
            for (int k = 0; k < 3; k++) {
                array[0] = valData[(j*w+i)*3+k];
                array[1] = valData[((j-1)*w+i-1)*3+k];
                array[2] = valData[((j-1)*w+i)*3+k];
                array[3] = valData[((j-1)*w+i+1)*3+k];
                array[4] = valData[(j*w+i-1)*3+k];
                array[5] = valData[(j*w+i+1)*3+k];
                array[6] = valData[((j+1)*w+i-1)*3+k];
                array[7] = valData[((j+1)*w+i)*3+k];
                array[8] = valData[((j+1)*w+i+1)*3+k];

                Arrays.sort(array);
                medianFilterData[(j*w+i)*3+k] = (array[3] + array[4]) / 2; 
            }
          }
        }

        JpegCodec jpegCodec = new JpegCodec();

        FileOutputStream fileStream = new FileOutputStream(filename + "_mean");
        FloatImage floatImage = new FloatImage(w, h, value.getNumBands(), meanFilterData);
        jpegCodec.encodeImage(floatImage, fileStream);

        fileStream = new FileOutputStream(filename + "_median");
        floatImage = new FloatImage(w, h, value.getNumBands(), medianFilterData);
        jpegCodec.encodeImage(floatImage, fileStream);

        // Emit record to reducer
        context.write(new Text(filename), floatImage);     
    } // map()

  } // HelloWorldMapper
  
  public static class HelloWorldReducer extends Reducer<Text, FloatImage, Text, Text> {

    public void reduce(Text key, FloatImage values, Context context) throws IOException, InterruptedException {   
        context.write(key, key);
    } // reduce()
  } // HelloWorldReducer
  
  public int run(String[] args) throws Exception {
    // Check input arguments
    if (args.length != 2) {
      System.out.println("Usage: helloWorld <input HIB> <output directory>");
      System.exit(0);
    }
    
    // Initialize and configure MapReduce job
    Job job = Job.getInstance();
    // Set input format class which parses the input HIB and spawns map tasks
    job.setInputFormatClass(HibInputFormat.class);
    // Set the driver, mapper, and reducer classes which express the computation
    job.setJarByClass(HelloWorld.class);
    job.setMapperClass(HelloWorldMapper.class);
    job.setReducerClass(HelloWorldReducer.class);
    // Set the types for the key/value pairs passed to/from map and reduce layers
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatImage.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    // Set the input and output paths on the HDFS
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Execute the MapReduce job and block until it complets
    boolean success = job.waitForCompletion(true);
    
    // Return success or failure
    return success ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new HelloWorld(), args);
    System.exit(0);
  }
  
}
  

