package org.hipi.examples;

import org.hipi.image.*;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.opencv.OpenCVMatWritable;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.Color;
import java.io.File;
import java.util.Date;

public class HelloWorld extends Configured implements Tool {
  
  public static class HelloWorldMapper extends Mapper<HipiImageHeader, FloatImage, Text, FloatImage> {
    public void map(HipiImageHeader key, FloatImage value, Context context) 
      throws IOException, InterruptedException {
      	
    	if (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getNumBands() == 3) {

    		// dimensiunea imaginii
    		int w = value.getWidth();
    		int h = value.getHeight();

    		float[] valData = value.getData();
    		float[] avgData = new float[3 * (w * h)];
    		double filter[][] =  new double[][]{{ -1.0f, -1.0f,  0.0f},	
    											{-1.0f,  0.0f,  1.0f},								 			
    											{0.0f,  1.0f,  1.0f  }};
								 				       
    		float factor = 1.0f;
    		float bias = 128.0f; //sau 1280
    		double red, green, blue;
    		double auxr = 0.0f, auxg = 0.0f, auxb = 0.0f;
    		int row, col;
    		int indexi, indexj, offseti, offsetj;
    		offseti = offsetj = - 3 /2;
    		
    		for (int j = 0; j < h; j++) {
    			for (int i = 0; i < w; i++) {
    				valData[(j * w + i) * 3 + 0] = valData[(j * w + i) * 3 + 0] * 255.0f;
    				valData[(j * w + i) * 3 + 1] = valData[(j * w + i) * 3 + 1] * 255.0f;
    				valData[(j * w + i) * 3 + 2] = valData[(j * w + i) * 3 + 2] * 255.0f;
    			}
    		}
        
    		for (int j = 0; j < h; j++) {
    			for (int i = 0; i < w; i++) {
		            red = 0.0f;
					green = 0.0f;
					blue = 0.0f; 
					for(row = 0; row < 3; row++) {
						for(col = 0; col < 3; col++) {
							indexi = (i + col + w + offseti) % w; 
							indexj = (j + row + h + offsetj) % h;
							auxr = valData[(indexj * w + indexi) * 3 + 0];
							auxg = valData[(indexj * w + indexi) * 3 + 1];
							auxb = valData[(indexj * w + indexi) * 3 + 2];
							red += auxr * filter[row][col];
							green += auxg * filter[row][col];
							blue += auxb * filter[row][col]; 
						} 
					}
					auxr = factor * red + bias;
					if (auxr < 0)
						auxr = 0;
					if (auxr > 255)
						auxr = 255;
					auxg = factor * green + bias;
					if (auxg < 0)
						auxg = 0;
					if (auxg > 255)
						auxg = 255;
					auxb = factor * blue + bias;
					if (auxb < 0)
						auxb = 0;
					if (auxb > 255)
						auxb = 255;
					avgData[(j * w + i) * 3 + 0] = (float)auxr;
					avgData[(j * w + i) * 3 + 1] = (float)auxg;
					avgData[(j * w + i) * 3 + 2] = (float)auxb;
    			}
    		}

    		FloatImage avg = new FloatImage(w, h, 3, avgData);

    		// Emit record to reducer
    		String filename = key.getAllMetaData().get("filename");
    		context.write(new Text(filename), avg);

    	}
    }
  }
  
  public static class HelloWorldReducer extends Reducer<Text, FloatImage, Text, FloatImage> {
    public void reduce(Text key, Iterable<FloatImage> values, Context context) 
      throws IOException, InterruptedException {

		int total = 0;
		int size = 100;
		File outputfile[] = new File[size];
      
		for (FloatImage val : values) {
			
			float[] avgData = val.getData();
			FloatImage avg = new FloatImage(val.getWidth(), val.getHeight(), 3, avgData);
			String result  =  new String();       
        	context.write(key, avg);
        	
        	BufferedImage bufferedImage = new BufferedImage(val.getWidth(), val.getHeight(),
        	BufferedImage.TYPE_INT_RGB);
		
        	Color[][] image = new Color[val.getHeight()][val.getWidth()];
		
        	for (int j = 0; j < val.getHeight(); j++)
        		image[j] = new Color[val.getWidth()];
			
        	for (int j = 0; j < val.getHeight(); j++)
        		for (int i = 0; i < val.getWidth(); i++)
        			image[j][i] = new Color(avgData[(j * val.getWidth() + i) * 3 + 0]/255.0f, 
        					avgData[(j * val.getWidth() + i) * 3 + 1]/255.0f, 
        					avgData[(j * val.getWidth() + i) * 3 + 2]/255.0f);
          	
        	for (int j = 0; j < val.getHeight(); j++)
        		for (int i = 0; i < val.getWidth(); i++)
        			bufferedImage.setRGB(i, j, image[j][i].getRGB());
        	
        	total++;       
        	outputfile[total] = new File(key + "-emboss.png");
        	ImageIO.write(bufferedImage, "png", outputfile[total]);
      }
    }
  }
  
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

	long startTime = System.currentTimeMillis();
    // Execute the MapReduce job and block until it complets
    boolean success = job.waitForCompletion(true);
    long estimatedTime = System.currentTimeMillis() - startTime;
	System.out.println("Time: " + (float)estimatedTime/1000);
    
    // Return success or failure
    return success ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
  	ToolRunner.run(new HelloWorld(), args);
  	System.exit(0);
  }
}
