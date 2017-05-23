package org.hipi.examples;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.hipi.image.FloatImage;
import org.hipi.image.io.JpegCodec;


public class SharpenFilterReducer extends
		Reducer<Text, FloatImage, Text, FloatImage> {

	public void reduce(Text key, Iterable<FloatImage> values,
			Context context) throws IOException, InterruptedException {
		System.out.println("*****DEBUG Reduce: key = " + key);
				
		for (FloatImage val : values) {
			String filename = "out_" + key.toString();
			try {
			    float[] data = val.getData();
			    int w = val.getWidth();
			    int h = val.getHeight(); 
			    BufferedImage bi = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
			    for (int y = 0; y < h; y++) {
					for (int x = 0; x < w; x++) {
						Color color = new Color((float)data[(y * w + x) * 3 + 0], 
												(float)data[(y * w + x) * 3 + 1], 
												(float)data[(y * w + x) * 3 + 2]);
						bi.setRGB(x, y, color.getRGB());
					}
			    }
			    ImageIO.write(bi, "png", new File(filename));
			    context.write(new Text(filename), val);
			    
			} catch (IOException e) {
				System.out.println("*****Error in REDUCE******");
				e.printStackTrace();
			}
		}
	}

}

