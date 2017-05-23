package org.hipi.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;

public class SharpenFilterMapper extends
		Mapper<HipiImageHeader, FloatImage, Text, FloatImage> {
	private static final int filterWidth = 3;
	private static final int filterHeight = 3;

	private static double filter[][] = { { -1.0f, -1.0f, -1.0f }, { -1.0f, 9.0f, -1.0f }, { -1.0f, -1.0f, -1.0f} };
	private static double filter2[][] = { { 1.0f, 1.0f, 1.0f }, { 1.0f, -7.0f, 1.0f }, { 1.0f, 1.0f, 1.0f} };

	private static final double factor = 1.0f;
	private static final double bias = 0.0f;

	public void map(HipiImageHeader key, FloatImage value, Context context)
			throws IOException, InterruptedException {

		// Verify that image was properly decoded, is of sufficient size, and
		// has three color channels (RGB)
		if (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getNumBands() == 3) {

			// Get dimensions of image
			int w = value.getWidth();
			int h = value.getHeight();
			
			// Get pointer to image data
			float[] valData = value.getData();
			float[] result = new float[valData.length];
			
			// apply the filter
			for (int x = 0; x < w; x++) {
				for (int y = 0; y < h; y++) {
					double red = 0.0f, green = 0.0f, blue = 0.0f;

					// multiply every value of the filter with corresponding
					// image pixel
					for (int filterY = 0; filterY < filterHeight; filterY++)
						for (int filterX = 0; filterX < filterWidth; filterX++) {
							int imageX = (x - filterWidth / 2 + filterX + w) % w;
							int imageY = (y - filterHeight / 2 + filterY + h) % h;
							red += valData[(imageY * w + imageX) * 3 + 0]
									* filter[filterY][filterX] * 255.0f;
							green += valData[(imageY * w + imageX) * 3 + 1]
									* filter[filterY][filterX] * 255.0f;
							blue += valData[(imageY * w + imageX) * 3 + 2]
									* filter[filterY][filterX] * 255.0f;
						}
					
					// truncate values smaller than zero and larger than 255
					result[(y * w + x) * 3 + 0] = Math.min(Math.max((float)(factor * red + bias), 0.0f), 255.0f)/255.0f;
					result[(y * w + x) * 3 + 1] = Math.min(Math.max((float)(factor * green + bias), 0.0f), 255.0f)/255.0f;
					result[(y * w + x) * 3 + 2] = Math.min(Math.max((float)(factor * blue + bias), 0.0f),  255.0f)/255.0f;
				}
			}
			// Create a FloatImage to store the average value
			FloatImage res = new FloatImage(w, h, value.getNumBands(), result);

			// Emit record to reducer
			String filename = key.getAllMetaData().get("filename");
			context.write(new Text(filename), res);
		}

	}

}

