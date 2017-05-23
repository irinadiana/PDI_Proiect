import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;


public class SharpenSerial {

	static double filter[][] =  new double[][]{{ -1.0f, -1.0f,  -1.0f},	
										{-1.0f,   9.0f,  -1.0f},								 			
										{-1.0f,  -1.0f,  -1.0f}};

	static double factor = 1.0;
	static double bias = 0.0;
	
	public static void main(String[] args) throws IOException {
		
		long startTime = System.currentTimeMillis();
		File files[]  = new File[8];
		
		files[0] = new File("imageInputs/brisbane.png");
		files[1] = new File("imageInputs/vesuvius.png");
		files[2] = new File("imageInputs/madrid.png");
		files[3] = new File("imageInputs/victoria-falls.png");
		files[4] = new File("imageInputs/quito.png");
		files[5] = new File("imageInputs/nazca.png");
		files[6] = new File("imageInputs/beijing.png");
		files[7] = new File("imageInputs/dubai.png");
		
		for (int file = 0; file < 8; file++) {
			BufferedImage image = ImageIO.read(files[file]);
			int width = image.getWidth();
			int height = image.getHeight();

			double[][][] auxImage = new double[height][width][3];
			for (int row = 0; row < height; row++) {
				for (int col = 0; col < width; col++) {
					int pixel = image.getRGB(col, row);
					double redPixel = (pixel >> 16) & 0xff;
				    double greenPixel = (pixel >> 8) & 0xff;
				    double bluePixel = (pixel) & 0xff;
				    auxImage[row][col][0] = redPixel * 255.0f;
				    auxImage[row][col][1] = greenPixel * 255.0f;
				    auxImage[row][col][2] = bluePixel * 255.0f;
				}
			}
			
			double red, green, blue;
			double auxr = 0.0, auxg = 0.0, auxb = 0.0;
			int indexi, indexj, offseti, offsetj;
			offseti = offsetj = 3 /2;
			BufferedImage bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
			
			for (int j = 0; j < height; j++) {
				for (int i = 0; i < width; i++) {	
					red = 0.0f;
					green = 0.0f;
					blue = 0.0f; 
					for(int rowFilter = 0; rowFilter < 3; rowFilter++) {
						for(int colFilter = 0; colFilter < 3; colFilter++) {
							indexi = (i + colFilter + width - offseti) % width; 
							indexj = (j + rowFilter + height - offsetj) % height;
							auxr = auxImage[indexj][indexi][0];
							auxg = auxImage[indexj][indexi][1];
							auxb = auxImage[indexj][indexi][2];
							red += auxr * filter[rowFilter][colFilter];
							green += auxg * filter[rowFilter][colFilter];
							blue += auxb * filter[rowFilter][colFilter]; 
						} 
					}
					
					auxr = Math.min(Math.max((float)(factor * red + bias), 0.0f), 255.0f)/255.0f;
					auxg = Math.min(Math.max((float)(factor * green + bias), 0.0f), 255.0f)/255.0f;
					auxb = Math.min(Math.max((float)(factor * blue + bias), 0.0f), 255.0f)/255.0f;
										
					bufferedImage.setRGB(i, j, new Color((float)auxr, (float)auxg, (float)auxb).getRGB());
				}
			}
					
			ImageIO.write(bufferedImage, "png", new File("sharpenOutputs/" + files[file].getName() + "-output.png"));
		}
		long estimatedTime = System.currentTimeMillis() - startTime;
		System.out.println("Time: " + (float)estimatedTime/1000);
	}
}
