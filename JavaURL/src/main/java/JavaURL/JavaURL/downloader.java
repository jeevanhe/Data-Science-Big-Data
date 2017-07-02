package JavaURL.JavaURL;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class downloader {

	//Downloads the the file from given URL and stores the same in Destination directory. 
    public static boolean downloadFile(String fileURL, String dstDir)
            throws IOException {
    	
    	if (fileURL == null || dstDir == null)
    	{
    		System.out.println("FileURL or Destination directory not present!");
    		return false;
    	}
    	System.out.println("Downloading file " + fileURL);
    	URL url = new URL(fileURL);
        InputStream in = new BufferedInputStream(url.openStream());
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(URI.create(dstDir), conf);
        if(!(fs.exists(new Path(dstDir)))){
        	OutputStream out = fs.create(new Path(dstDir));
        	IOUtils.copyBytes(in, out, 4096, true);
        	return true;
        }
        else {
        	System.out.println("File already exists!");
        	return false;
        }
     }          
  }

