package JavaURL.JavaURL;

import java.io.IOException;

public class Myclass 
{
    public static void main( String[] args )
    {  
        String fileURLs[]={"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
        		"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
        		"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
        		"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
        		"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
        		"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"};
        String Book_extens[]={"20417.txt.bz2","5000-8.txt.bz2","132.txt.bz2","1661-8.txt.bz2","972.txt.bz2","19699.txt.bz2"};
        int j=0;
        
        try {
        	for(String fileURL : fileURLs)
        	{
        		String dstDir=args[0]+"/"+Book_extens[j++];
        		boolean status = downloader.downloadFile(fileURL, dstDir);
        		if(status) {
        			decompressor.decompressFile(dstDir);
        		}
        	}		   
        } 
        catch (IOException ex) {
            ex.printStackTrace();
        }
        
        
    }
}
