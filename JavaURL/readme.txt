write Java code to download the following books (the .txt version only) and then upload them to your HDFS directory. 

	The Outline of Science, Vol. 1 (of 4) by J. Arthur Thomson (download)
	The Notebooks of Leonardo Da Vinci (download)
	The Art of War by 6th cent. B.C. Sunzi (download)
	The Adventures of Sherlock Holmes by Sir Arthur Conan Doyle (download)
	The Devil's Dictionary by Ambrose Bierce (download)
	Encyclopaedia Britannica, 11th Edition, Volume 4, Part 3 (download)


You should then decompress the files on the server using any of examples shown in class. 

You will use these in the next weeks to run WordCount algorithm using MapReduce computing paradigm. Try to make your code as efficient as possible and incorporate error checking e.g. what would happen if the file already exists.
############################################################################################################################
Filename: JavaURL.rar

Java files location:
JavaURL\src\main\java\JavaURL\JavaURL

Command to execution:
hadoop jar <.jar file> <main execution> <filepath>
hadoop jar JavaURL-0.0.1-SNAPSHOT.jar JavaURL.JavaURL.Myclass hdfs://cshadoop1/user/jhe140030

output case:
if file is present: it gives error messages. "File already exists!"
                                     

