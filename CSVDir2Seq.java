import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class CSVDir2Seq {
private static void listFilesForFolder(File folder,ArrayList<String> filenames) {
        for (File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry,filenames);
            } else {
                //System.out.println("Files"+fileEntry.getName());
                filenames.add(fileEntry.getName());
            }
          }
       }

	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.err.println("Arguments: [input csv file] [output sequence file]");
			return;
		}
		String inputFolderName = args[0];
		String outputDirName = args[1];
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
    		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(inputFolderName), true);

		Writer writer = new SequenceFile.Writer(fs, configuration, new Path(outputDirName + "/chunk-0"),
				Text.class, Text.class);
		
		int count = 0;
               /* File folder = new File(inputFolderName );
                ArrayList<String> filenames = new ArrayList<>();
                listFilesForFolder(folder,filenames);*/

                //for (int i = 0; i < filenames.size(); i++) {
   		while(fileStatusListIterator.hasNext()){
        	LocatedFileStatus filenames = fileStatusListIterator.next();

                Path inputFileName = filenames.getPath();
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inputFileName)));
		Text key = new Text();
		Text value = new Text();
		while(true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			String[] tokens = line.split("`");
			if (tokens.length > 7) {
				System.out.println("Skip line: " + line);
				continue;
			}
			String language = tokens[2];
			String pageviews = tokens[5];
			String hour = tokens[1];
                        String project = tokens[4];
                        String date = tokens[0];
                        String category = tokens[3];
                        String numBytes = tokens[6];
			//key.set("/" + inputFileName);
                        key.set("/"+pageviews);
			value.set(language+" "+project+" "+" "+category+" "+numBytes);
			//value.set(pageviews);
			writer.append(key, value);
			count++;
		}
		reader.close();
                }
                writer.close();
		System.out.println("Wrote " + count + " entries.");
	}
}
