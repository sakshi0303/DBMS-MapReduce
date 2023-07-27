import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IMDbData {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Define the date ranges and genre categories
        int[][] dateRanges = { { 2000, 2006 }, { 2007, 2013 }, { 2014, 2020 } };
        String[][] genreCategories = { { "Comedy", "Romance" }, { "Action", "Drama" }, { "Adventure", "Sci-Fi" } };
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        // mapper is tranformer
        // object key associated with this mapper execution is unique,value=sentence
        // context knows which file it is and which split it is
        // mapper emits all the occurance of the word inside one sentence
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            // we can use log that will tell which file name is running inside this map
            String filename = fileSplit.getPath().getName();
            // StringTokenizer itr = new StringTokenizer(value.toString());
            // Split the sentence into individual parts
            String[] parts = value.toString().split(";");
            // Erro solution: Null /N is coming
            if (parts.length > 3 && parts[3].matches("-?\\d+(\\.\\d+)?") 
                && parts[1].equals("movie") ) {
                // Extract the year value from the sentence
                int year = Integer.parseInt(parts[3]);
                // Extract the genres from the sentence
                List<String> genres = Arrays.asList(parts[4].split(","));
                // Find the date range that the year falls in
                String dateRangeLabel = "Unknown";
                for (int i = 0; i < dateRanges.length; i++) {
                    if (year >= dateRanges[i][0] && year <= dateRanges[i][1]) {
                        dateRangeLabel = dateRanges[i][0] + "-" + dateRanges[i][1];
                        break;
                    }
                }
                // Find the genre category that matches the genres in the sentence
                String genreCategoryLabel = "Unknown";
                for (int i = 0; i < genreCategories.length; i++) {
                    List<String> categoryGenres = Arrays.asList(genreCategories[i]);
                    if (genres.containsAll(categoryGenres)) {
                        genreCategoryLabel = String.join("/", categoryGenres);
                        break;
                    }
                }
                // Create the key with the specified format
                if (!dateRangeLabel.equals("Unknown") && !genreCategoryLabel.equals("Unknown")) {
                    String key1 = dateRangeLabel + " | " + genreCategoryLabel;
                    System.out.println(key1); // Output: 2007-2013 | Drama/Action
                    // word is text type , key1 is in string format
                    word.set(key1);
                    context.write(word, one);
                } else {
                    System.out.println("Unable to create key");
                }
                
            }
        }
    }    
    // reducer accepts the mapper output and aggregates them to a final result
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        // key=word
        // values=list of occurances reading all the occuracance and summing
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            // key=word of text type
            // result=Iterable
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(IMDbData.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
