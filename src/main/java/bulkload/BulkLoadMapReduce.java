package bulkload; 

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class BulkLoadMapReduce {

	static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {


	    // The column family name
	    static byte[] family = Bytes.toBytes("c");
	    
		@Override
		public void map(LongWritable offset, Text value, Context context)
				throws IOException {
			try {
				
				String line = value.toString();
				
				// Logic just for the demo purpose. 
				// Load each line from the input file and take first 5 characters from each line as row key.
				String rowkey = line.substring(0, 5);
				byte[] bRowKey = Bytes.toBytes(rowkey);
				ImmutableBytesWritable rowKey = new ImmutableBytesWritable(
						bRowKey);
				Put p = new Put(bRowKey);
				
				// Take next 5 characters as value for the column.
				String columnValue = line.substring(5,10).trim();

				// Adding the details to 'Put' object.
				p.add(family, Bytes.toBytes("ColumnValue"), Bytes.toBytes(columnValue));
				
				// Writing to MapRDB/HBase table
				context.write(rowKey, p);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	
	public static Job createSubmittableJob(Configuration conf, String[] args)
			throws IOException {

	    // Print out the usage if there are no enough arguments passed to the job
	    if (args.length == 0) {
	    	System.out.println("Usage for setup: java -cp `hbase classpath`:./bulk-load-test-1.0.jar bulkload.BulkloadMapReduce <TABLE> <DIR PATH to the input file>");
	    	System.out.println(" java -cp `hbase classpath`:./bulk-load-test-1.0.jar  bulkload.BulkLoadMapReduce /user/maprdb_test /user/input_test_data");
	    } 
	    
		// First argument is the table name
		String tableName = args[0];
		// Second argument is path to the input data
		Path inputDir = new Path(args[1]);
		Job job = new Job(conf, "bulk_load_test");
		job.setJarByClass(ImportMapper.class);
		FileInputFormat.setInputPaths(job, inputDir);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ImportMapper.class);

		if (args.length < 3) {
			TableMapReduceUtil.initTableReducerJob(tableName, null, job);
			job.setNumReduceTasks(0);
		} else {
			HTable table = new HTable(conf, tableName);
			job.setReducerClass(PutSortReducer.class);
			Path outputDir = new Path(args[2]);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			HFileOutputFormat.configureIncrementalLoad(job, table);			
		}		
		
		TableMapReduceUtil.addDependencyJars(job);
		return job;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = createSubmittableJob(conf, args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
