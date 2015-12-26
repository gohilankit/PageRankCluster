package crawl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import crawl.WARCFileInputFormat;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class WATGetLinks extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WATGetLinks(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		System.out.println("st: " + startTime);
		Configuration conf = getConf();
		conf.setLong("mapred.task.timeout", 3600000);
		int startWAT = Integer.parseInt(args[1]);
		int endWAT = Integer.parseInt(args[2]);
		int numDomains = Integer.parseInt(args[3]);
		conf.setInt("numDomains", numDomains);
		for(int i = 0; i < numDomains; i++){
			conf.setStrings("domain"+(i+1), args[i+4]);
		}
		
		Job job = new Job(conf, "watlink");
		job.setJarByClass(WATGetLinks.class);
		job.setNumReduceTasks(1);
		
		AWSCredentials as = new AWSCredentials("AKIAI6EHHAFK63EUD2SQ","p1ZQJBQsbMDms40Kzgcdq0V9w59HKQnfelZuPkn0");
		S3Service s3s = new RestS3Service(as);
		String fn = "wat.paths";
		S3Object f = s3s.getObject("ccwatcc", fn, null, null, null, null, null, null);
		BufferedReader br = new BufferedReader(new InputStreamReader(f.getDataInputStream()));
		String line; 
		ArrayList<String> inputPath = new ArrayList<String>();
		int i=1;
		while((line = br.readLine()) != null) {
			if(i>startWAT){
				String path = "s3n://aws-publicdatasets/" + line;
				inputPath.add(path);
			}
			if(i>endWAT)
				break;
			i++;
		}
		
		br.close();
		
		for(int j=0; j<inputPath.size(); j++){
			MultipleInputs.addInputPath(job, new Path(inputPath.get(j)),WARCFileInputFormat.class, DomainSpecific.URLMapper.class);
		}
		
		inputPath.clear();

		//String inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-35/segments/1440644059455.0/wat/CC-MAIN-20150827025419-00000-ip-10-171-96-226.ec2.internal.warc.wat.gz";
		//FileInputFormat.addInputPath(job, new Path(inputPath));
		
		String outputPath = args[0];
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		for(int k=0; k<numDomains; k++){
			String dom = args[k+4].substring(0, args[k+4].indexOf("."));
			MultipleOutputs.addNamedOutput(job, dom, TextOutputFormat.class, Text.class, Text.class);
		}
		
		//job.setInputFormatClass(WARCFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
		//job.setMapperClass(URLMap.URLMapper.class);
		job.setReducerClass(DomainSpecific.URLReducer.class);
		
		if (job.waitForCompletion(true)) {
			long endTime = System.currentTimeMillis();
			System.out.println(endTime - startTime);
	    	return 0;
	    } else {
			return 1;
	    }
	}
}
