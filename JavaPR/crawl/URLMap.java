package crawl;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.Iterator;

public class URLMap {
	private static final Logger LOG = Logger.getLogger(URLMap.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		NO_SERVER,
		EXCEPTIONS
	}

	protected static class URLMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {
			for (ArchiveRecord r : value) {
				// Skip any records that are not JSON
				if (!r.getHeader().getMimetype().equals("application/json")) {
					continue;
				}
				try {
					context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
					// Convenience function that reads the full message into a raw byte array
					byte[] rawData = IOUtils.toByteArray(r, r.available());
					String content = new String(rawData);
					JSONObject json = new JSONObject(content);
					try {
						String source = json.getJSONObject("Envelope").getJSONObject("WARC-Header-Metadata").getString("WARC-Target-URI");
						if(source.substring(0, 7).equals("http://")){
							source = source.substring(7);
						}
						else if(source.substring(0, 8).equals("https://")){
							source = source.substring(8);
						}
						
						if(source.indexOf('?') != -1){
							source = source.substring(0, source.indexOf('?'));	//remove suffix
						}
						
						if(source.indexOf('#') != -1){
							source = source.substring(0, source.indexOf('#'));	//remove suffix
						}
						
						if(source.indexOf('/') != -1){
							source = source.substring(0, source.indexOf('/'));	//remove suffix
						}
						
						/*if(source.charAt(source.length()-1) == '/'){
							source = source.substring(0, source.length()-1);
						}*/

						JSONArray linkarray = (JSONArray)json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").getJSONObject("HTTP-Response-Metadata").getJSONObject("HTML-Metadata").getJSONArray("Links");
						for(int i=0; i<linkarray.length(); i++){
							String path = (String)linkarray.getJSONObject(i).getString("path");
							String link = (String)linkarray.getJSONObject(i).getString("url");
							if(path.equals("A@/href") && !link.isEmpty()){
								if(link.length() >=7 && link.substring(0, 7).equals("http://")){
									link = link.substring(7);
								}
								else if(link.length() >=8 && link.substring(0, 8).equals("https://")){
									link = link.substring(8);
								}
								else if(link.length() >=2 && link.substring(0, 2).equals("//")){
									//link = link.substring(2);
									continue;
								}
								else if(link.charAt(0) == '#' || (link.length() >= 10 && link.substring(0, 10).equals("javascript"))){
									continue;
								}
								else if(link.charAt(0) == '/' || link.charAt(0) == '.'){
									//String prefix = source.substring(0, source.indexOf('/'));
									//link = prefix + link;
									continue;
								}
								
								if(link.indexOf('?') != -1){
									link = link.substring(0, link.indexOf('?'));	//remove suffix
								}
								
								if(link.indexOf('#') != -1){
									link = link.substring(0, link.indexOf('#'));	//remove suffix
								}
								
								if(link.indexOf('/') != -1){
									link = link.substring(0, link.indexOf('/'));	//remove suffix
								}
								
								/*if(link.charAt(link.length()-1) == '/'){
									link = link.substring(0, link.length()-1);
								}*/
								
								if(link.contains(source) || source.contains(link)){
									continue;
								}

								outKey.set(source + "\t" + link);
								context.write(outKey, outVal);
							}
						}
					} catch (JSONException ex) {
						// If we reach here, the JSON object didn't have the header we were looking for
					}
				}
				catch (Exception ex) {
					LOG.error("Caught Exception", ex);
					context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				}
			}
		}
	}
	protected static class URLReducer extends Reducer<Text, LongWritable, Text, Text> {
	    public void reduce(Text key, Iterable<LongWritable> values, Context context) 
	    	throws IOException, InterruptedException
	    {
	        long sum = 0;
	        for (LongWritable x : values) {
	            sum += x.get();
	        }
	        if(sum > 0){
	        	String[] parts = key.toString().split("\t");
	        	try{
	        		context.write(new Text(parts[0]), new Text(parts[1]));
	        	}
	        	catch(ArrayIndexOutOfBoundsException e){
	        		System.out.println(e.toString() + " " + key.toString());
	        	}
	        }
	        
	        //context.write(key, new LongWritable(sum));
	    }
	}
}
