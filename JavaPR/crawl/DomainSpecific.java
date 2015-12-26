package crawl;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class DomainSpecific {
	private static final Logger LOG = Logger.getLogger(DomainSpecific.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		NO_SERVER,
		EXCEPTIONS
	}

	protected static class URLMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);
		private ArrayList<String> domains = new ArrayList<String>();
		private int numDomains;
		
		public String findLink(String source, String link){
			//in case of ../
			boolean i = true;
			String currdir, updir = "";
			currdir = source;
			while(i){
				if(source.endsWith(".htm") || source.endsWith(".html") || source.endsWith(".php")){
					link = link.substring(3);
					currdir = currdir.substring(0, currdir.lastIndexOf("/"));
					updir = currdir.substring(0, currdir.lastIndexOf("/")); 
					if(!link.startsWith("../")){
						i = false;
						link = updir + "/" + link;
					}
				}
				else{
					i = false;
				}
			}
			return link;
		}
		
		@Override
		public void setup(Context context) throws IOException{
			Configuration c = context.getConfiguration();
			numDomains = c.getInt("numDomains", 0);
			for(int i=0; i<numDomains; i++){
				String dom = c.get("domain"+(i+1));
				domains.add(dom);
			}
		}
		
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
					String currDom = "";
					
					JSONObject json = new JSONObject(content);
					try {
						String source = json.getJSONObject("Envelope").getJSONObject("WARC-Header-Metadata").getString("WARC-Target-URI");
						int j=0;
						for(String dom : domains){
							if(source.contains(dom)){
								j = 1;
								currDom = dom;
								break;
							}
						}
						if(j == 0){
							continue;
						}
						
						if(source.startsWith("http://")){
							source = source.substring(7);
						}
						else if(source.startsWith("https://")){
							source = source.substring(8);
						}
						
						if(source.indexOf('?') != -1){
							source = source.substring(0, source.indexOf('?'));	//remove suffix
						}
						
						if(source.indexOf('#') != -1){
							source = source.substring(0, source.indexOf('#'));	//remove suffix
						}
						
						if(source.indexOf('%') != -1){
							source = source.substring(0, source.indexOf('%'));	//remove suffix
						}
						
						if(source.charAt(source.length()-1) == '/'){
							source = source.substring(0, source.length()-1);
						}
						
						if(source.length() == 0){
							continue;
						}

						JSONArray linkarray = (JSONArray)json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").getJSONObject("HTTP-Response-Metadata").getJSONObject("HTML-Metadata").getJSONArray("Links");
						for(int i=0; i<linkarray.length(); i++){
							String path = (String)linkarray.getJSONObject(i).getString("path");
							String link = (String)linkarray.getJSONObject(i).getString("url");
							if(path.equals("A@/href") && !link.isEmpty()){
								if(link.startsWith("http://")){
									link = link.substring(7);
								}
								else if(link.startsWith("https://")){
									link = link.substring(8);
								}
								else if(link.startsWith("//")){
									link = link.substring(2);
								}
								else if(link.charAt(0) == '#' || link.startsWith("mailto") || link.startsWith("javascript")){
									continue;
								}
								else if(link.charAt(0) == '/'){
									String prefix = source.substring(0, source.indexOf('/'));
									link = prefix + link;
								}
								else if((!link.contains("/")) && (link.endsWith(".htm") || link.endsWith(".html") || link.endsWith(".php"))){
									String prefix = source.substring(0, source.lastIndexOf('/')+1);
									link = prefix + link;
								}
								else if(link.startsWith("../")){
									link = findLink(source, link);
								}
								
								if(!link.contains(currDom)){
									continue;
								}
								
								if(link.indexOf('?') != -1){
									link = link.substring(0, link.indexOf('?'));	//remove suffix
								}
								
								if(link.indexOf('#') != -1){
									link = link.substring(0, link.indexOf('#'));	//remove suffix
								}
								
								if(link.indexOf('%') != -1){
									link = link.substring(0, link.indexOf('%'));	//remove suffix
								}
								
								if(link.charAt(link.length()-1) == '/'){
									link = link.substring(0, link.length()-1);
								}
								
								if(link.equals(source)){
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
		private MultipleOutputs<Text, Text> mos;
		private ArrayList<String> domains = new ArrayList<String>();
		private int numDomains;
		
		@Override
        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, Text>(context);
            Configuration c = context.getConfiguration();
			numDomains = c.getInt("numDomains", 0);
			for(int i=0; i<numDomains; i++){
				String dom = c.get("domain"+(i+1));
				domains.add(dom);
			}
        }
		
		@Override
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
	        		for(String dom : domains){
		        		if(parts[0].contains(dom)){
	        				String dm = dom.substring(0, dom.indexOf(".")); 
	        				mos.write(dm, new Text(parts[0]), new Text(parts[1]));
	        			}
	        		}
	        	}
	        	catch(ArrayIndexOutOfBoundsException e){
	        		System.out.println(e.toString() + " " + key.toString());
	        	}
	        }
	    }
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			mos.close();
		}
	}
}
