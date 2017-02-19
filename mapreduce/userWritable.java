package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;



public class userWritable implements WritableComparable<userWritable>{
	private IntWritable useful;
	private IntWritable funny;
	private IntWritable cool;
	private IntWritable rating;
	public userWritable(){
		set(0,0,0,0);
	}
	public userWritable(int u, int f, int c, int r){
		set(u,f,c,r);
	}
	private void set(int useful, int funny, int cool, int rating) {
		// TODO Auto-generated method stub
		this.useful=new IntWritable(useful);
		this.funny=new IntWritable(funny);
		this.cool=new IntWritable(cool);
		this.rating=new IntWritable(rating);
		
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		useful.readFields(arg0);
		funny.readFields(arg0);
		cool.readFields(arg0);
		rating.readFields(arg0);
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		useful.write(arg0);
		funny.write(arg0);
		cool.write(arg0);
		rating.write(arg0);
	}
	@Override
	public int compareTo(userWritable o) {
		// TODO Auto-generated method stub
		int cmp1=useful.compareTo(o.useful);
		if(cmp1!=0){
			return cmp1;
		}
		int cmp2=funny.compareTo(o.funny);
		if(cmp2!=0){
			return cmp2;
		}
		int cmp3=cool.compareTo(o.cool);
		if(cmp3!=0){
			return cmp3;
		}
		int cmp4=rating.compareTo(o.rating);
		if(cmp4!=0){
			return cmp4;
		}
		return 0;
	}
	 @Override 
	    public int hashCode(){ 
	        return useful.hashCode()*71+ funny.hashCode()*73+cool.hashCode()*127+rating.hashCode()*2; 
	    } 
	    
	    @Override 
	    public boolean equals (Object o ){ 
	        if ( o instanceof userWritable){ 
	             
	        	userWritable pw = (userWritable) o; 
	            boolean equals = useful.equals(pw.useful) && funny.equals(pw.funny) && cool.equals(pw.cool) && rating.equals(pw.rating); 
	            return equals; 
	        } 
	        return false; 
	    } 
	     
	    @Override 
	    public String toString(){ 
	        StringBuffer sb = new StringBuffer(); 
	        sb.append("["); 
	        sb.append("useful： "+useful+","); 
	        sb.append("funny: "+funny+","); 
	        sb.append("cool： "+cool+",");
	        sb.append("rating: "+rating);
	        sb.append("]"); 
	        return sb.toString(); 
	    }
	    
	    public int getU(){
	    	return useful.get();
	    }
	    public int getF(){
	    	return funny.get();
	    }
	    public int getC(){
	    	return cool.get();
	    }
	    public int getR(){
	    	return rating.get();
	    }
	
}