package org.apache.mahout.classifier;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.compress.utils.Charsets;
import org.apache.mahout.classifier.sgd.L2;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.Dictionary;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class YelpClassification {
	//@test
	public void yelpData() throws IOException{
		//Snip ...
		RandomUtils.useTestSeed();
		Splitter onSpace = Splitter.on(" ");
		
		// read the data 
		List raw = Resources.readLines(Resources.getResource("yelp.csv"), Charsets.UTF_8);
		
		//holds feature
		List data = Lists.newArrayList();
		
		//holds target variable 
		List target = Lists.newArrayList();
		
		//for decoding target values 
		Dictionary dict = new Dictionary();
		
		//for permuting data later 
		List order = Lists.newArrayList();
		
		List<String> rawSublist = raw.subList(1, raw.size());
		for( String line : rawSublist){
			// order gets a list of indexes 
			order.add(order.size());
			
			// parse the predictor variables 
			Vector v = new DenseVector(5);
			v.set(0, 1);
			int i = 1;
			Iterable<String> values = onSpace.split(line);
			for(String value: Iterables.limit(values, 4)){
				//v.set(0, 1);
				//int i = 1;
				//Iterable values = onSpace.split(line);
				//for(String value : Iterables.limit(value, 4)){
					v.set(i++, Double.parseDouble(value));
				}
				data.add(v);
				
				// and the target 
				target.add(dict.intern(Iterables.get(values, 4).toString()));
			}
			
			//randomize the order ... original data has each species all together 
			//note that this randomization is deterministic
			Random random = RandomUtils.getRandom();
			Collections.shuffle(order, random);
			
			//select training and test data 
			List train = order.subList(0, 100);
			List test = order.subList(100, 150);
			//logger.warn("Training set = {}", train);
			//logger.warn("Test set = {}", test);
		
		
		//now train many times and collect information on accuracy each time 
		int[] correct = new int[test.size()+1];
		for(int run=0; run<200; run++){
			OnlineLogisticRegression lr = new OnlineLogisticRegression(3, 5, new L2(1));
			// 30 training passes should converge to >95% accuracy nearly always but never to 100%
			for(int pass=0; pass<30; pass++){
				Collections.shuffle(train, random);
				for(int k=0; k<train.size(); k++){
					lr.train((Integer)target.get(k), (Vector)data.get(k));
				}
			}
			
			// check the accuracy on held out data 
			int x = 0;
			int[] count = new int[3];
			for(int k=0; k<test.size(); k++){
				int r = lr.classifyFull((Vector)data.get(k)).maxValueIndex();
				count[r]++;
				//r==((Integer)target.get(k)) ? 1:0;
				x += (r==((Integer)target.get(k)) ? 1:0);
			}
			correct[x]++;
/**			
			for(int i=0; i<Math.floor(0.95*test.size()); i++){
				assertEquals(String.format("%d trials had unacceptable accuracy of only %. of %%: ", correct[i], 100.0*i/test.size()), 0, correct[i] );
			}
			// nor perfect 
			assertEquals(String.format("%d trails had unrealistic accuracy of 100%", correct[test.size()-1]), 0, correct[test.size()]);
**/
		}
	}
		
}
