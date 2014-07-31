package org.envirocar.analyse;

import org.junit.Test;

public class AggregationAlgorithmPostgresTest {

	@Test
	public void testAlgorithm(){
		
		double maxx = 7.6339;
		double maxy = 51.95;
		double minx = 7.6224;
		double miny = 51.94799;
		
//		double maxx = 7.6539;
//		double maxy = 51.96519;
//		double minx = 7.6224;
//		double miny = 51.94799;
		        
        new AggregationAlgorithm(minx, miny, maxx, maxy, 0.00045).runAlgorithm();
		
	}
}
