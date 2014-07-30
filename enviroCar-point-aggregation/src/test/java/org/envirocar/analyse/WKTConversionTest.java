package org.envirocar.analyse;

import static org.junit.Assert.assertTrue;

import org.envirocar.analyse.util.Utils;
import org.junit.Test;

public class WKTConversionTest {

	
	@Test
	public void testWKTConversion(){
		
		double[] convertedWKT = Utils.convertWKTPointToXY("POINT(7.607246 51.960133)");
		
		assertTrue(convertedWKT[0] == 7.607246);
		assertTrue(convertedWKT[1] == 51.960133);
		
	}
	
}
