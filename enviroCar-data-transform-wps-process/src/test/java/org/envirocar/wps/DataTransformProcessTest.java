/**
 * Copyright (C) 2013
 * by 52 North Initiative for Geospatial Open Source Software GmbH
 *
 * Contact: Andreas Wytzisk
 * 52 North Initiative for Geospatial Open Source Software GmbH
 * Martin-Luther-King-Weg 24
 * 48155 Muenster, Germany
 * info@52north.org
 *
 * This program is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public License version 2 as published by the
 * Free Software Foundation.
 *
 * This program is distributed WITHOUT ANY WARRANTY; even without the implied
 * WARRANTY OF MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program (see gnu-gpl v2.txt). If not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA or
 * visit the Free Software Foundation web page, http://www.fsf.org.
 */
package org.envirocar.wps;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.junit.Test;

public class DataTransformProcessTest {

	@Test
	public void testDataTransform(){
		try {
			
			URL url = new URL("https://envirocar.org/api/stable/tracks/53433169e4b09d7b34fa824a");
			
			SimpleFeatureCollection sft = new DataTransformProcess().createFeaturesFromJSON(url);
			
			assertTrue(sft.size() == 449);
		} catch (IOException e) {
			fail(e.getMessage());
		}
		
	}
	
}
