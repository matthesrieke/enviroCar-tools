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
package org.envirocar.analyse;
/**
 * Copyright 2014 52°North Initiative for Geospatial Open Source
 * Software GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.entities.Point;
import org.envirocar.analyse.util.PointViaJsonMapIterator;
import org.envirocar.analyse.util.Utils;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

public class PointViaJsonIteratorTest {
	
	@Test
	public void testProcessing() throws IOException {
		Map<?, ?> json = Utils.parseJsonStream(getClass().getResourceAsStream("/dummy-track.json"));
		
		Iterator<Point> it = new PointViaJsonMapIterator(json);
		
		List<Point> result = new ArrayList<Point>();
		
		while (it.hasNext()) {
			result.add(it.next());
		}
		
		Assert.assertThat(result.size(), is(3));
		
		Point p = result.get(0);
		Assert.assertThat(p.getX(), is(2.0));
		Assert.assertThat(p.getY(), is(2.1));
		
		p = result.get(1);
		Assert.assertThat(p.getX(), is(2.05));
		Assert.assertThat(p.getY(), is(2.15));
		
		p = result.get(2);
		Assert.assertThat(p.getX(), is(2.1));
		Assert.assertThat(p.getY(), is(2.2));
	}
	
}
