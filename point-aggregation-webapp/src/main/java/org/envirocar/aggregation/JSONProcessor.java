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
package org.envirocar.aggregation;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.entities.InMemoryPoint;
import org.envirocar.analyse.entities.Point;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class JSONProcessor {

	public Iterator<Point> initializeIterator(Map<?, ?> json) throws JsonParseException, JsonMappingException, IOException {
		return new PointViaJsonIterator(json);
	}
	
	public class PointViaJsonIterator implements Iterator<Point> {

		private List<Object> features;
		private int index = 0;
		private String trackID;
		
		@SuppressWarnings("unchecked")
		public PointViaJsonIterator(Map<?, ?> json) {
			this.features = (List<Object>) json.get("features");
			Map<?, ?> properties = (Map<?, ?>) json.get("properties");
			if (properties != null) {
				this.trackID = (String) properties.get("id");
			}
			
			if (this.features == null || this.features.isEmpty() || this.trackID == null) {
				throw new IllegalArgumentException("Not a valid enviroCar track");
			}
		}

		@Override
		public boolean hasNext() {
			return index < features.size();
		}

		@Override
		public Point next() {
			if (!hasNext()) {
				return null;
			}
			return InMemoryPoint.fromMap((Map<?, ?>) features.get(index++),
					this.trackID);
		}

		@Override
		public void remove() {
		}
		
	}

}
