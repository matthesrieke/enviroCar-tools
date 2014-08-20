/**
 * Copyright (C) 2014
 * by 52 North Initiative for Geospatial Open Source Software GmbH
 *
 * Contact: Andreas Wytzisk
 * 52 North Initiative for Geospatial Open Source Software GmbH
 * Martin-Luther-King-Weg 24
 * 48155 Muenster, Germany
 * info@52north.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.envirocar.analyse.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.entities.InMemoryPoint;
import org.envirocar.analyse.entities.Point;


public class PointViaJsonMapIterator implements Iterator<Point> {

	private List<Object> features;
	private int index = 0;
	private String trackID;

	@SuppressWarnings("unchecked")
	public PointViaJsonMapIterator(Map<?, ?> json) throws IOException {
		this.features = (List<Object>) json.get("features");
		Map<?, ?> properties = (Map<?, ?>) json.get("properties");
		if (properties != null) {
			this.trackID = (String) properties.get("id");
		}

		if (this.features == null || this.features.isEmpty()
				|| this.trackID == null) {
			throw new IOException("Not a valid enviroCar track");
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

	public String getOriginalTrackId() {
		return this.trackID;
	}

}
