/**
 * Copyright (C) 2013
 * by Matthes Rieke
 *
 * Contact: http://matthesrieke.github.io
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
package org.envirocar.geojson;

public class Feature extends AbstractObject {

	private FeatureProperties properties = new FeatureProperties();
	private AbstractObject geometry;
	private String id;

	public AbstractObject getGeometry() {
		return geometry;
	}

	public void setGeometry(AbstractObject geometry) {
		this.geometry = geometry;
	}

	public FeatureProperties getProperties() {
		return properties;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
