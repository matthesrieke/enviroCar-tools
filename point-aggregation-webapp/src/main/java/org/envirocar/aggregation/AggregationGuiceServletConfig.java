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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;

public class AggregationGuiceServletConfig extends GuiceServletContextListener {

	@Override
	protected Injector getInjector() {
		ServiceLoader<Module> loader = ServiceLoader.load(Module.class);

		List<Module> modules = new ArrayList<Module>();
		for (Module module : loader) {
			modules.add(module);
		}

		modules.add(new ServletModule() {

			@Override
			protected void configureServlets() {
				serve("/receiveTrack/*").with(ReceiveTracksServlet.class);
				serve("/receiveTrack").with(ReceiveTracksServlet.class);
				serve(AggregatedTracksServlet.PATH.concat("/*")).with(AggregatedTracksServlet.class);
				serve(AggregatedTracksServlet.PATH).with(AggregatedTracksServlet.class);
			}

		});

		return Guice.createInjector(modules);
	}
}
