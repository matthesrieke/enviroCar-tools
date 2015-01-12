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
package org.envirocar.aggregation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.envirocar.analyse.AggregationAlgorithm;
import org.envirocar.analyse.util.PointViaJsonMapIterator;
import org.envirocar.analyse.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;

@Singleton
public class ReceiveTracksServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4589872023160154399L;
	private static final Logger logger = LoggerFactory
			.getLogger(ReceiveTracksServlet.class);
	private static final String PRODUCERS_FILE = "/allowed_producers.cfg";
	private ExecutorService executor;

	private Set<String> allowedProducers;
	private AggregationAlgorithm algorithm;

	public ReceiveTracksServlet() {
		this.executor = Executors.newSingleThreadExecutor();
		this.algorithm = new AggregationAlgorithm();
	}

	public void init() throws ServletException {
		super.init();
		try {
			this.allowedProducers = FileUtil
					.readConfigFilePerLine(PRODUCERS_FILE);
		} catch (IOException e) {
			throw new ServletException(e);
		}

	}

	@Override
	public void destroy() {
		super.destroy();
		this.executor.shutdown();
	}


	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		final String contentType = req.getHeader("Content-Type");
		final InputStream stream = req.getInputStream();
		
		if (!(contentType != null && contentType.startsWith("application/json"))) {
			throw new IllegalArgumentException("Invalid ContentType");
		}
		
		final Map<?, ?> json = Utils.parseJsonStream(stream);
		
		if (verifyRemoteHost(req.getRemoteHost())) {
			this.executor.submit(new Runnable() {

				public void run() {
					PointViaJsonMapIterator it = new PointViaJsonMapIterator(json);
						
					algorithm.runAlgorithm(it, it.getOriginalTrackId());
				}
				
			});
		} else {
			logger.info("Host {} is not whitelisted. Ignoring request.",
					req.getRemoteHost());
		}

		resp.setStatus(204);
	}

	public synchronized Set<String> getAllowedProducers() {
		return this.allowedProducers;
	}

	private synchronized boolean verifyRemoteHost(String remoteHost) {
		for (String prod : getAllowedProducers()) {
			if (remoteHost.contains(prod)) {
				return true;
			}
		}
		return false;
	}

	protected String readContent(HttpServletRequest req) throws IOException {
		String enc = req.getCharacterEncoding();
		Scanner sc = new Scanner(req.getInputStream(), enc == null ? "utf-8"
				: enc);
		StringBuilder sb = new StringBuilder();

		while (sc.hasNext()) {
			sb.append(sc.nextLine());
		}

		sc.close();
		return sb.toString();
	}

	public static class LocalGuiceServletConfig extends
			GuiceServletContextListener {

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
					serve("/*").with(ReceiveTracksServlet.class);
				}

			});

			return Guice.createInjector(modules);
		}
	}

}
