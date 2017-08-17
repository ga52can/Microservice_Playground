package de.sebis.sleuthextension;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Enumeration;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.gridkit.lab.jvm.perfdata.JStatData;
import org.gridkit.lab.jvm.perfdata.JStatData.StringCounter;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.web.filter.GenericFilterBean;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerMapping;

public class CustomFilter extends GenericFilterBean {

	private final Tracer tracer;

	public CustomFilter(Tracer tracer) {
		this.tracer = tracer;
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		// System.out.println("CustomFilter triggert");

		for (String name : request.getParameterMap().keySet()) {
			tracer.addTag("servlet.param." + name, request.getParameter(name));
			// System.out.println("filter.param."+name+":
			// "+request.getParameter(name));
		}

		try {
			tracer.addTag("cpu.system.utilizationAvgLastMinute",
					Double.toString(ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage()));
		} catch (Exception e) {
			tracer.addTag("cpu.system.utilization", e.getMessage());
			System.out.println("Issue while trying to access cpu utilization information");
		}

		int mb = 1024 * 1024;

		Runtime runtime = Runtime.getRuntime();

		Long totalMemory = runtime.totalMemory() / mb;
		Long freeMemory = runtime.freeMemory() / mb;
		Long maxMemory = runtime.maxMemory() / mb;
		Long usedMemory = totalMemory - freeMemory;
		Long memoryUtilization = (long) ((usedMemory.doubleValue() / maxMemory.doubleValue()) * 100);

		tracer.addTag("jvm.usedMemory.mb", usedMemory + "");
		tracer.addTag("jvm.totalMemory.mb", totalMemory + "");
		tracer.addTag("jvm.freeMemory.mb", freeMemory + "");
		tracer.addTag("jvm.maxMemory.mb", maxMemory + "");
		tracer.addTag("jvm.memoryUtilization", memoryUtilization + "");

		// System.out.println("Request - Local Address:" +
		// request.getLocalAddr());
		// System.out.println("Request - LocalName:" + request.getLocalName());
		// System.out.println("Request - Local Port:" + request.getLocalPort());
		// System.out.println("Request - Remote Address:" +
		// request.getRemoteAddr());
		// System.out.println("Request - Remote Host:" +
		// request.getRemoteHost());
		// System.out.println("Request - Remote Port:" +
		// request.getRemotePort());
		// System.out.println("Request - Protocol:" + request.getProtocol());
		// System.out.println("Request - Character Encoding:" +
		// request.getCharacterEncoding());
		// System.out.println("Request - Content Length:" +
		// request.getContentLengthLong());
		tracer.addTag("servlet.content_length", Long.toString(request.getContentLengthLong()));
		// Enumeration<String> attributeNames = request.getAttributeNames();
		// while(attributeNames.hasMoreElements()){
		// String name = attributeNames.nextElement();
		//// System.out.println("attr."+name+": " + request.getAttribute(name));
		// }

		if (request instanceof HttpServletRequest) {
			// System.out.println("It is a http request");
			HttpServletRequest httpRequest = (HttpServletRequest) request;
			
			
			if (httpRequest.getUserPrincipal() != null) {
				// System.out.println("HttpRequest - UserPrincipal.Name: " +
				// httpRequest.getUserPrincipal().getName());
				tracer.addTag("servlet.user_principal.name", httpRequest.getUserPrincipal().getName());
				 

			}
			
			

			
			String requestUri = httpRequest.getRequestURI();
			
			

			String infoParam = tracer.getCurrentSpan().tags().getOrDefault("servlet.param.info", "not set");

			if (infoParam.contains("maps-highCPU") && requestUri.equals("/maps-helper-service/distance")) {
				tracer.addTag("cpu.system.utilizationAvgLastMinute", "99");
			}
				

			
			// else{
			// System.out.println("HttpRequest - UserPrincipal.Name: null");
			// }
			// System.out.println("HttpRequest - AuthType:" +
			// httpRequest.getAuthType());
			// System.out.println("HttpRequest - Method:" +
			// httpRequest.getMethod());

			// System.out.println("HttpRequest - PathTranslated:" +
			// httpRequest.getPathTranslated());
			// System.out.println("HttpRequest - QueryString:" +
			// httpRequest.getQueryString());
			tracer.addTag("servlet.query_string", httpRequest.getQueryString());
			// System.out.println("HttpRequest - RemoteUser:" +
			// httpRequest.getRemoteUser());
			// System.out.println("HttpRequest - RequestUri:" +
			// httpRequest.getRequestURI());

			// Enumeration<String> headerNames = httpRequest.getHeaderNames();
			// while(headerNames.hasMoreElements()){
			// String name = headerNames.nextElement();
			//
			//// System.out.println("header."+name+": " +
			// httpRequest.getHeader(name));
			// }

			String headerReferer = httpRequest.getHeader("referer");
			if (headerReferer != null) {
				tracer.addTag("http.header.referer", headerReferer);
			}

		}
		chain.doFilter(request, response);

	}

}
