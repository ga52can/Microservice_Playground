package com.sebis.mobility.config;

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
import org.springframework.web.servlet.HandlerMapping;

public class CustomFilter extends GenericFilterBean {

	private final Tracer tracer;
	
	public CustomFilter(Tracer tracer){
		this.tracer = tracer;
	}
	
	private int pid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        name = name.substring(0, name.indexOf("@"));
        return Integer.parseInt(name);
}
	
	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		System.out.println("CustomFilter triggert");
		
		JStatData data = JStatData.connect(pid());
    	for(JStatData.Counter<?> c: data.getAllCounters().values()) {
    		if(c.getName().equals("sun.gc.collector.0.time")||c.getName().equals("sun.gc.collector.1.time")||c.getName().equals("sun.gc.collector.0.invocations")||c.getName().equals("sun.gc.collector.1.invocations")){
    			String message = c.getName()+";"+c.getUnits()+";"+c.getVariability()+";"+c.getValue();
        		if (c instanceof StringCounter) {
        			String val = (String) c.getValue();
        			message = message + val;
        		}
        		tracer.addTag(c.getName(),""+ c.getValue());
        		System.out.println(message);
    		}
    		
    		
}
		try {
			tracer.addTag("cpu.system.utilization", Double.toString(getSystemCpuLoad()));
			tracer.addTag("cpu.process.utilization", Double.toString(getProcessCpuLoad()));
			System.out.println("Process CPU Utilization: "+getProcessCpuLoad());
			System.out.println("System CPU Utilization: "+getSystemCpuLoad());
		} catch (Exception e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			System.out.println("Issue while trying to access cpu utilization information");
		}
		
		for(String name : request.getParameterMap().keySet()){
			tracer.addTag("filter.param."+name, request.getParameter(name));
			System.out.println("filter.param."+name+": "+request.getParameter(name));
		}
//		System.out.println("Request - Local Address:" + request.getLocalAddr());
//		System.out.println("Request - LocalName:" + request.getLocalName());
//		System.out.println("Request - Local Port:" + request.getLocalPort());
//		System.out.println("Request - Remote Address:" + request.getRemoteAddr());
//		System.out.println("Request - Remote Host:" + request.getRemoteHost());
//		System.out.println("Request - Remote Port:" + request.getRemotePort());
//		System.out.println("Request - Protocol:" + request.getProtocol());
//		System.out.println("Request - Character Encoding:" + request.getCharacterEncoding());
//		System.out.println("Request - Content Length:" + request.getContentLengthLong());
		tracer.addTag("request.content_length", Long.toString(request.getContentLengthLong()));
//		Enumeration<String> attributeNames = request.getAttributeNames();
//		while(attributeNames.hasMoreElements()){
//			String name = attributeNames.nextElement();
////			System.out.println("attr."+name+": " + request.getAttribute(name));
//		}
		
		if (request instanceof HttpServletRequest) {
			System.out.println("It is a http request");
			HttpServletRequest httpRequest = (HttpServletRequest) request;
			if(httpRequest.getUserPrincipal() != null){
//				System.out.println("HttpRequest - UserPrincipal.Name: " + httpRequest.getUserPrincipal().getName());
				tracer.addTag("http.user_principal.name", httpRequest.getUserPrincipal().getName());
				
			}
//			else{
//				System.out.println("HttpRequest - UserPrincipal.Name: null");
//			}
//			System.out.println("HttpRequest - AuthType:" + httpRequest.getAuthType());
//			System.out.println("HttpRequest - Method:" + httpRequest.getMethod());
//			System.out.println("HttpRequest - PathInfo:" + httpRequest.getPathInfo());
//			System.out.println("HttpRequest - PathTranslated:" + httpRequest.getPathTranslated());
//			System.out.println("HttpRequest - QueryString:" + httpRequest.getQueryString());
			tracer.addTag("http.query_string", httpRequest.getQueryString());
//			System.out.println("HttpRequest - RemoteUser:" + httpRequest.getRemoteUser());
//			System.out.println("HttpRequest - RequestUri:" + httpRequest.getRequestURI());

//			Enumeration<String> headerNames = httpRequest.getHeaderNames();
//			while(headerNames.hasMoreElements()){
//				String name = headerNames.nextElement();
//				
////				System.out.println("header."+name+": " + httpRequest.getHeader(name));
//			}
			
			String headerReferer = httpRequest.getHeader("referer");
			if (headerReferer!=null){
				tracer.addTag("http.header.referer", headerReferer);
			}
			
			
			
		}
		chain.doFilter(request, response);

	}
	
	public static double getProcessCpuLoad() throws Exception {

	    MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
	    ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
	    AttributeList list = mbs.getAttributes(name, new String[]{ "ProcessCpuLoad" });

	    if (list.isEmpty())     return Double.NaN;

	    Attribute att = (Attribute)list.get(0);
	    Double value  = (Double)att.getValue();

	    // usually takes a couple of seconds before we get real values
	    if (value == -1.0)      return Double.NaN;
	    // returns a percentage value with 1 decimal point precision
	    return ((int)(value * 1000) / 10.0);
	}
	
	public static double getSystemCpuLoad() throws Exception {

	    MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
	    ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
	    AttributeList list = mbs.getAttributes(name, new String[]{ "SystemCpuLoad" });

	    if (list.isEmpty())     return Double.NaN;

	    Attribute att = (Attribute)list.get(0);
	    Double value  = (Double)att.getValue();

	    // usually takes a couple of seconds before we get real values
	    if (value == -1.0)      return Double.NaN;
	    // returns a percentage value with 1 decimal point precision
	    return ((int)(value * 1000) / 10.0);
	}

}
