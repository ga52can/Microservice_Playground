package de.sebis.sleuthextension;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.web.ErrorController;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.core.MethodParameter;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

public class CustomTraceHandlerInterceptor extends HandlerInterceptorAdapter {
	private final BeanFactory beanFactory;

	private Tracer tracer;
	private TraceKeys traceKeys;
	private AtomicReference<ErrorController> errorController;

	public CustomTraceHandlerInterceptor(BeanFactory beanFactory, Tracer tracer) {
		this.beanFactory = beanFactory;
		this.tracer = tracer;
	}
	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws Exception {
		
		tracer.addTag("interceptorTag", "preHandle");
			
//		System.out.println("Custom Trace handler triggert");
//		// "/{service}/{route_id}/book"
//		String matchingPattern = (String) request
//		                    .getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
		
//		System.out.println(matchingPattern);
		// "service" => "fooService", "route_id" => "42"
		Map<String, String> templateVariables = (Map<String, String>) request
		                    .getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);

		for(String key: templateVariables.keySet()){
//			System.out.println("inter.param."+key+": "+templateVariables.get(key));
			tracer.addTag("inter.param."+key, templateVariables.get(key));
		}
		
		for(String name : request.getParameterMap().keySet()){
			tracer.addTag("inter.param."+name, request.getParameter(name));
//			System.out.println("inter.param."+name+": "+request.getParameter(name));
		}
		
		
//		if (handler instanceof HandlerMethod) {
//			HandlerMethod handlerMethod = (HandlerMethod) handler;
//			System.out.println("interceptor - short log message: "+handlerMethod.getShortLogMessage());
//			System.out.println("interceptor - bean: "+handlerMethod.getBean());
//			System.out.println("interceptor - bean type: "+handlerMethod.getBeanType());
//			System.out.println("interceptor - class: "+handlerMethod.getClass().getSimpleName());
//			System.out.println("interceptor - method: "+handlerMethod.getMethod().getName());
//			for(MethodParameter param: handlerMethod.getMethodParameters()){
//				System.out.println("interceptor - param: "+param.getParameterName());
//				System.out.println("interceptor - param: "+param.toString());
//				System.out.println("interceptor - param: "+param.getMethod().getName());
//				System.out.println("interceptor - param: "+param.getParameterType());
//			}
//			
//			
//			System.out.println(": "+handlerMethod.getReturnType());
//			
//		
//		}
		
	
		return super.preHandle(request, response, handler);
		
		
	}
	
	
	
//	private void addClassMethodTag(Object handler, Span span) {
//		if (handler instanceof HandlerMethod) {
//			String methodName = ((HandlerMethod) handler).getMethod().getName();
//			getTracer().addTag(getTraceKeys().getMvc().getControllerMethod(), methodName);
//			if (log.isDebugEnabled()) {
//				log.debug("Adding a method tag with value [" + methodName + "] to a span " + span);
//			}
//		}
//	}
//
//	private void addClassNameTag(Object handler, Span span) {
//		String className;
//		if (handler instanceof HandlerMethod) {
//			className = ((HandlerMethod) handler).getBeanType().getSimpleName();
//		} else {
//			className = handler.getClass().getSimpleName();
//		}
//		if (log.isDebugEnabled()) {
//			log.debug("Adding a class tag with value [" + className + "] to a span " + span);
//		}
//		getTracer().addTag(getTraceKeys().getMvc().getControllerClass(), className);
//	}
	
	@Override
	public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
			ModelAndView modelAndView) throws Exception {
		
		tracer.addTag("interceptorTag", "postHandle");
		
		super.postHandle(request, response, handler, modelAndView);
	}

}
