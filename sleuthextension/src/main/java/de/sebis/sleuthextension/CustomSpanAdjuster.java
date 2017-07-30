package de.sebis.sleuthextension;

import java.lang.management.ManagementFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.gridkit.lab.jvm.perfdata.JStatData;
import org.gridkit.lab.jvm.perfdata.JStatData.StringCounter;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanAdjuster;

public class CustomSpanAdjuster implements SpanAdjuster {

	@Override
	public Span adjust(Span span) {

		try {
			span.tag("cpu.system.utilization", Double.toString(getSystemCpuLoad()));
			span.tag("cpu.process.utilization", Double.toString(getProcessCpuLoad()));
		} catch (Exception e) {
			span.tag("cpu.system.utilization", e.getMessage());
			span.tag("cpu.process.utilization", e.getMessage());
			System.out.println("Issue while trying to access cpu utilization information");
		}

		
        int mb = 1024*1024;
		
		Runtime runtime = Runtime.getRuntime();
		
		Long totalMemory = runtime.totalMemory()/ mb;
		Long freeMemory = runtime.freeMemory()/ mb;
		Long maxMemory = runtime.maxMemory()/mb;
		Long usedMemory = totalMemory - freeMemory;
		Long memoryUtilization = (long) ((usedMemory.doubleValue()/maxMemory.doubleValue())*100);
		
		span.tag("jvm.usedMemory.mb", usedMemory+"");
		span.tag("jvm.totalMemory.mb", totalMemory+"");
		span.tag("jvm.freeMemory.mb", freeMemory+"");
		span.tag("jvm.maxMemory.mb", maxMemory+"");
		System.out.println(memoryUtilization);
		span.tag("jvm.memoryUtilization", memoryUtilization+"");

		return span;
	}



	public static double getProcessCpuLoad() throws Exception {

		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
		AttributeList list = mbs.getAttributes(name, new String[] { "ProcessCpuLoad" });

		if (list.isEmpty())
			return Double.NaN;

		Attribute att = (Attribute) list.get(0);
		Double value = (Double) att.getValue();

		// usually takes a couple of seconds before we get real values
		if (value == -1.0)
			return Double.NaN;
		// returns a percentage value with 1 decimal point precision
		return ((int) (value * 1000) / 10.0);
	}


	public static double getSystemCpuLoad() throws Exception {

		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
		AttributeList list = mbs.getAttributes(name, new String[] { "SystemCpuLoad" });

		if (list.isEmpty())
			return Double.NaN;

		Attribute att = (Attribute) list.get(0);
		Double value = (Double) att.getValue();

		// usually takes a couple of seconds before we get real values
		if (value == -1.0)
			return Double.NaN;
		// returns a percentage value with 1 decimal point precision
		return ((int) (value * 1000) / 10.0);
	}

}
