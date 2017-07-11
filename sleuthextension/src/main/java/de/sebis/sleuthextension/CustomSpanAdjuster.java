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

		JStatData data = JStatData.connect(pid());
		for (JStatData.Counter<?> c : data.getAllCounters().values()) {
			if (c.getName().equals("sun.gc.collector.0.time") || c.getName().equals("sun.gc.collector.1.time")
					|| c.getName().equals("sun.gc.collector.0.invocations")
					|| c.getName().equals("sun.gc.collector.1.invocations")) {
				String message = c.getName() + ";" + c.getUnits() + ";" + c.getVariability() + ";" + c.getValue();
				if (c instanceof StringCounter) {
					String val = (String) c.getValue();
					message = message + val;
				}
				span.tag(c.getName(), "" + c.getValue());
				System.out.println(message);
			}
		}

		return span;
	}

	private int pid() {
		String name = ManagementFactory.getRuntimeMXBean().getName();
		name = name.substring(0, name.indexOf("@"));
		return Integer.parseInt(name);
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
