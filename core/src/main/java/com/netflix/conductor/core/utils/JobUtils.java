package com.netflix.conductor.core.utils;
import com.netflix.conductor.contribs.correlation.Correlator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobUtils {
	private static Logger logger = LoggerFactory.getLogger(JobUtils.class);
	private static final String JOB_ID_URN_PREFIX = "urn:deluxe:one-orders:deliveryjob:";
	private static final String SHRLK_JOB_ID_URN_PREFIX = "urn:deluxe:sherlock:jobid:";
	private static final String ORDER_ID_URN_PREFIX = "urn:deluxe:one-orders:order:";
	private static final String SHRLK_ORDER_ID_URN_PREFIX = "urn:deluxe:sherlock:orderid:";

	public static String getJobId(String correlationId) {
		Correlator correlator = new Correlator(logger, correlationId);

		String jobIdUrn = correlator.getContext().getUrn(JOB_ID_URN_PREFIX);
		if (StringUtils.isNotEmpty(jobIdUrn))
			return jobIdUrn.substring(JOB_ID_URN_PREFIX.length());

		String shrlkJobIdUrn = correlator.getContext().getUrn(SHRLK_JOB_ID_URN_PREFIX);
		if (StringUtils.isNotEmpty(shrlkJobIdUrn))
			return shrlkJobIdUrn.substring(SHRLK_JOB_ID_URN_PREFIX.length());

		return null;
	}

	public static String getOrderId(String correlationId) {
		Correlator correlator = new Correlator(logger, correlationId);

		String orderIdUrn = correlator.getContext().getUrn(ORDER_ID_URN_PREFIX);
		if (StringUtils.isNotEmpty(orderIdUrn))
			return orderIdUrn.substring(ORDER_ID_URN_PREFIX.length());

		String shrlkOrderIdUrn = correlator.getContext().getUrn(SHRLK_ORDER_ID_URN_PREFIX);
		if (StringUtils.isNotEmpty(shrlkOrderIdUrn))
			return shrlkOrderIdUrn.substring(SHRLK_ORDER_ID_URN_PREFIX.length());

		return null;
	}
}
