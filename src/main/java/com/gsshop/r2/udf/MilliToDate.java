package com.gsshop.r2.udf;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MilliToDate extends UDF {

	private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

	public MilliToDate() {
	}

	public Text evaluate(Text millisecond, Text dateFormat) {
		Text result = null;
		if (millisecond != null) {
			if (dateFormat == null) {
				dateFormat = new Text(DEFAULT_FORMAT);
			}

			SimpleDateFormat df = new SimpleDateFormat(dateFormat.toString());
			String formattedDate = df.format(new Date(Long.parseLong(millisecond.toString())));

			result = new Text(formattedDate);
		}

		return result;
	}
}
