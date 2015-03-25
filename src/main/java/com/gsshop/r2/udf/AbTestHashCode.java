package com.gsshop.r2.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * pcid hashcode기반 A/B테스트 분기비율 적용값을 산출한다. 
 * @author admin
 *
 */
public class AbTestHashCode extends UDF {

	public AbTestHashCode() {
	}

	public Text evaluate(Text pcid) {

		String pcidStr = pcid.toString();

		int pcidHashCode = pcidStr.hashCode();
		
		//System.out.println("-> udf hash : " + pcidHashCode);
		
		String pcidHashCodeStr = String.valueOf(pcidHashCode);
		int pcidHashCodeStrLen = pcidHashCodeStr.length();
		// 끝에 두자리만 취한다.
		String splitStr = pcidHashCodeStr.substring(pcidHashCodeStrLen - 2, pcidHashCodeStrLen);

		return new Text(splitStr);
	}
}
