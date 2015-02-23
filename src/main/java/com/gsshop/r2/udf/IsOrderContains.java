package com.gsshop.r2.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;

public class IsOrderContains extends UDF {
	
	public IsOrderContains() {
	}

	public BooleanWritable evaluate(Text prdid, Text orderInfo, BooleanWritable dividedealprd) {
		BooleanWritable result = new BooleanWritable(false);
		Boolean isdivide = dividedealprd.get();
		String targetprd = prdid.toString();
		if(orderInfo!=null){
			if(!orderInfo.toString().equals("")){
				String orderinfostr = orderInfo.toString();		
				String[] orders = orderinfostr.split("[|]");
				ArrayList<String> ordprds = new ArrayList<String>();
				
				for(String aord : orders){
					String[] aordinfo = aord.split(",");
					String prd = aordinfo[0];
					String deal = null;
					if(aordinfo.length==4) deal= aordinfo[3];
					if(isdivide){
						if(!ordprds.contains(prd)) ordprds.add(prd);
						if(deal!=null && !ordprds.contains(deal)) ordprds.add(deal);
					}else{
						if(deal!=null && !deal.equals("") && !deal.equals("0") && !deal.toLowerCase().equals("null")){
							if(!ordprds.contains(deal)) ordprds.add(deal);
						}else{
							if(!ordprds.contains(prd)) ordprds.add(prd);
						}
					}
				}
				if(ordprds.contains(targetprd)) result.set(true);
			}
		}

		return result;
	}

}
