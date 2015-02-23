package com.gsshop.r2.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class PrdDealListFromOrderInform extends UDF {
	public PrdDealListFromOrderInform(){
		
	}
	public Text evaluate(Text contents, Text type, Text delim) {
		String eventline = contents.toString();
		String contenttype = type.toString();
		String delimeter = delim.toString();
		Text result=null;
		String rst="";
		if(contenttype.equals("ordprd")){
			String[] ords = eventline.split("[|]");
			
			for(String aord : ords){
				if(rst.equals("")) rst=aord.split(",")[0];
				else rst=rst+delimeter+ aord.split(",")[0];
			}
		}else if(contenttype.equals("cartprd")){
			String[] carts = eventline.split("[|]");
			for(String acart : carts){
				if(rst.equals("")) rst=acart.split(",")[0];
				else rst=rst+delimeter+ acart.split(",")[0];
			}
		}else if(contenttype.equals("orddeal")){
			String[] ords = eventline.split("[|]");
			
			for(String aord : ords){
				if(rst.equals("")) rst=aord.split(",")[3];
				else rst=rst+delimeter+ aord.split(",")[3];
			}
		}else if(contenttype.equals("cartdeal")){
			String[] carts = eventline.split("[|]");
			for(String acart : carts){
				if(rst.equals("")) rst=acart.split(",")[1];
				else rst=rst+delimeter+ acart.split(",")[1];
			}
		}
		result=new Text(rst);
		return result;
	}
}
