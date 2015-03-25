import org.apache.hadoop.io.Text;

import com.gsshop.r2.udf.AbTestHashCode;

public class Test {

	public static void main(String[] args) {
		
		String str = "121212121asdfadf";
		
		System.out.print("string hash : " + str.hashCode());
		
		
		
		Text text = new Text(str);
		
		AbTestHashCode abTestHashCode = new AbTestHashCode();
		abTestHashCode.evaluate(text);
	}
}
