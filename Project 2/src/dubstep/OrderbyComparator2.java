package dubstep;

import java.util.Comparator;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;

public class OrderbyComparator2 implements Comparator<PrimitiveValue[]> {
	
	int[] orderby;
	boolean[] order;
	
	public OrderbyComparator2(int[] orderby,boolean[] order)
	{
		this.orderby = orderby;
		this.order = order;
	}

	@Override
	public int compare(PrimitiveValue[] r1, PrimitiveValue[] r2) {
		for(int i =0;i<orderby.length;i++)
		{
			int ret = 0;
			PrimitiveValue col1 = r1[orderby[i]];
			PrimitiveValue col2 = r2[orderby[i]];
			if(col1 instanceof LongValue)
			{
				try {
					ret =  (Long.valueOf(col1.toLong()).compareTo(Long.valueOf(col2.toLong())));
				} catch (InvalidPrimitive e) {
					e.printStackTrace();
				}
			}
			else if(col1 instanceof DoubleValue)
			{
				try {
					ret =  (Double.valueOf(col1.toDouble()).compareTo(Double.valueOf(col2.toDouble())));
				} catch (InvalidPrimitive e) {
					e.printStackTrace();
				}
			}
			else if(col1 instanceof StringValue)
			{
				ret = col1.toString().compareTo(col2.toString());
			}
			else if(col1 instanceof DateValue)
			{
				ret = ((DateValue)col1).getValue().compareTo(((DateValue)col2).getValue());
			}
			if(ret != 0)
			{
				if(order[i])
					return ret;
				else
					return -1*ret;
			}
		}
		return 0;
	}
}
