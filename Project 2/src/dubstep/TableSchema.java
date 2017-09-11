package dubstep;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class TableSchema {
	String[] headers;
	String[] dataType;
	Map<String,Integer> colnum=new HashMap<String,Integer>();
	int columns;

	public String[] getHeaders() {
		return headers;
	}

	public void setHeaders(String[] headers) {
		this.headers = headers;
	}
	
	public int getColumns() {
		return columns;
	}

	public void setColumns(int columns) {
		this.columns = columns;
	}

	public void getSchema(CreateTable table)
	{
		List<ColumnDefinition> coldef= table.getColumnDefinitions();
		this.headers = new String[coldef.size()];
		this.dataType = new String[coldef.size()];
		int count = 0;
		for(ColumnDefinition col : coldef)
		{
			this.headers[count] = col.getColumnName();
			this.dataType[count] = col.getColDataType().getDataType().toUpperCase();
			this.colnum.put(col.getColumnName(), count);
			count++;
		}
		this.columns = count;
	}

	public String getDataType(int i) {
		return this.dataType[i];
	}

	public void setDataType(String[] dataType) {
		this.dataType = dataType;
	}

	public Map<String, Integer> getColnum() {
		return colnum;
	}

	public void setColnum(Map<String, Integer> colnum) {
		this.colnum = colnum;
	}
}
