package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class Row implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	PrimitiveValue[] coldata;
	int sortInd;
	String filename;
	
	Row(int x)
	{
		coldata = new PrimitiveValue[x];
		sortInd = 0;
		filename = "";
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public Row(PrimitiveValue[] r, int secsortInd) {
		coldata = r;
		sortInd = secsortInd;
	}

	public int getSortInd() {
		return sortInd;
	}

	public void setSortInd(int sortInd) {
		this.sortInd = sortInd;
	}

	public PrimitiveValue[] getColdata() {
		return coldata;
	}

	public void setColdata(PrimitiveValue[] coldata) {
		this.coldata = coldata;
	}

}
