package dubstep;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
//import java.util.regex.Pattern;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class QueryOutput {
	public enum functions{MIN,MAX,COUNT,SUM,AVG};
	public enum datatype{DECIMAL,INT,DATE,STRING,VARCHAR,CHAR};
	String[] splitline;
	public void getOutput(PlainSelect select,Map<String, TableSchema> tables) {
    	Table table = (Table) select.getFromItem();
    	String file = "data/"+table.getName()+".csv";
    	List<SelectItem> selectclauses = select.getSelectItems();
    	boolean function = false;
    	@SuppressWarnings({ "unchecked", "rawtypes" })
		List<Expression> funcexp = new ArrayList();
    	int funclist[] = new int[selectclauses.size()];
    	PrimitiveValue functionouput[] = null;
    	int count=0;
    	for(SelectItem selectclause : selectclauses)
    	{
    		if(((SelectExpressionItem)selectclause).getExpression() instanceof Function)
    		{
    			function = true;
    			
    			switch(functions.valueOf(((Function)((SelectExpressionItem)selectclause).getExpression()).getName()))
    			{
    			case MAX:
    				funclist[count]=0;
    				funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
    				break;
    			case MIN:
    				funclist[count]=1;
    				funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
    				break;
    			case COUNT:
    				funclist[count]=2;
    				funcexp.add(((SelectExpressionItem)selectclause).getExpression());
    				break;
    			case SUM:
    				funclist[count]=3;
    				funcexp.add(((Function)((SelectExpressionItem)selectclause).getExpression()).getParameters().getExpressions().get(0));
    				break;
    			case AVG:
    				break;
    			}
    			count++;
    		}
    	}
    	
    	functionouput = new PrimitiveValue[funcexp.size()];
		
    	try 
		{
    		TableSchema tblschema = tables.get(((Table)select.getFromItem()).getName());
    		splitline = new String[tblschema.getColumns()];
    		
    		Eval eval = new Eval() 
			{
				public PrimitiveValue eval(Column c)
				{ 
					String temp = splitline[tblschema.getColnum().get(c.getColumnName())];
					PrimitiveValue result;
					switch(datatype.valueOf(tblschema.getDataType(tblschema.getColnum().get(c.getColumnName()))))
					{
					case DECIMAL:
						result = new DoubleValue(temp);
						break;							
					case INT:
						result = new LongValue(temp);
						break; 
					case DATE:
						result = new DateValue(temp);
						break;
					default :
						result = new StringValue(temp);
					}
					
					return result;
				}
			};
    		
		    File fileRead = new File(file);
		    BufferedReader reader = new BufferedReader(new FileReader(fileRead));
		    //Pattern pat=Pattern.compile("\\|");
		    String line;
		    
		    while ((line = reader.readLine()) != null) 
			{		
		    	//splitline = pat.split(line);
		    	
		        line = line + "|";
		        int temp1 = 0;
		        int temp2 =-1;
                int temp3 = 0;
		        while( (temp2 = line.indexOf( '|', temp1 )) != -1 && temp3 < tblschema.getColumns()+1 ){
		            if( temp2 == temp1 ){
		            	splitline[temp3++] = "NA";
		            } else {
		            	splitline[temp3++] = line.substring( temp1, temp2 );
		            }
		            temp1 = temp2 + 1;
		        }
		    		    	
		    	if(select.getWhere() != null)
		    	{
		    		if(!eval.eval(select.getWhere()).toBool())
		    			continue;
		    	}

				if(function)
				{					
					for(int funcCnt=0;funcCnt<funclist.length;funcCnt++)
					{
						if(funclist[funcCnt] == 0)
						{
							PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
							if(functionouput[funcCnt] == null)
								functionouput[funcCnt] = temp;
							//else
							//{
								/*if(functionouput[funcCnt] instanceof LongValue)
								{
									if(functionouput[funcCnt].toLong() < temp.toLong())
										functionouput[funcCnt] = temp;
								}
								if(functionouput[funcCnt] instanceof DoubleValue)
								{*/
							else if(functionouput[funcCnt].toDouble() < temp.toDouble())
										functionouput[funcCnt] = temp;
								/*}
								if(functionouput[funcCnt] instanceof StringValue)
								{
									if(functionouput[funcCnt].toString().compareTo(temp.toString()) < 0)
										functionouput[funcCnt] = temp;
								}
								if(functionouput[funcCnt] instanceof DateValue)
								{
									if(((DateValue)functionouput[funcCnt]).getValue().compareTo(((DateValue)temp).getValue()) < 0 )
										functionouput[funcCnt] = temp;
								}*/
							//}
						}
						if(funclist[funcCnt] == 1)
						{
							PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
							if(functionouput[funcCnt] == null)
								functionouput[funcCnt] = temp;
							//else
							//{
								/*if(functionouput[funcCnt] instanceof LongValue)
								{
									if(functionouput[funcCnt].toLong() > temp.toLong())
										functionouput[funcCnt] = temp;
								}
								if(functionouput[funcCnt] instanceof DoubleValue)
								{*/
							else if(functionouput[funcCnt].toDouble() > temp.toDouble())
										functionouput[funcCnt] = temp;
								/*}
								if(functionouput[funcCnt] instanceof StringValue)
								{
									if(functionouput[funcCnt].toString().compareTo(temp.toString()) > 0)
										functionouput[funcCnt] = temp;
								}
								if(functionouput[funcCnt] instanceof DateValue)
								{
									if(((DateValue)functionouput[funcCnt]).getValue().compareTo(((DateValue)temp).getValue()) > 0 )
										functionouput[funcCnt] = temp;
								}*/
							//}
						}
						if(funclist[funcCnt] == 2)
						{
							if(functionouput[funcCnt] == null)
								functionouput[funcCnt] = new LongValue("1");
							else
								functionouput[funcCnt] =  new LongValue(functionouput[funcCnt].toLong() + 1);
						}
						if(funclist[funcCnt] == 3)
						{
							PrimitiveValue temp = eval.eval(funcexp.get(funcCnt));
							if(functionouput[funcCnt] == null)
							{
								functionouput[funcCnt] = temp;
							}
							else
							{
								//if(temp instanceof LongValue)
									//functionouput[funcCnt] = new LongValue(functionouput[funcCnt].toLong() + temp.toLong());
								//if(temp instanceof DoubleValue)
									functionouput[funcCnt] = new DoubleValue(functionouput[funcCnt].toDouble() + temp.toDouble());								
							}
						}
					}
				}
				else
				{
					StringBuilder print = new StringBuilder();
					for(SelectItem selectclause : selectclauses)
					{
						print.append(eval.eval(((SelectExpressionItem)selectclause).getExpression()).toString());
						print.append("|");
					}
					print.deleteCharAt(print.length()-1);
					System.out.print(print.toString()+"\n");
				}					
			}
			reader.close();
			if(function)
			{
				StringBuilder print = new StringBuilder();
				for(PrimitiveValue temp: functionouput)
				{
					if(temp != null)
					{
						print.append(temp.toString());
					}
					print.append("|");
				}
				if(print.length()>0)
				{
					print.deleteCharAt(print.length()-1);
					System.out.print(print.toString()+"\n");
				}
			}
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
	}
}