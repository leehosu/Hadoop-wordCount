package common;
import org.apache.hadoop.io.Text;
public class Parser {
	private int year;
	private int month;
	private int ArrDelay = 0;
	private String airlineName = null;
	private int dayOfWeek =0;
	
	public Parser(Text text){
		try{
			String[] columns = text.toString().split(",");
			year = Integer.parseInt(columns[0]);
			month = Integer.parseInt(columns[1]);
			airlineName = columns[8];
			dayOfWeek = Integer.parseInt(columns[3]);
			
			if(!columns[14].equals("NA")){
				ArrDelay = Integer.parseInt(columns[14]);
			} 
		} catch(Exception e){
			System.out.println("Error parsing a record :" + e.getMessage());
		}
	}
	public int getYear() {	return year;}
	public int getMonth() {	return month;}
	public int getArrDelay() {return ArrDelay;}
	public String getAirlineName() {return airlineName;}
	public int getDayOfWeek() {return dayOfWeek; }
}