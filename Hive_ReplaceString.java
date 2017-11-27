import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

@Description(name = "my_to_upper",
value = "_FUNC_(str) - Converts a string to uppercase",
extended = "Example:\n"
+ "  > SELECT my_to_upper(author_name) FROM authors a;")



public class ChangeString extends UDF {
    public Text evaluate(Text str1) {
        Text to_value = new Text("");
        if (str1 != null) {
            try {
                //to_value.set(str.toString().toUpperCase());
				
				String str= str1.toString();
				
				
            String tokens[] = str.split("\\|");
            	if (tokens.length == 3)
                {
            	String first = tokens[0];
                String second = tokens[1];
                String third = tokens[2];
                StringBuilder theBuilder = new StringBuilder();
                theBuilder.append(first);
                theBuilder.append(" , ");
                theBuilder.append(second);
                theBuilder.append(", and ");
                theBuilder.append(third);
                String pen=theBuilder.toString();
                to_value.set(pen);
                }
            	else if (tokens.length == 2)
                {
            	String first = tokens[0];
                String second = tokens[1];
             
                StringBuilder theBuilder = new StringBuilder();
            
                theBuilder.append(first);
                theBuilder.append(", and ");
                theBuilder.append(second);
                String pen=theBuilder.toString();
                to_value.set(pen);
                }
            
            
				
				
            } catch (Exception e) { // Should never happen
                to_value = new Text(str1);
            }
        }
        return to_value;
    }
}
