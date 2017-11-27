import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import java.util.*;
public class ReplaceString extends EvalFunc <String> {

@Override
    public String exec(Tuple input) {
        try {
            if (input == null || input.size() == 0) 
            {
                return null;
            }
            String str = (String) input.get(0);
            String tokens[] = str.split("\\|");
            	if (tokens.length == 3)
                {
            	String first = tokens[0];
                String second = tokens[1];
                String third = tokens[2];
                StringBuilder theBuilder = new StringBuilder();
                theBuilder.append(third);
                theBuilder.append(" , ");
                theBuilder.append(first);
                theBuilder.append(" and ");
                theBuilder.append(second);
                String pen=theBuilder.toString();
                return pen;
                }
            	else if (tokens.length == 2)
                {
            	String first = tokens[0];
                String second = tokens[1];
             
                StringBuilder theBuilder = new StringBuilder();
            
                theBuilder.append(second);
                theBuilder.append(" and ");
                theBuilder.append(first);
                String pen=theBuilder.toString();
                return pen;
                }
            
            	else
            	{
            		return str;
            	}
        } 
        catch (ExecException ex) {
            System.out.println("Error: " + ex.toString());
        }

        return null;
    }
}
