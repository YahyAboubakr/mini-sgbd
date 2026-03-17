import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parseur {
    private String query;

    private static final String regex  = "(?i)SELECT\\s+(.*?)\\s+FROM\\s+(.*?)(?:\\s+WHERE\\s+(.*))?$";

    public Parseur(String query){
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void stringAnalyser(){
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(this.query.trim());

        if(matcher.find()){
            String selectPart = matcher.group(1);
            String fromPart = matcher.group(2);
            String wherePart = matcher.group(3);

            System.out.println("SELECT: " + selectPart);
            System.out.println("FROM: " + fromPart);
            
            if(wherePart != null){
                System.out.println("WHERE: " + wherePart);
            } else {
                System.out.println("WHERE: None");
            }
        } else {
            System.out.println("Invalid query format.");
        }
    }
}
