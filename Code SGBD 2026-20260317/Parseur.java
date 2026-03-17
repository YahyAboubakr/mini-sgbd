import java.util.ArrayList;
import java.util.List;
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

    public ResultatRequete stringAnalyser(){
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(this.query.trim());
        String selectPart = "";
        String fromPart = "";
        String wherePart = null;

        if(matcher.find()){
            selectPart = matcher.group(1);
            fromPart = matcher.group(2);
            wherePart = matcher.group(3);

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
        return new ResultatRequete(selectPart, fromPart, wherePart);
    }

    public String[] tables(String fromPart){
        return fromPart.split("\\s*,\\s*");
    }

    public String[] champs(String selectPart){
        return selectPart.split("\\s*,\\s*");
    }

    public List<Condition> whereParseur(String wherePart) {
        if(wherePart == null) {
            return new ArrayList<>(); // Retourne une liste vide si pas de conditions
        }
        List<Condition> listeConditions = new ArrayList<>();
        
        // Sécurité : s'il n'y a pas de WHERE, on retourne une liste vide
        if (wherePart == null || wherePart.trim().isEmpty()) {
            return listeConditions;
        }

        // Explication de la Regex :
        // (.+?)              -> Groupe 1 : Opérande gauche (ex: cond1)
        // \s*(=|<|>)\s* -> Groupe 2 : Opérateur (=, <, >)
        // (.+?)              -> Groupe 3 : Opérande droit (ex: 5)
        // (?:\s+(?i)(AND|OR)\s+|$) -> Groupe 4 : Le AND/OR qui suit, OU la fin de la phrase ($)
        String regexWhere = "(.+?)\\s*(=|<|>)\\s*(.+?)(?:\\s+(?i)(AND|OR)\\s+|$)";
        Pattern pattern = Pattern.compile(regexWhere);
        Matcher matcher = pattern.matcher(wherePart.trim());

        while (matcher.find()) {
            String gauche = matcher.group(1).trim();
            String operateurStr = matcher.group(2).trim();
            String droite = matcher.group(3).trim();
            String logiqueStr = matcher.group(4); // Peut être null pour la toute dernière condition

            // 1. Traduction de l'opérateur de comparaison vers ton Enum Tests
            Tests operateur = switch (operateurStr) {
                case "=" -> Tests.EQUALS;
                case ">" -> Tests.GREATER_THAN;
                case "<" -> Tests.LESS_THAN;
                default -> throw new IllegalArgumentException("Opérateur inconnu : " + operateurStr);
            };

            // 2. Traduction de l'opérateur logique vers ton Enum Tests
            Tests lienLogique = null;
            if (logiqueStr != null) {
                if (logiqueStr.equalsIgnoreCase("AND")) {
                    lienLogique = Tests.AND;
                } else if (logiqueStr.equalsIgnoreCase("OR")) {
                    lienLogique = Tests.OR;
                }
            }

            // 3. Détecter si c'est une condition de jointure
            boolean isJoin = Condition.isJoinCondition(gauche, droite);

            // 4. On ajoute la condition structurée à notre liste
            listeConditions.add(new Condition(gauche, operateur, droite, lienLogique, isJoin));
        }

        return listeConditions;
    }

    public String[] conditions(String wherePart){
        if(wherePart == null){
            return new String[0];
        }
        return wherePart.split("\\s+AND\\s+");
    }
}
