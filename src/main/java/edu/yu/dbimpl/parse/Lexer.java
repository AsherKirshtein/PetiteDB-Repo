package edu.yu.dbimpl.parse;
import java.io.*;

public class Lexer extends LexerBase{

    String command;
    public StreamTokenizer tokenizer;
    private Token firstToken;
    private int tokensParsed = 0;

	public Lexer(String s) 
    {
		super(s);
        this.command = s;

        commonMistakesChecker(s);

		tokenizer = new StreamTokenizer(new StringReader(s));
        tokenizer.ordinaryChar('.');   //disallow "." in identifiers
        tokenizer.ordinaryChar('(');
        tokenizer.ordinaryChar(')');
        tokenizer.ordinaryChar('<');
        tokenizer.ordinaryChar('>');
        tokenizer.ordinaryChar('=');
        tokenizer.ordinaryChar(';');
        tokenizer.ordinaryChar(',');  

        tokenizer.wordChars('_', '_'); //allow "_" in identifiers
        
        tokenizer.quoteChar('\'');

        // Treat comma as delimiter
        

        tokenizer.lowerCaseMode(true); //ids and keywords are converted

        firstToken = nextToken();
	}

	@Override
	public Token firstTokenRetrieved() 
    {
		return this.firstToken;
	}

	@Override
	public Token nextToken() throws BadSyntaxException 
    {
		int nextToken;
        try
        {
            nextToken = tokenizer.nextToken();
        }
        catch(IOException e)
        {
            throw new BadSyntaxException();
        }
        tokensParsed++;
        switch(nextToken)
        {
            case StreamTokenizer.TT_EOF:
                return EOF_Token;    
            case StreamTokenizer.TT_EOL:
                Token t = new Token(TokenType.DELIMITER, "");
                //System.out.println(t);
                return t;
            case StreamTokenizer.TT_NUMBER:
                 Token t1 = numberHandle(this.tokenizer.nval);
                 //System.out.println(t1);
                 return t1;
            case StreamTokenizer.TT_WORD:
                 Token t2 = wordHandle(this.tokenizer.sval);
                 //System.out.println(t2);
                 return t2;
            default:
                 Token t3 = delimiterHandle(nextToken);
                 //System.out.println(t3);
                 return t3;
        }
	} 

    @Override
    public String toString()
    {
        String s = "First Token " + firstToken.toString() + "\n";
        s += "Current token string: " + this.tokenizer.sval + "\n";
        s += "Current token int: " + this.tokenizer.nval + "\n";
        s += "tokens parsed: " + this.tokensParsed;
        return s;
    }
    
    //Below are personal private assisting methods

    private Token delimiterHandle(int nextToken)
    {
		switch(nextToken) 
        {
            case '\'':
                return new Token(TokenType.STRING_CONSTANT, this.tokenizer.sval);
            case '(':
                return new Token(TokenType.DELIMITER, "(");
            case ')':
                return new Token(TokenType.DELIMITER, ")");
            case ';':
                return new Token(TokenType.DELIMITER, ";");
            case '=':
                return new Token(TokenType.COMPARISON_OP, "=");
            case '>':
                return new Token(TokenType.COMPARISON_OP, ">");
            case '<':
                return new Token(TokenType.COMPARISON_OP, "<");
            case ',':
                return new Token(TokenType.DELIMITER, ",");
            default:
                throw new BadSyntaxException();
        }
	}

	private Token wordHandle(String word)
    {
        if(isKeyWord(word))
        {
            return new Token(TokenType.KEYWORD, word);
        }
        else if(word.length() == 1 && isComparisonOperator(word))
        {
            return new Token(TokenType.COMPARISON_OP, word);
        }
        else if(word.length() == 1 && isDelimiter(word))
        {
            return new Token(TokenType.DELIMITER, word);
        }
        else if(isID(word))
        {
            return new Token(TokenType.ID, word);
        }
        else if(isBoolean(word))
        {
            return new Token(TokenType.BOOLEAN_CONSTANT, word);
        }
        else if(isDelimiter(word))
        {
            return new Token(TokenType.DELIMITER, word);
        }

        return new Token(TokenType.STRING_CONSTANT, word);
    }

    

    private Token numberHandle(Double number)
    {
        if(shouldBeInteger(number))
        {
            String value = number.toString().split("\\.")[0];//removes the decimal point from the int so we don't get 1.0
            return new Token(TokenType.INT_CONSTANT, value);
        }
        String dVal = number.toString();
        return new Token(TokenType.DOUBLE_CONSTANT, dVal);
    }

    private boolean isBoolean(String word)
    {
        return "true".equalsIgnoreCase(word) || "false".equalsIgnoreCase(word);
    }

    private boolean isID(String word)
    {
        try
        {
            Integer.parseInt(word);
        }
        catch(Exception e)
        {
            return false;
        }
        return true;
    }

    private boolean shouldBeInteger(Double number)
    {
        return number % 1.0 == 0;
    }

    private boolean isKeyWord(String word)
    {
        return keywords.contains(word.toLowerCase());
    }

    private boolean isDelimiter(String word)
    {
        return word == " " || word == "." || word == ",";
    }

    private boolean isComparisonOperator(String word)
    {
        try
        {
            char c = word.charAt(0);
            return comparisonOperators.contains(c);
        }
        catch(Exception e)
        {
            return false;
        }
    }

    private void commonMistakesChecker(String s)
    {
       if(!hasEqualBrackets(s) || !endsCorrectly(s))
       {
           throw new BadSyntaxException();
       }
       if(s.startsWith("INSERT INTO"))
       {
            String[] words = s.split(" ");
            if(!words[3].startsWith("("))
            {
                throw new BadSyntaxException("Missing bracket");
            }
       }
       commaChecker(s);
        
    }

    private void commaChecker(String s)
    {
        // Extracting the column list part and values list part
        if(s.contains("VALUES")){
        String[] parts = s.split("VALUES");
        if (parts.length != 2) {
            throw new BadSyntaxException(); // The statement is not properly formatted
        }

        String columnPart = parts[0];
        String valuesPart = parts[1];

        if(countCommas(columnPart) != countCommas(valuesPart))
        {
            throw new BadSyntaxException();
        }
        
        // Checking commas in the column list part
        if (!areCommasCorrect(columnPart, "(", ")")) {
            throw new BadSyntaxException();
        }

        // Checking commas in the values list part
        if(!areCommasCorrect(valuesPart, "(", ")"))
        {
            throw new BadSyntaxException();
        }
        }
        if(s.contains("SELECT") && !s.contains("AS") && !s.contains("COUNT"))
        {
            String[] words = s.split(" ");
            int i = 0;
            while (!"from".equalsIgnoreCase(words[i]) && !"from".equals(words[2]))
            {
                i++;
                if("from".equalsIgnoreCase(words[i]))
                {
                    break;
                }
                if(!words[i].contains(","))
                {
                    try
                    {
                        if(!"from".equalsIgnoreCase(words[i+1]))
                        {
                            //System.out.println(s);
                            //System.out.println(words[i]);
                            throw new BadSyntaxException(s + " is missing comma at " + words[i]);
                        }
                    }
                    catch(IndexOutOfBoundsException e)
                    {
                        throw new BadSyntaxException();
                    }
                }
            }
        }
        
    }
 
    private static boolean hasEqualBrackets(String str) {
       int openBrackets = 0;
       int closeBrackets = 0;

       for (int i = 0; i < str.length(); i++) {
           char ch = str.charAt(i);
           if (ch == '(') {
               openBrackets++;
           } else if (ch == ')') {
               closeBrackets++;
           }
       }

       return openBrackets == closeBrackets;
   }

   private boolean endsCorrectly(String s)
   {
       return s.endsWith(";");
   }

   public static boolean hasCorrectCommas(String sql) {
    // Extracting the column list part and values list part
    String[] parts = sql.split("VALUES");
    if (parts.length != 2) {
        return false; // The statement is not properly formatted
    }

    String columnPart = parts[0];
    String valuesPart = parts[1];

    // Checking commas in the column list part
    if (!areCommasCorrect(columnPart, "(", ")")) {
        return false;
    }

    // Checking commas in the values list part
    return areCommasCorrect(valuesPart, "(", ")");
}

private static int countCommas(String str) {
    int commaCount = 0;

    for (int i = 0; i < str.length(); i++) {
        if (str.charAt(i) == ',') {
            commaCount++;
        }
    }

    return commaCount;
}

private static boolean areCommasCorrect(String part, String startDelimiter, String endDelimiter) {
    int startIndex = part.indexOf(startDelimiter);
    int endIndex = part.lastIndexOf(endDelimiter);
    if (startIndex == -1 || endIndex == -1 || endIndex <= startIndex) {
        return false; // Delimiters are not correctly placed
    }

    String content = part.substring(startIndex + 1, endIndex).trim();
    String[] elements = content.split(",");

    for (String element : elements)
    {
        if (element.trim().isEmpty()) {
            return false; // Found an empty element (,,)
        }
    }
    return true;
}



}
