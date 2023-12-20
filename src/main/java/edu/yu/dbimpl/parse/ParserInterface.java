package edu.yu.dbimpl.parse;

import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.yu.dbimpl.parse.LexerBase.Token;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.query.Expression;
import edu.yu.dbimpl.query.Predicate;
import edu.yu.dbimpl.query.Term;

public class ParserInterface extends ParserInterfaceBase {

	private static Lexer lexer;
    private Action action;
    
    public ParserInterface(String s)
    {
		super(s);
		lexer = new Lexer(s);
        commonMistakesChecker(s);
        action = getAction(s);
	}

	@Override
	public String field()
    {
		if (!isValidID())
        {
            throw new BadSyntaxException(lexer.tokenizer.sval + " is not a valid ID");
        }
        String s = lexer.tokenizer.sval;
        lexer.nextToken();
        return s;
	}

	@Override
	public DatumBase constant()
    {
		DatumBase db = null;
        checkConstant();
        if(isBoolean())
        {
            String sVal = lexer.tokenizer.sval;
            if(sVal.equalsIgnoreCase("true"))
            {
                db = new Datum(true);
            }
            else
            {
                db = new Datum(false);
            }
            lexer.nextToken();
            return db;  
        }
        else if(isInt())
        {
            int iVal = (int) lexer.tokenizer.nval;
            db = new Datum(iVal);
            return db;
        }
        else if(isDouble())
        {
            Double dVal = lexer.tokenizer.nval;
            db = new Datum(dVal);
            return db;
        }
        else if(isString())
        {
            String sVal = lexer.tokenizer.sval;
            db = new Datum(sVal);
            return db;
        }
        else if(isQuote())
        {
            String sVal = lexer.tokenizer.sval;
            db = new Datum(sVal);
            return db;
        }
        lexer.nextToken();
        return db;
	}

	

	@Override
	public Expression expression() {
		if (isValidID())
        {
            return new Expression(field());
        }
        return new Expression(constant());
   }

	@Override
	public Term term() 
    {
		Expression lhs = expression();
        if(!isDelimiter('=') && !isDelimiter('>') && isDelimiter('<'))
        {
            throw new BadSyntaxException();
        }
        char operator = getOp();
        lexer.nextToken();
        Expression rhs = expression();
        return new Term(lhs, operator,rhs);
	}

	@Override
	public Predicate predicate()
    {
		Predicate predifile = new Predicate(term());
        //System.out.println(predifile);
        lexer.nextToken();//might cause problems
        if(isCombining()) 
        {
            lexer.nextToken();
            predifile.add(term());
        }
        //System.out.print(predifile);
        return predifile;
	}

	@Override
	public ParseQueryBase query()
    {
        switch (action) 
        {
            case SELECT:
                return selectQuery();
            case DELETE:
                return null; //not implementing
			case CHILL:
				return null; //not implementing
			case CREATE:
				return null; //not implementing
			case INSERT:
				return insertQuery();
			case UPDATE:
				return null;
			default:
				return null;
        }
	}

    private ParseQueryBase insertQuery()
    {
		List<String> inserts = handleInsert();
        lexer.nextToken();
        Collection<String> values = handleValues();
        lexer.nextToken();
        Predicate pred = new Predicate();
        return new ParseQuery(inserts, values, pred);
	}

	private ParseQueryBase selectQuery()
    {
        List<String> fields = handleSelect();
        handleAVG();
        Collection<String> tables = handleFrom();
        Predicate pred = new Predicate();
        if(isWhere())
        {
            lexer.nextToken();
            pred = predicate();
        }
        return new ParseQuery(fields, tables, pred);
    }

    @Override
    public String toString()
    {
        return lexer.toString();
    }

    //below are my private helper methods

    private boolean isValidID() //tells if the token is possible identifier.
    {
        return lexer.tokenizer.ttype==StreamTokenizer.TT_WORD && !LexerBase.keywords.contains(lexer.tokenizer.sval);
    }

    private boolean isString()//tells if token is a string
    {
        return StreamTokenizer.TT_WORD == lexer.tokenizer.ttype;
    }

    private boolean isInt()
    {
        return lexer.tokenizer.ttype == StreamTokenizer.TT_NUMBER && lexer.tokenizer.nval % 1.0 == 0;
    }

    private boolean isDouble()
    {
		return lexer.tokenizer.ttype == StreamTokenizer.TT_NUMBER && lexer.tokenizer.nval % 1.0 != 0;
	}

    private boolean isBoolean()
    {
        if(!(lexer.tokenizer.ttype == StreamTokenizer.TT_WORD))
        {
            return false;
        }
        return lexer.tokenizer.sval.equalsIgnoreCase("true") || lexer.tokenizer.sval.equalsIgnoreCase("false");
    }

    private boolean isQuote()
    {
        return lexer.tokenizer.ttype == 39;
    }

    private void checkConstant()//sees if we need to throw an exception
    {
        if(!isString() && !isInt() && !isDouble() && !isBoolean() && !isQuote())
        {
            throw new BadSyntaxException(lexer.tokenizer.sval + " is not a string, boolean, Integer, or Double " + lexer.tokenizer.ttype);
        }
    }

    private boolean isDelimiter(char d)
    {
        return d == (char)lexer.tokenizer.ttype;
    }

    private boolean isCombining()
    {
        return lexer.tokenizer.ttype == StreamTokenizer.TT_WORD && lexer.tokenizer.sval.equalsIgnoreCase("and");
    }

    private boolean isComma()
    {
        return (char)lexer.tokenizer.ttype == ',';
    }

    private boolean isSelectStatement()
    {
        return (lexer.tokenizer.sval != null) && lexer.tokenizer.sval.equals("select");
    }

    private boolean isFrom()
    {
        return(lexer.tokenizer.sval != null) && lexer.tokenizer.sval.equalsIgnoreCase("from");
    }

    private boolean isAS()
    {
        return(lexer.tokenizer.sval != null) && lexer.tokenizer.sval.equalsIgnoreCase("as");
    }

    private boolean isWhere()
    {
        return (lexer.tokenizer.sval != null) && lexer.tokenizer.sval.equals("where");
    }

    private List<String> handleSelect()
    {
        if(!isSelectStatement())
        {
           throw new BadSyntaxException("Expected SELECT instead was " + lexer.tokenizer.sval);
        }
        Token t = lexer.nextToken();
        if(t.value.equalsIgnoreCase("avg"))
        {
            handleAVG();
        }
        return getSelectList();
    }

    private List<String> handleInsert()
    {
        if(!isInsertStatement())
        {
           throw new BadSyntaxException("Expected SELECT instead was " + lexer.tokenizer.sval);
        }
        lexer.nextToken();
        if(!isIntoStatement())
        {
            throw new BadSyntaxException("Expected SELECT instead was " + lexer.tokenizer.sval);
        }
        Token t = lexer.nextToken();
        List<String> inserting = new ArrayList<>();
        while (t.value != ")")
        {
            t = lexer.nextToken();
            if(t.value == "," || t.value == "(" || t.value == ")")
            {
                continue;
            }
            inserting.add(t.value);    
        }
        return inserting;
    }



    private boolean isIntoStatement() {
		return (lexer.tokenizer.sval != null) && lexer.tokenizer.sval.equals("into");
	}

	private boolean isInsertStatement()
    {
		return (lexer.tokenizer.sval != null) && lexer.tokenizer.sval.equals("insert");
	}

	private List<String> handleFrom()
    {
        if(!isFrom() && !isAS())
        {
            throw new BadSyntaxException("Expected FROM or AS instead was " + lexer.tokenizer.sval + " with lexer " + lexer.toString());
        }
        if(isAS())
        {
            handleAs();
        }
        lexer.nextToken();
        return getFieldList();
    }

    private List<String> handleValues()
    {
        if(!isValue())
        {
            throw new BadSyntaxException("expected VALUES instead got " + lexer.tokenizer.sval + " with lexer " + lexer.toString());
        }
        Token t = lexer.nextToken();
        List<String> values = new ArrayList<>();
        while (t.value != ")")
        {
            t = lexer.nextToken();
            if(t.value == "," || t.value == "(" || t.value == ")")
            {
                continue;
            }
            values.add(t.value);    
        }
        return values;
    }

    private boolean isValue() 
    {
        return (lexer.tokenizer.sval != null) && lexer.tokenizer.sval.equals("values");
    }

    private void handleAVG()
    {
        
    }

    private void handleAs()
    {
        if (!isAS())
        {
            throw new BadSyntaxException();
        }
        lexer.nextToken();
        lexer.nextToken();
	}

	private List<String> getSelectList()
    {
        List<String> list = new ArrayList<String>();
        list.add(field());
        if(isComma())
        {
            lexer.nextToken();
            list.addAll(getSelectList());
        }
        return list;
    }

    private List<String> getFieldList()
    {
        List<String> list = new ArrayList<String>();
        if(!isValidID())
        {
            throw new BadSyntaxException("the id " + lexer.tokenizer.sval + " is not a valid id");
        }
        String sVal = lexer.tokenizer.sval;
        list.add(sVal);
        lexer.nextToken();
        if(isComma())
        {
           lexer.nextToken();
           list.addAll(getFieldList());
        }
        return list;
     }

     private char getOp()
     {
        return (char)lexer.tokenizer.ttype;
     }

     private Action getAction(String s)
     {
        if(s.toLowerCase().startsWith("select"))
        {
            return Action.SELECT;
        }
        if(s.toLowerCase().startsWith("delete"))
        {
            return Action.DELETE;
        }
        if(s.toLowerCase().startsWith("update"))
        {
            return Action.UPDATE;
        }
        if(s.toLowerCase().startsWith("insert"))
        {
            return Action.INSERT;
        }
        if(s.toLowerCase().startsWith("create"))
        {
            return Action.CREATE;
        }
        else
        {
            return Action.CHILL;
        }
     }

     private void commonMistakesChecker(String s)
     {
        if(!hasEqualBrackets(s) || !endsCorrectly(s))
        {
            throw new BadSyntaxException();
        }
     }
  
     public static boolean hasEqualBrackets(String str) {
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


    private enum Action
    {
        INSERT,
        DELETE,
        UPDATE,
        CREATE,
        SELECT,
        CHILL
    }
}
