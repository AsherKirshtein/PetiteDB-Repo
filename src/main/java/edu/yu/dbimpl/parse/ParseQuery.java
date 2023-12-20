package edu.yu.dbimpl.parse;

import java.util.Collection;
import java.util.List;

import edu.yu.dbimpl.query.Predicate;

public class ParseQuery extends ParseQueryBase
{
    private List<String> fields;
    private Collection<String> tables;
    private Predicate predifile;

	public ParseQuery(List<String> fields, Collection<String> tables, Predicate predicate)
    {
		super(fields, tables, predicate);
		this.fields = fields;
        this.tables = tables;
        this.predifile = predicate;
	}

	@Override
	public List<String> fields() {
		return this.fields;
	}

	@Override
	public Collection<String> tables() {
		return this.tables;
	}

	@Override
	public Predicate predicate() {
		return this.predifile;
	}

    @Override
    public String toString() {
        String s = "+---------------------------------------------------+\n";
        s +=       "| Fields | " + this.fields + "\n";
        s +=       "| Tables | " + this.tables + "\n";
        s +=       "| Predicate  | " + this.predifile;
        s +=       "+---------------------------------------------------+\n";
        return s;
    }
    
}
