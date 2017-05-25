package com.asiainfo.elexplain;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.springframework.expression.spel.standard.SpelExpressionParser;

public class BaseEL<T> {
	protected static SpelExpressionParser parser = new SpelExpressionParser();
	protected String stringExpression;
	protected Class<?> returnTypeExpression;
	public HashMap<String, String> paramMap;

	@SuppressWarnings("unchecked")
	public T getResult() {
		Iterator<Entry<String, String>> iter = paramMap.entrySet().iterator();
		String newExp = stringExpression;
		while(iter.hasNext()) {
			Entry<String, String> pair = iter.next();
			newExp = newExp.replace(pair.getKey(), "\""+pair.getValue()+"\"");
		}
		return (T) parser.parseExpression(newExp).getValue(returnTypeExpression);
	}
	
	public void setStringExpression(String stringExpression) {
		this.stringExpression = stringExpression;
	}

	public void setReturnTypeExpression(Class<?> returnTypeExpression) {
		this.returnTypeExpression = returnTypeExpression;
	}
	
	public void setParamMap(HashMap<String, String> paramMap) {
		this.paramMap = paramMap;
	}

	public void putParamMapValue(String key, String value) {
		paramMap.put(key, value);
	}
}
