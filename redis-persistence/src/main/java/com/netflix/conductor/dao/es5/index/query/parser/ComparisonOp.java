/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.dao.es5.index.query.parser;

import java.io.InputStream;

/**
 * @author Viren
 */
public class ComparisonOp extends AbstractNode {
	
	public static enum Operators {
		BETWEEN("BETWEEN"), EQUALS("="), LESS_THAN("<"), GREATER_THAN(">"), IN("IN"), NOT_EQUALS("!="), IS("IS");
		
		private String value;
		Operators(String value){
			this.value = value;
		}
		
		public String value(){
			return value;
		}
	}
	
	private static final int betwnLen = Operators.BETWEEN.value().length();
	
	private String value;
	
	public ComparisonOp(InputStream is) throws ParserException {
		super(is);
	}

	@Override
	protected void _parse() throws Exception {
		byte[] peeked = peek(betwnLen);
		if(peeked[0] == '=' || peeked[0] == '>' || peeked[0] == '<'){
			this.value = new String(peeked, 0, 1);
		}else if(peeked[0] == 'I' && peeked[1] == 'N'){
			this.value = "IN";
		}else if(peeked[0] == 'I' && peeked[1] == 'S'){
			this.value = "IS";
		}else if(peeked[0] == '!' && peeked[1] == '='){
			this.value = "!=";
		}else if(peeked.length == betwnLen && new String(peeked).equals(Operators.BETWEEN.value())){
			this.value = Operators.BETWEEN.value();
		}else{
			throw new ParserException("Expecting an operator (=, >, <, !=, BETWEEN, IN), but found none.  Peeked=>" + new String(peeked));
		}
		
		read(this.value.length());
	}
	
	@Override
	public String toString(){
		return " " + value + " ";
	}
	
	public String getOperator(){
		return value;
	}

}
