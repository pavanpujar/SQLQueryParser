package edu.buffalo.cse.sql.plan;

import java.util.Stack;

public class EvaluateExpression {
	
public float evaluate(String infix_Expression)
{
	String postfix_Expression=null;
	postfix_Expression=infixToPostfix(infix_Expression);
	return 0;
}
public String infixToPostfix(String infix)
{
	String postfix=null;
	Stack<Character> st=new Stack<Character>();
    for (int index = 0; index < infix.length(); ++index) {
        char chValue = infix.charAt(index);
        if (chValue == '(') {
            st.push('(');
        } else if (chValue == ')') {
            Character oper = st.peek();
            while (!(oper.equals('(')) && !(st.isEmpty())) {
                postfix += oper.charValue();
                st.pop();
                oper = st.peek();
            }
            st.pop();
        } else if (chValue == '+' || chValue == '-') {
            //Stack is empty
            if (st.isEmpty()) {
                st.push(chValue);
                //current Stack is not empty
            } else {
                Character oper = st.peek();
                while (!(st.isEmpty() || oper.equals(new Character('(')) || oper.equals(new Character(')')))) {
                    st.pop();
                    postfix += oper.charValue();
                }
                st.push(chValue);
            }
        } else if (chValue == '*' || chValue == '/') {
            if (st.isEmpty()) {
                st.push(chValue);
            } else {
                Character oper = st.peek();
                while (!oper.equals(new Character('+')) && !oper.equals(new Character('-')) && !st.isEmpty()) {
                    st.pop();
                    postfix += oper.charValue();
                }
                st.push(chValue);
            }
        } else {
            postfix += chValue;
        }
        while (!st.isEmpty()) {
            Character oper = st.peek();
            if (!oper.equals(new Character('('))) {
                st.pop();
                postfix += oper.charValue();
            }
        }
}
	return postfix;
}
}
