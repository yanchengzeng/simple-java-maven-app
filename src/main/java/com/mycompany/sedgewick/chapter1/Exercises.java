package com.mycompany.sedgewick.chapter1;

import edu.princeton.cs.algs4.StdOut;
import edu.princeton.cs.algs4.StdIn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Stack;
import java.util.function.BiFunction;

import static com.mycompany.util.PZCommon.*;

/**
 *
 */
public class Exercises {

	//1.1.9: outputs binary representation of a positive integer in string
	public static String binaryRepresentation(int num) {
		String s = "";
		do {
			s += (num % 2);
			num /= 2;
		} while (num > 0);
		return s;
	}

	//1.1.14: largest int not larger than base-2 logarithm of n
	public static int lg(int num) {
		int res = 0;
		while (num >= 2) {
			num /= 2;
			res++;
		}
		return res;
	}

	//1.1.15: returns an array of length m whose ith entry is the number
	//of times the inter i appeared in the argunent array
	public static int[] histogram(int[] a, int m) {
		int[] res = new int[m];
		for (int i = 0; i < a.length; i++) {
			if (a[i] < m && a[i] >= 0) {
				res[a[i]]++;
			}
		}
		return res;
	}

	public static int power(int a, int b) {
		if (b == 0) return 1;
		if (b % 2 == 0) {
			return power(a * a, b / 2);
		} else {
			return power(a * a, b / 2) * a;
		}
	}

	public static boolean isCircularShiftString(String a, String b) {
		return a.length() == b.length() && ((a + a).indexOf(b) != -1);
	}

	//fully paranthesized and whitespaced arithmetic expression
	public static double dijkstraArithmeticEvaluation(String input) {
		BiFunction<Double, Double, Double> add = ((a, b) -> a+b);
		BiFunction<Double, Double, Double> sub = ((a, b) -> a-b);
		BiFunction<Double, Double, Double> mul = ((a, b) -> a*b);
		BiFunction<Double, Double, Double> div = ((a, b) -> a/b);
		String[] expr = input.split("\\s+");
		Stack<Double> operands = new Stack<>();
		Stack<String> operators = new Stack<>();
		for (int i = 0; i < expr.length; i++) {
			String s = expr[i];
			switch (s) {
				case "(":
					break;
				case ")":
					String operator = operators.pop();
					double res = 0;
					switch (operator) {
						case "+":
							res = add.apply(operands.pop(), operands.pop());
							break;
						case "-":
							res = sub.apply(operands.pop(), operands.pop());
							break;
						case "*":
							res = mul.apply(operands.pop(), operands.pop());
							break;
						case "/":
							res = div.apply(operands.pop(), operands.pop());
							break;
					}
					operands.push(res);
					break;
				case "+":
				case "-":
				case "*":
				case "/":
					operators.push(s);
					break;
				default:
					double val = Double.parseDouble(s);
					operands.push(val);
					break;
			}
		}
		return operands.pop();
	}

	public static int[] shuffle(int[] arr) {
		int n = arr.length;
		Random r = new Random();
		for (int i = n - 1; i > 0; i--) {
			int next = r.nextInt(i+1);
			swap(arr, next, i);
		}
		return arr;
	}

	public static void main(String[] args) {
		int[] arr1 = {1, 2, 3, 4, 5};
		printArray(shuffle(arr1));
		ArrayList<Object> a1 = new ArrayList<Object>();
		a1.add(1, "");
	}
}
