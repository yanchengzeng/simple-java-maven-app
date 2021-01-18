package com.mycompany.app;

/**
 *
 */
public class A {
	interface B {
		int run();
	}

	interface C {
		int run();
	}

	class D implements B, C {

		public int run() {
			return 0;
		}
	}
}

