package com.mycompany.app;

import java.io.UnsupportedEncodingException;

/**
 * Hello world!
 */
public class App
{

    private final String message = "Hello World!";

    public App() {}

    public static void main(String[] args) {
        int a = 2;
		System.out.println((a++) % 3);
		System.out.println(a);
    }

    public static void printBytes(byte[] array, String name) {
        for (int k = 0; k < array.length; k++) {
            System.out.println(name + "[" + k + "] = " + "0x" +
                UnicodeFormatter.byteToHex(array[k]));
        }
    }

    private final String getMessage() {
        return message;
    }



}

class UnicodeFormatter  {

   static public String byteToHex(byte b) {
      // Returns hex String representation of byte b
      char hexDigit[] = {
         '0', '1', '2', '3', '4', '5', '6', '7',
         '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
      };
      char[] array = { hexDigit[(b >> 4) & 0x0f], hexDigit[b & 0x0f] };
      return new String(array);
   }

   static public String charToHex(char c) {
      // Returns hex String representation of char c
      byte hi = (byte) (c >>> 8);
      byte lo = (byte) (c & 0xff);
      return byteToHex(hi) + byteToHex(lo);
   }

}
