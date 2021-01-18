package com.mycompany.dasgupta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mycompany.util.PZCommon.*;

/**
 *
 */
public class DC {


	public static void splitArrayByHead(int[] arr) {
		int head = arr[0];
		int i = 0, j = arr.length-1;
		for (int k = 1; k <= j;) {
			if (arr[k] < head) {
				swap(arr, k, i);
				i++;
			} else if (arr[k] == head) {
				k++;
			} else {
				swap(arr, k, j);
				j--;
			}
		}
		printArray(arr);
	}


	public static int kthSmallest(List<Integer> arr, int k) {
		List<Integer> left = new ArrayList<>();
		List<Integer> middle = new ArrayList<>();
		List<Integer> right = new ArrayList<>();
		for (int i = 0; i < arr.size(); i++) {
			if (arr.get(i) < arr.get(0)) {
				left.add(arr.get(i));
			} else if (arr.get(i) > arr.get(0)) {
				right.add(arr.get(i));
			} else {
				middle.add(arr.get(i));
			}
		}
		if (k <= left.size()) {
			return kthSmallest(left, k);
		} else if (k <= (left.size() + middle.size())) {
			return middle.get(0);
		} else {
			return kthSmallest(right, k - (left.size() + middle.size()));
		}
	}

	public static void main(String[] args) {
		int[] arr1 = new int[] {4, 0, 3, 5, 0, 4, 1, 1, 7};
		List<Integer> list = Arrays.asList(3, 5, 0, 4, 1, 1, 7);
		splitArrayByHead(arr1);
	}
}
