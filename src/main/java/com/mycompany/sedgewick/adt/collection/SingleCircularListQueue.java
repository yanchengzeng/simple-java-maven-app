package com.mycompany.sedgewick.adt.collection;

import java.util.Iterator;

/**
 * Notes(01/09):
 * 1. Why do you need two points when one does the job
 */
public class SingleCircularListQueue<Item> implements Iterable<Item> {

	private Node last;
	private int size = 0;

	public boolean isEmpty() {
		return size() == 0;
	}

	public int size() {
		return size;
	}

	public void enqueue(Item item) {
		size++;
		Node added = new Node();
		added.item = item;
		if (last == null) {
			last = added;
			last.next = last;
		} else {
			added.next = last.next;
			last.next = added;
			last = last.next;
		}
	}

	public Item dequeue() {
		size--;
		Item res = last.next.item;
		if (size == 0) {
			last = null;
		} else {
			last.next = last.next.next;
		}
		return res;
	}

	@Override
	public Iterator<Item> iterator() {
		return new SingleCircularListQueueIterator();
	}

	private class SingleCircularListQueueIterator implements Iterator<Item> {

		private Node current = last;

		@Override
		public boolean hasNext() {
			return current != null;
		}

		@Override
		public Item next() {
			Item res = current.next.item;
			if (current.next == last)
				current = null;
			else
				current = current.next;
			return res;
		}
	}


	private class Node {
		public Item item;
		public Node next;
	}

	public static void main(String[] args) {
		SingleCircularListQueue<String> sclq = new SingleCircularListQueue<>();
		sclq.enqueue("a");
		sclq.enqueue("b");
		sclq.enqueue("c");
		sclq.enqueue("d");
		for (String s : sclq) {
			System.out.println(s);
		}
	}
}
