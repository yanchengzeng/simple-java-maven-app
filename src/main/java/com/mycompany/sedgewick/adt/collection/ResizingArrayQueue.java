package com.mycompany.sedgewick.adt.collection;

import java.util.Iterator;

/**
 *
 * Notes(01/07):
 * 1) It's easier to set simple invariants, such as constantly incrementing
 * first and last and taking modulo to locate, than to juggle moving targets.
 *
 * 2) Don't keep an air of mystery about your code: If there is a mystery, break it.
 * For example, shrinking the size of the array can be done before or after dequeue operation.
 * No difference really and don't habitually think it should be done before.
 *
 */
public class ResizingArrayQueue<Item> implements Iterable<Item> {

	private Item[] items;
	private int size;
	private int capacity;
	private int first; // the first taken slot
	private int last; // the next available slot
	private int INITIAL_CAPACITY = 4;

	public ResizingArrayQueue() {
		size = 0;
		items = (Item[]) new Object[INITIAL_CAPACITY];
		first = 0;
		last = 0;
		capacity = INITIAL_CAPACITY;
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public int size() {
		return size;
	}

	public void enqueue(Item item) {
		if (size == capacity) resize(2 * capacity);
		items[last % capacity] = item;
		last++;
		size++;
	}

	public Item dequeue() {
		Item res = items[first % capacity];
		items[first % capacity] = null;
		first++;
		size--;
		if (size > 0 && size == capacity / 4) resize(capacity / 2);
		return res;
	}

	private void resize(int newSize) {
		Item[] newItems = (Item[]) new Object[newSize];
		for (int i = 0, j = first; j < last; j++, i++) {
			newItems[i] = items[j % capacity];
		}
		first = 0;
		last = size;
		capacity = newItems.length;
		items = newItems;
	}

	public Iterator<Item> iterator() {
		return new ResizingArrayQueueIterator();
	}

	private class ResizingArrayQueueIterator implements Iterator<Item> {

		private int current = first;

		@Override
		public boolean hasNext() {
			return current < last;
		}

		@Override
		public Item next() {
			return items[(current++) % capacity];
		}

	}

	public static void main(String[] args) {
		ResizingArrayQueue<String> raq = new ResizingArrayQueue<>();
		raq.enqueue("a");
		raq.enqueue("b");
		raq.enqueue("c");
		raq.enqueue("d");
		raq.dequeue();
		raq.enqueue("e");
		for (String s : raq) {
			System.out.print(s + " ");
		}
	}

}
