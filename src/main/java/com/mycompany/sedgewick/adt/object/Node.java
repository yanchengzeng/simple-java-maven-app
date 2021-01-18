package com.mycompany.sedgewick.adt.object;

/**
 *
 */
public class Node<Item> {

	public Item item;
	public Node next;

	public Node(Item item) {
		this(item, null);
	}

	public Node(Item item, Node next) {
		this.item = item;
		this.next = next;
	}

	public static Node reverseIterative(Node node) {
		if (node == null || node.next == null) return node;
		Node curr = node;
		Node prev = null;
		while (curr != null) {
			Node temp = curr.next;
			curr.next = prev;
			prev = curr;
			curr = temp;
		}
		return prev;
	}

	public static Node reverseRecursive(Node node) {
		if (node == null || node.next == null) return node;
		Node newHead = reverseRecursive(node.next);
		node.next.next = node;
		node.next = null;
		return newHead;
	}

	public static void printNodes(Node head) {
		while (head != null) {
			System.out.println(head.item);
			head = head.next;
		}
	}

	public static void main(String[] args) {
		Node d = new Node("d");
		Node c = new Node("c", d);
		Node b = new Node("b", c);
		Node a = new Node("a", b);
		printNodes(reverseRecursive(a));
	}

}
