package edu.gmu.stc.learning;

import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Created by Fei Hu on 12/20/16.
 */
public class KVLinkedList<K, V> {
  private int n;
  private Node first;

  private class Node<K, V> {
    private K key;
    private V value;
    private Node next;

    public Node(K key, V value, Node next) {
      this.key = key;
      this.value = value;
      this.next = next;
    }

    @Override
    public String toString() {
      return String.format("Key " + key + " : " + value);
    }
  }

  public KVLinkedList(){

  }

  public int size() {
    return n;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public boolean contains(K key) {
    return getKey(key) != null;
  }

  public V getKey(K key) {
    for (Node x = first; x != null; x = x.next) {
      if (x.key.equals(key)) {
        return (V) x.value;
      }
    }
    return null;
  }

  public void put(K key, V value) {
    if (value == null) {
      delete(key);
    }

    for (Node x = first; x != null; x = x.next) {
      if (x.key.equals(key)) {
        x.value = value;
        return;
      }
    }

    first = new Node(key, value, first);
    n++;
  }

  public void delete(K key) {
    first = delete(first, key);
  }

  public Node delete(Node x, K key) {
    if (x == null) return null;
    if (x.key.equals(key)) {
      n--;
      return x.next;
    }

    x.next = delete(x.next, key);
    return x;
  }

  public V get(K key) {
    if (key == null) return null;
    for (Node x = first; x != null; x = x.next) {
      if (x.key.equals(key)) {
        return (V) x.value;
      }
    }
    return null;
  }

  public Iterable<K> keys() {
    List<K> list = new ArrayList<K>();
    for (Node x = first; x != null; x = x.next) {
      list.add((K) x.key);
    }
    return list;
  }

  public void test() {
    Node<Integer, Integer> node1 = new Node<Integer, Integer>(1, 1, null);
    Node<Integer, Integer> node2 = new Node<Integer, Integer>(2, 2, null);
    Node<Integer, Integer> node3 = new Node<Integer, Integer>(3, 3, null);

    System.out.println(node1);
    node1.next = node2;
    //changeValue(node2);

    Node test = changeValue(node1);
    System.out.print(test);

    System.out.println(node1);
    node2 = node3;
    Node node = node2;
    node.key = 5;
    System.out.println(node1.next);


    Map<Integer, Integer> map = new HashMap<Integer, Integer>();
    for (Map.Entry<Integer, Integer> entry: map.entrySet()) {

    }

  }

  public Node changeValue( Node node) {
    node = node.next; //node.next;
    return node;
  }

  public static void main(String[] args) {
    KVLinkedList test = new KVLinkedList();
    test.test();
  }



}
