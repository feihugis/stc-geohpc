package edu.gmu.stc.learning;

import java.security.Key;

/**
 * Created by Fei Hu on 12/20/16.
 */
public class LinkedArrayHashTable<K, V> {
  private static final int INIT_CAPACITY = 4;
  private int n;  //number of key-pairs
  private int m;  //hashtable size

  private KVLinkedList<K, V>[] table;

  public LinkedArrayHashTable() {
    this(INIT_CAPACITY);
  }

  public LinkedArrayHashTable(int m) {
    this.m = m;
    table = (KVLinkedList<K, V>[]) new KVLinkedList[m];
    for(int i = 0; i < m; i++) {
      table[i] = new KVLinkedList<K, V>();
    }
  }

  public void resize(int chains) {
    LinkedArrayHashTable<K, V> temp = new LinkedArrayHashTable<K, V>(chains);
    for (int i = 0; i < m; i++) {
      for (K key : table[i].keys()) {
        temp.put(key, table[i].get(key));
      }
    }

    this.table = temp.table;
    this.m = temp.m;
    this.n = temp.n;
  }


  public int hash(K key) {
    return (key.hashCode() & 0x7fffffff) % m;
  }

  public int size() {
    return n;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public boolean contains(K key) {
    if (key == null) throw new IllegalArgumentException( "argument to contains() is null" );
    int indice = hash(key);
    return table[indice].contains(key);
  }

  public V get(K key) {
    if (key == null) throw new IllegalArgumentException( "argument to get() is null" );
    int i = hash(key);
    return table[i].get(key);
  }

  public void put(K key, V value) {
    if ( key == null) throw new IllegalArgumentException("first argument to put() is null");
    if (value == null) {
      this.delete(key);
    }
    int indice = hash(key);
    if (!table[indice].contains(key)) n++;
    table[indice].put(key, value);
  }

  public void delete(K key) {
    if (key == null) throw new IllegalArgumentException("argument to delete() is null");

    int i = hash(key);
    if (table[i].contains(key)) n--;
    table[i].delete(key);

    if ( m > INIT_CAPACITY && n <= 2*m) resize(m/2);
  }

  public static void main(String[] args) {
    LinkedArrayHashTable<Integer, Integer> hashTable = new LinkedArrayHashTable<Integer, Integer>();
    hashTable.put(1, 1);
    hashTable.put(2,2);
    hashTable.put(4, 4);
    hashTable.put(5, 5);
    hashTable.delete(4);

    System.out.println(hashTable.get(4));
  }
}


