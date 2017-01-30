package edu.gmu.stc.learning;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Created by Fei Hu on 1/22/17.
 */
public class RandomizedSet {
  HashMap<Integer, Integer> hashMap = null;
  List<Integer> results = null;

  /** Initialize your data structure here. */
  public RandomizedSet() {
    hashMap = new HashMap<Integer, Integer>();
    results = new ArrayList<Integer>();
  }

  /** Inserts a value to the set. Returns true if the set did not already contain the specified element. */
  public boolean insert(int val) {
    if (hashMap.containsKey(val)) return false;

    results.add(val);
    hashMap.put(val, results.size() - 1);
    return true;
  }

  /** Removes a value from the set. Returns true if the set contained the specified element. */
  public boolean remove(int val) {
    Integer index = hashMap.get(val);
    if (index == null) {
      return false;
    } else {
      int last = results.get(results.size()-1);
      results.set(index, last);
      hashMap.put(last, index);
      Integer value = results.remove(results.size() - 1);
      hashMap.remove(val);
      return true;
    }
  }

  /** Get a random element from the set. */
  public int getRandom() {
    int size = results.size();
    Random random = new Random();
    int index = random.nextInt(size);
    return results.get(index);
  }

  public List<String> Generate(int n) {
    List<String> result = new ArrayList<String>();
    result.add("");
    for (int i = 0; i < n; i++) {
      result = nextValue(result);
    }
    return result;
  }

  public List<String> nextValue(List<String> cur) {
    List<String> first = new ArrayList<String>();
    List<String> second = new ArrayList<String>();
    int size = cur.size();
    for(int i = size - 1; i >= 0; i--) {
      first.add("0" + cur.get(size - i - 1));
      second.add("1" + cur.get(i));
    }
    first.addAll(second);
    return first;
  }


  public static void main(String[] args) {
    RandomizedSet randomizedSet = new RandomizedSet();
    System.out.println(randomizedSet.insert(1));
    System.out.println(randomizedSet.remove(2));
    System.out.println(randomizedSet.insert(2));
    System.out.println(randomizedSet.getRandom());
    System.out.println(randomizedSet.remove(1));
    System.out.println(randomizedSet.insert(2));
    System.out.println(randomizedSet.getRandom());
    double tt = Math.pow(2.0, 3.0);


    int a = 0;
    if ("tt" instanceof String) {
      System.out.println(Integer.parseInt("1001", 2));
    }
    switch ('a') {
      case 'a': System.out.println('a');
        break;
    }

    List<String> re = randomizedSet.Generate(5);
    for (String s : re) {
      System.out.println(s);
    }
  }
}
