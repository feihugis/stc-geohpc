package edu.gmu.stc.learning;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Fei Hu on 1/22/17.
 */
public class PeekingIterator<T> implements Iterator {

  Iterator<T> itor;
  T current = null;

  public PeekingIterator(Iterator<T> iterator) {
    // initialize any member here.
    itor = iterator;
    if (itor.hasNext()) {
      current = itor.next();
    }
  }

  // Returns the next element in the iteration without advancing the iterator.
  public T peek() {
    return current;
  }

  // hasNext() and next() should behave the same as in the Iterator interface.
  // Override them if needed.
  @Override
  public T next() {
    T tmp = current;
    current = itor.hasNext()? itor.next() : null;
    return tmp;
  }

  @Override
  public void remove() {

  }

  @Override
  public boolean hasNext() {
    return current != null;
  }

  public static void main(String[] args) {
    List<Integer> input = Arrays.asList(1,2,3,4);
    PeekingIterator<Integer> test = new PeekingIterator(input.iterator());

    System.out.println(test.next());
  }
}
