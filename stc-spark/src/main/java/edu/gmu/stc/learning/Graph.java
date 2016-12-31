package edu.gmu.stc.learning;

/**
 * Created by Fei Hu on 12/11/16.
 */
public class Graph {
  public static int MAX_VERTICES = 9;
  private Node vertices[];
  public int count;
  public Graph() {
    vertices = new Node[MAX_VERTICES];
    count = 0;
  }

  public void addNode (Node x) {
    if (count < vertices.length) {
      vertices[count] = x;
      count++;
    } else {
      System.out.print("Graph Full");
    }
  }

  public Node[] getNodes() {
    return vertices;
  }

}
