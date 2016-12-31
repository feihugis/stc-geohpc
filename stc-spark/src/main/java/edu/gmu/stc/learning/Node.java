package edu.gmu.stc.learning;

/**
 * Created by Fei Hu on 12/11/16.
 */
public class Node {
  private Node adjacent[];
  public int adjacentCount;
  private String vertex;
  public State state;

  public Node(String vertex, int adjancentLength) {
    this.vertex = vertex;
    this.adjacentCount = 0;
    this.adjacent = new Node[adjancentLength];
  }

  public void addAdjacent(Node x) {
    if (adjacentCount < adjacent.length) {
      this.adjacent[adjacentCount] = x;
      adjacentCount++;
    } else {
      System.out.print("No more adjacent can be added");
    }
  }

  public Node[] getAdjacent() {
    return adjacent;
  }

  public String getVertex() {
    return vertex;
  }

  public String toString() {
    return vertex + " --> ";
  }


}
