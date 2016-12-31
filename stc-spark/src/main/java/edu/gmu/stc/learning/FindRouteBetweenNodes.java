package edu.gmu.stc.learning;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Created by Fei Hu on 12/11/16.
 */
public class FindRouteBetweenNodes {

  public static Graph createNewGraph() {
    Graph g = new Graph();
    Node[] temp = new Node[9];

    temp[0] = new Node("a", 3);
    temp[1] = new Node("b", 2);
    temp[2] = new Node("c", 0);
    temp[3] = new Node("d", 2);
    temp[4] = new Node("e", 1);
    temp[5] = new Node("f", 0);
    temp[6] = new Node("g", 0);
    temp[7] = new Node("h", 0);
    temp[8] = new Node("i", 0);

    temp[0].addAdjacent(temp[1]);
    temp[0].addAdjacent(temp[2]);
    temp[0].addAdjacent(temp[3]);
    temp[1].addAdjacent(temp[6]);
    temp[1].addAdjacent(temp[7]);
    temp[3].addAdjacent(temp[4]);
    temp[3].addAdjacent(temp[8]);
    temp[4].addAdjacent(temp[5]);

    for (int i = 0; i < 9; i++) {
      g.addNode(temp[i]);
    }

    return g;
  }

  public static boolean breadthFirstSearch(Graph g, Node start, Node end) {
    LinkedList<Node> q = new LinkedList<Node>();

    for (Node node : g.getNodes()) {
      node.state = State.Unvisited;
    }

    start.state = State.Visisting;
    q.add(start);

    Node u;

    while (!q.isEmpty()) {
      u = q.removeFirst();
      System.out.println(u);
      if ( u != null) {
        for (Node v : u.getAdjacent()) {
          if (v.state == State.Unvisited) {
            if (v == end) {
              return true;
            } else {
              v.state = State.Visisting;
              q.add(v);
            }
          }
        }
        u.state = State.Visited;
      }
    }
    return false;
  }

  public static boolean depthFirstSearch(Graph g, Node start, Node end) {
    LinkedList<Node> q = new LinkedList<Node>();

    for (Node node : g.getNodes()) {
      node.state = State.Unvisited;
    }

    start.state = State.Visisting;
    q.add(start);

    Node u;

    while (!q.isEmpty()) {
      u = q.removeFirst();
      System.out.println(u);
      if ( u != null) {
        for (Node v : u.getAdjacent()) {
          if (v.state == State.Unvisited) {
            if (v == end) {
              return true;
            } else {
              v.state = State.Visisting;
              q.addFirst(v);
            }
          }
        }
        u.state = State.Visited;
      }
    }
    return false;
  }

  public static String findPath(Graph g, Node start, Node end) {
    LinkedList<Node> q = new LinkedList<Node>();
    LinkedList<Node> route = new LinkedList<Node>();

    for (Node node : g.getNodes()) {
      node.state = State.Unvisited;
    }

    start.state = State.Visisting;
    q.add(start);
    route.add(start);

    Node u;

    while (!q.isEmpty()) {
      u = q.removeFirst();
      if ( u != null) {
        for (Node v : u.getAdjacent()) {
          if (v.state == State.Unvisited) {
            if (v == end) {
              route.add(v);
              return Arrays.toString(route.toArray());
            } else {
              v.state = State.Visisting;
              q.addFirst(v);
              route.add(v);
            }
          }
        }
        u.state = State.Visited;
      }
    }
    return "";
  }

  public static void main(String[] args) {
    Graph g = createNewGraph();
    Node[] nodes = g.getNodes();
    Node start = nodes[0];
    Node end = nodes[5];
    System.out.print(breadthFirstSearch(g, start, end));


  }

}
