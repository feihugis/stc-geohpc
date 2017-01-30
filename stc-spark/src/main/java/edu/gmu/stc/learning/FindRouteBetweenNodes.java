package edu.gmu.stc.learning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

import ucar.nc2.NetcdfFile;

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

  public static int lengthLongestPath(String input) {
    if(input == null || !input.contains(".")) return 0;

    Stack<String> folderStack = new Stack<String>();
    int maxLength = 0, currentLength = 0, preFolderLevel = -2, currentFolderLevel = -2;

    String[] subPaths = input.split("\n");
    for(int i = 0; i < subPaths.length; i++) {
      currentFolderLevel = subPaths[i].lastIndexOf("\t");
      if(currentFolderLevel > preFolderLevel) {
        folderStack.push(subPaths[i]);
      } else{
        int gap = preFolderLevel - currentFolderLevel  + 1;
        for(int j = 0; j < gap; j++) {
          String delStr = folderStack.pop();
          currentLength = currentLength - (delStr.length() - delStr.lastIndexOf("\t"));
        }
        folderStack.push(subPaths[i]);
      }

      currentLength = currentLength + subPaths[i].length() - subPaths[i].lastIndexOf("\t");
      if(subPaths[i].contains(".")) {
        maxLength = Math.max(maxLength, currentLength);
      }
      preFolderLevel = currentFolderLevel;
    }

    return maxLength - 1;

  }

  public static String licenseKeyFormatting(String S, int K) {

    S = S.toUpperCase();
    S.replace("-", "");
    if(S.length() < K) return null;
    int parts = S.length()/K + 1;
    int firstNum = S.length() % K;

    String result = "";
    for(int i = 0; i < parts; i++) {
      if(i == 0) {
        result = result + S.substring(0, firstNum) + "-";
      } else {
        result = result + S.substring(firstNum + (i - 1)*K, firstNum + i*K) + "-";
      }
    }

    result = result.substring(0, result.length()-1);
    return result;
  }

  public static String decodeString(String s) {
    if(s == null) return null;

    while(s.contains("[")) {
      s = decodeOneBracket(s);
    }

    return s;
  }

  public class TreeNode {
         int val;
         TreeNode left;
         TreeNode right;
         TreeNode(int x) { val = x; }
    }

  public int kthSmallest(TreeNode root, int k) {
    if (root == null) throw new IllegalArgumentException("Illegal input parameter");
    Stack<TreeNode> stack = new Stack<TreeNode>();
    inOrderTraverse(root, stack);
    int count = 0;

    while (!stack.isEmpty() && count < k - 1) {
      stack.pop();
    }

    int kthSmall = stack.pop().val;
    return kthSmall;
  }

  public void inOrderTraverse(TreeNode root, Stack<TreeNode> stack) {
    if (root == null) return;
    if (root.right != null) inOrderTraverse(root.right, stack);
    stack.push(root);
    if (root.left != null) inOrderTraverse(root.left, stack);
  }


  public static String decodeOneBracket(String s) {
    if(!s.contains("[")) return s;
    int leftBracketNum = 0, rightBracketNum = 0;
    int leftBracketLoc = -1, rightBracketLoc = -1;
    for(int i = 0; i < s.length(); i++) {
      if(s.charAt(i) == '[') {
        if(leftBracketLoc == -1) leftBracketLoc = i;
        leftBracketNum++;
      }
      if(s.charAt(i) == ']') {
        rightBracketLoc = i;
        rightBracketNum++;
      }
      if(leftBracketNum == rightBracketNum && leftBracketNum != 0) break;
    }

    int repeatNum = getRepeatNum(s, leftBracketLoc);
    int numLen = (repeatNum + "").length();
    String prefix = s.substring(0, leftBracketLoc - numLen);
    String repeat = s.substring(leftBracketLoc+1, rightBracketLoc);
    String suffix = s.substring(rightBracketLoc+1, s.length());
    String mid = "";
    for(int i = 0; i < repeatNum; i++) {
      mid = mid + repeat;
    }

    return prefix + mid + suffix;
  }

  public static int getRepeatNum(String s, int leftBracketLoc) {
    //leftBracketLoc should be larger than 1
    StringBuilder numStr = new StringBuilder();
    for(int i = leftBracketLoc - 1; i >= 0; i--) {
      char c = s.charAt(i);
      if(c >= '0' && c <= '9') {
        numStr.append(c);
      } else {
        break;
      }
    }

    String numString = numStr.reverse().toString();
    int num = Integer.parseInt(numString);
    return num;
  }

  public static int numSquares(int n) {
    if( n <= 1) return n;
    int[] candidates = getCandidates(n);
    return numSquares(n, candidates);
  }

  public static int numSquares(int n, int[] candidates) {
    if( n <= 1) return n;
    int num = Integer.MAX_VALUE;
    for(int candidate : candidates) {
      if(candidate > n) continue;
      int subNum = numSquares(n - candidate);
      num = Math.min(num, 1 + subNum);
    }
    return num;
  }

  public static int[] getCandidates(int n) {
    int num = 1;
    for(num = 1; num < n; num++) {
      if(num * num > n) {
        break;
      }
    }
    num = num - 1;
    int[] candidates = new int[num];
    for(int i = 0; i < num; i++) {
      candidates[i] = (num - i) * (num - i);
    }
    return candidates;
  }

  public static int[][] reconstructQueue(int[][] people) {
    if (people == null || people.length == 0 || people[0].length == 0)
      return new int[0][0];

    Arrays.sort(people, new Comparator<int[]>() {
      public int compare(int[] a, int[] b) {
        if (b[0] == a[0]) return a[1] - b[1];
        return b[0] - a[0];
      }
    });

    int n = people.length;
    ArrayList<int[]> tmp = new ArrayList<>();

    for (int i = 0; i < n; i++)
      tmp.add(people[i][1], people[i]/*new int[]{people[i][0], people[i][1]}*/);

    int[][] res = new int[people.length][2];
    int i = 0;
    for (int[] k : tmp) {
      res[i][0] = k[0];
      res[i++][1] = k[1];
    }

    return res;
  }

  public static void wallsAndGates(int[][] rooms) {
    if (rooms == null) return;
    boolean[][] visited;
    Queue<int[]> queue = new LinkedList<int[]>();
    visited = new boolean[rooms.length][rooms[0].length];

    for (int i = 0; i < rooms.length; i++) {
      for (int j = 0; j < rooms[0].length; j++) {
        if (rooms[i][j] == 0) {
          queue.add(new int[]{i, j});
        }
      }
    }

    while (!queue.isEmpty()) {
      int[] gate = queue.remove();
      visited[gate[0]][gate[1]] = true;
      int x = gate[0], y = gate[1];
      List<int[]> nextSteps = new ArrayList<int[]>();
      nextSteps.add(new int[]{x, y - 1});
      nextSteps.add(new int[]{x, y + 1});
      nextSteps.add(new int[]{x - 1, y});
      nextSteps.add(new int[]{x + 1, y});
      for (int[] next : nextSteps) {
          int x_next = next[0];
          int y_next = next[1];
          if (x_next >= 0 && x_next < rooms.length
              && y_next >= 0 && y_next < rooms[0].length
              && !visited[x_next][y_next]) {
            if (rooms[x_next][y_next] >= 0 && rooms[x_next][y_next] < Integer.MAX_VALUE) {
              queue.add(new int[]{x_next, y_next});
            } else if (rooms[x_next][y_next] == Integer.MAX_VALUE) {
              rooms[x_next][y_next] = rooms[x][y] + 1;
              queue.add(new int[]{x_next, y_next});
            } else if (rooms[x_next][y_next] == -1) {
              continue;
            }
          }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    Graph g = createNewGraph();
    Node[] nodes = g.getNodes();
    Node start = nodes[0];
    Node end = nodes[5];
    System.out.print(breadthFirstSearch(g, start, end) + "\n");
    String test = "dir\n\tsubdir1\n\t\tfile1.ext\n\t\tsubsubdir1\n\tsubdir2\n\t\tsubsubdir2\n\t\t\tfile2.ext";
    int lenght = FindRouteBetweenNodes.lengthLongestPath(test);
    System.out.println(test.length() + "\n");
    System.out.println(lenght + "\n");

    String result = FindRouteBetweenNodes.licenseKeyFormatting("2-4A0r7-4k", 4);
    System.out.println(result);

    String bracket = "3[a]2[bc]";
    String output = FindRouteBetweenNodes.decodeString(bracket);
    System.out.println(output);

    test = "adf/adf";
    String[] results = test.split("/");
    System.out.println(results.length);

    System.out.println(FindRouteBetweenNodes.numSquares(49));
    String yes = start instanceof Node ? "YES" : "NO";
    System.out.println(yes);

    NetcdfFile file = NetcdfFile.open("/Users/feihu/Documents/Data/modis_hdf/MOD08_D3/MOD08_D3.A2016001.006.2016008061022.hdf");
    int varNUm = file.getVariables().size();
    System.out.println(varNUm);

    //[[7,0], [4,4], [7,1], [5,0], [6,1], [5,2]]
    int[][] people = new int[][]{{7, 0},{4, 4},{7, 1},{5, 0},{6, 1},{5, 2}};
    int[][] queue = FindRouteBetweenNodes.reconstructQueue(people);


    int[][] rooms = new int[][]{{2147483647,-1,0,2147483647},
                                {2147483647,2147483647,2147483647,-1},
                                {2147483647,-1,2147483647,-1},
                                {0,-1,2147483647,2147483647}};
    FindRouteBetweenNodes.wallsAndGates(rooms);



    System.out.print("");


  }

}
