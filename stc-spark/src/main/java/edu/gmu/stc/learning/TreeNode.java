package edu.gmu.stc.learning;

import com.google.gson.Gson;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Created by Fei Hu on 12/11/16.
 */
public class TreeNode {
  int val;
  TreeNode left;
  TreeNode right;
  State state;

  public TreeNode(int v) {
    this.val = v;
  }

  public TreeNode() {

  }

  public TreeNode (int v, TreeNode lNode, TreeNode rNode) {
    this.val = v;
    this.left = lNode;
    this.right = rNode;
  }

  public void setLeftNode(TreeNode left) {
    this.left = left;
  }

  public void setRightNode(TreeNode right) {
    this.right = right;
  }

  public TreeNode createBalancedBinaryTree(int[] arr) {
    if (arr == null) return null;

    return createTreeNode(arr, 0, arr.length-1);

  }

  public TreeNode createTreeNode(int[] arr, int left, int right) {
    if (right < left) {
      return null;
    }

    int mid = (left + right) / 2;

    TreeNode node = new TreeNode(arr[mid]);
    TreeNode leftNode = node.createTreeNode(arr, left, mid - 1);
    TreeNode rightNode = node.createTreeNode(arr, mid + 1, right);
    node.setLeftNode(leftNode);
    node.setRightNode(rightNode);
    return node;
  }

  public TreeNode search(int val) {

    if (this.val == val) {
      return this;
    } else {
      LinkedList<TreeNode> q = new LinkedList<TreeNode>();
      q.add(this.left);
      q.add(this.right);
      TreeNode node = q.pop();

      while (node != null) {
        if (node.val == val) {
          return node;
        } else {
          q.add(node.left);
          q.add(node.right);
        }
        node = q.removeFirst();
      }
    }

    return null;
  }

  public TreeNode search(TreeNode node, int val) {
    if ( node.val == val) {
      return node;
    } else {
      TreeNode n = search(node.left, val);
      if (n == null) {
        n = search(node.right, val);
      }
      return n;
    }
  }

  public int checkHeight(TreeNode node) {
    if (node == null) return -1;

    int leftHeight = checkHeight(node.left);
    int rightHeight = checkHeight(node.left);

    if (leftHeight == Integer.MIN_VALUE) return Integer.MIN_VALUE;
    if (rightHeight == Integer.MIN_VALUE) return Integer.MIN_VALUE;

    if (Math.abs(leftHeight - rightHeight) > 1)
      return Integer.MIN_VALUE;
    else
      return Math.max(leftHeight, rightHeight) + 1;

  }

  // there is a bug here
  public boolean checkBST_1(TreeNode node) {
    if (node == null) return true;

    if (!checkBST_1(node.left)) return false;
    if (node.left != null && node.left.val >= node.val) return false;
    if (node.right != null && node.right.val <= node.val) return false;
    if (!checkBST_1(node.right)) return false;

    return true;
  }

  Integer last_printed = null;

  //from bottom to up
  public boolean checkBST_2(TreeNode node) {
    if (node == null) return true;

    if (!checkBST_2(node.left)) return false;

    //check current
    if (last_printed != null && node.val <= last_printed) return false;

    last_printed = node.val;

    if (!checkBST_2(node.right)) return false;

    return true;
  }

  public int getSize(TreeNode node) {
    if (node == null) return 0;
    int left_size = getSize(node.left);
    int right_size = getSize(node.right);
    int size = left_size + right_size + 1;
    return size;
  }

  //from bottom to up
  public boolean checkBST_3(TreeNode node) {
    int[] arrays = new int[node.getSize(node)];
    copyNodeToArray(node, arrays);
    System.out.println(Arrays.toString(arrays));
    for (int i = 1; i< arrays.length; i++) {
      if (arrays[i] <= arrays[i - 1]) {
        return false;
      }
    }
    return true;
  }

  int index = 0;
  public void copyNodeToArray(TreeNode node,  int[] array) {
    if (node == null) return;
    copyNodeToArray(node.left, array);
    array[index] = node.val;
    index++;
    copyNodeToArray(node.right, array);
  }


  //from the up to bottom
  public boolean checkBST_4(TreeNode node, Integer max, Integer min) {
    if (node == null) return true;
    if (max != null && node.val >= max) return false;
    if (min != null && node.val <= min) return false;
    if (!checkBST_4(node.left, node.val, min) || !checkBST_4(node.right, max, node.val)) return false;
    return true;
  }



  public static void main(String[] args) {
    int[] arrays = new int[] {1, 2, 5, 4, 5, 6, 7};
    TreeNode treeNode = new TreeNode();
    treeNode = treeNode.createBalancedBinaryTree(arrays);
    Gson gson = new Gson();
    String tree = gson.toJson(treeNode);
    System.out.println(treeNode.search(5).val);
    System.out.println(treeNode.search(treeNode, 4).val);
    System.out.println(treeNode.checkBST_1(treeNode));
    System.out.println(treeNode.checkBST_2(treeNode));
    System.out.println(treeNode.checkBST_3(treeNode));
  }

}
