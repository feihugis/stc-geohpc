package edu.gmu.stc.learning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;
import java.util.TreeMap;

/**
 * Created by Fei Hu on 12/3/16.
 */
public class Solution {

  public int[] twoSum(int[] nums, int target) {
    Map<Integer, Integer> map = new HashMap<>();
    for (int i = 0; i < nums.length; i++) {
      int complement = target - nums[i];
      if (map.containsKey(complement)) {
        return new int[]{i, map.get(complement)};
      }
      map.put(nums[i], i);
    }

    throw new IllegalArgumentException("No such numbers");
  }


  /**
   * You are given two linked lists representing two non-negative numbers. The digits are stored in
   * reverse order and each of their nodes contain a single digit. Add the two numbers and return
   * it as a linked list.
   * Input: (2 -> 4 -> 3) + (5 -> 6 -> 4)
   * Output: 7 -> 0 -> 8
   */
  public class ListNode {
       int val;
      ListNode next;
      ListNode(int x) { val = x; }
  }


  public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
    ListNode result = new ListNode(0);
    ListNode nextNode = result;

    while ( l1 != null || l2 != null) {
      int x = (l1 != null) ? l1.val : 0;
      int y = (l2 != null) ? l2.val : 0;
      int sum = x + y + nextNode.val;

      l1 = (l1 != null) ? l1.next : null;
      l2 = (l2 != null) ? l2.next : null;

      if ( sum >= 10) {
        nextNode.val = sum % 10;
        nextNode.next = new ListNode(1);
        nextNode = nextNode.next;
      } else {
        if( l1 != null || l2 != null) {
          nextNode.val = sum;
          nextNode.next = new ListNode(0);
          nextNode = nextNode.next;
        } else {
          nextNode.val = sum;
        }
      }
    }

    return result;
  }

  int fib(int n) {
    if ( n <= 0) return 0;
    else if (n == 1) return 1;
    return fib(n - 1) + fib(n - 2);
  }

  String compression(String input) {
    String output = "";
    for ( int i = 0; i < input.length();) {
      int count = 0;
      count++;
      char record = input.charAt(i);
      output = output + input.charAt(i);
      for ( i = i + 1; i < input.length(); i++) {
        if( record == input.charAt(i) ) {
          count++;
        } else {
          break;
        }
      }

      output = output + count;
    }

    if (output.length() <= input.length()) {
      return output;
    } else {
      return input;
    }
  }

  public ListNode addTwoNumbersII(ListNode l1, ListNode l2) {
    int l1_number = 1;
    int l2_number = 1;
    ListNode mark = l1;
    while ( mark.next != null) {
      l1_number++;
      mark = mark.next;
    }
    mark = l2;
    while ( mark.next != null) {
      l2_number++;
      mark = mark.next;
    }

    if ( l1_number >= l2_number) {
      mark = l1;
      for (int i = 0; i < l1_number - l2_number; i++) {
        mark = mark.next;
      }

      while (mark.next != null) {
        mark.val = mark.val + l2.val;
        l2 = l2.next;
        mark = mark.next;
      }
      mark.val = mark.val + l2.val;

      mark = l1;
      ListNode result = new ListNode(l1.val/10);
      result.next = l1;

      while ( mark.next != null) {
        mark.val = mark.val%10 + mark.next.val/10;
        mark = mark.next;
      }

      mark.val = mark.val%10;

      if (result.val > 0) {
        return result;
      } else {
        return result.next;
      }


    } else {
      mark = l2;
      for (int i = 0; i < l2_number - l1_number; i++) {
        mark = mark.next;
      }

      while (mark.next != null) {
        mark.val = mark.val + l1.val;
        l1 = l1.next;
        mark = mark.next;
      }
      mark.val = mark.val + l1.val;

      mark = l2;
      ListNode result = new ListNode(l2.val/10);
      result.next = l2;

      while ( mark.next != null) {
        mark.val = mark.val%10 + mark.next.val/10;
        mark = mark.next;
      }

      mark.val = mark.val%10;

      if (result.val > 0) {
        return result;
      } else {
        return result.next;
      }
    }
  }

  public ListNode initializeNode ( int n) {
    ListNode listNode = new ListNode(n);
    ListNode next = new ListNode(0);
    listNode.next = next;
    for (int i = n - 1; i > 0; i--) {
      next.val = i;
      next.next = new ListNode(i-1);
      next = next.next;
    }
    return listNode;
  }

  public int wordsTyping(String[] sentence, int rows, int cols) {
    if (sentence == null) return -1;

    String str = "";
    for (int i = 0; i < sentence.length; i++) {
      str = str + sentence[i] + ' ';
    }

    int len = str.length();
    int start = 0;

    for (int i = 0; i < rows; i++) {
      start = start + cols;
      if ((start%len) == 0) continue;
      if ((start%len) == (len - 1) || str.charAt(start%len) == ' ') {
        start++;
        continue;
      }

      while ((start%len) > 0 && str.charAt(((start%len) - 1)) != ' ') {
        start--;
      }
    }

    return start/len;
  }

  public int[] plusOne(int[] digits) {
    if( digits == null || digits.length == 0)
      throw new IllegalArgumentException();

    int carry = 0;
    int len = digits.length;
    digits[len - 1] = (digits[len - 1] + 1) % 10;
    carry = (digits[len - 1] + 1) / 10;
    if (len == 1) {
      if (carry > 0) {
        return new int[]{carry, digits[0]};
      } else {
        return digits;
      }
    }

    for (int i = digits.length - 2; i >= 0; i--) {
      digits[i] = (digits[i] + carry) % 10;
      carry = (digits[i] + carry) / 10;
    }

    if ( carry == 0) {
      return digits;
    } else {
      int[] result = new int[1 + digits.length];
      result[0] = carry;
      System.arraycopy(digits, 0, result, 1, digits.length);
      return result;
    }

  }

  public String decodeString(String s) {
    char[] chars = s.toCharArray();
    Stack<Integer> counts = new Stack<Integer>();
    Stack<String> encodedStrings = new Stack<String>();
    StringBuilder result = new StringBuilder();
    StringBuilder unit = new StringBuilder();
    int count = 0;

    for (int i = 0; i < chars.length; i++) {
      if (chars[i] >= '0' && chars[i] <= '9') {
        count = count * 10 + chars[i] - '0';
      } else if (chars[i] != '[' && chars[i] != ']') {
        if (counts.size() < 1) {
          result.append(chars[i]);
        } else {
          unit.append(chars[i]);
        }
      }

      if (chars[i] == '[') {
        counts.push(count);
        count = 0;

        if (counts.size() > 1) {
          encodedStrings.push(unit.toString());
          unit = new StringBuilder();
        }
      }


      if (chars[i] == ']') {
        String split;
        int times = counts.pop();
        if (counts.size() == encodedStrings.size()) {
          split = unit.toString();
        } else {
          split = encodedStrings.pop() + unit.toString();
        }
        StringBuilder repeat = new StringBuilder();
        for (int t = 0; t < times; t++) {
          repeat.append(split);
        }

        if (encodedStrings.size() > 0) {
          String lastUnit = encodedStrings.pop() + repeat.toString();
          encodedStrings.push(lastUnit);
        } else {
          result.append(repeat);
        }

        unit = new StringBuilder();
      }
    }

    return result.toString();

  }

  public void quickSort(int[] nums, int left, int right) {
    int index = partition(nums, left, right);
    if (left < index - 1) quickSort(nums, left, index - 1);
    if (index < right) quickSort(nums, index, right);
  }

  public int partition(int[] nums, int left, int right) {
    int pivot = nums[(left + right) / 2];
    while (left <= right) {
      while (nums[left] < pivot) left++;
      while (nums[right] > pivot) right--;
      if (left <= right) {
        int tmp = nums[left];
        nums[left] = nums[right];
        nums[right] = tmp;
        left++;
        right--;
      }
    }
    return left;
  }

  boolean[][] visited;

  public int numIslands(char[][] grid) {
    if(grid == null || grid.length == 0 || grid[0].length == 0) return 0;
    int height = grid.length, width = grid[0].length;
    int result = 0;

    visited = new boolean[height][width];
    for (int i = 0; i < height; i++) {
      for (int j = 0; j < width; j++) {
        if (!visited[i][j] && grid[i][j] == '1') {
          result++;
          bfs(j, i, grid);
        }
      }
    }
    return result;
  }

  public void bfs(int x, int y, char[][] grid) {
    System.out.print(String.format("x = %d, y = %d  width = %d height = %d \n", x, y, grid[0].length, grid.length));

    if (x >= 0 && x < grid[0].length && y >= 0 && y < grid.length && !visited[y][x] && grid[y][x] == '1') {
      visited[y][x] = true;
      bfs(x-1, y, grid);
      bfs(x+1, y, grid);
      bfs(x, y-1, grid);
      bfs(x, y+1, grid);
    }

  }

  public boolean isStrobogrammatic(String num) {
    if ( num == null) return false;
    int len = num.length();
    for (int i = 0; i <= len/2; i++) {
      char left = num.charAt(i), right = num.charAt(len - 1 - i);
      boolean v = isStrobogrammatic(left, right);
      if (!isStrobogrammatic(left, right)) {
        return false;
      }
    }
    return true;
  }

  public boolean isStrobogrammatic(char left, char right) {
    if (left == '0' && right == '0') return true;
    if (left == '1' && right == '1') return true;
    if (left == '6' && right == '9') return true;
    if (left == '8' && right == '8') return true;
    if (left == '9' && right == '6') return true;
    return false;
  }

  public String reverseVowels(String s) {
    if (s == null || s.length() == 0 || s.equals(" ")) return s;

    HashSet<Character> map = new HashSet<Character>();
    map.add('a');
    map.add('e');
    map.add('i');
    map.add('o');
    map.add('u');
    map.add('A');
    map.add('E');
    map.add('I');
    map.add('O');
    map.add('U');
    char[] cArray = s.toCharArray();

    int low = 0, high = s.length() - 1;
    while (low <= high) {
      while (!map.contains(cArray[low])) low++;
      while (!map.contains(cArray[high])) high--;

      if(low <= high) {
        char tmp = cArray[low];
        cArray[low] = cArray[high];
        cArray[high] = tmp;
      }

      low++;
      high--;
    }
    return new String(cArray);
  }

  public List<String> readBinaryWatch(int num) {
    if (num < 0 || num > 10) return null;

    List<String> results = new ArrayList<String>();
    int[] hours = new int[] {8, 4, 2, 1};
    int[] minutes = new int[] {32, 16, 8, 4, 2, 1};

    int hNum = 0;
    while (hNum <= Math.min(num, 4)) {
      int mNum = num - hNum;
      if (mNum > 6) {
        hNum++;
        continue;
      }

      List<Integer> hoursList = generateDigits(hours, hNum, 0, hours.length - 1);
      List<Integer> minituesList = generateDigits(minutes, mNum, 0, minutes.length - 1);

      for (Integer hour : hoursList) {
        if (hour >= 0 && hour <= 11) {
          for (Integer mini: minituesList) {
            if (mini >= 0 && mini <= 59) {
              String mm = (mini < 10)? String.format("0%d", mini): String.format("%d", mini);
              results.add(hour + ":" + mm);
            }
          }
        }
      }

      hNum++;
    }

    return results;
  }

  public List<Integer> generateDigits(int[] array, int num, int low, int high) {
    List<Integer> sum = new ArrayList<Integer>();

    if (array == null || num <= 0 || array.length < num || (high - low + 1 < num)) {
      sum.add(0);
      return sum;
    }

    for (int i = low; i <= high - num + 1; i++ ) {
      int first = array[i];
      List<Integer> subSum = generateDigits(array, num - 1, i+1, high);
      for (Integer rest : subSum) {
        sum.add(first + rest);
      }
    }

    return sum;
  }

  int count = 0;
  public int rob(int[] nums) {
    if (nums == null || nums.length == 0) return 0;
    return rob(nums, 0, nums.length - 1);
  }

  public int rob(int[] nums, int start, int end) {
    if (start > end) return 0;
    if (start == end) return nums[start];
    count++;
    int situation1 = nums[start] + rob(nums, start + 2, end);
    int situation2 = rob(nums, start + 1, end);
    return Math.max(situation1, situation2);
  }

  int [] record;

  public int rob_2(int[] nums) {
    if (nums == null || nums.length == 0) return 0;
    record = new int[nums.length];
    count = 0;
    return rob_2(nums, 0, nums.length - 1);
  }

  public int rob_2(int[] nums, int start, int end) {

    if (start > end) return 0;
    if (start == end) return nums[start];
    if (record[start] > 0) return record[start];

    count++;

    int situation1 = nums[start] + rob(nums, start + 2, end);
    int situation2 = rob(nums, start + 1, end);
    int max = Math.max(situation1, situation2);
    record[start] = max;
    return max;
  }

  public static void main(String[] args) {
    Solution solution = new Solution();
    int[] numbers = new int[]{2, 7, 11, 15};
    int target = 9;
    //solution.twoSum(numbers, target);
    //System.out.print(solution.fib(6));
    System.out.println(solution.compression("111aaabbbceddddddddd"));

    ListNode node = solution.addTwoNumbersII(solution.initializeNode(4), solution.initializeNode(5));

    while (node.next != null) {
      System.out.println(node.val);
      node = node.next;
    }

    int[] array = new int[4];
    int[][] arrays = new int[4][5];

    Stack<Integer> stack = new Stack<Integer>();
    stack.push(1);
    stack.peek();
    stack.empty();
    stack.isEmpty();

    int[] copy = Arrays.copyOf(numbers, numbers.length);
    System.out.print(Arrays.toString(copy));

    for (int i = 1; i < numbers.length; i++) {
      copy[i] = Math.min(numbers[i - 1], copy[i - 1]);
    }
    System.out.print(Arrays.toString(copy));
    //TreeMap

    List<Integer> arr = new ArrayList<Integer>();

    Queue<Integer> queue = new LinkedList<Integer>();

    //LinkedList

    MovingAverage movingAverage = new MovingAverage(3);
    System.out.println(movingAverage.next(1));
    System.out.println(movingAverage.next(2));
    System.out.println(movingAverage.next(3));
    System.out.println(movingAverage.next(4));

    System.out.println((int) "0!abc".charAt(1));

    String[] strings = new String[]{"hello", "world"};

    System.out.println(solution.wordsTyping(strings, 2, 8));

    System.out.println(Arrays.toString(solution.plusOne(new int[]{9})));
    System.out.println(solution.decodeString("3[a2[c]]"));

    int[] nums = new int[] {2,6,3,6};
    solution.quickSort(nums, 0 , nums.length - 1);
    System.out.println(Arrays.toString(nums));

    char[][] grid = new char[][]{"11110".toCharArray(),"11010".toCharArray(),"11000".toCharArray(),"00000".toCharArray()};
    System.out.println(solution.numIslands(grid));

    System.out.println(solution.isStrobogrammatic("6"));

    //System.out.println(solution.reverseVowels("."));

    System.out.println(Arrays.toString(solution.readBinaryWatch(7).toArray()));
    int[] input = new int[]{117,207,117,235,82,90,67,143,146,53,108,200,91,80,223,58,170,110,236,81,90,222,160,165,195,187,199,114,235,197,187,69,129,64,214,228,78,188,67,205,94,205,169,241,202,144, 240};
    //System.out.println(solution.rob(input));
    System.out.println(solution.count + ":" + input.length);
    //System.out.println(solution.rob_2(input));
    System.out.println(solution.count + ":" + input.length);

    Queue<Integer> qi = new PriorityQueue<Integer>();

    qi.add(5);
    qi.add(2);
    qi.add(1);
    qi.add(10);
    qi.add(3);

    while (!qi.isEmpty()) {
      System.out.print(qi.poll() + ",");
    }
    System.out.println();
    System.out.println("-----------------------------");
    // 自定义的比较器，可以让我们自由定义比较的顺序 Comparator<Integer> cmp;
    Comparator<Integer> cmp = new Comparator<Integer>() {
      public int compare(Integer e1, Integer e2) {
        if (e1 >= e2) {
          return 1;
        } else {
          return -1;
        }
      }
    };

    Queue<Integer> q2 = new PriorityQueue<Integer>(5, cmp);
    q2.add(2);
    q2.add(8);
    q2.add(9);
    q2.add(1);
    while (!q2.isEmpty()) {
      System.out.print(q2.poll() + ",");
    }

  }

}


class MovingAverage {
  List<Integer> valList;
  int size = -1;
  double avg;

  /** Initialize your data structure here. */
  public MovingAverage(int size) {
    this.size = size;
    valList = new ArrayList<Integer>();
  }

  public double next(int val) {
    if(valList.size() == this.size) {
      valList.remove(0);
    }

    valList.add(val);

    double sum = 0.0;
    for(int x : valList) {
      sum = sum + x;
    }

    this.avg = sum / valList.size();

    return this.avg;
  }
}
