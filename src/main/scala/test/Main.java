package test;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n, m, n1, m1;
        n = sc.nextInt();
        m = sc.nextInt();
        int[][] arr = new int[n][m];
        for (int i = 0; i < n; i++)
            for (int j = 0; j < m; j++)
                arr[i][j] = sc.nextInt();
        n1 = sc.nextInt();
        m1 = sc.nextInt();
        int[][] arr2 = new int[n1][m1];
        for (int i = 0; i < n1; i++)
            for (int j = 0; j < m1; j++)
                arr2[i][j] = sc.nextInt();

        if (m != n1)
            System.out.println("Error: " + m + " != " + n1);
        else {
            System.out.println(n + " " + m1);
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < m1; j++) {
                    int sum = 0;
                    for (int k = 0; k < m; k++)
                        sum = sum + arr[i][k] * arr2[k][j];
                    if (j != m1 - 1)
                        System.out.print(sum + " ");
                    else
                        System.out.print(sum);
                }
                System.out.println();
            }
        }
    }
}
