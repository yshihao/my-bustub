/* #include <iostream>



void testarray(int **p) {
  int *t = p[0];
  for (int i = 0; i < 5; i++) {
    cout << t[i] << endl;
  }
  for (int i = 0; i < 5; i++) {
    t[i] = i;
  }
}
void testarray2(int *p) {
  for (int i = 0; i < 5; i++) {
    cout << p[i] << endl;
  }
  for (int i = 0; i < 5; i++) {
    p[i] = i;
  }
}

int main() {
  //  int array[0];  // 长度为0的数组
  //
  //  array[0] = 1;
  //  array[1] = 2;
  //  array[3] = 3;
  //  int a{0};
  //  printf("%d %d\n", array[0], array[1]);
  //  // printf(sizeof(array));
  //
  //  int arr[] = {1, 2, 3, 4, 5};
  //  for (auto r : arr + 1) {
  //    cout << r << endl;
  //  }
  int *p = new int[5];
  for (int i = 0; i < 5; i++) {
    p[i] = 10 + i;
  }
  testarray2(p);
  for (int i = 0; i < 5; i++) {
    cout << p[i] << endl;
  }
  return 0;
}
*/
