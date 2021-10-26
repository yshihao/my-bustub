#include <iostream>

using namespace std;

int main() {
  int array[0];  // 长度为0的数组

  array[0] = 1;
  array[1] = 2;
  array[3] = 3;
  int a{0};
  printf("%d %d\n", array[0], array[1]);
  // printf(sizeof(array));

  int arr[] = {1, 2, 3, 4, 5};
  for (auto r : arr + 1) {
    cout << r << endl;
  }
  return 0;
}