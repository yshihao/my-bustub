//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) {
    rows = r;
    cols = c;
    linear = new T[r * c];
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix 纯虚函数
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    data_ = new T *[r];
    for (int i = 0; i < (this->rows); i++) {
      data_[i] = &this->linear[i * this->cols];
    }
  }
  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }
  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }
  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return this->linear[i * this->cols + j]; }
  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override {
    // data_[i][j] = val;
    this->linear[i * this->cols + j] = val;
  }
  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    for (int i = 0; i < (this->rows) * (this->cols); i++) {
      this->linear[i] = arr[i];
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override { delete[] data_; };

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetRows() != mat2->GetRows() || mat1->GetColumns() != mat2->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int rows = mat1->GetRows();
    int cols = mat1->GetColumns();
    std::unique_ptr<RowMatrix<T>> result(new RowMatrix<T>(rows, cols));
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        T tmp = mat1->GetElem(i, j) + mat2->GetElem(i, j);
        result->SetElem(i, j, tmp);
      }
    }
    return result;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetColumns() != mat2->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int rows = mat1->GetRows();
    int cols = mat2->GetColumns();
    int tmpRow = mat1->GetColumns();
    std::unique_ptr<RowMatrix<T>> results(new RowMatrix<T>(rows, cols));
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        T tmp = 0;
        for (int k = 0; k < tmpRow; k++) {
          tmp += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        results->SetElem(i, j, tmp);
      }
    }
    return results;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    std::unique_ptr<RowMatrix<T>> resultMul = std::move(MultiplyMatrices(std::move(matA), std::move(matB)));
    if (resultMul == std::unique_ptr<RowMatrix<T>>(nullptr)) {
      return resultMul;
    }
    std::unique_ptr<RowMatrix<T>> resultAdd = std::move(AddMatrices(std::move(resultMul), std::move(matC)));
    return resultAdd;
  }
};
}  // namespace bustub
