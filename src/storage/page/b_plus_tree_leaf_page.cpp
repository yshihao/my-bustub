//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  // 都是私有成员
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 返回值 是大于等于key的最小index 或是maxsindex+1
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  // 第一次进来 会返回index == 1 所以要加判断
  if (GetSize() == 0) {
    return 0;
  }
  int index = -1;
  int l = 0;
  int r = GetSize() - 1;
  // 最后l == r 一定是小于key的最大值或是大于等于key的最小值
  while (l < r) {
    int mid = (l + r) / 2;
    if (comparator(array[mid].first, key) >= 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  index = l;
  if (comparator(array[index].first, key) < 0) {
    index++;
  }
  return index;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  MappingType tmp = array[index];
  // replace with your own code
  KeyType key{tmp.first};
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int size = GetSize();
  int index = KeyIndex(key, comparator);
  for (int i = size - 1; i >= index; i--) {
    array[i + 1] = array[i];
  }
  MappingType tmp = std::make_pair(key, value);
  array[index] = tmp;
  SetSize(size + 1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // 场景:移动一半到右节点
  int size = GetSize();
  int movePos = size / 2;
  int moveLen = size - movePos;
  recipient->CopyNFrom(&array[movePos], moveLen);
  SetSize(movePos);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // 默认实现为 有序的 直接加到当前size的 后面.
  // 叶子节点是真实的数据 不是页面了
  int j = GetSize();
  for (int i = 0; i < size; i++, j++) {
    MappingType p = items[i];
    array[j] = p;
    SetSize(GetSize() + 1);
    // Page *page = bu
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int index = KeyIndex(key, comparator);
  if (index == GetSize()) {
    return false;
  }
  if (comparator(array[index].first, key) > 0) {
    return false;
  }
  *value = array[index].second;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int size = GetSize();
  int index = KeyIndex(key, comparator);
  if (index >= GetSize()) {
    return GetSize();
  }
  if (comparator(array[index].first, key) > 0) {
    return GetSize();
  }
  for (int i = index; i < size; i++) {
    array[i] = array[i + 1];
  }
  SetSize(size - 1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // 从右兄弟 移动到左兄弟
  int len = GetSize();
  recipient->CopyNFrom(array, len);
  SetSize(0);
  recipient->SetNextPageId(next_page_id_);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // 右兄弟移动到左兄弟
  MappingType tmp = array[0];
  int size = GetSize();
  for (int i = 0; i < size - 1; i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
  recipient->CopyNFrom(&tmp, 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int size = GetSize();
  array[size] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // 左兄弟 到 右兄弟
  int size = GetSize();
  MappingType tmp = GetItem(size - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(tmp);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  int size = GetSize();
  for (int i = size; i > 0; i--) {
    array[i] = array[i - 1];
  }
  array[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
