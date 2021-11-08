//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  //  page_id_t page_id = root_page_id_;
  //  if (page_id == INVALID_PAGE_ID) {
  //    return false;
  //  }
  // 要用到根节点的地方 都应该统一处理 统一加锁 和释放锁 没有提供transaction
  // ToTest(buffer_pool_manager_->FetchPage(root_page_id_), buffer_pool_manager_);
  Page *page = IteratorFindLeafPage(key, false);
  if (page == nullptr) {
    // 空树
    my_latch.unlock();
    return false;
  }
  LeafPage *btlp = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType tmp;
  bool tmpResult = btlp->Lookup(key, &tmp, comparator_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  if (tmpResult) {
    result->push_back(tmp);
    return true;
  }
  return false;
}

/*
 * 只transaction里的写锁 和unpin page
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLock(Transaction *transaction, bool isChange, bool isWLock) {
  if (transaction == nullptr) {
    return;
  }
  std::deque<Page *> &de = *(transaction->GetPageSet());
  while (!de.empty()) {
    Page *tmpPage = de.front();
    de.pop_front();
    if (tmpPage == nullptr) {
      my_latch.unlock();
      continue;
    }
    if (isWLock) {
      tmpPage->WUnlatch();
    } else {
      tmpPage->RUnlatch();
    }
    if (isChange) {
      buffer_pool_manager_->UnpinPage(tmpPage->GetPageId(), true);
    } else {
      buffer_pool_manager_->UnpinPage(tmpPage->GetPageId(), false);
    }
    //    assert( == true );
  }
  // Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  // ToTest(page, buffer_pool_manager_);
  for (auto tmp : *(transaction->GetDeletedPageSet())) {
    // assert(buffer_pool_manager_->DeletePage(tmp) == true);
    buffer_pool_manager_->DeletePage(tmp);
  }
  transaction->GetDeletedPageSet()->clear();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //  my_latch.lock();
  //  if (IsEmpty()) {
  //    StartNewTree(key, value);
  //    my_latch.unlock();
  //    return true;
  //  }
  //  my_latch.unlock();
  // LOG_INFO("事务是 %d,插入值是 %lld", transaction->GetTransactionId() , key.ToString());
  bool result = InsertIntoLeaf(key, value, transaction);
  // if (result) {
  //   LOG_INFO("事务是 %d,插入值是 %lld,插入成功", transaction->GetTransactionId(), key.ToString());
  // } else {
  //   LOG_INFO("事务是 %d,插入值是 %lld,插入失败", transaction->GetTransactionId(), key.ToString());
  // }
  // Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  // ToTest(page, buffer_pool_manager_);

  return result;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "OUT_OF_MEMORY");
  }
  LeafPage *btlp = reinterpret_cast<LeafPage *>(page->GetData());
  btlp->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  btlp->SetPageType(IndexPageType::LEAF_PAGE);
  btlp->Insert(key, value, comparator_);
  root_page_id_ = page_id;
  buffer_pool_manager_->UnpinPage(page_id, true);
  UpdateRootPageId(1);  // set true when insert
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 对于插入和删除 返回的page set不空 一定是leafpage可能变化的情况
  Page *page = FindLeafPage(key, transaction, OPERATION_TYPE::INSERT);
  if (page == nullptr) {
    StartNewTree(key, value);
    ReleaseLock(transaction, true, true);
    return true;
  }
  //  while ((page = FindLeafPage(key, false, transaction, OPERATION_TYPE::INSERT)) == nullptr) {
  //  }
  // LOG_INFO("事务是 %d,插入值是 %lld,开始插入第%d 页", transaction->GetTransactionId(), key.ToString(),
  //          page->GetPageId());
  LeafPage *result = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType tmpValue;
  if (result->Lookup(key, &tmpValue, comparator_)) {
    ReleaseLock(transaction, false, true);
    return false;
  }
  result->Insert(key, value, comparator_);
  // result->GetSize(); 要注意拆分
  if (result->GetSize() == result->GetMaxSize()) {
    LeafPage *tmp = Split<LeafPage>(result);
    KeyType keyv = tmp->KeyAt(0);
    InsertIntoParent(result, keyv, tmp, transaction);
    // 对新生成的页刷新过了
    // buffer_pool_manager_->UnpinPage(tmp->GetPageId(), true);
  }
  // 对旧页面进行刷新 不刷新会丢失数据 报错
  ReleaseLock(transaction, true, true);
  // LOG_INFO("事务是 %d,插入值是 %lld,插入成功", transaction->GetTransactionId(), key.ToString());
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "OUT_OF_MEMORY");
  }
  BPlusTreePage *mynode = reinterpret_cast<BPlusTreePage *>(node);

  if (mynode->IsLeafPage()) {
    LeafPage *oldLeafPage = reinterpret_cast<LeafPage *>(mynode);
    LeafPage *newPage = reinterpret_cast<LeafPage *>(page->GetData());
    newPage->Init(page_id, mynode->GetParentPageId(), leaf_max_size_);
    newPage->SetPageType(IndexPageType::LEAF_PAGE);
    oldLeafPage->MoveHalfTo(newPage);
    newPage->SetNextPageId(oldLeafPage->GetNextPageId());
    oldLeafPage->SetNextPageId(page_id);
    buffer_pool_manager_->UnpinPage(page_id, true);
    N *result = reinterpret_cast<N *>(newPage);
    return result;
  }
  InternalPage *inOldPage = reinterpret_cast<InternalPage *>(mynode);
  InternalPage *inPage = reinterpret_cast<InternalPage *>(page->GetData());
  inPage->Init(page_id, mynode->GetParentPageId(), internal_max_size_);
  inPage->SetPageType(IndexPageType::INTERNAL_PAGE);
  inOldPage->MoveHalfTo(inPage, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(page_id, true);
  N *result = reinterpret_cast<N *>(inPage);
  return result;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // LOG_INFO("what happended 2\n");
  page_id_t parent_page_id = old_node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    Page *parent_page = buffer_pool_manager_->NewPage(&parent_page_id);
    if (parent_page == nullptr) {
      ReleaseLock(transaction, true, true);
      throw Exception(ExceptionType::OUT_OF_MEMORY, "OUT_OF_MEMORY");
    }

    InternalPage *in_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    in_page->Init(parent_page_id, INVALID_PAGE_ID, internal_max_size_);
    in_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    in_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = parent_page_id;  // 根结点变化后 需要修改
    UpdateRootPageId(0);
    // 还要设置父节点号
    old_node->SetParentPageId(parent_page_id);
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return;
  }
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *in_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 内部节点插入后大于max_size 才分裂,无法存下
  in_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (in_page->GetSize() > in_page->GetMaxSize()) {
    InternalPage *in_new_page = Split<InternalPage>(in_page);
    // 分裂后要 设置父页面号  这样设置正确嘛？
    // 内部节点的分裂是不一样的
    in_new_page->SetParentPageId(in_page->GetParentPageId());
    KeyType middle_key = in_new_page->KeyAt(0);
    InsertIntoParent(in_page, middle_key, in_new_page);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 * 为什么需要transaction保存已加锁的page set呢？ 删除时若redistribute 则不会再向上。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  //  my_latch.lock();
  //  if (IsEmpty()) {
  //    my_latch.unlock();
  //    return;
  //  }
  //  my_latch.unlock();
  Page *page = FindLeafPage(key, transaction, OPERATION_TYPE::DELETE);
  if (page == nullptr) {
    ReleaseLock(transaction, false);
    return;
  }
  // LOG_INFO("事务是 %d,删除值是 %lld，在第%d 页开始删除", transaction->GetTransactionId(), key.ToString(),
  //          page->GetPageId());
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(page->GetData());
  int size = leafPage->RemoveAndDeleteRecord(key, comparator_);
  int minSize = leafPage->GetMinSize();
  if (size < minSize) {
    // 合并或是 重新排列
    CoalesceOrRedistribute(leafPage, transaction);
  }
  ReleaseLock(transaction, true, true);
  // LOG_INFO("事务是 %d,删除值是 %lld，已经完成", transaction->GetTransactionId(), key.ToString());
  // std::string name = std::to_string(key.ToString()) + "delete.dot";
  // Draw(buffer_pool_manager_, name);
  // page = buffer_pool_manager_->FetchPage(root_page_id_);
  // ToTest(page, buffer_pool_manager_);
  // page->WUnlatch();
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}
/*
 * find the sibling of node
 * find the left sibling node if left == true,
 * otherwise find the right sibling node
 * @return: sibling node
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::GetSiblings(N *node, bool left, Transaction *transaction) {
  BPlusTreePage *bpt = reinterpret_cast<BPlusTreePage *>(node);
  page_id_t parentId = bpt->GetParentPageId();
  if (parentId == INVALID_PAGE_ID) {
    return nullptr;
  }
  InternalPage *internalPage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parentId)->GetData());
  int index = internalPage->ValueIndex(bpt->GetPageId());
  if (left) {
    N *result = nullptr;
    if (index > 0) {
      page_id_t vt = internalPage->ValueAt(index - 1);
      Page *page = buffer_pool_manager_->FetchPage(vt);
      page->WLatch();
      transaction->AddIntoPageSet(page);
      result = reinterpret_cast<N *>(page->GetData());
    }
    buffer_pool_manager_->UnpinPage(parentId, false);
    return result;
  }
  N *result = nullptr;
  if (index < internalPage->GetSize() - 1) {
    page_id_t vt = internalPage->ValueAt(index + 1);
    Page *page = buffer_pool_manager_->FetchPage(vt);
    page->WLatch();
    //为空？
    transaction->AddIntoPageSet(page);
    result = reinterpret_cast<N *>(page->GetData());
  }
  // 函数内的fetchpage 要unpin掉
  buffer_pool_manager_->UnpinPage(parentId, false);
  return result;
}
/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * 某个函数 调用它
 * 返回的是 node这个节点需不需要被删除
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  BPlusTreePage *bpt = reinterpret_cast<BPlusTreePage *>(node);
  int nowSize = bpt->GetSize();
  if (bpt->GetParentPageId() == INVALID_PAGE_ID) {
    if (bpt->IsLeafPage()) {
      LeafPage *leafPage = reinterpret_cast<LeafPage *>(node);
      bool isDel = AdjustRoot(leafPage);
      // 特判
      transaction->AddIntoDeletedPageSet(leafPage->GetPageId());
      return isDel;
    }
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(node);
    bool isDel = AdjustRoot(internalPage);
    return isDel;
  }
  if (bpt->IsLeafPage()) {
    LeafPage *leafPage = reinterpret_cast<LeafPage *>(node);
    LeafPage *rightBro = GetSiblings(leafPage, false, transaction);
    // 对这些节点的缓冲区管理
    if (rightBro != nullptr) {
      int rightSize = rightBro->GetSize();
      if (nowSize + rightSize < leafPage->GetMaxSize()) {
        InternalPage *parentPage =
            reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(bpt->GetParentPageId())->GetData());
        bool needDel = Coalesce(&rightBro, &leafPage, &parentPage, 1, transaction);
        if (needDel) {
          // 加入的不会有 pincount ==0
          transaction->AddIntoDeletedPageSet(parentPage->GetPageId());
          // buffer_pool_manager_->DeletePage(parentPage->GetPageId());
        }
        return false;
      }
      Redistribute(rightBro, leafPage, 0);
      return false;
    }
    LeafPage *leftBro = GetSiblings(leafPage, true, transaction);
    int leftSize = leftBro->GetSize();
    if (leftSize + nowSize < leafPage->GetMaxSize()) {
      InternalPage *parentPage =
          reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(bpt->GetParentPageId())->GetData());
      bool needDel = Coalesce(&leftBro, &leafPage, &parentPage, 0, transaction);
      if (needDel) {
        transaction->AddIntoDeletedPageSet(parentPage->GetPageId());
        // buffer_pool_manager_->DeletePage(parentPage->GetPageId());
      }
      return true;
    }
    Redistribute(leftBro, leafPage, 1);
    return false;
  }
  // 为internal节点
  InternalPage *internalPage = reinterpret_cast<InternalPage *>(node);
  InternalPage *internalRightBro = GetSiblings(internalPage, false, transaction);
  if (internalRightBro != nullptr) {
    int rightSize = internalRightBro->GetSize();
    if (nowSize + rightSize <= internalPage->GetMaxSize()) {
      InternalPage *parentPage =
          reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(bpt->GetParentPageId())->GetData());
      bool needDel = Coalesce(&internalRightBro, &internalPage, &parentPage, 1, transaction);
      if (needDel) {
        transaction->AddIntoDeletedPageSet(parentPage->GetPageId());
        // buffer_pool_manager_->DeletePage(parentPage->GetPageId());
      }
      return false;
    }
    Redistribute(internalRightBro, internalPage, 0);
    return false;
  }
  InternalPage *internalLeftBro = GetSiblings(internalPage, true, transaction);
  int leftSize = internalLeftBro->GetSize();
  if (nowSize + leftSize <= internalPage->GetMaxSize()) {
    // 应该是一次FetchPage 对应于一次UnPinPage
    InternalPage *parentPage =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(bpt->GetParentPageId())->GetData());
    bool needDel = Coalesce(&internalLeftBro, &internalPage, &parentPage, 0, transaction);
    if (needDel) {
      transaction->AddIntoDeletedPageSet(parentPage->GetPageId());
      // buffer_pool_manager_->DeletePage(parentPage->GetPageId());
    }
    return true;
  }
  Redistribute(internalLeftBro, internalPage, 1);
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 * 有可能会导致递归
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  // index == 0  neighbor_node 是 当前node的左节点
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parentNode = parent[0];
  BPlusTreePage *tmpNode = reinterpret_cast<BPlusTreePage *>(node[0]);
  if (tmpNode->IsLeafPage()) {
    LeafPage *neighborNode = reinterpret_cast<LeafPage *>(neighbor_node[0]);
    LeafPage *nowNode = reinterpret_cast<LeafPage *>(node[0]);
    if (index == 0) {
      nowNode->MoveAllTo(neighborNode);
      neighborNode->SetNextPageId(nowNode->GetNextPageId());
      int pindex = parentNode->ValueIndex(nowNode->GetPageId());
      parentNode->Remove(pindex);
      // buffer_pool_manager_->
      // // 要先unpin才行
      // buffer_pool_manager_->DeletePage(nowNode->GetPageId());
      // buffer_pool_manager_->UnpinPage(neighborNode->GetPageId(), true);
      transaction->AddIntoDeletedPageSet(nowNode->GetPageId());
    } else {
      neighborNode->MoveAllTo(nowNode);
      nowNode->SetNextPageId(neighborNode->GetNextPageId());
      int pindex = parentNode->ValueIndex(neighborNode->GetPageId());
      parentNode->Remove(pindex);
      transaction->AddIntoDeletedPageSet(neighborNode->GetPageId());
      // buffer_pool_manager_->DeletePage(neighborNode->GetPageId());
      // buffer_pool_manager_->UnpinPage(nowNode->GetPageId(), true);
    }

    if (parentNode->GetSize() < parentNode->GetMinSize()) {
      return CoalesceOrRedistribute(parentNode, transaction);
    }
    buffer_pool_manager_->UnpinPage(parentNode->GetPageId(), true);
    return false;
  }
  InternalPage *neighborNode = reinterpret_cast<InternalPage *>(neighbor_node[0]);
  InternalPage *nowNode = reinterpret_cast<InternalPage *>(node[0]);
  // index ==0 if 邻居节点是左邻居
  if (index == 0) {
    int pindex = parentNode->ValueIndex(nowNode->GetPageId());
    KeyType middle_key = parentNode->KeyAt(pindex);
    nowNode->MoveAllTo(neighborNode, middle_key, buffer_pool_manager_);
    parentNode->Remove(pindex);
    transaction->AddIntoDeletedPageSet(nowNode->GetPageId());
    // buffer_pool_manager_->DeletePage(nowNode->GetPageId());
    // buffer_pool_manager_->UnpinPage(neighborNode->GetPageId(), true);
  } else {
    int pindex = parentNode->ValueIndex(neighborNode->GetPageId());
    KeyType middle_key = parentNode->KeyAt(pindex);
    neighborNode->MoveAllTo(nowNode, middle_key, buffer_pool_manager_);
    parentNode->Remove(pindex);
    transaction->AddIntoDeletedPageSet(neighborNode->GetPageId());
    // buffer_pool_manager_->DeletePage(neighborNode->GetPageId());
    // buffer_pool_manager_->UnpinPage(nowNode->GetPageId(), true);
  }
  if (parentNode->GetSize() < parentNode->GetMinSize()) {
    return CoalesceOrRedistribute(parentNode, transaction);
  }
  buffer_pool_manager_->UnpinPage(parentNode->GetPageId(), true);
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  BPlusTreePage *bpt = reinterpret_cast<BPlusTreePage *>(node);
  // index == 0 neighbor_node 是右兄弟
  // 肯定满足重新分布的条件 先看能合并 再看重新分配
  if (index == 0) {
    if (bpt->IsLeafPage()) {
      LeafPage *leafPage = reinterpret_cast<LeafPage *>(node);
      LeafPage *neighborLeafPage = reinterpret_cast<LeafPage *>(neighbor_node);
      InternalPage *parentPage =
          reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leafPage->GetParentPageId())->GetData());
      int pindex = parentPage->ValueIndex(neighborLeafPage->GetPageId());
      // 此时父节点的key也会变化
      // parentPage->SetKeyAt(pindex, neighborLeafPage->KeyAt(0));
      // 这里的顺序很重要！！！
      neighborLeafPage->MoveFirstToEndOf(leafPage);
      parentPage->SetKeyAt(pindex, neighborLeafPage->KeyAt(0));
      // buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
      // buffer_pool_manager_->UnpinPage(neighborLeafPage->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
      return;
    }
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighborInternalPage = reinterpret_cast<InternalPage *>(neighbor_node);
    InternalPage *parentPage =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internalPage->GetParentPageId())->GetData());
    int pindex = parentPage->ValueIndex(neighborInternalPage->GetPageId());
    KeyType middle_key = parentPage->KeyAt(pindex);
    // 内部节点第一个key是无效的
    KeyType tmp = neighborInternalPage->KeyAt(1);
    parentPage->SetKeyAt(pindex, tmp);
    neighborInternalPage->MoveFirstToEndOf(internalPage, middle_key, buffer_pool_manager_);
    // buffer_pool_manager_->UnpinPage(internalPage->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(neighborInternalPage->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
    return;
  }
  if (bpt->IsLeafPage()) {
    LeafPage *leafPage = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighborLeafPage = reinterpret_cast<LeafPage *>(neighbor_node);
    InternalPage *parentPage =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leafPage->GetParentPageId())->GetData());
    int pindex = parentPage->ValueIndex(leafPage->GetPageId());
    // 此时父节点的key也会变化
    parentPage->SetKeyAt(pindex, neighborLeafPage->KeyAt(neighborLeafPage->GetSize() - 1));
    neighborLeafPage->MoveLastToFrontOf(leafPage);
    // buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(neighborLeafPage->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
    return;
  }
  InternalPage *internalPage = reinterpret_cast<InternalPage *>(node);
  InternalPage *neighborInternalPage = reinterpret_cast<InternalPage *>(neighbor_node);
  InternalPage *parentPage =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internalPage->GetParentPageId())->GetData());
  int pindex = parentPage->ValueIndex(internalPage->GetPageId());
  KeyType middle_key = parentPage->KeyAt(pindex);
  KeyType tmp = neighborInternalPage->KeyAt(neighborInternalPage->GetSize() - 1);
  // 此时父节点的key也会变化
  parentPage->SetKeyAt(pindex, tmp);
  neighborInternalPage->MoveLastToFrontOf(internalPage, middle_key, buffer_pool_manager_);
  // buffer_pool_manager_->UnpinPage(internalPage->GetPageId(), true);
  // buffer_pool_manager_->UnpinPage(neighborInternalPage->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() > 1) {
    return false;
  }
  if (old_root_node->GetSize() > 0) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t vt = internalPage->ValueAt(0);
    Page *page = buffer_pool_manager_->FetchPage(vt);
    BPlusTreePage *bpt = reinterpret_cast<BPlusTreePage *>(page->GetData());
    bpt->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = vt;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(vt, true);
    return true;
  }
  root_page_id_ = INVALID_PAGE_ID;
  UpdateRootPageId(0);
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  Page *page = IteratorFindLeafPage(KeyType{}, true);
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(page->GetData());
  // page->RUnlatch();
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return INDEXITERATOR_TYPE(leafPage, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = IteratorFindLeafPage(key, false);
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(page->GetData());
  int offset = leafPage->KeyIndex(key, comparator_);
  // page->RUnlatch();
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return INDEXITERATOR_TYPE(leafPage, buffer_pool_manager_, offset);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  my_latch.lock();
  Page *parent_page = nullptr;
  Page *page = nullptr;
  page_id_t pageId = root_page_id_;
  while (true) {
    page = buffer_pool_manager_->FetchPage(pageId);
    page->RLatch();
    BPlusTreePage *bpt = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (bpt->IsLeafPage()) {
      if (parent_page != nullptr) {
        parent_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      } else {
        my_latch.unlock();
      }
      break;
    }
    InternalPage *btip = reinterpret_cast<InternalPage *>(bpt);
    page_id_t tmp_page_id = btip->ValueAt(btip->GetSize() - 1);
    if (parent_page != nullptr) {
      parent_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    } else {
      my_latch.unlock();
    }
    parent_page = page;
    pageId = tmp_page_id;
  }
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(leafPage, buffer_pool_manager_, leafPage->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::IteratorFindLeafPage(const KeyType &key, bool leftmost) {
  my_latch.lock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }
  Page *parent_page = nullptr;
  Page *page = nullptr;
  page_id_t pageId = root_page_id_;
  while (true) {
    page = buffer_pool_manager_->FetchPage(pageId);
    page->RLatch();
    BPlusTreePage *bpt = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (bpt->IsLeafPage()) {
      if (parent_page != nullptr) {
        parent_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      } else {
        my_latch.unlock();
      }
      return page;
    }
    InternalPage *btip = reinterpret_cast<InternalPage *>(bpt);
    page_id_t tmp_page_id = leftmost ? btip->ValueAt(0) : btip->Lookup(key, comparator_);
    if (parent_page != nullptr) {
      parent_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    } else {
      my_latch.unlock();
    }
    parent_page = page;
    pageId = tmp_page_id;
  }
}
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * @parameter: operType  0--query 1--insert 2--delete meaningful when leftMost equals false
 * @return transaction 拥有锁的所有page
 * 只被search delete 和insert调用
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction, OPERATION_TYPE operType) {
  my_latch.lock();
  // 表示已经加锁
  transaction->AddIntoPageSet(nullptr);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }
  page_id_t page_id = root_page_id_;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if (operType == OPERATION_TYPE::INSERT || operType == OPERATION_TYPE::DELETE) {
      page->WLatch();
    } else {
      page->RLatch();
    }
    BPlusTreePage *btp = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (btp->IsLeafPage()) {
      if (operType == OPERATION_TYPE::INSERT) {
        if (btp->GetSize() < btp->GetMaxSize() - 1) {
          ReleaseLock(transaction, false);
        }
        transaction->AddIntoPageSet(page);
      }
      if (operType == OPERATION_TYPE::DELETE) {
        if (btp->GetSize() > btp->GetMinSize()) {
          ReleaseLock(transaction, false);
        }
        transaction->AddIntoPageSet(page);
      }
      if (operType == OPERATION_TYPE::SEARCH) {
        ReleaseLock(transaction, false, false);
        transaction->AddIntoPageSet(page);
      }
      // transaction->AddIntoPageSet(page);
      return page;
    }
    InternalPage *btip = reinterpret_cast<InternalPage *>(btp);
    page_id_t tmp_page_id = btip->Lookup(key, comparator_);
    if (operType == OPERATION_TYPE::SEARCH) {
      ReleaseLock(transaction, false, false);
      transaction->AddIntoPageSet(page);
    } else if (operType == OPERATION_TYPE::INSERT) {
      // 插入操作
      InternalPage *bpt = reinterpret_cast<InternalPage *>(page->GetData());
      if (bpt->GetSize() < bpt->GetMaxSize()) {
        ReleaseLock(transaction, false);
      }
      transaction->AddIntoPageSet(page);
    } else if (operType == OPERATION_TYPE::DELETE) {
      // 删除操作
      InternalPage *bpt = reinterpret_cast<InternalPage *>(page->GetData());
      if (bpt->GetSize() > bpt->GetMinSize()) {
        ReleaseLock(transaction, false);
      }
      transaction->AddIntoPageSet(page);
    }
    page_id = tmp_page_id;
  }
  return nullptr;
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToTest(Page *mypage, BufferPoolManager *bpm) const {
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(mypage->GetData());
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
    //           << " next: " << leaf->GetNextPageId() <<"Pin count:"<< leaf->GetPinCount()<< std::endl;
    std::cout << "Leaf Page: " << leaf->GetPageId() << "pin count:" << mypage->GetPinCount() - 1 << std::endl;
    // for (int i = 0; i < leaf->GetSize(); i++) {
    //   std::cout << leaf->KeyAt(i) << ",";
    // }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    // std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() <<"Pin
    // count:"<< leaf->GetPinCount()<< std::endl; for (int i = 0; i < internal->GetSize(); i++) {
    //   std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    // }
    std::cout << "Internal Page: " << internal->GetPageId() << "pin count:" << mypage->GetPinCount() - 1 << std::endl;
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToTest(bpm->FetchPage(internal->ValueAt(i)), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}
/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;

    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
