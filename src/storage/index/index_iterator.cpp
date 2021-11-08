/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(TreeLeafPage *leafPage, BufferPoolManager *bufferPoolManager, int offset) {
  this->bufferPoolManager = bufferPoolManager;
  this->offset = offset;
  this->leafPage = leafPage;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  page_id_t page_id = leafPage->GetPageId();
  bufferPoolManager->FetchPage(page_id)->RUnlatch();
  bufferPoolManager->UnpinPage(page_id, false);
  bufferPoolManager->UnpinPage(page_id, false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  page_id_t next_page_id = leafPage->GetNextPageId();
  return next_page_id == INVALID_PAGE_ID && offset == leafPage->GetSize() - 1;
  // throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  // 要用引用才行
  const MappingType &tmp = leafPage->GetItem(offset);
  return tmp;
  // throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  offset++;
  if (offset == leafPage->GetSize()) {
    page_id_t next_page_id = leafPage->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      return *this;
    }
    Page *page = bufferPoolManager->FetchPage(next_page_id);
    page->RLatch();
    bufferPoolManager->FetchPage(leafPage->GetPageId())->RUnlatch();
    bufferPoolManager->UnpinPage(leafPage->GetPageId(), false);
    bufferPoolManager->UnpinPage(leafPage->GetPageId(), false);
    TreeLeafPage *leafPage2 = reinterpret_cast<TreeLeafPage *>(page->GetData());
    leafPage = leafPage2;
    offset = 0;
  }
  return *this;
  // throw std::runtime_error("unimplemented");
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
