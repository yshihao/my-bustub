#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**return its metad
   * Create a new table and ata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    // TableMetadata tableMetadata
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");
    table_oid_t table_oid = next_table_oid_++;
    names_.insert({table_name, table_oid});
    // 创建一张实体表
    std::unique_ptr<TableHeap> tableHeap(new TableHeap(bpm_, lock_manager_, log_manager_, txn));
    std::unique_ptr<TableMetadata> p(new TableMetadata(schema, table_name, std::move(tableHeap), table_oid));
    tables_.insert({table_oid, std::move(p)});
    return &(*tables_[table_oid]);
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) {
    if (names_.find(table_name) == names_.end()) {
      throw std::out_of_range("out of range");
    }
    table_oid_t table_identifier = names_[table_name];
    return GetTable(table_identifier);
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    auto p = tables_.find(table_oid);
    if (p == tables_.end()) {
      throw std::out_of_range("out of range");
    }
    // 只能有一个指针 指向他
    return &(*(p->second));
    // return nullptr;
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    if (names_.find(table_name) == names_.end()) {
      throw std::out_of_range("out of range");
    }
    auto indexToId = index_names_.find(table_name);
    index_oid_t index_id = next_index_oid_++;
    if (indexToId == index_names_.end()) {
      std::unordered_map<std::string, index_oid_t> tmp({{index_name, index_id}});
      index_names_.insert({table_name, tmp});
    } else {
      std::unordered_map<std::string, index_oid_t> nameToId = indexToId->second;
      nameToId.insert({index_name, index_id});
    }
    // 为何传的是schema
    // std::shared_ptr<IndexMetadata> indexMeta(new IndexMetadata(index_name, table_name, &schema, key_attrs));
    auto indexMeta = new IndexMetadata(index_name, table_name, &schema, key_attrs);
    // 基类 指向子类 利用了多态性质
    std::unique_ptr<Index> bplusTreeIndex(new BPlusTreeIndex<KeyType, ValueType, KeyComparator>(indexMeta, bpm_));
    // auto tableMetaP = tables_[names_[table_name]];
    auto tableMetaP = GetTable(table_name);
    for (auto tableIderator = tableMetaP->table_->Begin(txn); tableIderator != tableMetaP->table_->End();
         tableIderator++) {
      Tuple tuple_ = *tableIderator;
      Tuple result = tuple_.KeyFromTuple(schema, key_schema, key_attrs);
      bplusTreeIndex->InsertEntry(result, tuple_.GetRid(), txn);
    }

    std::unique_ptr<IndexInfo> p(
        new IndexInfo(key_schema, index_name, std::move(bplusTreeIndex), index_id, table_name, keysize));
    indexes_.insert({index_id, std::move(p)});
    return &(*indexes_[index_id]);
  }

  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    auto tableNameToId = index_names_.find(table_name);
    if (tableNameToId == index_names_.end()) {
      throw std::out_of_range("out of range");
    }
    auto indexNameToId = tableNameToId->second;
    if (indexNameToId.find(index_name) == indexNameToId.end()) {
      throw std::out_of_range("out of range");
    }
    return GetIndex(indexNameToId[index_name]);
  }

  IndexInfo *GetIndex(index_oid_t index_oid) {
    if (indexes_.find(index_oid) == indexes_.end()) {
      throw std::out_of_range("out of range");
    }
    return &(*indexes_[index_oid]);
  }

  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    std::vector<IndexInfo *> result;
    for (auto &nameToId : index_names_[table_name]) {
      result.push_back(&(*indexes_[nameToId.second]));
    }
    return result;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
