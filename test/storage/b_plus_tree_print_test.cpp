/**
 * b_plus_tree_test.cpp
 *
 * This ia a local Debug test.
 * Feel free to change this file for your own testing purpose.
 *
 * THIS TEST WILL NOT BE RUN ON GRADESCOPE
 * THIS TEST WILL NOT BE RUN ON GRADESCOPE
 * THIS TEST WILL NOT BE RUN ON GRADESCOPE
 * THIS TEST WILL NOT BE RUN ON GRADESCOPE
 * THIS TEST WILL NOT BE RUN ON GRADESCOPE
 * THIS TEST WILL NOT BE RUN ON GRADESCOPE
 *
 */

#include <cstdio>
#include <iostream>
#include <random>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

std::string usageMessage() {
  std::string message =
      "Enter any of the following commands after the prompt > :\n"
      "\ti <k>  -- Insert <k> (int64_t) as both key and value).\n"
      "\tf <filename>  -- insert multiple keys from reading file.\n"
      "\tc <filename>  -- delete multiple keys from reading file.\n"
      "\td <k>  -- Delete key <k> and its associated value.\n"
      "\tg <filename>.dot  -- Output the tree in graph format to a dot file\n"
      "\tp -- Print the B+ tree.\n"
      "\tq -- Quit. (Or use Ctl-D.)\n"
      "\t? -- Print this help message.\n\n"
      "Please Enter Leaf node max size and Internal node max size:\n"
      "Example: 5 5\n>";
  return message;
}

// Remove 'DISABLED_' when you are ready
TEST(BptTreeTest, DISABLED_UnitTest) {
  int64_t key = 0;
  GenericKey<8> index_key;
  RID rid;
  std::string filename;
  char instruction;
  bool quit = false;
  int leaf_max_size;
  int internal_max_size;

  std::cout << usageMessage();
  std::cin >> leaf_max_size;
  std::cin >> internal_max_size;

  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(100, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, leaf_max_size, internal_max_size);
  // create transaction
  Transaction *transaction = new Transaction(0);
  while (!quit) {
    std::cout << "> ";
    std::cin >> instruction;
    switch (instruction) {
      case 'c':
        std::cin >> filename;
        tree.RemoveFromFile(filename, transaction);
        break;
      case 'd':
        std::cin >> key;
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
        break;
      case 'i':
        std::cin >> key;
        rid.Set(static_cast<int32_t>(key >> 32), static_cast<int>(key & 0xFFFFFFFF));
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
        break;
      case 'f':
        std::cin >> filename;
        tree.InsertFromFile(filename, transaction);
        break;
      case 'q':
        quit = true;
        break;
      case 'p':
        tree.Print(bpm);
        break;
      case 'g':
        std::cin >> filename;
        tree.Draw(bpm, filename);
        break;
      case '?':
        std::cout << usageMessage();
        break;
      default:
        std::cin.ignore(256, '\n');
        std::cout << usageMessage();
        break;
    }
  }
  bpm->UnpinPage(header_page->GetPageId(), true);
  delete key_schema;
  delete bpm;
  delete transaction;
  delete disk_manager;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 20
 * Description: Insert keys range from 1 to 5 repeatedly,
 * check whether insertion of repeated keys fail.
 * Then check whether the keys are distributed in separate
 * leaf nodes
 */
TEST(BPlusTreeTests, DISABLED_SplitTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  // tree.Print(bpm);
  // insert into repetitive key, all failed
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    EXPECT_EQ(false, tree.Insert(index_key, rid, transaction));
  }
  index_key.SetFromInteger(1);
  auto leaf_node =
      reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(tree.FindLeafPage(index_key));
  ASSERT_NE(nullptr, leaf_node);
  // LOG_INFO(" leaf node size %d\n", leaf_node->GetSize());
  // tree.Print(bpm);
  EXPECT_EQ(1, leaf_node->GetSize());
  EXPECT_EQ(2, leaf_node->GetMaxSize());

  // Check the next 4 pages
  for (int i = 0; i < 4; i++) {
    EXPECT_NE(INVALID_PAGE_ID, leaf_node->GetNextPageId());
    leaf_node = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(
        bpm->FetchPage(leaf_node->GetNextPageId()));
  }

  EXPECT_EQ(INVALID_PAGE_ID, leaf_node->GetNextPageId());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  delete key_schema;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 20
 * Description: Insert a set of keys range from 1 to 5 in the
 * increasing order. Check whether the key-value pair is valid
 * using GetValue
 */
TEST(BPlusTreeTests, DISABLED_InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 30
 * Description: Insert a set of keys range from 1 to 5 in
 * a reversed order. Check whether the key-value pair is valid
 * using GetValue
 */
TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 30
 * Description: Insert a set of keys range from 1 to 10000 in
 * a random order. Check whether the key-value pair is valid
 * using GetValue
 */
TEST(BPlusTreeTests, ScaleTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(30, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int64_t scale = 10000;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key < scale; key++) {
    keys.push_back(key);
  }

  // randomized the insertion order
  // RID用在叶子结点中，存在哪一页 哪个位置
  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);
  int count = 0;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    count++;
    // LOG_INFO("what happended!!! %d\n",count);
    tree.Insert(index_key, rid, transaction);
  }
  // LOG_INFO("what happended!!!");
  //  tree.Print(bpm);
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
