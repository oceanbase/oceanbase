/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "common/row/ob_row_store.h"
#include "lib/utility/ob_test_util.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class TestRowStore: public ::testing::Test
{
public:
  TestRowStore();
  virtual ~TestRowStore();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  TestRowStore(const TestRowStore &other);
  TestRowStore& operator=(const TestRowStore &other);
protected:
  void add_row(int32_t i, int64_t COL_NUM, ObRowStore &store, int expect_ret = OB_SUCCESS);
  void add_rows(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store);
  void verify_rows(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store);
  void add_rows_payload(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store);
  void verify_rows_payload(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store);
  void verify_rows_by_deep_copy(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store);
  // data members
};

TestRowStore::TestRowStore()
{
}

TestRowStore::~TestRowStore()
{
}

void TestRowStore::SetUp()
{
}

void TestRowStore::TearDown()
{
}

void TestRowStore::add_row(int32_t i, int64_t COL_NUM, ObRowStore &store, int expect_ret /*=OB_SUCCESS*/)
{
  // 1. fill data
  ObNewRow row;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  int ret = OB_SUCCESS;
  for (int j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
    row.cells_[j].set_int(i*COL_NUM+j);
  } // end for
  ASSERT_EQ(expect_ret, store.add_row(row));
  //_OB_LOG(INFO, "row=%s", S(row));
}

void TestRowStore::add_rows(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store)
{
  // 1. fill data
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < ROW_NUM; ++i) {
    add_row(i, COL_NUM, store);
  } // end for
  _OB_LOG(INFO, "store=%s", S(store));
  ASSERT_EQ(ROW_NUM, store.get_row_count());
}

void TestRowStore::verify_rows(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store)
{
  int ret = OB_SUCCESS;
  ObRowStore::Iterator it = store.begin();
  ObNewRow row2;
  ObObj objs[COL_NUM];
  ASSERT_EQ(OB_INVALID_ARGUMENT, it.get_next_row(row2));
  for (int round = 0; round < 3; ++round) {
    row2.count_ = COL_NUM;
    row2.cells_ = objs;
    for (int i = 0; OB_SUCC(ret) && i < ROW_NUM; ++i) {
      OK(it.get_next_row(row2));
      ASSERT_EQ(COL_NUM, row2.count_);
      for (int j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
        ASSERT_EQ(row2.cells_[j].get_int(), i*COL_NUM+j);;
      } // end for
    } // end for
    ASSERT_EQ(OB_ITER_END, it.get_next_row(row2));
    ASSERT_EQ(OB_ITER_END, it.get_next_row(row2));
    // test iterator reset
    it.reset();
  }
}

TEST_F(TestRowStore, serialization_2)
{
  ObRowStore store;
  ObRowStore s;
  const int64_t BUF_SIZE = store.get_serialize_size();
  char* buf = static_cast<char*>(ob_malloc(BUF_SIZE, ObModIds::TEST));
  ASSERT_TRUE(NULL != buf);
  int64_t pos = 0;
  OK(store.serialize(buf, BUF_SIZE, pos));
  pos = 0;
  OK(s.deserialize(buf, BUF_SIZE, pos));
}

TEST_F(TestRowStore, serialization_3)
{
  //the data_buffer_size of BlockInfo is 8157,between 8152 and 8196
  int ret = OB_SUCCESS;
  ObRowStore store;
  ObNewRow row;
  ObObj objs[2756];
  for (int64_t i = 0; i < 2756; ++i) {
    objs[i].set_int(i);
  }
  row.cells_ = const_cast<ObObj *>(objs);
  row.count_ = 2756;

  ret = store.add_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);

  //serialize
  int64_t serialize_size = store.get_serialize_size();
  char *buf = static_cast<char*>(ob_malloc(serialize_size, ObModIds::TEST));
  int64_t pos = 0;
  ret = store.serialize(buf, serialize_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  //deserialize
  ObRowStore store_des;
  int64_t pos_des = 0;
  ret = store_des.deserialize(buf, serialize_size, pos_des);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestRowStore, add_row)
{
  static const int64_t COL_NUM = 18;
  static const int64_t ROW_NUM = 10240;

  ObRowStore store1;
  ASSERT_EQ(0, store1.get_used_mem_size());
  ASSERT_EQ(0, store1.get_block_count());
  ASSERT_TRUE(!store1.is_read_only());

  for (int32_t round = 0; round < 8; ++round) {
    _OB_LOG(INFO, "round %d", round);
    ASSERT_TRUE(!store1.is_read_only());
    ASSERT_EQ(0, store1.get_data_size());
    ASSERT_TRUE(store1.is_empty());
    ASSERT_EQ(0, store1.get_row_count());
    ASSERT_EQ(0, store1.get_col_count());
    add_rows(COL_NUM, ROW_NUM, store1);

    ASSERT_TRUE(!store1.is_read_only());
    ASSERT_TRUE(!store1.is_empty());
    ASSERT_NE(0, store1.get_used_mem_size());
    ASSERT_NE(0, store1.get_block_count());
    ASSERT_EQ(COL_NUM, store1.get_col_count());
    ASSERT_EQ(ROW_NUM, store1.get_row_count());
    // 2. verify
    verify_rows(COL_NUM, ROW_NUM, store1);
    // reset & reuse
    if (0 == round % 2) {
      store1.reset();
    } else {
      store1.reuse();
    }
  }
}

void TestRowStore::add_rows_payload(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store)
{
  ASSERT_EQ(OB_SUCCESS, store.init_reserved_column_count(5));
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.add_reserved_column(-1));
  OK(store.add_reserved_column(1));
  OK(store.add_reserved_column(3));
  OK(store.add_reserved_column(5));
  OK(store.add_reserved_column(7));
  OK(store.add_reserved_column(9));
  ASSERT_EQ(5, store.get_reserved_column_count());
  // 1. fill data
  ObNewRow row;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  int ret = OB_SUCCESS;
  const ObRowStore::StoredRow *stored_row = NULL;
  for (int i = 0; OB_SUCC(ret) && i < ROW_NUM; ++i) {
    for (int j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
      row.cells_[j].set_int(i*COL_NUM+j);
    } // end for
    OK(store.add_row(row, stored_row, i, true));
    ASSERT_EQ(5, stored_row->reserved_cells_count_);
  } // end for
  _OB_LOG(INFO, "store=%s", S(store));
  ASSERT_EQ(ROW_NUM, store.get_row_count());
}

void TestRowStore::verify_rows_payload(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store)
{
  _OB_LOG(INFO, "verify rows payload");
  int ret = OB_SUCCESS;
  ObRowStore::Iterator it = store.begin();
  ObNewRow row2;
  ObObj objs[COL_NUM];
  ObString compact_row;
  ObRowStore::StoredRow *stored_row = NULL;
  ASSERT_EQ(OB_INVALID_ARGUMENT, it.get_next_row(row2));
  for (int round = 0; round < 3; ++round) {
    row2.count_ = COL_NUM;
    row2.cells_ = objs;
    for (int i = 0; OB_SUCC(ret) && i < ROW_NUM; ++i) {
      OK(it.get_next_row(row2, &compact_row, &stored_row));
      ASSERT_EQ(COL_NUM, row2.count_);
      ASSERT_NE(0, compact_row.length());
      ASSERT_EQ(5, stored_row->reserved_cells_count_);
      ASSERT_EQ(i*COL_NUM+1, stored_row->reserved_cells_[0].get_int());
      ASSERT_EQ(i*COL_NUM+3, stored_row->reserved_cells_[1].get_int());
      ASSERT_EQ(i*COL_NUM+5, stored_row->reserved_cells_[2].get_int());
      ASSERT_EQ(i*COL_NUM+7, stored_row->reserved_cells_[3].get_int());
      ASSERT_EQ(i*COL_NUM+9, stored_row->reserved_cells_[4].get_int());
      ASSERT_EQ(i, stored_row->payload_);
      // verify row data
      for (int j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
        ASSERT_EQ(row2.cells_[j].get_int(), i*COL_NUM+j);;
      } // end for
    } // end for
    ASSERT_EQ(OB_ITER_END, it.get_next_row(row2));
    ASSERT_EQ(OB_ITER_END, it.get_next_row(row2));
    // test iterator reset
    it.reset();
  }
}

TEST_F(TestRowStore, add_row_with_payload)
{
  static const int64_t COL_NUM = 18;
  static const int64_t ROW_NUM = 10;
  ObRowStore store1;
  add_rows_payload(COL_NUM, ROW_NUM, store1);
  store1.dump();
  verify_rows_payload(COL_NUM, ROW_NUM, store1);
  // clear rows
  store1.clear_rows();
  ASSERT_TRUE(store1.is_empty());
  ASSERT_EQ(5, store1.get_reserved_column_count());
}

TEST_F(TestRowStore, rollback_get_last_row)
{
  static const int64_t COL_NUM = 18;
  int ret = OB_SUCCESS;
  ObRowStore store1;
  const ObRowStore::StoredRow *stored_row = NULL;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, store1.get_last_row(stored_row));
  // add->get->rollback->add->get->rollback
  for (int round = 1; OB_SUCC(ret) && round < 1024; ++round) {
    _OB_LOG(INFO, "round=%d", round);
    for (int i = 0; i < round; ++i) {
      add_row(i, COL_NUM, store1);
      OK(store1.get_last_row(stored_row));
      ASSERT_TRUE(0 < stored_row->compact_row_size_);
      OK(store1.rollback_last_row());
      add_row(i, COL_NUM, store1);
      OK(store1.get_last_row(stored_row));
      ASSERT_TRUE(0 < stored_row->compact_row_size_);
    }
    ASSERT_EQ(round, store1.get_row_count());
    ASSERT_EQ(COL_NUM, store1.get_col_count());
    OK(store1.rollback_last_row());
    if (1 == round) {
      ASSERT_TRUE(store1.is_empty());
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, store1.get_last_row(stored_row));
    } else {
      OK(store1.get_last_row(stored_row));
      ASSERT_TRUE(0 < stored_row->compact_row_size_);
    }
    if (2 == round) {
      OK(store1.rollback_last_row());
      ASSERT_TRUE(store1.is_empty());
    } else {
      ASSERT_EQ(OB_NOT_SUPPORTED, store1.rollback_last_row());
    }
    store1.clear_rows();
  } // end for
}

TEST_F(TestRowStore, serialization_etc)
{
  static const int64_t COL_NUM = 18;
  static const int64_t ROW_NUM = 10240;
  ObRowStore store1;
  add_rows_payload(COL_NUM, ROW_NUM, store1);
  ASSERT_TRUE(!store1.is_read_only());
  // column consistency check
  add_row(0, COL_NUM-1, store1, OB_INVALID_ARGUMENT);
  // serialize
  const int64_t BUF_SIZE = store1.get_serialize_size();
  char* buf = static_cast<char*>(ob_malloc(BUF_SIZE, ObModIds::TEST));
  ASSERT_TRUE(NULL != buf);
  int64_t pos = 0;
  OK(store1.serialize(buf, BUF_SIZE, pos));
  // deserialize
  ObRowStore store2;
  int64_t len = pos;
  pos = 0;
  OK(store2.deserialize(buf, len, pos));
  verify_rows_payload(COL_NUM, ROW_NUM, store2);
  // get_last_row
  const ObRowStore::StoredRow *stored_row = NULL;
  OK(store2.get_last_row(stored_row));
  ASSERT_TRUE(0 < stored_row->compact_row_size_);
  // read only
  ASSERT_TRUE(!store2.is_read_only());
  //add_row(0, COL_NUM, store2, OB_ERR_READ_ONLY);
  ob_free(buf);
}

TEST_F(TestRowStore, KVCacheAPIs)
{
  static const int64_t COL_NUM = 18;
  static const int64_t ROW_NUM = 10240;
  ObRowStore store1;
  add_rows_payload(COL_NUM, ROW_NUM, store1);
  static const int64_t BUF_SIZE = 1024*1024*10;
  char* buf = static_cast<char*>(ob_malloc(BUF_SIZE, ObModIds::TEST));
  ASSERT_TRUE(NULL != buf);
  // clone
  _OB_LOG(INFO, "meta_size=%d copy_size=%d", store1.get_meta_size(), store1.get_copy_size());
  ObRowStore *store2 = store1.clone(buf, BUF_SIZE);
  ASSERT_TRUE(NULL == store2);
  store2 = store1.clone(NULL, BUF_SIZE);
  ASSERT_TRUE(NULL == store2);

  store1.reset();
  add_rows(COL_NUM, ROW_NUM, store1);
  ObRowStore *store3 = store1.clone(buf, BUF_SIZE);
  ASSERT_TRUE(NULL != store3);
  ASSERT_TRUE(store3->is_read_only());
  verify_rows(COL_NUM, ROW_NUM, *store3);
  ASSERT_EQ(store1.get_row_count(), store3->get_row_count());
  ASSERT_EQ(store1.get_col_count(), store3->get_col_count());
  ASSERT_EQ(store1.get_data_size(), store3->get_data_size());
  ASSERT_EQ(store1.get_label(), store3->get_label());
  ASSERT_EQ(store1.get_block_size(), store3->get_block_size());
  ASSERT_EQ(store1.get_copy_size(), store3->get_copy_size());
  ASSERT_EQ(store1.get_meta_size(), store3->get_meta_size());
  ASSERT_EQ(store1.get_reserved_column_count(), store3->get_reserved_column_count());
  ASSERT_TRUE(store1.get_used_mem_size() >= store3->get_used_mem_size());

  ob_free(buf);
}

TEST_F(TestRowStore, empty_store)
{
  ObRowStore store1;
  ASSERT_TRUE(store1.is_empty());
  ObRowStore::Iterator it;
  ObNewRow row;
  ObObj objs[3];
  row.cells_ = objs;
  row.count_ = 3;
  ASSERT_EQ(OB_ERR_UNEXPECTED, it.get_next_row(row));
  it = store1.begin();
  ASSERT_EQ(OB_ITER_END, it.get_next_row(row));
}

TEST_F(TestRowStore, mem_block_management)
{
  ObRowStore store1;
  ASSERT_EQ(OB_MALLOC_NORMAL_BLOCK_SIZE, store1.get_block_size());

  ObNewRow row;
  ObObj objs[1];
  row.cells_ = objs;
  row.count_ = 1;
  objs[0].set_int(100);
  OK(store1.add_row(row));

  static const int64_t BUF_1K = 1024;  // 1K
  char* buf1k = (char*)ob_malloc(BUF_1K, ObModIds::TEST);
  ObString str1k(BUF_1K, BUF_1K, buf1k);
  objs[0].set_varchar(str1k);
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < 128; ++i) {
    OK(store1.add_row(row));
  } // end for

  // 256K buffer
  static const int64_t BUF_256K = 256*1024L;  // 256K
  char* buf256k = (char*)ob_malloc(BUF_256K, ObModIds::TEST);
  ObString str256k(BUF_256K, BUF_256K, buf256k);
  objs[0].set_varchar(str256k);
  OK(store1.add_row(row));

  // 2M buffer
  static const int64_t BUF_2M = 2*1024*1024;  // 2M
  char* buf2m = (char*)ob_malloc(BUF_2M, ObModIds::TEST);
  ObString str2m(BUF_2M, BUF_2M, buf2m);
  objs[0].set_varchar(str2m);
  ASSERT_EQ(OB_SIZE_OVERFLOW, store1.add_row(row));

  // teardown
  ob_free(buf1k);
  ob_free(buf256k);
  ob_free(buf2m);
}

void TestRowStore::verify_rows_by_deep_copy(int64_t COL_NUM, int64_t ROW_NUM, ObRowStore &store)
{
  int ret = OB_SUCCESS;
  ObRowStore::Iterator it = store.begin();
  ObNewRow *row2 = NULL;
  for (int round = 0; round < 3; ++round) {
    for (int i = 0; OB_SUCC(ret) && i < ROW_NUM; ++i) {
      OK(it.get_next_row(row2, NULL));
      ASSERT_EQ(COL_NUM, row2->count_);
      for (int j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
        ASSERT_EQ(row2->cells_[j].get_int(), i*COL_NUM+j);;
      } // end for
    } // end for
    ASSERT_EQ(OB_ITER_END, it.get_next_row(row2, NULL));
    ASSERT_EQ(OB_ITER_END, it.get_next_row(row2, NULL));
    // test iterator reset
    it.reset();
  }
}

TEST_F(TestRowStore, row_store_by_deep_copy)
{
  static const int64_t COL_NUM = 18;
  static const int64_t ROW_NUM = 10240;

  ObRowStore store1(ObModIds::TEST,
                    OB_SERVER_TENANT_ID,
                    false);
  ASSERT_EQ(0, store1.get_used_mem_size());
  ASSERT_EQ(0, store1.get_block_count());
  ASSERT_TRUE(!store1.is_read_only());

  for (int32_t round = 0; round < 8; ++round) {
    _OB_LOG(INFO, "round %d", round);
    ASSERT_TRUE(!store1.is_read_only());
    ASSERT_EQ(0, store1.get_data_size());
    ASSERT_TRUE(store1.is_empty());
    ASSERT_EQ(0, store1.get_row_count());
    ASSERT_EQ(0, store1.get_col_count());
    add_rows(COL_NUM, ROW_NUM, store1);

    ASSERT_TRUE(!store1.is_read_only());
    ASSERT_TRUE(!store1.is_empty());
    ASSERT_NE(0, store1.get_used_mem_size());
    ASSERT_NE(0, store1.get_block_count());
    ASSERT_EQ(COL_NUM, store1.get_col_count());
    ASSERT_EQ(ROW_NUM, store1.get_row_count());
    // 2. verify
    verify_rows_by_deep_copy(COL_NUM, ROW_NUM, store1);
    // reset & reuse
    if (0 == round % 2) {
      store1.reset();
    } else {
      store1.reuse();
    }
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO", "WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
