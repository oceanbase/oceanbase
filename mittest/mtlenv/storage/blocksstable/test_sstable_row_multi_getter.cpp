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

#include <gtest/gtest.h>
#define private public
#define protected public

#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/access/ob_sstable_row_multi_getter.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestSSTableRowMultiGetter : public TestIndexBlockDataPrepare
{
public:
  TestSSTableRowMultiGetter();
  virtual ~TestSSTableRowMultiGetter();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
public:
  void test_one_case(
      const ObIArray<int64_t> &seeds,
      const int64_t hit_mode,
      const bool is_reverse_scan);
  void test_border(const bool is_reverse_scan);
  void test_normal(const bool is_reverse_scan);

protected:
  static const int64_t TEST_MULTI_GET_CNT = 2000;
  enum CacheHitMode
  {
    HIT_ALL = 0,
    HIT_NONE,
    HIT_PART,
    HIT_MAX,
  };
private:
  ObArenaAllocator allocator_;
  ObDatumRow start_row_;
  ObDatumRow check_row_;
};

TestSSTableRowMultiGetter::TestSSTableRowMultiGetter()
  : TestIndexBlockDataPrepare("Test sstable row multi getter")
{
}

TestSSTableRowMultiGetter::~TestSSTableRowMultiGetter()
{
}

void TestSSTableRowMultiGetter::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestSSTableRowMultiGetter::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestSSTableRowMultiGetter::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, start_row_.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, check_row_.init(allocator_, TEST_COLUMN_CNT));
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestSSTableRowMultiGetter::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestSSTableRowMultiGetter::test_one_case(
    const ObIArray<int64_t> &seeds,
    const int64_t hit_mode,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  UNUSED(is_reverse_scan);
  ObArray<ObDatumRowkey> rowkeys;
  ObDatumRowkey rowkey;
  ObDatumRow row;
  const ObDatumRow *prow = NULL;
  const ObDatumRow *kv_prow = NULL;
  ObSSTableRowMultiGetter getter;
  ObSSTableRowMultiGetter kv_getter;

  // prepare rowkeys
  ObDatumRowkey mget_rowkeys[TEST_MULTI_GET_CNT];
  for (int64_t i = 0; i < seeds.count(); ++i) {
    ObDatumRowkey tmp_rowkey;
    ret = row_generate_.get_next_row(seeds.at(i), start_row_);
    tmp_rowkey.assign(start_row_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_rowkeys[i], allocator_));
    ASSERT_EQ(OB_SUCCESS, rowkeys.push_back(mget_rowkeys[i]));
  }

  if (hit_mode == HIT_PART) {
    ObArray<ObDatumRowkey> part_rowkeys;
    ObArray<ObDatumRowkey> tmp_rowkeys;

    ret = tmp_rowkeys.assign(rowkeys);
    ASSERT_EQ(OB_SUCCESS, ret);
    std::random_shuffle(tmp_rowkeys.begin(), tmp_rowkeys.end());

    part_rowkeys.reset();
    for (int64_t i = 0; i < tmp_rowkeys.count() / 3; ++i) {
      ret = part_rowkeys.push_back(tmp_rowkeys.at(i));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    if (part_rowkeys.count() > 0) {
      ASSERT_EQ(OB_SUCCESS, getter.inner_open(iter_param_, context_, &sstable_, &part_rowkeys));
      ASSERT_EQ(OB_SUCCESS, kv_getter.inner_open(iter_param_, context_, &ddl_kv_, &part_rowkeys));
      for (int64_t i = 0; i < part_rowkeys.count(); ++i) {
        ret = getter.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = kv_getter.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ret = getter.inner_get_next_row(prow);
      ASSERT_EQ(OB_ITER_END, ret);
      ret = kv_getter.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_ITER_END, ret);
    }
    getter.reuse();
    kv_getter.reuse();
  }

  // in io
  ASSERT_EQ(OB_SUCCESS, getter.inner_open(iter_param_, context_, &sstable_, &rowkeys));
  ASSERT_EQ(OB_SUCCESS, kv_getter.inner_open(iter_param_, context_, &ddl_kv_, &rowkeys));
  for (int64_t i = 0; i < seeds.count(); ++i) {
    ret = getter.inner_get_next_row(prow);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = kv_getter.inner_get_next_row(kv_prow);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (seeds.at(i) >= row_cnt_) {
      ASSERT_TRUE(prow->row_flag_.is_not_exist());
      ASSERT_TRUE(kv_prow->row_flag_.is_not_exist());
    } else {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seeds.at(i), check_row_));
      ASSERT_EQ(OB_SUCCESS, (const_cast<ObDatumRow *>(prow))->prepare_new_row(schema_cols_));
      ASSERT_EQ(OB_SUCCESS, (const_cast<ObDatumRow *>(kv_prow))->prepare_new_row(schema_cols_));
      ASSERT_TRUE(check_row_ == *prow);
      ASSERT_TRUE(check_row_ == *kv_prow);
    }
  }
  ret = getter.inner_get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  ret = kv_getter.inner_get_next_row(kv_prow);
  ASSERT_EQ(OB_ITER_END, ret);
  getter.reuse();
  kv_getter.reuse();

  // in cache
  if (hit_mode == HIT_ALL) {
    ASSERT_EQ(OB_SUCCESS, getter.inner_open(iter_param_, context_, &sstable_, &rowkeys));
    ASSERT_EQ(OB_SUCCESS, kv_getter.inner_open(iter_param_, context_, &ddl_kv_, &rowkeys));
    for (int64_t i = 0; i < seeds.count(); ++i) {
      ret = getter.inner_get_next_row(prow);
      ret = kv_getter.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (seeds.at(i) >= row_cnt_) {
        ASSERT_TRUE(prow->row_flag_.is_not_exist());
        ASSERT_TRUE(kv_prow->row_flag_.is_not_exist());
      } else {
        ASSERT_EQ(OB_SUCCESS, (const_cast<ObDatumRow *>(prow))->prepare_new_row(schema_cols_));
        ASSERT_EQ(OB_SUCCESS, (const_cast<ObDatumRow *>(kv_prow))->prepare_new_row(schema_cols_));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seeds.at(i), check_row_));
        ASSERT_TRUE(check_row_ == *prow);
        ASSERT_TRUE(check_row_ == *kv_prow);
      }
    }
    ret = getter.inner_get_next_row(prow);
    ASSERT_EQ(OB_ITER_END, ret);
    ret = kv_getter.inner_get_next_row(kv_prow);
    ASSERT_EQ(OB_ITER_END, ret);
  }
  getter.reuse();
  kv_getter.reuse();
}

void TestSSTableRowMultiGetter::test_border(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArray<ObStoreRowkey> rowkeys;
  ObStoreRowkey rowkey;
  ObStoreRow row;
  ObArray<int64_t> seeds;
  ObSSTableRowMultiGetter getter;

  // prepare query param
  prepare_query_param(is_reverse_scan);

  /*
  // empty rowkey
  rowkeys.reuse();
  ret = rowkeys.push_back(rowkey);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, getter.inner_open(iter_param_, context_, &sstable_, &rowkeys));

  // uinited sstable
  ObSSTable sstable;
  rowkeys.reuse();
  ret = rowkeys.push_back(rowkey);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = rowkeys.push_back(rowkey);
  ASSERT_EQ(OB_SUCCESS, getter.inner_open(iter_param_, context_, &sstable_, &rowkeys));
  */

  // the row of sstable
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, false);
  }

  // last row of sstable
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, false);
  }

  // single row not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, false);
  }

  // TEST_MULTI_GET_CNT row with the same rowkey
  seeds.reset();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(row_cnt_ / 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableRowMultiGetter::test_normal(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;

  // prepare query param
  prepare_query_param(is_reverse_scan);

  // 2 rows exist test
  seeds.reuse();
  for (int64_t i = 0; i < 2; ++i) {
    ret = seeds.push_back(i * 11 + 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // 10 rows exist test
  seeds.reuse();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i * 11 + 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // TEST_MULTI_GET_CNT rows exist test
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // single row exist test
  seeds.reuse();
  ret = seeds.push_back(3);
  ASSERT_EQ(OB_SUCCESS, ret);

  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i + (i % 2 ? row_cnt_ : 0));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // 2 row not exist test
  seeds.reuse();
  for (int64_t i = 0; i < 2; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // 10 rows not exist test
  seeds.reuse();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }

  // TEST_MULTI_GET_CNT rows not exist test
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, i, is_reverse_scan);
  }
  destroy_query_param();
}

TEST_F(TestSSTableRowMultiGetter, test_border)
{
  bool is_reverse_scan = false;
  test_border(is_reverse_scan);
  is_reverse_scan = true;
  test_border(is_reverse_scan);
}

TEST_F(TestSSTableRowMultiGetter, test_normal)
{
  bool is_reverse_scan = false;
  test_normal(is_reverse_scan);
  is_reverse_scan = true;
  test_normal(is_reverse_scan);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_sstable_row_multi_getter.log*");
  OB_LOGGER.set_file_name("test_sstable_row_multi_getter.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
