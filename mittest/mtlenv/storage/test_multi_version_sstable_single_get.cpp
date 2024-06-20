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
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/mock_access_service.h"
#include "storage/mock_ls_tablet_service.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace share::schema;
namespace storage
{

class TestMultiVersionSSTableSingleGet : public ObMultiVersionSSTableTest
{
public:
  TestMultiVersionSSTableSingleGet();
  virtual ~TestMultiVersionSSTableSingleGet() {}

  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_query_param(const ObVersionRange &version_range, const bool is_reverse_scan = false);
private:
  ObStoreCtx store_ctx_;
};

void TestMultiVersionSSTableSingleGet::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  // create tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));
}

void TestMultiVersionSSTableSingleGet::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
}

TestMultiVersionSSTableSingleGet::TestMultiVersionSSTableSingleGet()
  : ObMultiVersionSSTableTest("testmultiversionsingleget")
{
}

void TestMultiVersionSSTableSingleGet::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMultiVersionSSTableSingleGet::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
}

void TestMultiVersionSSTableSingleGet::prepare_query_param(
    const ObVersionRange &version_range,
    const bool is_reverse_scan)
{
  context_.reset();
  ObLSID ls_id(ls_id_);
  iter_param_.table_id_ = table_id_;
  iter_param_.tablet_id_ = tablet_id_;
  iter_param_.read_info_ = &full_read_info_;
  iter_param_.out_cols_project_ = nullptr;
  iter_param_.is_same_schema_column_ = true;
  iter_param_.has_virtual_columns_ = false;
  iter_param_.vectorized_enabled_ = false;
  ASSERT_EQ(OB_SUCCESS,
            store_ctx_.init_for_read(ls_id,
                                     iter_param_.tablet_id_,
                                     INT64_MAX, // query_expire_ts
                                     -1, // lock_timeout_us
                                     share::SCN::max_scn()));
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         false, /*is daily merge scan*/
                         false, /*is read multiple macro block*/
                         false, /*sys task scan, read one macro block in single io*/
                         false /*full row scan flag, obsoleted*/,
                         false,/*index back*/
                         false); /*query_stat*/
  query_flag.set_not_use_row_cache();
  query_flag.set_not_use_block_cache();
  ASSERT_EQ(OB_SUCCESS,
            context_.init(query_flag,
                          store_ctx_,
                          allocator_,
                          allocator_,
                          version_range));
  context_.limit_param_ = nullptr;
}

TEST_F(TestMultiVersionSSTableSingleGet, exist)
{
  ObTableHandleV2 handle;
  const int64_t schema_rowkey_cnt = 2;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint bigint  bigint   flag    multi_version_row_flag\n"
      "1        var1   -8      0     NOP       EXIST   C\n"
      "1        var1   -2      0      2        EXIST   L\n"
      "2        var2   -7      0      4        DELETE  CL\n"
      "3        var3  -28      0      7        EXIST   C\n"
      "3        var3  -25      0     NOP       EXIST   N\n"
      "3        var3  -23      0      7        EXIST   N\n";

  micro_data[1] =
      "bigint   var   bigint bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -18      0     8       EXIST   C\n"
      "3        var3  -15      0     11      EXIST   N\n"
      "3        var3  -13      0     9       EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint  flag    multi_version_row_flag\n"
      "3        var3  -8       0     8       EXIST   N\n"
      "3        var3  -5       0     10      EXIST   N\n"
      "3        var3  -3       0     9       EXIST   L\n";

  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle);

  const char *rowkeys =
      "bigint   var   flag\n"
      "1        var2  EXIST\n"
      "1        var1  EXIST\n"
      "2        var2  EXIST\n"
      "3        var3  EXIST\n"
      "4        var1  EXIST\n";
  ObMockIterator rowkey_iter;
  OK(rowkey_iter.from(rowkeys));

  blocksstable::ObSSTable *sstable;
  OK(handle.get_sstable(sstable));
  ObStoreRow *row = NULL;
  ObDatumRow datum_row;
  ObDatumRowkey rowkey;
  bool is_exist = false;
  bool is_found = false;

  datum_row.init(allocator_, full_read_info_.get_request_count());
  ObVersionRange version_range;
  version_range.snapshot_version_ = 8;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

  OK(rowkey_iter.get_row(0, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  OK(sstable->exist(iter_param_,
		            context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);

  OK(rowkey_iter.get_row(1, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = false;
  is_found = false;
  OK(sstable->exist(iter_param_,
		            context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(2, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = false;
  is_found = true;
  OK(sstable->exist(iter_param_,
                    context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(3, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = false;
  is_found = false;
  OK(sstable->exist(iter_param_,
                    context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(4, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  OK(sstable->exist(iter_param_,
                    context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);

  // read newest snapshot_version
  version_range.snapshot_version_ = 30;
  prepare_query_param(version_range);

  OK(rowkey_iter.get_row(0, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  OK(sstable->exist(iter_param_,
                    context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);

  OK(rowkey_iter.get_row(1, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  OK(sstable->exist(iter_param_,
                    context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(2, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  OK(sstable->exist(iter_param_,
                    context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(3, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  OK(sstable->exist(iter_param_,
                    context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(4, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  OK(sstable->exist(iter_param_,
                    context_,
                    rowkey,
                    is_exist,
                    is_found));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);
}

} // end namespace oceanbase
} // end namspace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_multi_version_sstable_single_get.log");
  OB_LOGGER.set_file_name("test_multi_version_sstable_single_get.log");
  STORAGE_LOG(INFO, "begin unittest: test_multi_version_sstable_single_get");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
