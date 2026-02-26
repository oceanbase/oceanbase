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
#define UNITTEST
#include "storage/access/ob_index_skip_scanner.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/test_tablet_helper.h"
#include "test_merge_basic.h"
#define DEFAULT_LOG_LEVEL "DEBUG"

namespace oceanbase
{

using namespace common;
using namespace storage;
using namespace share::schema;

namespace storage
{
class TestSkipMicro : public TestMergeBasic
{
public:
  TestSkipMicro();
  virtual ~TestSkipMicro() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_scan_param(const ObVersionRange &version_range);
  ObDatumRow scan_row1_;
  ObDatumRow scan_row2_;
  ObDatumRange scan_range_;
  ObDatumRow skip_row1_;
  ObDatumRow skip_row2_;
  ObDatumRange skip_range_;
  ObArenaAllocator range_alloc_;
  ObTableReadInfo read_info_;
  ObStoreCtx store_ctx_;
  ObFixedArray<int32_t, ObIAllocator> output_cols_project_;
  ObFixedArray<share::schema::ObColumnParam*, ObIAllocator> cols_param_;
};

TestSkipMicro::TestSkipMicro()
  : TestMergeBasic("test_skip_scan_micro")
{}

void TestSkipMicro::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestSkipMicro::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
}


void TestSkipMicro::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  MTL(ObTenantTabletScheduler*)->resume_major_merge();

  // create tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));

}

void TestSkipMicro::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
}

TEST_F(TestSkipMicro, test_reverse_scan1)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  const char *micro_data[4];

  micro_data[0] =
    "bigint  bigint  varchar  bigint  bigint  bigint  flag  flag_type  multi_version_row_flag\n"
    "1004  1  6.12.234.22  56000  -1767480554103992000  -9223372036854775807  INSERT  NORMAL  CL\n"
    "1004  1  6.12.234.22  56500  -1767480554103992000  -9223372036854775807  DELETE  NORMAL  SCF\n";

  micro_data[1] =
    "bigint  bigint  varchar  bigint  bigint  bigint  flag  flag_type  multi_version_row_flag\n"
    "1004  1  6.12.234.22  56500  -1767480486958909000  0  INSERT  NORMAL  C\n"
    "1004  1  6.12.234.22  56500  -1767480486492908000  0  INSERT  NORMAL  CL\n";

  int schema_rowkey_cnt = 4;
  int64_t snapshot_version = 1767480436300312000;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1767480436300312000);
  scn_range.end_scn_.convert_for_tx(1767480554103992003);
  ObMultiVersionSSTableTest::prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle, ObITable::MINI_SSTABLE);
  ObITable *table = handle.get_table();

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1767480554232610000;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  prepare_scan_param(trans_version_range);

  scan_row1_.reset();
  scan_row2_.reset();
  scan_range_.reset();
  skip_row1_.reset();
  skip_row2_.reset();
  skip_range_.reset();

  ASSERT_EQ(OB_SUCCESS, scan_row1_.init(range_alloc_, schema_rowkey_cnt));
  ASSERT_EQ(OB_SUCCESS, scan_row2_.init(range_alloc_, schema_rowkey_cnt));
  scan_row1_.storage_datums_[0].set_int(1004);
  scan_row2_.storage_datums_[0].set_int(1004);
  for (int64_t i = 1; i < schema_rowkey_cnt; ++i) {
    scan_row1_.storage_datums_[i].set_min();
    scan_row2_.storage_datums_[i].set_max();
  }
  scan_range_.start_key_.assign(scan_row1_.storage_datums_, schema_rowkey_cnt);
  scan_range_.end_key_.assign(scan_row2_.storage_datums_, schema_rowkey_cnt);
  scan_range_.set_left_open(); scan_range_.set_right_open();

  ASSERT_EQ(OB_SUCCESS, skip_row1_.init(range_alloc_, 2));
  ASSERT_EQ(OB_SUCCESS, skip_row2_.init(range_alloc_, 2));
  ObString ip_str("6.12.234.22");
  skip_row1_.storage_datums_[0].set_string(ip_str);
  skip_row1_.storage_datums_[1].set_int(40000);
  skip_row2_.storage_datums_[0].set_string(ip_str);
  skip_row2_.storage_datums_[1].set_int(40000);
  skip_range_.start_key_.assign(skip_row1_.storage_datums_, 2);
  skip_range_.end_key_.assign(skip_row2_.storage_datums_, 2);
  skip_range_.set_left_closed(); skip_range_.set_right_closed();

  iter_param_.set_skip_scan_range(skip_range_);
  iter_param_.ss_rowkey_prefix_cnt_ = 2;

  ObSSTableRowScanner<> skip_scanner;
  ASSERT_EQ(OB_SUCCESS, skip_scanner.init(iter_param_, context_, table, &scan_range_));
  const ObDatumRow *skip_prow = nullptr;
  int64_t count = 0;
  ret = skip_scanner.inner_get_next_row(skip_prow);
  ASSERT_EQ(OB_ITER_END, ret);
}

void TestSkipMicro::prepare_scan_param(const ObVersionRange &version_range)
{
  context_.reset();
  read_info_.reset();
  output_cols_project_.reset();
  cols_param_.reset();

  ObTabletHandle tablet_handle;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  int64_t schema_column_count = full_read_info_.get_schema_column_count();
  int64_t schema_rowkey_count = full_read_info_.get_schema_rowkey_count();
  output_cols_project_.set_allocator(&range_alloc_);
  output_cols_project_.init(schema_column_count);
  for (int64_t i = 0; i < schema_column_count; i++) {
    output_cols_project_.push_back(i);
  }

  ObSEArray<ObColDesc, 8> tmp_col_descs;
  ObSEArray<int32_t, 8> tmp_cg_idxs;
  const common::ObIArray<ObColDesc> &cols_desc = full_read_info_.get_columns_desc();
  for (int64_t i = 0; i < schema_column_count; i++) {
    if (i < schema_rowkey_count) {
      tmp_col_descs.push_back(cols_desc.at(i));
    } else {
      tmp_col_descs.push_back(cols_desc.at(i + 2));
    }
  }

  cols_param_.set_allocator(&range_alloc_);
  cols_param_.init(tmp_col_descs.count());
  for (int64_t i = 0; i < tmp_col_descs.count(); ++i) {
    void *col_param_buf = range_alloc_.alloc(sizeof(ObColumnParam));
    ObColumnParam *col_param = new(col_param_buf) ObColumnParam(range_alloc_);
    col_param->set_meta_type(tmp_col_descs.at(i).col_type_);
    col_param->set_nullable_for_write(true);
    col_param->set_column_id(common::OB_APP_MIN_COLUMN_ID + i);
    cols_param_.push_back(col_param);
    tmp_cg_idxs.push_back(i + 1);
  }

  ASSERT_EQ(OB_SUCCESS,
            read_info_.init(range_alloc_,
                            full_read_info_.get_schema_column_count(),
                            full_read_info_.get_schema_rowkey_count(),
                            lib::is_oracle_mode(),
                            tmp_col_descs,
                            nullptr/*storage_cols_index*/,
                            &cols_param_,
                            &tmp_cg_idxs));
  iter_param_.read_info_ = &read_info_;
  iter_param_.rowkey_read_info_ = &full_read_info_;

  iter_param_.table_id_ = table_id_;
  iter_param_.tablet_id_ = tablet_id_;
  iter_param_.is_same_schema_column_ = true;
  iter_param_.has_virtual_columns_ = false;
  iter_param_.vectorized_enabled_ = true;
  iter_param_.has_lob_column_out_ = false;
  iter_param_.pd_storage_flag_.pd_blockscan_ = true;
  iter_param_.pd_storage_flag_.pd_filter_ = true;
  iter_param_.is_delete_insert_ = false;

  iter_param_.out_cols_project_ = &output_cols_project_;
  ASSERT_EQ(OB_SUCCESS,
            store_ctx_.init_for_read(ls_id,
                                    tablet_id,
                                    INT64_MAX, // query_expire_ts
                                    -1, // lock_timeout_us
                                    share::SCN::max_scn()));

  ObQueryFlag query_flag(ObQueryFlag::Forward,
                        false, // daily_merge
                        false, // optimize
                        false, // sys scan
                        false, // full_row
                        false, // index_back
                        false, // query_stat
                        ObQueryFlag::MysqlMode, // sql_mode
                        true // read_latest
                        );
  query_flag.set_not_use_row_cache();
  query_flag.set_not_use_block_cache();
  //query_flag.multi_version_minor_merge_ = true;
  ASSERT_EQ(OB_SUCCESS,
            context_.init(query_flag,
                          store_ctx_,
                          range_alloc_,
                          range_alloc_,
                          version_range));
  context_.ls_id_ = ls_id_;
  context_.limit_param_ = nullptr;
  context_.alloc_skip_scan_factory();
  context_.query_flag_.scan_order_ = ObQueryFlag::ScanOrder::Reverse;
  context_.is_inited_ = true;
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_index_skip_micro.log*");
  OB_LOGGER.set_file_name("test_index_skip_micro.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level(DEFAULT_LOG_LEVEL);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}