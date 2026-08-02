// owner: dengzhi.ldz
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define private public
#define protected public
#include "storage/test_tablet_helper.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace share::schema;
namespace storage
{

class TestMultiVersionSSTableSingleGet : public ObMultiVersionSSTableTest, public ::testing::WithParamInterface<bool>
{
public:
  TestMultiVersionSSTableSingleGet();
  virtual ~TestMultiVersionSSTableSingleGet() {}

  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_query_param(const ObVersionRange &version_range, const bool is_reverse_scan = false);
  void get_and_check_row(ObSSTable *sstable,
                         const int64_t rowkey_value,
                         const bool expected_found,
                         const int64_t expected_value);
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
  // toggle row store type by parameter: false -> FLAT_ROW_STORE, true -> CS_ENCODING_ROW_STORE
  const bool use_cs_encoding = GetParam();
  row_store_type_ = use_cs_encoding ? CS_ENCODING_ROW_STORE : FLAT_ROW_STORE;
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
  store_ctx_.reset();
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

void TestMultiVersionSSTableSingleGet::get_and_check_row(
    ObSSTable *sstable,
    const int64_t rowkey_value,
    const bool expected_found,
    const int64_t expected_value)
{
  ObDatumRow query_row;
  ASSERT_EQ(OB_SUCCESS, query_row.init(allocator_, 1));
  query_row.storage_datums_[0].set_int(rowkey_value);
  ObDatumRowkey rowkey;
  ASSERT_EQ(OB_SUCCESS, rowkey.assign(query_row.storage_datums_, 1));

  ObStoreRowIterator *row_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, sstable->get(iter_param_, context_, rowkey, row_iter));
  ASSERT_NE(nullptr, row_iter);

  const ObDatumRow *row = nullptr;
  const int ret = row_iter->get_next_row(row);
  ASSERT_TRUE(OB_SUCCESS == ret || OB_ITER_END == ret);
  const bool found = OB_SUCCESS == ret && nullptr != row && !row->row_flag_.is_not_exist();
  EXPECT_EQ(expected_found, found)
      << "rowkey=" << rowkey_value
      << ", iter_uncommitted_row=" << context_.query_flag_.iter_uncommitted_row();
  if (found) {
    ASSERT_GT(row->count_, 3);
    EXPECT_EQ(expected_value, row->storage_datums_[3].get_int());
  }

  row_iter->~ObStoreRowIterator();
  context_.stmt_allocator_->free(row_iter);
}

TEST_P(TestMultiVersionSSTableSingleGet, exist)
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
  ObStoreRowIterator *store_row_iter = nullptr;
  const ObDatumRow *get_datum_row = nullptr;
  int ret = OB_SUCCESS;

  datum_row.init(allocator_, full_read_info_.get_request_count());
  ObVersionRange version_range;
  version_range.snapshot_version_ = 8;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  prepare_query_param(version_range);

#define GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found) \
  store_row_iter = nullptr; \
  get_datum_row = nullptr; \
  OK(sstable->get(iter_param_, context_, rowkey, store_row_iter)); \
  ret = store_row_iter->get_next_row(get_datum_row); \
  ASSERT_TRUE(OB_SUCCESS == ret || OB_ITER_END == ret); \
  is_found = is_exist = false; \
  if (OB_ITER_END == ret || get_datum_row->row_flag_.is_not_exist()) { \
  } else { \
    is_found = true; \
    is_exist = !get_datum_row->row_flag_.is_delete(); \
  } \
  store_row_iter->~ObStoreRowIterator(); \
  context_.stmt_allocator_->free(store_row_iter); \

  OK(rowkey_iter.get_row(0, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);

  OK(rowkey_iter.get_row(1, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = false;
  is_found = false;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(2, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = false;
  is_found = true;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(3, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = false;
  is_found = false;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(4, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
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
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);

  OK(rowkey_iter.get_row(1, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(2, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(3, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(true, is_found);

  OK(rowkey_iter.get_row(4, row));
  OK(datum_row.from_store_row(*row));
  ASSERT_TRUE(NULL != row);
  rowkey.assign(datum_row.storage_datums_, schema_rowkey_cnt);
  is_exist = true;
  is_found = true;
  GET_AND_CHECK_EXIST_FROM_SSTABLE(is_exist, is_found);
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(false, is_found);
}

TEST_P(TestMultiVersionSSTableSingleGet, iter_uncommitted_honors_base_and_bypasses_snapshot)
{
  ObTableHandleV2 handle;
  const int64_t schema_rowkey_cnt = 1;
  const char *micro_data[1];
  const char *flat_micro_data =
      "bigint   bigint  bigint  bigint  flag   multi_version_row_flag  trans_id\n"
      "1        -5      0       50      EXIST  CL                      trans_id_0\n"
      "2        -12     0       120     EXIST  CL                      trans_id_0\n"
      "3        MIN     -1      999     EXIST  ULF                     trans_id_1\n"
      "4        -30     0       300     EXIST  CL                      trans_id_0\n";
  const char *cs_micro_data =
      "bigint   bigint  bigint  bigint  flag   multi_version_row_flag  trans_id\n"
      "1        -5      0       50      EXIST  CL                      trans_id_0\n"
      "2        -12     0       120     EXIST  CL                      trans_id_0\n"
      "3        MIN     -1      999     EXIST  ULF                     trans_id_2\n"
      "4        -30     0       300     EXIST  CL                      trans_id_0\n";
  micro_data[0] = GetParam() ? cs_micro_data : flat_micro_data;
  const int64_t uncommitted_tx_id = GetParam() ? 2 : 1;

  const int64_t table_snapshot_version = 40;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(table_snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, table_snapshot_version);
  reset_writer(table_snapshot_version);

  // The row on key 3 sets the table-level uncommitted marker.  That marker routes
  // point-get to the multi-version getter, which must enforce the same boundaries
  // both before and after the transaction state is resolved.
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle, ObITable::MINOR_SSTABLE);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS,
            MTL(ObLSService*)->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::STORAGE_MOD));
  ObTxTableGuard tx_table_guard;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_table_guard(tx_table_guard));
  ObTxTable *tx_table = tx_table_guard.get_tx_table();
  ASSERT_NE(nullptr, tx_table);
  ObTxDataGuard tx_data_guard;
  ASSERT_EQ(OB_SUCCESS, tx_table->alloc_tx_data(tx_data_guard, false /* enable_throttle */));
  ObTxData *tx_data = tx_data_guard.tx_data();
  ASSERT_NE(nullptr, tx_data);
  tx_data->tx_id_ = transaction::ObTransID(uncommitted_tx_id);
  tx_data->commit_version_.convert_for_tx(30);
  tx_data->start_scn_.convert_for_tx(1);
  tx_data->end_scn_ = tx_data->commit_version_;
  tx_data->state_ = ObTxData::COMMIT;
  ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));

  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_sstable(sstable));
  ASSERT_NE(nullptr, sstable);
  ASSERT_TRUE(sstable->contain_uncommitted_row());

  ObVersionRange version_range;
  version_range.base_version_ = 10;
  version_range.multi_version_start_ = 0;
  version_range.snapshot_version_ = 20;
  prepare_query_param(version_range);

  // The ordinary query path applies the base-version boundary correctly.
  get_and_check_row(sstable, 1, false, 50);
  get_and_check_row(sstable, 2, true, 120);
  // The physical row is uncommitted, but its transaction committed above snapshot.
  get_and_check_row(sstable, 3, false, 999);
  get_and_check_row(sstable, 4, false, 300);

  context_.query_flag_.set_iter_uncommitted_row();
  // Version 5 is already covered by the base table and must not be returned.
  get_and_check_row(sstable, 1, false, 50);
  // A committed version above base remains visible with iter_uncommitted_row.
  get_and_check_row(sstable, 2, true, 120);
  // The result must not depend on whether the committed transaction was cleaned out.
  get_and_check_row(sstable, 3, true, 999);
  // iter_uncommitted_row intentionally allows committed versions above snapshot.
  get_and_check_row(sstable, 4, true, 300);
  handle.reset();
}

INSTANTIATE_TEST_CASE_P(
  FlatAndCSEncoding,
  TestMultiVersionSSTableSingleGet,
  ::testing::Values(false, true));

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
