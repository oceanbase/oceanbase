/**
 * Copyright (c) 2025 OceanBase
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

#include "ob_index_block_data_prepare.h"
#include "storage/access/ob_skip_index_sortedness.h"

namespace oceanbase
{
class TestSkipIndexSortedness : public TestIndexBlockDataPrepare
{
public:
  TestSkipIndexSortedness()
      : TestIndexBlockDataPrepare("Test Skip Index Sortedness",
                                  /* merge_type */ MAJOR_MERGE,
                                  /* need_aggregate_data */ true)
  {
    is_cg_data_ = true;
  }

  virtual ~TestSkipIndexSortedness() {}

  static void SetUpTestCase() { TestIndexBlockDataPrepare::SetUpTestCase(); }

  static void TearDownTestCase() { TestIndexBlockDataPrepare::TearDownTestCase(); }

  virtual void SetUp()
  {
    TestIndexBlockDataPrepare::SetUp();

    ObLSID ls_id(ls_id_);
    ObTabletID tablet_id(tablet_id_);
    ObLSHandle ls_handle;
    ObLSService *ls_svr = MTL(ObLSService *);

    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
    sstable_.key_.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
  }

  void prepare_cg_data() override
  {
    ObMacroBlockWriter data_writer;
    row_generate_.reset();

    // get IntType column
    uint16_t cg_cols[1];
    ObWholeDataStoreDesc desc;
    share::SCN scn;
    scn.convert_for_tx(SNAPSHOT_VERSION);
    ASSERT_EQ(OB_SUCCESS,
              desc.init(false /*is_ddl*/,
                        table_schema_,
                        ObLSID(ls_id_),
                        ObTabletID(tablet_id_),
                        merge_type_,
                        SNAPSHOT_VERSION,
                        DATA_CURRENT_VERSION,
                        table_schema_.get_micro_index_clustered(),
                        0 /*transfer_seq*/,
                        0 /*concurrent_cnt*/,
                        scn));
    ObIArray<ObColDesc> &col_descs = desc.get_desc().col_desc_->col_desc_array_;
    for (int64_t i = 0; i < col_descs.count(); ++i) {
      if (col_descs.at(i).col_type_.type_ == ObIntType) {
        cg_cols[0] = i;
        int_column_id_ = col_descs.at(i).col_id_;
      }
    }

    storage::ObStorageColumnGroupSchema cg_schema;
    cg_schema.type_ = share::schema::SINGLE_COLUMN_GROUP;
    cg_schema.compressor_type_ = table_schema_.get_compressor_type();
    cg_schema.row_store_type_ = table_schema_.get_row_store_type();
    cg_schema.block_size_ = table_schema_.get_block_size();
    cg_schema.column_cnt_ = 1;
    cg_schema.schema_column_cnt_ = table_schema_.get_column_count();
    cg_schema.column_idxs_ = cg_cols;

    ASSERT_EQ(merge_type_, ObMergeType::MAJOR_MERGE);
    ObWholeDataStoreDesc data_desc;
    OK(data_desc.init(false /*is_ddl*/,
                      table_schema_,
                      ObLSID(ls_id_),
                      ObTabletID(tablet_id_),
                      merge_type_,
                      SNAPSHOT_VERSION,
                      DATA_CURRENT_VERSION,
                      table_schema_.get_micro_index_clustered(),
                      0 /*transfer_seq*/,
                      0 /*concurrent_cnt*/,
                      share::SCN::min_scn()/*reorgnize scn*/,
                      scn,
                      &cg_schema,
                      0));
    data_desc.get_desc().static_desc_->schema_version_ = 10;
    void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
    root_index_builder_
        = new (builder_buf) ObSSTableIndexBuilder(false /* not need writer buffer*/);
    ASSERT_NE(nullptr, root_index_builder_);
    data_desc.get_desc().sstable_index_builder_ = root_index_builder_;

    OK(prepare_cg_read_info(data_desc.get_desc().get_col_desc_array().at(0)));

    ASSERT_TRUE(data_desc.is_valid());

    OK(root_index_builder_->init(data_desc.get_desc()));
    ObMacroSeqParam seq_param;
    seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    seq_param.start_ = 0;
    ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(data_writer.open(data_desc.get_desc(),
                        0 /*parallel_idx*/,
                        seq_param /*start_seq*/,
                        pre_warm_param,
                        cleaner));
    OK(row_generate_.init(table_schema_, &allocator_));
    insert_cg_data(data_writer);
    OK(data_writer.close());
    // data write ctx has been moved to root_index_builder
    ASSERT_EQ(data_writer.get_macro_block_write_ctx().get_macro_block_count(), 0);
    data_macro_block_cnt_ = root_index_builder_->roots_[0]->meta_block_info_.get_row_count();
    ASSERT_GE(data_macro_block_cnt_, 0);

    const int64_t column_cnt = data_desc.get_desc().get_row_column_count();
    close_builder_and_prepare_sstable(column_cnt);
  }

  virtual void TearDown()
  {
    tablet_handle_.reset();
    TestIndexBlockDataPrepare::TearDown();
  }

  void insert_cg_data(ObMacroBlockWriter &data_writer) override
  {
    row_cnt_ = 0;
    const int64_t test_row_cnt = 10000000;
    ObDatumRow cg_row;
    OK(cg_row.init(1));
    random_.seed(12345);

    while (true) {
      if (row_cnt_ >= test_row_cnt) {
        break;
      }
      uint32_t number = random_.get(1, 10000000);
      cg_row.storage_datums_[0].set_int(number);
      // cg_row.storage_datums_[0].set_number(generate_number(number));
      // cg_row.storage_datums_[0].set_string(generate_string(number));
      OK(data_writer.append_row(cg_row));
      if ((row_cnt_ + 1) % 100 == 0) {
        OK(data_writer.build_micro_block());
      }
      if ((row_cnt_ + 1) % 10000 == 0) {
        OK(data_writer.try_switch_macro_block());
      }
      ++row_cnt_;
    }
  }

  ObString generate_string(int val)
  {
    ObString str;
    char *buf = static_cast<char *>(allocator_.alloc(VARIABLE_BUF_LEN));
    sprintf(buf, "%040d", val);
    str.assign(buf, 40);
    return str;
  }

  ObNumber generate_number(int val)
  {
    ObNumber number;
    char *buf = static_cast<char *>(allocator_.alloc(VARIABLE_BUF_LEN));
    sprintf(buf, "%.6lf", val / 1000.0);
    number.from_v3(buf, strlen(buf), allocator_);
    return number;
  }

  int int_column_id_;
  ObRandom random_;
};

TEST_F(TestSkipIndexSortedness, sample)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> res_count;
  const ObITableReadInfo *index_read_info = nullptr;
  if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
    SERVER_LOG(WARN, "failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
  }

  for (int rep = 0; rep <= 10; rep++) {
    for (int i = 1000; i <= 100000; i *= 10) {
      int64_t start_us = ObTimeUtility::current_time();

      double sortedness;
      int64_t sample_count = random_.get(0, 100);
      ObSkipIndexSortedness sortedness_calcer;
      ASSERT_EQ(OB_SUCCESS,
                sortedness_calcer
                    .init(sstable_, table_schema_, index_read_info, sample_count, int_column_id_, res_count));
      ASSERT_EQ(OB_SUCCESS, sortedness_calcer.sample_and_calc(sortedness));

      int64_t end_us = ObTimeUtility::current_time();
      STORAGE_LOG(INFO, "Time Elapsed", K(i), K(sample_count), K((end_us - start_us) / 1000.0), K(sortedness));
      ASSERT_LE(sortedness, 0.4);
      ASSERT_GE(sortedness, 0);
    }
  }
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_skip_index_sortedness.log*");
  OB_LOGGER.set_file_name("test_skip_index_sortedness.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
