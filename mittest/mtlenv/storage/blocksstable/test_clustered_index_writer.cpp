// owner: baichangmin.bcm
// owner group: storage

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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#define protected public

#include "mittest/mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"

bool mock_micro_index_clustered_result = true;

namespace oceanbase
{
using namespace common;

// Mock
namespace blocksstable
{
bool ObDataStoreDesc::micro_index_clustered() const
{
  return ::mock_micro_index_clustered_result;
}
}

namespace blocksstable
{

class TestClusteredIndexWriter : public TestIndexBlockDataPrepare
{
public:
  TestClusteredIndexWriter();
  virtual ~TestClusteredIndexWriter() {}
  virtual void SetUp();
  virtual void TearDown();
  void prepare_data_store_desc(ObWholeDataStoreDesc &data_desc, ObSSTableIndexBuilder *sstable_index_builder);
  bool is_equal(ObMicroBlockDesc &l, ObMicroBlockDesc &r);
};

TestClusteredIndexWriter::TestClusteredIndexWriter()
  : TestIndexBlockDataPrepare(
      "test_clustered_index_writer",
      MAJOR_MERGE,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      65535 * 4)
{
}

void TestClusteredIndexWriter::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestClusteredIndexWriter::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestClusteredIndexWriter::prepare_data_store_desc(
    ObWholeDataStoreDesc &data_desc, ObSSTableIndexBuilder *sstable_index_builder)
{
  int ret = OB_SUCCESS;
  ret = data_desc.init(false/*is_ddl*/, table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), MAJOR_MERGE,
                       ObTimeUtility::fast_current_time() /*snapshot_version*/,
                       DATA_CURRENT_VERSION,
                       table_schema_.get_micro_index_clustered(),
                       0 /*transfer_seq*/);
  data_desc.get_desc().sstable_index_builder_ = sstable_index_builder;
  ASSERT_EQ(OB_SUCCESS, ret);
}

bool TestClusteredIndexWriter::is_equal(ObMicroBlockDesc &l, ObMicroBlockDesc &r) {
  bool res = false;
  if (!(l.last_rowkey_== r.last_rowkey_)) {
    res = false;
  } else if (l.buf_size_ != r.buf_size_ || l.data_size_ != r.data_size_
      || l.original_size_ != r.original_size_ || l.row_count_ != r.row_count_
      || l.column_count_ != r.column_count_ || l.max_merged_trans_version_ != r.max_merged_trans_version_
      || l.macro_id_ != r.macro_id_ || l.block_offset_ != r.block_offset_
      || l.block_checksum_ != r.block_checksum_ || l.row_count_delta_ != r.row_count_delta_
      || l.contain_uncommitted_row_ != r.contain_uncommitted_row_ || l.can_mark_deletion_ != r.can_mark_deletion_
      || l.has_string_out_row_ != r.has_string_out_row_ || l.has_lob_out_row_ != r.has_lob_out_row_
      || l.is_last_row_last_flag_ != r.is_last_row_last_flag_) {
        res = false;
  } else if (l.header_->header_checksum_ != r.header_->header_checksum_
      || l.header_->data_checksum_ != r.header_->data_checksum_){
        res = false;
  } else if (l.header_->has_column_checksum_) {
    res = true;
    for (int64_t i = 0; i < l.header_->column_count_; ++i) {
      if(l.header_->column_checksums_[i] != r.header_->column_checksums_[i]) {
        res = false;
        break;
      }
    }
  } else {
    res = true;
  }
  return res;
}

TEST_F(TestClusteredIndexWriter, test_reuse_macro_block)
{
  LOG_INFO("BEGIN TestClusteredIndexWriter.test_reuse_macro_block");

  // prepare data store desc and macro block writer
  ObWholeDataStoreDesc data_desc;
  prepare_data_store_desc(data_desc, root_index_builder_);

  ObMacroBlockWriter data_writer;
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 0;
  ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
  ObSSTablePrivateObjectCleaner cleaner;
  OK(data_writer.open(data_desc.get_desc(), 0/*parallel_idx*/, seq_param/*start_seq*/, pre_warm_param, cleaner));

  // prepare dual iterator
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObDatumRange range;
  range.set_whole_range();
  ObDualMacroMetaIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(sstable_, range, index_read_info, allocator_));

  // iterator sstable through dual iterator and reuse data macro block
  int ret = OB_SUCCESS;
  blocksstable::ObMacroBlockDesc macro_desc;
  blocksstable::ObDataMacroBlockMeta macro_meta;
  const blocksstable::ObMicroBlockData *clustered_micro_block_data = nullptr;
  macro_desc.macro_meta_ = &macro_meta;
  int i = 0;
  do {
    ret = iter.get_next_macro_block(macro_desc);
    if (OB_SUCCESS == ret) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = iter.get_current_clustered_index_info(clustered_micro_block_data);
      ASSERT_EQ(OB_SUCCESS, ret);
      LOG_INFO("get next", K(ret), K(i), KPC(clustered_micro_block_data));
      ASSERT_EQ(OB_SUCCESS, data_writer.append_macro_block(macro_desc, clustered_micro_block_data));
      macro_desc.reuse();
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
    ++i;
  } while (OB_SUCC(ret));

  if (ret != OB_SUCCESS) {
    LOG_INFO("iter end", K(ret));
  }
  ASSERT_EQ(OB_ITER_END, ret);

  LOG_INFO("FINISH TestClusteredIndexWriter.test_reuse_macro_block");
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_clustered_index_writer.log*");
  OB_LOGGER.set_file_name("test_clustered_index_writer.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
