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

#include "storage/blocksstable/ob_micro_block_cache.h"
#include "ob_index_block_data_prepare.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
class TestObMicroBlockCache : public TestIndexBlockDataPrepare
{
public:
  TestObMicroBlockCache();
  virtual ~TestObMicroBlockCache();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
protected:
  ObDataMicroBlockCache *data_block_cache_;
  ObIndexMicroBlockCache *index_block_cache_;
};

TestObMicroBlockCache::TestObMicroBlockCache()
  : TestIndexBlockDataPrepare("Test index block row scanner"),
    data_block_cache_(nullptr),
    index_block_cache_(nullptr)
{
}

TestObMicroBlockCache::~TestObMicroBlockCache()
{
}

void TestObMicroBlockCache::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestObMicroBlockCache::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestObMicroBlockCache::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  data_block_cache_ = &ObStorageCacheSuite::get_instance().get_block_cache();
  index_block_cache_ = &ObStorageCacheSuite::get_instance().get_index_block_cache();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));

  prepare_query_param(false);
}

void TestObMicroBlockCache::TearDown()
{
  destroy_query_param();
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

TEST_F(TestObMicroBlockCache, test_block_cache)
{
  // cache key basic func
  ObMicroBlockCacheKey key;
  MacroBlockId block_a(0, 2, 0);
  ObMicroBlockCacheKey a(1, block_a, 0, 10);
  MacroBlockId block_b(0, 3, 0);
  ObMicroBlockCacheKey b(2, block_b, 0, 10);
  ObMicroBlockCacheKey c(a);
  ASSERT_EQ(a, c);
  ASSERT_FALSE(a == b);
  ASSERT_EQ(a.hash(), c.hash());
  ASSERT_NE(a.hash(), b.hash());
  ASSERT_EQ(a.size(), c.size());
  b.set(a.tenant_id_, a.block_id_.macro_id_, a.block_id_.offset_, a.block_id_.size_);
  ASSERT_EQ(a, c);
  ObIKVCacheKey *copy_key;
  ASSERT_EQ(OB_INVALID_ARGUMENT, a.deep_copy(NULL, a.size(), copy_key));
  char buf[1024];
  ASSERT_EQ(OB_INVALID_ARGUMENT, a.deep_copy(buf, 0, copy_key));
  a.set(0, MacroBlockId(0, 2, 0), 0, 0);
  ASSERT_EQ(OB_INVALID_DATA, a.deep_copy(buf, a.size(), copy_key));

  // prefetch and get
  ObMicroBlockBufferHandle idx_buf_handle;
  ObMicroBlockBufferHandle data_buf_handle;
  ObMacroBlockHandle idx_io_handle;
  ObMacroBlockHandle data_io_handle;
  ObMacroBlockHandle multi_io_handle;

  ObIndexBlockRowScanner idx_row_scanner;
  ObMicroBlockData root_block;
  ObMicroIndexInfo micro_idx_info;
  ObArray<ObMicroIndexInfo> micro_idx_infos;
  sstable_.get_index_tree_root(root_block);
  ASSERT_EQ(OB_SUCCESS, idx_row_scanner.init(
      tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils(),
      allocator_,
      context_.query_flag_,
      0));
  ASSERT_EQ(OB_SUCCESS, idx_row_scanner.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID, root_block, ObDatumRowkey::MIN_ROWKEY));
  ASSERT_EQ(OB_SUCCESS, idx_row_scanner.get_next(micro_idx_info));
  ASSERT_TRUE(micro_idx_info.is_valid());
  ASSERT_TRUE(micro_idx_info.is_leaf_block());

  //ASSERT_EQ(OB_ENTRY_NOT_EXIST, index_block_cache_->get_cache_block(
      //MTL_ID(),
      //micro_idx_info.get_macro_id(),
      //micro_idx_info.get_block_offset(),
      //micro_idx_info.get_block_size(),
      //idx_buf_handle));

  ASSERT_EQ(OB_SUCCESS, index_block_cache_->prefetch(
      MTL_ID(),
      micro_idx_info.get_macro_id(),
      micro_idx_info,
      context_.query_flag_.is_use_block_cache(),
      idx_io_handle,
      &allocator_));
  ASSERT_EQ(OB_SUCCESS, idx_io_handle.wait());
  ASSERT_EQ(OB_SUCCESS, index_block_cache_->get_cache_block(
      MTL_ID(),
      micro_idx_info.get_macro_id(),
      micro_idx_info.get_block_offset(),
      micro_idx_info.get_block_size(),
      idx_buf_handle));
  ObMicroBlockData idx_prefetch_data =
      reinterpret_cast<const ObMicroBlockCacheValue*>(idx_io_handle.get_buffer())->get_block_data();
  ASSERT_EQ(idx_buf_handle.get_block_data()->size_, idx_prefetch_data.size_);
  ASSERT_EQ(idx_buf_handle.get_block_data()->extra_size_, idx_prefetch_data.extra_size_);
  ASSERT_EQ(0, memcmp(idx_buf_handle.get_block_data()->get_buf(),
      idx_prefetch_data.get_buf(), idx_prefetch_data.get_buf_size()));
  ASSERT_EQ(idx_prefetch_data.get_extra_size(), idx_buf_handle.get_block_data()->get_extra_size());
  //if (idx_prefetch_data.get_extra_size() > 0 && idx_buf_handle.get_block_data()->get_extra_size() > 0) {
    //int ret = OB_SUCCESS;
    //const ObIndexBlockDataHeader * header1 = reinterpret_cast<const ObIndexBlockDataHeader *>(idx_buf_handle.get_block_data()->get_extra_buf());
    //const ObIndexBlockDataHeader * header2 = reinterpret_cast<const ObIndexBlockDataHeader *>(idx_prefetch_data.get_extra_buf());
    //ASSERT_EQ(0, memcmp(idx_buf_handle.get_block_data()->get_extra_buf(), idx_prefetch_data.get_extra_buf(), 32));
  //}
  ASSERT_EQ(ObMicroBlockData::INDEX_BLOCK, idx_buf_handle.get_block_data()->type_);
  ASSERT_EQ(ObMicroBlockData::INDEX_BLOCK, idx_prefetch_data.type_);

  idx_row_scanner.reuse();
  int tmp_ret = OB_SUCCESS;
  ObDatumRange full_range;
  full_range.set_whole_range();
  idx_row_scanner.open(micro_idx_info.get_macro_id(), idx_prefetch_data, full_range, 0, true, true);
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = idx_row_scanner.get_next(micro_idx_info);
    if (OB_SUCCESS == tmp_ret) {
      ASSERT_EQ(OB_SUCCESS, micro_idx_infos.push_back(micro_idx_info));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_NE(0, micro_idx_infos.count());
  ObMicroIndexInfo &data_idx_info = micro_idx_infos[0];
  ASSERT_TRUE(data_idx_info.is_data_block());
  ASSERT_EQ(OB_SUCCESS, data_block_cache_->prefetch(
      MTL_ID(),
      data_idx_info.get_macro_id(),
      data_idx_info,
      context_.query_flag_.is_use_block_cache(),
      data_io_handle,
      &allocator_));
  ASSERT_EQ(OB_SUCCESS, data_io_handle.wait());
  ASSERT_EQ(OB_SUCCESS, data_block_cache_->get_cache_block(
      MTL_ID(),
      data_idx_info.get_macro_id(),
      data_idx_info.get_block_offset(),
      data_idx_info.get_block_size(),
      data_buf_handle));
  ASSERT_TRUE(data_io_handle.is_valid());
  ASSERT_TRUE(data_buf_handle.is_valid());
  ObMicroBlockData data_prefetch_data =
      reinterpret_cast<const ObMicroBlockCacheValue*>(data_io_handle.get_buffer())->get_block_data();
  ASSERT_EQ(data_buf_handle.get_block_data()->size_, data_prefetch_data.size_);
  ASSERT_EQ(data_buf_handle.get_block_data()->extra_size_, data_prefetch_data.extra_size_);
  ASSERT_EQ(0, memcmp(data_buf_handle.get_block_data()->get_buf(),
      data_prefetch_data.get_buf(), data_prefetch_data.get_buf_size()));
  ASSERT_EQ(0, memcmp(data_buf_handle.get_block_data()->get_extra_buf(),
      data_prefetch_data.get_extra_buf(), data_prefetch_data.get_extra_size()));
  ASSERT_EQ(ObMicroBlockData::DATA_BLOCK, data_buf_handle.get_block_data()->type_);
  ASSERT_EQ(ObMicroBlockData::DATA_BLOCK, data_prefetch_data.type_);


  // multi block io
  int64_t block_count = 0;
  ObMultiBlockIOParam multi_io_param;
  multi_io_param.row_header_ = micro_idx_infos.at(0).row_header_;
  multi_io_param.micro_infos_.set_allocator(&allocator_);
  multi_io_param.micro_infos_.prepare_reallocate(micro_idx_infos.count());
  while (block_count < 16 && block_count < micro_idx_infos.count()) {
    multi_io_param.micro_infos_[block_count].set(
        micro_idx_infos.at(block_count).get_block_offset(), micro_idx_infos.at(block_count).get_block_size());
    multi_io_param.data_cache_size_ += micro_idx_infos.at(block_count).get_block_size();
    multi_io_param.micro_block_count_++;
    block_count++;
  }
  ASSERT_EQ(OB_SUCCESS, data_block_cache_->prefetch_multi_block(
          MTL_ID(),
          data_idx_info.get_macro_id(),
          multi_io_param,
          context_.query_flag_.is_use_block_cache(),
          multi_io_handle));
  ASSERT_EQ(OB_SUCCESS, multi_io_handle.wait());
  const ObMultiBlockIOResult *io_result
      = reinterpret_cast<const ObMultiBlockIOResult *>(multi_io_handle.get_buffer());
  ASSERT_NE(nullptr, io_result);

  int64_t idx = 0;
  ObMicroBlockData data_block_data;
  while (idx != block_count) {
    ASSERT_EQ(OB_SUCCESS, io_result->get_block_data(idx, multi_io_param.micro_infos_[idx], data_block_data));
    ASSERT_TRUE(data_block_data.is_valid());
    ASSERT_EQ(ObMicroBlockData::DATA_BLOCK, data_block_data.type_);
    ASSERT_EQ(data_block_data.get_micro_header()->row_count_, micro_idx_infos[idx].get_row_count());
    ++idx;
  }


  // load data cache block
  ObMacroBlockReader macro_reader;
  ObMicroBlockDesMeta micro_des_meta;
  ObMicroBlockData loaded_micro_data;
  ObMicroBlockId micro_block_id;
  micro_block_id.macro_id_ = data_idx_info.get_macro_id();
  micro_block_id.offset_ = data_idx_info.get_block_offset();
  micro_block_id.size_ = data_idx_info.get_block_size();
  ASSERT_EQ(OB_SUCCESS, data_idx_info.row_header_->fill_micro_des_meta(false, micro_des_meta));
  ASSERT_EQ(OB_SUCCESS, data_block_cache_->load_block(
      micro_block_id,
      micro_des_meta,
      &macro_reader,
      loaded_micro_data,
      &allocator_));
  ASSERT_TRUE(loaded_micro_data.is_valid());
  ASSERT_EQ(ObMicroBlockData::DATA_BLOCK, loaded_micro_data.type_);
  ASSERT_NE(loaded_micro_data.get_micro_header(), nullptr);
  ASSERT_EQ(loaded_micro_data.get_micro_header()->row_count_, data_idx_info.get_row_count());


  // load index cache block
  ObMicroBlockData loaded_index_data;
  idx_row_scanner.reuse();
  ASSERT_EQ(OB_SUCCESS, idx_row_scanner.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID, root_block,  full_range, 0, true, true));
  ASSERT_EQ(OB_SUCCESS, idx_row_scanner.get_next(micro_idx_info));
  ASSERT_EQ(OB_SUCCESS, idx_row_scanner.get_next(micro_idx_info));
  ASSERT_TRUE(micro_idx_info.is_valid());
  micro_block_id.macro_id_ = micro_idx_info.get_macro_id();
  micro_block_id.offset_ = micro_idx_info.get_block_offset();
  micro_block_id.size_ = micro_idx_info.get_block_size();
  ASSERT_EQ(OB_SUCCESS, micro_idx_info.row_header_->fill_micro_des_meta(false, micro_des_meta));
  ASSERT_EQ(OB_SUCCESS, index_block_cache_->load_block(
      micro_block_id,
      micro_des_meta,
      nullptr,
      loaded_index_data,
      &allocator_));
  ASSERT_TRUE(loaded_index_data.is_valid());
  ASSERT_EQ(ObMicroBlockData::INDEX_BLOCK, loaded_index_data.type_);
  ASSERT_TRUE(loaded_index_data.get_micro_header()->is_valid());
}


} // blocksstable
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_block_cache.log*");
  OB_LOGGER.set_file_name("test_block_cache.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
