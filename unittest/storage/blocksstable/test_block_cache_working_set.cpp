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
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/compaction/ob_micro_block_iterator.h"
#include "storage/blocksstable/ob_block_cache_working_set.h"
#include "ob_data_file_prepare.h"
#include "ob_row_generate.h"
#include "ob_schema_generator.h"
#include "share/ob_tenant_mgr.h"
#undef private
namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
using namespace share;
namespace unittest
{
class TestBlockCacheWorkingSet : public TestDataFilePrepare
{
public:
  TestBlockCacheWorkingSet();
  virtual ~TestBlockCacheWorkingSet();
  virtual void SetUp();
  virtual void TearDown();
protected:
  void prepare_data();
  void get_micro_infos_and_datas(
      const int64_t micro_block_count,
      ObIArray<ObMicroBlockInfo> &block_infos,
      ObArray<ObMicroBlockData> &block_datas);
  void io_handle_reset_before_io_submit();
  ObMicroBlockCache block_cache_;
  ObBlockCacheWorkingSet block_cache_ws_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObMacroBlockWriter::MacroBlockList block_ids_;
  ObArenaAllocator allocator_;
};
TestBlockCacheWorkingSet::TestBlockCacheWorkingSet()
  : TestDataFilePrepare("TestBlockCacheWorkingSet"),
    block_cache_(),
    block_cache_ws_()
{
}
TestBlockCacheWorkingSet::~TestBlockCacheWorkingSet()
{
}
void TestBlockCacheWorkingSet::SetUp()
{
  int ret = OB_SUCCESS;
  TestDataFilePrepare::SetUp();
  ret = block_cache_.init("micro_block_cache", 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, block_cache_ws_.init(1));
  // prepare schema
  tenant_id_ = 1;
  table_id_ = combine_id(tenant_id_, 1);
  const int64_t column_count = ObExtendType - 1;
  const int64_t rowkey_count = 8;
  ObSchemaGenerator::generate_table(table_id_, column_count, rowkey_count, table_schema_);
  prepare_data();
}
void TestBlockCacheWorkingSet::TearDown()
{
  allocator_.reset();
  block_cache_ws_.reset();
  block_cache_.destroy();
  row_generate_.reset();
  table_schema_.reset();
  TestDataFilePrepare::TearDown();
  allocator_.reset();
}
void TestBlockCacheWorkingSet::prepare_data()
{
  int ret = OB_SUCCESS;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  int64_t data_version = 1;
  ObDataStoreDesc desc;
  ret = desc.init(table_schema_, data_version, desc.default_encoder_opt(), NULL, 1, true, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.open(desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStoreRow row;
  ObObj cells[ObExtendType - 1];
  row.row_val_.cells_ = cells;
  row.row_val_.count_ = ObExtendType - 1;
  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  while (writer.get_macro_block_list().count() < 3) {
    ret = row_generate_.get_next_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t lsn = 0;
  ret = SLOGGER.commit(lsn);
  ASSERT_EQ(OB_SUCCESS, ret);
  block_ids_ = writer.get_macro_block_list();
}
void TestBlockCacheWorkingSet::get_micro_infos_and_datas(
    const int64_t micro_block_count,
    ObIArray<ObMicroBlockInfo> &block_infos,
    ObArray<ObMicroBlockData> &block_datas)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle meta_handle;
  const ObMacroBlockMeta *macro_meta = NULL;
  ret = ObMacroBlockMetaMgr::get_instance().get_meta(block_ids_.at(0), meta_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  macro_meta = meta_handle.get_meta();
  ObStoreRange range;
  range.start_key_.set_min_row();
  range.end_key_ = ObRowkey(macro_meta->endkey_, macro_meta->rowkey_column_number_);
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ObArray<ObColDesc> column_ids;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(column_ids, true));
  compaction::ObMicroBlockIterator micro_iter;
  ASSERT_EQ(OB_SUCCESS, micro_iter.init(table_id_, column_ids, range, block_ids_.at(0)));
  ASSERT_EQ(OB_SUCCESS, block_infos.assign(micro_iter.get_micro_block_infos()));
  const ObMicroBlock *micro_block = NULL;
  for (int64_t i = 0; i < micro_block_count; ++i) {
    micro_iter.next(micro_block);
    ObMicroBlockData data;
    void *ptr = allocator_.alloc(micro_block->data_.size_);
    MEMCPY(ptr, micro_block->data_.buf_, micro_block->data_.size_);
    data.buf_ = (const char *)ptr;
    data.size_ = micro_block->data_.size_;
    ASSERT_EQ(OB_SUCCESS, block_datas.push_back(data));
  }
}
void TestBlockCacheWorkingSet::io_handle_reset_before_io_submit()
{
  int ret = OB_SUCCESS;
  MacroBlockId block_id;
  ObMacroBlockHandle io_handle;
  ObBlockCacheWorkingSet block_cache_ws;
  ASSERT_EQ(OB_SUCCESS, block_cache_ws.init(1));
  ObArray<ObMicroBlockInfo> block_infos;
  ObArray<ObMicroBlockData> block_datas;
  int64_t micro_block_count = 1;
  block_id = block_ids_.at(0);
  get_micro_infos_and_datas(micro_block_count, block_infos, block_datas);
  STORAGE_LOG(INFO, "xxx", K(block_infos.at(0)));
  ret = block_cache_ws.prefetch(table_id_, block_id, block_infos.at(0).offset_, block_infos.at(0).size_, NULL, false, io_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}
TEST(ObBlockCacheWorkingSetKey, basic)
{
  ObMicroBlockCacheKey key;
  MacroBlockId block_a(2);
  ObMicroBlockCacheKey a(1, block_a, 0, 10);
  MacroBlockId block_b(3);
  ObMicroBlockCacheKey b(2, block_b, 0, 10);
  ObMicroBlockCacheKey c(a);
  ASSERT_EQ(a, c);
  ASSERT_FALSE(a == b);
  ASSERT_EQ(a.hash(), c.hash());
  ASSERT_NE(a.hash(), b.hash());
  ASSERT_EQ(a.size(), c.size());
  b.set(a.table_id_, a.block_id_, a.offset_, a.size_);
  ASSERT_EQ(a, c);
  ObIKVCacheKey *copy_key;
  ASSERT_EQ(OB_INVALID_ARGUMENT, a.deep_copy(NULL, a.size(), copy_key));
  char buf[1024];
  ASSERT_EQ(OB_INVALID_ARGUMENT, a.deep_copy(buf, 0, copy_key));
  a.set(0, MacroBlockId(2), 0, 0);
  ASSERT_EQ(OB_INVALID_DATA, a.deep_copy(buf, a.size(), copy_key));
}
TEST_F(TestBlockCacheWorkingSet, put_get)
{
  int ret = OB_SUCCESS;
  MacroBlockId block_id;
  ObMicroBlockBufferHandle handle;
  ObMacroBlockHandle io_handle;
  //invalid argument
  ret = block_cache_ws_.get_cache_block(table_id_, block_id, 0, 0, handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = block_cache_ws_.prefetch(table_id_, block_id, 0, 0, NULL, false, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  //normal miss get
  block_id = block_ids_.at(0);
  ret = block_cache_ws_.get_cache_block(table_id_, block_id, 0, 1024, handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  //prefetch and hit get
  ObArray<ObMicroBlockInfo> block_infos;
  ObArray<ObMicroBlockData> block_datas;
  int64_t micro_block_count = 1;
  get_micro_infos_and_datas(micro_block_count, block_infos, block_datas);
  STORAGE_LOG(INFO, "xxx", K(block_infos.at(0)));
  ret = block_cache_ws_.prefetch(table_id_, block_id, block_infos.at(0).offset_, block_infos.at(0).size_, NULL, false, io_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = io_handle.wait(DEFAULT_IO_WAIT_TIME_MS);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_cache_ws_.get_cache_block(table_id_, block_id, block_infos.at(0).offset_, block_infos.at(0).size_, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObMicroBlockData prefetch_data =
      *reinterpret_cast<const ObMicroBlockData*>(io_handle.get_buffer());
  ASSERT_EQ(block_datas.at(0).size_, prefetch_data.size_);
  ASSERT_EQ(0, memcmp(block_datas.at(0).get_buf(), prefetch_data.get_buf(), block_datas.at(0).size_));
  ASSERT_EQ(block_datas.at(0).size_, handle.get_block_data()->size_);
  ASSERT_EQ(0, memcmp(block_datas.at(0).get_buf(), handle.get_block_data()->get_buf(), block_datas.at(0).size_));
  ObMacroBlockReader reader;
  ObMicroBlockData block_data;
  ASSERT_EQ(OB_SUCCESS, block_cache_ws_.load_cache_block(reader, table_id_, block_id,
        block_infos.at(0).offset_, block_infos.at(0).size_, block_data));
  ASSERT_EQ(block_datas.at(0).size_, block_data.size_);
  ASSERT_EQ(0, memcmp(block_datas.at(0).get_buf(), block_data.get_buf(), block_datas.at(0).size_));
}
TEST_F(TestBlockCacheWorkingSet, multiblock_io)
{
  MacroBlockId block_id;
  ObMacroBlockHandle io_handle;
  ObMultiBlockIOParam io_param;
  // invalid argument
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_cache_ws_.prefetch(table_id_,
      block_ids_.at(0), io_param, false, io_handle));
  ObArray<ObMicroBlockInfo> block_infos;
  ObArray<ObMicroBlockData> block_datas;
  int64_t micro_block_count = 10;
  get_micro_infos_and_datas(micro_block_count, block_infos, block_datas);
  io_param.micro_block_infos_ = &block_infos;
  io_param.start_index_ = 0;
  io_param.block_count_ = micro_block_count;
  ASSERT_EQ(OB_SUCCESS, block_cache_ws_.prefetch(table_id_, block_ids_.at(0), io_param, false, io_handle));
  ASSERT_EQ(OB_SUCCESS, io_handle.wait(DEFAULT_IO_WAIT_TIME_MS));
  const ObMultiBlockIOResult *result =
      reinterpret_cast<const ObMultiBlockIOResult *>(io_handle.get_buffer());
  for (int64_t i = 0; i < micro_block_count; ++i) {
    ObMicroBlockData block_data;
    ASSERT_EQ(OB_SUCCESS, result->get_block_data(i, block_data));
    ASSERT_EQ(block_datas.at(i).size_, block_data.size_);
    ASSERT_EQ(0, memcmp(block_datas.at(i).get_buf(), block_data.buf_, block_data.size_));
  }
  ObMicroBlockData temp_block_data;
  ASSERT_EQ(OB_INVALID_ARGUMENT, result->get_block_data(micro_block_count, temp_block_data));
}
TEST_F(TestBlockCacheWorkingSet, put_ws)
{
  ObTenantManager &tenant_mgr = ObTenantManager::get_instance();
  const int64_t mem_limit = 1 * 1024 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, tenant_mgr.init());
  ASSERT_EQ(OB_SUCCESS, tenant_mgr.add_tenant(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, tenant_mgr.set_tenant_mem_limit(tenant_id_, mem_limit, mem_limit));
  MacroBlockId block_id(2);
  const int64_t size = 16 * 1024;
  char buf[size];
  ObMicroBlockCacheValue value(buf, size);
  ASSERT_FALSE(block_cache_ws_.use_working_set_);
  for (int64_t i = 0; i < 2 * 1024; ++i) {
    ObMicroBlockCacheKey key(table_id_, block_id, i, size);
    ASSERT_EQ(OB_SUCCESS, block_cache_ws_.put(key, value));
  }
  ASSERT_TRUE(block_cache_ws_.use_working_set_);
  tenant_mgr.destroy();
}
TEST_F(TestBlockCacheWorkingSet, io_handle_reset_before_io_submit)
{
  io_handle_reset_before_io_submit();
  char buf[128 * 1024];
  memset(buf, 0, sizeof(buf));
  STORAGE_LOG(INFO, "xxx");
  usleep(3 * 1000 * 1000);
}
}//end namespace unittest
}//end namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}