// owner: saitong.zst
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

#include <gtest/gtest.h>
#define private public
#define protected public

#include "share/datum/ob_datum.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
namespace blocksstable
{

class TestMacroMetaTempStore : public TestIndexBlockDataPrepare
{
public:
  TestMacroMetaTempStore()
    : TestIndexBlockDataPrepare("TestMacroMetaTempStore"),
      root_index_builder_(false)
  {}
  virtual ~TestMacroMetaTempStore() {}

  virtual void SetUp();
  // virtual void TearDown();

  void prepare_macro_writer();
  void write_one_macro_block();

  ObMacroBlockWriter macro_writer_;
  ObWholeDataStoreDesc desc_;
  ObSSTableIndexBuilder root_index_builder_;
  ObDatumRow datum_row_;
  ObDatumRow mv_row_;
};

void TestMacroMetaTempStore::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(true, MockTenantModuleEnv::get_instance().is_inited());
  // MTL(ObTenantTmpFileManager *)
}

void TestMacroMetaTempStore::prepare_macro_writer()
{
  share::SCN scn;
  scn.convert_for_tx(SNAPSHOT_VERSION);
  ASSERT_EQ(OB_SUCCESS, desc_.init(false, table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_),
      merge_type_, SNAPSHOT_VERSION, DATA_CURRENT_VERSION, table_schema_.get_micro_index_clustered(), 0, 0/*concurrent_cnt*/, scn));
  desc_.get_desc().static_desc_->schema_version_ = 10;
  desc_.get_desc().sstable_index_builder_ = &root_index_builder_;
  ASSERT_TRUE(desc_.is_valid());
  ASSERT_EQ(OB_SUCCESS, root_index_builder_.init(desc_.get_desc()));

  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 0;
  ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
  ObSSTablePrivateObjectCleaner cleaner;
  ASSERT_EQ(OB_SUCCESS, macro_writer_.open(desc_.get_desc(), 0, seq_param, pre_warm_param, cleaner));
  ASSERT_EQ(OB_SUCCESS, datum_row_.init(allocator_, MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, mv_row_.init(allocator_, MAX_TEST_COLUMN_CNT));
}

void TestMacroMetaTempStore::write_one_macro_block()
{
  datum_row_.reuse();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(datum_row_));
  convert_to_multi_version_row(datum_row_, table_schema_, SNAPSHOT_VERSION, ObDmlFlag::DF_INSERT, mv_row_);
  ASSERT_EQ(OB_SUCCESS, macro_writer_.append_row(mv_row_));
  ASSERT_EQ(OB_SUCCESS, macro_writer_.build_micro_block());
  ASSERT_EQ(OB_SUCCESS, macro_writer_.try_switch_macro_block());
}

TEST_F(TestMacroMetaTempStore, test_macro_meta_serialize)
{
  char buf[2 * 1024 * 1024]; // 2MB
  ObDataMacroBlockMeta macro_meta;

  const int64_t test_macro_block_cnt = 1;
  prepare_macro_writer();
  for (int64_t i = 0; i < test_macro_block_cnt; ++i) {
    write_one_macro_block();
  }
  ObSEArray<MacroBlockId, 10> write_macro_ids;
  ASSERT_EQ(OB_SUCCESS, write_macro_ids.assign(macro_writer_.get_macro_block_write_ctx().get_macro_block_list()));
  ASSERT_EQ(OB_SUCCESS, macro_writer_.close());
  ObMacroMetaTempStore macro_meta_temp_store;
  int64_t dir_id = 0;
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id));
  ASSERT_EQ(OB_SUCCESS, macro_meta_temp_store.init(dir_id));
  for (int64_t i = 0; i < write_macro_ids.count(); ++i) {
    ObStorageObjectHandle obj_handle;
    ObStorageObjectReadInfo read_info;
    read_info.offset_ = 0;
    read_info.size_ = DEFAULT_MACRO_BLOCK_SIZE;
    read_info.io_timeout_ms_ = 5000;
    read_info.macro_block_id_ = write_macro_ids.at(i);
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.buf_ = static_cast<char *>(allocator_.alloc(read_info.size_));
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    ASSERT_TRUE(nullptr != read_info.buf_);
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::async_read_object(read_info, obj_handle));
    ASSERT_EQ(OB_SUCCESS, obj_handle.wait());

    ObSSTableMacroBlockHeader macro_header;
    ASSERT_EQ(OB_SUCCESS,
              macro_meta_temp_store.get_macro_block_header(obj_handle.get_buffer(),
                                                           obj_handle.get_data_size(),
                                                           macro_header));
    ASSERT_EQ(OB_SUCCESS,
              macro_meta_temp_store.get_macro_meta_from_block_buf(macro_header,
                                                                  write_macro_ids.at(i),
                                                                  obj_handle.get_buffer() + macro_header.fixed_header_.meta_block_offset_,
                                                                  macro_header.fixed_header_.meta_block_size_,
                                                                  macro_meta));
  }

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro_meta.serialize(buf, 2 * 1024 * 1024, pos, CLUSTER_CURRENT_VERSION));
  ObDataMacroBlockMeta deserialized_macro_meta;
  ASSERT_EQ(deserialized_macro_meta.end_key_.datums_, nullptr);
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialized_macro_meta.deserialize(buf, 2 * 1024 * 1024, allocator_, deserialize_pos));
  ASSERT_EQ(pos, deserialize_pos);
  ASSERT_EQ(macro_meta.end_key_, deserialized_macro_meta.end_key_);
}

TEST_F(TestMacroMetaTempStore, test_reuse_macro_meta)
{
  ObDataMacroBlockMeta macro_meta;
  {
    const int64_t test_macro_block_cnt = 1;
    prepare_macro_writer();
    for (int64_t i = 0; i < test_macro_block_cnt; ++i) {
      write_one_macro_block();
    }
    ObSEArray<MacroBlockId, 10> write_macro_ids;
    ASSERT_EQ(OB_SUCCESS, write_macro_ids.assign(macro_writer_.get_macro_block_write_ctx().get_macro_block_list()));
    ASSERT_EQ(OB_SUCCESS, macro_writer_.close());
    ObMacroMetaTempStore macro_meta_temp_store;
    int64_t dir_id = 0;
    ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id));
    ASSERT_EQ(OB_SUCCESS, macro_meta_temp_store.init(dir_id));
    for (int64_t i = 0; i < write_macro_ids.count(); ++i) {
      ObStorageObjectHandle obj_handle;
      ObStorageObjectReadInfo read_info;
      read_info.offset_ = 0;
      read_info.size_ = DEFAULT_MACRO_BLOCK_SIZE;
      read_info.io_timeout_ms_ = 5000;
      read_info.macro_block_id_ = write_macro_ids.at(i);
      read_info.mtl_tenant_id_ = MTL_ID();
      read_info.buf_ = static_cast<char *>(allocator_.alloc(read_info.size_));
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      ASSERT_TRUE(nullptr != read_info.buf_);
      ASSERT_EQ(OB_SUCCESS, ObObjectManager::async_read_object(read_info, obj_handle));
      ASSERT_EQ(OB_SUCCESS, obj_handle.wait());

      ObSSTableMacroBlockHeader macro_header;
      ASSERT_EQ(OB_SUCCESS,
                macro_meta_temp_store.get_macro_block_header(obj_handle.get_buffer(),
                                                             obj_handle.get_data_size(),
                                                             macro_header));
      ASSERT_EQ(OB_SUCCESS,
                macro_meta_temp_store.get_macro_meta_from_block_buf(macro_header,
                                                                    write_macro_ids.at(i),
                                                                    obj_handle.get_buffer()
                                                                        + macro_header.fixed_header_.meta_block_offset_,
                                                                    macro_header.fixed_header_.meta_block_size_,
                                                                    macro_meta));
    }
  }
  ObMacroMetaTempStore macro_meta_temp_store;
  int64_t dir_id = 0;
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id));
  ASSERT_EQ(OB_SUCCESS, macro_meta_temp_store.init(dir_id));
  ASSERT_EQ(OB_SUCCESS, macro_meta_temp_store.append(macro_meta, nullptr));
}

TEST_F(TestMacroMetaTempStore, test_macro_meta_temp_store)
{
  const int64_t test_macro_block_cnt = 10;
  prepare_macro_writer();
  for (int64_t i = 0; i < test_macro_block_cnt; ++i) {
    write_one_macro_block();
  }

  ObSEArray<MacroBlockId, 10> write_macro_ids;
  ASSERT_EQ(OB_SUCCESS, write_macro_ids.assign(macro_writer_.get_macro_block_write_ctx().get_macro_block_list()));

  ASSERT_EQ(OB_SUCCESS, macro_writer_.close());


  ObMacroMetaTempStore macro_meta_temp_store;
  int64_t dir_id = 0;
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id));
  ASSERT_EQ(OB_SUCCESS, macro_meta_temp_store.init(dir_id));

  for (int64_t i = 0; i < write_macro_ids.count(); ++i) {
    ObStorageObjectHandle obj_handle;
    ObStorageObjectReadInfo read_info;
    read_info.offset_ = 0;
    read_info.size_ = DEFAULT_MACRO_BLOCK_SIZE;
    read_info.io_timeout_ms_ = 5000;
    read_info.macro_block_id_ = write_macro_ids.at(i);
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.buf_ = static_cast<char *>(allocator_.alloc(read_info.size_));
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    ASSERT_TRUE(nullptr != read_info.buf_);
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::async_read_object(read_info, obj_handle));
    ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
    ASSERT_EQ(OB_SUCCESS, macro_meta_temp_store.append(obj_handle.get_buffer(), obj_handle.get_data_size(), write_macro_ids.at(i)));
  }

  ObMacroMetaTempStoreIter tmp_store_iter;
  ObSSTableIndexBuilder sst_index_builder(false);
  ObIndexBlockRebuilder index_block_rebuilder;
  ASSERT_EQ(OB_SUCCESS, tmp_store_iter.init(macro_meta_temp_store));
  ASSERT_EQ(OB_SUCCESS, sst_index_builder.init(desc_.get_desc(), ObSSTableIndexBuilder::DISABLE));
  ObITable::TableKey table_key;
  int64_t tenant_id = 1;
  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key.tablet_id_ = tablet_id_;
  table_key.version_range_.snapshot_version_ = SNAPSHOT_VERSION;
  blocksstable::ObMacroSeqParam macro_seq_param;
  macro_seq_param.start_ = 0;
  macro_seq_param.seq_type_ = blocksstable::ObMacroSeqParam::SeqType::SEQ_TYPE_INC;
  ASSERT_EQ(OB_SUCCESS, index_block_rebuilder.init(sst_index_builder, macro_seq_param, nullptr, table_key));

  int64_t iter_cnt = 0;
  ObDataMacroBlockMeta macro_meta;
  ObMicroBlockData leaf_index_block;
  while (iter_cnt < test_macro_block_cnt) {
    ASSERT_EQ(OB_SUCCESS, tmp_store_iter.get_next(macro_meta, leaf_index_block));
    ASSERT_EQ(OB_SUCCESS, index_block_rebuilder.append_macro_row(macro_meta));
    ++iter_cnt;
  }
  ASSERT_EQ(OB_ITER_END, tmp_store_iter.get_next(macro_meta, leaf_index_block));
  ASSERT_EQ(OB_SUCCESS, index_block_rebuilder.close());

}



} // blocksstable
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_macro_meta_temp_store.log*");
  OB_LOGGER.set_file_name("test_macro_meta_temp_store.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
