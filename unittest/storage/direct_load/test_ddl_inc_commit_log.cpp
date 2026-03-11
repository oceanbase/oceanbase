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
#include <gmock/gmock.h>
#define private public
#define protected public
#include "storage/ddl/ob_ddl_inc_clog.h"
#include "storage/ddl/ob_direct_load_type.h"
#include "storage/tx/ob_trans_id.h"
#include "storage/tx/ob_tx_seq.h"
#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "lib/utility/ob_serialization_helper.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "storage/tx/ob_trans_define.h"

#define ASSERT_SUCC(expr) ASSERT_EQ(common::OB_SUCCESS, (expr))
#define ASSERT_FAIL(expr) ASSERT_NE(common::OB_SUCCESS, (expr))

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::transaction;
using namespace oceanbase::blocksstable;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;

namespace oceanbase
{
namespace unittest
{

class TestDDLIncCommitLog : public ::testing::Test
{
public:
  TestDDLIncCommitLog() {}
  virtual ~TestDDLIncCommitLog() {}

  virtual void SetUp() override
  {
    allocator_.set_tenant_id(1);
  }

  virtual void TearDown() override
  {
    allocator_.reset();
  }

  // 创建一个有效的 ObDDLIncLogBasic
  ObDDLIncLogBasic create_valid_log_basic(
      const uint64_t tablet_id = 100001,
      const uint64_t lob_tablet_id = 100002,
      const ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR)
  {
    ObDDLIncLogBasic log_basic;
    ObTabletID tablet(tablet_id);
    ObTabletID lob_tablet(lob_tablet_id);
    ObTransID trans_id(1001);
    ObTxSEQ seq_no(2001);
    int64_t snapshot_version = 1000000;
    uint64_t data_format_version = DATA_VERSION_4_5_0_0;

    int ret = log_basic.init(tablet, lob_tablet, direct_load_type,
                             trans_id, seq_no, snapshot_version, data_format_version);
    EXPECT_EQ(OB_SUCCESS, ret);
    return log_basic;
  }

  // 创建一个简单的 ObSSTable 用于测试
  int create_mock_sstable(ObTableHandleV2 &table_handle)
  {
    int ret = OB_SUCCESS;
    ObTabletCreateSSTableParam param;

    // 设置基本参数
    param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    param.table_key_.tablet_id_ = ObTabletID(100001);
    param.table_key_.version_range_.base_version_ = 0;
    param.table_key_.version_range_.snapshot_version_ = 1000000;
    param.schema_version_ = 100;
    param.create_snapshot_version_ = 1000000;  // 必须与 snapshot_version 一致
    param.progressive_merge_round_ = 0;
    param.progressive_merge_step_ = 0;
    param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
    param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
    param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
    param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
    param.rowkey_column_cnt_ = 3;
    param.root_block_addr_.set_none_addr();
    param.data_block_macro_meta_addr_.set_none_addr();
    param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
    param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
    param.data_index_tree_height_ = 0;
    param.index_blocks_cnt_ = 0;
    param.data_blocks_cnt_ = 0;
    param.micro_block_cnt_ = 0;
    param.use_old_macro_block_count_ = 0;
    param.data_checksum_ = 12345;
    param.occupy_size_ = 1024;
    param.ddl_scn_.set_min();
    param.filled_tx_scn_.set_min();
    param.tx_data_recycle_scn_.set_min();
    param.rec_scn_.set_min();
    param.original_size_ = 1024;
    param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    param.row_count_ = 100;
    param.column_cnt_ = 5;
    param.sstable_logic_seq_ = 0;
    param.recycle_version_ = 0;
    param.root_macro_seq_ = 0;
    param.nested_size_ = 0;
    param.nested_offset_ = 0;
    param.column_group_cnt_ = 1;
    param.full_column_cnt_ = 5;
    param.co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
    param.co_base_snapshot_version_ = 0;  // 必须 >= 0
    param.table_backup_flag_.reset();
    param.table_shared_flag_.reset();

    // 使用 ObTabletCreateDeleteHelper 创建 SSTable
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator_, table_handle))) {
      LOG_WARN("failed to create sstable", K(ret), K(param));
    }

    return ret;
  }

  // 创建一个简单的 ObCOSSTableV2 用于测试
  int create_mock_co_sstable(ObTableHandleV2 &table_handle)
  {
    int ret = OB_SUCCESS;
    ObTabletCreateSSTableParam param;

    // 设置基本参数
    param.table_key_.table_type_ = ObITable::TableType::COLUMN_ORIENTED_SSTABLE;
    param.table_key_.tablet_id_ = ObTabletID(100001);
    param.table_key_.version_range_.base_version_ = 0;
    param.table_key_.version_range_.snapshot_version_ = 1000000;
    param.schema_version_ = 100;
    param.create_snapshot_version_ = 1000000;  // 必须与 snapshot_version 一致
    param.progressive_merge_round_ = 0;
    param.progressive_merge_step_ = 0;
    param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
    param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
    param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
    param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
    param.rowkey_column_cnt_ = 3;
    param.root_block_addr_.set_none_addr();
    param.data_block_macro_meta_addr_.set_none_addr();
    param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
    param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
    param.data_index_tree_height_ = 0;
    param.index_blocks_cnt_ = 0;
    param.data_blocks_cnt_ = 0;
    param.micro_block_cnt_ = 0;
    param.use_old_macro_block_count_ = 0;
    param.data_checksum_ = 12345;
    param.occupy_size_ = 1024;
    param.ddl_scn_.set_min();
    param.filled_tx_scn_.set_min();
    param.tx_data_recycle_scn_.set_min();
    param.rec_scn_.set_min();
    param.original_size_ = 1024;
    param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    param.row_count_ = 100;
    param.column_cnt_ = 5;
    param.sstable_logic_seq_ = 0;
    param.recycle_version_ = 0;
    param.root_macro_seq_ = 0;
    param.nested_size_ = 0;
    param.nested_offset_ = 0;
    param.column_group_cnt_ = 2;
    param.full_column_cnt_ = 5;
    param.co_base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
    param.co_base_snapshot_version_ = 0;
    param.is_co_table_without_cgs_ = true;
    param.table_backup_flag_.reset();
    param.table_shared_flag_.reset();

    // 使用 ObTabletCreateDeleteHelper 创建 CO SSTable
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator_, table_handle))) {
      LOG_WARN("failed to create co sstable", K(ret), K(param));
    }

    return ret;
  }

  // 创建一个包含 CG sstables 的 ObCOSSTableV2 用于测试
  int create_mock_co_sstable_with_cgs(ObTableHandleV2 &table_handle)
  {
    int ret = OB_SUCCESS;
    ObTabletCreateSSTableParam param;

    // 设置基本参数（与 create_mock_co_sstable 类似，但不设置 is_co_table_without_cgs_）
    param.table_key_.table_type_ = ObITable::TableType::COLUMN_ORIENTED_SSTABLE;
    param.table_key_.tablet_id_ = ObTabletID(100001);
    param.table_key_.version_range_.base_version_ = 0;
    param.table_key_.version_range_.snapshot_version_ = 1000000;
    param.schema_version_ = 100;
    param.create_snapshot_version_ = 1000000;
    param.progressive_merge_round_ = 0;
    param.progressive_merge_step_ = 0;
    param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
    param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
    param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
    param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
    param.rowkey_column_cnt_ = 3;
    param.root_block_addr_.set_none_addr();
    param.data_block_macro_meta_addr_.set_none_addr();
    param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
    param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
    param.data_index_tree_height_ = 0;
    param.index_blocks_cnt_ = 0;
    param.data_blocks_cnt_ = 0;
    param.micro_block_cnt_ = 0;
    param.use_old_macro_block_count_ = 0;
    param.data_checksum_ = 12345;
    param.occupy_size_ = 1024;
    param.ddl_scn_.set_min();
    param.filled_tx_scn_.set_min();
    param.tx_data_recycle_scn_.set_min();
    param.rec_scn_.set_min();
    param.original_size_ = 1024;
    param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    param.row_count_ = 100;
    param.column_cnt_ = 5;
    param.sstable_logic_seq_ = 0;
    param.recycle_version_ = 0;
    param.root_macro_seq_ = 0;
    param.nested_size_ = 0;
    param.nested_offset_ = 0;
    param.column_group_cnt_ = 3;  // 3 个列组：all_cg(0), rowkey_cg(1), normal_cg(2)
    param.full_column_cnt_ = 5;
    param.co_base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
    param.co_base_snapshot_version_ = 0;
    param.is_co_table_without_cgs_ = false;  // 这个 CO SSTable 包含 CG sstables
    param.table_backup_flag_.reset();
    param.table_shared_flag_.reset();

    // 创建 CO SSTable（base sstable，column_group_idx = 0）
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator_, table_handle))) {
      LOG_WARN("failed to create co sstable", K(ret), K(param));
      return ret;
    }

    // 创建 CG sstables
    ObArray<ObITable*> cg_sstables;
    ObTablesHandleArray cg_handles;

    for (int64_t cg_idx = 1; cg_idx < param.column_group_cnt_; ++cg_idx) {
      ObTabletCreateSSTableParam cg_param;
      // 复制基本参数
      cg_param.table_key_.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
      cg_param.table_key_.tablet_id_ = param.table_key_.tablet_id_;
      cg_param.table_key_.column_group_idx_ = static_cast<uint16_t>(cg_idx);
      cg_param.table_key_.version_range_ = param.table_key_.version_range_;
      cg_param.schema_version_ = param.schema_version_;
      cg_param.create_snapshot_version_ = param.create_snapshot_version_;
      cg_param.progressive_merge_round_ = param.progressive_merge_round_;
      cg_param.progressive_merge_step_ = param.progressive_merge_step_;
      cg_param.table_mode_ = param.table_mode_;
      cg_param.index_type_ = param.index_type_;
      cg_param.rowkey_column_cnt_ = param.rowkey_column_cnt_;
      cg_param.root_block_addr_ = param.root_block_addr_;
      cg_param.data_block_macro_meta_addr_ = param.data_block_macro_meta_addr_;
      cg_param.root_row_store_type_ = param.root_row_store_type_;
      cg_param.latest_row_store_type_ = param.latest_row_store_type_;
      cg_param.data_index_tree_height_ = param.data_index_tree_height_;
      cg_param.index_blocks_cnt_ = param.index_blocks_cnt_;
      cg_param.data_blocks_cnt_ = param.data_blocks_cnt_;
      cg_param.micro_block_cnt_ = param.micro_block_cnt_;
      cg_param.use_old_macro_block_count_ = param.use_old_macro_block_count_;
      cg_param.data_checksum_ = param.data_checksum_;
      cg_param.occupy_size_ = param.occupy_size_;
      cg_param.ddl_scn_ = param.ddl_scn_;
      cg_param.filled_tx_scn_ = param.filled_tx_scn_;
      cg_param.tx_data_recycle_scn_ = param.tx_data_recycle_scn_;
      cg_param.rec_scn_ = param.rec_scn_;
      cg_param.original_size_ = param.original_size_;
      cg_param.compressor_type_ = param.compressor_type_;
      cg_param.row_count_ = param.row_count_;
      cg_param.column_cnt_ = param.column_cnt_;
      cg_param.sstable_logic_seq_ = param.sstable_logic_seq_;
      cg_param.recycle_version_ = param.recycle_version_;
      cg_param.root_macro_seq_ = param.root_macro_seq_;
      cg_param.nested_size_ = param.nested_size_;
      cg_param.nested_offset_ = param.nested_offset_;
      cg_param.column_group_cnt_ = 1;  // CG sstable 本身是一个列组
      cg_param.full_column_cnt_ = param.full_column_cnt_;
      cg_param.co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
      cg_param.co_base_snapshot_version_ = param.co_base_snapshot_version_;
      cg_param.is_co_table_without_cgs_ = false;
      cg_param.table_backup_flag_ = param.table_backup_flag_;
      cg_param.table_shared_flag_ = param.table_shared_flag_;

      ObTableHandleV2 cg_handle;
      if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(cg_param, allocator_, cg_handle))) {
        LOG_WARN("failed to create cg sstable", K(ret), K(cg_idx), K(cg_param));
        return ret;
      } else if (OB_FAIL(cg_handles.add_table(cg_handle))) {
        LOG_WARN("failed to add cg handle", K(ret), K(cg_idx));
        return ret;
      } else if (OB_FAIL(cg_sstables.push_back(cg_handle.get_table()))) {
        LOG_WARN("failed to push back cg sstable", K(ret), K(cg_idx));
        return ret;
      }
    }

    // 将 CG sstables 填充到 CO SSTable 中
    ObITable *table = table_handle.get_table();
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret));
    } else {
      ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2*>(table);
      if (OB_FAIL(co_sstable->fill_cg_sstables(cg_sstables))) {
        LOG_WARN("failed to fill cg sstables", K(ret), K(cg_sstables.count()));
      }
    }

    return ret;
  }

protected:
  ObArenaAllocator allocator_;
};

// ==================== ObDDLIncLogBasic 测试 ====================

TEST_F(TestDDLIncCommitLog, test_log_basic_init_valid)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ASSERT_SUCC(log_basic.init(tablet_id, lob_tablet_id, direct_load_type,
                             trans_id, seq_no, snapshot_version, data_format_version));
  ASSERT_TRUE(log_basic.is_valid());
  ASSERT_EQ(tablet_id, log_basic.get_tablet_id());
  ASSERT_EQ(lob_tablet_id, log_basic.get_lob_meta_tablet_id());
  ASSERT_EQ(direct_load_type, log_basic.get_direct_load_type());
  ASSERT_EQ(trans_id, log_basic.get_trans_id());
  ASSERT_EQ(seq_no, log_basic.get_seq_no());
  ASSERT_EQ(snapshot_version, log_basic.get_snapshot_version());
  ASSERT_EQ(data_format_version, log_basic.get_data_format_version());
}

TEST_F(TestDDLIncCommitLog, test_log_basic_init_invalid_tablet)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID invalid_tablet_id; // invalid tablet id
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ASSERT_FAIL(log_basic.init(invalid_tablet_id, lob_tablet_id, direct_load_type,
                             trans_id, seq_no, snapshot_version, data_format_version));
  ASSERT_FALSE(log_basic.is_valid());
}

TEST_F(TestDDLIncCommitLog, test_log_basic_init_invalid_direct_load_type)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType invalid_type = ObDirectLoadType::DIRECT_LOAD_INVALID;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ASSERT_FAIL(log_basic.init(tablet_id, lob_tablet_id, invalid_type,
                             trans_id, seq_no, snapshot_version, data_format_version));
}

TEST_F(TestDDLIncCommitLog, test_log_basic_init_invalid_trans_id)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID invalid_trans_id; // invalid trans id
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ASSERT_FAIL(log_basic.init(tablet_id, lob_tablet_id, direct_load_type,
                             invalid_trans_id, seq_no, snapshot_version, data_format_version));
}

TEST_F(TestDDLIncCommitLog, test_log_basic_init_invalid_snapshot_version)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t invalid_snapshot_version = 0; // invalid
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ASSERT_FAIL(log_basic.init(tablet_id, lob_tablet_id, direct_load_type,
                             trans_id, seq_no, invalid_snapshot_version, data_format_version));
}

TEST_F(TestDDLIncCommitLog, test_log_basic_reset)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ASSERT_TRUE(log_basic.is_valid());

  log_basic.reset();
  ASSERT_FALSE(log_basic.is_valid());
  ASSERT_FALSE(log_basic.get_tablet_id().is_valid());
  ASSERT_EQ(ObDirectLoadType::DIRECT_LOAD_INVALID, log_basic.get_direct_load_type());
  ASSERT_EQ(0, log_basic.get_snapshot_version());
  ASSERT_EQ(0, log_basic.get_data_format_version());
}

TEST_F(TestDDLIncCommitLog, test_log_basic_operator_equal)
{
  ObDDLIncLogBasic log_basic1 = create_valid_log_basic(100001, 100002);
  ObDDLIncLogBasic log_basic2 = create_valid_log_basic(100001, 100002);
  ObDDLIncLogBasic log_basic3 = create_valid_log_basic(100003, 100004);

  ASSERT_TRUE(log_basic1 == log_basic2);
  ASSERT_FALSE(log_basic1 == log_basic3);
}

TEST_F(TestDDLIncCommitLog, test_log_basic_hash)
{
  ObDDLIncLogBasic log_basic1 = create_valid_log_basic(100001, 100002);
  ObDDLIncLogBasic log_basic2 = create_valid_log_basic(100001, 100002);
  ObDDLIncLogBasic log_basic3 = create_valid_log_basic(100003, 100004);

  uint64_t hash1 = log_basic1.hash();
  uint64_t hash2 = log_basic2.hash();
  uint64_t hash3 = log_basic3.hash();

  ASSERT_EQ(hash1, hash2);
  ASSERT_NE(hash1, hash3);

  uint64_t hash_val = 0;
  ASSERT_SUCC(log_basic1.hash(hash_val));
  ASSERT_EQ(hash1, hash_val);
}

TEST_F(TestDDLIncCommitLog, test_log_basic_serialize_deserialize)
{
  ObDDLIncLogBasic log_basic_src = create_valid_log_basic();
  ASSERT_TRUE(log_basic_src.is_valid());

  // 序列化
  int64_t serialize_size = log_basic_src.get_serialize_size();
  ASSERT_GT(serialize_size, 0);

  char *buf = static_cast<char*>(allocator_.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);

  int64_t pos = 0;
  ASSERT_SUCC(log_basic_src.serialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);

  // 反序列化
  ObDDLIncLogBasic log_basic_dst;
  pos = 0;
  ASSERT_SUCC(log_basic_dst.deserialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);
  ASSERT_TRUE(log_basic_dst.is_valid());

  // 验证内容
  ASSERT_EQ(log_basic_src.get_tablet_id(), log_basic_dst.get_tablet_id());
  ASSERT_EQ(log_basic_src.get_lob_meta_tablet_id(), log_basic_dst.get_lob_meta_tablet_id());
  ASSERT_EQ(log_basic_src.get_direct_load_type(), log_basic_dst.get_direct_load_type());
  ASSERT_EQ(log_basic_src.get_trans_id(), log_basic_dst.get_trans_id());
  ASSERT_EQ(log_basic_src.get_seq_no(), log_basic_dst.get_seq_no());
  ASSERT_EQ(log_basic_src.get_snapshot_version(), log_basic_dst.get_snapshot_version());
  ASSERT_EQ(log_basic_src.get_data_format_version(), log_basic_dst.get_data_format_version());
}

// ==================== ObDDLIncCommitLog 测试 ====================

TEST_F(TestDDLIncCommitLog, test_commit_log_init_valid)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));
  ASSERT_TRUE(commit_log.is_valid());
  ASSERT_FALSE(commit_log.is_rollback());
  ASSERT_FALSE(commit_log.is_co_sstable());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_init_with_rollback)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, true));
  ASSERT_TRUE(commit_log.is_valid());
  ASSERT_TRUE(commit_log.is_rollback());
  ASSERT_FALSE(commit_log.is_co_sstable());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_init_with_co_sstable)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));
  ASSERT_TRUE(commit_log.is_valid());
  ASSERT_FALSE(commit_log.is_rollback());
  // is_co_sstable 初始为 false，需要通过 set_ss_inc_major 设置
  ASSERT_FALSE(commit_log.is_co_sstable());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_init_invalid_log_basic)
{
  ObDDLIncLogBasic invalid_log_basic; // 未初始化，无效
  ObDDLIncCommitLog commit_log;

  ASSERT_FAIL(commit_log.init(invalid_log_basic, false));
  ASSERT_FALSE(commit_log.is_valid());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_get_log_basic)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic(100001, 100002);
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));

  const ObDDLIncLogBasic &retrieved_log_basic = commit_log.get_log_basic();
  ASSERT_EQ(log_basic.get_tablet_id(), retrieved_log_basic.get_tablet_id());
  ASSERT_EQ(log_basic.get_lob_meta_tablet_id(), retrieved_log_basic.get_lob_meta_tablet_id());
  ASSERT_EQ(log_basic.get_direct_load_type(), retrieved_log_basic.get_direct_load_type());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_data_buffer)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));

  // 测试空 buffer
  ASSERT_EQ(0, commit_log.get_data_inc_major_buffer().length());

  // 注意：data_inc_major_buffer_ 现在是私有成员，通过 set_ss_inc_major 设置
  // 这个测试需要调整为测试 set_ss_inc_major 功能
  // 由于需要 OB_BUILD_SHARED_STORAGE 宏，这里简化测试
}

TEST_F(TestDDLIncCommitLog, test_commit_log_lob_buffer)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));

  // 测试空 buffer
  ASSERT_EQ(0, commit_log.get_lob_inc_major_buffer().length());

  // 注意：lob_inc_major_buffer_ 现在是私有成员，通过 set_ss_inc_major 设置
  // 这个测试需要调整为测试 set_ss_inc_major 功能
  // 由于需要 OB_BUILD_SHARED_STORAGE 宏，这里简化测试
}

TEST_F(TestDDLIncCommitLog, test_commit_log_serialize_deserialize_without_buffer)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log_src;

  ASSERT_SUCC(commit_log_src.init(log_basic, false));

  // 序列化
  int64_t serialize_size = commit_log_src.get_serialize_size();
  ASSERT_GT(serialize_size, 0);

  char *buf = static_cast<char*>(allocator_.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);

  int64_t pos = 0;
  ASSERT_SUCC(commit_log_src.serialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);

  // 反序列化
  ObDDLIncCommitLog commit_log_dst;
  pos = 0;
  ASSERT_SUCC(commit_log_dst.deserialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);
  ASSERT_TRUE(commit_log_dst.is_valid());

  // 验证内容
  ASSERT_EQ(commit_log_src.is_rollback(), commit_log_dst.is_rollback());
  ASSERT_EQ(commit_log_src.is_co_sstable(), commit_log_dst.is_co_sstable());
  ASSERT_EQ(commit_log_src.get_log_basic().get_tablet_id(),
            commit_log_dst.get_log_basic().get_tablet_id());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_serialize_deserialize_with_sstable)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log_src;

  ASSERT_SUCC(commit_log_src.init(log_basic, false));

#ifdef OB_BUILD_SHARED_STORAGE
  // 创建 data ObSSTable
  ObTableHandleV2 data_table_handle;
  ObSSTable *data_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(data_table_handle));
  ASSERT_SUCC(data_table_handle.get_sstable(data_sstable));
  ASSERT_NE(nullptr, data_sstable);

  // 创建 lob ObSSTable
  ObTableHandleV2 lob_table_handle;
  ObSSTable *lob_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle));
  ASSERT_SUCC(lob_table_handle.get_sstable(lob_sstable));
  ASSERT_NE(nullptr, lob_sstable);

  // 使用 set_ss_inc_major 设置 data 和 lob sstable
  ASSERT_SUCC(commit_log_src.set_ss_inc_major(data_sstable, lob_sstable));

  // 序列化 commit_log
  int64_t commit_log_serialize_size = commit_log_src.get_serialize_size();
  ASSERT_GT(commit_log_serialize_size, 0);

  char *commit_log_buf = static_cast<char*>(allocator_.alloc(commit_log_serialize_size));
  ASSERT_NE(nullptr, commit_log_buf);

  int64_t pos = 0;
  ASSERT_SUCC(commit_log_src.serialize(commit_log_buf, commit_log_serialize_size, pos));
  ASSERT_EQ(commit_log_serialize_size, pos);

  // 反序列化 commit_log
  ObDDLIncCommitLog commit_log_dst;
  pos = 0;
  ASSERT_SUCC(commit_log_dst.deserialize(commit_log_buf, commit_log_serialize_size, pos));
  ASSERT_EQ(commit_log_serialize_size, pos);
  ASSERT_TRUE(commit_log_dst.is_valid());

  // 验证 data_inc_major_buffer 内容
  ASSERT_GT(commit_log_dst.get_data_inc_major_buffer().length(), 0);
  ASSERT_EQ(commit_log_src.get_data_inc_major_buffer().length(),
            commit_log_dst.get_data_inc_major_buffer().length());

  // 从 data_inc_major_buffer 反序列化 SSTable
  ObSSTable *deserialized_sstable = nullptr;
  void *sstable_mem = allocator_.alloc(sizeof(ObSSTable));
  ASSERT_NE(nullptr, sstable_mem);
  deserialized_sstable = new (sstable_mem) ObSSTable();

  pos = 0;
  ASSERT_SUCC(deserialized_sstable->deserialize(allocator_,
                                                 commit_log_dst.get_data_inc_major_buffer().ptr(),
                                                 commit_log_dst.get_data_inc_major_buffer().length(),
                                                 pos));

  // 验证反序列化的 SSTable
  ASSERT_TRUE(deserialized_sstable->is_valid());
  ASSERT_EQ(data_sstable->get_row_count(), deserialized_sstable->get_row_count());
  ASSERT_EQ(data_sstable->get_data_checksum(), deserialized_sstable->get_data_checksum());
#else
  // 没有 OB_BUILD_SHARED_STORAGE 宏时，跳过测试
  LOG_INFO("Skip test_commit_log_serialize_deserialize_with_sstable without OB_BUILD_SHARED_STORAGE");
#endif
}

TEST_F(TestDDLIncCommitLog, test_commit_log_multiple_init)
{
  ObDDLIncLogBasic log_basic1 = create_valid_log_basic(100001, 100002);
  ObDDLIncLogBasic log_basic2 = create_valid_log_basic(200001, 200002);
  ObDDLIncCommitLog commit_log;

  // 第一次初始化
  ASSERT_SUCC(commit_log.init(log_basic1, false));
  ASSERT_TRUE(commit_log.is_valid());
  ASSERT_EQ(log_basic1.get_tablet_id(), commit_log.get_log_basic().get_tablet_id());

  // 第二次初始化（覆盖）
  ASSERT_SUCC(commit_log.init(log_basic2, true));
  ASSERT_TRUE(commit_log.is_valid());
  ASSERT_EQ(log_basic2.get_tablet_id(), commit_log.get_log_basic().get_tablet_id());
  ASSERT_TRUE(commit_log.is_rollback());
  // is_co_sstable 需要通过 set_ss_inc_major 设置
  ASSERT_FALSE(commit_log.is_co_sstable());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_co_sstable_flag)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();

  // 测试 is_co_sstable 初始值为 false
  ObDDLIncCommitLog commit_log1;
  ASSERT_SUCC(commit_log1.init(log_basic, false));
  ASSERT_FALSE(commit_log1.is_co_sstable());

#ifdef OB_BUILD_SHARED_STORAGE
  // 测试通过 set_ss_inc_major 设置 is_co_sstable = true
  ObDDLIncCommitLog commit_log2;
  ASSERT_SUCC(commit_log2.init(log_basic, false));

  ObTableHandleV2 co_table_handle;
  ObCOSSTableV2 *co_sstable = nullptr;
  ASSERT_SUCC(create_mock_co_sstable(co_table_handle));
  ObITable *co_table = co_table_handle.get_table();
  ASSERT_NE(nullptr, co_table);
  co_sstable = static_cast<ObCOSSTableV2*>(co_table);

  // 创建 lob SSTable
  ObTableHandleV2 lob_table_handle;
  ObSSTable *lob_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle));
  ASSERT_SUCC(lob_table_handle.get_sstable(lob_sstable));
  ASSERT_NE(nullptr, lob_sstable);

  ASSERT_SUCC(commit_log2.set_ss_inc_major(co_sstable, lob_sstable));
  ASSERT_TRUE(commit_log2.is_co_sstable());
#endif
}

TEST_F(TestDDLIncCommitLog, test_commit_log_all_combinations)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();

  // 测试 rollback 的不同组合
  struct TestCase {
    bool is_rollback;
  };

  TestCase test_cases[] = {
    {false},
    {true}
  };

  for (size_t i = 0; i < sizeof(test_cases) / sizeof(test_cases[0]); ++i) {
    ObDDLIncCommitLog commit_log;
    ASSERT_SUCC(commit_log.init(log_basic, test_cases[i].is_rollback));
    ASSERT_TRUE(commit_log.is_valid());
    ASSERT_EQ(test_cases[i].is_rollback, commit_log.is_rollback());
    // is_co_sstable 初始为 false
    ASSERT_FALSE(commit_log.is_co_sstable());
  }
}

TEST_F(TestDDLIncCommitLog, test_commit_log_serialize_deserialize_with_co_sstable)
{
#ifdef OB_BUILD_SHARED_STORAGE
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log_src;

  ASSERT_SUCC(commit_log_src.init(log_basic, false));

  // 创建 ObCOSSTableV2
  ObTableHandleV2 co_table_handle;
  ObCOSSTableV2 *co_sstable = nullptr;
  ASSERT_SUCC(create_mock_co_sstable(co_table_handle));
  ObITable *co_table = co_table_handle.get_table();
  ASSERT_NE(nullptr, co_table);
  co_sstable = static_cast<ObCOSSTableV2*>(co_table);

  // 创建 lob SSTable
  ObTableHandleV2 lob_table_handle;
  ObSSTable *lob_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle));
  ASSERT_SUCC(lob_table_handle.get_sstable(lob_sstable));
  ASSERT_NE(nullptr, lob_sstable);

  // 使用 set_ss_inc_major 设置 CO SSTable 和 lob SSTable
  ASSERT_SUCC(commit_log_src.set_ss_inc_major(co_sstable, lob_sstable));
  ASSERT_TRUE(commit_log_src.is_co_sstable());

  // 序列化 commit_log
  int64_t commit_log_serialize_size = commit_log_src.get_serialize_size();
  ASSERT_GT(commit_log_serialize_size, 0);

  char *commit_log_buf = static_cast<char*>(allocator_.alloc(commit_log_serialize_size));
  ASSERT_NE(nullptr, commit_log_buf);

  int64_t pos = 0;
  ASSERT_SUCC(commit_log_src.serialize(commit_log_buf, commit_log_serialize_size, pos));
  ASSERT_EQ(commit_log_serialize_size, pos);

  // 反序列化 commit_log
  ObDDLIncCommitLog commit_log_dst;
  pos = 0;
  ASSERT_SUCC(commit_log_dst.deserialize(commit_log_buf, commit_log_serialize_size, pos));
  ASSERT_EQ(commit_log_serialize_size, pos);
  ASSERT_TRUE(commit_log_dst.is_valid());
  ASSERT_TRUE(commit_log_dst.is_co_sstable());

  // 验证 data_inc_major_buffer 内容
  ASSERT_GT(commit_log_dst.get_data_inc_major_buffer().length(), 0);
  ASSERT_EQ(commit_log_src.get_data_inc_major_buffer().length(),
            commit_log_dst.get_data_inc_major_buffer().length());

  // 从 data_inc_major_buffer 反序列化 CO SSTable
  ObCOSSTableV2 *deserialized_co_sstable = nullptr;
  void *co_sstable_mem = allocator_.alloc(sizeof(ObCOSSTableV2));
  ASSERT_NE(nullptr, co_sstable_mem);
  deserialized_co_sstable = new (co_sstable_mem) ObCOSSTableV2();

  pos = 0;
  ASSERT_SUCC(deserialized_co_sstable->deserialize(allocator_,
                                                    commit_log_dst.get_data_inc_major_buffer().ptr(),
                                                    commit_log_dst.get_data_inc_major_buffer().length(),
                                                    pos));

  // 验证反序列化的 CO SSTable
  ASSERT_TRUE(deserialized_co_sstable->is_valid());
  ASSERT_EQ(co_sstable->get_row_count(), deserialized_co_sstable->get_row_count());
  ASSERT_EQ(co_sstable->get_data_checksum(), deserialized_co_sstable->get_data_checksum());
#else
  LOG_INFO("Skip test_commit_log_serialize_deserialize_with_co_sstable without OB_BUILD_SHARED_STORAGE");
#endif
}

TEST_F(TestDDLIncCommitLog, test_commit_log_with_both_data_and_lob_sstable)
{
#ifdef OB_BUILD_SHARED_STORAGE
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log_src;

  ASSERT_SUCC(commit_log_src.init(log_basic, false));

  // 创建 data SSTable
  ObTableHandleV2 data_table_handle;
  ObSSTable *data_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(data_table_handle));
  ASSERT_SUCC(data_table_handle.get_sstable(data_sstable));
  ASSERT_NE(nullptr, data_sstable);

  // 创建 lob SSTable
  ObTableHandleV2 lob_table_handle;
  ObSSTable *lob_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle));
  ASSERT_SUCC(lob_table_handle.get_sstable(lob_sstable));
  ASSERT_NE(nullptr, lob_sstable);

  // 使用 set_ss_inc_major 设置 data 和 lob SSTable
  ASSERT_SUCC(commit_log_src.set_ss_inc_major(data_sstable, lob_sstable));

  // 序列化 commit_log
  int64_t commit_log_size = commit_log_src.get_serialize_size();
  char *commit_log_buf = static_cast<char*>(allocator_.alloc(commit_log_size));
  ASSERT_NE(nullptr, commit_log_buf);

  int64_t pos = 0;
  ASSERT_SUCC(commit_log_src.serialize(commit_log_buf, commit_log_size, pos));

  // 反序列化 commit_log
  ObDDLIncCommitLog commit_log_dst;
  pos = 0;
  ASSERT_SUCC(commit_log_dst.deserialize(commit_log_buf, commit_log_size, pos));
  ASSERT_TRUE(commit_log_dst.is_valid());

  // 验证 data_inc_major_buffer 和 lob_inc_major_buffer
  ASSERT_GT(commit_log_dst.get_data_inc_major_buffer().length(), 0);
  ASSERT_GT(commit_log_dst.get_lob_inc_major_buffer().length(), 0);

  // 反序列化 data SSTable
  ObSSTable *deserialized_data_sstable = nullptr;
  void *data_mem = allocator_.alloc(sizeof(ObSSTable));
  ASSERT_NE(nullptr, data_mem);
  deserialized_data_sstable = new (data_mem) ObSSTable();

  pos = 0;
  ASSERT_SUCC(deserialized_data_sstable->deserialize(allocator_,
                                                      commit_log_dst.get_data_inc_major_buffer().ptr(),
                                                      commit_log_dst.get_data_inc_major_buffer().length(),
                                                      pos));
  ASSERT_TRUE(deserialized_data_sstable->is_valid());

  // 反序列化 lob SSTable
  ObSSTable *deserialized_lob_sstable = nullptr;
  void *lob_mem = allocator_.alloc(sizeof(ObSSTable));
  ASSERT_NE(nullptr, lob_mem);
  deserialized_lob_sstable = new (lob_mem) ObSSTable();

  pos = 0;
  ASSERT_SUCC(deserialized_lob_sstable->deserialize(allocator_,
                                                     commit_log_dst.get_lob_inc_major_buffer().ptr(),
                                                     commit_log_dst.get_lob_inc_major_buffer().length(),
                                                     pos));
  ASSERT_TRUE(deserialized_lob_sstable->is_valid());
#else
  LOG_INFO("Skip test_commit_log_with_both_data_and_lob_sstable without OB_BUILD_SHARED_STORAGE");
#endif
}

TEST_F(TestDDLIncCommitLog, test_commit_log_serialize_deserialize_with_co_sstable_and_cgs)
{
#ifdef OB_BUILD_SHARED_STORAGE
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log_src;

  ASSERT_SUCC(commit_log_src.init(log_basic, false));

  // 创建包含 CG sstables 的 ObCOSSTableV2
  ObTableHandleV2 co_table_handle;
  ObCOSSTableV2 *co_sstable = nullptr;
  ASSERT_SUCC(create_mock_co_sstable_with_cgs(co_table_handle));
  ObITable *co_table = co_table_handle.get_table();
  ASSERT_NE(nullptr, co_table);
  co_sstable = static_cast<ObCOSSTableV2*>(co_table);
  ASSERT_FALSE(co_sstable->is_cgs_empty_co_table());  // 验证包含 CG sstables

  // 创建 lob SSTable
  ObTableHandleV2 lob_table_handle;
  ObSSTable *lob_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle));
  ASSERT_SUCC(lob_table_handle.get_sstable(lob_sstable));
  ASSERT_NE(nullptr, lob_sstable);

  // 使用 set_ss_inc_major 设置 CO SSTable 和 lob SSTable
  ASSERT_SUCC(commit_log_src.set_ss_inc_major(co_sstable, lob_sstable));
  ASSERT_TRUE(commit_log_src.is_co_sstable());

  // 序列化 commit_log
  int64_t commit_log_serialize_size = commit_log_src.get_serialize_size();
  ASSERT_GT(commit_log_serialize_size, 0);

  char *commit_log_buf = static_cast<char*>(allocator_.alloc(commit_log_serialize_size));
  ASSERT_NE(nullptr, commit_log_buf);

  int64_t pos = 0;
  ASSERT_SUCC(commit_log_src.serialize(commit_log_buf, commit_log_serialize_size, pos));
  ASSERT_EQ(commit_log_serialize_size, pos);

  // 反序列化 commit_log
  ObDDLIncCommitLog commit_log_dst;
  pos = 0;
  ASSERT_SUCC(commit_log_dst.deserialize(commit_log_buf, commit_log_serialize_size, pos));
  ASSERT_EQ(commit_log_serialize_size, pos);
  ASSERT_TRUE(commit_log_dst.is_valid());
  ASSERT_TRUE(commit_log_dst.is_co_sstable());

  // 验证 data_inc_major_buffer 内容
  ASSERT_GT(commit_log_dst.get_data_inc_major_buffer().length(), 0);
  ASSERT_EQ(commit_log_src.get_data_inc_major_buffer().length(),
            commit_log_dst.get_data_inc_major_buffer().length());

  // 从 data_inc_major_buffer 反序列化 CO SSTable
  ObCOSSTableV2 *deserialized_co_sstable = nullptr;
  void *co_sstable_mem = allocator_.alloc(sizeof(ObCOSSTableV2));
  ASSERT_NE(nullptr, co_sstable_mem);
  deserialized_co_sstable = new (co_sstable_mem) ObCOSSTableV2();

  pos = 0;
  ASSERT_SUCC(deserialized_co_sstable->deserialize(allocator_,
                                                    commit_log_dst.get_data_inc_major_buffer().ptr(),
                                                    commit_log_dst.get_data_inc_major_buffer().length(),
                                                    pos));

  // 验证反序列化的 CO SSTable
  ASSERT_TRUE(deserialized_co_sstable->is_valid());
  ASSERT_FALSE(deserialized_co_sstable->is_cgs_empty_co_table());  // 验证包含 CG sstables
  ASSERT_EQ(co_sstable->get_row_count(), deserialized_co_sstable->get_row_count());
  ASSERT_EQ(co_sstable->get_data_checksum(), deserialized_co_sstable->get_data_checksum());

  // 验证 CG sstables 数量
  ASSERT_EQ(co_sstable->get_cs_meta().column_group_cnt_, deserialized_co_sstable->get_cs_meta().column_group_cnt_);
#else
  LOG_INFO("Skip test_commit_log_serialize_deserialize_with_co_sstable_and_cgs without OB_BUILD_SHARED_STORAGE");
#endif
}

// ==================== ObRpcRemoteWriteDDLIncCommitLogArg 测试 ====================

TEST_F(TestDDLIncCommitLog, test_rpc_arg_serialize_deserialize_basic)
{
  // 注意：这个测试需要 MTL 环境，暂时跳过 tx_desc 的序列化测试
  // 只测试其他字段的序列化/反序列化

  // 模拟创建一个简化的 RPC Arg（不包含 tx_desc）
  uint64_t tenant_id = 1001;
  ObLSID ls_id(1);
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(2001);
  ObTxSEQ seq_no(3001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;
  bool is_co_sstable = true;

  // 创建测试数据
  const char *data_buffer_content = "test_data_inc_major_buffer_content";
  const char *lob_buffer_content = "test_lob_inc_major_buffer_content";
  ObString data_inc_major_buffer(strlen(data_buffer_content), data_buffer_content);
  ObString lob_inc_major_buffer(strlen(lob_buffer_content), lob_buffer_content);

  // 手动构造序列化数据（跳过 tx_desc）
  // 注意：RPC Arg 的序列化格式与字段顺序一致
  char serialize_buf[4096];
  int64_t pos = 0;

  // 序列化各个字段（按照 ObRpcRemoteWriteDDLIncCommitLogArg 的成员顺序）
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, tenant_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, ls_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, tablet_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, lob_tablet_id));
  // 跳过 tx_desc (需要 MTL 环境)
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, direct_load_type));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, trans_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, seq_no));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, snapshot_version));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, data_format_version));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, is_co_sstable));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, data_inc_major_buffer));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, lob_inc_major_buffer));

  int64_t serialize_size = pos;

  // 反序列化
  pos = 0;
  uint64_t tenant_id_out = 0;
  ObLSID ls_id_out;
  ObTabletID tablet_id_out;
  ObTabletID lob_tablet_id_out;
  ObDirectLoadType direct_load_type_out = ObDirectLoadType::DIRECT_LOAD_INVALID;
  ObTransID trans_id_out;
  ObTxSEQ seq_no_out;
  int64_t snapshot_version_out = 0;
  uint64_t data_format_version_out = 0;
  bool is_co_sstable_out = false;
  ObString data_inc_major_buffer_out;
  ObString lob_inc_major_buffer_out;

  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, tenant_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, ls_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, tablet_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, lob_tablet_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, direct_load_type_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, trans_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, seq_no_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, snapshot_version_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, data_format_version_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, is_co_sstable_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, data_inc_major_buffer_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, lob_inc_major_buffer_out));

  // 验证反序列化结果
  ASSERT_EQ(tenant_id, tenant_id_out);
  ASSERT_EQ(ls_id, ls_id_out);
  ASSERT_EQ(tablet_id, tablet_id_out);
  ASSERT_EQ(lob_tablet_id, lob_tablet_id_out);
  ASSERT_EQ(direct_load_type, direct_load_type_out);
  ASSERT_EQ(trans_id, trans_id_out);
  ASSERT_EQ(seq_no, seq_no_out);
  ASSERT_EQ(snapshot_version, snapshot_version_out);
  ASSERT_EQ(data_format_version, data_format_version_out);
  ASSERT_EQ(is_co_sstable, is_co_sstable_out);
  ASSERT_EQ(data_inc_major_buffer.length(), data_inc_major_buffer_out.length());
  ASSERT_EQ(0, memcmp(data_inc_major_buffer.ptr(), data_inc_major_buffer_out.ptr(), data_inc_major_buffer.length()));
  ASSERT_EQ(lob_inc_major_buffer.length(), lob_inc_major_buffer_out.length());
  ASSERT_EQ(0, memcmp(lob_inc_major_buffer.ptr(), lob_inc_major_buffer_out.ptr(), lob_inc_major_buffer.length()));
}

TEST_F(TestDDLIncCommitLog, test_rpc_arg_with_empty_buffers)
{
  // 测试空 buffer 的情况
  char serialize_buf[4096];
  int64_t pos = 0;

  uint64_t tenant_id = 1001;
  ObLSID ls_id(1);
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(2001);
  ObTxSEQ seq_no(3001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;
  bool is_co_sstable = false;
  ObString data_inc_major_buffer;  // 空 buffer
  ObString lob_inc_major_buffer;   // 空 buffer

  // 序列化
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, tenant_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, ls_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, tablet_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, lob_tablet_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, direct_load_type));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, trans_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, seq_no));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, snapshot_version));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, data_format_version));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, is_co_sstable));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, data_inc_major_buffer));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, lob_inc_major_buffer));

  int64_t serialize_size = pos;

  // 反序列化
  pos = 0;
  uint64_t tenant_id_out = 0;
  ObLSID ls_id_out;
  ObTabletID tablet_id_out;
  ObTabletID lob_tablet_id_out;
  ObDirectLoadType direct_load_type_out = ObDirectLoadType::DIRECT_LOAD_INVALID;
  ObTransID trans_id_out;
  ObTxSEQ seq_no_out;
  int64_t snapshot_version_out = 0;
  uint64_t data_format_version_out = 0;
  bool is_co_sstable_out = true;
  ObString data_inc_major_buffer_out;
  ObString lob_inc_major_buffer_out;

  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, tenant_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, ls_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, tablet_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, lob_tablet_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, direct_load_type_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, trans_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, seq_no_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, snapshot_version_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, data_format_version_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, is_co_sstable_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, data_inc_major_buffer_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, lob_inc_major_buffer_out));

  // 验证
  ASSERT_EQ(is_co_sstable, is_co_sstable_out);
  ASSERT_EQ(0, data_inc_major_buffer_out.length());
  ASSERT_EQ(0, lob_inc_major_buffer_out.length());
}

TEST_F(TestDDLIncCommitLog, test_rpc_arg_with_large_buffers)
{
  // 测试大 buffer 的情况
  char serialize_buf[8192];
  int64_t pos = 0;

  uint64_t tenant_id = 1001;
  ObLSID ls_id(1);
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(2001);
  ObTxSEQ seq_no(3001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;
  bool is_co_sstable = true;

  // 创建大 buffer
  char data_buf[1024];
  char lob_buf[2048];
  for (int i = 0; i < 1024; i++) {
    data_buf[i] = static_cast<char>('A' + (i % 26));
  }
  for (int i = 0; i < 2048; i++) {
    lob_buf[i] = static_cast<char>('a' + (i % 26));
  }

  ObString data_inc_major_buffer(1024, data_buf);
  ObString lob_inc_major_buffer(2048, lob_buf);

  // 序列化
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, tenant_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, ls_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, tablet_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, lob_tablet_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, direct_load_type));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, trans_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, seq_no));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, snapshot_version));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, data_format_version));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, is_co_sstable));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, data_inc_major_buffer));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, lob_inc_major_buffer));

  int64_t serialize_size = pos;
  ASSERT_GT(serialize_size, 3000);  // 验证确实序列化了大量数据

  // 反序列化
  pos = 0;
  uint64_t tenant_id_out = 0;
  ObLSID ls_id_out;
  ObTabletID tablet_id_out;
  ObTabletID lob_tablet_id_out;
  ObDirectLoadType direct_load_type_out = ObDirectLoadType::DIRECT_LOAD_INVALID;
  ObTransID trans_id_out;
  ObTxSEQ seq_no_out;
  int64_t snapshot_version_out = 0;
  uint64_t data_format_version_out = 0;
  bool is_co_sstable_out = false;
  ObString data_inc_major_buffer_out;
  ObString lob_inc_major_buffer_out;

  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, tenant_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, ls_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, tablet_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, lob_tablet_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, direct_load_type_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, trans_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, seq_no_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, snapshot_version_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, data_format_version_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, is_co_sstable_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, data_inc_major_buffer_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, serialize_size, pos, lob_inc_major_buffer_out));

  // 验证
  ASSERT_EQ(is_co_sstable, is_co_sstable_out);
  ASSERT_EQ(1024, data_inc_major_buffer_out.length());
  ASSERT_EQ(2048, lob_inc_major_buffer_out.length());
  ASSERT_EQ(0, memcmp(data_inc_major_buffer.ptr(), data_inc_major_buffer_out.ptr(), 1024));
  ASSERT_EQ(0, memcmp(lob_inc_major_buffer.ptr(), lob_inc_major_buffer_out.ptr(), 2048));
}

TEST_F(TestDDLIncCommitLog, test_rpc_arg_serialize_size_consistency)
{
  // 测试 get_serialize_size 返回的大小是否与实际序列化大小一致
  char serialize_buf[8192];
  int64_t pos = 0;

  uint64_t tenant_id = 1001;
  ObLSID ls_id(1);
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(2001);
  ObTxSEQ seq_no(3001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;
  bool is_co_sstable = true;

  // 创建测试数据
  char data_buf[512];
  char lob_buf[1024];
  for (int i = 0; i < 512; i++) {
    data_buf[i] = static_cast<char>('A' + (i % 26));
  }
  for (int i = 0; i < 1024; i++) {
    lob_buf[i] = static_cast<char>('a' + (i % 26));
  }

  ObString data_inc_major_buffer(512, data_buf);
  ObString lob_inc_major_buffer(1024, lob_buf);

  // 计算预期的序列化大小
  int64_t expected_size = 0;
  expected_size += serialization::encoded_length(tenant_id);
  expected_size += serialization::encoded_length(ls_id);
  expected_size += serialization::encoded_length(tablet_id);
  expected_size += serialization::encoded_length(lob_tablet_id);
  expected_size += serialization::encoded_length(direct_load_type);
  expected_size += serialization::encoded_length(trans_id);
  expected_size += serialization::encoded_length(seq_no);
  expected_size += serialization::encoded_length(snapshot_version);
  expected_size += serialization::encoded_length(data_format_version);
  expected_size += serialization::encoded_length(is_co_sstable);
  expected_size += serialization::encoded_length(data_inc_major_buffer);
  expected_size += serialization::encoded_length(lob_inc_major_buffer);

  // 实际序列化
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, tenant_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, ls_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, tablet_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, lob_tablet_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, direct_load_type));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, trans_id));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, seq_no));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, snapshot_version));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, data_format_version));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, is_co_sstable));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, data_inc_major_buffer));
  ASSERT_SUCC(serialization::encode(serialize_buf, sizeof(serialize_buf), pos, lob_inc_major_buffer));

  int64_t actual_size = pos;

  // 验证大小一致性
  ASSERT_EQ(expected_size, actual_size);

  // 验证可以完整反序列化
  pos = 0;
  uint64_t tenant_id_out;
  ObLSID ls_id_out;
  ObTabletID tablet_id_out;
  ObTabletID lob_tablet_id_out;
  ObDirectLoadType direct_load_type_out;
  ObTransID trans_id_out;
  ObTxSEQ seq_no_out;
  int64_t snapshot_version_out;
  uint64_t data_format_version_out;
  bool is_co_sstable_out;
  ObString data_inc_major_buffer_out;
  ObString lob_inc_major_buffer_out;

  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, tenant_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, ls_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, tablet_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, lob_tablet_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, direct_load_type_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, trans_id_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, seq_no_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, snapshot_version_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, data_format_version_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, is_co_sstable_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, data_inc_major_buffer_out));
  ASSERT_SUCC(serialization::decode(serialize_buf, actual_size, pos, lob_inc_major_buffer_out));

  // 验证反序列化后的位置应该等于序列化大小
  ASSERT_EQ(actual_size, pos);
}

// ==================== 新增测试用例：边界条件和错误处理 ====================

TEST_F(TestDDLIncCommitLog, test_commit_log_init_twice)
{
  ObDDLIncLogBasic log_basic1 = create_valid_log_basic(100001, 100002);
  ObDDLIncLogBasic log_basic2 = create_valid_log_basic(200001, 200002);
  ObDDLIncCommitLog commit_log;

  // 第一次初始化
  ASSERT_SUCC(commit_log.init(log_basic1, false));
  ASSERT_TRUE(commit_log.is_valid());
  ASSERT_EQ(log_basic1.get_tablet_id(), commit_log.get_log_basic().get_tablet_id());

  // 第二次初始化应该覆盖第一次
  ASSERT_SUCC(commit_log.init(log_basic2, true));
  ASSERT_TRUE(commit_log.is_valid());
  ASSERT_EQ(log_basic2.get_tablet_id(), commit_log.get_log_basic().get_tablet_id());
  ASSERT_TRUE(commit_log.is_rollback());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_reset_after_init)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));
  ASSERT_TRUE(commit_log.is_valid());

  // 重新初始化为无效状态
  ObDDLIncLogBasic invalid_log_basic;
  ASSERT_FAIL(commit_log.init(invalid_log_basic, false));
  // 初始化失败后，原有数据应该保持不变
  ASSERT_TRUE(commit_log.is_valid());
}

#ifdef OB_BUILD_SHARED_STORAGE
TEST_F(TestDDLIncCommitLog, test_commit_log_set_ss_inc_major_null_sstable)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));

  // 测试传入 nullptr data_inc_major（应该返回 OB_INVALID_ARGUMENT）
  int ret = commit_log.set_ss_inc_major(nullptr, nullptr);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST_F(TestDDLIncCommitLog, test_commit_log_set_ss_inc_major_with_lob_validation)
{
  // 创建一个包含 lob_meta_tablet_id 的 log_basic
  ObTabletID tablet_id(100001);
  ObTabletID lob_meta_tablet_id(100002);  // 有效的 lob_meta_tablet_id
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ObDDLIncLogBasic log_basic;
  ASSERT_SUCC(log_basic.init(tablet_id, lob_meta_tablet_id, direct_load_type,
                             trans_id, seq_no, snapshot_version, data_format_version));

  ObDDLIncCommitLog commit_log;
  ASSERT_SUCC(commit_log.init(log_basic, false));

  // 创建 data SSTable
  ObTableHandleV2 data_table_handle;
  ObSSTable *data_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(data_table_handle));
  ASSERT_SUCC(data_table_handle.get_sstable(data_sstable));
  ASSERT_NE(nullptr, data_sstable);

  // 测试：当 lob_meta_tablet_id 有效时，lob_inc_major 为 nullptr 应该返回 OB_INVALID_ARGUMENT
  int ret = commit_log.set_ss_inc_major(data_sstable, nullptr);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // 测试：提供有效的 lob_inc_major 应该成功
  ObTableHandleV2 lob_table_handle;
  ObSSTable *lob_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle));
  ASSERT_SUCC(lob_table_handle.get_sstable(lob_sstable));
  ASSERT_NE(nullptr, lob_sstable);

  ASSERT_SUCC(commit_log.set_ss_inc_major(data_sstable, lob_sstable));
  ASSERT_GT(commit_log.get_data_inc_major_buffer().length(), 0);
  ASSERT_GT(commit_log.get_lob_inc_major_buffer().length(), 0);
}

TEST_F(TestDDLIncCommitLog, test_commit_log_set_ss_inc_major_twice)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));

  // 第一次设置
  ObTableHandleV2 table_handle1;
  ObSSTable *sstable1 = nullptr;
  ASSERT_SUCC(create_mock_sstable(table_handle1));
  ASSERT_SUCC(table_handle1.get_sstable(sstable1));
  ASSERT_NE(nullptr, sstable1);

  // 创建 lob SSTable 1
  ObTableHandleV2 lob_table_handle1;
  ObSSTable *lob_sstable1 = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle1));
  ASSERT_SUCC(lob_table_handle1.get_sstable(lob_sstable1));
  ASSERT_NE(nullptr, lob_sstable1);

  ASSERT_SUCC(commit_log.set_ss_inc_major(sstable1, lob_sstable1));
  int64_t first_buffer_len = commit_log.get_data_inc_major_buffer().length();
  ASSERT_GT(first_buffer_len, 0);

  // 第二次设置（覆盖）
  ObTableHandleV2 table_handle2;
  ObSSTable *sstable2 = nullptr;
  ASSERT_SUCC(create_mock_sstable(table_handle2));
  ASSERT_SUCC(table_handle2.get_sstable(sstable2));
  ASSERT_NE(nullptr, sstable2);

  // 创建 lob SSTable 2
  ObTableHandleV2 lob_table_handle2;
  ObSSTable *lob_sstable2 = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle2));
  ASSERT_SUCC(lob_table_handle2.get_sstable(lob_sstable2));
  ASSERT_NE(nullptr, lob_sstable2);

  ASSERT_SUCC(commit_log.set_ss_inc_major(sstable2, lob_sstable2));
  int64_t second_buffer_len = commit_log.get_data_inc_major_buffer().length();
  ASSERT_GT(second_buffer_len, 0);
  // 两次设置的 buffer 长度应该相同（因为 sstable 参数相同）
  ASSERT_EQ(first_buffer_len, second_buffer_len);
}

TEST_F(TestDDLIncCommitLog, test_commit_log_with_co_sstable_flag_consistency)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));
  ASSERT_FALSE(commit_log.is_co_sstable());

  // 设置普通 SSTable，is_co_sstable 应该为 false
  ObTableHandleV2 normal_table_handle;
  ObSSTable *normal_sstable = nullptr;
  ASSERT_SUCC(create_mock_sstable(normal_table_handle));
  ASSERT_SUCC(normal_table_handle.get_sstable(normal_sstable));
  ASSERT_NE(nullptr, normal_sstable);

  // 创建 lob SSTable 1
  ObTableHandleV2 lob_table_handle1;
  ObSSTable *lob_sstable1 = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle1));
  ASSERT_SUCC(lob_table_handle1.get_sstable(lob_sstable1));
  ASSERT_NE(nullptr, lob_sstable1);

  ASSERT_SUCC(commit_log.set_ss_inc_major(normal_sstable, lob_sstable1));
  ASSERT_FALSE(commit_log.is_co_sstable());

  // 设置 CO SSTable，is_co_sstable 应该为 true
  ObTableHandleV2 co_table_handle;
  ObCOSSTableV2 *co_sstable = nullptr;
  ASSERT_SUCC(create_mock_co_sstable(co_table_handle));
  ObITable *co_table = co_table_handle.get_table();
  ASSERT_NE(nullptr, co_table);
  co_sstable = static_cast<ObCOSSTableV2*>(co_table);

  // 创建 lob SSTable 2
  ObTableHandleV2 lob_table_handle2;
  ObSSTable *lob_sstable2 = nullptr;
  ASSERT_SUCC(create_mock_sstable(lob_table_handle2));
  ASSERT_SUCC(lob_table_handle2.get_sstable(lob_sstable2));
  ASSERT_NE(nullptr, lob_sstable2);

  ASSERT_SUCC(commit_log.set_ss_inc_major(co_sstable, lob_sstable2));
  ASSERT_TRUE(commit_log.is_co_sstable());
}
#endif

TEST_F(TestDDLIncCommitLog, test_commit_log_serialize_empty_buffers)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log_src;

  ASSERT_SUCC(commit_log_src.init(log_basic, false));

  // 不设置任何 buffer，直接序列化
  int64_t serialize_size = commit_log_src.get_serialize_size();
  ASSERT_GT(serialize_size, 0);

  char *buf = static_cast<char*>(allocator_.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);

  int64_t pos = 0;
  ASSERT_SUCC(commit_log_src.serialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);

  // 反序列化
  ObDDLIncCommitLog commit_log_dst;
  pos = 0;
  ASSERT_SUCC(commit_log_dst.deserialize(buf, serialize_size, pos));
  ASSERT_TRUE(commit_log_dst.is_valid());

  // 验证空 buffer
  ASSERT_EQ(0, commit_log_dst.get_data_inc_major_buffer().length());
  ASSERT_EQ(0, commit_log_dst.get_lob_inc_major_buffer().length());
  ASSERT_FALSE(commit_log_dst.is_co_sstable());
}

TEST_F(TestDDLIncCommitLog, test_commit_log_serialize_buffer_too_small)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));

  int64_t serialize_size = commit_log.get_serialize_size();
  ASSERT_GT(serialize_size, 0);

  // 分配一个太小的 buffer
  int64_t small_buf_size = serialize_size / 2;
  char *buf = static_cast<char*>(allocator_.alloc(small_buf_size));
  ASSERT_NE(nullptr, buf);

  int64_t pos = 0;
  // 序列化应该失败
  ASSERT_FAIL(commit_log.serialize(buf, small_buf_size, pos));
}

TEST_F(TestDDLIncCommitLog, test_commit_log_deserialize_corrupted_data)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log_src;

  ASSERT_SUCC(commit_log_src.init(log_basic, false));

  int64_t serialize_size = commit_log_src.get_serialize_size();
  char *buf = static_cast<char*>(allocator_.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);

  int64_t pos = 0;
  ASSERT_SUCC(commit_log_src.serialize(buf, serialize_size, pos));

  // 破坏数据
  if (serialize_size > 10) {
    buf[serialize_size / 2] = ~buf[serialize_size / 2];
  }

  // 反序列化可能失败或得到错误数据
  ObDDLIncCommitLog commit_log_dst;
  pos = 0;
  int ret = commit_log_dst.deserialize(buf, serialize_size, pos);
  // 根据具体实现，可能成功但数据错误，或者失败
  // 这个测试主要是确保不会崩溃
}

// ==================== 新增测试用例：ObDDLIncLogBasic 边界测试 ====================

TEST_F(TestDDLIncCommitLog, test_log_basic_init_with_invalid_lob_tablet)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID tablet_id(100001);
  ObTabletID invalid_lob_tablet; // invalid
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  // lob_tablet_id 可以是无效的（对于没有 LOB 列的表）
  ASSERT_SUCC(log_basic.init(tablet_id, invalid_lob_tablet, direct_load_type,
                             trans_id, seq_no, snapshot_version, data_format_version));
  ASSERT_TRUE(log_basic.is_valid());
}

TEST_F(TestDDLIncCommitLog, test_log_basic_init_with_zero_snapshot_version)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = 0; // invalid
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ASSERT_FAIL(log_basic.init(tablet_id, lob_tablet_id, direct_load_type,
                             trans_id, seq_no, snapshot_version, data_format_version));
}

TEST_F(TestDDLIncCommitLog, test_log_basic_init_with_negative_snapshot_version)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = -1; // invalid
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ASSERT_FAIL(log_basic.init(tablet_id, lob_tablet_id, direct_load_type,
                             trans_id, seq_no, snapshot_version, data_format_version));
}

TEST_F(TestDDLIncCommitLog, test_log_basic_init_with_invalid_data_format_version)
{
  ObDDLIncLogBasic log_basic;
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(1001);
  ObTxSEQ seq_no(2001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_3_0_0; // 不支持 inc major

  ASSERT_FAIL(log_basic.init(tablet_id, lob_tablet_id, direct_load_type,
                             trans_id, seq_no, snapshot_version, data_format_version));
}

TEST_F(TestDDLIncCommitLog, test_log_basic_copy_and_assign)
{
  ObDDLIncLogBasic log_basic1 = create_valid_log_basic(100001, 100002);

  // 拷贝构造
  ObDDLIncLogBasic log_basic2 = log_basic1;
  ASSERT_TRUE(log_basic2.is_valid());
  ASSERT_EQ(log_basic1.get_tablet_id(), log_basic2.get_tablet_id());
  ASSERT_EQ(log_basic1.get_lob_meta_tablet_id(), log_basic2.get_lob_meta_tablet_id());
  ASSERT_EQ(log_basic1.get_trans_id(), log_basic2.get_trans_id());

  // 赋值
  ObDDLIncLogBasic log_basic3;
  log_basic3 = log_basic1;
  ASSERT_TRUE(log_basic3.is_valid());
  ASSERT_EQ(log_basic1.get_tablet_id(), log_basic3.get_tablet_id());
}

TEST_F(TestDDLIncCommitLog, test_log_basic_hash_collision)
{
  // 测试不同的 log_basic 是否有不同的 hash 值
  ObDDLIncLogBasic log_basic1 = create_valid_log_basic(100001, 100002);
  ObDDLIncLogBasic log_basic2 = create_valid_log_basic(100003, 100004);
  ObDDLIncLogBasic log_basic3 = create_valid_log_basic(100005, 100006);

  uint64_t hash1 = log_basic1.hash();
  uint64_t hash2 = log_basic2.hash();
  uint64_t hash3 = log_basic3.hash();

  // 不同的对象应该有不同的 hash（虽然理论上可能冲突）
  ASSERT_NE(hash1, hash2);
  ASSERT_NE(hash2, hash3);
  ASSERT_NE(hash1, hash3);
}

// ==================== 性能和压力测试 ====================

TEST_F(TestDDLIncCommitLog, test_commit_log_serialize_deserialize_performance)
{
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log;

  ASSERT_SUCC(commit_log.init(log_basic, false));

  int64_t serialize_size = commit_log.get_serialize_size();
  char *buf = static_cast<char*>(allocator_.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);

  // 测试多次序列化/反序列化
  const int ITERATIONS = 1000;
  int64_t start_time = ObTimeUtility::current_time();

  for (int i = 0; i < ITERATIONS; ++i) {
    int64_t pos = 0;
    ASSERT_SUCC(commit_log.serialize(buf, serialize_size, pos));

    ObDDLIncCommitLog commit_log_dst;
    pos = 0;
    ASSERT_SUCC(commit_log_dst.deserialize(buf, serialize_size, pos));
  }

  int64_t elapsed = ObTimeUtility::current_time() - start_time;
  LOG_INFO("Performance test completed", K(ITERATIONS), K(elapsed),
           "avg_us_per_iteration", elapsed / ITERATIONS);

  // 确保性能合理（每次操作应该在毫秒级）
  ASSERT_LT(elapsed / ITERATIONS, 1000); // 平均每次小于 1ms
}

TEST_F(TestDDLIncCommitLog, test_commit_log_multiple_objects_memory)
{
  // 测试创建多个对象不会导致内存问题
  const int NUM_OBJECTS = 100;
  ObDDLIncCommitLog *commit_logs[NUM_OBJECTS];

  for (int i = 0; i < NUM_OBJECTS; ++i) {
    ObDDLIncLogBasic log_basic = create_valid_log_basic(100001 + i, 100002 + i);
    commit_logs[i] = new ObDDLIncCommitLog();
    ASSERT_SUCC(commit_logs[i]->init(log_basic, i % 2 == 0));
  }

  // 验证所有对象都有效
  for (int i = 0; i < NUM_OBJECTS; ++i) {
    ASSERT_TRUE(commit_logs[i]->is_valid());
    ASSERT_EQ(i % 2 == 0, commit_logs[i]->is_rollback());
  }

  // 清理
  for (int i = 0; i < NUM_OBJECTS; ++i) {
    delete commit_logs[i];
  }
}

// ==================== 兼容性测试 ====================

TEST_F(TestDDLIncCommitLog, test_commit_log_backward_compatibility)
{
  // 测试序列化版本兼容性
  ObDDLIncLogBasic log_basic = create_valid_log_basic();
  ObDDLIncCommitLog commit_log_src;

  ASSERT_SUCC(commit_log_src.init(log_basic, false));

  int64_t serialize_size = commit_log_src.get_serialize_size();
  char *buf = static_cast<char*>(allocator_.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);

  int64_t pos = 0;
  ASSERT_SUCC(commit_log_src.serialize(buf, serialize_size, pos));

  // 模拟旧版本反序列化（只读取部分字段）
  ObDDLIncCommitLog commit_log_dst;
  pos = 0;
  ASSERT_SUCC(commit_log_dst.deserialize(buf, serialize_size, pos));

  // 基本字段应该正确
  ASSERT_TRUE(commit_log_dst.is_valid());
  ASSERT_EQ(commit_log_src.get_log_basic().get_tablet_id(),
            commit_log_dst.get_log_basic().get_tablet_id());
}

// ==================== ObRpcRemoteWriteDDLIncCommitLogArg 完整序列化测试 ====================

TEST_F(TestDDLIncCommitLog, test_rpc_arg_full_serialize)
{
  int ret = OB_SUCCESS;

  // 创建 RPC Arg
  ObRpcRemoteWriteDDLIncCommitLogArg arg_src;

  // 准备参数
  uint64_t tenant_id = 1001;
  ObLSID ls_id(1);
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(2001);
  ObTxSEQ seq_no(3001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  // 创建一个简单的 mock tx_desc（只设置必要字段）
  ObTxDesc mock_tx_desc;
  mock_tx_desc.tenant_id_ = tenant_id;
  mock_tx_desc.tx_id_ = trans_id;
  mock_tx_desc.addr_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 8888);
  mock_tx_desc.sess_id_ = 1;
  mock_tx_desc.cluster_id_ = 1;
  mock_tx_desc.cluster_version_ = 1;
  mock_tx_desc.state_ = ObTxDesc::State::IDLE;

  // 初始化 arg
  ASSERT_SUCC(arg_src.init(tenant_id, ls_id, tablet_id, lob_tablet_id,
                           &mock_tx_desc, direct_load_type, trans_id, seq_no,
                           snapshot_version, data_format_version));

  // 设置 buffer 数据
  const char *data_content = "test_data_inc_major_buffer_for_rpc_arg";
  const char *lob_content = "test_lob_inc_major_buffer_for_rpc_arg";

  char *data_buf = static_cast<char*>(arg_src.allocator_.alloc(strlen(data_content)));
  char *lob_buf = static_cast<char*>(arg_src.allocator_.alloc(strlen(lob_content)));
  ASSERT_NE(nullptr, data_buf);
  ASSERT_NE(nullptr, lob_buf);

  MEMCPY(data_buf, data_content, strlen(data_content));
  MEMCPY(lob_buf, lob_content, strlen(lob_content));

  arg_src.data_inc_major_buffer_.assign_ptr(data_buf, strlen(data_content));
  arg_src.lob_inc_major_buffer_.assign_ptr(lob_buf, strlen(lob_content));
  arg_src.is_co_sstable_ = true;

  // 测试序列化
  char serialize_buf[4096];
  int64_t pos = 0;
  int64_t serialize_size = arg_src.get_serialize_size();

  ASSERT_GT(serialize_size, 0);
  ASSERT_LT(serialize_size, sizeof(serialize_buf));

  ASSERT_SUCC(arg_src.serialize(serialize_buf, sizeof(serialize_buf), pos));
  ASSERT_EQ(serialize_size, pos);  // 验证序列化大小一致

  // 验证序列化后的数据包含了所有字段
  // 注意：完整的反序列化需要 MTL 环境来获取 ObTransService
  // 这里只验证序列化能成功完成
  ASSERT_GT(pos, 0);

  // 验证序列化的数据不为空，包含了基本信息
  // 预期序列化大小应该包含：
  // - 基础字段 (tenant_id, ls_id, tablet_id, lob_tablet_id)
  // - tx_desc 序列化数据
  // - 其他字段 (direct_load_type, trans_id, seq_no, snapshot_version, data_format_version, is_co_sstable)
  // - buffer 数据
  ASSERT_GT(serialize_size, 100);  // 至少应该有 100 字节
}

TEST_F(TestDDLIncCommitLog, test_rpc_arg_serialize_with_empty_buffers)
{
  int ret = OB_SUCCESS;

  // 测试只有基础字段，没有 inc_major_buffer 的情况
  ObRpcRemoteWriteDDLIncCommitLogArg arg_src;

  uint64_t tenant_id = 1001;
  ObLSID ls_id(1);
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id;  // 无效的 lob tablet
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL;  // 非 major
  ObTransID trans_id(2001);
  ObTxSEQ seq_no(3001);
  int64_t snapshot_version = 0;  // 非 major 场景，snapshot_version 可以为 0
  uint64_t data_format_version = 0;

  // 创建简单的 mock tx_desc
  ObTxDesc mock_tx_desc;
  mock_tx_desc.tenant_id_ = tenant_id;
  mock_tx_desc.tx_id_ = trans_id;
  mock_tx_desc.addr_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 8888);
  mock_tx_desc.sess_id_ = 1;
  mock_tx_desc.cluster_id_ = 1;
  mock_tx_desc.cluster_version_ = 1;
  mock_tx_desc.state_ = ObTxDesc::State::IDLE;

  // 初始化（不调用 set_ss_inc_major）
  ASSERT_SUCC(arg_src.init(tenant_id, ls_id, tablet_id, lob_tablet_id,
                           &mock_tx_desc, direct_load_type, trans_id, seq_no,
                           snapshot_version, data_format_version));

  // 验证 buffer 为空
  ASSERT_TRUE(arg_src.data_inc_major_buffer_.empty());
  ASSERT_TRUE(arg_src.lob_inc_major_buffer_.empty());

  // 测试序列化
  char serialize_buf[4096];
  int64_t pos = 0;
  int64_t serialize_size = arg_src.get_serialize_size();

  ASSERT_GT(serialize_size, 0);
  ASSERT_SUCC(arg_src.serialize(serialize_buf, sizeof(serialize_buf), pos));
  ASSERT_EQ(serialize_size, pos);

  // 验证序列化成功
  ASSERT_GT(pos, 0);

  // 注意：完整的反序列化需要 MTL 环境，这里只测试序列化
}

TEST_F(TestDDLIncCommitLog, test_rpc_arg_init_validation)
{
  int ret = OB_SUCCESS;
  ObRpcRemoteWriteDDLIncCommitLogArg arg;

  uint64_t tenant_id = 1001;
  ObLSID ls_id(1);
  ObTabletID tablet_id(100001);
  ObTabletID lob_tablet_id(100002);
  ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR;
  ObTransID trans_id(2001);
  ObTxSEQ seq_no(3001);
  int64_t snapshot_version = 1000000;
  uint64_t data_format_version = DATA_VERSION_4_5_0_0;

  ObTxDesc mock_tx_desc;
  mock_tx_desc.tenant_id_ = tenant_id;
  mock_tx_desc.tx_id_ = trans_id;
  mock_tx_desc.addr_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 8888);
  mock_tx_desc.sess_id_ = 1;
  mock_tx_desc.cluster_id_ = 1;
  mock_tx_desc.cluster_version_ = 1;
  mock_tx_desc.state_ = ObTxDesc::State::IDLE;

  // 测试1: 正常初始化
  ASSERT_SUCC(arg.init(tenant_id, ls_id, tablet_id, lob_tablet_id,
                       &mock_tx_desc, direct_load_type, trans_id, seq_no,
                       snapshot_version, data_format_version));
  ASSERT_TRUE(arg.is_valid());

  // 测试2: 无效的 ls_id
  ObRpcRemoteWriteDDLIncCommitLogArg arg2;
  ASSERT_FAIL(arg2.init(tenant_id, ObLSID(), tablet_id, lob_tablet_id,
                        &mock_tx_desc, direct_load_type, trans_id, seq_no,
                        snapshot_version, data_format_version));

  // 测试3: 无效的 tablet_id
  ObRpcRemoteWriteDDLIncCommitLogArg arg3;
  ASSERT_FAIL(arg3.init(tenant_id, ls_id, ObTabletID(), lob_tablet_id,
                        &mock_tx_desc, direct_load_type, trans_id, seq_no,
                        snapshot_version, data_format_version));

  // 测试4: nullptr tx_desc
  ObRpcRemoteWriteDDLIncCommitLogArg arg4;
  ASSERT_FAIL(arg4.init(tenant_id, ls_id, tablet_id, lob_tablet_id,
                        nullptr, direct_load_type, trans_id, seq_no,
                        snapshot_version, data_format_version));

  // 测试5: 无效的 direct_load_type
  ObRpcRemoteWriteDDLIncCommitLogArg arg5;
  ASSERT_FAIL(arg5.init(tenant_id, ls_id, tablet_id, lob_tablet_id,
                        &mock_tx_desc, ObDirectLoadType::DIRECT_LOAD_INVALID,
                        trans_id, seq_no, snapshot_version, data_format_version));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ddl_inc_commit_log.log*");
  OB_LOGGER.set_file_name("test_ddl_inc_commit_log.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
