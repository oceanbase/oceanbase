
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
#include "test_backup.h"
#define private public
#define protected public

using namespace oceanbase;
using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

struct CompatBackupSSTableMeta {
  OB_UNIS_VERSION(1);

public:
  CompatBackupSSTableMeta();
  virtual ~CompatBackupSSTableMeta() = default;
  bool is_valid() const;
  void reset();
  int assign(const CompatBackupSSTableMeta &backup_sstable_meta);

  TO_STRING_KV(K_(tablet_id), K_(sstable_meta), K_(logic_id_list),
      K_(entry_block_addr_for_other_block), K_(total_other_block_count),
      K_(is_major_compaction_mview_dep));

  common::ObTabletID tablet_id_;
  blocksstable::ObMigrationSSTableParam sstable_meta_;
  common::ObSArray<blocksstable::ObLogicMacroBlockId> logic_id_list_;
  ObBackupPhysicalID entry_block_addr_for_other_block_;
  int64_t total_other_block_count_;
  bool is_major_compaction_mview_dep_;
};

OB_SERIALIZE_MEMBER(CompatBackupSSTableMeta,
                    tablet_id_,
                    sstable_meta_,
                    logic_id_list_,
                    entry_block_addr_for_other_block_,
                    total_other_block_count_,
                    is_major_compaction_mview_dep_);

CompatBackupSSTableMeta::CompatBackupSSTableMeta() : tablet_id_(), sstable_meta_(), logic_id_list_(),
    entry_block_addr_for_other_block_(), total_other_block_count_(0), is_major_compaction_mview_dep_(false)
{}

bool CompatBackupSSTableMeta::is_valid() const
{
  return tablet_id_.is_valid() && sstable_meta_.is_valid();
}

void CompatBackupSSTableMeta::reset()
{
  tablet_id_.reset();
  sstable_meta_.reset();
  logic_id_list_.reset();
  entry_block_addr_for_other_block_.reset();
  total_other_block_count_ = 0;
  is_major_compaction_mview_dep_ = false;
}

int CompatBackupSSTableMeta::assign(const CompatBackupSSTableMeta &backup_sstable_meta)
{
  int ret = OB_SUCCESS;
  if (!backup_sstable_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup sstable meta is invalid", K(ret), K(backup_sstable_meta));
  } else if (OB_FAIL(sstable_meta_.assign(backup_sstable_meta.sstable_meta_))) {
    LOG_WARN("failed to assign sstable meta", K(ret), K(backup_sstable_meta));
  } else if (OB_FAIL(logic_id_list_.assign(backup_sstable_meta.logic_id_list_))) {
    LOG_WARN("failed to assign logic id list", K(ret), K(backup_sstable_meta));
  } else {
    entry_block_addr_for_other_block_ = backup_sstable_meta.entry_block_addr_for_other_block_;
    total_other_block_count_ = backup_sstable_meta.total_other_block_count_;
    tablet_id_ = backup_sstable_meta.tablet_id_;
    is_major_compaction_mview_dep_ = backup_sstable_meta.is_major_compaction_mview_dep_;
  }
  return ret;
}

void prepare_migration_sstable_param(ObMigrationSSTableParam &mig_param)
{
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::TableType::MAJOR_SSTABLE;
  table_key.tablet_id_ = 20221106;
  table_key.version_range_.base_version_ = 1;
  table_key.version_range_.snapshot_version_ = 11;

  blocksstable::ObSSTableMeta sstable_meta;
  ObMetaDiskAddr addr;
  addr.set_none_addr();
  sstable_meta.basic_meta_.row_count_ = 111;
  sstable_meta.basic_meta_.occupy_size_ = 222;
  sstable_meta.basic_meta_.data_checksum_ = 333;
  sstable_meta.basic_meta_.index_type_ = 0;
  sstable_meta.basic_meta_.rowkey_column_count_ = 1;
  sstable_meta.basic_meta_.column_cnt_ = 1;
  sstable_meta.basic_meta_.index_macro_block_count_ = 3;
  sstable_meta.basic_meta_.use_old_macro_block_count_ = 1;
  sstable_meta.basic_meta_.index_macro_block_count_ = 2;
  sstable_meta.basic_meta_.sstable_format_version_ = 0;
  sstable_meta.basic_meta_.schema_version_ = 3;
  sstable_meta.basic_meta_.create_snapshot_version_ = 0;
  sstable_meta.basic_meta_.progressive_merge_round_ = 0;
  sstable_meta.basic_meta_.progressive_merge_step_ = 0;
  sstable_meta.basic_meta_.upper_trans_version_ = 0;
  sstable_meta.basic_meta_.max_merged_trans_version_ = 0;
  sstable_meta.basic_meta_.ddl_scn_.set_min();
  sstable_meta.basic_meta_.filled_tx_scn_.set_min();
  sstable_meta.basic_meta_.contain_uncommitted_row_ = 0;
  sstable_meta.data_root_info_.addr_ = addr;
  sstable_meta.basic_meta_.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  sstable_meta.basic_meta_.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  sstable_meta.basic_meta_.data_index_tree_height_ = 0;
  sstable_meta.macro_info_.macro_meta_info_.addr_ = addr;
  ASSERT_TRUE(sstable_meta.check_meta());
  sstable_meta.is_inited_ = true;
  ASSERT_TRUE(!mig_param.is_valid());
  mig_param.reset();
  ASSERT_TRUE(!mig_param.is_valid());
  mig_param.basic_meta_ = sstable_meta.get_basic_meta();
  for (int64_t i = 0; i < sstable_meta.get_col_checksum_cnt(); ++i) {
    ASSERT_EQ(OB_SUCCESS, mig_param.column_checksums_.push_back(sstable_meta.get_col_checksum()[i]));
  }
  mig_param.table_key_ = table_key;
  char str[15] ="test serialize";
  mig_param.root_block_addr_.set_mem_addr(0, 15);
  mig_param.root_block_buf_ = str;
  blocksstable::MacroBlockId block_id(4096,0,0);
  mig_param.data_block_macro_meta_addr_.set_block_addr(block_id, 1024, 2048, ObMetaDiskAddr::DiskType::BLOCK);
  ASSERT_EQ(nullptr, mig_param.data_block_macro_meta_buf_);
  mig_param.is_meta_root_ = true;

}

TEST(TestBackupCompatible, test_backup_compatible)
{
  CompatBackupSSTableMeta compat_sstable_meta;
  compat_sstable_meta.tablet_id_ = ObTabletID(1);
  prepare_migration_sstable_param(compat_sstable_meta.sstable_meta_);
  compat_sstable_meta.entry_block_addr_for_other_block_ = ObBackupPhysicalID::get_default();
  compat_sstable_meta.total_other_block_count_ = 0;
  compat_sstable_meta.is_major_compaction_mview_dep_ = false;

  int64_t pos = 0;
  const int64_t buf_len = compat_sstable_meta.get_serialize_size();
  char *buf = new char [buf_len];
  ASSERT_EQ(OB_SUCCESS, compat_sstable_meta.serialize(buf, buf_len, pos));

  CompatBackupSSTableMeta tmp_compat_sstable_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_compat_sstable_meta.deserialize(buf, buf_len, pos));

  ObBackupSSTableMeta backup_sstable_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, backup_sstable_meta.deserialize(buf, buf_len, pos));

  ASSERT_EQ(compat_sstable_meta.tablet_id_, backup_sstable_meta.tablet_id_);
  ASSERT_EQ(compat_sstable_meta.sstable_meta_.basic_meta_, backup_sstable_meta.sstable_meta_.basic_meta_);
  ASSERT_EQ(compat_sstable_meta.total_other_block_count_, backup_sstable_meta.total_other_block_count_);
  ASSERT_EQ(compat_sstable_meta.is_major_compaction_mview_dep_, backup_sstable_meta.is_major_compaction_mview_dep_);
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_compatible.log*");
  OB_LOGGER.set_file_name("test_backup_compatible.log", true);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
