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
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#define private public
#define protected public

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

static int make_random_mapping_meta(ObBackupMacroBlockIDMappingsMeta &mapping_meta)
{
  int ret = OB_SUCCESS;
  const int64_t sstable_count = random(1, MAX_SSTABLE_CNT_IN_STORAGE);
  mapping_meta.version_ = ObBackupMacroBlockIDMappingsMeta::MAPPING_META_VERSION_V1;
  mapping_meta.sstable_count_ = sstable_count;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_count; ++i) {
    ObBackupMacroBlockIDMapping &mapping = mapping_meta.id_map_list_[i];
    make_random_table_key(mapping.table_key_);
    const int64_t pair_count = random(1, 10000);
    for (int64_t j = 0; OB_SUCC(ret) && j < pair_count; ++j) {
      ObBackupMacroBlockIDPair pair;
      make_random_pair(pair);
      ret = mapping.id_pair_list_.push_back(pair);
      EXPECT_EQ(OB_SUCCESS, ret);
    }
  }
  return ret;
}

static bool meta_is_equal(const ObBackupMacroBlockIDMappingsMeta &lhs, const ObBackupMacroBlockIDMappingsMeta &rhs)
{
  bool bret = true;
  if (lhs.version_ != rhs.version_ || lhs.sstable_count_ != rhs.sstable_count_) {
    bret = false;
  } else {
    for (int64_t i = 0; i < lhs.sstable_count_; ++i) {
      const ObBackupMacroBlockIDMapping &lhs_map = lhs.id_map_list_[i];
      const ObBackupMacroBlockIDMapping &rhs_map = rhs.id_map_list_[i];
      if (lhs_map.table_key_ != rhs_map.table_key_) {
        bret = false;
        break;
      } else if (lhs_map.id_pair_list_.count() != rhs_map.id_pair_list_.count()) {
        bret = false;
        break;
      } else {
        for (int64_t j = 0; j < lhs_map.id_pair_list_.count(); ++j) {
          const ObBackupMacroBlockIDPair &lhs_pair = lhs_map.id_pair_list_.at(j);
          const ObBackupMacroBlockIDPair &rhs_pair = rhs_map.id_pair_list_.at(j);
          if (lhs_pair.logic_id_ != rhs_pair.logic_id_ || lhs_pair.physical_id_ != rhs_pair.physical_id_) {
            bret = false;
            break;
          }
        }
      }
    }
  }
  return bret;
}

// macro_index={logic_id:{data_seq:0, logic_version:1657251061256045963, tablet_id:549755814602}, backup_set_id:2, ls_id:{id:1003}, turn_id:1, retry_id:0, file_id:3, offset:-1901723648, length:2015232})
// backup_physic_block_id:{backup_set_id:2, ls_id:1003, turn_id:1, retry_id:0, file_id:3, offset:584288, length:492}})
TEST(TestBackupDataStruct, BackupPhysicalID)
{
  int ret = OB_SUCCESS;
  blocksstable::ObLogicMacroBlockId logic_id;
  logic_id.data_seq_ = 0;
  logic_id.logic_version_ = 1657251061256045963;
  logic_id.tablet_id_ = 549755814602;
  ObBackupPhysicalID backup_physical_id;
  ObBackupMacroBlockIndex macro_index;
  backup_physical_id.backup_set_id_ = 2;
  backup_physical_id.ls_id_ = 1003;
  backup_physical_id.turn_id_ = 1;
  backup_physical_id.retry_id_ = 0;
  backup_physical_id.file_id_ = 3;
  backup_physical_id.offset_ = 584288;
  backup_physical_id.length_ = 492;
  ret = backup_physical_id.get_backup_macro_block_index(logic_id, macro_index);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBackupMacroBlockIndex actual_macro_index;
  actual_macro_index.logic_id_.data_seq_ = 0;
  actual_macro_index.logic_id_.logic_version_ = 1657251061256045963;
  actual_macro_index.logic_id_.tablet_id_ = 549755814602;
  actual_macro_index.backup_set_id_ = 2;
  actual_macro_index.ls_id_ = ObLSID(1003);
  actual_macro_index.turn_id_ = 1;
  actual_macro_index.retry_id_ = 0;
  actual_macro_index.file_id_ = 3;
  actual_macro_index.offset_ = 2393243648LL;
  actual_macro_index.length_ = 2015232LL;
  ASSERT_EQ(macro_index, actual_macro_index);
}

TEST(TestBackupDataStruct, BackupMacroBlockIDMappingsMeta)
{
  ObBackupMacroBlockIDMappingsMeta write_meta;
  ObBackupMacroBlockIDMappingsMeta read_meta;
  make_random_mapping_meta(write_meta);
  const int64_t buf_len = write_meta.get_serialize_size();
  char *buf = static_cast<char *>(malloc(buf_len));
  int64_t pos = 0;
  write_meta.serialize(buf, buf_len, pos);
  pos = 0;
  read_meta.deserialize(buf, buf_len, pos);
  ASSERT_TRUE(meta_is_equal(write_meta, read_meta));
}

TEST(TestBackupDataStruct, BackupMetaKeyCompare)
{
  ObBackupMetaKey lhs_key;
  lhs_key.tablet_id_ = 200001;
  lhs_key.meta_type_ = BACKUP_SSTABLE_META;

  ObBackupMetaKey rhs_key;
  rhs_key.tablet_id_ = 200001;
  rhs_key.meta_type_ = BACKUP_SSTABLE_META;

  ASSERT_FALSE(lhs_key > rhs_key);
  ASSERT_FALSE(lhs_key < rhs_key);
  ASSERT_TRUE(lhs_key == rhs_key);
  ASSERT_FALSE(lhs_key != rhs_key);
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_data_struct.log*");
  OB_LOGGER.set_file_name("test_backup_data_struct.log", true);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}