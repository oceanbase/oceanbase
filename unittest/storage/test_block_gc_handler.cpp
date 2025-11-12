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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#include "close_modules/shared_storage/storage/shared_storage/ob_private_block_gc_task.h"
#include "storage/ls/ob_ls.h"
namespace oceanbase
{
using namespace common;

namespace unittest
{

class TestBlockGCHandler : public ::testing::Test,
                           public storage::ObBlockGCHandler
{
public:
  TestBlockGCHandler()
  : storage::ObBlockGCHandler(ObTabletID(1))
  {}
  virtual ~TestBlockGCHandler() {}

  virtual int list_tablet_meta_version(
    ObIArray<int64_t> &tablet_versions)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < 3; i++) {
      ret = tablet_versions.push_back(i);
    }
    return ret;
  }
  virtual int get_blocks_for_tablet(
    int64_t tablet_meta_version,
    const bool is_shared,
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    UNUSED(tablet_meta_version);
    UNUSED(is_shared);
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < 3; i++) {
      ret = block_ids.push_back(macro_block_ids_[tablet_meta_version][i]);
    }
    return ret;
  }
  virtual int delete_tablet_meta_version(
    int64_t tablet_meta_version)
  {
    return OB_SUCCESS;
  }
  virtual int try_delete_tablet_meta_dir()
  {
    return OB_SUCCESS;
  }

  virtual int delete_macro_blocks(
      ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    gc_blocks_.push_back(block_ids);
    return OB_SUCCESS;
  }

  virtual int delete_macro_cache_(const ObIArray<blocksstable::MacroBlockId> &block_ids) override
  {
    return OB_SUCCESS;
  }
  void reset()
  {
    macro_block_ids_ = NULL;
    gc_blocks_.reset();
  }

  int64_t to_string(
      char* buf,
      const int64_t buf_len) const
  { return OB_SUCCESS; }

  int get_block_ids_from_dir(
      ObIArray<blocksstable::MacroBlockId> &block_ids)
  { return OB_SUCCESS; }

  int try_delete_tablet_data_dir()
  { return OB_SUCCESS; }

  int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &collect_check_block_op)
  { return OB_SUCCESS; }

  blocksstable::MacroBlockId (*macro_block_ids_)[3];
  ObArray<blocksstable::MacroBlockId> gc_blocks_;
};

TEST_F(TestBlockGCHandler, test_block_gc)
{
  ObArray<int64_t> tablet_versions;
  blocksstable::MacroBlockId test_array[3][3] =
  {
    { blocksstable::MacroBlockId(0, 1, 0, 0), blocksstable::MacroBlockId(0, 2, 0, 0), blocksstable::MacroBlockId(0, 3, 0, 0) },
    { blocksstable::MacroBlockId(0, 4, 0, 0), blocksstable::MacroBlockId(0, 5, 0, 0), blocksstable::MacroBlockId(0, 3, 0, 0) },
    { blocksstable::MacroBlockId(0, 6, 0, 0), blocksstable::MacroBlockId(0, 4, 0, 0), blocksstable::MacroBlockId(0, 3, 0, 0) },
  };

  macro_block_ids_ = test_array;
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(OB_SUCCESS, tablet_versions.push_back(i));
  }
  ASSERT_EQ(OB_SUCCESS, gc_tablet_meta_versions(tablet_versions,
                                                2/*min_retain_tablet_meta_version*/,
                                                false/*is_transfer_out_deleted*/));
  ASSERT_EQ(gc_blocks_.count(), 3);
  for (int i = 0; i < gc_blocks_.count(); i++) {
    if (1 == gc_blocks_.at(i).second_id_
        || 2 == gc_blocks_.at(i).second_id_ || 5 == gc_blocks_.at(i).second_id_) {
    } else {
      abort();
    }
  }

  reset();
  macro_block_ids_ = test_array;
  ASSERT_EQ(OB_SUCCESS, gc_tablet(tablet_versions, GCTabletType::DropTablet));
  ASSERT_EQ(gc_blocks_.count(), 6);
  for (int i = 0; i < gc_blocks_.count(); i++) {
    if (1 == gc_blocks_.at(i).second_id_
        || 2 == gc_blocks_.at(i).second_id_
        || 3 == gc_blocks_.at(i).second_id_
        || 4 == gc_blocks_.at(i).second_id_
        || 5 == gc_blocks_.at(i).second_id_
        || 6 == gc_blocks_.at(i).second_id_) {
    } else {
      abort();
    }
  }
}

class TestPrivateBlockGCHandler : public ::testing::Test,
                                  public storage::ObPrivateBlockGCHandler
{
public:
  TestPrivateBlockGCHandler()
  : storage::ObPrivateBlockGCHandler(share::ObLSID(1), 1, ObTabletID(1), 1, -1, 1, 10, 20, 0)
  {
    for (int i = 0; i < 30; i++) {
      macro_block_ids_[i].id_mode_ = 2; // ID_MODE_SHARE
      macro_block_ids_[i].storage_object_type_ = 0; //blocksstable::ObStorageObjectType::PRIVATE_DATA_MACRO;
      macro_block_ids_[i].third_id_ = 1;
      macro_block_ids_[i].tenant_seq_ = i;
    }
  }
  virtual ~TestPrivateBlockGCHandler() {}

  TO_STRING_KV(K(macro_block_ids_[0].tenant_seq_), K(macro_block_ids_[11].tenant_seq_), K(macro_block_ids_[12].tenant_seq_), K(macro_block_ids_[21].tenant_seq_));

  int get_blocks_for_tablet(
      int64_t tablet_meta_version,
      const bool is_shared,
      ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    UNUSED(tablet_meta_version);
    UNUSED(is_shared);
    for (int i = 11; i < 20; i++) {
      if (0 == i % 2) {
        block_ids.push_back(macro_block_ids_[i]);
      }
    }
    return OB_SUCCESS;
  }
  int list_tablet_meta_version(
      ObIArray<int64_t> &tablet_versions,
      const bool,
      int64_t &)
  {
    tablet_versions.push_back(1);
    return OB_SUCCESS;
  }

  int list_tablet_meta_version(
      ObIArray<int64_t> &tablet_versions)
  {
    tablet_versions.push_back(1);
    return OB_SUCCESS;
  }

  int get_block_ids_from_dir(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    for (int i = 0; i < 30; i++) {
      if (10 != i && 20 != i) {
        block_ids.push_back(macro_block_ids_[i]);
      }
    }
    return OB_SUCCESS;
  }
  int delete_macro_blocks(
        ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < block_ids.count(); i++) {
      macro_block_ids_[block_ids.at(i).tenant_seq_].third_id_ = 2;
    }
    return ret;
  }
  virtual int delete_macro_cache_(const ObIArray<blocksstable::MacroBlockId> &block_ids) override
  {
    return OB_SUCCESS;
  }

  int update_tablet_last_gc_version_(const int64_t) {
    return OB_SUCCESS;
  }

  int safe_delete_blocks_for_macro_check_(
        ObIArray<blocksstable::MacroBlockId> &block_ids) override
  {
    return delete_macro_blocks(block_ids);
  }

  blocksstable::MacroBlockId macro_block_ids_[30];
};

TEST_F(TestPrivateBlockGCHandler, test_macro_check)
{
  LOG_INFO("test detect_and_gc start", KPC(this));
  ASSERT_EQ(OB_SUCCESS, macro_block_check());
  for (int i = 11; i < 20; i++) {
    if (0 == i % 2) {
      ASSERT_EQ(1, macro_block_ids_[i].third_id_);
    } else {
      ASSERT_EQ(2, macro_block_ids_[i].third_id_);
    }
  }
  for (int i = 0; i < 30; i++) {
    if (i < 10 || i >= 20) {
      ASSERT_EQ(1, macro_block_ids_[i].third_id_);
    }
  }
  LOG_INFO("test test_macro_check finish", KPC(this));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_block_gc_handler.log*");
  OB_LOGGER.set_file_name("test_block_gc_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
