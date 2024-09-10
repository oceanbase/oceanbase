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

// #include "storage/blocksstable/ob_data_file_prepare.h"
// #include "share/ob_simple_mem_limit_getter.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_public_block_gc_service.h"

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
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
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
  ASSERT_EQ(OB_SUCCESS, gc_tablet_meta_versions(tablet_versions, 2));
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
  ASSERT_EQ(OB_SUCCESS, gc_tablet(tablet_versions));
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

class TestPublicBlockGCHandler : public ::testing::Test,
                           public storage::ObPublicBlockGCHandler
{
public:
  TestPublicBlockGCHandler()
  : storage::ObPublicBlockGCHandler(ObTabletID(1))
  {
    for (int i = 0; i < 1000; i++) {
      macro_block_ids_[i].third_id_ = i;
      macro_block_ids_[i].first_id_ = 1;
    }
    for (int i = 0; i < 2000; i++) {
      macro_block_ids_[i].third_id_ = i;
      macro_block_ids_[i].first_id_ = 1;
    }
    for (int i = 2000; i < 3000; i++) {
      if (0 == i % 1000) {
        macro_block_ids_[i].third_id_ = i;
        macro_block_ids_[i].first_id_ = 0;
      } else {
        macro_block_ids_[i].third_id_ = i;
        macro_block_ids_[i].first_id_ = 1;
      }
    }
    for (int i = 3000; i < 4000; i++) {
      macro_block_ids_[i].third_id_ = i;
      macro_block_ids_[i].first_id_ = 0;
    }
  }
  virtual ~TestPublicBlockGCHandler() {}

  TO_STRING_KV(K(macro_block_ids_[0]),
      K(macro_block_ids_[500]),
      K(macro_block_ids_[1000]),
      K(macro_block_ids_[1500]),
      K(macro_block_ids_[2000]),
      K(macro_block_ids_[2500]),
      K(macro_block_ids_[3000]),
      K(macro_block_ids_[3500]));

  int is_exist_macro_block_(
      const blocksstable::MacroBlockId &block_id,
      bool &is_exist)
  {
    is_exist = false;
    if (1 == macro_block_ids_[block_id.third_id_].first_id_) {
      is_exist = true;
    }
    return OB_SUCCESS;
  }

  int delete_macro_blocks(
        ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < block_ids.count(); i++) {
      macro_block_ids_[block_ids.at(i).third_id_].first_id_ = 2;
    }
    return ret;
  }

  blocksstable::MacroBlockId macro_block_ids_[4000];
};

TEST_F(TestPublicBlockGCHandler, test_detect_and_gc)
{
  LOG_INFO("test detect_and_gc start", KPC(this));
  blocksstable::MacroBlockId block_id;
  block_id.third_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, detect_and_gc_block_(block_id));
  for (int i = 0; i < 3000; i++) {
    ASSERT_EQ(2, macro_block_ids_[i].first_id_);
  }
  for (int i = 3000; i < 4000; i++) {
    ASSERT_EQ(0, macro_block_ids_[i].first_id_);
  }
  LOG_INFO("test detect_and_gc finish", KPC(this));
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
