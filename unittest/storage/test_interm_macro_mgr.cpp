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

#include "storage/ob_interm_macro_mgr.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_local_file_system.h"
#include <gtest/gtest.h>

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace unittest {

class ObIntermMacroMgrTest : public TestDataFilePrepare {
public:
  ObIntermMacroMgrTest();
  virtual ~ObIntermMacroMgrTest();
  virtual void SetUp();
  virtual void TearDown();
  ObStorageFile* get_storage_file()
  {
    return storage_file_;
  }

private:
  ObStorageFile* storage_file_;
  ObStorageFileWithRef file_with_ref_;
};

ObIntermMacroMgrTest::ObIntermMacroMgrTest()
    : TestDataFilePrepare("TestIntermMacroMgr", 64 * 1024, 1024), storage_file_(NULL)
{}

ObIntermMacroMgrTest::~ObIntermMacroMgrTest()
{}

void ObIntermMacroMgrTest::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, INTERM_MACRO_MGR.init());
  TestDataFilePrepare::SetUp();
  util_.get_file_system().alloc_file(storage_file_);
  common::ObAddr addr(201212092019);
  storage_file_->init(addr, ObStorageFile::FileType::TMP_FILE);
  file_with_ref_.file_ = storage_file_;
}

void ObIntermMacroMgrTest::TearDown()
{
  INTERM_MACRO_MGR.destroy();
  file_with_ref_.file_ = NULL;
  storage_file_->close();
  util_.get_file_system().free_file(storage_file_);
  TestDataFilePrepare::TearDown();
}

TEST_F(ObIntermMacroMgrTest, test_single)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaV2 meta;
  ObMacroBlockSchemaInfo schema;
  ObFullMacroBlockMeta full_meta;
  full_meta.meta_ = &meta;
  full_meta.schema_ = &schema;
  ObArray<blocksstable::MacroBlockId> macro_blocks;
  ObIntermMacroHandle handle;
  ObIntermMacroKey key;
  key.execution_id_ = 1;
  key.task_id_ = 1;
  blocksstable::ObMacroBlocksWriteCtx write_ctx;
  ObStorageFileWithRef file_with_ref;
  ObStorageFileHandle file_handle;
  file_with_ref.file_ = get_storage_file();
  file_handle.set_storage_file_with_ref(file_with_ref);
  write_ctx.file_handle_.reset();
  ASSERT_EQ(common::OB_SUCCESS, write_ctx.file_handle_.assign(file_handle));
  ASSERT_EQ(OB_SUCCESS, write_ctx.file_ctx_.init(STORE_FILE_SYSTEM_LOCAL, STORE_FILE_MACRO_BLOCK, INT64_MAX));
  ret = write_ctx.add_macro_block(MacroBlockId(1), full_meta);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = macro_blocks.push_back(MacroBlockId(1));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = INTERM_MACRO_MGR.put(key, write_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = INTERM_MACRO_MGR.get(key, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObIArray<MacroBlockId>& handle_macro_blocks = handle.get_resource_ptr()->get_macro_blocks().macro_block_list_;
  ASSERT_EQ(macro_blocks.count(), handle_macro_blocks.count());
  ASSERT_EQ(macro_blocks.at(0), handle_macro_blocks.at(0));
  ret = INTERM_MACRO_MGR.remove(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_blocks.at(0), handle_macro_blocks.at(0));
  ret = INTERM_MACRO_MGR.get(key, handle);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST_F(ObIntermMacroMgrTest, test_multi)
{
  int ret = OB_SUCCESS;
  static const int64_t KEY_CNT = 1000;
  for (int64_t i = 0; OB_SUCC(ret) && i < KEY_CNT; ++i) {
    ObArray<MacroBlockId> macro_blocks;
    ObIntermMacroHandle handle;
    ObIntermMacroKey key;
    ObMacroBlockMetaV2 meta;
    ObMacroBlockSchemaInfo schema;
    ObFullMacroBlockMeta full_meta;
    full_meta.meta_ = &meta;
    full_meta.schema_ = &schema;
    key.execution_id_ = 1;
    key.task_id_ = i;
    blocksstable::ObMacroBlocksWriteCtx write_ctx;
    ObStorageFileHandle file_handle;
    file_handle.set_storage_file_with_ref(file_with_ref_);
    write_ctx.file_handle_.reset();
    ASSERT_EQ(common::OB_SUCCESS, write_ctx.file_handle_.assign(file_handle));
    ASSERT_EQ(OB_SUCCESS, write_ctx.file_ctx_.init(STORE_FILE_SYSTEM_LOCAL, STORE_FILE_MACRO_BLOCK, INT64_MAX));
    ret = write_ctx.add_macro_block(MacroBlockId(i), full_meta);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = INTERM_MACRO_MGR.put(key, write_ctx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = INTERM_MACRO_MGR.get(key, handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, handle.get_resource_ptr()->get_macro_blocks().macro_block_list_.count());
    ASSERT_EQ(MacroBlockId(i), handle.get_resource_ptr()->get_macro_blocks().macro_block_list_.at(0));
  }
}

TEST_F(ObIntermMacroMgrTest, test_border)
{
  int ret = OB_SUCCESS;
  ObIntermMacroKey key;
  ObIntermMacroHandle handle;
  key.execution_id_ = 1;
  key.task_id_ = 11;
  ret = INTERM_MACRO_MGR.get(key, handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = INTERM_MACRO_MGR.remove(key);
  ASSERT_NE(OB_SUCCESS, ret);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_interm_macro_mgr.log*");
  OB_LOGGER.set_file_name("test_interm_macro_mgr.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_interm_macro_mgr");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
