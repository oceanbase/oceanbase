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

#include <gmock/gmock.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
using namespace tmp_file;
/* ---------------------------- Unittest Class ----------------------------- */
class TestTmpFileBlock : public ::testing::Test
{
public:
  TestTmpFileBlock() = default;
  virtual ~TestTmpFileBlock() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestTmpFileBlock::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestTmpFileBlock::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestTmpFileBlock, test_block_manager_op)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockManager block_mgr;
  ret = block_mgr.init(MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t begin_page_id = 0;
  int64_t page_num = ObTmpFileGlobal::BLOCK_PAGE_NUMS;
  int64_t block_index1 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  int64_t block_index2 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  int64_t block_index3 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  int64_t block_index4 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  int64_t block_index5 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  int64_t invalid_logic_block_index = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  // 1. create tmp file blocks
  ret = block_mgr.create_tmp_file_block(begin_page_id + 10, page_num - 20, block_index1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index1, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  ret = block_mgr.create_tmp_file_block(begin_page_id, page_num, block_index2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index2, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  ret = block_mgr.create_tmp_file_block(begin_page_id + 10, page_num - 10, block_index3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index3, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  ret = block_mgr.create_tmp_file_block(begin_page_id, page_num, block_index4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index4, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  ret = block_mgr.create_tmp_file_block(begin_page_id, page_num, block_index5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index5, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  ret = block_mgr.create_tmp_file_block(-1, page_num, invalid_logic_block_index);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = block_mgr.create_tmp_file_block(begin_page_id, -1, invalid_logic_block_index);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = block_mgr.create_tmp_file_block(begin_page_id, page_num+1, invalid_logic_block_index);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = block_mgr.create_tmp_file_block(begin_page_id+1, page_num, invalid_logic_block_index);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // 2. alloc three macro block
  ObMacroBlockHandle macro_block_handle1;
  ObMacroBlockHandle macro_block_handle2;
  ObMacroBlockHandle macro_block_handle3;
  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_block_handle1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_block_handle2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_block_handle3);
  ASSERT_EQ(OB_SUCCESS, ret);
  MacroBlockId macro_block_id1 = macro_block_handle1.get_macro_id();
  ASSERT_EQ(true, macro_block_id1.is_valid());
  MacroBlockId macro_block_id2 = macro_block_handle2.get_macro_id();
  ASSERT_EQ(true, macro_block_id2.is_valid());
  MacroBlockId macro_block_id3 = macro_block_handle3.get_macro_id();
  ASSERT_EQ(true, macro_block_id3.is_valid());

  // 3. switch state op
  ret = block_mgr.write_back_failed(block_index1);
  ASSERT_EQ(OB_OP_NOT_ALLOW, ret);
  ret = block_mgr.write_back_succ(block_index1, macro_block_id1);
  ASSERT_EQ(OB_OP_NOT_ALLOW, ret);

  ret = block_mgr.write_back_start(block_index2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_start(block_index3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_start(block_index4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_start(block_index5);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. test releasing all pages of block which is writing back
  // 4.1 block exists in block manager before releasing all pages of block
  ObTmpFileBlockHandle handle;
  ret = block_mgr.get_tmp_file_block_handle(block_index4, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, handle.get());
  handle.reset();
  ret = block_mgr.get_tmp_file_block_handle(block_index5, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, handle.get());
  handle.reset();
  // 4.2 block exists in block manager after releasing block
  ret = block_mgr.release_tmp_file_page(block_index4, begin_page_id, page_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.release_tmp_file_page(block_index5, begin_page_id, page_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = block_mgr.get_tmp_file_block_handle(block_index4, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, handle.get());
  handle.reset();
  ret = block_mgr.get_tmp_file_block_handle(block_index5, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, handle.get());
  handle.reset();
  // 4.3 block doesn't exist in block manager after writing back over
  ret = block_mgr.write_back_succ(block_index4, macro_block_id1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_failed(block_index5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.get_tmp_file_block_handle(block_index4, handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = block_mgr.get_tmp_file_block_handle(block_index5, handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // 5. test releasing pages of block which is writing back over
  ret = block_mgr.write_back_succ(block_index2, macro_block_id2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_succ(block_index2, macro_block_id2);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = block_mgr.write_back_succ(block_index3, MacroBlockId());
  ASSERT_NE(OB_SUCCESS, ret);
  ret = block_mgr.write_back_succ(block_index3, macro_block_id3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = block_mgr.release_tmp_file_page(block_index1, begin_page_id + 10, page_num - 20);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.release_tmp_file_page(block_index2, begin_page_id, page_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.release_tmp_file_page(block_index3, begin_page_id + 10, 10);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = block_mgr.get_tmp_file_block_handle(block_index1, handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = block_mgr.get_tmp_file_block_handle(block_index2, handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = block_mgr.get_tmp_file_block_handle(block_index3, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, handle.get());

  LOG_INFO("test_block_manager_op");
}

TEST_F(TestTmpFileBlock, test_block_manager_stat)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockManager block_mgr;
  ret = block_mgr.init(MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t begin_page_id = 0;
  int64_t page_num = ObTmpFileGlobal::BLOCK_PAGE_NUMS;
  int64_t block_index1 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  int64_t block_index2 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  int64_t block_index3 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  int64_t block_index4 = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  // 1. create tmp file blocks
  ret = block_mgr.create_tmp_file_block(begin_page_id + 10, page_num - 20, block_index1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index1, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  ret = block_mgr.create_tmp_file_block(begin_page_id, page_num, block_index2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index2, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  ret = block_mgr.create_tmp_file_block(begin_page_id + 10, page_num - 10, block_index3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index3, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  ret = block_mgr.create_tmp_file_block(begin_page_id, page_num, block_index4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(block_index4, ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX);

  // 2. alloc three macro block
  ObMacroBlockHandle macro_block_handle1;
  ObMacroBlockHandle macro_block_handle2;
  ObMacroBlockHandle macro_block_handle3;
  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_block_handle1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_block_handle2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_block_handle3);
  ASSERT_EQ(OB_SUCCESS, ret);
  MacroBlockId macro_block_id1 = macro_block_handle1.get_macro_id();
  ASSERT_EQ(true, macro_block_id1.is_valid());
  MacroBlockId macro_block_id2 = macro_block_handle2.get_macro_id();
  ASSERT_EQ(true, macro_block_id2.is_valid());
  MacroBlockId macro_block_id3 = macro_block_handle3.get_macro_id();
  ASSERT_EQ(true, macro_block_id3.is_valid());

  // 3. switch state op
  ret = block_mgr.write_back_start(block_index1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_start(block_index2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_start(block_index3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_succ(block_index1, macro_block_id1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_succ(block_index2, macro_block_id2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block_mgr.write_back_succ(block_index3, macro_block_id3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. test statistic
  int64_t macro_block_count = 0;
  ret = block_mgr.get_macro_block_count(macro_block_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, macro_block_count);

  ObArray<blocksstable::MacroBlockId> macro_id_list;
  ret = macro_id_list.push_back(macro_block_id1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = macro_id_list.push_back(macro_block_id2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = macro_id_list.push_back(macro_block_id3);
  ASSERT_EQ(OB_SUCCESS, ret);
  std::sort(macro_id_list.begin(), macro_id_list.end());

  ObArray<blocksstable::MacroBlockId> macro_id_list_stat;
  ret = block_mgr.get_macro_block_list(macro_id_list_stat);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(macro_id_list.count(), macro_id_list_stat.count());
  std::sort(macro_id_list_stat.begin(), macro_id_list_stat.end());
  for (int64_t i = 0; i < macro_id_list.count(); ++i) {
    ASSERT_EQ(true, macro_id_list[i] == macro_id_list_stat[i]);
  }

  int64_t used_page_num = 0;
  macro_block_count = 0;
  ret = block_mgr.get_block_usage_stat(used_page_num, macro_block_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, macro_block_count);
  ASSERT_EQ(page_num * 3 - 30, used_page_num);

  LOG_INFO("test_block_manager_stat");
}

TEST_F(TestTmpFileBlock, test_block)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlock block;
  int64_t block_index = 1;
  int64_t begin_page_id = 0;
  int64_t page_num = 0;

  begin_page_id = -1;
  page_num = ObTmpFileGlobal::BLOCK_PAGE_NUMS;
  ret = block.init_block(block_index, begin_page_id, page_num);
  ASSERT_NE(OB_SUCCESS, ret);

  begin_page_id = 0;
  page_num = ObTmpFileGlobal::BLOCK_PAGE_NUMS + 1;
  ret = block.init_block(block_index, begin_page_id, page_num);
  ASSERT_NE(OB_SUCCESS, ret);
  begin_page_id = 0;

  begin_page_id = 0;
  page_num = 0;
  ret = block.init_block(block_index, begin_page_id, page_num);
  ASSERT_NE(OB_SUCCESS, ret);

  begin_page_id = 10;
  page_num = ObTmpFileGlobal::BLOCK_PAGE_NUMS;
  ret = block.init_block(block_index, begin_page_id, page_num);
  ASSERT_NE(OB_SUCCESS, ret);

  begin_page_id = 10;
  page_num = ObTmpFileGlobal::BLOCK_PAGE_NUMS-20;
  ret = block.init_block(block_index, begin_page_id, page_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMacroBlockHandle macro_block_handle1;
  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_block_handle1);
  ASSERT_EQ(OB_SUCCESS, ret);
  MacroBlockId macro_block_id1 = macro_block_handle1.get_macro_id();
  ASSERT_EQ(true, macro_block_id1.is_valid());

  ret = block.write_back_failed();
  ASSERT_NE(OB_SUCCESS, ret);
  ret = block.write_back_succ(macro_block_id1);
  ASSERT_NE(OB_SUCCESS, ret);

  int64_t end_page_id = begin_page_id + page_num - 1;
  ret = block.release_pages(0, 15);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = block.release_pages(end_page_id - 30, 35);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = block.release_pages(9, 1);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = block.release_pages(end_page_id + 1, 1);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = block.release_pages(end_page_id, 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(false, block.on_disk());
  int64_t used_page_num = 0;
  ret = block.get_page_usage(used_page_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(page_num - 1, used_page_num);
  bool can_remove = false;
  ret = block.can_remove(can_remove);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, can_remove);

  ret = block.write_back_start();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block.write_back_succ(macro_block_id1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, block.on_disk());
  ret = block.get_page_usage(used_page_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(page_num - 1, used_page_num);
  ret = block.release_pages(10, used_page_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = block.can_remove(can_remove);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, can_remove);

  LOG_INFO("test_block");
}

TEST_F(TestTmpFileBlock, test_block_page_bit_map)
{
  int ret = OB_SUCCESS;
  bool value;
  ObTmpFileBlockPageBitmap bitmap;
  // 1. get/set for one bit of bitmap
  ret = bitmap.get_value(7, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);
  ret = bitmap.get_value(16, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);

  ret = bitmap.set_bitmap(7, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = bitmap.set_bitmap(16, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = bitmap.get_value(7, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, value);
  ret = bitmap.get_value(16, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, value);

  ret = bitmap.get_value(bitmap.get_capacity(), value);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = bitmap.get_value(-1, value);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = bitmap.set_bitmap(bitmap.get_capacity(), true);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = bitmap.set_bitmap(-1, true);
  ASSERT_NE(OB_SUCCESS, ret);

  // 2. set batch bits of bitmap
  int64_t start = 13;
  int64_t end = 80;
  ret = bitmap.get_value(start, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);
  ret = bitmap.get_value(40, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);
  ret = bitmap.get_value(end, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);

  ret = bitmap.set_bitmap_batch(start, end - start + 1, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = start; i <= end; i++) {
    ret = bitmap.get_value(i, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(true, value);
  }
  ret = bitmap.get_value(start - 1, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);
  ret = bitmap.get_value(end + 1, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);

  start = 5;
  end = 40;
  ret = bitmap.set_bitmap_batch(start , end - start + 1, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = start; i <= end; i++) {
    ret = bitmap.get_value(i, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(true, value);
  }

  start = 60;
  end = 100;
  ret = bitmap.set_bitmap_batch(start, end - start + 1, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = start; i <= end; i++) {
    ret = bitmap.get_value(i, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(false, value);
  }

  ret = bitmap.set_bitmap_batch(-1, 100, true);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = bitmap.set_bitmap_batch(0, bitmap.get_capacity() + 1, true);
  ASSERT_NE(OB_SUCCESS, ret);
  // 3. test is_all_true() and is_all_false()
  ret = bitmap.is_all_false(value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);
  ret = bitmap.is_all_true(value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);

  bitmap.reset();

  ret = bitmap.is_all_false(value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, value);
  ret = bitmap.is_all_true(value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);

  start = 60;
  end = 100;
  ret = bitmap.set_bitmap_batch(start, end - start + 1 , true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = bitmap.is_all_true(start, end, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, value);
  ret = bitmap.is_all_true(start - 1, end, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);
  ret = bitmap.is_all_true(start, end + 1, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);

  ret = bitmap.is_all_true(-1, end, value);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = bitmap.is_all_true(100, bitmap.get_capacity(), value);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = bitmap.set_bitmap_batch(0, bitmap.get_capacity(), true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = bitmap.is_all_true(value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, value);
  ret = bitmap.is_all_false(value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);

  start = 60;
  end = 100;
  ret = bitmap.set_bitmap_batch(start, end - start + 1 , false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = bitmap.is_all_false(start, end, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, value);
  ret = bitmap.is_all_false(start - 1, end, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);
  ret = bitmap.is_all_false(start, end + 1, value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, value);

  ret = bitmap.is_all_false(-1, end, value);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = bitmap.is_all_false(100, bitmap.get_capacity(), value);
  ASSERT_NE(OB_SUCCESS, ret);
  LOG_INFO("test_block_page_bit_map");
}

TEST_F(TestTmpFileBlock, test_block_page_bit_map_iter)
{
  int ret = OB_SUCCESS;
  bool value;
  ObTmpFileBlockPageBitmap bitmap;
  int64_t start_page_id = 0;
  int64_t end_page_id = bitmap.get_capacity() - 1;
  ObTmpFileBlockPageBitmapIterator iter;
  ret = iter.init(nullptr, start_page_id, end_page_id);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = iter.init(&bitmap, -1, end_page_id);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = iter.init(&bitmap, start_page_id, bitmap.get_capacity());
  ASSERT_NE(OB_SUCCESS, ret);
  ret = iter.init(&bitmap, start_page_id, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t starts[5] = {0, 10, 84, 163, bitmap.get_capacity() - 1};
  int64_t ends[5] = {0, 40, 101, 230, bitmap.get_capacity() - 1};
  ret = bitmap.set_bitmap(starts[0], true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = bitmap.set_bitmap_batch(starts[1], ends[1] - starts[1] + 1, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = bitmap.set_bitmap_batch(starts[2], ends[2] - starts[2] + 1, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = bitmap.set_bitmap_batch(starts[3], ends[3] - starts[3] + 1, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = bitmap.set_bitmap(starts[4], true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(true, iter.has_next());

  int i = 0;
  while (iter.has_next()) {
    bool value = false;
    ret = iter.next_range(value, start_page_id, end_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (value) {
      ASSERT_EQ(start_page_id, starts[i]);
      ASSERT_EQ(end_page_id, ends[i]);
      i++;
    } else {
      ASSERT_NE(i, 0);
      ASSERT_EQ(start_page_id, ends[i-1]+1);
      ASSERT_EQ(end_page_id, starts[i]-1);
    }
  }

  iter.reset();
  ret = iter.init(&bitmap, 5, 180);
  ASSERT_EQ(OB_SUCCESS, ret);
  ends[0] = 4;
  ends[3] = 180;
  i = 1;
  while (iter.has_next()) {
    bool value = false;
    ret = iter.next_range(value, start_page_id, end_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (value) {
      ASSERT_EQ(start_page_id, starts[i]);
      ASSERT_EQ(end_page_id, ends[i]);
      i++;
    } else {
      ASSERT_EQ(start_page_id, ends[i-1]+1);
      ASSERT_EQ(end_page_id, starts[i]-1);
    }
  }
  LOG_INFO("test_block_page_bit_map_iter");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_block_manager.log*");
  OB_LOGGER.set_file_name("test_tmp_file_block_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}