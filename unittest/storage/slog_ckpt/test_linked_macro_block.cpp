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

#include "lib/container/ob_array.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/ob_simple_mem_limit_getter.h"
#include <gtest/gtest.h>
#define private public
#include "blocksstable/ob_data_file_prepare.h"
#include "storage/slog_ckpt/ob_linked_macro_block_reader.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#undef private

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;
static ObSimpleMemLimitGetter getter;

namespace unittest
{

class TestLinkedMacroBlock : public TestDataFilePrepare
{
public:
  TestLinkedMacroBlock();
  virtual ~TestLinkedMacroBlock() = default;
  virtual void SetUp();
  virtual void TearDown();

  struct ItemInfo
  {
    char *buf_{nullptr};
    int64_t buf_len_{0};
    int64_t idx_{0};
    ObMetaDiskAddr addr_{};
    int32_t crc_{0};

    TO_STRING_KV(KP_(buf), K_(buf_len), K_(idx), K_(addr), K_(crc));
  };

protected:
  void build_test_data_buf();
  void build_item_buf(ObArray<ItemInfo> &item_arr);
  void write_items(ObArray<ItemInfo> &item_arr);
  void iter_read_items(const ObArray<ItemInfo> &item_arr);
  void read_items(const ObArray<ItemInfo> &item_arr);

protected:
  static const int64_t MACRO_BLOCK_SIZE = 128 * 1024;
  static const int64_t MAX_TEST_SIZE = 16 * MACRO_BLOCK_SIZE;

  ObArenaAllocator allocator_{ObModIds::TEST};
  char *data_buf_{nullptr};
  MacroBlockId entry_block_{};
  ObMetaBlockListHandle block_handle_;
};

TestLinkedMacroBlock::TestLinkedMacroBlock()
  : TestDataFilePrepare(&getter, "linked_macro_block_test", MACRO_BLOCK_SIZE)
{
  build_test_data_buf();
}

void TestLinkedMacroBlock::SetUp()
{
  TestDataFilePrepare::SetUp();
  ObTenantBase tenant_ctx(OB_SERVER_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
}

void TestLinkedMacroBlock::TearDown()
{
  TestDataFilePrepare::TearDown();
}

void TestLinkedMacroBlock::build_test_data_buf()
{
  data_buf_ = static_cast<char *>(allocator_.alloc(MAX_TEST_SIZE));
  ASSERT_NE(nullptr, data_buf_);

  int64_t long_integer_cnt = MAX_TEST_SIZE / sizeof(int64_t);
  int64_t *p = reinterpret_cast<int64_t *>(data_buf_);
  for (int64_t i = 0; i < long_integer_cnt; i++) {
    *p = i;
    ++p;
  }
}

void TestLinkedMacroBlock::build_item_buf(ObArray<ItemInfo> &item_arr)
{
  for (int64_t i = 0; i < item_arr.count(); i++) {
    char *&buf = item_arr.at(i).buf_;
    int64_t &buf_len = item_arr.at(i).buf_len_;
    ASSERT_TRUE(buf_len <= MAX_TEST_SIZE);
    buf = static_cast<char *>(allocator_.alloc(buf_len));
    ASSERT_NE(nullptr, buf);
    MEMCPY(buf, data_buf_, buf_len);
    item_arr.at(i).crc_ = static_cast<int32_t>(ob_crc64(buf, buf_len));
  }
}

void TestLinkedMacroBlock::write_items(ObArray<ItemInfo> &item_arr)
{
  ObLinkedMacroBlockItemWriter item_writer;
  ObMemAttr mem_attr(MTL_ID(), "test");
  ASSERT_EQ(OB_SUCCESS, item_writer.init(true, mem_attr));
  for (int64_t i = 0; i < item_arr.count(); i++) {
    ASSERT_EQ(OB_SUCCESS,
      item_writer.write_item(item_arr.at(i).buf_, item_arr.at(i).buf_len_, &item_arr.at(i).idx_));
  }
  ASSERT_EQ(OB_SUCCESS, item_writer.close());
  for (int64_t i = 0; i < item_arr.count(); i++) {
    ASSERT_EQ(
      OB_SUCCESS, item_writer.get_item_disk_addr(item_arr.at(i).idx_, item_arr.at(i).addr_));
    LOG_INFO("item addr", K(i), K(item_arr.at(i).addr_));
  }

  ObIArray<MacroBlockId> &block_list = item_writer.get_meta_block_list();

  block_handle_.add_macro_blocks(block_list);

  ASSERT_EQ(OB_SUCCESS, item_writer.get_entry_block(entry_block_));
}

void TestLinkedMacroBlock::iter_read_items(const ObArray<ItemInfo> &item_arr)
{
  ObLinkedMacroBlockItemReader item_reader;
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "test");
  ASSERT_EQ(OB_SUCCESS, item_reader.init(entry_block_, mem_attr));
  char *item_buf = nullptr;
  int64_t item_buf_len = 0;
  ObMetaDiskAddr addr;
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  while (true) {
    ret = item_reader.get_next_item(item_buf, item_buf_len, addr);
    if (OB_SUCC(ret)) {
      int32_t calc_crc = static_cast<int32_t>(ob_crc64(item_buf, item_buf_len));
      ASSERT_EQ(item_arr.at(idx).crc_, calc_crc);

      LOG_INFO("check addr", K(item_arr.at(idx).addr_), K(addr));
      ASSERT_EQ(item_arr.at(idx).addr_, addr);
      idx++;
    } else {
      break;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(idx, item_arr.count());
}

void TestLinkedMacroBlock::read_items(const ObArray<ItemInfo> &item_arr)
{
  ObLinkedMacroBlockItemReader item_reader;
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "test");
  ASSERT_EQ(OB_SUCCESS, item_reader.init(entry_block_, mem_attr));
  char *item_buf = nullptr;
  int64_t item_buf_len = 0;
  for (int64_t i = 0; i < item_arr.count(); i++) {
    item_buf = static_cast<char *>(allocator_.alloc(item_arr.at(i).addr_.size_));
    ASSERT_NE(nullptr, item_buf);
    ASSERT_EQ(OB_SUCCESS,
      item_reader.read_item(
        block_handle_.get_meta_block_list(), item_arr.at(i).addr_, item_buf, item_buf_len));
    int32_t calc_crc = static_cast<int32_t>(ob_crc64(item_buf, item_buf_len));
    ASSERT_EQ(item_arr.at(i).crc_, calc_crc);
  }
}

TEST_F(TestLinkedMacroBlock, empty_write_test)
{
  ObArray<ItemInfo> item_arr;
  ASSERT_NO_FATAL_FAILURE(write_items(item_arr));
  ASSERT_EQ(entry_block_, ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK);
}

TEST_F(TestLinkedMacroBlock, empty_read_test)
{
  ObArray<ItemInfo> item_arr;
  entry_block_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  ASSERT_TRUE(entry_block_.is_valid());
  ObLinkedMacroBlockItemReader item_reader;
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "test");
  ASSERT_EQ(OB_SUCCESS, item_reader.init(entry_block_, mem_attr));
  char *item_buf = nullptr;
  int64_t item_buf_len = 0;
  ObMetaDiskAddr addr;
  ASSERT_EQ(OB_ITER_END, item_reader.get_next_item(item_buf, item_buf_len, addr));
}

// one item just takes up the whole macro block space
TEST_F(TestLinkedMacroBlock, single_item_full_block_test)
{
  ObArray<ItemInfo> item_arr;
  ItemInfo item;
  item.buf_len_ = MACRO_BLOCK_SIZE - sizeof(ObMacroBlockCommonHeader) -
    sizeof(ObLinkedMacroBlockHeader) - sizeof(ObLinkedMacroBlockItemHeader);
  item_arr.push_back(item);
  ASSERT_NO_FATAL_FAILURE(build_item_buf(item_arr));

  ASSERT_NO_FATAL_FAILURE(write_items(item_arr));
  ASSERT_TRUE(entry_block_.is_valid());
  ASSERT_NO_FATAL_FAILURE(read_items(item_arr));
  ASSERT_NO_FATAL_FAILURE(iter_read_items(item_arr));
}

TEST_F(TestLinkedMacroBlock, multi_small_item_test)
{
  ObArray<ItemInfo> item_arr;
  ItemInfo item;
  item.buf_len_ = 1;
  item_arr.push_back(item);
  item.buf_len_ = MACRO_BLOCK_SIZE / 8;
  item_arr.push_back(item);
  item.buf_len_ = MACRO_BLOCK_SIZE / 4;
  item_arr.push_back(item);
  item.buf_len_ = MACRO_BLOCK_SIZE / 2;
  item_arr.push_back(item);
  item.buf_len_ = MACRO_BLOCK_SIZE / 2;
  item_arr.push_back(item);
  ASSERT_NO_FATAL_FAILURE(build_item_buf(item_arr));

  ASSERT_NO_FATAL_FAILURE(write_items(item_arr));
  ASSERT_TRUE(entry_block_.is_valid());
  ASSERT_NO_FATAL_FAILURE(read_items(item_arr));
  ASSERT_NO_FATAL_FAILURE(iter_read_items(item_arr));
}

TEST_F(TestLinkedMacroBlock, multi_large_item_test)
{
  ObArray<ItemInfo> item_arr;
  ItemInfo item;
  item.buf_len_ = MACRO_BLOCK_SIZE;
  item_arr.push_back(item);
  item.buf_len_ = 2 * MACRO_BLOCK_SIZE;
  item_arr.push_back(item);
  item.buf_len_ = 3 * MACRO_BLOCK_SIZE;
  item_arr.push_back(item);
  ASSERT_NO_FATAL_FAILURE(build_item_buf(item_arr));

  ASSERT_NO_FATAL_FAILURE(write_items(item_arr));
  ASSERT_TRUE(entry_block_.is_valid());
  ASSERT_NO_FATAL_FAILURE(read_items(item_arr));
  ASSERT_NO_FATAL_FAILURE(iter_read_items(item_arr));
}

TEST_F(TestLinkedMacroBlock, hybrid_small_and_large_item_test)
{
  ObArray<ItemInfo> item_arr;
  ItemInfo item;
  item.buf_len_ = MACRO_BLOCK_SIZE;
  item_arr.push_back(item);
  item.buf_len_ = 101;
  item_arr.push_back(item);
  item.buf_len_ = 2 * MACRO_BLOCK_SIZE;
  item_arr.push_back(item);
  item.buf_len_ = 202;
  item_arr.push_back(item);
  item.buf_len_ = 3 * MACRO_BLOCK_SIZE;
  item_arr.push_back(item);
  item.buf_len_ = 303;
  item_arr.push_back(item);
  ASSERT_NO_FATAL_FAILURE(build_item_buf(item_arr));

  ASSERT_NO_FATAL_FAILURE(write_items(item_arr));
  ASSERT_TRUE(entry_block_.is_valid());
  ASSERT_NO_FATAL_FAILURE(read_items(item_arr));
  ASSERT_NO_FATAL_FAILURE(iter_read_items(item_arr));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_linked_macro_block.log*");
  OB_LOGGER.set_file_name("test_linked_macro_block.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
