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
#include "ob_sstable_test.h"

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestSSTable : public ObSSTableTest {
public:
  TestSSTable();
  virtual ~TestSSTable();
};

TestSSTable::TestSSTable() : ObSSTableTest()
{}

TestSSTable::~TestSSTable()
{}

TEST_F(TestSSTable, macro_block_iter)
{
  int ret = OB_SUCCESS;
  ObMacroBlockIterator macro_iter;
  ObMacroBlockDesc block_desc;

  // invalid argument
  ret = macro_iter.init(NULL);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = macro_iter.get_next_macro_block(block_desc);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal argument
  ret = macro_iter.init(&sstable_.get_macro_blocks());
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < sstable_.get_macro_blocks().count(); ++i) {
    ret = macro_iter.get_next_macro_block(block_desc);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = macro_iter.get_next_macro_block(block_desc);
  ASSERT_EQ(OB_ITER_END, ret);
  ret = macro_iter.get_next_macro_block(block_desc);
  ASSERT_EQ(OB_ITER_END, ret);

  // reset iter and get
  macro_iter.reset();
  ret = macro_iter.get_next_macro_block(block_desc);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST_F(TestSSTable, init)
{
  int ret = OB_SUCCESS;

  // repeatedly init
  ret = sstable_.init(table_key_);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST_F(TestSSTable, open)
{
  int ret = OB_SUCCESS;
  // ObSSTable sstable;
  ObSSTableBaseMeta sstable_meta;
  ObTableSchema table_schema;

  // fake meta
  sstable_meta.index_id_ = 1;
  sstable_meta.row_count_ = 1;
  sstable_meta.occupy_size_ = 1;
  sstable_meta.data_checksum_ = 1;
  sstable_meta.row_checksum_ = 1;
  sstable_meta.data_version_ = 1;
  sstable_meta.rowkey_column_count_ = 1;
  sstable_meta.table_type_ = 1;
  sstable_meta.index_type_ = 0;
  sstable_meta.available_version_ = 1;
  sstable_meta.macro_block_count_ = 1;
  sstable_meta.use_old_macro_block_count_ = 0;
  sstable_meta.column_cnt_ = 2;
  sstable_meta.total_sstable_count_ = 1;
  sstable_meta.max_logic_block_index_ = -1;
  sstable_meta.set_allocator(allocator_);
  sstable_meta.set_capacity(sstable_meta.column_cnt_);

  for (int64_t i = 0; i < sstable_meta.column_cnt_; ++i) {
    ObSSTableColumnMeta column_meta;
    column_meta.column_id_ = 16 + i;
    column_meta.column_checksum_ = 1;
    column_meta.column_default_checksum_ = 1;
    ASSERT_EQ(OB_SUCCESS, sstable_meta.column_metas_.push_back(column_meta));
  }

  // open when not init
  {
    ObSSTable sstable;
    ret = sstable.open(sstable_meta);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  {
    ObSSTable sstable;
    ret = sstable.open(sstable_meta);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  // invalid argument
  {
    ObSSTable sstable;
    ret = sstable.init(table_key_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sstable.open(sstable_meta);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  {
    ObSSTable sstable;
    ret = sstable.init(table_key_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sstable.open(sstable_meta);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  // normal open
  {
    ObSSTable sstable;
    ret = sstable.init(table_key_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sstable.open(sstable_meta);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // normal open with sstable meta
  // sstable.destroy();
  {
    ObSSTable sstable;
    ret = sstable.init(table_key_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sstable.open(sstable_meta);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestSSTable, append)
{
  int ret = OB_SUCCESS;
  ObSSTable sstable;
  ObStoreRow row;
  MacroBlockId block_id;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);

  // invalid append when not init
  ret = sstable.append(block_id);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid append when not opened
  ret = sstable.init(table_key_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable.append(block_id);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid row and block id
  ObCreateSSTableParamWithTable sstable_param;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key.pkey_.init(1, 0, 0);
  table_key.table_id_ = 1;
  table_key.version_ = 1;
  sstable_param.table_key_ = table_key;
  sstable_param.logical_data_version_ = table_key.version_;
  sstable_param.schema_ = &table_schema_;
  sstable_param.schema_version_ = table_schema_.get_schema_version();
  sstable_param.create_snapshot_version_ = 1L;
  sstable_param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
  ret = sstable.open(sstable_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  block_id.write_seq_ = 10;
  ret = sstable.append(block_id);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal append
  ret = sstable.append(sstable_.get_macro_blocks().at(0));
  ASSERT_EQ(OB_SUCCESS, ret);

  // close sstable
  ret = sstable.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  // invalid append when have closed
  ret = sstable.append(block_id);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST_F(TestSSTable, serialize)
{
  int ret = OB_SUCCESS;
  ObSSTable sstable;
  const int64_t size = sstable_.get_serialize_size();
  char* buf = static_cast<char*>(allocator_.alloc(size));
  ASSERT_TRUE(NULL != buf);
  int64_t pos = 0;

  // invalid argument
  ret = sstable_.serialize(NULL, size, pos);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = sstable_.serialize(buf, 1, pos);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal serialize
  pos = 0;
  ret = sstable_.serialize(buf, size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, size);

  // deserialize when not init
  ret = sstable.deserialize(buf, size, pos);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid deserialize argument
  pos = 0;
  ret = sstable.deserialize(NULL, size, pos);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = sstable.deserialize(buf, 1, pos);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal deserialize
  pos = 0;
  ret = sstable.init(table_key_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = sstable.deserialize(buf, size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, size);
  ASSERT_TRUE(sstable.get_meta() == sstable_.get_meta());
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable.log*");
  OB_LOGGER.set_file_name("test_sstable.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_sstable");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
