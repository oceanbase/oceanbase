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

#include "storage/memtable/ob_memtable_mutator.h"

#include "lib/allocator/page_arena.h"

#include "utils_rowkey_builder.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

TEST(TestObMemtableMutator, smoke_test)
{
  static const int64_t BUFFER_SIZE = 1L<<21;
  static const int64_t INIT_POS_SIZE = 1L<<19;

  ObMemtableMutatorWriter mmw;
  ObMemtableMutatorIterator mmi;

  int ret = OB_SUCCESS;
  char *buffer = new char[BUFFER_SIZE];
  memset(buffer, '$', INIT_POS_SIZE);
  buffer[1] = '\0';
  mmw.set_buffer(buffer + INIT_POS_SIZE, BUFFER_SIZE - INIT_POS_SIZE);
  ObRowData new_row;
  ObRowData old_row;
  new_row.set(buffer, INIT_POS_SIZE);

  RK rk1(
    V("hello", 5),
    V(NULL, 0),
    I(1024),
    N("3.14")
    );

  RK rk2(
    V("world", 5),
    V(NULL, 0),
    I(1024),
    N("3.14")
    );

  ret = mmw.append_kv(1001, rk1.get_rowkey(), 1, 1, new_row, old_row, storage::T_DML_INSERT, 1, 0, 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = mmw.append_kv(1002, rk2.get_rowkey(), 2, 2, new_row, old_row, storage::T_DML_UPDATE, 2, 0, 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = mmw.append_kv(1002, rk2.get_rowkey(), 2, 2, new_row, old_row, storage::T_DML_DELETE, 3, 0, 1);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);

  int64_t res_len = 0;
  ret = mmw.serialize(0, res_len);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_LT(1024, res_len);

  int64_t res_pos = 0;
  ret = mmi.deserialize(buffer + INIT_POS_SIZE, res_len, res_pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObRowData got_row;
  uint64_t index_id = 0;
  ObStoreRowkey rowkey;
  int64_t schema_version = 0;
  storage::ObRowDml dml_type = storage::T_DML_UNKNOWN;
  uint32_t modify_count = 0;
  uint32_t acc_checksum = 0;
  int64_t memtable_version = 0;
  ret = mmi.get_next_row(index_id, rowkey, schema_version, got_row, dml_type, modify_count, acc_checksum, memtable_version);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1001UL, index_id);
  EXPECT_EQ(rk1.get_rowkey(), rowkey);
  EXPECT_EQ(1, schema_version);
  EXPECT_EQ(got_row, new_row);
  EXPECT_EQ(storage::T_DML_INSERT, dml_type);
  EXPECT_EQ(1U, modify_count);

  ret = mmi.get_next_row(index_id, rowkey, schema_version, got_row, dml_type, modify_count, acc_checksum, memtable_version);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1002UL, index_id);
  EXPECT_EQ(rk2.get_rowkey(), rowkey);
  EXPECT_EQ(2, schema_version);
  EXPECT_EQ(got_row, new_row);
  EXPECT_EQ(storage::T_DML_UPDATE, dml_type);
  EXPECT_EQ(2U, modify_count);

  ret = mmi.get_next_row(index_id, rowkey, schema_version, got_row, dml_type, modify_count, acc_checksum, memtable_version);
  EXPECT_EQ(OB_ITER_END, ret);


  ObMemtableMutatorIterator mmi_row;
  ObMemtableMutatorRow row;

  res_pos = 0;
  ret = mmi_row.deserialize(buffer + INIT_POS_SIZE, res_len, res_pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = mmi_row.get_next_row(row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1001UL, row.table_id_);
  EXPECT_EQ(rk1.get_rowkey(), row.rowkey_);
  EXPECT_EQ(new_row, row.new_row_);
  EXPECT_TRUE(ObDmlFlag::DF_INSERT == row.dml_flag_);
  EXPECT_EQ(1U, row.update_seq_);

  row.reset();
  ret = mmi_row.get_next_row(row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1002UL, row.table_id_);
  EXPECT_EQ(rk2.get_rowkey(), row.rowkey_);
  EXPECT_EQ(new_row, row.new_row_);
  EXPECT_TRUE(ObDmlFlag::DF_UPDATE == row.dml_flag_);
  EXPECT_EQ(2U, row.update_seq_);

  row.reset();
  EXPECT_TRUE(mmi_row.is_iter_end());
  ret = mmi_row.get_next_row(row);
  EXPECT_EQ(OB_ITER_END, ret);

  delete[] buffer;
  buffer = NULL;
}

}
}

int main(int argc, char **argv)
{
  //oceanbase::common::ObLogger::get_logger().set_file_name("test_memtable_mutator.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
