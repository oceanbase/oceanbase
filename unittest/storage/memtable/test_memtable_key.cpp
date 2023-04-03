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

#include "storage/memtable/ob_memtable_key.h"

#include "common/cell/ob_cell_writer.h"
#include "common/cell/ob_cell_reader.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

#include "utils_rowkey_builder.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

TEST(TestObMemtableKey, test_encode_decode)
{
  uint64_t table_id = 1024;
  RK rk(
      V("wORLD", 5),
      V(NULL, 0),
      I(-1024),
      //F(static_cast<float>(3.14)),
      N("-3.14"),
      U(),
      OBMIN(),
      OBMAX());

  CD cd(
      100, ObVarcharType, CS_TYPE_UTF8_GENERAL_CI,
      101, ObVarcharType, CS_TYPE_UTF8MB4_BIN,
      102, ObIntType, CS_TYPE_UTF8MB4_BIN,
      //103, ObFloatType, CS_TYPE_UTF8MB4_BIN,
      103, ObNumberType, CS_TYPE_UTF8MB4_BIN,
      104, ObNumberType, CS_TYPE_UTF8MB4_BIN,
      105, ObNumberType, CS_TYPE_UTF8MB4_BIN,
      106, ObNumberType, CS_TYPE_UTF8MB4_BIN
      );

  ObStoreRowkeyXcodeBuffer eb;
  ObMemtableKey mtk;
  int ret = OB_SUCCESS;

  ret = mtk.encode(table_id, cd.get_columns(), rk.get_rowkey(), eb);
  EXPECT_EQ(OB_SUCCESS, ret);

  CharArena allocator;
  uint64_t res_table_id = OB_INVALID_ID;
  ObStoreRowkey res_rowkey;
  bool allocate_rowkey_objs = true;
  ret = mtk.decode(res_table_id, res_rowkey, allocator, allocate_rowkey_objs);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(res_rowkey, rk.get_rowkey());

  ObMemtableKey *nil = NULL;
  ObMemtableKey *dup_mtk = mtk.dup(allocator);
  EXPECT_NE(nil, dup_mtk);
  ret = dup_mtk->decode(res_table_id, res_rowkey, allocator, allocate_rowkey_objs);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(res_rowkey, rk.get_rowkey());

  fprintf(stdout, "orig_rowkey=[%s]\n", to_cstring(rk.get_rowkey()));
  fprintf(stdout, "res_rowkey=[%s]\n", to_cstring(res_rowkey));
  fprintf(stdout, "orig_mtk=[%s]\n", to_cstring(mtk));
  fprintf(stdout, "dup_mtk=[%s]\n", to_cstring(*dup_mtk));
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_memtable_key.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
