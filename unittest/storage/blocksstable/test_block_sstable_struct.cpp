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
#include "storage/blocksstable/ob_data_buffer.h"
#define protected public
#define private public
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace unittest
{
TEST(ObCommitLogSpec, normal)
{
  //is_valid() test
  ObCommitLogSpec log_spec;
  log_spec.log_dir_ = "./";
  log_spec.max_log_file_size_ = 2L * 1024L;
  log_spec.log_sync_type_ = 0;
  ASSERT_TRUE(log_spec.is_valid());
  log_spec.log_dir_ = NULL;
  ASSERT_FALSE(log_spec.is_valid());
  //to_string() test
  const char *out = to_cstring(log_spec);
  ASSERT_STRNE(NULL, out);
}

TEST(ObStorageEnv, normal)
{
  //to_string() test
  ObStorageEnv env;
  const char *out = to_cstring(env);
  ASSERT_STRNE(NULL, out);
}

TEST(ObMicroBlockHeader, normal)
{
  //to_string() test
  ObMicroBlockHeader micro_header;
  const char *out = to_cstring(micro_header);
  ASSERT_STRNE(NULL, out);
}

TEST(ObMacroBlockCommonHeader, normal)
{
  //check() test
  ObMacroBlockCommonHeader common_header;
  common_header.set_attr(ObMacroBlockCommonHeader::LinkedBlock);
  ASSERT_TRUE(common_header.is_valid());
  //to_string() test
  const char *out = to_cstring(common_header);
  ASSERT_STRNE(NULL, out);
  //serialization length test
  ASSERT_EQ(common_header.header_size_, common_header.get_serialize_size());
}

TEST(ObSSTableMacroBlockHeader, normal)
{
  //to_string() test
  ObSSTableMacroBlockHeader sstable_header;
  const char *out = to_cstring(sstable_header);
  ASSERT_STRNE(NULL, out);
}
}//blocksstable
}//oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40L << 30);
  return RUN_ALL_TESTS();
}

