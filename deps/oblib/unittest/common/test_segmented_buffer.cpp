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
#include "common/ob_segmented_buffer.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

TEST(TestObSegmentedBuffer, base)
{
  const int block_size = 100;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "MemMeta",
                 ObCtxIds::DEFAULT_CTX_ID);
  ObSegmentedBufffer sb(block_size, attr);
  char buf[block_size * 10];
  int ret = OB_SUCCESS;
  for (int i = 0; i < 10; i++) {
    ret = sb.append(buf + block_size * i, block_size);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ObSegmentedBuffferIterator sbi(sb);
  char *cur_buf = nullptr;
  int64_t len = 0;
  int64_t pos = 0;
  while ((cur_buf = sbi.next(len)) != nullptr) {
    ASSERT_EQ(0, memcmp(cur_buf, buf + pos, len));
    pos += len;
  }

  // test dump to file
  ASSERT_EQ(OB_INVALID_ARGUMENT, sb.dump_to_file(nullptr));
  const static char *file_name = "tmp_file";
  ASSERT_EQ(OB_SUCCESS, sb.dump_to_file(file_name));

  FILE * file = fopen (file_name, "rb" );
  ASSERT_NE(file, nullptr);
  fseek(file, 0, SEEK_END);
  int64_t size = ftell(file);
  rewind(file);
  char *buffer = (char*) malloc (size);
  ASSERT_NE(buffer, nullptr);
  ObSegmentedBufffer new_sb(block_size, attr);
  int64_t real_size = fread(buffer, 1, size, file);
  ASSERT_EQ(real_size, size);
  ASSERT_EQ(OB_SUCCESS, new_sb.append(buffer, size));

  {
    ObSegmentedBuffferIterator sbi(sb);
    ObSegmentedBuffferIterator sbi2(new_sb);
    char *ptr = 0;
    int64_t len = 0;
    while ((ptr = sbi.next(len)) != nullptr) {
      int64_t len2 = 0;
      char *ptr2 = sbi2.next(len2);
      ASSERT_EQ(len2, len);
      ASSERT_EQ(0, MEMCMP(ptr, ptr2, len));
    }
    ASSERT_EQ(nullptr, sbi.next(len));
    ASSERT_EQ(nullptr, sbi2.next(len));
  }

  system("rm -rf tmp_file");
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
