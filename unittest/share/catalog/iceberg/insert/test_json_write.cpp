/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
#include "src/share/ob_define.h"
#include "src/sql/table_format/iceberg/spec/schema_field.h"
#include "src/sql/table_format/iceberg/spec/type.h"
#include "src/sql/table_format/iceberg/spec/manifest.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql::iceberg;
class TestJsonWrite : public ::testing::Test
{
public:
  TestJsonWrite() = default;
  ~TestJsonWrite() = default;
  ObArenaAllocator allocator_;
};

TEST_F(TestJsonWrite, test_schema_field_to_json)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = DEFAULT_BUF_LENGTH;
  int64_t pos = 0;
  ObString res;
  StructType test_struct = StructType({&DataFile::REQUIRED_CONTENT_FIELD, &DataFile::FILE_PATH_FIELD, &DataFile::FILE_FORMAT_FIELD});
  test_struct.set_schema_id(0);
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_len));
  } else if (OB_FAIL(test_struct.to_json_kv_string(buf, buf_len, pos))) {
    LOG_WARN("failed to convert struct to json", K(ret));
  } else {
    res.assign_ptr(buf, pos);
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
