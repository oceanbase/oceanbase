// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include <errno.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#define protected public
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
using namespace std;

namespace unittest
{

class TestAggRow : public ::testing::Test
{
public:
  TestAggRow();
  virtual ~TestAggRow();
  virtual void SetUp();
  virtual void TearDown();
protected:
  ObArenaAllocator allocator_;
};

TestAggRow::TestAggRow()
    : allocator_()
{
}
TestAggRow::~TestAggRow()
{
}

void TestAggRow::SetUp()
{
}

void TestAggRow::TearDown()
{
}

TEST_F(TestAggRow, test_agg_row)
{
  int ret = OB_SUCCESS;
  int test_cnt = 10;
  bool has[10][5];
  memset(has, false, sizeof(has));
  ObArray<ObSkipIndexColMeta> agg_cols;
  ObArray<bool> is_null;
  ObDatumRow agg_row;
  OK(agg_row.init(test_cnt));
  int cnt = 0;
  while (cnt < test_cnt) {
    ObSkipIndexColMeta col_meta;
    uint32_t col_idx = rand() % 5;
    uint32_t col_type = rand() % ObSkipIndexColType::SK_IDX_MAX_COL_TYPE;
    if (!has[col_idx][col_type]) {
      col_meta.col_idx_ = col_idx;
      col_meta.col_type_ = col_type;
      OK(agg_cols.push_back(col_meta));
      bool null = (rand() % 5 == 0);
      if (null) {
        agg_row.storage_datums_[cnt].set_null();
      } else {
        agg_row.storage_datums_[cnt].set_int(cnt);
      }
      OK(is_null.push_back(null));
      has[col_idx][col_type] = true;
      ++cnt;
    }
  }

  ObAggRowWriter row_writer;
  OK(row_writer.init(agg_cols, agg_row, allocator_));
  int64_t buf_size = row_writer.get_data_size();
  char *buf = reinterpret_cast<char *>(allocator_.alloc(buf_size));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, 0, buf_size);
  int64_t pos = 0;
  OK(row_writer.write_agg_data(buf, buf_size, pos));
  ASSERT_GE(buf_size, pos);

  ObAggRowReader row_reader;
  OK(row_reader.init(buf, buf_size));
  for (int i = 0; i < test_cnt; ++i) {
    ObDatum datum;
    OK(row_reader.read(agg_cols.at(i), datum));
    if (is_null.at(i)) {
      ASSERT_TRUE(datum.is_null());
    } else {
      int64_t data = datum.get_int();
      ASSERT_EQ(data, i);
    }
  }
  row_reader.reset();
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_agg_row_struct.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_agg_row_struct.log", true);
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
