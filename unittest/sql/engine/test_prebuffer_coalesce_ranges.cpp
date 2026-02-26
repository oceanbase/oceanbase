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

#define USING_LOG_PREFIX COMMON
#include <gtest/gtest.h>

#include "src/observer/omt/ob_tenant_config_mgr.h"
#include "src/sql/engine/table/ob_file_prebuffer.h"

#define private public

using namespace std;
namespace oceanbase
{
namespace sql
{
class ObTestFilePreBuffer : public ::testing::Test
{
public:
  friend class ObFilePreBuffer;
  ObTestFilePreBuffer()
  {}
  ~ObTestFilePreBuffer()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObTestFilePreBuffer);
};

TEST(ObTestFilePreBuffer, coalesce_column_ranges)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_SYS_TENANT_ID;
  ObExternalFileAccess file_reader;
  ObFilePreBuffer file_pre_buffer(tenant_id, file_reader);

  ObFilePreBuffer::ColumnRangeSlicesList column_range_slices_list;
  ObFilePreBuffer::CoalesceColumnRangesList column_range_entry_list;
  ObFilePreBuffer::ColumnRangeSlices col1_range_slices;
  ObFilePreBuffer::ColumnRangeSlices col2_range_slices;
  ObFilePreBuffer::ColumnRangeSlices col3_range_slices;

  OZ (col1_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(1, 100)));
  OZ (col1_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(101, 100)));
  OZ (col1_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(201, 100)));
  OZ (col1_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(301, 100)));

  OZ (col2_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(500, 100)));
  OZ (col2_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(601, 100)));
  OZ (col2_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(701, 100)));
  OZ (col2_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(801, 100)));

  OZ (col3_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(1000, 100)));
  OZ (col3_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(1101, 100)));
  OZ (col3_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(1201, 100)));
  OZ (col3_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(1301, 100)));
  OZ (col3_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(1801, 200)));
  OZ (col3_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(2201, 200)));
  OZ (col3_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(2601, 200)));
  OZ (col3_range_slices.range_list_.push_back(ObFilePreBuffer::ReadRange(2901, 500)));

  OZ (column_range_slices_list.push_back(&col1_range_slices));
  OZ (column_range_slices_list.push_back(&col2_range_slices));
  OZ (column_range_slices_list.push_back(&col3_range_slices));


  ObFilePreBuffer::CacheOptions options;
  options.hole_size_limit_ = 500;
  options.range_size_limit_ = 1000;
  options.lazy_ = true;
  options.prefetch_limit_ = 2;

  OZ (file_pre_buffer.init(options));
  OX (file_pre_buffer.set_timeout_timestamp(INT64_MAX));
  OZ (file_pre_buffer.coalesce_ranges(column_range_slices_list, column_range_entry_list));
}

}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  int ret = RUN_ALL_TESTS();
  return ret;
}
