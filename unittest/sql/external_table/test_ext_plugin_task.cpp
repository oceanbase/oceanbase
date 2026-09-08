/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX SQL

#include "plugin/v2/external_table/ob_ext_json_protocol.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"

namespace oceanbase
{
namespace sql
{
namespace unittest
{

using namespace common;
using namespace share;

class TestExtPluginTask : public ::testing::Test
{
protected:
  TestExtPluginTask() : allocator_("ExtPluginUT") {}

  void check_round_trip(const ObPluginScanTask &source)
  {
    const int64_t size = source.get_serialize_size();
    char *buf = static_cast<char *>(allocator_.alloc(size));
    int64_t pos = 0;
    ASSERT_NE(nullptr, buf);
    ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));

    ObPluginScanTask target;
    int64_t decode_pos = 0;
    ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
    EXPECT_EQ(source.reader_type_, target.reader_type_);
    EXPECT_EQ(source.plugin_task_json_, target.plugin_task_json_);
    EXPECT_EQ(source.file_url_, target.file_url_);
    EXPECT_EQ(source.file_size_, target.file_size_);
    EXPECT_EQ(source.part_id_, target.part_id_);
    EXPECT_EQ(source.record_count_, target.record_count_);
  }

  ObArenaAllocator allocator_;
};

TEST_F(TestExtPluginTask, plugin_reader_task_keeps_root_json)
{
  ObOptPluginFile opt_file(allocator_);
  opt_file.reader_type_ = ObPluginReaderType::PLUGIN;
  opt_file.plugin_task_json_ = ObString::make_string(R"({"plugin_split":"eA=="})");
  opt_file.part_id_ = 3;
  opt_file.record_count_ = 11;

  ObPluginScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.init_with_opt_lake_table_file(allocator_, opt_file));
  EXPECT_EQ(ObPluginReaderType::PLUGIN, task.reader_type_);
  EXPECT_FALSE(task.plugin_task_json_.empty());
  EXPECT_TRUE(task.file_url_.empty());
  EXPECT_EQ(11, task.record_count_);
  check_round_trip(task);
}

TEST_F(TestExtPluginTask, ob_file_task_is_a_physical_file)
{
  ObOptPluginFile opt_file(allocator_);
  opt_file.reader_type_ = ObPluginReaderType::OB_PARQUET;
  opt_file.file_url_ = ObString::make_string("data.parquet");
  opt_file.file_size_ = 1024;
  opt_file.part_id_ = 7;
  opt_file.record_count_ = 37;

  ObPluginScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.init_with_opt_lake_table_file(allocator_, opt_file));
  EXPECT_EQ(ObPluginReaderType::OB_PARQUET, task.reader_type_);
  EXPECT_TRUE(task.plugin_task_json_.empty());
  EXPECT_EQ(ObString::make_string("data.parquet"), task.file_url_);
  EXPECT_EQ(-1, task.record_count_);
  check_round_trip(task);
}

TEST_F(TestExtPluginTask, descriptor_and_partition_protocol)
{
  const char *json = "{\"plugin_split\":\"eA==\","
                     "\"partition_values\":[{\"field_id\":1,\"value\":\"a\"}],"
                     "\"ob_file_scan\":{\"version\":1,\"file_format\":\"parquet\","
                     "\"files\":[{\"path\":\"a.parquet\",\"byte_size\":10,"
                     "\"row_count\":3},{\"path\":\"b.parquet\","
                     "\"byte_size\":20,\"row_count\":4}]}}";
  ObExtFileScanDescriptor *descriptor = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObExtFileScanDescriptor::parse(
      allocator_, json, STRLEN(json), descriptor));
  ASSERT_NE(nullptr, descriptor);
  EXPECT_EQ(ObExtFileScanFormat::PARQUET, descriptor->file_format_);
  ASSERT_EQ(2, descriptor->file_count_);
  EXPECT_EQ(ObString::make_string("a.parquet"), descriptor->files_[0].file_path_);
  EXPECT_EQ(3, descriptor->files_[0].row_count_);
  EXPECT_EQ(ObString::make_string("b.parquet"), descriptor->files_[1].file_path_);
  EXPECT_EQ(4, descriptor->files_[1].row_count_);

  ObExtTaskPartitionValues values;
  ASSERT_EQ(OB_SUCCESS, ObExtTaskPartitionValues::parse(
      allocator_, json, STRLEN(json), values));
  ASSERT_EQ(1, values.count_);
  EXPECT_EQ(1, values.values_[0].field_id_);
  EXPECT_EQ(ObString::make_string("a"), values.values_[0].value_);
}

TEST_F(TestExtPluginTask, absent_ob_file_scan_keeps_plugin_reader)
{
  const char *json = R"({"plugin_split":"eA=="})";
  ObExtFileScanDescriptor *descriptor = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObExtFileScanDescriptor::parse(
      allocator_, json, STRLEN(json), descriptor));
  EXPECT_EQ(nullptr, descriptor);
}

TEST_F(TestExtPluginTask, invalid_ob_file_scan_is_error)
{
  const char *invalid[] = {
      "{\"ob_file_scan\":{\"version\":99,\"file_format\":\"parquet\","
      "\"files\":[{\"path\":\"a.parquet\",\"byte_size\":10,\"row_count\":3}]}}",
      "{\"ob_file_scan\":{\"version\":1,\"file_format\":\"parquet\","
      "\"files\":[{\"path\":\"a.parquet\",\"byte_size\":10}]}}",
      "{\"ob_file_scan\":{\"version\":1,\"file_format\":\"parquet\","
      "\"files\":[{\"path\":\"a.parquet\",\"byte_size\":10,\"row_count\":-1}]}}",
  };
  ObExtFileScanDescriptor *descriptor = nullptr;
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(invalid) / sizeof(invalid[0])); ++i) {
    ASSERT_EQ(OB_INVALID_DATA, ObExtFileScanDescriptor::parse(
        allocator_, invalid[i], STRLEN(invalid[i]), descriptor));
    EXPECT_EQ(nullptr, descriptor);
  }

  const char *malformed = "{";
  EXPECT_NE(OB_SUCCESS, ObExtFileScanDescriptor::parse(
      allocator_, malformed, STRLEN(malformed), descriptor));
  EXPECT_EQ(nullptr, descriptor);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObExtFileScanDescriptor::parse(
      allocator_, nullptr, 0, descriptor));
  EXPECT_EQ(nullptr, descriptor);
}

} // namespace unittest
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
