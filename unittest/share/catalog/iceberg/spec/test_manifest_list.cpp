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
#include "lib/string/ob_string.h"
#include "sql/table_format/iceberg/avro_schema_util.h"
#include "sql/table_format/iceberg/spec/manifest_list.h"

#include <avro/DataFile.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace sql::iceberg;
class TestIcebergManifestList : public ::testing::Test
{
public:
  TestIcebergManifestList() = default;
  ~TestIcebergManifestList() = default;
  ObArenaAllocator allocator;
};

TEST_F(TestIcebergManifestList, test_v1_manifest_file)
{
  std::string filename = "./data/v1-snapshot.avro";
  std::unique_ptr<avro::InputStream> in = avro::fileInputStream(filename.c_str());
  std::unique_ptr<avro::DataFileReaderBase> avro_reader_base
      = std::make_unique<avro::DataFileReaderBase>(std::move(in));
  const std::map<std::string, std::vector<uint8_t>> &metadata = avro_reader_base->metadata();
  StructType *expected_avro_schema = NULL;
  SchemaProjection schema_projection(allocator);
  ASSERT_EQ(OB_SUCCESS, ManifestFile::get_read_expected_schema(expected_avro_schema));
  ASSERT_NE(nullptr, expected_avro_schema);
  ASSERT_EQ(OB_SUCCESS,
            AvroSchemaProjectionUtils::project(allocator,
                                               *expected_avro_schema,
                                               avro_reader_base->dataSchema().root(),
                                               schema_projection));
  avro::DataFileReader<ManifestFileDatum> avro_reader(std::move(avro_reader_base));
  ManifestFileDatum manifest_file_datum(allocator, schema_projection);
  ASSERT_TRUE(avro_reader.read(manifest_file_datum));

  const ManifestFile *manifest_file = manifest_file_datum.manifest_file_;
  ASSERT_EQ(ObString("hdfs://100.88.106.200:9000/user/hive/warehouse/test_table_v1/metadata/"
                     "a2abc489-5822-4c69-b819-22939ae64cc4-m0.avro"),
            manifest_file->manifest_path);
  ASSERT_EQ(6535, manifest_file->manifest_length);
  ASSERT_EQ(7833199505409372110, manifest_file->added_snapshot_id);
  ASSERT_EQ(0, manifest_file->partitions.count());
  ASSERT_EQ(9, manifest_file->added_rows_count);
  ASSERT_EQ(0, manifest_file->existing_rows_count);
  ASSERT_EQ(0, manifest_file->deleted_rows_count);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}