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
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"

#include <avro/DataFile.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace sql::iceberg;
class TestIcebergManifest : public ::testing::Test
{
public:
  TestIcebergManifest() = default;
  ~TestIcebergManifest() = default;
  ObArenaAllocator allocator;
};

TEST_F(TestIcebergManifest, test_metadata)
{
  std::string filename = "./data/manifest_partition.avro";
  std::unique_ptr<avro::InputStream> in = avro::fileInputStream(filename.c_str());
  std::unique_ptr<avro::DataFileReaderBase> avro_reader_base
      = std::make_unique<avro::DataFileReaderBase>(std::move(in));
  const std::map<std::string, std::vector<uint8_t>> &metadata = avro_reader_base->metadata();
  ManifestMetadata manifest_metadata(allocator);
  ASSERT_EQ(OB_SUCCESS, manifest_metadata.init_from_metadata(metadata));
  ASSERT_EQ(0, manifest_metadata.schema_id);
  ASSERT_EQ(0, manifest_metadata.partition_spec_id);
  ASSERT_EQ(FormatVersion::V2, manifest_metadata.format_version);
  ASSERT_EQ(ManifestContent::DATA, manifest_metadata.content);
}

TEST_F(TestIcebergManifest, test_partition_parse)
{
  std::string filename = "data/manifest_partition.avro";
  std::unique_ptr<avro::InputStream> in = avro::fileInputStream(filename.c_str());
  std::unique_ptr<avro::DataFileReaderBase> avro_reader_base
      = std::make_unique<avro::DataFileReaderBase>(std::move(in));
  const std::map<std::string, std::vector<uint8_t>> &metadata = avro_reader_base->metadata();
  ManifestMetadata manifest_metadata(allocator);
  ASSERT_EQ(OB_SUCCESS, manifest_metadata.init_from_metadata(metadata));
  StructType *expected_avro_schema = NULL;
  SchemaProjection schema_projection(allocator);
  ASSERT_EQ(
      OB_SUCCESS,
      ManifestEntry::get_read_expected_schema(allocator, manifest_metadata, expected_avro_schema));
  ASSERT_EQ(OB_SUCCESS,
            AvroSchemaProjectionUtils::project(allocator,
                                               *expected_avro_schema,
                                               avro_reader_base->dataSchema().root(),
                                               schema_projection));
  avro::DataFileReader<ManifestEntryDatum> avro_reader(std::move(avro_reader_base));
  ManifestFile dummy_manifest_file(allocator);
  ManifestEntryDatum manifest_entry_datum(allocator,
                                          schema_projection,
                                          dummy_manifest_file,
                                          manifest_metadata);
  ASSERT_TRUE(avro_reader.read(manifest_entry_datum));

  ObFixedArray<ObObj, ObIAllocator> &partition_values
      = manifest_entry_datum.manifest_entry_->data_file.partition;

  {
    // bool true
    ObObj result;
    result.set_bool(true);
    ASSERT_EQ(result, partition_values[0]);
  }
  {
    // int 10
    ObObj result;
    result.set_int32(10);
    ASSERT_EQ(result, partition_values[1]);
  }
  {
    // long 500
    ObObj result;
    result.set_int(500);
    ASSERT_EQ(result, partition_values[2]);
  }
  {
    // float 1.23
    ObObj result;
    result.set_float(1.23);
    ASSERT_EQ(result, partition_values[3]);
  }
  {
    // double 1.456
    ObObj result;
    result.set_double(1.456);
    ASSERT_EQ(result, partition_values[4]);
  }
  {
    // date 2023-02-01
    ObObj result;
    result.set_date(19389);
    ASSERT_EQ(result, partition_values[5]);
  }
  {
    // timestamptz 2012-01-01 12:00:01
    ObObj result;
    result.set_timestamp(1325390401000000);
    ASSERT_EQ(result, partition_values[6]);
  }
  {
    // string hello world
    ObObj result;
    result.set_varchar(ObString("hello world"));
    result.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
    ASSERT_EQ(result, partition_values[7]);
  }
  {
    // binary
    std::vector<uint8_t> data{0x01, 0x23, 0x45, 0x6F};
    ObString binary(data.size(), reinterpret_cast<const char *>(data.data()));
    ObObj result;
    result.set_collation_type(ObCollationType::CS_TYPE_BINARY);
    result.set_binary(ObString(data.size(), reinterpret_cast<const char *>(data.data())));
    ASSERT_EQ(result, partition_values[8]);
  }
  {
    // decimal(9,2) 123.12
    ObObj result;
    int32_t buffer_size = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(9);
    char *buf = static_cast<char *>(allocator.alloc(buffer_size));
    memset(buf, 0, buffer_size);
    buf[0] = 0x18;
    buf[1] = 0x30;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    result.set_decimal_int(4, 2, decint);
    ASSERT_EQ(result, partition_values[9]);
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}