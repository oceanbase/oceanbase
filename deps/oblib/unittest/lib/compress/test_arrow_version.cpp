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
#include <string.h>
#include <unistd.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <arrow/io/api.h>
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace unittest
{

// 测试使用 parquet API 读取 snappy 压缩的数据
// Parquet 库会自动处理 snappy 解压
TEST(ObSnappyDecompressTest, test_parquet_with_snappy)
{
  const char* test_file = "test_snappy_parquet.parquet";

  try {
    // 1. 创建带 snappy 压缩的 parquet 文件
    std::shared_ptr<arrow::io::FileOutputStream> out_file;
    PARQUET_ASSIGN_OR_THROW(out_file, arrow::io::FileOutputStream::Open(test_file));

    // 定义 schema：一个 INT64 列
    parquet::schema::NodeVector fields;
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "int64_col",
        parquet::Repetition::REQUIRED,
        parquet::Type::INT64,
        parquet::ConvertedType::NONE));
    std::shared_ptr<parquet::schema::GroupNode> schema =
        std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    // 设置使用 snappy 压缩
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // 创建文件写入器
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    // 写入数据
    parquet::RowGroupWriter* rg_writer = file_writer->AppendRowGroup();
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());

    int64_t values[] = {100, 200, 300, 400, 500};
    int64_t num_values = sizeof(values) / sizeof(values[0]);
    int64_writer->WriteBatch(num_values, nullptr, nullptr, values);
    int64_writer->Close();
    rg_writer->Close();
    file_writer->Close();
    out_file->Close();

    // 2. 使用 parquet API 读取文件（parquet 库会自动解压 snappy 压缩的数据）
    std::shared_ptr<arrow::io::ReadableFile> in_file;
    PARQUET_ASSIGN_OR_THROW(in_file, arrow::io::ReadableFile::Open(test_file));

    parquet::ReaderProperties read_props;
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::Open(in_file, read_props);

    // 获取文件元数据
    std::shared_ptr<parquet::FileMetaData> file_metadata = reader->metadata();
    ASSERT_GT(file_metadata->num_row_groups(), 0);

    // 读取第一个行组
    std::shared_ptr<parquet::RowGroupReader> row_group_reader = reader->RowGroup(0);
    std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(0);

    // 读取数据（parquet 会自动解压 snappy 压缩的列数据块）
    parquet::Int64Reader* int64_reader =
        static_cast<parquet::Int64Reader*>(column_reader.get());

    int64_t read_values[10];
    int64_t values_read = 0;
    int64_t rows_read = int64_reader->ReadBatch(
        10, nullptr, nullptr, read_values, &values_read);

    ASSERT_EQ(num_values, rows_read);
    ASSERT_EQ(num_values, values_read);

    // 验证读取的数据
    for (int i = 0; i < num_values; ++i) {
      ASSERT_EQ(values[i], read_values[i]);
    }

    // 清理测试文件
    unlink(test_file);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
