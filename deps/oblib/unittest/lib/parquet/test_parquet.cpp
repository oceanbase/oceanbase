/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "gtest/gtest.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <arrow/util/logging.h>

#include <iostream>

#include "lib/allocator/page_arena.h"
#include "lib/file/ob_file.h"
#include "lib/file/file_directory_utils.h"
#include "lib/charset/ob_template_helper.h"
#include "lib/net/ob_net_util.h"

#define USING_LOG_PREFIX SQL

using namespace oceanbase::common;

class TestParquet: public ::testing::Test
{
public:
  TestParquet();
  virtual ~TestParquet();
  virtual void SetUp();
  virtual void TearDown();
};


TestParquet::TestParquet()
{
}

TestParquet::~TestParquet()
{
}

void TestParquet::SetUp()
{
}

void TestParquet::TearDown()
{
}

constexpr int NUM_ROWS_PER_ROW_GROUP = 500;
const char PARQUET_FILENAME[] = "parquet_cpp_example.parquet";


// #0 Build dummy data to pass around
// To have some input data, we first create an Arrow Table that holds
// some data.
std::shared_ptr<arrow::Table> generate_table() {
  arrow::Int64Builder i64builder;
  PARQUET_THROW_NOT_OK(i64builder.AppendValues({1, 2, 3, 4, 5}));
  std::shared_ptr<arrow::Array> i64array;
  PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));

  arrow::StringBuilder strbuilder;
  PARQUET_THROW_NOT_OK(strbuilder.Append("some"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("string"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("content"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("in"));
  PARQUET_THROW_NOT_OK(strbuilder.Append("rows"));
  std::shared_ptr<arrow::Array> strarray;
  PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));

  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("int", arrow::int64()), arrow::field("str", arrow::utf8())});

  return arrow::Table::Make(schema, {i64array, strarray});
}

// #1 Write out the data as a Parquet file
void write_parquet_file(const arrow::Table& table) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("parquet-arrow-example.parquet"));
  // The last argument to the function call is the size of the RowGroup in
  // the parquet file. Normally you would choose this to be rather large but
  // for the example, we use a small value to have multiple RowGroups.
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
}

// #2: Fully read in the file
void read_whole_file() {
  std::cout << "Reading parquet-arrow-example.parquet at once" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

// #3: Read only a single RowGroup of the parquet file
void read_single_rowgroup() {
  std::cout << "Reading first RowGroup of parquet-arrow-example.parquet" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->RowGroup(0)->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

// #4: Read only a single column of the whole parquet file
void read_single_column() {
  std::cout << "Reading first column of parquet-arrow-example.parquet" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::ChunkedArray> array;
  PARQUET_THROW_NOT_OK(reader->ReadColumn(0, &array));
  PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
  std::cout << std::endl;
}

// #5: Read only a single column of a RowGroup (this is known as ColumnChunk)
//     from the Parquet file.
void read_single_column_chunk() {
  std::cout << "Reading first ColumnChunk of the first RowGroup of "
               "parquet-arrow-example.parquet"
            << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::ChunkedArray> array;
  PARQUET_THROW_NOT_OK(reader->RowGroup(0)->Column(0)->Read(&array));
  PARQUET_THROW_NOT_OK(arrow::PrettyPrint(*array, 4, &std::cout));
  std::cout << std::endl;
}

class ObParquetAllocator : public ::arrow::MemoryPool
{
public:

  /// Allocate a new memory region of at least size bytes.
  ///
  /// The allocated region shall be 64-byte aligned.
  virtual arrow::Status Allocate(int64_t size, uint8_t** out) override
  {
    arrow::Status ret = arrow::Status::OK();
    void *buf = alloc_.alloc_aligned(size, 64);
    if (OB_ISNULL(buf)) {
      ret = arrow::Status::Invalid("allocate memory failed");
    } else {
      *out = static_cast<uint8_t*>(buf);
    }
    std::cout << "Allocing : " << size << std::endl;
    return arrow::Status::OK();
  }

  /// Resize an already allocated memory section.
  ///
  /// As by default most default allocators on a platform don't support aligned
  /// reallocation, this function can involve a copy of the underlying data.
  virtual arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr)
  {
    std::cout << "Reallocing : " << old_size << ',' << new_size << std::endl;
    return Allocate(new_size, ptr);
  }

  /// Free an allocated region.
  ///
  /// @param buffer Pointer to the start of the allocated memory region
  /// @param size Allocated size located at buffer. An allocator implementation
  ///   may use this for tracking the amount of allocated bytes as well as for
  ///   faster deallocation if supported by its backend.
  virtual void Free(uint8_t* buffer, int64_t size) {
    std::cout << "Freed : " << size << std::endl;
    alloc_.free(buffer);
  }

  /// Return unused memory to the OS
  ///
  /// Only applies to allocators that hold onto unused memory.  This will be
  /// best effort, a memory pool may not implement this feature or may be
  /// unable to fulfill the request due to fragmentation.
  virtual void ReleaseUnused() {
    std::cout << "ReleaseUnused" << std::endl;
  }

  /// The number of bytes that were allocated and not yet free'd through
  /// this allocator.
  virtual int64_t bytes_allocated() const override {
    std::cout << "bytes_allocated()" << std::endl;
    return alloc_.total();
  }

  /// Return peak memory allocation in this memory pool
  ///
  /// \return Maximum bytes allocated. If not known (or not implemented),
  /// returns -1
  virtual int64_t max_memory() const override { return -1; }

  /// The name of the backend used by this MemoryPool (e.g. "system" or "jemalloc").
  virtual std::string backend_name() const override { return "Parquet"; }
private:
  ObArenaAllocator alloc_;
  arrow::internal::MemoryPoolStats stats_;
};


class ObExternalFileReader : public arrow::io::RandomAccessFile {
public:
  ObExternalFileReader(const char*file_name, arrow::MemoryPool *pool) {
    file_reader_.open(file_name, false);
    pool_ = pool;
    file_name_ = file_name;
  }
  ~ObExternalFileReader() override {}

  virtual arrow::Status Close() override;

  virtual bool closed() const override;

  virtual arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
  virtual arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
  virtual arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
  virtual arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;


  virtual arrow::Status Seek(int64_t position) override;
  virtual arrow::Result<int64_t> Tell() const override;
  virtual arrow::Result<int64_t> GetSize() override;
private:
  ObFileReader file_reader_;
  int64_t position_;
  arrow::MemoryPool *pool_;
  const char* file_name_;
};

arrow::Status ObExternalFileReader::Seek(int64_t position) {
  std::cout<< "ObExternalFileReader::Seek" << std::endl;
  position_ = position;
  return arrow::Status::OK();
}

arrow::Result<int64_t> ObExternalFileReader::Read(int64_t nbytes, void *out)
{
  std::cout<< "ObExternalFileReader::Read(int64_t nbytes, void *out)" << std::endl;
  int64_t read_size = -1;
  file_reader_.pread(out, nbytes, position_, read_size);
  position_ += read_size;
  return read_size;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ObExternalFileReader::Read(int64_t nbytes)
{
  std::cout<< "ObExternalFileReader::Read(int64_t nbytes)" << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_));
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buffer->Resize(bytes_read));
  }
  return std::move(buffer);
}


arrow::Result<int64_t> ObExternalFileReader::ReadAt(int64_t position, int64_t nbytes, void* out)
{
  std::cout<< "ObExternalFileReader::ReadAt(int64_t position, int64_t nbytes, void* out)" << std::endl;
  int64_t read_size = -1;
  file_reader_.pread(out, nbytes, position, read_size);
  position_ = position + read_size;
  return read_size;
}
arrow::Result<std::shared_ptr<arrow::Buffer>> ObExternalFileReader::ReadAt(int64_t position, int64_t nbytes)
{
  std::cout<< "ObExternalFileReader::ReadAt(int64_t position, int64_t nbytes)" << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes, pool_));
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                        ReadAt(position, nbytes, buffer->mutable_data()));
  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buffer->Resize(bytes_read));
    buffer->ZeroPadding();
  }
  return std::move(buffer);
}


arrow::Result<int64_t> ObExternalFileReader::Tell() const
{
  std::cout<< "ObExternalFileReader::Tell()" << std::endl;
  return position_;
}

arrow::Result<int64_t> ObExternalFileReader::GetSize()
{
  std::cout<< "ObExternalFileReader::GetSize()" << std::endl;
  int64_t file_size = 0;
  FileDirectoryUtils::get_file_size(file_name_, file_size);
  return file_size;
}


arrow::Status ObExternalFileReader::Close()
{
  std::cout<< "ObExternalFileReader::Close()" << std::endl;
  file_reader_.close();
  return arrow::Status::OK();
}

bool ObExternalFileReader::closed() const
{
  std::cout<< "ObExternalFileReader::closed()" << std::endl;
  return !file_reader_.is_opened();
}

void read_column_schema() {
  std::cout << "Reading column schema "
               "parquet-arrow-example.parquet"
            << std::endl;

  ObParquetAllocator alloc;
  parquet::ReaderProperties read_props(&alloc);

  std::cout<< "create parquet_file : " << std::endl;
  std::shared_ptr<ObExternalFileReader> reader =
      std::make_shared<ObExternalFileReader>("parquet-arrow-example.parquet", &alloc);

  std::cout<< "create file reader : " << std::endl;
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::Open(reader, read_props);

    // Get the File MetaData
  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

  int num_row_groups = file_metadata->num_row_groups();
  int num_columns = file_metadata->num_columns();

  std::cout<< "num_row_groups : " << num_row_groups << std::endl;
  std::cout<< "num_columns : " << num_columns << std::endl;


  for (int i = 0; i < num_columns; i++) {
    std::cout<<"Path="<<file_metadata->schema()->Column(i)->path()->ToDotString()<<std::endl;
  }


  std::cout<<"Path="<<file_metadata->schema()->ColumnIndex(std::string("int"))<<std::endl;
  std::cout<<"Path="<<file_metadata->schema()->ColumnIndex(std::string("str"))<<std::endl;

  for (int i = 0; i < num_row_groups; i++) {
    for (int j = 0; j < num_columns; j++) {
      parquet::Type::type col_type = file_metadata->RowGroup(i)->ColumnChunk(j)->type();
      std::cout<<"ColumnType="<<col_type<<std::endl;
    }

  }


  int64_t value = 0;
  int64_t values_read = 0;
  int64_t rows_read = 0;

  for (int r = 0; r < num_row_groups; ++r) {
    // Get the RowGroup Reader
    std::shared_ptr<parquet::RowGroupReader> row_group_reader =
        parquet_reader->RowGroup(r);

    std::shared_ptr<parquet::ColumnReader> column_reader;
    column_reader = row_group_reader->Column(0);
    parquet::Int64Reader* int64_reader =
        static_cast<parquet::Int64Reader*>(column_reader.get());


    while (int64_reader->HasNext()) {
      std::cout << "before int64: " << std::endl;
      rows_read = int64_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
      std::cout << "read int64: " << value << std::endl;
      std::cout << "read rows: " << rows_read << std::endl;
      std::cout << "read values_read: " << values_read << std::endl;
    }


    column_reader = row_group_reader->Column(1);
    parquet::ByteArrayReader* ba_reader =
        static_cast<parquet::ByteArrayReader*>(column_reader.get());

    while (ba_reader->HasNext()) {
      parquet::ByteArray value;
      std::cout << "before bytes: " << std::endl;
      rows_read =
          ba_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
      std::cout << "read bytes: " << std::string(pointer_cast<const char*>(value.ptr), value.len) << std::endl;
      std::cout << "read rows: " << rows_read << std::endl;
      std::cout << "read values_read: " << values_read << std::endl;
    }
  }

}

using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

constexpr int FIXED_LENGTH = 10;
constexpr int FIXED_LENGTH_DEC = 14;

static std::shared_ptr<GroupNode> SetupSchema() {
  parquet::schema::NodeVector fields;
  fields.push_back(PrimitiveNode::Make("int", Repetition::OPTIONAL, parquet::LogicalType::Int(64, true), Type::INT64));
  fields.push_back(PrimitiveNode::Make("string", Repetition::OPTIONAL, parquet::LogicalType::String(), Type::BYTE_ARRAY));
  fields.push_back(PrimitiveNode::Make("decimal32", Repetition::OPTIONAL, parquet::LogicalType::Decimal(6, 3), Type::INT32));
  fields.push_back(PrimitiveNode::Make("decimal64", Repetition::OPTIONAL, parquet::LogicalType::Decimal(10, 0), Type::INT64));
  fields.push_back(PrimitiveNode::Make("decimalbytearr", Repetition::OPTIONAL, parquet::LogicalType::Decimal(20, 3), Type::BYTE_ARRAY));
  fields.push_back(PrimitiveNode::Make("date", Repetition::OPTIONAL, parquet::LogicalType::Date(), Type::INT32));
  fields.push_back(PrimitiveNode::Make("timestamp", Repetition::OPTIONAL, parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::MICROS), Type::INT64));


  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

void gen_test_parquet() {
  /**********************************************************************************
                               PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  try {
    // Create a local file output stream instance.
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    PARQUET_ASSIGN_OR_THROW(out_file, FileClass::Open(PARQUET_FILENAME));

    // Setup the parquet schema
    std::shared_ptr<GroupNode> schema = SetupSchema();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rg_writer = NULL;
/*
    // Write the Bool column
    parquet::BoolWriter* bool_writer =
        static_cast<parquet::BoolWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      bool value = ((i % 2) == 0) ? true : false;
      bool_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Int32 column
    parquet::Int32Writer* int32_writer =
        static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      int32_t value = i;
      int32_writer->WriteBatch(1, nullptr, nullptr, &value);
    }
    // Write the Int64 column. Each row has repeats twice.
    parquet::Int64Writer* int64_writer =
        static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
    for (int i = 0; i < 2 * NUM_ROWS_PER_ROW_GROUP; i++) {
      int64_t value = i * 1000 * 1000;
      value *= 1000 * 1000;
      int16_t definition_level = 1;
      int16_t repetition_level = 0;
      if ((i % 2) == 0) {
        repetition_level = 1;  // start of a new record
      }
      int64_writer->WriteBatch(1, &definition_level, &repetition_level, &value);
    }
    // Write the INT96 column.
    parquet::Int96Writer* int96_writer =
        static_cast<parquet::Int96Writer*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::Int96 value;
      value.value[0] = i;
      value.value[1] = i + 1;
      value.value[2] = i + 2;
      int96_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Float column
    parquet::FloatWriter* float_writer =
        static_cast<parquet::FloatWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      float value = static_cast<float>(i) * 1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the Double column
    parquet::DoubleWriter* double_writer =
        static_cast<parquet::DoubleWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      double value = i * 1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &value);
    }

    // Write the ByteArray column. Make every alternate values NULL
    parquet::ByteArrayWriter* ba_writer =
        static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::ByteArray value;
      char hello[FIXED_LENGTH] = "parquet";
      hello[7] = static_cast<char>(static_cast<int>('0') + i / 100);
      hello[8] = static_cast<char>(static_cast<int>('0') + (i / 10) % 10);
      hello[9] = static_cast<char>(static_cast<int>('0') + i % 10);
      if (i % 2 == 0) {
        int16_t definition_level = 1;
        value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
        value.len = FIXED_LENGTH;
        ba_writer->WriteBatch(1, &definition_level, nullptr, &value);
      } else {
        int16_t definition_level = 0;
        ba_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
      }
    }

    // Write the FixedLengthByteArray column
    parquet::FixedLenByteArrayWriter* flba_writer =
        static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->NextColumn());
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
      parquet::FixedLenByteArray value;
      char v = static_cast<char>(i);
      char flba[FIXED_LENGTH] = {v, v, v, v, v, v, v, v, v, v};
      value.ptr = reinterpret_cast<const uint8_t*>(&flba[0]);

      flba_writer->WriteBatch(1, nullptr, nullptr, &value);
    }
*/
#define HAS_NULL 1

    for (int j = 0; j < 10; j++) {
      rg_writer = file_writer->AppendRowGroup();

      parquet::Int64Writer* int64_writer =
          static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        if (HAS_NULL && i % 2 == 0) {
          int16_t definition_level = 0;
          int64_writer->WriteBatch(1, &definition_level, nullptr, NULL);
        } else {
          int64_t value = i * 1000 * 1000 + j;
          value *= 1000 * 1000;
          int16_t definition_level = 1;
          int64_writer->WriteBatch(1, &definition_level, nullptr, &value);
        }
      }

      parquet::ByteArrayWriter* ba_writer =
          static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        parquet::ByteArray value;
        char hello[FIXED_LENGTH] = "parquet";
        hello[7] = static_cast<char>(static_cast<int>('0') + i / 100);
        hello[8] = static_cast<char>(static_cast<int>('0') + (i / 10) % 10);
        hello[9] = static_cast<char>(static_cast<int>('0') + i % 10);
        if (HAS_NULL && i % 2 == 1) {
          int16_t definition_level = 0;
          ba_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        } else {
          int16_t definition_level = 1;
          value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
          value.len = FIXED_LENGTH;
          ba_writer->WriteBatch(1, &definition_level, nullptr, &value);
        }
      }

      parquet::Int32Writer* int32_writer2 =
          static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        if (HAS_NULL && i % 3 == 0) {
          int16_t definition_level = 0;
          int32_writer2->WriteBatch(1, &definition_level, nullptr, NULL);
        } else {
          int32_t value = j * 10000 + i;
          int16_t definition_level = 1;
          int32_writer2->WriteBatch(1, &definition_level, nullptr, &value);
        }
      }

      parquet::Int64Writer* int64_writer2 =
          static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        if (HAS_NULL && i % 3 == 1) {
          int16_t definition_level = 0;
          int64_writer2->WriteBatch(1, &definition_level, nullptr, NULL);
        } else {
          int64_t value = (j * 10000 + i) * 10000;
          int16_t definition_level = 1;
          int64_writer2->WriteBatch(1, &definition_level, nullptr, &value);
        }
      }

      parquet::ByteArrayWriter* ba_writer2 =
          static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        parquet::ByteArray value;
        char hello[FIXED_LENGTH_DEC] = "1234567890.";
        hello[11] = static_cast<char>(static_cast<int>('0') + i / 100);
        hello[12] = static_cast<char>(static_cast<int>('0') + (i / 10) % 10);
        hello[13] = static_cast<char>(static_cast<int>('0') + i % 10);
        if (HAS_NULL && i % 5 == 0) {
          int16_t definition_level = 0;
          ba_writer2->WriteBatch(1, &definition_level, nullptr, nullptr);
        } else {
          int16_t definition_level = 1;
          value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
          value.len = FIXED_LENGTH_DEC;
          ba_writer2->WriteBatch(1, &definition_level, nullptr, &value);
        }
      }

      parquet::Int32Writer* int32_writer3 =
          static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        if (HAS_NULL && i % 6 == 0) {
          int16_t definition_level = 0;
          int32_writer3->WriteBatch(1, &definition_level, nullptr, NULL);
        } else {
          int32_t value = 19857 + 365 * 10 * j + i;
          int16_t definition_level = 1;
          int32_writer3->WriteBatch(1, &definition_level, nullptr, &value);
        }
      }


      parquet::Int64Writer* int64_writer3 =
          static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
      for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
        if (HAS_NULL && i % 3 == 1) {
          int16_t definition_level = 0;
          int64_writer3->WriteBatch(1, &definition_level, nullptr, NULL);
        } else {
          int64_t value = (1716887565LL + i * 3600) * 1000 * 1000;
          int16_t definition_level = 1;
          int64_writer3->WriteBatch(1, &definition_level, nullptr, &value);
        }
      }

    }

    // Close the ParquetFileWriter
    file_writer->Close();

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << std::endl;
  }
}


TEST_F(TestParquet, example1)
{
  std::shared_ptr<arrow::Table> table = generate_table();
  write_parquet_file(*table);
  read_whole_file();
  read_single_rowgroup();
  read_single_column();
  read_single_column_chunk();
  read_column_schema();
  gen_test_parquet();
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
