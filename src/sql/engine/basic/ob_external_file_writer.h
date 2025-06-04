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

#ifndef SRC_SQL_ENGINE_BASIC_OB_EXTERNAL_FILE_WRITER_H_
#define SRC_SQL_ENGINE_BASIC_OB_EXTERNAL_FILE_WRITER_H_


#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "lib/file/ob_file.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#ifdef OB_BUILD_CPP_ODPS
#include <odps/odps_tunnel.h>
#include <odps/odps_api.h>
#endif
#include <parquet/api/writer.h>
#include "ob_select_into_basic.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObExternalFileWriter
{
public:
  ObExternalFileWriter(const common::ObObjectStorageInfo *access_info,
                       const IntoFileLocation &file_location):
    write_bytes_(0),
    is_file_opened_(false),
    file_appender_(),
    storage_appender_(),
    split_file_id_(0),
    url_(),
    access_info_(access_info),
    file_location_(file_location)
  {}

  virtual ~ObExternalFileWriter() {
    file_appender_.~ObFileAppender();
    storage_appender_.reset();
  }

  int open_file();
  virtual int close_file();
  int close_data_writer();
  virtual int write_file() = 0;
  virtual int64_t get_file_size() = 0;
  int64_t get_write_bytes() { return write_bytes_; }
  void set_write_bytes(int64_t write_bytes) { write_bytes_ = write_bytes; }
protected:
  int64_t write_bytes_;
public:
  bool is_file_opened_;
  ObFileAppender file_appender_;
  ObStorageAppender storage_appender_;
  int64_t split_file_id_;
  ObString url_;
  const common::ObObjectStorageInfo *access_info_;
  const IntoFileLocation &file_location_;
};

class ObCsvFileWriter : public ObExternalFileWriter
{
public:
  ObCsvFileWriter(const common::ObObjectStorageInfo *access_info,
                  const IntoFileLocation &file_location,
                  bool &use_shared_buf,
                  const bool &has_compress,
                  const bool &has_lob,
                  int64_t &write_offset):
    ObExternalFileWriter(access_info, file_location),
    buf_(NULL),
    buf_len_(0),
    curr_pos_(0),
    last_line_pos_(0),
    curr_line_len_(0),
    compress_stream_writer_(NULL),
    use_shared_buf_(use_shared_buf),
    has_compress_(has_compress),
    has_lob_(has_lob),
    write_offset_(write_offset)
  {}

  virtual ~ObCsvFileWriter()
  {
    if (OB_NOT_NULL(compress_stream_writer_)) {
      compress_stream_writer_->~ObCompressStreamWriter();
      compress_stream_writer_ = NULL;
    }
  }
  int alloc_buf(common::ObIAllocator &allocator, int64_t buf_len);
  int init_compress_writer(common::ObIAllocator &allocator,
                           const ObCSVGeneralFormat::ObCSVCompression &compression_algorithm,
                           const int64_t &buffer_size);
  char *get_buf() { return buf_; }
  int64_t get_buf_len() { return buf_len_; }
  int64_t get_curr_pos() { return curr_pos_; }
  int64_t get_last_line_pos() { return last_line_pos_; }
  int64_t get_curr_line_len() { return curr_line_len_; }
  ObCompressStreamWriter *get_compress_stream_writer() { return compress_stream_writer_; }
  void set_curr_pos(int64_t curr_pos) { curr_pos_ = curr_pos; }
  void update_last_line_pos() { last_line_pos_ = curr_pos_; }
  void reset_curr_line_len() { curr_line_len_ = 0; }
  void increase_curr_line_len() { curr_line_len_ += (curr_pos_ - last_line_pos_); }
  int flush_buf();
  int flush_shared_buf(const char *shared_buf, bool continue_use_shared_buf = false);
  int flush_data(const char * data, int64_t data_len);
  int flush_to_compress_stream(const char *data, int64_t data_len);
  int flush_to_storage(const char *data, int64_t data_len);
  virtual int write_file() override;
  virtual int close_file() override;
  virtual int64_t get_file_size() override;
  int64_t get_curr_bytes_exclude_curr_line();
  int64_t get_curr_file_pos();
private:
  char *buf_;
  int64_t buf_len_;
  int64_t curr_pos_;
  int64_t last_line_pos_;
  int64_t curr_line_len_;
  ObCompressStreamWriter *compress_stream_writer_;
  bool &use_shared_buf_;
  const bool &has_compress_;
  const bool &has_lob_;
  int64_t &write_offset_;
};

class ObBatchFileWriter : public ObExternalFileWriter
{
public:
  ObBatchFileWriter(const common::ObObjectStorageInfo *access_info,
                    const IntoFileLocation &file_location):
    ObExternalFileWriter(access_info, file_location),
    row_batch_size_(64),
    row_batch_offset_(0),
    batch_has_written_(true),
    batch_allocator_("ParquetOrc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}

  virtual ~ObBatchFileWriter()
  {
    batch_allocator_.reset();
  }

  int64_t get_row_batch_offset() { return row_batch_offset_; }
  void increase_row_batch_offset() { row_batch_offset_++; }
  void reset_row_batch_offset() { row_batch_offset_ = 0; }
  bool reach_batch_end() { return row_batch_offset_ == row_batch_size_; }
  void set_batch_written(bool has_written) { batch_has_written_ = has_written; }
  ObIAllocator &get_batch_allocator() { return batch_allocator_; }
  virtual int write_file() = 0;
  virtual int64_t get_file_size() = 0;

protected:
  int64_t row_batch_size_;
  int64_t row_batch_offset_;
  bool batch_has_written_;
  ObArenaAllocator batch_allocator_;
};

class ObParquetFileWriter : public ObBatchFileWriter
{
public:
  ObParquetFileWriter(const common::ObObjectStorageInfo *access_info,
                      const IntoFileLocation &file_location,
                      std::shared_ptr<parquet::schema::GroupNode> parquet_writer_schema):
    ObBatchFileWriter(access_info, file_location),
    parquet_file_writer_(nullptr),
    parquet_rg_writer_(NULL),
    parquet_row_batch_(),
    parquet_row_def_levels_(),
    parquet_value_offsets_(),
    estimated_bytes_(0),
    parquet_writer_schema_(parquet_writer_schema)
  {}

  virtual ~ObParquetFileWriter()
  {
    parquet_file_writer_.reset();
    parquet_writer_schema_.reset();
  }

  int open_parquet_file_writer(ObArrowMemPool &arrow_alloc,
                               const int64_t &row_group_size,
                               const int64_t &compress_type_index,
                               const int64_t &row_batch_size,
                               common::ObIAllocator &allocator);
  int create_parquet_row_batch(const int64_t &row_batch_size, common::ObIAllocator &allocator);
  bool is_file_writer_null() { return !parquet_file_writer_; }
  bool is_valid_to_write()
  {
    return parquet_file_writer_ && OB_NOT_NULL(parquet_rg_writer_) && !parquet_row_batch_.empty();
  }
  parquet::RowGroupWriter* get_row_group_writer() { return parquet_rg_writer_; }
  void open_next_row_group_writer() { parquet_rg_writer_ = parquet_file_writer_->AppendBufferedRowGroup(); }
  ObArrayWrap<void*> &get_parquet_row_batch() { return parquet_row_batch_; }
  ObArrayWrap<int16_t*> &get_parquet_row_def_levels() { return parquet_row_def_levels_; }
  ObArrayWrap<int64_t> &get_parquet_value_offsets() { return parquet_value_offsets_; }
  int64_t get_estimated_bytes() { return estimated_bytes_; }
  void reset_value_offsets()
  {
    for (int64_t col_idx = 0; col_idx < parquet_value_offsets_.count(); col_idx++) {
      parquet_value_offsets_.at(col_idx) = 0;
    }
  }
  int64_t get_file_size() override
  {
    return get_row_group_size() + write_bytes_;
  }
  int64_t get_row_group_size()
  {
    return parquet_rg_writer_->total_bytes_written() + parquet_rg_writer_->total_compressed_bytes()
           + estimated_bytes_;
  }
  virtual int write_file() override;
  virtual int close_file() override;

private:
  std::unique_ptr<parquet::ParquetFileWriter> parquet_file_writer_;
  parquet::RowGroupWriter* parquet_rg_writer_;
  ObArrayWrap<void*> parquet_row_batch_;
  ObArrayWrap<int16_t*> parquet_row_def_levels_;
  ObArrayWrap<int64_t> parquet_value_offsets_;
  int64_t estimated_bytes_;
  std::shared_ptr<parquet::schema::GroupNode> parquet_writer_schema_;
};

class ObOrcFileWriter : public ObBatchFileWriter
{
public:
  ObOrcFileWriter(const common::ObObjectStorageInfo *access_info,
                  const IntoFileLocation &file_location):
    ObBatchFileWriter(access_info, file_location),
    orc_output_stream_(nullptr),
    orc_file_writer_(nullptr),
    orc_row_batch_(nullptr)
  {}

  virtual ~ObOrcFileWriter()
  {
    orc_output_stream_.reset();
    orc_file_writer_.reset();
    orc_row_batch_.reset();
  }

  int open_orc_file_writer(const orc::Type &orc_schema,
                           const orc::WriterOptions &options,
                           const int64_t &row_batch_size);
  bool is_file_writer_null() {return !orc_file_writer_; }
  bool is_valid_to_write(orc::StructVectorBatch* &root)
  {
    root = static_cast<orc::StructVectorBatch *>(orc_row_batch_.get());
    return orc_file_writer_ && orc_row_batch_ && OB_NOT_NULL(root);
  }
  int64_t get_file_size() override
  {
    return orc_output_stream_->getLength();
  }
  virtual int write_file() override;
  virtual int close_file() override;
private:
  std::unique_ptr<orc::OutputStream> orc_output_stream_;
  std::unique_ptr<orc::Writer> orc_file_writer_;
  std::unique_ptr<orc::ColumnVectorBatch> orc_row_batch_;
};

}
}
#endif /* SRC_SQL_ENGINE_BASIC_OB_EXTERNAL_FILE_WRITER_H_ */