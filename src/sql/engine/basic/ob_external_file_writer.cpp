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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_external_file_writer.h"
#include "ob_select_into_basic.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObExternalFileWriter::open_file()
{
  int ret = OB_SUCCESS;
  if (IntoFileLocation::SERVER_DISK != file_location_) {//OSS,COS,S3,HDFS
    bool is_exist = false;
    ObExternalIoAdapter adapter;
    ObStorageAccessType access_type = OB_STORAGE_ACCESS_APPENDER;
    // for S3, should use OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER
    // OSS,COS can also use multipart writer. need to check performance
    if (IntoFileLocation::REMOTE_S3 == file_location_
        || IntoFileLocation::REMOTE_COS == file_location_) {
      access_type = OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER;
    }
    if (OB_FAIL(adapter.is_exist(url_, access_info_, is_exist))) {
      // When file object does not exist on hdfs then return OB_HDFS_PATH_NOT_FOUND.
      if (IntoFileLocation::REMOTE_HDFS == file_location_ &&
          OB_HDFS_PATH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
        is_exist = false;
      } else {
        LOG_WARN("fail to check file exist", KR(ret), K(url_), KPC(access_info_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_exist) {
      ret = OB_FILE_ALREADY_EXIST;
      LOG_WARN("file already exist", KR(ret), K(url_), K(access_info_));
    } else if (OB_FAIL(storage_appender_.open(access_info_, url_, access_type))) {
      LOG_WARN("fail to open file", KR(ret), K(url_), KPC(access_info_));
    } else {
      is_file_opened_ = true;
    }
  } else {//SERVER DISK
    if (OB_FAIL(file_appender_.create(url_, true))) {
      LOG_WARN("failed to create file", K(ret), K(url_));
    } else {
      is_file_opened_ = true;
    }
  }
  return ret;
}

int ObExternalFileWriter::close_file()
{
  int ret = OB_SUCCESS;
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    if (file_appender_.is_opened() && OB_FAIL(file_appender_.fsync())) {
      LOG_WARN("failed to do fsync", K(ret));
    } else {
      file_appender_.close();
    }
  } else if (OB_FAIL(storage_appender_.close())) {
    LOG_WARN("fail to close storage appender", K(ret), K(url_), KPC(access_info_));
  }
  if (OB_SUCC(ret)) {
    is_file_opened_ = false;
  }
  return ret;
}

int ObExternalFileWriter::close_data_writer()
{
  int ret = OB_SUCCESS;
  OZ(write_file());
  OZ(close_file());
  return ret;
}

int ObCsvFileWriter::alloc_buf(common::ObIAllocator &allocator, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buffer", K(ret), K(buf_len));
  } else {
    buf_ = buf;
    buf_len_ = buf_len;
  }
  return ret;
}

int ObCsvFileWriter::init_compress_writer(ObIAllocator &allocator,
                                          const ObCSVGeneralFormat::ObCSVCompression &compression_algorithm,
                                          const int64_t &buffer_size)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObCompressStreamWriter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate stream writer", K(ret), K(sizeof(ObCompressStreamWriter)));
  } else {
    compress_stream_writer_ = new(ptr) ObCompressStreamWriter();
  }
  if (OB_SUCC(ret)
      && OB_FAIL(compress_stream_writer_->init(&file_appender_,
                                               &storage_appender_,
                                               file_location_,
                                               compression_algorithm,
                                               allocator,
                                               buffer_size))) {
    LOG_WARN("failed to init compress stream writer", K(ret));
  }
  return ret;
}

int ObCsvFileWriter::flush_buf()
{
  int ret = OB_SUCCESS;
  if (use_shared_buf_) {
    // do nothing
  } else if (last_line_pos_ > 0 && OB_NOT_NULL(buf_)) {
    if (OB_FAIL(flush_data(buf_, last_line_pos_))) {
      LOG_WARN("failed to flush data", K(ret));
    } else {
      MEMMOVE(buf_, buf_ + last_line_pos_, curr_pos_ - last_line_pos_);
      curr_pos_ = curr_pos_ - last_line_pos_;
      last_line_pos_ = 0;
    }
  }
  return ret;
}

int ObCsvFileWriter::flush_shared_buf(const char *shared_buf, bool continue_use_shared_buf) {
  int ret = common::OB_SUCCESS;
  if (get_curr_pos() > 0 && use_shared_buf_) {
    if (OB_FAIL(flush_data(shared_buf, get_curr_pos()))) {
    } else {
      if (has_lob_) {
        increase_curr_line_len();
      }
      set_curr_pos(0);
      update_last_line_pos();
      use_shared_buf_ = continue_use_shared_buf;
    }
  }
  return ret;
}

int ObCsvFileWriter::flush_data(const char * data, int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (has_compress_) {
    if (OB_FAIL(flush_to_compress_stream(data, data_len))) {
      LOG_WARN("failed to flush to compress stream", K(ret));
    }
  } else {
    if (OB_FAIL(flush_to_storage(data, data_len))) {
      LOG_WARN("failed to flush to storage", K(ret));
    }
  }
  return ret;
}

int ObCsvFileWriter::flush_to_compress_stream(const char *data, int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (data == NULL || data_len == 0) {
  } else if (!has_compress_ || OB_ISNULL(compress_stream_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null compress stream writer", K(ret));
  } else if (!is_file_opened_ && OB_FAIL(open_file())) {
    LOG_WARN("failed to open file", K(ret), K(url_));
  } else if (OB_FAIL(compress_stream_writer_->write(data, data_len))) {
    LOG_WARN("failed to write to compress stream writer", K(ret), K(url_));
  }
  return ret;
}

int ObCsvFileWriter::flush_to_storage(const char *data, int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (data == NULL || data_len == 0) {
  } else if (!is_file_opened_ && OB_FAIL(open_file())) {
    LOG_WARN("failed to open file", K(ret), K(url_));
  } else {
    int64_t begin_ts = ObTimeUtility::current_time();
    if (file_location_ == IntoFileLocation::SERVER_DISK) {
      if (OB_FAIL(file_appender_.append(data, data_len, false))) {
        LOG_WARN("failed to append file", K(ret), K(data_len));
      } else {
        write_offset_ += data_len;
        int64_t end_ts = ObTimeUtility::current_time();
        int64_t cost_time = end_ts - begin_ts;
        long double speed = (cost_time <= 0) ? 0 :
                        (long double) data_len * 1000.0 * 1000.0 / 1024.0 / 1024.0 / cost_time;
        long double total_write = (long double) write_offset_ / 1024.0 / 1024.0;
        _OB_LOG(TRACE, "write local stat, time:%ld write_size:%ld speed:%.2Lf MB/s total_write:%.2Lf MB",
                cost_time, data_len, speed, total_write);
      }
    } else {
      int64_t write_size = 0;
      const char *location = nullptr;
      if (OB_FAIL(ObArrowUtil::get_location(file_location_, location))) {
        LOG_WARN("fail to get locatiton", K(ret), K_(file_location));
      } else if (OB_ISNULL(location)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid location string", K(ret));
      } else if (OB_FAIL(storage_appender_.append(data, data_len, write_size))) {
        LOG_WARN("fail to append data", KR(ret), KP(data), K(data_len), K(url_), KPC(access_info_));
      } else {
        write_offset_ += write_size;
        int64_t end_ts = ObTimeUtility::current_time();
        int64_t cost_time = end_ts - begin_ts;
        long double speed = (cost_time <= 0) ? 0 :
                        (long double) write_size * 1000.0 * 1000.0 / 1024.0 / 1024.0 / cost_time;
        long double total_write = (long double) write_offset_ / 1024.0 / 1024.0;
        _OB_LOG(TRACE, "write %s stat, time:%ld write_size:%ld speed:%.2Lf MB/s total_write:%.2Lf MB", location,
                cost_time, write_size, speed, total_write);
      }
    }
  }
  return ret;
}

int ObCsvFileWriter::write_file()
{
  return flush_buf();
}

int ObCsvFileWriter::close_file()
{
  int ret = OB_SUCCESS;
  if (has_compress_ && OB_NOT_NULL(compress_stream_writer_) && is_file_opened_
      && OB_FAIL(compress_stream_writer_->finish_file_compress())) {
    LOG_WARN("failed to flush compress buffer", K(ret));
  } else if (OB_FAIL(ObExternalFileWriter::close_file())) {
    LOG_WARN("failed to close file", K(ret));
  }
  return ret;
}

int64_t ObCsvFileWriter::get_file_size()
{
  int64_t curr_line_len = 0;
  if (!has_lob_ || get_curr_line_len() == 0) {
    curr_line_len = get_curr_pos() - get_last_line_pos();
  } else {
    curr_line_len = get_curr_pos() + get_curr_line_len();
  }
  return get_curr_bytes_exclude_curr_line() + curr_line_len;
}

int64_t ObCsvFileWriter::get_curr_bytes_exclude_curr_line()
{
  int64_t curr_bytes_exclude_curr_line = 0;
  if (has_compress_) {
    if (compress_stream_writer_ == NULL) {
      // do nothing
    } else {
      // If export to compressed file, curr_bytes is estimated.
      // The compression algorithm has internal buffer,
      // so in order to avoid the estimated compressed bytes exceeding MAX_OSS_FILE_SIZE,
      // the size of the internal buffer needs to be taken into account.
      // zstd: 128 KB, gzip: 64KB. use the maximum buffer size here.
      const int64_t COMPRESSION_INTERNAL_BUFFER_SIZE = 128 * 1024; // 128KB
      curr_bytes_exclude_curr_line = get_compress_stream_writer()->get_write_bytes();
    }
  } else {
    curr_bytes_exclude_curr_line = get_write_bytes();
  }
  return curr_bytes_exclude_curr_line;
}

int64_t ObCsvFileWriter::get_curr_file_pos()
{
  int64_t curr_bytes = 0;
  if (has_compress_) {
    if (compress_stream_writer_ == NULL) {
      // do nothing
    } else {
      curr_bytes = get_compress_stream_writer()->get_write_bytes();
    }
  } else {
    if (IntoFileLocation::SERVER_DISK != file_location_) { //OSS,COS,S3
      curr_bytes = storage_appender_.offset_;
    } else { // local disk
      curr_bytes = file_appender_.get_buffered_file_pos();
    }
  }
  return curr_bytes;
}

int ObParquetFileWriter::open_parquet_file_writer(ObArrowMemPool &arrow_alloc,
                                                  const int64_t &row_group_size,
                                                  const int64_t &compress_type_index,
                                                  const int64_t &row_batch_size,
                                                  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  try {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoParquet"));
    parquet::WriterProperties::Builder builder;
    builder.max_row_group_length(row_group_size);
    builder.compression(static_cast<parquet::Compression::type>(compress_type_index));
    builder.memory_pool(&arrow_alloc);
    std::shared_ptr<ObParquetOutputStream> cur_file =
          std::make_shared<ObParquetOutputStream>(&file_appender_,
                                                  &storage_appender_,
                                                  file_location_,
                                                  url_);
    if (!is_file_opened_ && OB_FAIL(open_file())) {
      LOG_WARN("failed to open file", K(ret));
    } else {
      parquet_file_writer_ = parquet::ParquetFileWriter::Open(cur_file, parquet_writer_schema_, builder.build());
      parquet_rg_writer_ = parquet_file_writer_->AppendBufferedRowGroup();
      if (parquet_row_batch_.empty() && OB_FAIL(create_parquet_row_batch(row_batch_size, allocator))) {
        LOG_WARN("failed to create parquet row batch", K(ret));
      }
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught exception when open parquet file writer", K(ret), "Info", ex.what());
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught exception when open parquet file writer", K(ret));
    }
  }
  return ret;
}

int ObParquetFileWriter::create_parquet_row_batch(const int64_t &row_batch_size, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<parquet::schema::PrimitiveNode> p_node;
  row_batch_offset_ = 0;
  row_batch_size_ = row_batch_size;
  if (!parquet_writer_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(parquet_row_batch_.allocate_array(allocator, parquet_writer_schema_->field_count()))) {
    LOG_WARN("failed to allocate array", K(ret));
  } else if (OB_FAIL(parquet_row_def_levels_.allocate_array(allocator, parquet_writer_schema_->field_count()))) {
    LOG_WARN("failed to allocate array", K(ret));
  } else if (OB_FAIL(parquet_value_offsets_.allocate_array(allocator, parquet_writer_schema_->field_count()))) {
    LOG_WARN("failed to allocate array", K(ret));
  }
  for (int col_idx = 0; OB_SUCC(ret) && col_idx < parquet_writer_schema_->field_count(); col_idx++) {
    if (!(p_node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(parquet_writer_schema_->field(col_idx)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null ptr", K(ret));
    } else if (p_node->is_group()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "group type in parquet");
      LOG_WARN("not support group type in parquet", K(ret));
    } else {
      switch (p_node->physical_type()) {
        case parquet::Type::BYTE_ARRAY:
        {
          ObArrayWrap<parquet::ByteArray> value_batch;
          if (OB_FAIL(value_batch.allocate_array(allocator, row_batch_size_))) {
            LOG_WARN("failed to allocate array", K(ret));
          } else {
            parquet_row_batch_.at(col_idx) = value_batch.get_data();
          }
          break;
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        {
          ObArrayWrap<parquet::FixedLenByteArray> value_batch;
          if (OB_FAIL(value_batch.allocate_array(allocator, row_batch_size_))) {
            LOG_WARN("failed to allocate array", K(ret));
          } else {
            parquet_row_batch_.at(col_idx) = value_batch.get_data();
          }
          break;
        }
        case parquet::Type::DOUBLE:
        {
          ObArrayWrap<double> value_batch;
          if (OB_FAIL(value_batch.allocate_array(allocator, row_batch_size_))) {
            LOG_WARN("failed to allocate array", K(ret));
          } else {
            parquet_row_batch_.at(col_idx) = value_batch.get_data();
          }
          break;
        }
        case parquet::Type::FLOAT:
        {
          ObArrayWrap<float> value_batch;
          if (OB_FAIL(value_batch.allocate_array(allocator, row_batch_size_))) {
            LOG_WARN("failed to allocate array", K(ret));
          } else {
            parquet_row_batch_.at(col_idx) = value_batch.get_data();
          }
          break;
        }
        case parquet::Type::INT32:
        {
          ObArrayWrap<int32_t> value_batch;
          if (OB_FAIL(value_batch.allocate_array(allocator, row_batch_size_))) {
            LOG_WARN("failed to allocate array", K(ret));
          } else {
            parquet_row_batch_.at(col_idx) = value_batch.get_data();
          }
          break;
        }
        case parquet::Type::INT64:
        {
          ObArrayWrap<int64_t> value_batch;
          if (OB_FAIL(value_batch.allocate_array(allocator, row_batch_size_))) {
            LOG_WARN("failed to allocate array", K(ret));
          } else {
            parquet_row_batch_.at(col_idx) = value_batch.get_data();
          }
          break;
        }
        case parquet::Type::INT96:
        {
          ObArrayWrap<parquet::Int96> value_batch;
          if (OB_FAIL(value_batch.allocate_array(allocator, row_batch_size_))) {
            LOG_WARN("failed to allocate array", K(ret));
          } else {
            parquet_row_batch_.at(col_idx) = value_batch.get_data();
          }
          break;
        }
        default:
        {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", K(p_node->physical_type()), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObArrayWrap<int16_t> def_levels;
      if (OB_FAIL(def_levels.allocate_array(allocator, row_batch_size_))) {
        LOG_WARN("failed to allocate array", K(ret));
      } else {
        parquet_row_def_levels_.at(col_idx) = def_levels.get_data();
        parquet_value_offsets_.at(col_idx) = 0;
      }
    }
  }
  return ret;
}

int ObParquetFileWriter::write_file()
{
  int ret = OB_SUCCESS;
  estimated_bytes_ = 0;
  std::shared_ptr<parquet::schema::PrimitiveNode> p_node;
  parquet::ColumnWriter *col_writer = nullptr;
  if (batch_has_written_) {
    // do nothing
  } else if (!parquet_writer_schema_ || OB_ISNULL(parquet_rg_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ptr", K(ret));
  } else {
    try {
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < parquet_rg_writer_->num_columns(); col_idx++) {
        if (OB_ISNULL(col_writer = parquet_rg_writer_->column(col_idx))
            || !(p_node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(parquet_writer_schema_->field(col_idx)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null ptr", K(ret));
        } else if (p_node->is_group()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "group type in parquet");
          LOG_WARN("not support group type in parquet", K(ret));
        } else {
          switch (p_node->physical_type()) {
            case parquet::Type::BYTE_ARRAY:
            {
              parquet::ByteArrayWriter *writer = static_cast<parquet::ByteArrayWriter *>(col_writer);
              writer->WriteBatch(row_batch_offset_,
                                parquet_row_def_levels_.at(col_idx),
                                nullptr,
                                reinterpret_cast<parquet::ByteArray*>(parquet_row_batch_.at(col_idx)));
              estimated_bytes_ += writer->EstimatedBufferedValueBytes();
              break;
            }
            case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            {
              parquet::FixedLenByteArrayWriter *writer = static_cast<parquet::FixedLenByteArrayWriter *>(col_writer);
              writer->WriteBatch(row_batch_offset_,
                                parquet_row_def_levels_.at(col_idx),
                                nullptr,
                                reinterpret_cast<parquet::FixedLenByteArray*>(parquet_row_batch_.at(col_idx)));
              estimated_bytes_ += writer->EstimatedBufferedValueBytes();
              break;
            }
            case parquet::Type::DOUBLE:
            {
              parquet::DoubleWriter *writer = static_cast<parquet::DoubleWriter *>(col_writer);
              writer->WriteBatch(row_batch_offset_,
                                parquet_row_def_levels_.at(col_idx),
                                nullptr,
                                reinterpret_cast<double*>(parquet_row_batch_.at(col_idx)));
              estimated_bytes_ += writer->EstimatedBufferedValueBytes();
              break;
            }
            case parquet::Type::FLOAT:
            {
              parquet::FloatWriter *writer = static_cast<parquet::FloatWriter *>(col_writer);
              writer->WriteBatch(row_batch_offset_,
                                parquet_row_def_levels_.at(col_idx),
                                nullptr,
                                reinterpret_cast<float*>(parquet_row_batch_.at(col_idx)));
              estimated_bytes_ += writer->EstimatedBufferedValueBytes();
              break;
            }
            case parquet::Type::INT32:
            {
              parquet::Int32Writer *writer = static_cast<parquet::Int32Writer *>(col_writer);
              writer->WriteBatch(row_batch_offset_,
                                parquet_row_def_levels_.at(col_idx),
                                nullptr,
                                reinterpret_cast<int32_t*>(parquet_row_batch_.at(col_idx)));
              estimated_bytes_ += writer->EstimatedBufferedValueBytes();
              break;
            }
            case parquet::Type::INT64:
            {
              parquet::Int64Writer *writer = static_cast<parquet::Int64Writer *>(col_writer);
              writer->WriteBatch(row_batch_offset_,
                                parquet_row_def_levels_.at(col_idx),
                                nullptr,
                                reinterpret_cast<int64_t*>(parquet_row_batch_.at(col_idx)));
              estimated_bytes_ += writer->EstimatedBufferedValueBytes();
              break;
            }
            case parquet::Type::INT96:
            {
              parquet::Int96Writer *writer = static_cast<parquet::Int96Writer *>(col_writer);
              writer->WriteBatch(row_batch_offset_,
                                parquet_row_def_levels_.at(col_idx),
                                nullptr,
                                reinterpret_cast<parquet::Int96*>(parquet_row_batch_.at(col_idx)));
              estimated_bytes_ += writer->EstimatedBufferedValueBytes();
              break;
            }
            default:
            {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected type", K(p_node->physical_type()), K(ret));
            }
          }
        }
      }
      batch_allocator_.reuse();
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("caught exception when write parquet row batch", K(ret), "Info", ex.what());
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("caught exception when write parquet row batch", K(ret));
      }
    }
  }
  return ret;
}

int ObParquetFileWriter::close_file()
{
  int ret = OB_SUCCESS;
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoParquet"));
  try {
    if (parquet_file_writer_) {
      parquet_file_writer_->Close();
      parquet_file_writer_.reset();
    }
  } catch (const std::exception& ex) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught exception when close parquet file", K(ret), K(url_), "Info", ex.what());
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught exception when close parquet file", K(ret), K(url_));
  }
  if (OB_SUCC(ret) && OB_FAIL(ObExternalFileWriter::close_file())) {
    LOG_WARN("failed to close file", K(ret));
  }
  return ret;
}

int ObOrcFileWriter::open_orc_file_writer(const orc::Type &orc_schema,
                                          const orc::WriterOptions &options,
                                          const int64_t &row_batch_size)
{
  int ret = OB_SUCCESS;
  try {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOrc"));
    row_batch_size_ = row_batch_size;
    if (!(orc_output_stream_ = std::unique_ptr<ObOrcOutputStream>(new ObOrcOutputStream(
                                                               &file_appender_,
                                                               &storage_appender_,
                                                               file_location_,
                                                               url_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error create orc output stream ", K(ret));
    } else if (!is_file_opened_ && OB_FAIL(open_file())) {
      LOG_WARN("failed to open file", K(ret));
    } else if (!(orc_file_writer_ = orc::createWriter(orc_schema, orc_output_stream_.get(), options))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error create file writer", K(ret));
    } else if (!(orc_row_batch_ = orc_file_writer_->createRowBatch(row_batch_size_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error create orc row batch", K(ret));
    }
  } catch (const std::exception& ex) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error in open orc file writer", K(ret), "Info", ex.what());
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error in open orc file writer", K(ret));
  }
  return ret;
}

int ObOrcFileWriter::write_file()
{
  int ret = OB_SUCCESS;
  orc::StructVectorBatch* root = static_cast<orc::StructVectorBatch *>(orc_row_batch_.get());
  orc::ColumnVectorBatch* col_vector_batch = NULL;
  if (batch_has_written_) {
    // do nothing
  } else if (OB_ISNULL(root) || !orc_row_batch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    try {
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < root->fields.size(); col_idx++) {
        if (OB_ISNULL(col_vector_batch = root->fields[col_idx])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          col_vector_batch->numElements = row_batch_offset_;
        }
      }
      if (OB_SUCC(ret)) {
        root->numElements = row_batch_offset_;
        orc_file_writer_->add(*orc_row_batch_);
        batch_allocator_.reuse();
      }
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("caught exception when write orc row batch", K(ret), "Info", ex.what());
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("caught exception when write orc row batch", K(ret));
      }
    }
  }
  return ret;
}

int ObOrcFileWriter::close_file()
{
  int ret = OB_SUCCESS;
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOrc"));
  try {
    if (orc_file_writer_) {
      orc_file_writer_->close();
      orc_file_writer_.reset();
    }
    if (orc_output_stream_) {
      orc_output_stream_->close();
      orc_output_stream_.reset();
    }
  } catch (const std::exception& ex) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught exception when close parquet file", K(ret), K(url_), "Info", ex.what());
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught exception when close parquet file", K(ret), K(url_));
  }
  if (OB_SUCC(ret) && OB_FAIL(ObExternalFileWriter::close_file())) {
    LOG_WARN("failed to close file", K(ret));
  }
  return ret;
}

}
}