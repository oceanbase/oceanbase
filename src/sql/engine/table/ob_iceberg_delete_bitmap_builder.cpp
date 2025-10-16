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

#include "sql/engine/table/ob_iceberg_delete_bitmap_builder.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/external_table/ob_external_table_utils.h"
#include <parquet/api/reader.h>
#include "sql/engine/table/ob_orc_table_row_iter.h"

// ORC 异常处理宏定义
#define CATCH_ORC_EXCEPTIONS                                  \
  catch (const ObErrorCodeException &ob_error) {              \
    if (OB_SUCC(ret)) {                                       \
      ret = ob_error.get_error_code();                        \
      LOG_WARN("fail to read orc file", K(ret));              \
    }                                                         \
  } catch (const std::exception& e) {                         \
    if (OB_SUCC(ret)) {                                       \
      ret = OB_ERR_UNEXPECTED;                                \
      LOG_WARN("unexpected error", K(ret), "Info", e.what()); \
    }                                                         \
  } catch(...) {                                              \
    if (OB_SUCC(ret)) {                                       \
      ret = OB_ERR_UNEXPECTED;                                \
      LOG_WARN("unexpected error", K(ret));                   \
    }                                                         \
  }

#define CATCH_PARQUET_EXCEPTIONS                                                                   \
  catch (const ObErrorCodeException &ob_error) {                                                   \
    if (OB_SUCC(ret)) {                                                                            \
      ret = ob_error.get_error_code();                                                             \
      LOG_WARN("fail to read file", K(ret));                                                       \
    }                                                                                              \
  } catch (const ::parquet::ParquetStatusException &e) {                                           \
    if (OB_SUCC(ret)) {                                                                            \
      status = e.status();                                                                         \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_WARN("unexpected error", K(ret), "Info", e.what());                                      \
    }                                                                                              \
  } catch (const ::parquet::ParquetException &e) {                                                 \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE, e.what());                                          \
      LOG_WARN("unexpected error", K(ret), "Info", e.what());                                      \
    }                                                                                              \
  } catch (...) {                                                                                  \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      LOG_WARN("unexpected error", K(ret));                                                        \
    }                                                                                              \
  }                                                                                                \

namespace oceanbase
{
namespace sql
{

using namespace common;

// ==================== ObIcebergDeleteBitmapBuilder ====================
ObIcebergDeleteBitmapBuilder::~ObIcebergDeleteBitmapBuilder()
{
  delete_file_prebuffer_.destroy();
}

int ObIcebergDeleteBitmapBuilder::init(const storage::ObTableScanParam *scan_param,
                                       ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param) || OB_ISNULL(options)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(scan_param), KP(options));
  } else {
    scan_param_ = scan_param;
    options_ = options;
    if (options_->enable_prebuffer_) {
      OZ(delete_file_prebuffer_.init(options_->cache_options_, scan_param->timeout_));
    }
    OZ(orc_reader_.init());
    OZ(parquet_reader_.init());
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::build_delete_bitmap(const ObString &data_file_path,
                                                      const int64_t task_idx,
                                                      ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(delete_bitmap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(delete_bitmap));
  } else {
    delete_bitmap->set_empty();
  }

  ObIcebergScanTask *scan_task = static_cast<ObIcebergScanTask *>(scan_param_->scan_tasks_.at(task_idx));
  if (OB_ISNULL(scan_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file is null", K(ret));
  }

  // 处理每个删除文件
  IDeleteFileReader *reader = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_task->delete_files_.count(); ++i) {
    const ObLakeDeleteFile &delete_file = scan_task->delete_files_.at(i);
    if (OB_FAIL(get_delete_file_reader(delete_file.file_format_, reader))) {
      LOG_WARN("failed to get delete file reader", K(ret), K(delete_file.file_url_));
    } else if (OB_FAIL(process_single_delete_file(delete_file, data_file_path,
                                                  reader, delete_bitmap))) {
      LOG_WARN("failed to process delete file", K(ret), K(delete_file.file_url_), K(data_file_path));
    }
  }

  return ret;
}

int ObIcebergDeleteBitmapBuilder::get_delete_file_reader(const iceberg::DataFileFormat file_format,
                                                        IDeleteFileReader *&reader)
{
  int ret = OB_SUCCESS;
  if (file_format == iceberg::DataFileFormat::ORC) {
    reader = &orc_reader_;
  } else if (file_format == iceberg::DataFileFormat::PARQUET) {
    reader = &parquet_reader_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported delete file format", K(file_format), K(ret));
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::process_single_delete_file(const ObLakeDeleteFile &delete_file,
                                                            const ObString &data_file_path,
                                                            IDeleteFileReader *reader,
                                                            ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reader is null", K(ret));
  } else {
    if (delete_file_access_driver_.is_opened()) {
      if (OB_FAIL(delete_file_access_driver_.close())) {
        LOG_WARN("failed to close delete file access driver", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(reader->open_delete_file(delete_file,
                                          delete_file_access_driver_,
                                          delete_file_prebuffer_,
                                          scan_param_,
                                          options_))) {
        LOG_WARN("failed to open delete file", K(ret), K(delete_file));
      } else if (options_->enable_prebuffer_ && OB_FAIL(pre_buffer(delete_file.file_size_))) {
        LOG_WARN("failed to pre buffer for delete file", K(ret));
      } else if (OB_FAIL(reader->read_delete_file(data_file_path, scan_param_, delete_bitmap))) {
        LOG_WARN("failed to read delete records", K(ret), K(data_file_path));
      }
    }
  }

  return ret;
}

// ==================== OrcDeleteFileReader ===================
int ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::init()
{
  int ret = OB_SUCCESS;
  std::list<std::string> include_names_list = {"file_path", "pos"};
  row_reader_options_.include(include_names_list);
  orc_alloc_.init(MTL_ID());
  return ret;
}

int ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::open_delete_file(
    const ObLakeDeleteFile &delete_file,
    ObExternalFileAccess &file_access_driver,
    ObFilePreBuffer &file_prebuffer,
    const storage::ObTableScanParam *scan_param,
    ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;

  delete_data_batch_.reset();
  delete_row_reader_.reset();
  delete_reader_.reset();

  int64_t file_size = delete_file.file_size_;
  ObExternalFileUrlInfo file_info(scan_param->external_file_location_,
                                  scan_param->external_file_access_info_, delete_file.file_url_,
                                  ObString::make_empty_string(), file_size,
                                  delete_file.modification_time_);
  ObExternalFileCacheOptions cache_options(options->enable_page_cache_,
                                            options->enable_disk_cache_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(file_access_driver.open(file_info, cache_options))) {
    if (OB_OBJECT_NOT_EXIST == ret || OB_HDFS_PATH_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
      file_size = 0;
    } else {
      LOG_WARN("fail to open delete file", K(ret), K(file_info));
    }
  } else if (file_size > 0) {
    try {
      std::unique_ptr<ObOrcFileAccess> inStream(new ObOrcFileAccess(file_access_driver,
                                                        delete_file.file_url_.ptr(), file_size));
      inStream->set_timeout_timestamp(scan_param->timeout_);
      if (options->enable_prebuffer_) {
        inStream->set_file_prebuffer(&file_prebuffer);
      }
      orc::ReaderOptions reader_options;
      reader_options.setMemoryPool(orc_alloc_);
      delete_reader_ = orc::createReader(std::move(inStream), reader_options);
      if (!delete_reader_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("orc create reader failed", K(ret));
        throw std::bad_exception();
      }
    } CATCH_ORC_EXCEPTIONS

    if (OB_SUCC(ret)) {
      try {
        delete_row_reader_ = delete_reader_->createRowReader(row_reader_options_);
        if (!delete_row_reader_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create row reader failed", K(ret));
        } else {
          int64_t capacity = MAX(1, scan_param->op_->get_eval_ctx().max_batch_size_);
          delete_data_batch_ = delete_row_reader_->createRowBatch(capacity);
        }
      } CATCH_ORC_EXCEPTIONS
    }
  }

  return ret;
}

int ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::read_delete_file(
                                                const ObString& data_file_path,
                                                const storage::ObTableScanParam *scan_param,
                                                ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t nstripes = delete_reader_->getNumberOfStripes();
  bool range_search_finished = false;

  for (int64_t stripe_idx = 0;
              OB_SUCC(ret) && stripe_idx < nstripes && !range_search_finished; ++stripe_idx) {
    std::unique_ptr<orc::StripeInformation> stripe = delete_reader_->getStripe(stripe_idx);
    if (OB_UNLIKELY(!stripe)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null stripe is unexpected", K(ret), K(stripe_idx));
    }
    int64_t stripe_num_rows = stripe->getNumberOfRows();
    int64_t rows_read = 0;
    while (OB_SUCC(ret) && rows_read < stripe_num_rows && !range_search_finished) {
      int64_t batch_size = std::min(scan_param->op_->get_eval_ctx().max_batch_size_,
                                    stripe_num_rows - rows_read);
      delete_data_batch_->capacity = batch_size;
      if (delete_row_reader_->next(*delete_data_batch_)) {
        orc::StructVectorBatch *root =
                                  dynamic_cast<orc::StructVectorBatch *>(delete_data_batch_.get());
        if (OB_ISNULL(root) || root->fields.size() < 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("orc batch struct invalid", K(ret));
        } else {
          orc::StringVectorBatch *file_path_batch =
                                            dynamic_cast<orc::StringVectorBatch *>(root->fields[0]);
          orc::LongVectorBatch *pos_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[1]);
          if (OB_ISNULL(file_path_batch) || OB_ISNULL(pos_batch)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("orc batch field type invalid", K(ret));
          }
          int64_t num = delete_data_batch_.get()->numElements;
          for (int64_t i = 0; i < num && OB_SUCC(ret) && !range_search_finished; ++i) {
            ObString read_file_path(file_path_batch->length[i],
                                    reinterpret_cast<const char*>(file_path_batch->data[i]));

            int compare_res = data_file_path.compare(read_file_path);
            if (compare_res == 0) {
              OZ(delete_bitmap->value_add(pos_batch->data[i]));
            } else if (compare_res < 0) {
              range_search_finished = true;
            }
          }
          rows_read += num;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read next batch failed", K(ret), K(stripe_idx), K(rows_read));
      }
    }
  }
  return ret;
}

// ==================== ParquetDeleteFileReader ====================
int ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::init()
{
  int ret = OB_SUCCESS;
  arrow_alloc_.init(MTL_ID());
  return ret;
}

int ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::open_delete_file(
    const ObLakeDeleteFile &delete_file,
    ObExternalFileAccess &file_access_driver,
    ObFilePreBuffer &file_prebuffer,
    const storage::ObTableScanParam *scan_param,
    ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;
  arrow::Status status = arrow::Status::OK();

  // 先重置之前的reader，避免析构时关闭文件
  delete_file_reader_.reset();

  try {
    std::shared_ptr<ObArrowFile> cur_file = std::make_shared<ObArrowFile>(file_access_driver,
                                                                          delete_file.file_url_.ptr(),
                                                                          &arrow_alloc_);
    int64_t file_size = delete_file.file_size_;
    ObExternalFileUrlInfo file_info(scan_param->external_file_location_,
                                    scan_param->external_file_access_info_, delete_file.file_url_,
                                    ObString::make_empty_string(), file_size,
                                    delete_file.modification_time_);
    ObExternalFileCacheOptions cache_options(options->enable_page_cache_,
                                            options->enable_disk_cache_);
    if (OB_SUCC(ret)) {
      if (options->enable_prebuffer_) {
        cur_file->set_file_prebuffer(&file_prebuffer);
      }
      cur_file->set_timeout_timestamp(scan_param->timeout_);
      read_props_.enable_buffered_stream();
      if (OB_FAIL(cur_file.get()->open(file_info, cache_options))) {
        LOG_WARN("failed to open file", K(ret));
      } else {
        delete_file_reader_ = parquet::ParquetFileReader::Open(cur_file, read_props_);
        if (!delete_file_reader_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create row reader failed", K(ret));
        }
      }
    }
  } CATCH_PARQUET_EXCEPTIONS

  return ret;
}

int ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::read_delete_file(
                                                      const ObString& data_file_path,
                                                      const storage::ObTableScanParam *scan_param,
                                                      ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = scan_param->op_->get_eval_ctx().max_batch_size_;

  std::shared_ptr<parquet::FileMetaData> delete_file_meta;
  int num_row_groups = 0;
  if (OB_ISNULL(delete_file_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete file reader is null", K(ret));
  } else if (OB_ISNULL(delete_file_meta = delete_file_reader_->metadata())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete file meta is null", K(ret));
  } else {
    num_row_groups = delete_file_meta->num_row_groups();
  }

  ObEvalCtx::TempAllocGuard tmp_alloc_g(scan_param->op_->get_eval_ctx());
  int64_t values_read = 0;
  int64_t rows_read = 0;
  bool range_search_finished = false;
  ObArrayWrap<int64_t> int_values;
  ObArrayWrap<parquet::ByteArray> ba_values;
  OZ (int_values.allocate_array(tmp_alloc_g.get_allocator(), batch_size));
  OZ (ba_values.allocate_array(tmp_alloc_g.get_allocator(), batch_size));

  for (int r = 0; OB_SUCC(ret) && r < num_row_groups && !range_search_finished; ++r) {
    std::shared_ptr<parquet::RowGroupReader> row_group_reader = delete_file_reader_->RowGroup(r);
    std::shared_ptr<parquet::ColumnReader> column_reader;

    column_reader = row_group_reader->Column(0);
    parquet::ByteArrayReader* ba_reader =
                                        static_cast<parquet::ByteArrayReader*>(column_reader.get());

    int64_t begin_file_row = -1; // 记录 compare_res == 0 范围在row group中的起始行号
    int64_t end_file_row = -1;   // 记录 compare_res == 0 范围在row group中的结束行号
    int64_t cumulative_rows_read = 0; // 记录当前批次之前已经读取的总行数

    while (OB_SUCC(ret) && ba_reader->HasNext() && !range_search_finished) {
      rows_read =
          ba_reader->ReadBatch(batch_size, nullptr, nullptr, ba_values.get_data(), &values_read);

      for (int i = 0; OB_SUCC(ret) && i < values_read; i++) {
        parquet::ByteArray &current_value = ba_values.at(i);
        ObString read_file_path(current_value.len, pointer_cast<const char*>(current_value.ptr));

        int compare_res = data_file_path.compare(read_file_path);
        if (compare_res == 0) {
          int current_file_row = cumulative_rows_read + i;
          if (begin_file_row == -1) {
            begin_file_row = current_file_row;
          }
          end_file_row = current_file_row;
        } else if (compare_res < 0) {
          range_search_finished = true;
        }
      }
      cumulative_rows_read += values_read;
    }

    column_reader = row_group_reader->Column(1);
    parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());

    int64_t current_row_idx = 0; // 全局行号起点

    if (OB_SUCC(ret) && begin_file_row != -1 && end_file_row != -1) {
      // 跳过 begin_file_row 之前的数据
      if (begin_file_row > 0) {
        int64_t skip_rows = begin_file_row - current_row_idx;
        int64_t skipped = int64_reader->Skip(skip_rows);
        current_row_idx += skipped;
      }
      while (OB_SUCC(ret) && int64_reader->HasNext() && current_row_idx <= end_file_row) {
        rows_read = int64_reader->ReadBatch(batch_size, nullptr, nullptr,
                                            int_values.get_data(), &values_read);

        // 当前 batch 的全局下标范围
        int64_t batch_start = current_row_idx;
        int64_t batch_end = current_row_idx + values_read - 1;

        // 计算和 [begin_file_row, end_file_row] 的交集
        int64_t start = std::max(batch_start, begin_file_row);
        int64_t end = std::min(batch_end, end_file_row);
        if (start <= end) {
          for (int64_t i = start; OB_SUCC(ret) && i <= end; ++i) {
            int64_t local_idx = i - batch_start;
            OZ (delete_bitmap->value_add(int_values.get_data()[local_idx]));
          }
        }
        current_row_idx += values_read;
      }
    }
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::pre_buffer(int64_t file_size)
{
  int ret = OB_SUCCESS;
  ObFilePreBuffer::ColumnRangeSlicesList column_range_slice_list;
  ObFilePreBuffer::ColumnRangeSlices column_range_slices;
  if (OB_FAIL(column_range_slices.range_list_.push_back(
                                                      ObFilePreBuffer::ReadRange(0, file_size)))) {
    LOG_WARN("failed to push back range", K(ret));
  } else if (OB_FAIL(column_range_slice_list.push_back(&column_range_slices))) {
    LOG_WARN("failed to push back column range slice list", K(ret));
  } else if (OB_FAIL(delete_file_prebuffer_.pre_buffer(column_range_slice_list))) {
    LOG_WARN("failed to pre buffer for delete file", K(ret));
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase

#undef CATCH_ORC_EXCEPTIONS
#undef CATCH_PARQUET_EXCEPTIONS
