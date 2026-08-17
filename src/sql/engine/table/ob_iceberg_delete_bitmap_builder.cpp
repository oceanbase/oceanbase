/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/table/ob_iceberg_delete_bitmap_builder.h"

#include "lib/time/ob_time_utility.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/table/ob_external_file_access.h"
#include "sql/engine/table/ob_orc_table_row_iter.h"

#include <limits>
#include <parquet/api/reader.h>
#include <zlib.h>

// ORC 异常处理宏定义
#define CATCH_ORC_EXCEPTIONS                                                                       \
  catch (const std::bad_alloc &e)                                                                  \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                             \
      LOG_WARN("fail to allocate memory when reading orc file", K(ret), "Info", e.what());         \
    }                                                                                              \
  }                                                                                                \
  catch (const ObErrorCodeException &ob_error)                                                     \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = ob_error.get_error_code();                                                             \
      LOG_WARN("fail to read orc file", K(ret));                                                   \
    }                                                                                              \
  }                                                                                                \
  catch (const std::exception &e)                                                                  \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_WARN("invalid orc delete file", K(ret), "Info", e.what());                               \
    }                                                                                              \
  }                                                                                                \
  catch (...)                                                                                      \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_WARN("invalid orc delete file", K(ret));                                                 \
    }                                                                                              \
  }

#define CATCH_PARQUET_EXCEPTIONS                                                                   \
  catch (const std::bad_alloc &e)                                                                  \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                             \
      LOG_WARN("fail to allocate memory when reading parquet file", K(ret), "Info", e.what());     \
    }                                                                                              \
  }                                                                                                \
  catch (const ObErrorCodeException &ob_error)                                                     \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = ob_error.get_error_code();                                                             \
      LOG_WARN("fail to read file", K(ret));                                                       \
    }                                                                                              \
  }                                                                                                \
  catch (const ::parquet::ParquetStatusException &e)                                               \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_WARN("invalid parquet delete file", K(ret), "Info", e.what());                           \
    }                                                                                              \
  }                                                                                                \
  catch (const ::parquet::ParquetException &e)                                                     \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE, e.what());                                          \
      LOG_WARN("invalid parquet delete file", K(ret), "Info", e.what());                           \
    }                                                                                              \
  }                                                                                                \
  catch (const std::exception &e)                                                                  \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_WARN("invalid parquet delete file", K(ret), "Info", e.what());                           \
    }                                                                                              \
  }                                                                                                \
  catch (...)                                                                                      \
  {                                                                                                \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_WARN("invalid parquet delete file", K(ret));                                             \
    }                                                                                              \
  }

namespace oceanbase
{
namespace sql
{

using namespace common;

namespace
{
constexpr int64_t POSITION_DELETE_FILE_PATH_FIELD_ID = 2147483546L;
constexpr int64_t POSITION_DELETE_POS_FIELD_ID = 2147483545L;
constexpr const char *ICEBERG_ID_ATTRIBUTE = "iceberg.id";
constexpr int64_t PUFFIN_LENGTH_SIZE = sizeof(uint32_t);
constexpr int64_t PUFFIN_MAGIC_SIZE = sizeof(uint32_t);
constexpr int64_t PUFFIN_CRC_SIZE = sizeof(uint32_t);
constexpr int64_t PUFFIN_MIN_ROARING_SIZE = sizeof(uint64_t);
constexpr unsigned char PUFFIN_MAGIC_BYTES[PUFFIN_MAGIC_SIZE] = {0xd1, 0xd3, 0x39, 0x64};
} // namespace

uint32_t ObIcebergDeleteBitmapBuilder::decode_big_endian_uint32(const char *buf)
{
  return (static_cast<uint32_t>(static_cast<unsigned char>(buf[0])) << 24)
         | (static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 16)
         | (static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 8)
         | static_cast<uint32_t>(static_cast<unsigned char>(buf[3]));
}

int ObIcebergDeleteBitmapBuilder::get_orc_iceberg_field_id(const orc::Type &type, int64_t &field_id)
{
  int ret = OB_SUCCESS;
  field_id = -1;
  if (type.hasAttributeKey(ICEBERG_ID_ATTRIBUTE)) {
    const std::string id_value = type.getAttributeValue(ICEBERG_ID_ATTRIBUTE);
    if (OB_FAIL(c_str_to_int(id_value.c_str(), field_id))) {
      LOG_WARN("invalid iceberg field id in orc delete file", K(ret), "id_value", id_value.c_str());
    }
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::find_orc_position_delete_fields(const orc::Type &root_type,
                                                                  const bool require_file_path,
                                                                  int64_t &file_path_idx,
                                                                  int64_t &pos_idx)
{
  int ret = OB_SUCCESS;
  file_path_idx = -1;
  pos_idx = -1;
  if (OB_UNLIKELY(orc::TypeKind::STRUCT != root_type.getKind())) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("orc position delete root type is not struct", K(ret), K(root_type.getKind()));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < root_type.getSubtypeCount(); ++i) {
    const orc::Type *child_type = root_type.getSubtype(i);
    int64_t field_id = -1;
    const std::string &field_name = root_type.getFieldName(i);
    if (OB_FAIL(get_orc_iceberg_field_id(*child_type, field_id))) {
      LOG_WARN("failed to get iceberg field id", K(ret), "field_name", field_name.c_str());
    } else if (field_id == POSITION_DELETE_FILE_PATH_FIELD_ID) {
      if (OB_UNLIKELY(orc::TypeKind::STRING != child_type->getKind() || file_path_idx >= 0)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("invalid orc file_path field in position delete file",
                 K(ret),
                 K(field_id),
                 "field_name",
                 field_name.c_str(),
                 K(child_type->getKind()),
                 K(file_path_idx));
      } else {
        file_path_idx = static_cast<int64_t>(i);
      }
    } else if (field_id == POSITION_DELETE_POS_FIELD_ID) {
      if (OB_UNLIKELY(orc::TypeKind::LONG != child_type->getKind() || pos_idx >= 0)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("invalid orc pos field in position delete file",
                 K(ret),
                 K(field_id),
                 "field_name",
                 field_name.c_str(),
                 K(child_type->getKind()),
                 K(pos_idx));
      } else {
        pos_idx = static_cast<int64_t>(i);
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY((require_file_path && file_path_idx < 0) || pos_idx < 0)) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("required fields are missing in orc position delete file",
             K(ret),
             K(file_path_idx),
             K(pos_idx));
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::find_parquet_position_delete_columns(
    const parquet::FileMetaData &file_meta,
    int &file_path_column_idx,
    int &pos_column_idx)
{
  int ret = OB_SUCCESS;
  file_path_column_idx = -1;
  pos_column_idx = -1;
  const parquet::SchemaDescriptor *schema = file_meta.schema();
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("parquet position delete schema is null", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < schema->num_columns(); ++i) {
    const parquet::schema::Node *root_node = schema->GetColumnRoot(i);
    const parquet::ColumnDescriptor *column_desc = schema->Column(i);
    if (OB_ISNULL(root_node) || OB_ISNULL(column_desc)) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("parquet position delete column metadata is null",
               K(ret),
               K(i),
               KP(root_node),
               KP(column_desc));
    } else {
      const int64_t field_id = root_node->field_id();
      const std::string &field_name = root_node->name();
      if (field_id == POSITION_DELETE_FILE_PATH_FIELD_ID) {
        const bool is_string
            = (nullptr != column_desc->logical_type() && column_desc->logical_type()->is_string())
              || parquet::ConvertedType::UTF8 == column_desc->converted_type();
        if (OB_UNLIKELY(!root_node->is_primitive() || !root_node->is_required()
                        || parquet::Type::BYTE_ARRAY != column_desc->physical_type() || !is_string
                        || file_path_column_idx >= 0)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("invalid parquet file_path field in position delete file",
                   K(ret),
                   K(field_id),
                   "field_name",
                   field_name.c_str(),
                   K(column_desc->physical_type()),
                   K(is_string),
                   K(file_path_column_idx));
        } else {
          file_path_column_idx = i;
        }
      } else if (field_id == POSITION_DELETE_POS_FIELD_ID) {
        if (OB_UNLIKELY(!root_node->is_primitive() || !root_node->is_required()
                        || parquet::Type::INT64 != column_desc->physical_type()
                        || pos_column_idx >= 0)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("invalid parquet pos field in position delete file",
                   K(ret),
                   K(field_id),
                   "field_name",
                   field_name.c_str(),
                   K(column_desc->physical_type()),
                   K(pos_column_idx));
        } else {
          pos_column_idx = i;
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(file_path_column_idx < 0 || pos_column_idx < 0)) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("required fields are missing in parquet position delete file",
             K(ret),
             K(file_path_column_idx),
             K(pos_column_idx));
  }
  return ret;
}

// Iceberg position deletes are sorted by file_path and then pos. Row-group
// min/max values can therefore skip unrelated paths and stop once the target
// path is smaller than the remaining ranges.
void ObIcebergDeleteBitmapBuilder::prune_parquet_position_delete_row_group(
    const std::shared_ptr<parquet::RowGroupReader> &row_group_reader,
    const int file_path_column_idx,
    const ObString &data_file_path,
    bool &skip_row_group,
    bool &range_search_finished)
{
  skip_row_group = false;
  std::shared_ptr<parquet::Statistics> statistics
      = row_group_reader->metadata()->ColumnChunk(file_path_column_idx)->statistics();
  std::shared_ptr<parquet::ByteArrayStatistics> path_statistics;
  if (OB_NOT_NULL(statistics) && statistics->HasMinMax() && OB_NOT_NULL(statistics->descr())
      && parquet::SortOrder::UNSIGNED == statistics->descr()->sort_order()) {
    path_statistics = std::dynamic_pointer_cast<parquet::ByteArrayStatistics>(statistics);
  }
  if (OB_NOT_NULL(path_statistics)) {
    const parquet::ByteArray &min_value = path_statistics->min();
    const parquet::ByteArray &max_value = path_statistics->max();
    if (min_value.len > 0 && max_value.len > 0
        && min_value.len <= static_cast<uint32_t>(std::numeric_limits<int32_t>::max())
        && max_value.len <= static_cast<uint32_t>(std::numeric_limits<int32_t>::max())
        && OB_NOT_NULL(min_value.ptr) && OB_NOT_NULL(max_value.ptr)) {
      ObString min_path(min_value.len, pointer_cast<const char *>(min_value.ptr));
      ObString max_path(max_value.len, pointer_cast<const char *>(max_value.ptr));
      if (min_path.compare(max_path) <= 0) {
        if (data_file_path.compare(min_path) < 0) {
          skip_row_group = true;
          range_search_finished = true;
        } else if (data_file_path.compare(max_path) > 0) {
          skip_row_group = true;
        }
      }
    }
  }
}

void ObIcebergDeleteBitmapBuilder::prune_orc_position_delete_stripe(
    const orc::Reader &delete_reader,
    const int64_t stripe_idx,
    const int64_t file_path_column_id,
    const ObString &data_file_path,
    bool &skip_stripe,
    bool &range_search_finished)
{
  skip_stripe = false;
  std::unique_ptr<orc::StripeStatistics> stripe_statistics
      = delete_reader.getStripeStatistics(stripe_idx, false);
  const orc::ColumnStatistics *column_statistics
      = OB_ISNULL(stripe_statistics)
            ? nullptr
            : stripe_statistics->getColumnStatistics(static_cast<uint32_t>(file_path_column_id));
  const orc::StringColumnStatistics *path_statistics
      = dynamic_cast<const orc::StringColumnStatistics *>(column_statistics);
  if (OB_NOT_NULL(path_statistics) && path_statistics->hasMinimum()
      && path_statistics->hasMaximum()) {
    const std::string &min_value = path_statistics->getMinimum();
    const std::string &max_value = path_statistics->getMaximum();
    if (!min_value.empty() && !max_value.empty()
        && min_value.length() <= static_cast<size_t>(std::numeric_limits<int32_t>::max())
        && max_value.length() <= static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      ObString min_path(static_cast<int32_t>(min_value.length()), min_value.data());
      ObString max_path(static_cast<int32_t>(max_value.length()), max_value.data());
      if (min_path.compare(max_path) <= 0) {
        if (data_file_path.compare(min_path) < 0) {
          skip_stripe = true;
          range_search_finished = true;
        } else if (data_file_path.compare(max_path) > 0) {
          skip_stripe = true;
        }
      }
    }
  }
}

int ObIcebergDeleteBitmapBuilder::match_position_delete_file_path(
    const ObString &data_file_path,
    const char *read_file_path,
    const int64_t read_file_path_length,
    bool &is_match,
    bool &range_search_finished)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (OB_ISNULL(read_file_path) || read_file_path_length <= 0) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("invalid file path in position delete file",
             K(ret),
             KP(read_file_path),
             K(read_file_path_length));
  } else if (OB_UNLIKELY(read_file_path_length > std::numeric_limits<int32_t>::max())) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("file path is too long in position delete file", K(ret), K(read_file_path_length));
  } else {
    const ObString current_file_path(static_cast<int32_t>(read_file_path_length), read_file_path);
    const int compare_res = data_file_path.compare(current_file_path);
    is_match = 0 == compare_res;
    if (compare_res < 0) {
      range_search_finished = true;
    }
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::add_delete_positions(const int64_t *positions,
                                                       const int64_t position_count,
                                                       const int64_t data_file_record_count,
                                                       const ObString &data_file_path,
                                                       ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;
  // value_add_many() accepts any uint64_t and cannot validate positions against
  // the data file's row count.
  for (int64_t i = 0; OB_SUCC(ret) && i < position_count; ++i) {
    if (OB_UNLIKELY(positions[i] < 0 || positions[i] >= data_file_record_count)) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("position delete row position is out of range",
               K(ret),
               K(i),
               K(positions[i]),
               K(data_file_record_count),
               K(data_file_path));
    }
  }
  if (OB_SUCC(ret)
      && OB_FAIL(delete_bitmap->value_add_many(reinterpret_cast<const uint64_t *>(positions),
                                               position_count))) {
    LOG_WARN("failed to batch add positions to delete bitmap", K(ret), K(position_count));
  }
  return ret;
}

// ==================== ObIcebergDeleteBitmapBuilder ====================
ObIcebergDeleteBitmapBuilder::~ObIcebergDeleteBitmapBuilder()
{
  reset_delete_file_state();
  delete_file_prebuffer_.destroy();
}

int ObIcebergDeleteBitmapBuilder::init(const storage::ObTableScanParam *scan_param,
                                       ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param) || OB_ISNULL(scan_param->op_) || OB_ISNULL(options)) {
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
    OZ(puffin_reader_.init());
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::register_metrics(ObLakeTableReaderProfile &reader_profile)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(options_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("iceberg delete bitmap builder is not initialized",
             K(ret),
             KP(scan_param_),
             KP(options_));
  } else if (OB_FAIL(
                 reader_profile.register_metrics(&delete_metrics_, ICEBERG_DELETE_METRICS_LABEL))) {
    LOG_WARN("failed to register iceberg delete metrics", K(ret));
  } else if (OB_FAIL(
                 delete_file_access_driver_.register_io_metrics(reader_profile,
                                                                ICEBERG_DELETE_IO_METRICS_LABEL))) {
    LOG_WARN("failed to register iceberg delete io metrics", K(ret));
  } else if (options_->enable_prebuffer_
             && OB_FAIL(
                 delete_file_prebuffer_.register_metrics(reader_profile,
                                                         ICEBERG_DELETE_PREBUFFER_METRICS_LABEL))) {
    LOG_WARN("failed to register iceberg delete prebuffer metrics", K(ret));
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::build_delete_bitmap(const ObString &data_file_path,
                                                      const int64_t task_idx,
                                                      ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t build_start_time_ns = 0;
  bool collect_build_metrics = false;

  ObIcebergScanTask *scan_task = nullptr;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(options_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("iceberg delete bitmap builder is not initialized",
             K(ret),
             KP(scan_param_),
             KP(options_));
  } else if (OB_ISNULL(delete_bitmap) || data_file_path.empty() || task_idx < 0
             || task_idx >= scan_param_->scan_tasks_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",
             K(ret),
             KP(delete_bitmap),
             K(data_file_path),
             K(task_idx),
             "task_count",
             scan_param_->scan_tasks_.count());
  } else if (OB_ISNULL(scan_task
                       = static_cast<ObIcebergScanTask *>(scan_param_->scan_tasks_.at(task_idx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iceberg scan task is null", K(ret), K(task_idx));
  } else if (OB_UNLIKELY(scan_task->record_count_ < 0)) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("invalid data file record count",
             K(ret),
             K(scan_task->record_count_),
             K(data_file_path));
  } else {
    delete_bitmap->set_empty();
    if (!scan_task->delete_files_.empty()) {
      collect_build_metrics = true;
      build_start_time_ns = ObTimeUtility::current_time_ns();
      ++delete_metrics_.data_file_build_count_;
    }
  }

  // 处理每个删除文件
  IDeleteFileReader *reader = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_task->delete_files_.count(); ++i) {
    const ObLakeDeleteFile &delete_file = scan_task->delete_files_.at(i);
    if (OB_FAIL(get_delete_file_reader(delete_file, reader))) {
      LOG_WARN("failed to get delete file reader", K(ret), K(delete_file.file_url_));
    } else if (OB_FAIL(process_single_delete_file(delete_file,
                                                  data_file_path,
                                                  scan_task->record_count_,
                                                  reader,
                                                  delete_bitmap))) {
      LOG_WARN("failed to process delete file",
               K(ret),
               K(delete_file.file_url_),
               K(data_file_path));
    }
  }

  if (collect_build_metrics) {
    delete_metrics_.build_time_ns_ += ObTimeUtility::current_time_ns() - build_start_time_ns;
    if (OB_SUCC(ret)) {
      delete_metrics_.built_deleted_row_count_
          += static_cast<int64_t>(delete_bitmap->get_cardinality());
    }
  }

  return ret;
}

int ObIcebergDeleteBitmapBuilder::get_delete_file_reader(const ObLakeDeleteFile &delete_file,
                                                         IDeleteFileReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  if (ObLakeDeleteFileType::POSITION_DELETE == delete_file.type_) {
    if (iceberg::DataFileFormat::ORC == delete_file.file_format_) {
      reader = &orc_reader_;
    } else if (iceberg::DataFileFormat::PARQUET == delete_file.file_format_) {
      reader = &parquet_reader_;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported position delete file format", K(ret), K(delete_file));
    }
  } else if (ObLakeDeleteFileType::DELETION_VECTOR == delete_file.type_) {
    if (iceberg::DataFileFormat::PUFFIN == delete_file.file_format_) {
      reader = &puffin_reader_;
    } else {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("deletion vector is not stored in a puffin file", K(ret), K(delete_file));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported delete file type", K(ret), K(delete_file));
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::process_single_delete_file(const ObLakeDeleteFile &delete_file,
                                                             const ObString &data_file_path,
                                                             int64_t data_file_record_count,
                                                             IDeleteFileReader *reader,
                                                             ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(delete_file.file_url_.empty() || delete_file.file_size_ <= 0)) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("invalid delete file metadata", K(ret), K(delete_file));
  } else if (OB_FAIL(reader->open_delete_file(delete_file,
                                              delete_file_access_driver_,
                                              delete_file_prebuffer_,
                                              scan_param_,
                                              options_))) {
    LOG_WARN("failed to open delete file", K(ret), K(delete_file));
  } else {
    ++delete_metrics_.delete_file_open_count_;
    if (options_->enable_prebuffer_ && OB_FAIL(pre_buffer_delete_file(delete_file))) {
      LOG_WARN("failed to pre buffer for delete file", K(ret), K(delete_file));
    } else if (OB_FAIL(reader->read_delete_file(data_file_path,
                                                scan_param_,
                                                data_file_record_count,
                                                delete_bitmap))) {
      LOG_WARN("failed to read delete records",
               K(ret),
               K(delete_file),
               K(data_file_path),
               K(data_file_record_count));
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(reset_delete_file_state())) {
    LOG_WARN("failed to clean up delete file state", K(tmp_ret), K(delete_file));
    ret = COVER_SUCC(tmp_ret);
  }

  return ret;
}

int ObIcebergDeleteBitmapBuilder::reset_delete_file_state()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(delete_file_prebuffer_.reset())) {
    LOG_WARN("failed to reset delete file prebuffer", K(ret));
  }
  orc_reader_.reset();
  parquet_reader_.reset();
  puffin_reader_.reset();
  if (delete_file_access_driver_.is_opened()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(delete_file_access_driver_.close())) {
      LOG_WARN("failed to close delete file access driver", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

// ==================== OrcDeleteFileReader ===================
int ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::init()
{
  int ret = OB_SUCCESS;
  orc_alloc_.init(MTL_ID());
  return ret;
}

void ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::reset()
{
  delete_data_batch_.reset();
  delete_row_reader_.reset();
  delete_reader_.reset();
  file_path_batch_idx_ = -1;
  pos_batch_idx_ = -1;
  file_path_column_id_ = -1;
  is_file_scoped_ = false;
}

int ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::open_delete_file(
    const ObLakeDeleteFile &delete_file,
    ObExternalFileAccess &file_access_driver,
    ObFilePreBuffer &file_prebuffer,
    const storage::ObTableScanParam *scan_param,
    ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;
  const int64_t file_size = delete_file.file_size_;
  ObExternalFileUrlInfo file_info(scan_param->external_file_location_,
                                  scan_param->external_file_access_info_,
                                  delete_file.file_url_,
                                  ObString::make_empty_string(),
                                  file_size,
                                  delete_file.modification_time_);
  ObExternalFileCacheOptions cache_options(options->enable_memory_cache_,
                                           options->enable_disk_cache_);
  if (OB_FAIL(file_access_driver.open(file_info, cache_options))) {
    LOG_WARN("failed to open orc position delete file", K(ret), K(file_info));
  }

  if (OB_SUCC(ret)) {
    try {
      std::unique_ptr<ObOrcFileAccess> inStream(new ObOrcFileAccess(file_access_driver,
                                                                    delete_file.file_url_.ptr(),
                                                                    delete_file.file_size_));
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
      }
    }
    CATCH_ORC_EXCEPTIONS

    if (OB_SUCC(ret)) {
      try {
        int64_t file_path_field_idx = -1;
        int64_t pos_field_idx = -1;
        if (OB_FAIL(find_orc_position_delete_fields(delete_reader_->getType(),
                                                    true,
                                                    file_path_field_idx,
                                                    pos_field_idx))) {
          LOG_WARN("invalid orc position delete schema", K(ret), K(delete_file.file_url_));
        } else {
          file_path_column_id_
              = static_cast<int64_t>(delete_reader_->getType()
                                         .getSubtype(static_cast<uint64_t>(file_path_field_idx))
                                         ->getColumnId());
          std::list<uint64_t> include_field_idxs = {static_cast<uint64_t>(pos_field_idx)};
          if (!delete_file.is_file_scoped_) {
            include_field_idxs.push_front(static_cast<uint64_t>(file_path_field_idx));
          }
          orc::RowReaderOptions row_reader_options;
          row_reader_options.include(include_field_idxs);
          delete_row_reader_ = delete_reader_->createRowReader(row_reader_options);
          is_file_scoped_ = delete_file.is_file_scoped_;
        }
      }
      CATCH_ORC_EXCEPTIONS
    }

    if (OB_SUCC(ret)) {
      try {
        // RowReader projects the source schema, so its StructVectorBatch
        // field indexes may differ from the source field indexes.
        if (!delete_row_reader_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create row reader failed", K(ret));
        } else if (OB_FAIL(find_orc_position_delete_fields(delete_row_reader_->getSelectedType(),
                                                           !is_file_scoped_,
                                                           file_path_batch_idx_,
                                                           pos_batch_idx_))) {
          LOG_WARN("invalid selected schema for orc position delete file",
                   K(ret),
                   K(delete_file.file_url_));
        } else {
          int64_t capacity = MAX(1, scan_param->op_->get_eval_ctx().max_batch_size_);
          delete_data_batch_ = delete_row_reader_->createRowBatch(capacity);
          if (OB_ISNULL(delete_data_batch_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to create row batch for orc position delete file",
                     K(ret),
                     K(capacity));
          }
        }
      }
      CATCH_ORC_EXCEPTIONS
    }
  }

  return ret;
}

int ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::add_path_scoped_positions(
    const orc::LongVectorBatch &position_batch,
    const orc::StringVectorBatch &file_path_batch,
    int64_t row_count,
    const ObString &data_file_path,
    int64_t data_file_record_count,
    ObRoaringBitmap *delete_bitmap,
    bool &range_search_finished) const
{
  int ret = OB_SUCCESS;
  int64_t match_begin = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count && !range_search_finished; ++i) {
    bool is_match = false;
    if (OB_FAIL(
            match_position_delete_file_path(data_file_path,
                                            reinterpret_cast<const char *>(file_path_batch.data[i]),
                                            file_path_batch.length[i],
                                            is_match,
                                            range_search_finished))) {
      LOG_WARN("failed to match file path in orc position delete file", K(ret), K(i));
    } else if (is_match) {
      if (match_begin < 0) {
        match_begin = i;
      }
    } else {
      if (match_begin >= 0
          && OB_FAIL(add_delete_positions(position_batch.data.data() + match_begin,
                                          i - match_begin,
                                          data_file_record_count,
                                          data_file_path,
                                          delete_bitmap))) {
        LOG_WARN("failed to batch add orc position deletes", K(ret), K(match_begin), K(i));
      }
      match_begin = -1;
    }
  }
  if (OB_SUCC(ret) && match_begin >= 0
      && OB_FAIL(add_delete_positions(position_batch.data.data() + match_begin,
                                      row_count - match_begin,
                                      data_file_record_count,
                                      data_file_path,
                                      delete_bitmap))) {
    LOG_WARN("failed to batch add orc position deletes", K(ret), K(match_begin), K(row_count));
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::process_position_batch(
    const ObString &data_file_path,
    int64_t data_file_record_count,
    ObRoaringBitmap *delete_bitmap,
    bool &range_search_finished) const
{
  int ret = OB_SUCCESS;
  orc::StructVectorBatch *root = static_cast<orc::StructVectorBatch *>(delete_data_batch_.get());
  orc::LongVectorBatch *position_batch
      = static_cast<orc::LongVectorBatch *>(root->fields[pos_batch_idx_]);
  const int64_t row_count = delete_data_batch_->numElements;
  if (position_batch->hasNulls) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("null pos in orc position delete file", K(ret));
  } else if (is_file_scoped_) {
    if (OB_FAIL(add_delete_positions(position_batch->data.data(),
                                     row_count,
                                     data_file_record_count,
                                     data_file_path,
                                     delete_bitmap))) {
      LOG_WARN("failed to process file-scoped orc position batch", K(ret), K(row_count));
    }
  } else {
    orc::StringVectorBatch *file_path_batch
        = static_cast<orc::StringVectorBatch *>(root->fields[file_path_batch_idx_]);
    if (file_path_batch->hasNulls) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("null file_path in orc position delete file", K(ret));
    } else if (OB_FAIL(add_path_scoped_positions(*position_batch,
                                                 *file_path_batch,
                                                 row_count,
                                                 data_file_path,
                                                 data_file_record_count,
                                                 delete_bitmap,
                                                 range_search_finished))) {
      LOG_WARN("failed to process path-scoped orc position batch", K(ret), K(row_count));
    }
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::OrcDeleteFileReader::read_delete_file(
    const ObString &data_file_path,
    const storage::ObTableScanParam *scan_param,
    int64_t data_file_record_count,
    ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;
  try {
    const int64_t nstripes = delete_reader_->getNumberOfStripes();
    bool range_search_finished = false;
    uint64_t stripe_first_row_id = 0;

    for (int64_t stripe_idx = 0; OB_SUCC(ret) && stripe_idx < nstripes && !range_search_finished;
         ++stripe_idx) {
      std::unique_ptr<orc::StripeInformation> stripe = delete_reader_->getStripe(stripe_idx);
      if (OB_UNLIKELY(!stripe)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("null stripe in orc position delete file", K(ret), K(stripe_idx));
      } else {
        const int64_t stripe_num_rows = stripe->getNumberOfRows();
        bool skip_stripe = false;
        if (!is_file_scoped_) {
          prune_orc_position_delete_stripe(*delete_reader_,
                                           stripe_idx,
                                           file_path_column_id_,
                                           data_file_path,
                                           skip_stripe,
                                           range_search_finished);
        }
        if (skip_stripe) {
          stripe_first_row_id += static_cast<uint64_t>(stripe_num_rows);
          if (!range_search_finished && stripe_idx + 1 < nstripes) {
            delete_row_reader_->seekToRow(stripe_first_row_id);
          }
          LOG_TRACE("skip orc position delete stripe by path statistics",
                    K(stripe_idx),
                    K(data_file_path));
        } else {
          int64_t rows_read = 0;
          while (OB_SUCC(ret) && rows_read < stripe_num_rows && !range_search_finished) {
            const int64_t batch_size = MIN(MAX(1, scan_param->op_->get_eval_ctx().max_batch_size_),
                                           stripe_num_rows - rows_read);
            delete_data_batch_->capacity = batch_size;
            if (!delete_row_reader_->next(*delete_data_batch_)) {
              ret = OB_INVALID_EXTERNAL_FILE;
              LOG_WARN("unexpected end of orc position delete file",
                       K(ret),
                       K(stripe_idx),
                       K(rows_read),
                       K(stripe_num_rows));
            } else {
              const int64_t row_count = delete_data_batch_->numElements;
              if (OB_UNLIKELY(row_count <= 0)) {
                ret = OB_INVALID_EXTERNAL_FILE;
                LOG_WARN("empty orc position delete batch", K(ret), K(stripe_idx));
              } else if (OB_FAIL(process_position_batch(data_file_path,
                                                        data_file_record_count,
                                                        delete_bitmap,
                                                        range_search_finished))) {
                LOG_WARN("failed to process orc position batch", K(ret), K(stripe_idx));
              }
              rows_read += row_count;
            }
          }
          stripe_first_row_id += static_cast<uint64_t>(stripe_num_rows);
        }
      }
    }
  }
  CATCH_ORC_EXCEPTIONS
  return ret;
}

// ==================== ParquetDeleteFileReader ====================
int ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::init()
{
  int ret = OB_SUCCESS;
  arrow_alloc_.init(MTL_ID());
  return ret;
}

void ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::reset()
{
  delete_file_reader_.reset();
  file_path_column_idx_ = -1;
  pos_column_idx_ = -1;
  is_file_scoped_ = false;
}

int ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::open_delete_file(
    const ObLakeDeleteFile &delete_file,
    ObExternalFileAccess &file_access_driver,
    ObFilePreBuffer &file_prebuffer,
    const storage::ObTableScanParam *scan_param,
    ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;

  try {
    std::shared_ptr<ObArrowFile> cur_file
        = std::make_shared<ObArrowFile>(file_access_driver,
                                        delete_file.file_url_.ptr(),
                                        &arrow_alloc_);
    const int64_t file_size = delete_file.file_size_;
    ObExternalFileUrlInfo file_info(scan_param->external_file_location_,
                                    scan_param->external_file_access_info_,
                                    delete_file.file_url_,
                                    ObString::make_empty_string(),
                                    file_size,
                                    delete_file.modification_time_);
    ObExternalFileCacheOptions cache_options(options->enable_memory_cache_,
                                             options->enable_disk_cache_);
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
      } else {
        std::shared_ptr<parquet::FileMetaData> file_meta = delete_file_reader_->metadata();
        if (OB_FAIL(find_parquet_position_delete_columns(*file_meta,
                                                         file_path_column_idx_,
                                                         pos_column_idx_))) {
          LOG_WARN("invalid parquet position delete schema", K(ret), K(delete_file.file_url_));
        } else {
          is_file_scoped_ = delete_file.is_file_scoped_;
        }
      }
    }
  }
  CATCH_PARQUET_EXCEPTIONS

  return ret;
}

int ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::find_matching_row_range(
    const std::shared_ptr<parquet::RowGroupReader> &row_group_reader,
    const ObString &data_file_path,
    int row_group_idx,
    int64_t batch_size,
    ObArrayWrap<parquet::ByteArray> &file_path_values,
    int64_t &begin_file_row,
    int64_t &end_file_row,
    bool &range_search_finished) const
{
  int ret = OB_SUCCESS;
  begin_file_row = is_file_scoped_ ? 0 : -1;
  end_file_row = is_file_scoped_ ? row_group_reader->metadata()->num_rows() - 1 : -1;

  if (!is_file_scoped_) {
    std::shared_ptr<parquet::ColumnReader> file_path_reader
        = row_group_reader->Column(file_path_column_idx_);
    parquet::ByteArrayReader *ba_reader
        = static_cast<parquet::ByteArrayReader *>(file_path_reader.get());
    int64_t cumulative_rows_read = 0;
    while (OB_SUCC(ret) && ba_reader->HasNext() && !range_search_finished) {
      int64_t values_read = 0;
      ba_reader->ReadBatch(batch_size, nullptr, nullptr, file_path_values.get_data(), &values_read);
      if (OB_UNLIKELY(values_read <= 0)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("failed to read file_path values from parquet position delete file",
                 K(ret),
                 K(row_group_idx),
                 K(values_read));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < values_read && !range_search_finished; ++i) {
        const parquet::ByteArray &current_value = file_path_values.at(i);
        bool is_match = false;
        if (OB_FAIL(match_position_delete_file_path(data_file_path,
                                                    pointer_cast<const char *>(current_value.ptr),
                                                    static_cast<int64_t>(current_value.len),
                                                    is_match,
                                                    range_search_finished))) {
          LOG_WARN("failed to match file path in parquet position delete file",
                   K(ret),
                   K(row_group_idx),
                   K(i));
        } else if (is_match) {
          const int64_t current_file_row = cumulative_rows_read + i;
          if (begin_file_row < 0) {
            begin_file_row = current_file_row;
          }
          end_file_row = current_file_row;
        }
      }
      cumulative_rows_read += values_read;
    }
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::read_and_add_position_range(
    const std::shared_ptr<parquet::RowGroupReader> &row_group_reader,
    const ObString &data_file_path,
    int row_group_idx,
    int64_t batch_size,
    int64_t begin_file_row,
    int64_t end_file_row,
    int64_t data_file_record_count,
    ObArrayWrap<int64_t> &position_values,
    ObRoaringBitmap *delete_bitmap) const
{
  int ret = OB_SUCCESS;
  if (begin_file_row >= 0 && end_file_row >= begin_file_row) {
    std::shared_ptr<parquet::ColumnReader> position_reader
        = row_group_reader->Column(pos_column_idx_);
    parquet::Int64Reader *int64_reader = static_cast<parquet::Int64Reader *>(position_reader.get());
    int64_t current_row_idx = 0;
    if (begin_file_row > 0) {
      const int64_t skipped = int64_reader->Skip(begin_file_row);
      if (OB_UNLIKELY(skipped != begin_file_row)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("failed to skip to matching position delete rows",
                 K(ret),
                 K(row_group_idx),
                 K(begin_file_row),
                 K(skipped));
      } else {
        current_row_idx = skipped;
      }
    }
    while (OB_SUCC(ret) && current_row_idx <= end_file_row) {
      int64_t values_read = 0;
      const int64_t rows_to_read = MIN(batch_size, end_file_row - current_row_idx + 1);
      int64_reader->ReadBatch(rows_to_read,
                              nullptr,
                              nullptr,
                              position_values.get_data(),
                              &values_read);
      if (OB_UNLIKELY(values_read <= 0)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("failed to read pos values from parquet position delete file",
                 K(ret),
                 K(row_group_idx),
                 K(values_read));
      }
      if (OB_SUCC(ret)
          && OB_FAIL(add_delete_positions(position_values.get_data(),
                                          values_read,
                                          data_file_record_count,
                                          data_file_path,
                                          delete_bitmap))) {
        LOG_WARN("failed to batch add parquet position deletes",
                 K(ret),
                 K(row_group_idx),
                 K(values_read));
      }
      current_row_idx += values_read;
    }
  }
  return ret;
}

int ObIcebergDeleteBitmapBuilder::ParquetDeleteFileReader::read_delete_file(
    const ObString &data_file_path,
    const storage::ObTableScanParam *scan_param,
    int64_t data_file_record_count,
    ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = MAX(1, scan_param->op_->get_eval_ctx().max_batch_size_);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(scan_param->op_->get_eval_ctx());
  ObArrayWrap<int64_t> int_values;
  ObArrayWrap<parquet::ByteArray> ba_values;
  if (OB_FAIL(int_values.allocate_array(tmp_alloc_g.get_allocator(), batch_size))) {
    LOG_WARN("failed to allocate parquet position buffer", K(ret), K(batch_size));
  } else if (!is_file_scoped_
             && OB_FAIL(ba_values.allocate_array(tmp_alloc_g.get_allocator(), batch_size))) {
    LOG_WARN("failed to allocate parquet path buffer", K(ret), K(batch_size));
  }

  try {
    const int num_row_groups = delete_file_reader_->metadata()->num_row_groups();

    bool range_search_finished = false;
    for (int r = 0; OB_SUCC(ret) && r < num_row_groups && !range_search_finished; ++r) {
      std::shared_ptr<parquet::RowGroupReader> row_group_reader = delete_file_reader_->RowGroup(r);
      if (OB_ISNULL(row_group_reader) || OB_ISNULL(row_group_reader->metadata())) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("parquet position delete row group is null", K(ret), K(r));
      } else {
        bool skip_row_group = false;
        if (!is_file_scoped_) {
          prune_parquet_position_delete_row_group(row_group_reader,
                                                  file_path_column_idx_,
                                                  data_file_path,
                                                  skip_row_group,
                                                  range_search_finished);
        }
        if (skip_row_group) {
          LOG_TRACE("skip parquet position delete row group by path statistics",
                    K(r),
                    K(data_file_path));
        } else {
          int64_t begin_file_row = -1;
          int64_t end_file_row = -1;
          if (OB_FAIL(find_matching_row_range(row_group_reader,
                                              data_file_path,
                                              r,
                                              batch_size,
                                              ba_values,
                                              begin_file_row,
                                              end_file_row,
                                              range_search_finished))) {
            LOG_WARN("failed to find matching parquet position delete rows", K(ret), K(r));
          } else if (OB_FAIL(read_and_add_position_range(row_group_reader,
                                                         data_file_path,
                                                         r,
                                                         batch_size,
                                                         begin_file_row,
                                                         end_file_row,
                                                         data_file_record_count,
                                                         int_values,
                                                         delete_bitmap))) {
            LOG_WARN("failed to read matching parquet position deletes", K(ret), K(r));
          }
        }
      }
    }
  }
  CATCH_PARQUET_EXCEPTIONS
  return ret;
}

// ==================== PuffinDeleteFileReader ===================
void ObIcebergDeleteBitmapBuilder::PuffinDeleteFileReader::reset()
{
  file_access_driver_ = nullptr;
  file_prebuffer_ = nullptr;
  file_content_offset_ = 0;
  file_content_size_in_bytes_ = 0;
}

int ObIcebergDeleteBitmapBuilder::PuffinDeleteFileReader::open_delete_file(
    const ObLakeDeleteFile &delete_file,
    ObExternalFileAccess &file_access_driver,
    ObFilePreBuffer &file_prebuffer,
    const storage::ObTableScanParam *scan_param,
    ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(delete_file.dv_content_offset_ < 0
                  || delete_file.dv_content_size_in_bytes_ < PUFFIN_LENGTH_SIZE + PUFFIN_MAGIC_SIZE
                                                                 + PUFFIN_MIN_ROARING_SIZE
                                                                 + PUFFIN_CRC_SIZE
                  || delete_file.dv_content_offset_
                         > delete_file.file_size_ - delete_file.dv_content_size_in_bytes_)) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("invalid puffin deletion vector range", K(ret), K(delete_file));
  } else {
    ObExternalFileUrlInfo file_info(scan_param->external_file_location_,
                                    scan_param->external_file_access_info_,
                                    delete_file.file_url_,
                                    ObString::make_empty_string(),
                                    delete_file.file_size_,
                                    delete_file.modification_time_);
    ObExternalFileCacheOptions cache_options(options->enable_memory_cache_,
                                             options->enable_disk_cache_);

    if (OB_FAIL(file_access_driver.open(file_info, cache_options))) {
      LOG_WARN("failed to open puffin deletion vector", K(ret), K(delete_file.file_url_));
    } else {
      file_access_driver_ = &file_access_driver;
      file_prebuffer_ = &file_prebuffer;
      file_content_offset_ = delete_file.dv_content_offset_;
      file_content_size_in_bytes_ = delete_file.dv_content_size_in_bytes_;
    }
  }

  return ret;
}

int ObIcebergDeleteBitmapBuilder::PuffinDeleteFileReader::read_delete_file(
    const ObString &data_file_path,
    const storage::ObTableScanParam *scan_param,
    int64_t data_file_record_count,
    ObRoaringBitmap *delete_bitmap)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  // Allocate a buffer for the selected DV blob only.
  char *file_buffer = nullptr;
  const int64_t buffer_size = file_content_size_in_bytes_;

  if (OB_ISNULL(file_buffer = static_cast<char *>(allocator.alloc(buffer_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buffer for file data", K(ret), K(buffer_size));
  } else {
    int64_t read_size = 0;
    const int64_t io_timeout_ms
        = MAX(0, (scan_param->timeout_ - ObTimeUtility::current_time()) / 1000);
    ObExternalReadInfo read_info(file_content_offset_, file_buffer, buffer_size, io_timeout_ms);

    // Read the complete DV blob from prebuffer or object storage.
    bool is_hit_cache = false;
    if (OB_FAIL(file_prebuffer_->read(file_content_offset_, buffer_size, file_buffer))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to read from prebuffer", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      is_hit_cache = true;
    }

    if (OB_FAIL(ret)) {
    } else if (!is_hit_cache && OB_FAIL(file_access_driver_->pread(read_info, read_size))) {
      LOG_WARN("failed to read delete file data", K(ret), K(file_content_size_in_bytes_));
    } else if (!is_hit_cache && OB_UNLIKELY(read_size != buffer_size)) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("incomplete puffin deletion vector read", K(ret), K(read_size), K(buffer_size));
    } else {
      const uint32_t combined_length = decode_big_endian_uint32(file_buffer);
      const int64_t expected_combined_length
          = file_content_size_in_bytes_ - PUFFIN_LENGTH_SIZE - PUFFIN_CRC_SIZE;
      if (OB_UNLIKELY(static_cast<int64_t>(combined_length) != expected_combined_length)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("invalid puffin deletion vector length",
                 K(ret),
                 K(combined_length),
                 K(expected_combined_length),
                 K(file_content_size_in_bytes_));
      } else if (0
                 != MEMCMP(file_buffer + PUFFIN_LENGTH_SIZE,
                           PUFFIN_MAGIC_BYTES,
                           PUFFIN_MAGIC_SIZE)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("invalid puffin deletion vector magic", K(ret));
      } else {
        const char *crc_ptr = file_buffer + PUFFIN_LENGTH_SIZE + combined_length;
        const uint32_t expected_crc = decode_big_endian_uint32(crc_ptr);
        const uint32_t actual_crc = static_cast<uint32_t>(
            ::crc32(0L,
                    reinterpret_cast<const Bytef *>(file_buffer + PUFFIN_LENGTH_SIZE),
                    static_cast<uInt>(combined_length)));
        // The deletion-vector-v1 format requires length, magic and CRC
        // validation before the portable Roaring payload is trusted.
        if (OB_UNLIKELY(expected_crc != actual_crc)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("puffin deletion vector crc mismatch", K(ret), K(expected_crc), K(actual_crc));
        } else {
          const char *roaring_data = file_buffer + PUFFIN_LENGTH_SIZE + PUFFIN_MAGIC_SIZE;
          const int64_t roaring_data_size = combined_length - PUFFIN_MAGIC_SIZE;
          if (OB_FAIL(delete_bitmap->deserialize_portable(roaring_data, roaring_data_size, true))) {
            const int deserialize_ret = ret;
            ret = OB_ALLOCATE_MEMORY_FAILED == deserialize_ret ? deserialize_ret
                                                               : OB_INVALID_EXTERNAL_FILE;
            LOG_WARN("failed to deserialize puffin deletion vector",
                     K(ret),
                     K(deserialize_ret),
                     K(roaring_data_size));
          } else if (delete_bitmap->get_cardinality() > 0
                     && OB_UNLIKELY(delete_bitmap->get_max()
                                    >= static_cast<uint64_t>(data_file_record_count))) {
            ret = OB_INVALID_EXTERNAL_FILE;
            LOG_WARN("puffin deletion vector position is out of range",
                     K(ret),
                     "max_position",
                     delete_bitmap->get_max(),
                     K(data_file_record_count),
                     K(data_file_path));
            delete_bitmap->set_empty();
          }
        }
      }
    }
  }

  return ret;
}

int ObIcebergDeleteBitmapBuilder::pre_buffer_delete_file(const ObLakeDeleteFile &delete_file)
{
  int ret = OB_SUCCESS;
  ObFilePreBuffer::ColumnRangeSlicesList column_range_slice_list;
  ObFilePreBuffer::ColumnRangeSlices column_range_slices;
  int64_t offset = 0;
  int64_t size = 0;
  bool split_large_range = false;
  if (ObLakeDeleteFileType::POSITION_DELETE == delete_file.type_) {
    size = delete_file.file_size_;
    split_large_range = true;
  } else if (ObLakeDeleteFileType::DELETION_VECTOR == delete_file.type_) {
    offset = delete_file.dv_content_offset_;
    size = delete_file.dv_content_size_in_bytes_;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported delete file type", K(ret), K(delete_file));
  }

  if (OB_SUCC(ret)) {
    // Position delete files can be large. Split them by the configured range
    // limit so lazy prebuffering fetches data on demand instead of allocating
    // one buffer for the complete file. A DV stays contiguous because its
    // portable bitmap is deserialized from one buffer.
    const int64_t range_size_limit = options_->cache_options_.range_size_limit_;
    const int64_t chunk_size = split_large_range && range_size_limit > 0 ? range_size_limit : size;
    const int64_t end_offset = offset + size;
    for (int64_t current_offset = offset; OB_SUCC(ret) && current_offset < end_offset;
         current_offset += MIN(chunk_size, end_offset - current_offset)) {
      const int64_t current_size = MIN(chunk_size, end_offset - current_offset);
      if (OB_FAIL(column_range_slices.range_list_.push_back(
              ObFilePreBuffer::ReadRange(current_offset, current_size)))) {
        LOG_WARN("failed to push back range", K(ret), K(current_offset), K(current_size));
      }
    }
  }
  if (OB_FAIL(ret)) {
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
