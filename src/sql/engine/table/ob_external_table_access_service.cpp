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
#include "ob_external_table_access_service.h"

#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_device_manager.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/table/ob_parquet_table_row_iter.h"

namespace oceanbase
{
namespace common {
extern const char *OB_STORAGE_ACCESS_TYPES_STR[];
}

using namespace share::schema;
using namespace common;
using namespace share;
namespace sql
{

ObExternalDataAccessDriver::~ObExternalDataAccessDriver() {
  close();
  if (OB_NOT_NULL(device_handle_)) {
    ObDeviceManager::get_instance().release_device(device_handle_);
  }
}

void ObExternalDataAccessDriver::close()
{
  if (OB_NOT_NULL(device_handle_) && fd_.is_valid()) {
    device_handle_->close(fd_);
    fd_.reset();
  }
}

bool ObExternalDataAccessDriver::is_opened() const
{
  return fd_.is_valid();
}

int ObExternalDataAccessDriver::get_file_sizes(const ObString &location,
                                               const ObIArray<ObString> &urls,
                                               ObIArray<int64_t> &file_sizes)
{
  int ret = OB_SUCCESS;
  file_sizes.reuse();
  ObString tmp_location(location.length(), location.ptr());
  for (int64_t i = 0; OB_SUCC(ret) && i < urls.count(); i++) {
    int64_t len = 0;
    ObSqlString path;
    if (OB_FAIL(path.append(tmp_location.trim())) ||
        (*(path.ptr() + path.length() - 1) != '/' && OB_FAIL(path.append("/"))) ||
        OB_FAIL(path.append(urls.at(i)))) {
      LOG_WARN("append string failed", K(ret));
    } else if (OB_FAIL(get_file_size(path.string(), len))) {
      LOG_WARN("get file size failed", K(ret));
    } else if (OB_FAIL(file_sizes.push_back(len))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}
int ObExternalDataAccessDriver::get_file_size(const ObString &url, int64_t &file_size)
{
  int ret = OB_SUCCESS;

  ObBackupIoAdapter util;

  if (OB_ISNULL(device_handle_)) {
    ret = OB_NOT_INIT;
  } else {
    ObIODFileStat statbuf;
    int temp_ret = device_handle_->stat(to_cstring(url), statbuf);
    if (OB_SUCCESS != temp_ret) {
      file_size = -1;
      if (OB_BACKUP_FILE_NOT_EXIST == temp_ret
          || OB_IO_ERROR == temp_ret) {
        file_size = -1;
      } else {
        ret = temp_ret;
      }
      LOG_WARN("fail to get file length", K(temp_ret), K(url));
    } else {
      file_size = statbuf.size_;
    }
  }
  return ret;
}

int ObExternalDataAccessDriver::open(const char *url)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(device_handle_)) {
    ret = OB_NOT_INIT;
  } else {
    ret = device_handle_->open(url, -1, 0, fd_, &iod_opts_);
  }
  return ret;
}

int ObExternalDataAccessDriver::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(device_handle_)) {
    ret = OB_NOT_INIT;
  } else {
    ret = device_handle_->pread(fd_, offset, count, buf, read_size);
  }
  return ret;
}

class ObExternalFileListArrayOpWithFilter : public ObBaseDirEntryOperator
{
public:
  ObExternalFileListArrayOpWithFilter(ObIArray <common::ObString>& name_array,
                                      ObIArray <int64_t>& file_size,
                              ObExternalPathFilter *filter,
                              ObIAllocator& array_allocator)
    : name_array_(name_array), file_size_(file_size), filter_(filter), allocator_(array_allocator) {}

  virtual bool need_get_file_size() const override { return true; }
  int func(const dirent *entry) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid list entry, entry is null");
    } else if (OB_ISNULL(entry->d_name)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid list entry, d_name is null");
    } else {
      const ObString file_name(entry->d_name);
      ObString tmp_file;
      bool is_filtered = false;
      if (!file_name.empty() && file_name[file_name.length() - 1] != '/') {
        if (OB_NOT_NULL(filter_) && OB_FAIL(filter_->is_filtered(file_name, is_filtered))) {
          LOG_WARN("fail check is filtered", K(ret));
        } else if (!is_filtered) {
          if (OB_FAIL(ob_write_string(allocator_, file_name, tmp_file, true))) {
            OB_LOG(WARN, "fail to save file name", K(ret), K(file_name));
          } else if (OB_FAIL(name_array_.push_back(tmp_file))) {
            OB_LOG(WARN, "fail to push filename to array", K(ret), K(tmp_file));
          } else if (OB_FAIL(file_size_.push_back(get_size()))) {
            OB_LOG(WARN, "fail to push size to array", K(ret), K(tmp_file));
          }
        }
      }
    }
    return ret;
  }

private:
  ObIArray <ObString>& name_array_;
  ObIArray <int64_t>& file_size_;
  ObExternalPathFilter *filter_;
  ObIAllocator& allocator_;
};

class ObLocalFileListArrayOpWithFilter : public ObBaseDirEntryOperator
{
public:
  ObLocalFileListArrayOpWithFilter(ObIArray <common::ObString> &name_array,
                                   ObIArray <int64_t>& file_size,
                                   const ObString &path,
                                   const ObString &origin_path,
                                   ObExternalPathFilter *filter,
                                   ObIAllocator &array_allocator)
    : name_array_(name_array), file_size_(file_size), path_(path), origin_path_(origin_path),
      filter_(filter), allocator_(array_allocator) {}
  virtual bool need_get_file_size() const override { return true; }
  int func(const dirent *entry)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid list entry, entry is null");
    } else if (OB_ISNULL(entry->d_name)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid list entry, d_name is null");
    } else {
      const ObString file_name(entry->d_name);
      ObSqlString full_path;
      ObString tmp_file;
      bool is_filtered = false;
      ObString cur_path = path_;
      if (file_name.case_compare(".") == 0
          || file_name.case_compare("..") == 0) {
        //do nothing
      } else if (OB_FAIL(full_path.assign(cur_path))) {
        OB_LOG(WARN, "assign string failed", K(ret));
      } else if (full_path.length() > 0 && *(full_path.ptr() + full_path.length() - 1) != '/' &&
                                                                OB_FAIL(full_path.append("/"))) {
        OB_LOG(WARN, "append failed", K(ret)) ;
      } else if (OB_FAIL(full_path.append(file_name))) {
        OB_LOG(WARN, "append file name failed", K(ret));
      } else if (OB_NOT_NULL(filter_) && OB_FAIL(filter_->is_filtered(full_path.string(), is_filtered))) {
        LOG_WARN("fail check is filtered", K(ret));
      } else if (!is_filtered) {
        ObString target = full_path.string();
        if (!is_dir_scan()) {
          target += origin_path_.length();
          if (!target.empty() && '/' == target[0]) {
            target += 1;
          }
        }
        if (target.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("empty dir or name", K(full_path), K(origin_path_));
        } else if (OB_FAIL(ob_write_string(allocator_, target, tmp_file))) {
          OB_LOG(WARN, "fail to save file name", K(ret), K(file_name));
        } else if (OB_FAIL(name_array_.push_back(tmp_file))) {
          OB_LOG(WARN, "fail to push filename to array", K(ret), K(tmp_file));
        } else if (OB_FAIL(file_size_.push_back(get_size()))) {
          OB_LOG(WARN, "fail to push size to array", K(ret), K(tmp_file));
        }
      }
    }
    return ret;
  }
private:
  ObIArray <ObString> &name_array_;
  ObIArray <int64_t> &file_size_;
  const ObString &path_;
  const ObString &origin_path_;
  ObExternalPathFilter *filter_;
  ObIAllocator &allocator_;
};


int ObExternalDataAccessDriver::get_file_list(const ObString &path,
                                              const ObString &pattern,
                                              const ObExprRegexpSessionVariables &regexp_vars,
                                              ObIArray<ObString> &file_urls,
                                              ObIArray<int64_t> &file_sizes,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_VISIT_COUNT = 100000;
  ObExprRegexContext regexp_ctx;
  ObExternalPathFilter filter(regexp_ctx, allocator);

  if (OB_ISNULL(device_handle_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalDataAccessDriver not init", K(ret));
  } else if (!pattern.empty() && OB_FAIL(filter.init(pattern, regexp_vars))) {
    LOG_WARN("fail to init filter", K(ret));
  } else if (get_storage_type() == OB_STORAGE_FILE) {
    ObSEArray<ObString, 4> file_dirs;
    bool is_dir = false;
    ObString path_without_prifix;
    path_without_prifix = path;
    path_without_prifix += strlen(OB_FILE_PREFIX);

    OZ (FileDirectoryUtils::is_directory(to_cstring(path_without_prifix), is_dir));
    if (!is_dir) {
      LOG_WARN("external location is not a directory", K(path_without_prifix));
    } else {
      OZ (file_dirs.push_back(path));
    }
    ObArray<int64_t> useless_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_dirs.count(); i++) {
      ObString file_dir = file_dirs.at(i);
      ObLocalFileListArrayOpWithFilter dir_op(file_dirs, useless_size, file_dir, path, NULL, allocator);
      ObLocalFileListArrayOpWithFilter file_op(file_urls, file_sizes, file_dir, path,
                                               pattern.empty() ? NULL : &filter, allocator);
      dir_op.set_dir_flag();
      if (OB_FAIL(device_handle_->scan_dir(to_cstring(file_dir), file_op))) {
        LOG_WARN("scan dir failed", K(ret));
      } else if (OB_FAIL(device_handle_->scan_dir(to_cstring(file_dir), dir_op))) {
        LOG_WARN("scan dir failed", K(ret));
      } else if (file_dirs.count() + file_urls.count() > MAX_VISIT_COUNT) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("too many files and dirs to visit", K(ret));
      }
    }
  } else {
    ObExternalFileListArrayOpWithFilter file_op(file_urls, file_sizes, pattern.empty() ? NULL : &filter, allocator);
    if (OB_FAIL(device_handle_->scan_dir(to_cstring(path), file_op))) {
      LOG_WARN("scan dir failed", K(ret));
    }
  }
  return ret;
}

int ObExternalDataAccessDriver::init(const ObString &location, const ObString &access_info)
{
  int ret = OB_SUCCESS;
  ObStorageType device_type = OB_STORAGE_MAX_TYPE;
  ObArenaAllocator temp_allocator;
  ObString location_cstr;
  ObString access_info_cstr;
  ObBackupIoAdapter util;

  iod_opts_.opts_ = opts_;
  iod_opts_.opt_cnt_ = 0;

  if (OB_FAIL(get_storage_type_from_path(location, device_type))) {
    LOG_WARN("fail to resove storage type", K(ret));
  } else {
    storage_type_ = device_type;
    if (device_type == OB_STORAGE_FILE) {
      OZ (ob_write_string(temp_allocator, location, location_cstr, true));
      access_info_cstr.assign_ptr(&dummy_empty_char, strlen(&dummy_empty_char));
    } else {
      OZ (ob_write_string(temp_allocator, location, location_cstr, true));
      OZ (ob_write_string(temp_allocator, access_info, access_info_cstr, true));
    }
  }

  OZ (access_info_.set(device_type, access_info_cstr.ptr()));
  OZ (util.get_and_init_device(device_handle_, &access_info_, location_cstr));
  OZ (util.set_access_type(&iod_opts_, false, 1));

  return ret;
}


int ObExternalTableAccessService::table_scan(
    ObVTableScanParam &param,
    ObNewRowIterator *&result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  ObExternalTableRowIterator* row_iter = NULL;

  auto &scan_param = static_cast<storage::ObTableScanParam&>(param);

  switch (param.external_file_format_.format_type_) {
    case ObExternalFileFormat::CSV_FORMAT:
      if (OB_ISNULL(row_iter = OB_NEWx(ObCSVTableRowIterator, (scan_param.allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      }
      break;

    case ObExternalFileFormat::PARQUET_FORMAT:
      if (OB_ISNULL(row_iter = OB_NEWx(ObParquetTableRowIterator, (scan_param.allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format", K(ret), "format", param.external_file_format_.format_type_);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter->init(&scan_param))) {
      LOG_WARN("fail to open iter", K(ret));
    } else {
      result = row_iter;
    }
  }

  LOG_DEBUG("external table access service iter init", K(ret), "type", param.external_file_format_.format_type_);

  return ret;
}

int ObExternalTableAccessService::table_rescan(ObVTableScanParam &param, ObNewRowIterator *result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter", K(ret));
  } else {
    switch (param.external_file_format_.format_type_) {
      case ObExternalFileFormat::CSV_FORMAT:
      case ObExternalFileFormat::PARQUET_FORMAT:
        result->reset();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected format", K(ret), "format", param.external_file_format_.format_type_);
    }
  }
  LOG_DEBUG("external table rescan", K(param.key_ranges_), K(param.range_array_pos_));
  return ret;
}

int ObExternalTableAccessService::reuse_scan_iter(const bool switch_param, ObNewRowIterator *iter)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  UNUSED(switch_param);
  iter->reset();
  return OB_SUCCESS;
}

int ObExternalTableAccessService::revert_scan_iter(ObNewRowIterator *iter)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter", K(ret));
  } else {
    iter->~ObNewRowIterator();
  }
  return ret;
}

int ObExternalTableRowIterator::init(const ObTableScanParam *scan_param)
{
   scan_param_ = scan_param;
   return init_exprs(scan_param);
}

ObCSVTableRowIterator::~ObCSVTableRowIterator()
{
  release_buf();
  if (nullptr != bit_vector_cache_) {
    malloc_alloc_.free(bit_vector_cache_);
  }
}

void ObCSVTableRowIterator::release_buf()
{
  if (nullptr != state_.buf_) {
    malloc_alloc_.free(state_.buf_);
  }

  if (nullptr != state_.escape_buf_) {
    malloc_alloc_.free(state_.escape_buf_);
  }
}

int ObCSVTableRowIterator::expand_buf()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUFFER_SIZE = (1 << 30); //MEMORY LIMIT 1G
  int64_t new_buf_len = 0;
  char *old_buf = state_.buf_;
  char *new_buf = nullptr;
  char *new_escape_buf = nullptr;

  if (nullptr != old_buf) {
    new_buf_len = state_.buf_len_ * 2;
  } else {
    if (data_access_driver_.get_storage_type() != OB_STORAGE_FILE) {
      //for better performance
      new_buf_len = OB_MALLOC_BIG_BLOCK_SIZE;
    } else {
      new_buf_len = OB_MALLOC_NORMAL_BLOCK_SIZE;
    }
  }

  if (OB_UNLIKELY(new_buf_len > MAX_BUFFER_SIZE)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer size overflow", K(ret), K(new_buf_len));
  } else if (OB_ISNULL(new_buf = static_cast<char *>(malloc_alloc_.alloc(new_buf_len)))
             || OB_ISNULL(new_escape_buf = static_cast<char *>(malloc_alloc_.alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (OB_NOT_NULL(new_buf)) {
      malloc_alloc_.free(new_buf);
    }
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    int64_t remain_len =  (nullptr != old_buf) ? (state_.data_end_ - state_.pos_) : 0;
    if (remain_len > 0) {
      MEMCPY(new_buf, state_.pos_, remain_len);
    }

    release_buf();

    state_.buf_ = new_buf;
    state_.escape_buf_ = new_escape_buf;
    state_.escape_buf_end_ = new_escape_buf + new_buf_len;
    state_.buf_len_ = new_buf_len;
    state_.pos_ = new_buf;
    state_.data_end_ = new_buf + remain_len;


    LOG_DEBUG("extend buf", K(new_buf_len));
  }

  return ret;
}

int ObExternalTableRowIterator::gen_ip_port(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_PORT_SQL_LENGTH];
  int32_t len = 0;
  OZ (GCONF.self_addr_.addr_to_buffer(buf, MAX_IP_PORT_SQL_LENGTH, len));
  OZ (ob_write_string(allocator, ObString(len, buf), ip_port_));
  return ret;
}

int ObExternalTableRowIterator::init_exprs(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else {
    if (scan_param->column_ids_.count() != scan_param->output_exprs_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column ids not equal to access expr", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < scan_param->column_ids_.count(); i++) {
      ObExpr *cur_expr = scan_param->output_exprs_->at(i);
      switch (scan_param->column_ids_.at(i)) {
        case OB_HIDDEN_LINE_NUMBER_COLUMN_ID:
          line_number_expr_ = cur_expr;
          break;
        case OB_HIDDEN_FILE_ID_COLUMN_ID:
          file_id_expr_ = cur_expr;
          break;
        default:
          OZ (column_exprs_.push_back(cur_expr));
          break;
      }
    }
    if (OB_SUCC(ret) && column_exprs_.count() != scan_param->ext_column_convert_exprs_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr not equal to convert convert expr", K(ret),
               K(column_exprs_), KPC(scan_param->ext_column_convert_exprs_));
    }
  }
  return ret;
}

int ObCSVTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else {
    malloc_alloc_.set_attr(lib::ObMemAttr(scan_param->tenant_id_, "CSVRowIter"));
    arena_alloc_.set_attr(lib::ObMemAttr(scan_param->tenant_id_, "CSVRowIter"));
    OZ (ObExternalTableRowIterator::init(scan_param));
    OZ (parser_.init(scan_param->external_file_format_.csv_format_));
    OZ (data_access_driver_.init(scan_param_->external_file_location_, scan_param->external_file_access_info_));
    OZ (expand_buf());

    if (OB_SUCC(ret)) {
      if (data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
        if (OB_ISNULL(state_.ip_port_buf_ = static_cast<char *>(arena_alloc_.alloc(max_ipv6_port_length)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCSVTableRowIterator::get_next_file_and_line_number(const int64_t task_idx,
                                                         ObString &file_url,
                                                         int64_t &file_id,
                                                         int64_t &part_id,
                                                         int64_t &start_line,
                                                         int64_t &end_line)
{
  int ret = OB_SUCCESS;
  if (task_idx >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObExternalTableUtils::resolve_line_number_range(
                                                              scan_param_->key_ranges_.at(task_idx),
                                                              ObExternalTableUtils::LINE_NUMBER,
                                                              start_line,
                                                              end_line))) {
    LOG_WARN("failed to resolve range in external table", K(ret));
  } else {
    part_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::PARTITION_ID].get_int();
    file_url = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
    file_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_ID].get_int();
  }
  return ret;
}

int ObExternalTableRowIterator::fill_file_partition_expr(ObExpr *expr, ObNewRow &value, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  ObDatum *datums = expr->locate_batch_datums(eval_ctx);
  int64_t loc_idx = expr->extra_ - 1;
  if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= value.get_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("loc idx is out of range", K(loc_idx), K(value), K(ret));
  } else {
    if (value.get_cell(loc_idx).is_null()) {
      for (int j = 0; OB_SUCC(ret) && j < row_count; j++) {
        datums[j].set_null();
      }
    } else {
      for (int j = 0; OB_SUCC(ret) && j < row_count; j++) {
        CK (OB_NOT_NULL(datums[j].ptr_));
        OZ (datums[j].from_obj(value.get_cell(loc_idx)));
      }
    }
  }
  return ret;
}

int ObExternalTableRowIterator::calc_file_partition_list_value(const int64_t part_id, ObIAllocator &allocator, ObNewRow &value)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObPartition *partition = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
              scan_param_->tenant_id_,
              schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(scan_param_->tenant_id_, scan_param_->index_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(scan_param_->index_id_), K(scan_param_->tenant_id_));
  } else if (table_schema->is_partitioned_table() && table_schema->is_user_specified_partition_for_external_table()) {
    if (OB_FAIL(table_schema->get_partition_by_part_id(part_id, CHECK_PARTITION_MODE_NORMAL, partition))) {
      LOG_WARN("get partition failed", K(ret), K(part_id));
    } else if (OB_ISNULL(partition) || OB_UNLIKELY(partition->get_list_row_values().count() != 1)
          || partition->get_list_row_values().at(0).get_count() != table_schema->get_partition_key_column_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is invalid", K(ret), K(part_id));
    } else {
      int64_t pos = 0;
      int64_t size = partition->get_list_row_values().at(0).get_deep_copy_size();
      char *buf = (char *)allocator.alloc(size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate mem failed", K(ret));
      }
      OZ (value.deep_copy(partition->get_list_row_values().at(0), buf, size, pos));
    }
  }
  return ret;
}
int ObCSVTableRowIterator::open_next_file()
{
  int ret = OB_SUCCESS;
  ObString location = scan_param_->external_file_location_;

  if (data_access_driver_.is_opened()) {
    data_access_driver_.close();
  }

  do {
    ObString file_url;
    int64_t file_id = 0;
    int64_t part_id = 0;
    int64_t start_line = 0;
    int64_t end_line = 0;
    int64_t task_idx = state_.file_idx_++;
    url_.reuse();
    ret = get_next_file_and_line_number(task_idx, file_url, file_id, part_id, start_line, end_line);
    if (OB_FAIL(ret)) {
    } else if (part_id == 0) {
      //empty file do not belong to any partitions
    } else {
      if (part_id != state_.part_id_) {
        state_.part_id_ = part_id;
        OZ (calc_file_partition_list_value(part_id, arena_alloc_, state_.part_list_val_));
      }
    }
    if (OB_SUCC(ret)) {
      if (start_line == MIN_EXTERNAL_TABLE_LINE_NUMBER && end_line == INT64_MAX) {
        state_.cur_file_name_ = file_url;
        state_.cur_file_id_ = file_id;
        state_.cur_line_number_ = MIN_EXTERNAL_TABLE_LINE_NUMBER;
        state_.skip_lines_ = parser_.get_format().skip_header_lines_;
        state_.line_count_limit_ = INT64_MAX;
      } else {
        // [3, 7] --> skip = 2, cnt = 5
        state_.cur_file_name_ = file_url;
        state_.cur_file_id_ = file_id;
        state_.cur_line_number_ = start_line;
        state_.skip_lines_ = parser_.get_format().skip_header_lines_ + start_line - 1;
        state_.line_count_limit_ = end_line - start_line + 1;
      }
      const char *split_char = "/";
      OZ (url_.append_fmt("%.*s%s%.*s", location.length(), location.ptr(),
                                        (location.empty() || location[location.length() - 1] == '/') ? "" : split_char,
                                        file_url.length(), file_url.ptr()));
      OZ (data_access_driver_.get_file_size(url_.string(), state_.file_size_));
      if (OB_SUCC(ret) && data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
        ObSqlString full_name;
        if (state_.ip_port_len_ == 0) {
          OZ (GCONF.self_addr_.addr_to_buffer(state_.ip_port_buf_, max_ipv6_port_length, state_.ip_port_len_));
        }
        OZ (full_name.append(state_.ip_port_buf_, state_.ip_port_len_));
        OZ (full_name.append("%"));
        OZ (full_name.append(this->state_.cur_file_name_));
        OZ (ob_write_string(arena_alloc_, full_name.string(), state_.file_with_url_));
      }
    }
    LOG_DEBUG("try next file", K(ret), K(url_), K(file_url), K(state_));
  } while (OB_SUCC(ret) && 0 >= state_.file_size_); //skip empty file
  OZ (data_access_driver_.open(url_.ptr()), url_);

  LOG_DEBUG("open external file", K(ret), K(url_), K(state_.file_size_), K(location));

  return ret;
}

int ObCSVTableRowIterator::load_next_buf()
{
  int ret = OB_SUCCESS;
  do {
    char *next_load_pos = NULL;
    int64_t next_buf_len = 0;
    if (state_.is_end_file_) {
      if (OB_FAIL(open_next_file())) {
        //do not print log
      } else {
        state_.is_end_file_ = false;
        state_.file_offset_ = 0;
        next_load_pos = state_.buf_;
        next_buf_len = state_.buf_len_;
      }
    } else {
      //move unfinish tail in old buf to the front
      int64_t remain_bytes = state_.data_end_ - state_.pos_;
      if (remain_bytes > 0) {
        if (state_.pos_ > state_.buf_) {
          MEMMOVE(state_.buf_, state_.pos_, remain_bytes);
        } else {
          // buffer is not big enough to store data line
          OZ (expand_buf());
        }
      }
      next_load_pos = state_.buf_ + remain_bytes;
      next_buf_len = state_.buf_len_ - remain_bytes;
    }

    if (OB_SUCC(ret)) {
      int64_t read_size = 0;
      OZ (data_access_driver_.pread(next_load_pos, next_buf_len, state_.file_offset_, read_size));
      if (OB_SUCC(ret)) {
        state_.file_offset_ += read_size;
        state_.pos_ = state_.buf_;
        state_.data_end_ = next_load_pos + read_size;
        state_.is_end_file_ = (state_.file_offset_ >= state_.file_size_);
      }
    }

  } while (false);
  return ret;
}

int ObCSVTableRowIterator::skip_lines()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  int64_t nrows = 0;
  auto temp_handle = [](ObIArray<ObCSVGeneralParser::FieldValue> &arr) -> int {
    UNUSED(arr);
    return OB_SUCCESS;
  };
  do {
    nrows = state_.skip_lines_;
    OZ (parser_.scan(state_.pos_, state_.data_end_, nrows, nullptr, nullptr,
                     temp_handle, error_msgs, state_.is_end_file_));
    error_msgs.reuse();
    state_.skip_lines_ -= nrows;
  } while (OB_SUCC(ret) && state_.skip_lines_ > 0 && OB_SUCC(load_next_buf()));
  return ret;
}

void ObCSVTableRowIterator::dump_error_log(ObIArray<ObCSVGeneralParser::LineErrRec> &error_msgs)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < error_msgs.count(); ++i) {
    LOG_WARN("parse row warning",
             "file_name", state_.cur_file_name_,
             "file_id", state_.cur_file_id_,
             "rownum", error_msgs.at(i).line_no + parser_.get_format().skip_header_lines_,
             "ret", common::ob_error_name(error_msgs.at(i).err_code));
  }
}

int ObCSVTableRowIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  bool is_oracle_mode = lib::is_oracle_mode();
  int64_t returned_row_cnt = 0; // rows count for scan output, exclude blank lines or skip header
  struct Functor {
    Functor(ObCSVTableRowIterator *csv_iter,
            const ExprFixedArray &file_column_exprs,
            ObEvalCtx &eval_ctx,
            bool is_oracle_mode,
            int64_t &returned_row_cnt) :
            csv_iter_(csv_iter), file_column_exprs_(file_column_exprs), eval_ctx_(eval_ctx),
            is_oracle_mode_(is_oracle_mode), returned_row_cnt_(returned_row_cnt)
    {}
    ObCSVTableRowIterator *csv_iter_;
    const ExprFixedArray &file_column_exprs_;
    ObEvalCtx &eval_ctx_;
    bool is_oracle_mode_;
    int64_t &returned_row_cnt_;

    int operator()(ObIArray<ObCSVGeneralParser::FieldValue> &arr) {
      int ret = OB_SUCCESS;
      for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
        ObDatum &datum = file_column_exprs_.at(i)->locate_datum_for_write(eval_ctx_);
        if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
          if (csv_iter_->data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
            datum.set_string(csv_iter_->state_.file_with_url_.ptr(), csv_iter_->state_.file_with_url_.length());
          } else {
            datum.set_string(csv_iter_->state_.cur_file_name_.ptr(), csv_iter_->state_.cur_file_name_.length());
          }
        } else if (file_column_exprs_.at(i)->type_ == T_PSEUDO_PARTITION_LIST_COL) {
          int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
          if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= csv_iter_->state_.part_list_val_.get_count())) {
            ret = OB_ERR_UNEXPECTED;
          } else if (csv_iter_->state_.part_list_val_.get_cell(loc_idx).is_null()) {
            datum.set_null();
          } else {
            CK (OB_NOT_NULL(datum.ptr_));
            OZ (datum.from_obj(csv_iter_->state_.part_list_val_.get_cell(loc_idx)));
          }
        } else if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
          int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
          if (OB_UNLIKELY(loc_idx < 0 || loc_idx > arr.count())) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            if (arr.at(loc_idx).is_null_ || (0 == arr.at(loc_idx).len_ && is_oracle_mode_)) {
              datum.set_null();
            } else {
              datum.set_string(arr.at(loc_idx).ptr_, arr.at(loc_idx).len_);
            }
          }
        }
      }

      returned_row_cnt_++;
      return ret;
    }
  };

  struct Functor handle_one_line(this, file_column_exprs, eval_ctx, is_oracle_mode, returned_row_cnt);

  int64_t nrows = 0;
  do {
    if (state_.skip_lines_ > 0) {
      OZ (skip_lines());
    }
    if (OB_SUCC(ret)) {
      nrows = MIN(1, state_.line_count_limit_);
      if (OB_UNLIKELY(0 == nrows)) {
        // if line_count_limit = 0, get next file.
        state_.is_end_file_ = true;
      } else {
        ret = parser_.scan<decltype(handle_one_line), true>(state_.pos_, state_.data_end_, nrows,
                                                  state_.escape_buf_, state_.escape_buf_end_,
                                                  handle_one_line, error_msgs, state_.is_end_file_);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to scan csv", K(ret));
        } else if (OB_UNLIKELY(error_msgs.count() > 0)) {
          dump_error_log(error_msgs);
        }
      }
    }
  } while (OB_SUCC(ret) && returned_row_cnt < 1 && OB_SUCC(load_next_buf()));
  if (OB_SUCC(ret) && returned_row_cnt > 0) {
    if (OB_NOT_NULL(file_id_expr_)) {
      ObDatum &datum = file_id_expr_->locate_datum_for_write(eval_ctx);
      datum.set_int(state_.cur_file_id_);
    }
    if (OB_NOT_NULL(line_number_expr_)) {
      ObDatum &datum = line_number_expr_->locate_datum_for_write(eval_ctx);
      datum.set_int(state_.cur_line_number_);
    }
    state_.line_count_limit_ -= returned_row_cnt;
    state_.cur_line_number_ += returned_row_cnt;
  }

  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
    file_column_exprs.at(i)->set_evaluated_flag(eval_ctx);
  }

  for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
    ObExpr *column_expr = column_exprs_.at(i);
    ObExpr *column_convert_expr = scan_param_->ext_column_convert_exprs_->at(i);
    ObDatum *convert_datum = NULL;
    OZ (column_convert_expr->eval(eval_ctx, convert_datum));
    if (OB_SUCC(ret)) {
      column_expr->locate_datum_for_write(eval_ctx) = *convert_datum;
      column_expr->set_evaluated_flag(eval_ctx);
    }
  }

  return ret;
}

int ObCSVTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  int64_t batch_size = capacity;
  int64_t returned_row_cnt = 0; // rows count for scan output, exclude blank lines or skip header
  bool is_oracle_mode = lib::is_oracle_mode();

  if (OB_ISNULL(bit_vector_cache_)) {
    void *mem = nullptr;
    if (OB_ISNULL(mem = malloc_alloc_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }
  struct Functor {
    Functor(ObCSVTableRowIterator *csv_iter,
            const ExprFixedArray &file_column_exprs,
            ObEvalCtx &eval_ctx,
            bool is_oracle_mode,
            int64_t &returned_row_cnt) :
            csv_iter_(csv_iter), file_column_exprs_(file_column_exprs), eval_ctx_(eval_ctx),
            is_oracle_mode_(is_oracle_mode), returned_row_cnt_(returned_row_cnt)
    {}
    ObCSVTableRowIterator *csv_iter_;
    const ExprFixedArray &file_column_exprs_;
    ObEvalCtx &eval_ctx_;
    bool is_oracle_mode_;
    int64_t &returned_row_cnt_;

    int operator()(ObIArray<ObCSVGeneralParser::FieldValue> &arr) {
      int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
      ObDatum *datums = file_column_exprs_.at(i)->locate_batch_datums(eval_ctx_);
      if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
        if (csv_iter_->data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
          datums[returned_row_cnt_].set_string(csv_iter_->state_.file_with_url_.ptr(), csv_iter_->state_.file_with_url_.length());
        } else {
          datums[returned_row_cnt_].set_string(csv_iter_->state_.cur_file_name_.ptr(), csv_iter_->state_.cur_file_name_.length());
        }
      } else if (file_column_exprs_.at(i)->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
        if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= csv_iter_->state_.part_list_val_.get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loc idx is out of range", K(loc_idx), K(csv_iter_->state_.part_list_val_), K(csv_iter_->state_.part_id_), K(ret));
        } else {
          if (csv_iter_->state_.part_list_val_.get_cell(loc_idx).is_null()) {
            datums[returned_row_cnt_].set_null();
          } else {
            CK (OB_NOT_NULL(datums[returned_row_cnt_].ptr_));
            OZ (datums[returned_row_cnt_].from_obj(csv_iter_->state_.part_list_val_.get_cell(loc_idx)));
          }
        }
      } else if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
        int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
        if (OB_UNLIKELY(loc_idx < 0 || loc_idx > arr.count())) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          if (arr.at(loc_idx).is_null_ || (0 == arr.at(loc_idx).len_ && is_oracle_mode_)) {
            datums[returned_row_cnt_].set_null();
          } else {
            datums[returned_row_cnt_].set_string(arr.at(loc_idx).ptr_, arr.at(loc_idx).len_);
          }
        }
      }
    }
    returned_row_cnt_++;
    return ret;
    }
  };
  struct Functor handle_one_line(this, file_column_exprs, eval_ctx, is_oracle_mode, returned_row_cnt);
  int64_t nrows = 0;
  do {
    if (state_.skip_lines_ > 0) {
      OZ (skip_lines());
    }
    if (OB_SUCC(ret)) {
      nrows = MIN(batch_size, state_.line_count_limit_);
      if (OB_UNLIKELY(0 == nrows)) {
        // if line_count_limit = 0, get next file.
        state_.is_end_file_ = true;
      } else {
        ret = parser_.scan<decltype(handle_one_line), true>(state_.pos_, state_.data_end_, nrows,
                                        state_.escape_buf_, state_.escape_buf_end_, handle_one_line,
                                        error_msgs, state_.is_end_file_);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to scan csv", K(ret));
        } else if (OB_UNLIKELY(error_msgs.count() > 0)) {
          dump_error_log(error_msgs);
        }
      }
    }
  } while (OB_SUCC(ret) && returned_row_cnt < 1 && OB_SUCC(load_next_buf()));

  if (OB_ITER_END == ret && returned_row_cnt > 0) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && returned_row_cnt > 0) {
    if (OB_NOT_NULL(file_id_expr_)) {
      ObDatum *datums = file_id_expr_->locate_batch_datums(eval_ctx);
      for (int64_t i = 0; i < returned_row_cnt; i++) {
        datums[i].set_int(state_.cur_file_id_);
      }
      file_id_expr_->set_evaluated_flag(eval_ctx);
    }
    if (OB_NOT_NULL(line_number_expr_)) {
      ObDatum *datums = line_number_expr_->locate_batch_datums(eval_ctx);
      for (int64_t i = 0; i < returned_row_cnt; i++) {
        datums[i].set_int(state_.cur_line_number_ + i);
      }
      line_number_expr_->set_evaluated_flag(eval_ctx);
    }
    state_.line_count_limit_ -= returned_row_cnt;
    state_.cur_line_number_ += returned_row_cnt;
  }

  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
    file_column_exprs.at(i)->set_evaluated_flag(eval_ctx);
  }

  for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
    ObExpr *column_expr = column_exprs_.at(i);
    ObExpr *column_convert_expr = scan_param_->ext_column_convert_exprs_->at(i);
    OZ (column_convert_expr->eval_batch(eval_ctx, *bit_vector_cache_, returned_row_cnt));
    if (OB_SUCC(ret)) {
      MEMCPY(column_expr->locate_batch_datums(eval_ctx),
            column_convert_expr->locate_batch_datums(eval_ctx), sizeof(ObDatum) * returned_row_cnt);
      column_expr->set_evaluated_flag(eval_ctx);
    }
  }

  count = returned_row_cnt;

  return ret;
}

void ObCSVTableRowIterator::reset()
{
  // reset state_ to initial values for rescan
  state_.reuse();
}




}
}
