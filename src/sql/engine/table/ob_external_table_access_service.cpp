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
#include "share/backup/ob_backup_io_adapter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_device_manager.h"
#include "sql/engine/table/ob_parquet_table_row_iter.h"
#ifdef OB_BUILD_CPP_ODPS
#include "sql/engine/table/ob_odps_table_row_iter.h"
#endif
#ifdef OB_BUILD_JNI_ODPS
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"
#endif
#include "sql/engine/cmd/ob_load_data_file_reader.h"
#include "sql/engine/table/ob_orc_table_row_iter.h"
#include "sql/engine/table/ob_csv_table_row_iter.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace common {
extern const char *OB_STORAGE_ACCESS_TYPES_STR[];
}

namespace share
{
struct ObPartitionIdRowPair;
class ObPartitionIdRowPairArray;
}
using namespace share::schema;
using namespace common;
using namespace share;
namespace sql
{

static constexpr uint64_t OB_STORAGE_ID_EXTERNAL = 2001;

ObExternalDataAccessDriver::~ObExternalDataAccessDriver() {
  close();
  if (OB_NOT_NULL(device_handle_)) {
    ObDeviceManager::get_instance().release_device(device_handle_);
  }
}

void ObExternalDataAccessDriver::close()
{
  if (OB_NOT_NULL(device_handle_) && fd_.is_valid()) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObBackupIoAdapter::close_device_and_fd(device_handle_, fd_))) {
      LOG_WARN("fail to close device and fd", KR(ret), K_(fd), KP_(device_handle));
    }
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
  file_size = -1;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_IMPORT);
  ObString url_cstring;
  ObArenaAllocator allocator;

  if (OB_FAIL(ob_write_string(allocator, url, url_cstring, true/*c_style*/))) {
    LOG_WARN("fail to copy string", KR(ret), K(url));
  } else if (OB_FAIL(ObBackupIoAdapter::get_file_length(url_cstring, &access_info_, file_size))) {
    LOG_WARN("fail to get file length", KR(ret), K(url_cstring), K_(access_info));
  }

  if (OB_OBJECT_NOT_EXIST == ret || OB_IO_ERROR == ret) {
    file_size = -1;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObExternalDataAccessDriver::open(const char *url)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Data Access Driver has been opened", KR(ret), K(url));
  } else if (OB_FAIL(ObBackupIoAdapter::open_with_access_type(
      device_handle_, fd_, &access_info_, url, OB_STORAGE_ACCESS_READER,
      ObStorageIdMod(OB_STORAGE_ID_EXTERNAL, ObStorageUsedMod::STORAGE_USED_EXTERNAL)))) {
    LOG_WARN("fail to open Data Access Driver", KR(ret), K_(access_info), K(url));
  }
  return ret;
}

int ObExternalDataAccessDriver::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObIOHandle io_handle;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);
  if (OB_FAIL(ObBackupIoAdapter::async_pread(*device_handle_, fd_,
      static_cast<char *>(buf), offset, count, io_handle))) {
    LOG_WARN("fail to async pread", KR(ret),
        KP_(device_handle), K_(fd), KP(buf), K(offset), K(count));
  } else if (OB_FAIL(io_handle.wait())) {
    LOG_WARN("fail to wait pread result", KR(ret),
        KP_(device_handle), K_(fd), KP(buf), K(offset), K(count));
  } else {
    read_size = io_handle.get_data_size();
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
        } else if (OB_FAIL(ob_write_string(allocator_, target, tmp_file, true/*c_style*/))) {
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
  ObString path_cstring;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);

  if (OB_UNLIKELY(!access_info_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalDataAccessDriver not init", KR(ret), K_(access_info));
  } else if (!pattern.empty() && OB_FAIL(filter.init(pattern, regexp_vars))) {
    LOG_WARN("fail to init filter", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, path, path_cstring, true/*c_style*/))) {
    LOG_WARN("fail to copy string", KR(ret), K(path));
  } else if (get_storage_type() == OB_STORAGE_FILE ||
             get_storage_type() == OB_STORAGE_HDFS) {
    ObSEArray<ObString, 4> file_dirs;
    bool is_dir = false;

    if (get_storage_type() == OB_STORAGE_FILE) {
      ObString path_without_prifix;
      path_without_prifix = path_cstring;
      path_without_prifix += strlen(OB_FILE_PREFIX);

      OZ(FileDirectoryUtils::is_directory(path_without_prifix.ptr(), is_dir));
      if (!is_dir) {
        LOG_INFO("external location is not a directory",
                 K(path_without_prifix));
      } else {
        OZ(file_dirs.push_back(path_cstring));
      }
    } else {
      // OB_STORAGE_HDFS
      OZ(ObBackupIoAdapter::is_directory(path_cstring, &access_info_, is_dir));
      if (!is_dir) {
        LOG_INFO("external location is not a directory", K(path_cstring));
      } else {
        OZ(file_dirs.push_back(path_cstring));
      }
    }

    ObArray<int64_t> useless_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_dirs.count(); i++) {
      ObString file_dir = file_dirs.at(i);
      ObLocalFileListArrayOpWithFilter dir_op(file_dirs, useless_size, file_dir, path_cstring, NULL, allocator);
      ObLocalFileListArrayOpWithFilter file_op(file_urls, file_sizes, file_dir, path_cstring,
                                               pattern.empty() ? NULL : &filter, allocator);
      dir_op.set_dir_flag();
      if (OB_FAIL(ObBackupIoAdapter::list_files(file_dir, &access_info_, file_op))) {
        LOG_WARN("fail to list files", KR(ret), K(file_dir), K_(access_info));
      } else if (OB_FAIL(ObBackupIoAdapter::list_directories(file_dir, &access_info_, dir_op))) {
        LOG_WARN("fail to list dirs", KR(ret), K(file_dir), K_(access_info));
      } else if (file_dirs.count() + file_urls.count() > MAX_VISIT_COUNT) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("too many files and dirs to visit", K(ret));
      }
    }
  } else {
    ObExternalFileListArrayOpWithFilter file_op(file_urls, file_sizes, pattern.empty() ? NULL : &filter, allocator);
    if (OB_FAIL(ObBackupIoAdapter::list_files(path_cstring, &access_info_, file_op))) {
      LOG_WARN("fail to list files", KR(ret), K(path_cstring), K_(access_info));
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

  return ret;
}

ObExternalStreamFileReader::~ObExternalStreamFileReader()
{
  reset();
}

const char *  ObExternalStreamFileReader::MEMORY_LABEL = "ExternalReader";
const int64_t ObExternalStreamFileReader::COMPRESSED_DATA_BUFFER_SIZE = 2 * 1024 * 1024;

int ObExternalStreamFileReader::init(const common::ObString &location,
                             const ObString &access_info,
                             ObCSVGeneralFormat::ObCSVCompression compression_format,
                             ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(allocator_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(data_access_driver_.init(location, access_info))) {
    LOG_WARN("failed to init data access driver", K(ret), K(location), K(access_info));
  } else {
    allocator_ = &allocator;
    compression_format_ = compression_format;
  }
  return ret;
}

int ObExternalStreamFileReader::open(const ObString &filename)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
  } else if (data_access_driver_.is_opened()) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(data_access_driver_.open(filename.ptr()))) {
    LOG_WARN("failed to open file", K(ret), K(filename));
  } else if (OB_FAIL(data_access_driver_.get_file_size(filename.ptr(), file_size_))) {
    LOG_WARN("failed to get file size", K(ret), K(filename));
  } else {
    is_file_end_ = false;

    ObCSVGeneralFormat::ObCSVCompression this_file_compression_format = compression_format_;
    if (this_file_compression_format == ObCSVGeneralFormat::ObCSVCompression::AUTO
        && OB_FAIL(compression_algorithm_from_suffix(filename, this_file_compression_format))) {
      LOG_WARN("failed to dectect compression format from filename", K(ret), K(filename));
    }

    if (OB_SUCC(ret) && OB_FAIL(create_decompressor(this_file_compression_format))) {
      LOG_WARN("failed to create decompressor", K(ret));
    }
  }

  LOG_TRACE("open file done", K(filename), K(ret));
  return ret;
}

void ObExternalStreamFileReader::close()
{
  if (data_access_driver_.is_opened()) {
    data_access_driver_.close();

    is_file_end_ = true;
    file_offset_ = 0;
    file_size_   = 0;
    LOG_DEBUG("close file");
  }
}

void ObExternalStreamFileReader::reset()
{
  close();
  if (OB_NOT_NULL(compressed_data_) && OB_NOT_NULL(allocator_)) {
    allocator_->free(compressed_data_);
    compressed_data_ = nullptr;
  }

  if (OB_NOT_NULL(decompressor_)) {
    ObDecompressor::destroy(decompressor_);
    decompressor_ = nullptr;
  }

  allocator_ = nullptr;
}

bool ObExternalStreamFileReader::eof()
{
  return is_file_end_;
}

int ObExternalStreamFileReader::read(char *buf, int64_t buf_len, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(decompressor_)) {
    ret = read_from_driver(buf, buf_len, read_size);
    is_file_end_ = file_offset_ >= file_size_;
    LOG_DEBUG("read file", K(is_file_end_), K(file_offset_), K(file_size_), K(read_size));
  } else {
    ret = read_decompress(buf, buf_len, read_size);
    is_file_end_ = (file_offset_ >= file_size_) && (consumed_data_size_ >= compress_data_size_);
  }
  return ret;
}

int ObExternalStreamFileReader::read_from_driver(char *buf, int64_t buf_len, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if(OB_FAIL(data_access_driver_.pread(buf, buf_len, file_offset_, read_size))) {
    LOG_WARN("failed to read data from data access driver", K(ret), K(file_offset_), K(buf_len));
  } else {
    file_offset_ += read_size;
  }
  return ret;
}

int ObExternalStreamFileReader::read_decompress(char *buf, int64_t buf_len, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (!data_access_driver_.is_opened()) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len));
  } else if (consumed_data_size_ >= compress_data_size_) {
    if (file_offset_ < file_size_) {
      ret = read_compressed_data();
    } else {
      is_file_end_ = true;
    }
  }

  if (OB_SUCC(ret) && compress_data_size_ > consumed_data_size_) {
    int64_t consumed_size = 0;
    ret = decompressor_->decompress(compressed_data_ + consumed_data_size_,
                                    compress_data_size_ - consumed_data_size_,
                                    consumed_size,
                                    buf,
                                    buf_len,
                                    read_size);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to decompress", K(ret));
    } else {
      consumed_data_size_ += consumed_size;
      uncompressed_size_  += read_size;
    }
  }
  return ret;
}

int ObExternalStreamFileReader::read_compressed_data()
{
  int ret = OB_SUCCESS;
  char *read_buffer = compressed_data_;
  if (!data_access_driver_.is_opened()) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(consumed_data_size_ < compress_data_size_)) {
    // backup data
    const int64_t last_data_size = compress_data_size_ - consumed_data_size_;
    MEMMOVE(compressed_data_, compressed_data_ + consumed_data_size_, last_data_size);
    read_buffer = compressed_data_ + last_data_size;
    consumed_data_size_ = 0;
    compress_data_size_ = last_data_size;
  } else if (consumed_data_size_ == compress_data_size_) {
    consumed_data_size_ = 0;
    compress_data_size_ = 0;
  }

  if (OB_SUCC(ret)) {
    // read data from source reader
    int64_t read_size = 0;
    int64_t capacity  = COMPRESSED_DATA_BUFFER_SIZE - compress_data_size_;
    ret = read_from_driver(read_buffer, capacity, read_size);
    if (OB_SUCC(ret)) {
      compress_data_size_ += read_size;
    }
  }
  return ret;
}

int ObExternalStreamFileReader::create_decompressor(ObCSVGeneralFormat::ObCSVCompression compression_format)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
  } else if (compression_format == ObCSVGeneralFormat::ObCSVCompression::NONE) {
    ObDecompressor::destroy(decompressor_);
    decompressor_ = nullptr;
  } else if (OB_NOT_NULL(decompressor_) && decompressor_->compression_format() == compression_format) {
    // do nothing
  } else {
    if (OB_NOT_NULL(decompressor_)) {
      ObDecompressor::destroy(decompressor_);
      decompressor_ = nullptr;
    }

    if (OB_FAIL(ObDecompressor::create(compression_format, *allocator_, decompressor_))) {
      LOG_WARN("failed to create decompressor", K(ret));
    } else if (OB_ISNULL(compressed_data_) &&
               OB_ISNULL(compressed_data_ = (char *)allocator_->alloc(COMPRESSED_DATA_BUFFER_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(COMPRESSED_DATA_BUFFER_SIZE));
    }
  }
  return ret;
}

int ObExternalTableAccessService::table_scan(
    ObVTableScanParam &param,
    ObNewRowIterator *&result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  const share::ObLSID &ls_id = param.ls_id_;
  const common::ObTabletID &data_tablet_id = param.tablet_id_;
  GET_DIAGNOSTIC_INFO->get_ash_stat().tablet_id_ = data_tablet_id.id();
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(ls_id_, ls_id.id());
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
    case ObExternalFileFormat::ODPS_FORMAT:
      if (!GCONF._use_odps_jni_connector) {
#if defined(OB_BUILD_CPP_ODPS)
        if (OB_ISNULL(row_iter = OB_NEWx(ObODPSTableRowIterator,
                                         (scan_param.allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        }
#else
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("odps cpp connector is not enabled", K(ret));
#endif
      } else {
#if defined(OB_BUILD_JNI_ODPS)
        if (OB_ISNULL(row_iter = OB_NEWx(ObODPSJNITableRowIterator,
                                         (scan_param.allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed for jni row iterator", K(ret));
        }
#else
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("odps jni connector is not enabled", K(ret));
#endif
      }
      break;
    case ObExternalFileFormat::ORC_FORMAT:
      if (OB_ISNULL(row_iter = OB_NEWx(ObOrcTableRowIterator, (scan_param.allocator_)))) {
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
  const share::ObLSID &ls_id = param.ls_id_;
  const common::ObTabletID &data_tablet_id = param.tablet_id_;
  GET_DIAGNOSTIC_INFO->get_ash_stat().tablet_id_ = data_tablet_id.id();
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(ls_id_, ls_id.id());
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter", K(ret));
  } else {
    switch (param.external_file_format_.format_type_) {
      case ObExternalFileFormat::CSV_FORMAT:
      case ObExternalFileFormat::PARQUET_FORMAT:
      case ObExternalFileFormat::ORC_FORMAT:
        result->reset();
        break;
      case ObExternalFileFormat::ODPS_FORMAT:
#if defined (OB_BUILD_CPP_ODPS) || defined (OB_BUILD_JNI_ODPS)
        result->reset();
#else
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
        LOG_WARN("not support to read odps in opensource", K(ret));
#endif
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
  UNUSED(switch_param);
  iter->reset();
  return OB_SUCCESS;
}

int ObExternalTableAccessService::revert_scan_iter(ObNewRowIterator *iter)
{
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
  ObExternalFileFormat::FormatType external_table_type;
  bool is_odps_external_table = false;
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
  } else if (OB_FAIL(ObSQLUtils::is_odps_external_table(table_schema, is_odps_external_table))) {
    LOG_WARN("failed to check is odps external table or not", K(ret));
  } else if (table_schema->is_partitioned_table() && (table_schema->is_user_specified_partition_for_external_table() || is_odps_external_table)) {
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
int ObExternalTableRowIterator::calc_file_part_list_value_by_array(const int64_t part_id,
                                                                  ObIAllocator &allocator,
                                                                  const share::ObPartitionIdRowPairArray *partition_array,
                                                                  ObNewRow &value)
{
  int ret = OB_SUCCESS;
  int64_t partition_index = OB_INVALID_INDEX;
  share::ObPartitionIdRowPair partition;

  int64_t partition_num = partition_array->count();
  if (OB_ISNULL(partition_array) || partition_num <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition array", K(ret), K(part_id), K(partition_num));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
    if (part_id == partition_array->at(i).part_id_) {
      partition_index = i;
      break;
    }
  }

  if (OB_SUCC(ret) && partition_index != OB_INVALID_INDEX) {
    partition = partition_array->at(partition_index);
  }

  if (OB_SUCC(ret)) {
    if (partition_index == OB_INVALID_INDEX || partition.part_id_ != part_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition", K(ret), K(partition), K(part_id));
    } else {
      int64_t pos = 0;
      int64_t size = partition.list_row_value_.get_deep_copy_size();
      char *buf = (char *)allocator.alloc(size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate mem failed", K(ret));
      }
      OZ (value.deep_copy(partition.list_row_value_, buf, size, pos));
    }
  }
  return ret;
}

int ObExternalTableRowIterator::calc_exprs_for_rowid(const int64_t read_count, ObExternalIteratorState &state)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_NOT_NULL(file_id_expr_)) {
    OZ (file_id_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_id_expr_->get_vector(eval_ctx));
      vec->set_int(i, state.cur_file_id_);
    }
    file_id_expr_->set_evaluated_flag(eval_ctx);
  }
  if (OB_NOT_NULL(line_number_expr_)) {
    OZ (line_number_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(line_number_expr_->get_vector(eval_ctx));
      vec->set_int(i, state.cur_line_number_ + i);
    }
    line_number_expr_->set_evaluated_flag(eval_ctx);
  }
  state.cur_line_number_ += read_count;
  return ret;
}

DEF_TO_STRING(ObExternalIteratorState)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(file_idx),
       K_(part_id),
       K_(cur_file_id),
       K_(cur_line_number),
       K_(cur_file_url),
       K_(part_list_val));
  J_OBJ_END();
  return pos;
}


}
}


