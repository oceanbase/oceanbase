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
#include "sql/engine/table/ob_dummy_table_row_iter.h"
#include "sql/engine/table/ob_orc_table_row_iter.h"
#include "sql/engine/table/ob_csv_table_row_iter.h"
#include "plugin/external_table/ob_plugin_external_table_row_iter.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/table/ob_iceberg_delete_bitmap_builder.h"
namespace oceanbase
{
namespace common {
extern const char *OB_STORAGE_ACCESS_TYPES_STR[];
}

namespace share
{
struct ObExternalTablePartInfo;
class ObExternalTablePartInfoArray;
}
using namespace share::schema;
using namespace common;
using namespace share;
using namespace plugin;

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
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObExternalIoAdapter::close_device_and_fd(device_handle_, fd_))) {
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
  } else if (OB_FAIL(ObExternalIoAdapter::get_file_length(url_cstring, access_info_, file_size))) {
    LOG_WARN("fail to get file length", KR(ret), K(url_cstring), K_(access_info));
  }

  if (OB_OBJECT_NOT_EXIST == ret || OB_HDFS_PATH_NOT_FOUND == ret || OB_IO_ERROR == ret) {
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
  } else if (OB_FAIL(ObExternalIoAdapter::open_with_access_type(
      device_handle_, fd_, access_info_, url, OB_STORAGE_ACCESS_READER,
      ObStorageIdMod::get_default_external_id_mod()))) {
    LOG_WARN("fail to open Data Access Driver", KR(ret), K_(access_info), K(url));
  }
  return ret;
}

int ObExternalDataAccessDriver::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObIOHandle io_handle;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);
  if (OB_FAIL(ObExternalIoAdapter::async_pread(*device_handle_, fd_,
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

int ObExternalDataAccessDriver::get_directory_list(const common::ObString &path,
                                                   common::ObIArray<common::ObString> &file_urls,
                                                   common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString path_cstring;
  ObArray<int64_t> useless_size;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);

  if (OB_UNLIKELY(!access_info_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalDataAccessDriver not init", KR(ret), K_(access_info));
  } else if (OB_FAIL(ob_write_string(allocator, path, path_cstring, true /*c_style*/))) {
    LOG_WARN("fail to copy string", KR(ret), K(path));
  } else if (get_storage_type() == OB_STORAGE_FILE || get_storage_type() == OB_STORAGE_HDFS) {
    ObString file_dir;
    bool is_dir = false;

    if (get_storage_type() == OB_STORAGE_FILE) {
      ObString path_without_prefix;
      path_without_prefix = path_cstring;
      path_without_prefix += strlen(OB_FILE_PREFIX);

      OZ(FileDirectoryUtils::is_directory(path_without_prefix.ptr(), is_dir));
      if (!is_dir) {
        LOG_INFO("external location is not a directory", K(path_without_prefix));
      } else {
        file_dir = path_cstring;
      }
    } else {
      // OB_STORAGE_HDFS
      OZ(ObExternalIoAdapter::is_directory(path_cstring, access_info_, is_dir));
      if (!is_dir) {
        LOG_INFO("external location is not a directory", K(path_cstring));
      } else {
        file_dir = path_cstring;
      }
    }

    if (OB_SUCC(ret)) {
      ObFileListArrayOp dir_op(file_urls, allocator);
      dir_op.set_dir_flag();
      if (OB_FAIL(ObExternalIoAdapter::list_directories(file_dir, access_info_, dir_op))) {
        LOG_WARN("fail to list dirs", KR(ret), K(file_dir), K_(access_info));
      }
    }
  } else {
    ObSEArray<ObString,4> content_digests;
    ObSEArray<int64_t,4> modify_times;
    ObExternalFileListArrayOpWithFilter dir_op(file_urls, useless_size, modify_times, content_digests, NULL, allocator);
    dir_op.set_dir_flag();
    if (OB_FAIL(ObExternalIoAdapter::list_files(path_cstring, access_info_, dir_op))) {
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
  ObExternalIoAdapter util;

  if (OB_FAIL(get_storage_type_from_path_for_external_table(location, device_type))) {
    LOG_WARN("fail to resove storage type", K(ret));
  } else {
    storage_type_ = device_type;
    // Note: if device type is file, the storage info is empty.
    // And if device type is hdfs, the storage info `may be` empty.
    if (device_type == OB_STORAGE_FILE ||
        (device_type == OB_STORAGE_HDFS &&
         (OB_ISNULL(access_info) || OB_LIKELY(0 == access_info.length())))) {
      OZ(ob_write_string(temp_allocator, location, location_cstr, true));
      access_info_cstr.assign_ptr(&dummy_empty_char, strlen(&dummy_empty_char));
    } else {
      OZ (ob_write_string(temp_allocator, location, location_cstr, true));
      OZ (ob_write_string(temp_allocator, access_info, access_info_cstr, true));
    }
  }
  if (device_type == OB_STORAGE_HDFS) {
    access_info_ = &hdfs_storage_info_;
  } else {
    access_info_ = &backup_storage_info_;
  }
  if (OB_ISNULL(access_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get access into", K(ret), K(device_type), K(access_info_cstr));
  }
  LOG_TRACE("resolve storage into", K(ret), K(device_type), K(access_info_cstr));
  OZ (access_info_->set(device_type, access_info_cstr.ptr()));

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

int64_t ObExternalStreamFileReader::get_file_size() const
{
  return file_size_;
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
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(param.tablet_id_.id());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(ls_id_, ls_id.id());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(tablet_id_, param.tablet_id_.id());
  int ret = OB_SUCCESS;
  ObExternalTableRowIterator* row_iter = NULL;

  auto &scan_param = static_cast<storage::ObTableScanParam&>(param);
  if (scan_param.key_ranges_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else {
    const ObObj *obj_ptr = scan_param.key_ranges_.at(0).get_start_key().get_obj_ptr();
    ObString file_path = obj_ptr[ObExternalTableUtils::FILE_URL].get_string();
    // ODPS外表会在每个SQC中加一个DUMMY_FILE，因此ODPS_FORMAT遇到DUMMY_FILE依然要用ObODPSTableRowIterator处理
    if (param.external_file_format_.format_type_ != ObExternalFileFormat::ODPS_FORMAT &&
        file_path.compare_equal(ObExternalTableUtils::dummy_file_name())) {
      if (OB_ISNULL(row_iter = OB_NEWx(ObDummyTableRowIterator, (scan_param.allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      }
    } else {
      ObExternalFileFormat::FormatType format_type = param.external_file_format_.format_type_;
      if (param.lake_table_format_ == ObLakeTableFormat::ICEBERG) {
        if (scan_param.key_ranges_.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        } else {
          const ObObj *obj_ptr = scan_param.key_ranges_.at(0).get_start_key().get_obj_ptr();
          int64_t file_format_int = obj_ptr[ObExternalTableUtils::DATA_FILE_FORMAT].get_int();
          format_type = static_cast<ObExternalFileFormat::FormatType>(file_format_int);
        }
      }

      switch (format_type) {
        case ObExternalFileFormat::CSV_FORMAT:
          if (OB_ISNULL(row_iter = OB_NEWx(ObCSVTableRowIterator, (scan_param.allocator_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret));
          }
          break;
        case ObExternalFileFormat::PARQUET_FORMAT :
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
        case ObExternalFileFormat::PLUGIN_FORMAT:
          if (OB_ISNULL(row_iter = OB_NEWx(ObPluginExternalTableRowIterator, (scan_param.allocator_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret), K(sizeof(ObPluginExternalTableRowIterator)));
          } else {
            LOG_TRACE("success to create plugin row iterator");
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected format", K(ret), "format", param.external_file_format_.format_type_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter->init(&scan_param))) {
      row_iter->~ObExternalTableRowIterator();
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
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(param.tablet_id_.id());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(ls_id_, ls_id.id());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(tablet_id_, param.tablet_id_.id());
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
      case ObExternalFileFormat::PLUGIN_FORMAT: {
        ObPluginExternalTableRowIterator *iter = static_cast<ObPluginExternalTableRowIterator *>(result);
        iter->reset();
        if (OB_FAIL(iter->rescan(static_cast<ObTableScanParam *>(&param)))) {
          LOG_WARN("failed to do rescan by plugin row iterator", K(ret));
        }
        break;
      }
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
  if (OB_ISNULL(scan_param) || OB_ISNULL(scan_param->table_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else {
    if (scan_param->column_ids_.count() != scan_param->output_exprs_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column ids not equal to access expr", K(ret));
    }
    const ObIArray<bool> &output_sel_mask = scan_param->table_param_->get_output_sel_mask();
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
          OZ (column_sel_mask_.push_back(output_sel_mask.at(i)));
          break;
      }
    }
    if (OB_SUCC(ret) && column_exprs_.count() != scan_param->ext_column_dependent_exprs_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr not equal to dependent expr", K(ret),
               K(column_exprs_), KPC(scan_param->ext_column_dependent_exprs_));
    }

    if (OB_SUCC(ret)) {
      ObArray<ObExpr*> file_column_exprs;
      ObArray<std::pair<uint64_t, uint64_t>> mapping_column_ids;
      ObArray<ObExpr*> file_meta_column_exprs;
      for (int i = 0; OB_SUCC(ret) && i < scan_param->ext_file_column_exprs_->count(); i++) {
        ObExpr* ext_file_column_expr = scan_param->ext_file_column_exprs_->at(i);
        if (OB_ISNULL(ext_file_column_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ptr", K(ret));
        } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL
                  || ext_file_column_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
          OZ (file_meta_column_exprs.push_back(ext_file_column_expr));
        } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
          OZ (file_column_exprs.push_back(ext_file_column_expr));
          OZ (generate_mapping_column_id(ext_file_column_expr, i, mapping_column_ids));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr", KPC(ext_file_column_expr));
        }
      }
      OZ (file_column_exprs_.assign(file_column_exprs));
      OZ (mapping_column_ids_.assign(mapping_column_ids));
      OZ (check_can_skip_conv());
      OZ (file_meta_column_exprs_.assign(file_meta_column_exprs));
    }
  }
  return ret;
}

int ObExternalTableRowIterator::generate_mapping_column_id(
  ObExpr* ext_file_column_expr,
  int64_t file_column_expr_idx,
  ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids)
{
  int ret = OB_SUCCESS;

  int64_t mapped_column_id = OB_INVALID_ID;

  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_1_0) {
    bool mapping_generated = !scan_param_->ext_mapping_column_exprs_->empty()
                              && !scan_param_->ext_mapping_column_ids_->empty();
    if (mapping_generated) {
      mapped_column_id = scan_param_->ext_mapping_column_ids_->at(file_column_expr_idx);
    }
  } else {
    ObDataAccessPathExtraInfo *data_access_info =
      static_cast<ObDataAccessPathExtraInfo *>(ext_file_column_expr->extra_info_);
    mapped_column_id = data_access_info->mapped_column_id_;
  }

  if (OB_SUCC(ret)) {
    if (mapped_column_id >= 0) {
      int index = 0;
      bool found = false;
      for (int i = 0; OB_SUCC(ret) && i < scan_param_->column_ids_.count() && !found; i++) {
        uint64_t column_id = scan_param_->column_ids_.at(i);
        if (OB_HIDDEN_LINE_NUMBER_COLUMN_ID != column_id && OB_HIDDEN_FILE_ID_COLUMN_ID != column_id) {
          if (column_id == mapped_column_id) {
            found = true;
            OZ (mapping_column_ids.push_back(std::make_pair(column_id, index)));
          }
          index++;
        }
      }
      if (!found) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mapped column id not found", K(ret), K(mapped_column_id));
      }
    } else {
      OZ (mapping_column_ids.push_back(std::make_pair(OB_INVALID_ID, OB_INVALID_ID)));
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

ObExternalTableRowIterator::~ObExternalTableRowIterator()
{
  if (nullptr != scan_param_ && nullptr != scan_param_->pd_storage_filters_) {
    scan_param_->pd_storage_filters_->clear();
  }
  if (OB_NOT_NULL(delete_bitmap_)) {
    delete_bitmap_->set_empty();
  }
  if (OB_NOT_NULL(delete_bitmap_builder_)) {
    delete_bitmap_builder_->~ObIcebergDeleteBitmapBuilder();
    delete_bitmap_builder_ = nullptr;
  }
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
int ObExternalTableRowIterator::calc_file_part_list_value_by_array(
                                  const int64_t part_id, ObIAllocator &allocator,
                                  const share::ObExternalTablePartInfoArray *partition_array, ObNewRow &value)
{
  int ret = OB_SUCCESS;
  int64_t partition_index = OB_INVALID_INDEX;
  share::ObExternalTablePartInfo partition;

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

int ObExternalTableRowIterator::calc_exprs_for_rowid(const int64_t read_count,
                                                     ObExternalIteratorState &state,
                                                     const bool update_state)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_NOT_NULL(file_id_expr_)) {
    if (scan_param_->op_->enable_rich_format_) {
      OZ (file_id_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
      for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
        ObFixedLengthBase* vec = static_cast<ObFixedLengthBase*>(file_id_expr_->get_vector(eval_ctx));
        vec->set_int(i, state.cur_file_id_);
      }
    } else {
      ObDatum* datums = file_id_expr_->locate_batch_datums(eval_ctx);
      for (int64_t i = 0; i < read_count; i++) {
        datums[i].set_int(state.cur_file_id_);
      }
    }
    OX (file_id_expr_->set_evaluated_flag(eval_ctx));
  }
  if (OB_NOT_NULL(line_number_expr_)) {
    if (scan_param_->op_->enable_rich_format_) {
      OZ (line_number_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
      for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
        ObFixedLengthBase* vec = static_cast<ObFixedLengthBase*>(line_number_expr_->get_vector(eval_ctx));
        vec->set_int(i, state.cur_line_number_ + i);
      }
    } else {
      ObDatum* datums = line_number_expr_->locate_batch_datums(eval_ctx);
      for (int64_t i = 0; i < read_count; i++) {
        datums[i].set_int(state.cur_line_number_ + i);
      }
    }
    OX (line_number_expr_->set_evaluated_flag(eval_ctx));
  }
  if (update_state) {
    state.cur_line_number_ += read_count;
    state.batch_first_row_line_num_ = state.cur_line_number_ - read_count;
  }
  return ret;
}

bool ObExternalTableRowIterator::is_dummy_file(const ObString &file_url)
{
  return (0 == file_url.compare(ObExternalTableUtils::dummy_file_name()));
}

int ObExternalTableRowIterator::build_delete_bitmap(const ObString &data_file_path,
                                                    const int64_t task_idx) {
  return delete_bitmap_builder_->build_delete_bitmap(data_file_path, task_idx, delete_bitmap_);
}

int ObExternalTableRowIterator::init_default_batch(ExprFixedArray &file_column_exprs)
{
  int ret = OB_SUCCESS;

  int64_t max_batch_size = scan_param_->op_->get_eval_ctx().max_batch_size_;
  ObBitVector *default_nulls = NULL;

  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); ++i) {
    ObExpr* expr = file_column_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret), K(i));
    } else {
      ObColumnDefaultValue col_def;
      OZ (colid_default_value_arr_.push_back(col_def));

      if (OB_SUCC(ret)) {
        ObObj default_value;
        // TODO: 目前还没有完成iceberg default value解析的方案，所以所有的default value都设置为null
        default_value.set_null();
        // 根据不同的数据类型构造批量数组
        // TODO: 完善iceberg所有的default value类型支持
        ObColumnDefaultValue &array_col_def = colid_default_value_arr_.at(i);
        if (default_value.is_null()) {
          array_col_def.is_null_ = true;
          if (OB_ISNULL(default_nulls)) {
            const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
            if (OB_ISNULL(default_nulls = to_bit_vector(allocator_.alloc(nulls_size)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
            } else {
              default_nulls->set_all(max_batch_size);
            }
          }
          array_col_def.batch_data_or_nulls_ = default_nulls;
        } else if (default_value.is_int() || default_value.is_uint64() || default_value.is_double()) {
          array_col_def.batch_data_or_nulls_ = allocator_.alloc(sizeof(int64_t) * max_batch_size);
          if (OB_ISNULL(array_col_def.batch_data_or_nulls_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc batch array for integer", K(ret));
          } else {
            if (default_value.is_int()) {
              const int64_t val = default_value.get_int();
              int64_t *batch_array = static_cast<int64_t*>(array_col_def.batch_data_or_nulls_);
              for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
                batch_array[j] = val;
              }
            } else if (default_value.is_uint64()) {
              const uint64_t val = default_value.get_uint64();
              uint64_t *batch_array = static_cast<uint64_t*>(array_col_def.batch_data_or_nulls_);
              for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
                batch_array[j] = val;
              }
            } else {
              const double val = default_value.get_double();
              double *batch_array = static_cast<double*>(array_col_def.batch_data_or_nulls_);
              for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
                batch_array[j] = val;
              }
            }
          }
        } else if (default_value.is_float()) {
          const float val = default_value.get_float();
          array_col_def.batch_data_or_nulls_
              = static_cast<float *>(allocator_.alloc(sizeof(float) * max_batch_size));
          if (OB_ISNULL(array_col_def.batch_data_or_nulls_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc batch array for float", K(ret));
          } else {
            float *batch_array = static_cast<float*>(array_col_def.batch_data_or_nulls_);
            for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
              batch_array[j] = val;
            }
          }
        } else if (default_value.is_varchar() || default_value.is_char()
                  || default_value.is_varbinary() || default_value.is_binary()) {
          const ObString str = default_value.get_string();
          array_col_def.batch_data_or_nulls_
              = static_cast<char **>(allocator_.alloc(sizeof(char *) * max_batch_size));
          array_col_def.batch_len_
              = static_cast<int32_t *>(allocator_.alloc(sizeof(int32_t) * max_batch_size));
          if (OB_ISNULL(array_col_def.batch_data_or_nulls_)
              || OB_ISNULL(array_col_def.batch_len_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc batch array for string", K(ret));
          } else {
            char **batch_array = static_cast<char**>(array_col_def.batch_data_or_nulls_);
            int32_t *length_array = static_cast<int32_t*>(array_col_def.batch_len_);
            for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
              batch_array[j] = const_cast<char*>(str.ptr());
              length_array[j] = static_cast<int32_t>(str.length());
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported default value type", K(default_value.get_type()));
        }
      }
    }
  }
  return ret;
}

int ObExternalTableRowIterator::init_for_iceberg(ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(delete_bitmap_ = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc bitmap", K(ret), K(sizeof(ObRoaringBitmap)));
  } else if (OB_ISNULL(delete_bitmap_builder_
                       = OB_NEWx(ObIcebergDeleteBitmapBuilder, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc bitmap builder", K(ret), K(sizeof(ObIcebergDeleteBitmapBuilder)));
  } else if (OB_FAIL(delete_bitmap_builder_->init(scan_param_, options))) {
    LOG_WARN("failed to init bitmap builder", K(ret));
  }

  return ret;
}

int ObExternalTableRowIterator::set_default_batch(const ObDatumMeta &datum_type,
                                                  const ObColumnDefaultValue &col_def,
                                                  ObIVector *vec)
{
  int ret = OB_SUCCESS;
  switch (vec->get_format()) {
    case VEC_FIXED: {
      ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vec);
      if (col_def.is_null_) {
        fixed_vec->set_nulls(to_bit_vector(col_def.batch_data_or_nulls_));
        fixed_vec->set_has_null();
      } else if (ObIntType == datum_type.type_
                || ObUInt64Type == datum_type.type_ || ObDoubleType == datum_type.type_) {
        fixed_vec->from(sizeof(int64_t), reinterpret_cast<char*>(col_def.batch_data_or_nulls_));
      } else if (ObFloatType == datum_type.type_) {
        fixed_vec->from(sizeof(float), reinterpret_cast<char*>(col_def.batch_data_or_nulls_));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported default value type", K(datum_type.type_));
      }
      break;
    }
    case VEC_DISCRETE: {
      StrDiscVec *str_vec = static_cast<StrDiscVec *>(vec);
      if (col_def.is_null_) {
        str_vec->set_nulls(to_bit_vector(col_def.batch_data_or_nulls_));
        str_vec->set_has_null();
      } else if (ob_is_string_type(datum_type.type_) || ObRawType == datum_type.type_) {
        str_vec->set_ptrs(reinterpret_cast<char**>(col_def.batch_data_or_nulls_));
        str_vec->set_lens(col_def.batch_len_);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported default value type", K(datum_type.type_));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported vector format for default value", K(vec->get_format()));
      break;
    }
  }
  return ret;
}

int ObExternalTableRowIterator::check_can_skip_conv()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(column_need_conv_.prepare_allocate(column_exprs_.count()))) {
    LOG_WARN("failed to prepare allocate for column_need_conv_", K(ret));
  } else {
    MEMSET(column_need_conv_.get_data(), 1, column_exprs_.count() * sizeof(bool));
  }

  const ExprFixedArray &column_dependent_exprs = *(scan_param_->ext_column_dependent_exprs_);

  for (int i = 0; OB_SUCC(ret) && i < mapping_column_ids_.count(); i++) {
    uint64_t column_id = mapping_column_ids_.at(i).first;
    uint64_t column_expr_index = mapping_column_ids_.at(i).second;
    if (column_id != OB_INVALID_ID && column_expr_index != OB_INVALID_ID &&
        column_expr_index < column_dependent_exprs.count() &&
        column_dependent_exprs.at(column_expr_index)->type_ != T_FUN_COLUMN_CONV) {
      column_need_conv_.at(column_expr_index) = false;
    }
  }

  return ret;
}

ObExpr* ObExternalTableRowIterator::get_column_expr_by_id(int64_t file_column_expr_idx)
{
  uint64_t column_expr_index = mapping_column_ids_.at(file_column_expr_idx).second;
  ObExpr *res_expr = nullptr;
  if (column_expr_index != OB_INVALID_ID && column_expr_index < column_exprs_.count() &&
      !column_need_conv_.at(column_expr_index)) {
    res_expr = column_exprs_.at(column_expr_index);
  } else {
    res_expr = file_column_exprs_.at(file_column_expr_idx);
  }
  return res_expr;
}

int ObColumnDefaultValue::assign(const ObColumnDefaultValue &other)
{
  int ret = OB_SUCCESS;
  this->is_null_ = other.is_null_;
  this->batch_data_or_nulls_ = other.batch_data_or_nulls_;
  this->batch_len_ = other.batch_len_;
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

const std::string ObExternalTableRowIterator::ICEBERG_ID_KEY = "iceberg.id";

}
}


