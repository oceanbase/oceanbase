/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef OB_EXTERNAL_FILE_ACCESS_H_
#define OB_EXTERNAL_FILE_ACCESS_H_


#include "lib/file/ob_file.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/basic/ob_lake_table_reader_profile.h"

// #include "ob_external_data_page_cache.h"

namespace oceanbase
{

namespace sql {

struct ObExternalReadInfo final
{
  ObExternalReadInfo() { reset(); }
  ObExternalReadInfo(const int64_t offset, void *buffer, const int64_t buf_size,
                     const int64_t time_out_ms) :
    offset_(offset),
    buffer_(buffer), size_(buf_size), io_timeout_ms_(MAX(0, time_out_ms)), io_desc_(),
    io_callback_(nullptr), io_metrics_(nullptr)
  {}
  ObExternalReadInfo(const int64_t offset, void *buffer, const int64_t buf_size,
                     const int64_t time_out_ms, const ObIOFlag &io_desc) :
    offset_(offset),
    buffer_(buffer), size_(buf_size), io_timeout_ms_(MAX(0, time_out_ms)), io_desc_(io_desc),
    io_callback_(nullptr), io_metrics_(nullptr)
  {}
  ~ObExternalReadInfo()
  {
    reset();
  }
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(io_desc), K_(io_timeout_ms), K_(offset), K_(size), KP_(buffer), KP_(io_callback));
  // assignment by sql
  int64_t offset_;
  void* buffer_;
  int64_t size_;
  int64_t io_timeout_ms_;
  // assignment by DAM
  ObIOFlag io_desc_;
  common::ObIOCallback *io_callback_;
  ObLakeTableIOMetrics *io_metrics_;
};

struct ObExternalFileUrlInfo
{
  friend class ObExternalFileAccess;
  ObExternalFileUrlInfo(const ObString &location, const ObString &access_info, const ObString &url,
                        const ObString &content_digest, const int64_t file_size,
                        const int64_t modify_time) :
    location_(location),
    access_info_(access_info), url_(url), content_digest_(content_digest), file_size_(file_size),
    modify_time_(modify_time)
  {}
  ObExternalFileUrlInfo(const ObString &location, const ObString &access_info,
                        const ObString &url) :
    location_(location),
    access_info_(access_info), url_(url), content_digest_(), file_size_(0), modify_time_(0)
  {}
  TO_STRING_KV(K_(location), K_(access_info), K_(url), K_(content_digest), K_(file_size),
               K_(modify_time));
  int64_t get_file_size() const { return file_size_; }
private:
  const ObString location_;
  const ObString access_info_;
  const ObString url_;
  const ObString content_digest_;
  const int64_t file_size_;
  const int64_t modify_time_;
};

struct ObExternalFileCacheOptions
{
  ObExternalFileCacheOptions():enable_page_cache_(false), enable_disk_cache_(false) {}
  ObExternalFileCacheOptions(const bool enable_page_cache, const bool enable_disk_cache) :
    enable_page_cache_(enable_page_cache), enable_disk_cache_(enable_disk_cache)
  {}
  ~ObExternalFileCacheOptions() { reset(); }
  void set_enable_page_cache() { enable_page_cache_ = true; }
  void set_enable_disk_cache() { enable_disk_cache_ = true; }
  bool enable_page_cache() const { return enable_page_cache_; }
  bool enable_disk_cache() const { return enable_disk_cache_; }
  void reset() { enable_disk_cache_ = false; enable_page_cache_ = false;  }
  TO_STRING_KV(K_(enable_page_cache), K_(enable_disk_cache));
private:
  // If enable_page_cache_ == False :
  //    disable put to and get from page cache
  bool enable_page_cache_;
  // If enable_disk_cache_ == False :
  //   disable put to and get from disk cache
  bool enable_disk_cache_;
};

class ObExternalFileReadHandle final
{
  friend class ObExternalDataAccessMgr;
public:
  ObExternalFileReadHandle()
    : object_handles_(), metrics_(nullptr)
  {}
  virtual ~ObExternalFileReadHandle() = default;
  int wait();
  void reset();
  bool is_valid() const;
  int get_user_buf_read_data_size(int64_t &read_size) const;
  void set_io_metrics(ObLakeTableIOMetrics &metrics);
  TO_STRING_KV(K(object_handles_.count()), K_(object_handles), K_(expect_read_size), K_(cache_hit_size));
private:
  int add_object_handle(
    const blocksstable::ObStorageObjectHandle &object_handle,
    const int64_t user_rd_length);
  int get_cache_read_data_size(int64_t &read_size) const;
protected:
  ObSEArray<blocksstable::ObStorageObjectHandle, 1> object_handles_;
  ObSEArray<int64_t, 1> expect_read_size_;
  int64_t cache_hit_size_;
  ObLakeTableIOMetrics *metrics_;
  DISALLOW_COPY_AND_ASSIGN(ObExternalFileReadHandle);
};

class ObExternalFileAccess final
{
public:
  ObExternalFileAccess() :
    cache_options_(), storage_type_(OB_STORAGE_MAX_TYPE), fd_(), file_size_(0)
  {}
  ~ObExternalFileAccess();
  int reset();
  int open(
      const ObExternalFileUrlInfo &info,
      const ObExternalFileCacheOptions &cache_options);
  int close();
  int async_read(
      const ObExternalReadInfo &info,
      ObExternalFileReadHandle &handle);
  int pread(
      const ObExternalReadInfo &info,
      int64_t &read_size);
  int get_file_size(int64_t &file_size);
  common::ObStorageType get_storage_type() { return storage_type_; };
  bool is_opened() const;
  int register_io_metrics(ObLakeTableReaderProfile &reader_profile, const ObString &label);
  TO_STRING_KV(K_(cache_options), K_(fd));
private:
  int inner_process_read_info_(
      const ObExternalReadInfo &input_info,
      ObExternalReadInfo &out_info) const;
private:
  const char DUMMY_EMPTY_CHAR_ = '\0';
  ObExternalFileCacheOptions cache_options_;
  common::ObStorageType storage_type_;
  ObIOFd fd_;
  int64_t file_size_;
  ObLakeTableIOMetrics metrics_;
};

} // namespace sql
} // namespace oceanbase
#endif // OB_EXTERNAL_FILE_ACCESS_H_
