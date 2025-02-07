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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_S3_BASE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_S3_BASE_H_

#include <openssl/md5.h>
#include <malloc.h>
#include "lib/restore/ob_i_storage.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include <algorithm>
#include <iostream>
#include "lib/utility/ob_tracepoint.h"

#pragma push_macro("private")
#undef private
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectTaggingRequest.h>
#include <aws/s3/model/GetObjectTaggingRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/ListPartsRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/ListMultipartUploadsRequest.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/crypto/CRC32.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#pragma pop_macro("private")

namespace oceanbase
{
namespace common
{

// Before using s3, you need to initialize s3 enviroment.
// Thread safe guaranteed by user.
int init_s3_env();

// You need to clean s3 resource when not use cos any more.
// Thread safe guaranteed by user.
void fin_s3_env();

static constexpr int64_t S3_CONNECT_TIMEOUT_MS = 10 * 1000;
static constexpr int64_t S3_REQUEST_TIMEOUT_MS = 10 * 1000;
static constexpr int64_t MAX_S3_CONNECTIONS_PER_CLIENT = 128;
static constexpr int64_t STOP_S3_TIMEOUT_US = 10 * 1000L;   // 10ms

// TODO: check length
static constexpr int MAX_S3_REGION_LENGTH = 128;
static constexpr int MAX_S3_ENDPOINT_LENGTH = 128;
static constexpr int MAX_S3_ACCESS_ID_LENGTH = 128;   // ak, access key id
static constexpr int MAX_S3_SECRET_KEY_LENGTH = 128;  // sk, secret key
static constexpr int MAX_S3_CLIENT_NUM = 97;
static constexpr int MAX_S3_PART_NUM = 10000;
static constexpr int64_t S3_MULTIPART_UPLOAD_BUFFER_SIZE = 8 * 1024 * 1024L;

static constexpr char OB_S3_APPENDABLE_FORMAT_CONTENT_V1[] = "version=1";
static constexpr char OB_STORAGE_S3_ALLOCATOR[] = "StorageS3";
static constexpr char S3_SDK[] = "S3SDK";

// TODO @fangdan: Validate the effectiveness of the ZeroCopyStreambuf
class ZeroCopyStreambuf : public std::streambuf
{
public:
  ZeroCopyStreambuf(const char *base, std::size_t size)
  {
    setg(const_cast<char*>(base), const_cast<char*>(base), const_cast<char*>(base) + size);
  }

protected:
  virtual std::streampos seekoff(
      std::streamoff off,
      std::ios_base::seekdir dir,
      std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override
  {
    std::streampos result = std::streampos(std::streamoff(-1));
    if (which & std::ios_base::in) {
      char *new_pos = gptr();
      if (dir == std::ios_base::beg) {
        new_pos = eback() + off;
      } else if (dir == std::ios_base::end) {
        new_pos = egptr() + off;
      } else if (dir == std::ios_base::cur) {
        new_pos = gptr() + off;
      }

      if (new_pos >= eback() && new_pos <= egptr()) {
        setg(eback(), new_pos, egptr());
        result = gptr() - eback();
      }
    }
    return result;
  }

  virtual std::streampos seekpos(
      std::streampos pos,
      std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override
  {
    return seekoff(std::streamoff(pos), std::ios_base::beg, which);
  }
};

int set_max_s3_client_idle_duration_us(const int64_t duration_us);
int64_t get_max_s3_client_idle_duration_us();

struct ObS3Account
{
  ObS3Account();
  ~ObS3Account();
  void reset();
  bool is_valid() const { return is_valid_; }
  int64_t hash() const;
  TO_STRING_KV(K_(is_valid), K_(delete_mode), K_(region), K_(endpoint), K_(access_id), KP_(secret_key), K_(sts_token), K_(addressing_model));

  int parse_from(const char *storage_info_str, const int64_t size);

  bool is_valid_;
  int64_t delete_mode_;
  char region_[MAX_S3_REGION_LENGTH];           // region of endpoint
  char endpoint_[MAX_S3_ENDPOINT_LENGTH];
  char access_id_[MAX_S3_ACCESS_ID_LENGTH];     // ak
  char secret_key_[MAX_S3_SECRET_KEY_LENGTH];   // sk
  ObSTSToken sts_token_;
  ObStorageAddressingModel addressing_model_;
};

class ObS3MemoryManager : public Aws::Utils::Memory::MemorySystemInterface
{
  enum
  {
    N_WAY = 32,
    DEFAULT_BLOCK_SIZE = 128 * 1024, // 128KB
  };
public:
  ObS3MemoryManager() : attr_(), mem_limiter_(), allocator_()
  {
    attr_.label_ = S3_SDK;
  }
  virtual ~ObS3MemoryManager() {}
  // when aws init/shutdown, it will execute like this: init/shutdown_memory_system->Begin()/End()
  virtual void Begin() override {}
  virtual void End() override {}
  virtual void *AllocateMemory(std::size_t blockSize,
      std::size_t alignment, const char *allocationTag = nullptr) override;
  virtual void FreeMemory(void *memoryPtr) override;
  int init();
private:
  ObMemAttr attr_;
  // mem_limiter_ is the allocator that actually allocates memory internally for allocator_
  // which can set the limit of memory that can be requestd, default to infinity
  // mem_limiter_ is passed in during allocator_ init
  ObBlockAllocMgr mem_limiter_;
  ObVSliceAlloc allocator_;
};

class ObS3Logger : public Aws::Utils::Logging::LogSystemInterface
{
public:
  ObS3Logger() {}
  virtual ~ObS3Logger() {}
  // Gets the currently configured log level for this logger.
  virtual Aws::Utils::Logging::LogLevel GetLogLevel(void) const override;
  // Does a printf style output to the output stream. Don't use this, it's unsafe. See LogStream
  virtual void Log(Aws::Utils::Logging::LogLevel logLevel, const char* tag, const char* formatStr, ...) override;
  // Writes the stream to the output stream.
  virtual void LogStream(Aws::Utils::Logging::LogLevel logLevel, const char* tag, const Aws::OStringStream &messageStream) override;
  // Writes any buffered messages to the underlying device if the logger supports buffering.
  virtual void Flush() override {}
};

class ObS3Client
{
public:
  ObS3Client();
  virtual ~ObS3Client();
  int init(const ObS3Account &account);
  int check_status();
  void destroy();
  bool is_stopped() const;
  bool try_stop(const int64_t timeout = STOP_S3_TIMEOUT_US);
  void stop();
  void increase();
  void release();
  TO_STRING_KV(KP(&lock_), K_(is_inited), K_(ref_cnt), K_(last_modified_ts), KP(client_));

  int head_object(const Aws::S3::Model::HeadObjectRequest &request,
      Aws::S3::Model::HeadObjectOutcome &outcome);
  int put_object(const Aws::S3::Model::PutObjectRequest &request,
      Aws::S3::Model::PutObjectOutcome &outcome);
  int get_object(const Aws::S3::Model::GetObjectRequest &request,
      Aws::S3::Model::GetObjectOutcome &outcome);
  int delete_object(const Aws::S3::Model::DeleteObjectRequest &request,
      Aws::S3::Model::DeleteObjectOutcome &outcome);
  int delete_objects(const Aws::S3::Model::DeleteObjectsRequest &request,
      Aws::S3::Model::DeleteObjectsOutcome &outcome);
  int put_object_tagging(const Aws::S3::Model::PutObjectTaggingRequest &request,
      Aws::S3::Model::PutObjectTaggingOutcome &outcome);
  int list_objects_v2(const Aws::S3::Model::ListObjectsV2Request &request,
      Aws::S3::Model::ListObjectsV2Outcome &outcome);
  int list_objects(const Aws::S3::Model::ListObjectsRequest &request,
      Aws::S3::Model::ListObjectsOutcome &outcome);
  int get_object_tagging(const Aws::S3::Model::GetObjectTaggingRequest &request,
      Aws::S3::Model::GetObjectTaggingOutcome &outcome);
  int create_multipart_upload(const Aws::S3::Model::CreateMultipartUploadRequest &request,
      Aws::S3::Model::CreateMultipartUploadOutcome &outcome);
  int list_parts(const Aws::S3::Model::ListPartsRequest &request,
      Aws::S3::Model::ListPartsOutcome &outcome);
  int complete_multipart_upload(const Aws::S3::Model::CompleteMultipartUploadRequest &request,
      Aws::S3::Model::CompleteMultipartUploadOutcome &outcome);
  int abort_multipart_upload(const Aws::S3::Model::AbortMultipartUploadRequest &request,
      Aws::S3::Model::AbortMultipartUploadOutcome &outcome);
  int upload_part(const Aws::S3::Model::UploadPartRequest &request,
      Aws::S3::Model::UploadPartOutcome &outcome);
  int list_multipart_uploads(const Aws::S3::Model::ListMultipartUploadsRequest &request,
      Aws::S3::Model::ListMultipartUploadsOutcome &outcome);

private:
  int init_s3_client_configuration_(const ObS3Account &account,
                                    Aws::S3::S3ClientConfiguration &config);

  template<typename RequestType, typename OutcomeType>
  using S3OperationFunc = OutcomeType (Aws::S3::S3Client::*)(const RequestType &) const;

  template<typename RequestType, typename OutcomeType>
  int do_s3_operation_(S3OperationFunc<RequestType, OutcomeType> s3_op_func,
                       const RequestType &request, OutcomeType &outcome,
                       const int64_t retry_timeout_us = ObObjectStorageTenantGuard::get_timeout_us());

private:
  SpinRWLock lock_;
  bool is_inited_;
  bool stopped_;
  int64_t ref_cnt_;
  int64_t last_modified_ts_;
  Aws::S3::S3Client *client_;
};

class ObS3Env
{
public:
  static ObS3Env &get_instance();

  // global init s3 env resource, must and only can be called once
  int init();
  // global clean s3 resource when don't use s3 any more
  void destroy();

  int get_or_create_s3_client(const ObS3Account &account, ObS3Client *&client);
  void stop();

private:
  ObS3Env();
  int clean_s3_client_map_();

private:
  SpinRWLock lock_;
  bool is_inited_;
  ObS3MemoryManager s3_mem_manger_;
  Aws::SDKOptions aws_options_;
  hash::ObHashMap<int64_t, ObS3Client *> s3_client_map_;
};

template <typename OutcomeType>
class ObStorageS3RetryStrategy : public ObStorageIORetryStrategy<OutcomeType>
{
public:
  using typename ObStorageIORetryStrategy<OutcomeType>::RetType;
  using ObStorageIORetryStrategy<OutcomeType>::start_time_us_;
  using ObStorageIORetryStrategy<OutcomeType>::timeout_us_;

  ObStorageS3RetryStrategy(const int64_t timeout_us = ObObjectStorageTenantGuard::get_timeout_us())
      : ObStorageIORetryStrategy<OutcomeType>(timeout_us)
  {}
  virtual ~ObStorageS3RetryStrategy() {}

  virtual void log_error(
      const RetType &outcome, const int64_t attempted_retries) const override
  {
    const char *request_id = outcome.GetResult().GetRequestId().c_str();
    if (outcome.GetResult().GetRequestId().empty()) {
      const Aws::Http::HeaderValueCollection &headers = outcome.GetError().GetResponseHeaders();
      Aws::Http::HeaderValueCollection::const_iterator it = headers.find("x-amz-request-id");
      if (it != headers.end()) {
        request_id = it->second.c_str();
      }
    }
    const int code = static_cast<int>(outcome.GetError().GetResponseCode());
    const char *exception = outcome.GetError().GetExceptionName().c_str();
    const char *err_msg = outcome.GetError().GetMessage().c_str();
    OB_LOG_RET(WARN, OB_SUCCESS, "S3 log error",
        K(start_time_us_), K(timeout_us_), K(attempted_retries),
        K(request_id), K(code), K(exception), K(err_msg));
  }

protected:
  virtual bool should_retry_impl_(
      const RetType &outcome, const int64_t attempted_retries) const override
  {
    bool bret = false;
    if (OB_SUCCESS != EventTable::EN_OBJECT_STORAGE_IO_RETRY) {
      bret = true;
      OB_LOG(INFO, "errsim object storage IO retry", K(outcome.IsSuccess()));
    } else if (outcome.IsSuccess()) {
      bret = false;
    } else if (outcome.GetError().ShouldRetry()) {
      bret = true;
    }
    return bret;
  }
};

struct S3ObjectMeta : public ObStorageObjectMetaBase
{
};

class SafeExecutor
{
public:
  template<typename Function, typename Obj, typename ... Args>
  int do_safely(Function f, Obj obj, Args && ... args)
  {
    int ret = OB_SUCCESS;
    try {
      ret = std::mem_fn(f)(obj, std::forward<Args>(args)...);
    } catch (const std::exception &e) {
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      OB_LOG(WARN, "caught exception when doing s3 operation", K(ret), K(e.what()), KP(this));
    } catch (...) {
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      OB_LOG(WARN, "caught unknown exception when doing s3 operation", K(ret), KP(this));
    }
    return ret;
  }
};

class ObStorageS3Util;

class ObStorageS3Base : public SafeExecutor
{
public:
  ObStorageS3Base();
  virtual ~ObStorageS3Base();
  virtual void reset();

  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info);
  virtual int inner_open(const ObString &uri, ObObjectStorageInfo *storage_info);
  virtual bool is_inited() const { return is_inited_; }

  int get_s3_file_meta(S3ObjectMeta &meta)
  {
    return do_safely(&ObStorageS3Base::get_s3_file_meta_, this, meta);
  }

protected:
  int get_s3_file_meta_(S3ObjectMeta &meta);
  int do_list_(const int64_t max_list_num, const char *delimiter,
      const Aws::String &next_marker, Aws::S3::Model::ListObjectsOutcome &outcome);

protected:
  ObArenaAllocator allocator_;
  ObS3Client *s3_client_;
  ObString bucket_;
  ObString object_;
  // The default is ObStorageChecksumType::OB_MD5_ALGO
  // The S3 SDK cannot disable checksum,
  // therefore ObStorageChecksumType::OB_NO_CHECKSUM_ALGO is not supported
  ObStorageChecksumType checksum_type_;

private:
  bool is_inited_;
  ObS3Account s3_account_;
  friend class ObStorageS3Util;
  DISALLOW_COPY_AND_ASSIGN(ObStorageS3Base);
};

class ObStorageS3Writer : public ObStorageS3Base, public ObIStorageWriter
{
public:
  ObStorageS3Writer();
  virtual ~ObStorageS3Writer();

  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override
  {
    return do_safely(&ObStorageS3Writer::open_, this, uri, storage_info);
  }
  virtual int write(const char *buf, const int64_t size) override
  {
    return do_safely(&ObStorageS3Writer::write_, this, buf, size);
  }
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override
  {
    return do_safely(&ObStorageS3Writer::pwrite_, this, buf, size, offset);
  }
  virtual int close() override
  {
    return do_safely(&ObStorageS3Writer::close_, this);
  }
  virtual int64_t get_length() const override { return file_length_; }
  virtual bool is_opened() const override { return is_opened_; }

protected:
  int open_(const ObString &uri, ObObjectStorageInfo *storage_info);
  int write_(const char *buf, const int64_t size);
  int write_obj_(const char *obj_name, const char *buf, const int64_t size);
  int pwrite_(const char *buf, const int64_t size, const int64_t offset);
  int close_();

protected:
  bool is_opened_;
  int64_t file_length_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageS3Writer);
};

class ObStorageS3Reader : public ObStorageS3Base, public ObIStorageReader
{
public:
  ObStorageS3Reader();
  virtual ~ObStorageS3Reader();
  virtual void reset() override;

  virtual int open(const ObString &uri,
                   ObObjectStorageInfo *storage_info, const bool head_meta = true) override
  {
    return do_safely(&ObStorageS3Reader::open_, this, uri, storage_info, head_meta);
  }
  virtual int pread(char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size) override
  {
    return do_safely(&ObStorageS3Reader::pread_, this, buf, buf_size, offset, read_size);
  }
  virtual int close() override
  {
    return do_safely(&ObStorageS3Reader::close_, this);
  }
  virtual int64_t get_length() const override { return file_length_; }
  virtual bool is_opened() const override { return is_opened_; }

protected:
  int open_(const ObString &uri, ObObjectStorageInfo *storage_info, const bool head_meta = true);
  int pread_(char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size);
  int close_();

protected:
  bool is_opened_;
  bool has_meta_;
  int64_t file_length_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageS3Reader);
};

class ObStorageS3Util : public SafeExecutor, public ObIStorageUtil
{
public:
  ObStorageS3Util();
  virtual ~ObStorageS3Util();

  virtual int open(ObObjectStorageInfo *storage_info) override;
  virtual void close() override;
  virtual int head_object_meta(const ObString &uri, ObStorageObjectMetaBase &obj_meta) override;

  virtual int is_exist(const ObString &uri, bool &exist) override
  {
    return do_safely(&ObStorageS3Util::is_exist_, this, uri, exist);
  }
  virtual int get_file_length(const ObString &uri, int64_t &file_length) override
  {
    return do_safely(&ObStorageS3Util::get_file_length_, this, uri, file_length);
  }
  virtual int del_file(const ObString &uri) override
  {
    return do_safely(&ObStorageS3Util::del_file_, this, uri);
  }
  virtual int batch_del_files(
      const ObString &uri,
      hash::ObHashMap<ObString, int64_t> &files_to_delete,
      ObIArray<int64_t> &failed_files_idx) override
  {
    return do_safely(&ObStorageS3Util::batch_del_files_, this,
                     uri, files_to_delete, failed_files_idx);
  }
  virtual int write_single_file(const ObString &uri, const char *buf, const int64_t size) override
  {
    return do_safely(&ObStorageS3Util::write_single_file_, this, uri, buf, size);
  }
  virtual int mkdir(const ObString &uri) override
  {
    return do_safely(&ObStorageS3Util::mkdir_, this, uri);
  }
  virtual int list_files(const ObString &uri, ObBaseDirEntryOperator &op) override
  {
    return do_safely(&ObStorageS3Util::list_files_, this, uri, op);
  }
  virtual int list_files(const ObString &uri, ObStorageListCtxBase &list_ctx) override
  {
    return do_safely(&ObStorageS3Util::list_files2_, this, uri, list_ctx);
  }
  virtual int del_dir(const ObString &uri) override
  {
    return do_safely(&ObStorageS3Util::del_dir_, this, uri);
  }
  virtual int list_directories(const ObString &uri, ObBaseDirEntryOperator &op) override
  {
    return do_safely(&ObStorageS3Util::list_directories_, this, uri, op);
  }
  virtual int is_tagging(const ObString &uri, bool &is_tagging) override
  {
    return do_safely(&ObStorageS3Util::is_tagging_, this, uri, is_tagging);
  }
  virtual int del_unmerged_parts(const ObString &uri) override
  {
    return do_safely(&ObStorageS3Util::del_unmerged_parts_, this, uri);
  }

private:
  int is_exist_(const ObString &uri, bool &exist);
  int get_file_length_(const ObString &uri, int64_t &file_length);
  int del_file_(const ObString &uri);
  int batch_del_files_(
      const ObString &uri,
      hash::ObHashMap<ObString, int64_t> &files_to_delete,
      ObIArray<int64_t> &failed_files_idx);
  int write_single_file_(const ObString &uri, const char *buf, const int64_t size);
  int mkdir_(const ObString &uri);
  int list_files_(const ObString &uri, ObBaseDirEntryOperator &op);
  int list_files2_(const ObString &uri, ObStorageListCtxBase &list_ctx);
  int del_dir_(const ObString &uri);
  int list_directories_(const ObString &uri, ObBaseDirEntryOperator &op);
  int is_tagging_(const ObString &uri, bool &is_tagging);
  int del_unmerged_parts_(const ObString &uri);

  int delete_object_(ObStorageS3Base &s3_base);
  int tagging_object_(ObStorageS3Base &s3_base);

private:
  bool is_opened_;
  ObObjectStorageInfo *storage_info_;
};

class ObStorageS3AppendWriter : public ObStorageS3Writer
{
public:
  ObStorageS3AppendWriter();
  virtual ~ObStorageS3AppendWriter();

  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override
  {
    return do_safely(&ObStorageS3AppendWriter::open_, this, uri, storage_info);
  }
  virtual int write(const char *buf, const int64_t size) override
  {
    return do_safely(&ObStorageS3AppendWriter::write_, this, buf, size);
  }
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override
  {
    return do_safely(&ObStorageS3AppendWriter::pwrite_, this, buf, size, offset);
  }
  virtual int close() override
  {
    return do_safely(&ObStorageS3AppendWriter::close_, this);
  }
  virtual int64_t get_length() const override;
  virtual bool is_opened() const override { return is_opened_; }

protected:
  int open_(const ObString &uri, ObObjectStorageInfo *storage_info);
  int write_(const char *buf, const int64_t size);
  int pwrite_(const char *buf, const int64_t size, const int64_t offset);
  int close_();

private:
  ObObjectStorageInfo *storage_info_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageS3AppendWriter);
};

class ObStorageS3MultiPartWriter : public ObStorageS3Writer,
                                   public ObIStorageMultiPartWriter,
                                   public ObStoragePartInfoHandler
{
public:
  ObStorageS3MultiPartWriter();
  virtual ~ObStorageS3MultiPartWriter();
  virtual void reset() override;

  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override
  {
    return do_safely(&ObStorageS3MultiPartWriter::open_, this, uri, storage_info);
  }
  virtual int write(const char *buf, const int64_t size) override
  {
    return do_safely(&ObStorageS3MultiPartWriter::write_, this, buf, size);
  }
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override
  {
    return do_safely(&ObStorageS3MultiPartWriter::pwrite_, this, buf, size, offset);
  }
  virtual int complete() override
  {
    return do_safely(&ObStorageS3MultiPartWriter::complete_, this);
  }
  virtual int abort() override
  {
    return do_safely(&ObStorageS3MultiPartWriter::abort_, this);
  }
  virtual int close() override
  {
    return do_safely(&ObStorageS3MultiPartWriter::close_, this);
  }
  virtual int64_t get_length() const override { return file_length_; }
  virtual bool is_opened() const override { return is_opened_; }

private:
  int open_(const ObString &uri, ObObjectStorageInfo *storage_info);
  int write_(const char *buf, const int64_t size);
  int pwrite_(const char *buf, const int64_t size, const int64_t offset);
  int complete_();
  int abort_();
  int close_();
  int write_single_part_();

protected:
  char *base_buf_;
  int64_t base_buf_pos_;
  char *upload_id_;
  int partnum_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageS3MultiPartWriter);
};

class ObStorageParallelS3MultiPartWriter : public ObStorageS3Writer,
                                           public ObIStorageParallelMultipartWriter,
                                           public ObStoragePartInfoHandler
{
public:
  ObStorageParallelS3MultiPartWriter();
  virtual ~ObStorageParallelS3MultiPartWriter();
  virtual void reset() override;

  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override
  {
    return do_safely(&ObStorageParallelS3MultiPartWriter::open_, this, uri, storage_info);
  }
  virtual int upload_part(const char *buf, const int64_t size, const int64_t part_id) override
  {
    return do_safely(&ObStorageParallelS3MultiPartWriter::upload_part_, this, buf, size, part_id);
  }
  virtual int complete() override
  {
    return do_safely(&ObStorageParallelS3MultiPartWriter::complete_, this);
  }
  virtual int abort() override
  {
    return do_safely(&ObStorageParallelS3MultiPartWriter::abort_, this);
  }
  virtual int close() override
  {
    return do_safely(&ObStorageParallelS3MultiPartWriter::close_, this);
  }
  virtual bool is_opened() const override { return is_opened_; }

private:
  int open_(const ObString &uri, ObObjectStorageInfo *storage_info);
  int upload_part_(const char *buf, const int64_t size, const int64_t part_id);
  int complete_();
  int abort_();
  int close_();

protected:
  char *upload_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageParallelS3MultiPartWriter);
};

} // common
} // oceanbase

#endif
