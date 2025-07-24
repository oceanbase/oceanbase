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

#ifndef OCEANBASE_AGENTSERVER_OSS_STORAGE_OSS_BASE_H_
#define OCEANBASE_AGENTSERVER_OSS_STORAGE_OSS_BASE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <openssl/md5.h>
#include "aos_log.h"
#include "aos_util.h"
#include "aos_string.h"
#include "aos_status.h"
#include "oss_auth.h"
#include "oss_util.h"
#include "oss_api.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "ob_i_storage.h"
#include "common/storage/ob_device_common.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{

static const int OSS_BAD_REQUEST = 400;
static const int OSS_OBJECT_NOT_EXIST = 404;
static const int OSS_PERMISSION_DENIED = 403;
static const int OSS_OBJECT_PWRITE_OFFSET_NOT_MATH = 409;
static const int OSS_TOO_MANY_REQUESTS = 429;
static const int OSS_LIMIT_EXCEEDED = 503;
static const int MD5_STR_LENGTH = 32;//md5 buffer length
static const char OSS_META_MD5[] = "x-oss-meta-md5";
const static int64_t AOS_TABLE_INIT_SIZE = 1;
const static int64_t OSS_INVALID_OBJECT_LENGTH = 1;//only include '\0'
const static int64_t MAX_OSS_ENDPOINT_LENGTH = 128;
const static int64_t MAX_OSS_ID_LENGTH = 128;
const static int64_t MAX_OSS_KEY_LENGTH = 128;
const static int64_t OSS_BASE_BUFFER_SIZE = 8 * 1024 * 1024L;//the buf size of upload data
const static int64_t MAX_ELEMENT_COUNT = 10000;//oss limit element count
const static int64_t MULTI_BASE_BUFFER_SIZE = 16 * 1024 * 1024L;//the buf size of upload data
static constexpr char OB_STORAGE_OSS_ALLOCATOR[] = "StorageOSS";

// Before using oss, you need to initialize oss environment.
// Thread safe guaranteed by user.
int init_oss_env();

// You need to clean oss resource when not use oss any more.
// Thread safe guaranteed by user.
void fin_oss_env();

int ob_oss_str_assign(aos_string_t &dst, const int64_t len, const char *src);

class ObStorageOSSRetryStrategy : public ObStorageIORetryStrategy<aos_status_t *>
{
public:
  ObStorageOSSRetryStrategy(const int64_t timeout_us = ObObjectStorageTenantGuard::get_timeout_us());
  virtual ~ObStorageOSSRetryStrategy();

  int set_retry_headers(apr_pool_t *p, apr_table_t *&headers);
  int set_retry_buffer(aos_list_t *write_content_buffer);
  // When batch deleting, the names of successfully deleted objects will be added to the deleted_object_list,
  // so they need to be reset during retries.
  // Only used for errsim cases.
  int set_retry_deleted_object_list(aos_list_t *deleted_object_list);
  // When listing, the names of successfully listed objects will be added to the params,
  // so they need to be reset during retries.
  // Only used for errsim cases.
  int set_retry_list_object_params(oss_list_object_params_t *params);

  virtual void log_error(
      const RetType &outcome, const int64_t attempted_retries) const override;

protected:
  virtual bool should_retry_impl_(
      const RetType &outcome, const int64_t attempted_retries) const override;

  int reinitialize_headers_() const;
  int reinitialize_buffer_() const;

private:
  // When the OSS SDK sends a request, it modifies the header information,
  // which results in the header containing additional fields during retries
  aos_table_t *origin_headers_;
  aos_table_t **ref_headers_;
  // In the write phase, the OSS SDK will remove nodes from the aos list,
  // so an array is used to record the initial list entries
  ObSEArray<void *, 1> origin_list_entries_;
  aos_list_t *ref_buffer_;
  aos_list_t *deleted_object_list_;
  oss_list_object_params_t *params_;
};

struct FrozenInfo
{
  int64_t frozen_version_;
  int64_t frozen_time_;
};

class ObOssEnvIniter
{
public:
  static ObOssEnvIniter &get_instance();

  int global_init();
  void global_destroy();
private:
  ObOssEnvIniter();
  common::SpinRWLock lock_;
  bool is_global_inited_;
};

class ObOssAccount {
public:
  ObOssAccount();
  virtual ~ObOssAccount();
  int parse_oss_arg(const common::ObString &storage_info);
  void reset_account();
  int set_delete_mode(const char *parameter);
  TO_STRING_KV(K_(oss_domain), K_(delete_mode), K_(oss_id), KP_(oss_key), K_(is_inited), K_(sts_token));

  char oss_domain_[MAX_OSS_ENDPOINT_LENGTH];
  char oss_id_[MAX_OSS_ID_LENGTH];
  char oss_key_[MAX_OSS_KEY_LENGTH];
  char *oss_sts_token_;
  // "阿里云STS服务返回的安全令牌（STS Token）的长度不固定，强烈建议您不要假设安全令牌的最大长度。"
  // therefore, use allocator to alloc mem for sts_token dynamically
  common::ObArenaAllocator allocator_;
  ObStorageDeleteMode delete_mode_;
  bool is_inited_;
  ObSTSToken sts_token_;
};

class ObStorageOssBase
{
public:
  ObStorageOssBase();
  virtual ~ObStorageOssBase();

  void reset();
  int init(const common::ObString &storage_info);
  int init_oss_options(aos_pool_t *&aos_pool, oss_request_options_t *&oss_option);
  virtual bool is_inited();
  int get_oss_file_meta(
      const common::ObString &bucket, const common::ObString &object,
      ObStorageObjectMetaBase &meta, const char *&remote_md5);
  void handle_oss_error(aos_table_t *req_headers, aos_table_t *resp_headers, aos_status_s *aos_ret, int &ob_errcode);

  int init_with_storage_info(common::ObObjectStorageInfo *storage_info);
  int init_oss_endpoint();

  int check_endpoint_validaty() const;

  aos_pool_t *aos_pool_;
  oss_request_options_t *oss_option_;
  char oss_endpoint_[MAX_OSS_ENDPOINT_LENGTH];
  bool is_inited_;
  ObOssAccount oss_account_;
  ObStorageChecksumType checksum_type_;
  bool enable_worm_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageOssBase);
};

class ObStorageOssWriter : public ObStorageOssBase, public ObIStorageWriter
{
public:
  ObStorageOssWriter();
  virtual ~ObStorageOssWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  virtual int write(const char *buf,const int64_t size) override;
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override;
  virtual int close() override;
  virtual int64_t get_length() const override { return file_length_;}
  virtual bool is_opened() const override {return is_opened_;}
protected:
  int write_obj_(const char *obj_name, const char *buf, const int64_t size);
protected:
  bool is_opened_;
  int64_t file_length_;
  common::ObArenaAllocator allocator_;
  common::ObString bucket_;
  common::ObString object_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageOssWriter);
};

class ObStorageOssMultiPartWriter: public ObStorageOssBase,
                                   public ObIStorageMultiPartWriter,
                                   public ObStoragePartInfoHandler
{

public:
  ObStorageOssMultiPartWriter();
  virtual ~ObStorageOssMultiPartWriter();
  int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  int write(const char *buf,const int64_t size);
  int pwrite(const char *buf, const int64_t size, const int64_t offset);
  virtual int complete() override;
  virtual int abort() override;
  int close();
  int64_t get_length() const { return file_length_; }
  virtual bool is_opened() const { return is_opened_; }

private:
  int write_single_part();
  int upload_data(const char *buf, const int64_t size, char *&upload_buf, int64_t &upload_size);

private:
  common::ModuleArena allocator_;
  char *base_buf_;
  int64_t base_buf_pos_;
  common::ObString bucket_;
  common::ObString object_;
  aos_string_t upload_id_;
  int partnum_;
  bool is_opened_;
  int64_t file_length_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageOssMultiPartWriter);
};

class ObStorageParallelOssMultiPartWriter: public ObStorageOssBase,
                                           public ObIStorageParallelMultipartWriter,
                                           public ObStoragePartInfoHandler
{

public:
  ObStorageParallelOssMultiPartWriter();
  virtual ~ObStorageParallelOssMultiPartWriter();
  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override;
  virtual int upload_part(const char *buf, const int64_t size, const int64_t part_id) override;
  virtual int complete() override;
  virtual int abort() override;
  virtual int close() override;
  virtual bool is_opened() const override { return is_opened_; }

private:
  bool is_opened_;
  common::ObString bucket_;
  common::ObString object_;
  aos_string_t upload_id_;
  common::ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageParallelOssMultiPartWriter);
};

class ObStorageOssReader: public ObStorageOssBase, public ObIStorageReader
{
//TODO: buf reuse
public:
  ObStorageOssReader();
  virtual ~ObStorageOssReader();
  virtual int open(const common::ObString &uri,
      common::ObObjectStorageInfo *storage_info, const bool head_meta = true) override;
  virtual int pread(char *buf,
      const int64_t buf_size, const int64_t offset, int64_t &read_size) override;
  int close();
  int64_t get_length() const { return file_length_; }
  virtual bool is_opened() const { return is_opened_; }

private:
  common::ObString bucket_;
  common::ObString object_;
  int64_t file_length_;
  bool is_opened_;
  bool has_meta_;
  common::ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageOssReader);
};

int ob_strtotime(const char *date_time, int64_t &time_s);

class ObStorageOssUtil: public ObIStorageUtil
{
public:
  ObStorageOssUtil();
  virtual ~ObStorageOssUtil();
  virtual int open(common::ObObjectStorageInfo *storage_info);
  virtual void close();
  virtual int is_exist(const common::ObString &uri, bool &exist);
  virtual int get_file_length(const common::ObString &uri, int64_t &file_length);
  virtual int head_object_meta(const common::ObString &uri, ObStorageObjectMetaBase &obj_meta);
  virtual int write_single_file(const common::ObString &uri, const char *buf,
                                const int64_t size);

  //oss no dir
  virtual int mkdir(const common::ObString &uri);
  virtual int del_file(const common::ObString &uri);
  virtual int batch_del_files(
      const ObString &uri,
      hash::ObHashMap<ObString, int64_t> &files_to_delete,
      ObIArray<int64_t> &failed_files_idx) override;
  virtual int list_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op);
  virtual int list_files(const common::ObString &uri, ObStorageListCtxBase &list_ctx);
  virtual int del_dir(const common::ObString &uri);
  virtual int list_directories(const common::ObString &uri, common::ObBaseDirEntryOperator &op);
  virtual int is_tagging(const common::ObString &uri, bool &is_tagging);
  virtual int del_unmerged_parts(const ObString &uri) override;
private:
  int tagging_object_(
      const common::ObString &uri,
      ObStorageOssBase &oss_base,
      const common::ObString &bucket_str,
      const common::ObString &object_str);
  int delete_object_(
      const common::ObString &uri,
      ObStorageOssBase &oss_base,
      const common::ObString &bucket_str,
      const common::ObString &object_str);
  int do_list_(ObStorageOssBase &oss_base,
      const ObString &bucket, const char *full_dir_path,
      const int64_t max_ret, const char *delimiter,
      const char *next_marker, oss_list_object_params_t *&params);
private:
  bool is_opened_;
  common::ObObjectStorageInfo *storage_info_;
};

class ObStorageOssAppendWriter : public ObStorageOssWriter
{
public:
  ObStorageOssAppendWriter():ObStorageOssWriter() {}
  virtual ~ObStorageOssAppendWriter();

public:
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  virtual int write(const char *buf, const int64_t size) override;
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override;
  virtual int close() override;

private:
  int do_write(const char *buf, const int64_t size, const int64_t offset, const bool is_pwrite);
  int simulate_append_write_for_worm_(const char *buf, const int64_t size, const int64_t offset);
  DISALLOW_COPY_AND_ASSIGN(ObStorageOssAppendWriter);
};




}//common
}//oceanbase
#endif
