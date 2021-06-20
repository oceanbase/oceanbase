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

#ifdef _WITH_OSS
#include "aos_log.h"
#include "aos_util.h"
#include "aos_string.h"
#include "aos_status.h"
#include "oss_auth.h"
#include "oss_util.h"
#include "oss_api.h"
#endif

#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "ob_i_storage.h"

namespace oceanbase {
namespace common {

static const int OSS_OBJECT_NOT_EXIST = 404;
static const int MD5_STR_LENGTH = 32;  // md5 buffer length
static const char OSS_META_MD5[] = "x-oss-meta-md5";
const static int64_t AOS_TABLE_INIT_SIZE = 1;
const static int64_t OSS_INVALID_OBJECT_LENGTH = 1;  // only include '\0'
const static int64_t MAX_OSS_ENDPOINT_LENGTH = 128;
const static int64_t MAX_OSS_ID_LENGTH = 128;
const static int64_t MAX_OSS_KEY_LENGTH = 128;
const static int64_t BASE_BUFFER_SIZE = 8 * 1024 * 1024L;         // the buf size of upload data
const static int64_t MAX_ELEMENT_COUNT = 10000;                   // oss limit element count
const static int64_t MULTI_BASE_BUFFER_SIZE = 16 * 1024 * 1024L;  // the buf size of upload data

class ObStorageOssStaticVar {
public:
  ObStorageOssStaticVar();
  virtual ~ObStorageOssStaticVar();
  static ObStorageOssStaticVar& get_instance();
  int set_oss_compress_name(const char* name);
  common::ObCompressor* get_oss_compressor();
  common::ObCompressorType get_compressor_type();

private:
  common::ObCompressor* compressor_;
  common::ObCompressorType compress_type_;
};

struct FrozenInfo {
  int64_t frozen_version_;
  int64_t frozen_time_;
};

class ObOssEnvIniter {
public:
  static ObOssEnvIniter& get_instance();

  int global_init();
  void global_destroy();

private:
  ObOssEnvIniter();
  common::SpinRWLock lock_;
  bool is_global_inited_;
};

class ObStorageOssBase {
public:
  ObStorageOssBase();
  virtual ~ObStorageOssBase();

  void reset();
  int init(const common::ObString& storage_info);
  int reinit_oss_option();
  int init_oss_options(aos_pool_t*& aos_pool, oss_request_options_t*& oss_option);
  virtual bool is_inited();
  int get_oss_file_meta(const common::ObString& bucket, const common::ObString& object, bool& is_file_exist,
      char*& remote_md5, int64_t& file_length);
  int get_oss_file_length(const common::ObString& name, int64_t& file_length);
  void print_oss_info(aos_status_s* aos_ret);

protected:
  aos_pool_t* aos_pool_;
  oss_request_options_t* oss_option_;

private:
  int parse_oss_arg(const common::ObString& uri);
  static int set_oss_field(const char* info, char* field, const int64_t length);

private:
  bool is_inited_;
  char oss_domain_[OB_MAX_URI_LENGTH];
  char oss_endpoint_[MAX_OSS_ENDPOINT_LENGTH];
  char oss_id_[MAX_OSS_ID_LENGTH];
  char oss_key_[MAX_OSS_KEY_LENGTH];
  int init_oss_endpoint();
  DISALLOW_COPY_AND_ASSIGN(ObStorageOssBase);
};

class ObStorageOssMultiPartWriter : public ObStorageOssBase, public ObIStorageWriter {

public:
  ObStorageOssMultiPartWriter();
  virtual ~ObStorageOssMultiPartWriter();
  int open(const common::ObString& uri, const common::ObString& storage_info);
  int write(const char* buf, const int64_t size);
  int close();
  int cleanup();
  int64_t get_length() const
  {
    return file_length_;
  }
  virtual bool is_opened() const
  {
    return is_opened_;
  }

private:
  int write_single_part();
  int upload_data(const char* buf, const int64_t size, char*& upload_buf, int64_t& upload_size);

private:
  common::ModulePageAllocator mod_;
  common::ModuleArena allocator_;
  char* base_buf_;
  int64_t base_buf_pos_;
  common::ObString bucket_;
  common::ObString object_;
  aos_string_t upload_id_;
  int partnum_;
  MD5_CTX whole_file_md5_;
  bool is_opened_;
  int64_t file_length_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageOssMultiPartWriter);
};

class ObStorageOssReader : public ObStorageOssBase, public ObIStorageReader {
  // TODO: buf reuse
public:
  ObStorageOssReader();
  virtual ~ObStorageOssReader();
  int open(const common::ObString& uri, const common::ObString& storage_info);
  int pread(char* buf, const int64_t buf_size, int64_t offset, int64_t& read_size);
  int close();
  int64_t get_length() const
  {
    return file_length_;
  }
  virtual bool is_opened() const
  {
    return is_opened_;
  }

private:
  common::ObString bucket_;
  common::ObString object_;
  int64_t file_length_;
  bool is_opened_;
  common::ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageOssReader);
};

class ObStorageOssUtil : public ObStorageOssBase, public ObIStorageUtil {
public:
  ObStorageOssUtil();
  virtual ~ObStorageOssUtil();
  virtual int is_exist(const common::ObString& uri, const common::ObString& storage_info, bool& exist);
  virtual int get_file_length(const common::ObString& uri, const common::ObString& storage_info, int64_t& file_length);
  virtual int write_single_file(
      const common::ObString& uri, const common::ObString& storage_info, const char* buf, const int64_t size);

  // oss no dir
  virtual int mkdir(const common::ObString& uri, const common::ObString& storage_info);
  virtual int del_file(const common::ObString& uri, const common::ObString& storage_info);
  virtual int update_file_modify_time(const common::ObString& uri, const common::ObString& storage_info);
  virtual int list_files(const common::ObString& dir_path, const common::ObString& storage_info,
      common::ObIAllocator& allocator, common::ObIArray<common::ObString>& file_names);
  virtual int del_dir(const common::ObString& uri, const common::ObString& storage_info);
  virtual int get_pkeys_from_dir(const common::ObString& dir_path, const common::ObString& storage_info,
      common::ObIArray<common::ObPartitionKey>& pkeys);
  virtual int delete_tmp_files(const common::ObString& dir_path, const common::ObString& storage_info);

private:
  int strtotime(const char* date_time, int64_t& time);
};

class ObStorageOssAppendWriter : public ObStorageOssBase, public ObIStorageWriter {
public:
  ObStorageOssAppendWriter();
  virtual ~ObStorageOssAppendWriter();

public:
  int open(const common::ObString& uri, const common::ObString& storage_info);
  int write(const char* buf, const int64_t size);
  int close();
  int64_t get_length() const
  {
    return file_length_;
  }
  virtual bool is_opened() const
  {
    return is_opened_;
  }

private:
  bool is_opened_;
  int64_t file_length_;
  common::ObArenaAllocator allocator_;
  common::ObString bucket_;
  common::ObString object_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageOssAppendWriter);
};

class ObStorageOssMetaMgr : public ObStorageOssBase, public ObIStorageMetaWrapper {
public:
  ObStorageOssMetaMgr();
  virtual ~ObStorageOssMetaMgr();

public:
  int get(const common::ObString& uri, const common::ObString& storage_info, char* buf, const int64_t buf_size,
      int64_t& read_size);
  int set(const common::ObString& uri, const common::ObString& storage_info, const char* buf, const int64_t buf_size);

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageOssMetaMgr);
};

// TODO qiaxi remove, compat to old version
class ObStorageOssMetaMgrOld : public ObStorageOssBase {
public:
  ObStorageOssMetaMgrOld();
  virtual ~ObStorageOssMetaMgrOld();

  int read(const common::ObString& uri, const common::ObString& storage_info,
      const common::ObArray<const char*>& meta_names, common::ObArray<int64_t>& meta_values);

  int str_to_int64(const char* nptr, int64_t& value);

private:
  common::ModulePageAllocator mod_;
  common::ModuleArena allocator_;
  common::ObString bucket_;
  common::ObString object_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageOssMetaMgrOld);
};

}  // namespace common
}  // namespace oceanbase
#endif
