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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_OBDAL_BASE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_OBDAL_BASE_H_

#include "ob_i_storage.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_tracepoint.h"
#include "obdal/obdal_accessor.h"

namespace oceanbase
{
namespace common
{

int init_obdal_env();
void fin_obdal_env();

static constexpr int POOL_MAX_IDLE_PER_HOST = 32;         // the max idle http client count 
static constexpr int POOL_MAX_IDLE_TIME_S = 45;          // the max time of idle http client (unit s)

static constexpr int MAX_OBDAL_REGION_LENGTH = 128;
static constexpr int MAX_OBDAL_ENDPOINT_LENGTH = 256;
static constexpr int MAX_OBDAL_ACCESS_ID_LENGTH = 256;
static constexpr int MAX_OBDAL_SECRET_KEY_LENGTH = 256;
static constexpr char OB_STORAGE_OBDAL_ALLOCATOR[] = "StorageObDal";
static constexpr char OB_DAL_SDK[] = "OBDALSDK";

class ObDalMemoryManager
{
public:
  static ObDalMemoryManager &get_instance();
  int init(); 
  void *allocate(std::size_t size, std::size_t align);
  void free(void *ptr);
  

private:
  ObDalMemoryManager();
  ~ObDalMemoryManager();
  
private:
  static constexpr int64_t N_WAY = 32;
  static constexpr int64_t DEFAULT_BLOCK_SIZE = 128 * 1024; // 128KB
  ObMemAttr attr_;
  ObBlockAllocMgr mem_limiter_;
  ObVSliceAlloc allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObDalMemoryManager);
};

class ObDalEnvIniter
{
public:
  static ObDalEnvIniter &get_instance();

  int global_init();
  void global_destroy();
private:
  ObDalEnvIniter();
  common::SpinRWLock lock_;
  bool is_global_inited_;
};

struct ObDalAccount : public ObStorageAccount
{
public:
  ObDalAccount();
  virtual ~ObDalAccount() override;
  virtual void reset() override;
  virtual bool is_valid() const override { return is_valid_; }
  virtual int assign(const ObObjectStorageInfo *storage_info) override;
  INHERIT_TO_STRING_KV("ObStorageAccount", ObStorageAccount, K(region_), K(addressing_model_));
  
  char region_[MAX_OBDAL_REGION_LENGTH];
  ObStorageAddressingModel addressing_model_;
};

struct ObDalObjectMeta : public ObStorageObjectMeta
{
};

class ObStorageObDalBase
{
public:
  ObStorageObDalBase();
  virtual ~ObStorageObDalBase();
public:
  virtual void reset();
  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info);
  virtual int inner_open(const ObString &uri, ObObjectStorageInfo *storage_info);
  virtual bool is_inited() { return is_inited_; }
public:
  int get_file_meta(ObDalObjectMeta &meta);

  friend class ObStorageObDalUtil;

protected:
  bool is_inited_;
  ObArenaAllocator allocator_;
  ObStorageType storage_type_;
  common::ObString bucket_;
  common::ObString object_;
  ObDalAccount obdal_account_;
  ObStorageChecksumType checksum_type_;
  opendal_operator_options *options_;
  opendal_operator *op_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageObDalBase);
};


class ObStorageObDalWriter : public ObStorageObDalBase, public ObIStorageWriter
{
public:
  ObStorageObDalWriter();
  virtual ~ObStorageObDalWriter();
  virtual void reset() override;
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  virtual int write(const char *buf,const int64_t size) override;
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override;
  virtual int close() override;
  virtual int64_t get_length() const override { return file_length_;}
  virtual bool is_opened() const override {return is_opened_;}

protected:
  bool is_opened_;
  int64_t file_length_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageObDalWriter);
};

class ObStorageObDalReader : public ObStorageObDalBase, public ObIStorageReader
{
public:
  ObStorageObDalReader();
  virtual ~ObStorageObDalReader();
  virtual void reset() override;
  virtual int open(const ObString &uri,
           ObObjectStorageInfo *storage_info, const bool head_meta = true) override;
  virtual int pread(char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size) override;
  virtual int close() override;
  virtual int64_t get_length() const override { return file_length_; }
  virtual bool is_opened() const override { return is_opened_; }

protected:
  bool is_opened_;
  bool has_meta_;
  int64_t file_length_;
  opendal_reader *opendal_reader_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageObDalReader);
};

class ObStorageObDalUtil : public ObIStorageUtil
{
public:
  ObStorageObDalUtil();
  virtual ~ObStorageObDalUtil();
  virtual int open(common::ObObjectStorageInfo *storage_info);
  virtual void close();
  virtual int is_exist(const common::ObString &uri, bool &exist);
  virtual int get_file_length(const common::ObString &uri, int64_t &file_length);
  virtual int head_object_meta(const common::ObString &uri, ObStorageObjectMetaBase &obj_meta);
  virtual int write_single_file(const common::ObString &uri, const char *buf,
                                const int64_t size);

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
  bool is_opened_;
  common::ObObjectStorageInfo *storage_info_;
};

class ObStorageObDalAppendWriter : public ObStorageObDalBase, public ObIStorageWriter
{
public:
  ObStorageObDalAppendWriter();
  virtual ~ObStorageObDalAppendWriter();
  virtual void reset() override;
  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override;
  virtual int write(const char *buf, const int64_t size) override;
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override;
  virtual int close() override;
  virtual int64_t get_length() const override;
  virtual bool is_opened() const override { return is_opened_; }
private:
  bool is_opened_;
  ObObjectStorageInfo *storage_info_;
  int64_t file_length_;
  opendal_writer *writer_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageObDalAppendWriter);
};

class ObStorageObDalMultiPartWriter : public ObStorageObDalBase, public ObIStorageMultiPartWriter
{
public:
  ObStorageObDalMultiPartWriter();
  virtual ~ObStorageObDalMultiPartWriter();
  virtual void reset() override;
  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override;
  virtual int write(const char *buf, const int64_t size) override;
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override;
  virtual int abort() override; 
  virtual int complete() override;
  virtual int close() override;
  virtual int64_t get_length() const override { return file_length_; };
  virtual bool is_opened() const override { return is_opened_; }

private:
  bool is_opened_;
  opendal_writer *opendal_writer_;
  int64_t file_length_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageObDalMultiPartWriter);
};

class ObStorageParallelObDalMultiPartWriter : public ObStorageObDalBase,
                                              public ObIStorageParallelMultipartWriter
{
public:
  ObStorageParallelObDalMultiPartWriter();
  virtual ~ObStorageParallelObDalMultiPartWriter();

  virtual void reset() override;
  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override;
  virtual int upload_part(const char *buf, const int64_t size, const int64_t part_id) override;
  virtual int complete() override;
  virtual int abort() override;
  virtual int close() override;
  virtual bool is_opened() const override { return is_opened_; }

private:
  bool is_opened_;
  bool has_writed_;
  opendal_multipart_writer *opendal_multipart_writer_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageParallelObDalMultiPartWriter);
};

} // common
} // oceanbase

#endif

