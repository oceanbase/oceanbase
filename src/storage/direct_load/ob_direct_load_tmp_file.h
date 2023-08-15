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
#pragma once

#include "observer/table_load/ob_table_load_object_allocator.h"
#include "storage/blocksstable/ob_tmp_file.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTmpFileManager;

struct ObDirectLoadTmpFileId
{
  ObDirectLoadTmpFileId() : dir_id_(-1), fd_(-1) {}
  bool is_valid() const { return -1 != dir_id_ && -1 != fd_; }
  void reset()
  {
    dir_id_ = -1;
    fd_ = -1;
  }
  TO_STRING_KV(K_(dir_id), K_(fd));
public:
  int64_t dir_id_;
  int64_t fd_;
};

class ObDirectLoadTmpFile
{
public:
  ObDirectLoadTmpFile(ObDirectLoadTmpFileManager *file_mgr, const ObDirectLoadTmpFileId &file_id)
    : file_mgr_(file_mgr), file_id_(file_id), ref_count_(0)
  {
  }
  bool is_valid() const { return nullptr != file_mgr_ && file_id_.is_valid(); }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_AAF(&ref_count_, -1); }
  ObDirectLoadTmpFileManager *get_file_mgr() const { return file_mgr_; }
  const ObDirectLoadTmpFileId &get_file_id() const { return file_id_; }
  TO_STRING_KV(KP_(file_mgr), K_(file_id), K_(ref_count));
private:
  ObDirectLoadTmpFileManager *const file_mgr_;
  const ObDirectLoadTmpFileId file_id_;
  int64_t ref_count_ CACHE_ALIGNED;
  DISABLE_COPY_ASSIGN(ObDirectLoadTmpFile);
};

class ObDirectLoadTmpFileHandle final
{
public:
  ObDirectLoadTmpFileHandle();
  ~ObDirectLoadTmpFileHandle();
  void reset();
  bool is_valid() const { return nullptr != tmp_file_ && tmp_file_->is_valid(); }
  int assign(const ObDirectLoadTmpFileHandle &other);
  int set_file(ObDirectLoadTmpFile *tmp_file);
  ObDirectLoadTmpFile *get_file() const { return tmp_file_; }
  TO_STRING_KV(KPC_(tmp_file));
private:
  ObDirectLoadTmpFile *tmp_file_;
  DISABLE_COPY_ASSIGN(ObDirectLoadTmpFileHandle);
};

class ObDirectLoadTmpFilesHandle final
{
public:
  ObDirectLoadTmpFilesHandle();
  ~ObDirectLoadTmpFilesHandle();
  void reset();
  int assign(const ObDirectLoadTmpFilesHandle &other);
  int add(const ObDirectLoadTmpFileHandle &tmp_file_handle);
  int add(const ObDirectLoadTmpFilesHandle &tmp_files_handle);
  int count() const { return tmp_file_list_.count(); }
  bool empty() const { return tmp_file_list_.empty(); }
  int get_file(int64_t idx, ObDirectLoadTmpFileHandle &tmp_file_handle) const;
  TO_STRING_KV(K_(tmp_file_list));
private:
  int add_file(ObDirectLoadTmpFile *tmp_file);
private:
  common::ObArray<ObDirectLoadTmpFile *> tmp_file_list_;
  DISABLE_COPY_ASSIGN(ObDirectLoadTmpFilesHandle);
};

class ObDirectLoadTmpFileIOHandle final
{
public:
  ObDirectLoadTmpFileIOHandle();
  ~ObDirectLoadTmpFileIOHandle();
  void reset();
  bool is_valid() const { return file_handle_.is_valid(); }
  int open(const ObDirectLoadTmpFileHandle &file_handle);
  int aio_read(char *buf, int64_t size);
  int aio_pread(char *buf, int64_t size, int64_t offset);
  int read(char *buf, int64_t &size, int64_t timeout_ms);
  int pread(char *buf, int64_t &size, int64_t offset, int64_t timeout_ms);
  int aio_write(char *buf, int64_t size);
  int write(char *buf, int64_t size, int64_t timeout_ms);
  int wait(int64_t timeout_ms);
  // for aio read to get real read size when ret = OB_ITER_END
  OB_INLINE int64_t get_data_size() { return file_io_handle_.get_data_size(); }
  static int seek(const ObDirectLoadTmpFileHandle &file_handle, int64_t offset, int whence);
  static int sync(const ObDirectLoadTmpFileHandle &file_handle, int64_t timeout_ms);
  TO_STRING_KV(K_(file_handle));
private:
  ObDirectLoadTmpFileHandle file_handle_;
  blocksstable::ObTmpFileIOInfo io_info_;
  blocksstable::ObTmpFileIOHandle file_io_handle_;
  DISABLE_COPY_ASSIGN(ObDirectLoadTmpFileIOHandle);
};

class ObDirectLoadTmpFileManager
{
public:
  ObDirectLoadTmpFileManager();
  ~ObDirectLoadTmpFileManager();
  int init(uint64_t tenant_id);
  int alloc_dir(int64_t &dir_id);
  int alloc_file(int64_t dir_id, ObDirectLoadTmpFileHandle &tmp_file_handle);
  void put_file(ObDirectLoadTmpFile *tmp_file);
private:
  observer::ObTableLoadObjectAllocator<ObDirectLoadTmpFile> file_allocator_;
  bool is_inited_;
  DISABLE_COPY_ASSIGN(ObDirectLoadTmpFileManager);
};

} // namespace storage
} // namespace oceanbase
