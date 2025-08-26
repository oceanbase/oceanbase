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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_H_

#include "lib/lock/ob_tc_rwlock.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_info.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileIOReadCtx;
class ObTmpFileIOWriteCtx;

class ObITmpFile
{
public:
  ObITmpFile();
  virtual ~ObITmpFile();
  virtual int init(const int64_t tenant_id,
                   const int64_t dir_id,
                   const int64_t fd,
                   ObIAllocator *callback_allocator,
                   const char* label);
  virtual void reset();
  virtual int release_resource() = 0;
  int delete_file();
  virtual int aio_pread(ObTmpFileIOReadCtx &io_ctx);
  virtual int write(ObTmpFileIOWriteCtx &io_ctx) = 0;
  // truncate offset is open interval
  virtual int truncate(const int64_t truncate_offset) = 0;

public:
  void set_compressible_info(const OB_TMP_FILE_TYPE file_type,
                             const int64_t compressible_fd,
                             const void* compressible_file)
  {
    file_type_ = file_type;
    compressible_fd_ = compressible_fd;
    compressible_file_ = compressible_file;
  }

protected:
  virtual int inner_delete_file_() = 0;
  int inner_read_truncated_part_(ObTmpFileIOReadCtx &io_ctx);
  virtual int inner_read_valid_part_(ObTmpFileIOReadCtx &io_ctx) = 0;
public:
  enum class ObTmpFileMode
  {
    INVALID = -1,
    SHARED_NOTHING = 0,
    SHARED_STORAGE,
    MAX
  };

public:
  bool can_remove();
  bool is_deleting();

  int64_t get_file_size();
  void update_read_offset(int64_t read_offset);

  OB_INLINE ObTmpFileMode get_mode() const { return mode_; }
  OB_INLINE int64_t get_fd() const { return fd_; }
  OB_INLINE int64_t get_dir_id() const { return dir_id_; }
  OB_INLINE void inc_ref_cnt() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE void dec_ref_cnt() { ATOMIC_AAF(&ref_cnt_, -1); }
  OB_INLINE void dec_ref_cnt(int64_t &ref_cnt)
  {
    ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  }
  OB_INLINE int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }

  VIRTUAL_TO_STRING_KV(K(is_inited_), K(mode_), K(tenant_id_), K(dir_id_), K(fd_),
                       K(is_deleting_), K(is_sealed_),
                       K(ref_cnt_), K(truncated_offset_), K(read_offset_),
                       K(file_size_), KP(callback_allocator_),
                       K(trace_id_), K(birth_ts_), K(label_),
                       K(file_type_), K(compressible_fd_));

public:
  // for virtual table
  void set_read_stats_vars(const ObTmpFileIOReadCtx &ctx, const int64_t read_size);
  void set_write_stats_vars(const ObTmpFileIOWriteCtx &ctx);
  virtual int copy_info_for_virtual_table(ObTmpFileBaseInfo &tmp_file_info) = 0;

protected:
  // for virtual table
  virtual void inner_set_read_stats_vars_(const ObTmpFileIOReadCtx &ctx, const int64_t read_size) = 0;
  virtual void inner_set_write_stats_vars_(const ObTmpFileIOWriteCtx &ctx) = 0;

protected:
  OB_INLINE bool has_unfinished_page_() const { return file_size_ % ObTmpFileGlobal::PAGE_SIZE != 0; }
  OB_INLINE int64_t get_page_begin_offset_(const int64_t offset) const
  {
    return common::lower_align(offset, ObTmpFileGlobal::PAGE_SIZE);
  }
  OB_INLINE int64_t get_page_end_offset_(const int64_t offset) const
  {
    return common::upper_align(offset, ObTmpFileGlobal::PAGE_SIZE);
  }
  OB_INLINE int64_t get_page_begin_offset_by_virtual_id_(const int64_t virtual_page_id) const
  {
    return virtual_page_id * ObTmpFileGlobal::PAGE_SIZE;
  }
  OB_INLINE int64_t get_page_end_offset_by_virtual_id_(const int64_t virtual_page_id) const
  {
    return (virtual_page_id + 1) * ObTmpFileGlobal::PAGE_SIZE;
  }
  OB_INLINE int64_t get_page_virtual_id_(const int64_t offset, const bool is_open_interval) const
  {
    return is_open_interval ?
           common::upper_align(offset, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE - 1 :
           offset / ObTmpFileGlobal::PAGE_SIZE;
  }
  OB_INLINE int64_t get_offset_in_page_(const int64_t offset) const
  {
    return offset % ObTmpFileGlobal::PAGE_SIZE;
  }

protected:
  bool is_inited_;
  ObTmpFileMode mode_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  int64_t fd_;
  bool is_deleting_;
  bool is_sealed_;
  int64_t ref_cnt_;
  int64_t truncated_offset_;      // read data befor truncated_offset will be set as 0
  int64_t read_offset_;           // read offset is on the entire file
  int64_t file_size_;             // has written size of this file
  common::SpinRWLock meta_lock_; // handle conflicts between writing and reading meta tree and meta data of file
  ObSpinLock stat_lock_;
  ObIAllocator *callback_allocator_;
  /********for virtual table begin********/
  common::ObCurTraceId::TraceId trace_id_;
  int64_t birth_ts_;
  ObFixedLengthString<ObTmpFileGlobal::TMP_FILE_MAX_LABEL_SIZE + 1> label_;
  OB_TMP_FILE_TYPE file_type_;
  int64_t compressible_fd_;
  const void* compressible_file_;
  /********for virtual table end********/
};

class ObITmpFileHandle
{
public:
  ObITmpFileHandle() : ptr_(nullptr) {}
  ObITmpFileHandle(ObITmpFile *tmp_file);
  ObITmpFileHandle(const ObITmpFileHandle &handle);
  ObITmpFileHandle & operator=(const ObITmpFileHandle &other);
  ~ObITmpFileHandle() { reset(); }
  OB_INLINE ObITmpFile * get() const {return ptr_; }
  bool is_inited() const { return nullptr != ptr_; }
  void reset();
  int init(ObITmpFile *tmp_file);
  TO_STRING_KV(KP(ptr_));
protected:
  ObITmpFile *ptr_;
};

struct ObTmpFileKey final
{
  explicit ObTmpFileKey(const int64_t fd) : fd_(fd) {}
  OB_INLINE int hash(uint64_t &hash_val) const
  {
    hash_val = murmurhash(&fd_, sizeof(int64_t), 0);
    return OB_SUCCESS;
  }
  OB_INLINE bool operator==(const ObTmpFileKey &other) const { return fd_ == other.fd_; }
  TO_STRING_KV(K(fd_));
  int64_t fd_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_H_
