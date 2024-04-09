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

#ifndef DEPS_OBLIB_SRC_COMMON_STORAGE_OB_IO_DEVICE_H_
#define DEPS_OBLIB_SRC_COMMON_STORAGE_OB_IO_DEVICE_H_

#include "ob_device_common.h"
#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObIODevice;
/**
 * ObIOFd may represent super block, normal file or block id.
 *
 * Super block is maintained in ObIODevice, it can't be directly open or alloc from outside.
 * But it can be read/write directly after ObIODevice ready.
 * When the value of first_id_ and second_id_ are both 0, it represents super block.
 * When the value of first_id_ equals NORMAL_FILE_ID, it represents a normal file.
 * Otherwise, it represents a block.
 */
struct ObIOFd
{
  ObIOFd(ObIODevice *device_handle = nullptr, const int64_t first_id = -1, const int64_t second_id = -1);
  bool is_super_block() const { return 0 == first_id_ && 0 == second_id_; }
  bool is_normal_file() const { return NORMAL_FILE_ID == first_id_ && second_id_ > 0; }
  bool is_block_file() const { return first_id_ != NORMAL_FILE_ID; }
  void reset();
  uint64_t hash() const;
  bool operator == (const ObIOFd& other) const
  {
    return other.first_id_ == this->first_id_ && other.second_id_ == this->second_id_ && other.device_handle_ == this->device_handle_;
  }
  bool operator != (const ObIOFd& other) const
  {
    return other.first_id_ != this->first_id_ || other.second_id_ != this->second_id_ || other.device_handle_ != this->device_handle_;
  }
  bool is_valid() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(first_id), K_(second_id), KP_(device_handle));
  static const int64_t NORMAL_FILE_ID = 0xFFFFFFFFFFFFFFFF;// all of bit is one.
  int64_t first_id_;
  int64_t second_id_;
  ObIODevice *device_handle_; // no need to serialize
};

template<typename T>
class ObIOCBPool final
{
public:
  ObIOCBPool()
    : free_iocbs_(),
      first_iocb_(nullptr),
      pre_allocated_size_(0),
      allocator_(nullptr),
      is_inited_(false)
  {
  }
  ~ObIOCBPool()
  {
    reset();
  }
  int init(common::ObIAllocator &allocator, const int64_t pre_allocated_size)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "cannot initialize twice", K(ret));
    } else if (FALSE_IT(pre_allocated_size_ = MAX(2, pre_allocated_size))) {
    } else if (OB_ISNULL(first_iocb_ = static_cast<T *>(allocator.alloc(pre_allocated_size_ * sizeof(T))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to pre-allocate iocbs", K(ret), K(pre_allocated_size_));
    } else if (OB_FAIL(free_iocbs_.init(pre_allocated_size_, &allocator))) {
      STORAGE_LOG(WARN, "fail to init free iocb queue", K(ret));
    } else {
      allocator_ = &allocator;
      for (int64_t i = 0; OB_SUCC(ret) && i < pre_allocated_size_; ++i) {
        if (OB_FAIL(free_iocbs_.push(first_iocb_ + i))) {
          STORAGE_LOG(WARN, "fail to push into free iocbs", K(ret), K(i), K(first_iocb_), K(first_iocb_ + i));
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
    if (OB_UNLIKELY(!is_inited_)) {
      reset();
    }
    return ret;
  }
  void reset()
  {
    free_iocbs_.destroy();
    if (OB_NOT_NULL(first_iocb_)) {
      if (OB_ISNULL(allocator_)) {
        STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "allocator is nullptr!!!", K_(is_inited), KP_(allocator), KP_(first_iocb));
      } else {
        allocator_->free(first_iocb_);
      }
      first_iocb_ = nullptr;
    }
    pre_allocated_size_ = 0;
    allocator_ = nullptr;
    is_inited_ = false;
  }
  T *alloc()
  {
    int ret = OB_SUCCESS;
    T *t = nullptr;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "IOCBPreAllocatedPool isn't initialized", K(ret));
    } else if (OB_SUCC(free_iocbs_.pop(t))) {
      if (OB_ISNULL(t)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "t is nullptr!!!", K(ret), KP(t));
      }
    } else if (OB_UNLIKELY(common::OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "fail to pop from free iocbs queue", K(ret));
    } else if (OB_ISNULL(t = static_cast<T *>(allocator_->alloc(sizeof(T))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate iocb", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
    return t;
  }
  void free(T *ptr)
  {
    if (OB_ISNULL(ptr)) {
    } else if (is_pre_allocated(ptr)) {
      free_iocbs_.push(ptr);
    } else {
      if (OB_ISNULL(allocator_)) {
        STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "allocator is nullptr!!!", K_(is_inited), KP_(allocator), KP_(first_iocb), KP(ptr));
      } else {
        allocator_->free(ptr);
      }
    }
  }
private:
  typedef common::ObFixedQueue<T> FreeIOCBQueue;
  bool is_pre_allocated(T *ptr) const { return first_iocb_ <= ptr && ptr < first_iocb_ + pre_allocated_size_; }
private:
  FreeIOCBQueue free_iocbs_;
  T *first_iocb_;
  int64_t pre_allocated_size_;
  ObIAllocator *allocator_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObIOCBPool);
};

class ObIOCB
{
public:
  ObIOCB() = default;
  virtual ~ObIOCB() = default;
};

class ObIOContext
{
public:
  ObIOContext() = default;
  virtual ~ObIOContext() = default;
};

class ObIOEvents
{
public:
  ObIOEvents() : max_event_cnt_(0) {}
  virtual ~ObIOEvents() {}
  virtual int64_t get_complete_cnt() const = 0;
  virtual int get_ith_ret_code(const int64_t i) const = 0;
  virtual int get_ith_ret_bytes(const int64_t i) const = 0;
  virtual void *get_ith_data(const int64_t i) const = 0;
protected:
  int64_t max_event_cnt_;
};

union ObOptValue {
  ObOptValue() : value_str (NULL) {}
  void reset() {value_str = NULL;}
  const char * value_str;
  int64_t      value_int64;
  bool         value_bool;
};

struct ObIODOpt
{
  ObIODOpt() : key_(nullptr)
  {
    value_.value_str = NULL;
  }

  void set(const char *key, const char *value)
  {
    key_ = key;
    value_.value_str = value;
  }

  void set(const char* key, int64_t value)
  {
    key_ = key;
    value_.value_int64 = value;
  }

  void set(const char* key, bool value)
  {
    key_ = key;
    value_.value_bool = value;
  }
  const char *key_;
  ObOptValue value_;
};

struct ObIODOpts
{
  ObIODOpts() : opts_(nullptr), opt_cnt_(0) {}
  ObIODOpt *opts_;
  int64_t opt_cnt_;
};

struct ObIODFileStat
{
  ObIODFileStat()
  {
    memset(this, 0x00, sizeof(ObIODFileStat));
  }
  uint64_t rdev_;        // device id (if special file)
  uint64_t dev_;         // device id
  uint64_t inode_;       // inode number
  uint32_t mask_;        // stat got mask
  uint32_t mode_;        // file type and mode
  uint32_t nlink_;       // number of hard links
  uint64_t uid_;         // user id of owner
  uint64_t gid_;         // group id of owner
  uint64_t size_;        // total size, in bytes
  uint64_t block_cnt_;   // number of 512B blocks allocated
  uint64_t block_size_;  // block size for filesystem I/O
  int64_t atime_s_;      // time of last access
  int64_t mtime_s_;      // time of last modification
  int64_t ctime_s_;      // time of last status change
  int64_t btime_s_;      // time of birth

  TO_STRING_KV(K_(rdev), K_(dev), K_(inode), K_(mask), K_(mode), K_(nlink), K_(uid), K_(gid),
    K_(size), K_(block_cnt), K_(block_size), K_(atime_s), K_(mtime_s), K_(ctime_s), K_(btime_s));
};

class ObIODPreadChecker
{
public:
  ObIODPreadChecker() = default;
  virtual ~ObIODPreadChecker() = default;
  virtual int do_check(void *read_buf, const int64_t read_size) = 0;
};

struct ObIODirentEntry {
  ObIODirentEntry() : type_(0) {}
  ObIODirentEntry(const char *name, const int type) : type_(type) {
    STRNCPY(name_, name, sizeof(name_) - 1);
  }
  TO_STRING_KV(KCSTRING_(name), K_(type));
  bool is_valid() const { return 0 != name_[0] && (DT_DIR == type_ || DT_REG == type_); }
  bool is_dir() const { return DT_DIR == type_; }
  bool is_file() const { return DT_REG == type_; }

  char name_[common::MAX_PATH_SIZE];
  int  type_;
};

class ObDirRegularEntryNameFilter : public ObBaseDirEntryOperator
{
public:
  enum FilterType
  {
    PREFIX = 1,
    KEY_WORD = 2,
    SUFFIX = 3,
    MAX,
  };
  ObDirRegularEntryNameFilter(const char *filter_str, FilterType type, common::ObIArray<ObIODirentEntry> &d_entrys)
    : type_(type), d_entrys_(d_entrys)
  {
    STRNCPY(filter_str_, filter_str, sizeof(filter_str_) - 1);
  }
  virtual ~ObDirRegularEntryNameFilter() = default;
  virtual int func(const dirent *entry) override;

private:
  char filter_str_[common::MAX_PATH_SIZE];
  FilterType type_;
  common::ObIArray<ObIODirentEntry> &d_entrys_;
};

class ObIBlockIterator
{
public:
  ObIBlockIterator() = default;
  virtual ~ObIBlockIterator() = default;
  virtual int get_next_block(ObIOFd &block_id) = 0;
};

class ObIODevice
{
public:
  ObIODevice() : device_type_(OB_STORAGE_MAX_TYPE), media_id_(0) {}
  virtual ~ObIODevice() {}
  virtual int init(const ObIODOpts &opts) = 0;
  virtual int reconfig(const ObIODOpts &opts) = 0;
  virtual int get_config(ObIODOpts &opts) = 0;
  virtual void destroy() = 0;

  virtual int start(const ObIODOpts &opts) = 0;

  //file/dir interfaces
  virtual int open(const char *pathname, const int flags, const mode_t mode,
                    ObIOFd &fd, ObIODOpts *opts= NULL) = 0;
  virtual int complete(const ObIOFd &fd) = 0;
  virtual int abort(const ObIOFd &fd) = 0;
  virtual int close(const ObIOFd &fd) = 0;
  virtual int mkdir(const char *pathname, mode_t mode) = 0;
  virtual int rmdir(const char *pathname) = 0;
  virtual int unlink(const char *pathname) = 0;
  virtual int rename(const char *oldpath, const char *newpath) = 0;
  virtual int seal_file(const ObIOFd &fd) = 0;
  virtual int scan_dir(const char *dir_name, int (*func)(const dirent *entry)) = 0;
  virtual int scan_dir(const char *dir_name, ObBaseDirEntryOperator &op) = 0;
  virtual int is_tagging(const char *pathname, bool &is_tagging) = 0;
  int scan_dir_with_prefix(
        const char *dir_name,
        const char *file_prefix,
        common::ObIArray<ObIODirentEntry> &d_entrys);
  virtual int fsync(const ObIOFd &fd) = 0;
  virtual int fdatasync(const ObIOFd &fd) = 0;
  virtual int fallocate(const ObIOFd &fd, mode_t mode, const int64_t offset, const int64_t len) = 0;
  virtual int lseek(const ObIOFd &fd, const int64_t offset, const int whence, int64_t &result_offset) = 0;
  virtual int truncate(const char *pathname, const int64_t len) = 0;
  virtual int exist(const char *pathname, bool &is_exist) = 0;
  virtual int stat(const char *pathname, ObIODFileStat &statbuf) = 0;
  virtual int fstat(const ObIOFd &fd, ObIODFileStat &statbuf) = 0;
  virtual int del_unmerged_parts(const char *pathname) = 0;
  virtual int adaptive_exist(const char *pathname, bool &is_exist) = 0;
  virtual int adaptive_stat(const char *pathname, ObIODFileStat &statbuf) = 0;
  virtual int adaptive_unlink(const char *pathname) = 0;
  virtual int adaptive_scan_dir(const char *dir_name, ObBaseDirEntryOperator &op) = 0;

  //block interfaces
  virtual int mark_blocks(ObIBlockIterator &block_iter) = 0;
  virtual int alloc_block(const ObIODOpts *opts, ObIOFd &block_id) = 0;
  virtual int alloc_blocks(
    const ObIODOpts *opts,
    const int64_t count,
    ObIArray<ObIOFd> &blocks) = 0;
  virtual void free_block(const ObIOFd &block_id) = 0;
  virtual int fsync_block() = 0;
  virtual int mark_blocks(const ObIArray<ObIOFd> &blocks) = 0;
  virtual int get_restart_sequence(uint32_t &restart_id) const = 0;

  //sync io interfaces
  virtual int pread(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    void *buf,
    int64_t &read_size,
    ObIODPreadChecker *checker = nullptr) = 0;
  virtual int pwrite(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    const void *buf,
    int64_t &write_size) = 0;
  virtual int read(
    const ObIOFd &fd,
    void *buf,
    const int64_t size,
    int64_t &read_size) = 0;
  virtual int write(
    const ObIOFd &fd,
    const void *buf,
    const int64_t size,
    int64_t &write_size) = 0;

  //async io interfaces
  virtual int io_setup(
    uint32_t max_events,
    ObIOContext *&io_context) = 0;
  virtual int io_destroy(ObIOContext *io_context) = 0;
  virtual int io_prepare_pwrite(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback) = 0;
  virtual int io_prepare_pread(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback) = 0;
  virtual int io_submit(
    ObIOContext *io_context,
    ObIOCB *iocb) = 0;
  virtual int io_cancel(
    ObIOContext *io_context,
    ObIOCB *iocb) = 0;
  virtual int io_getevents(
    ObIOContext *io_context,
    int64_t min_nr,
    ObIOEvents *events,
    struct timespec *timeout) = 0;
  virtual ObIOCB *alloc_iocb() = 0;
  virtual ObIOEvents *alloc_io_events(const uint32_t max_events) = 0;
  virtual void free_iocb(ObIOCB *iocb) = 0;
  virtual void free_io_events(ObIOEvents *io_event) = 0;

  // space management interface
  virtual int64_t get_total_block_size() const = 0;
  virtual int64_t get_free_block_count() const = 0;
  virtual int64_t get_max_block_size(int64_t reserved_size) const = 0;
  virtual int64_t get_max_block_count(int64_t reserved_size) const = 0;
  virtual int64_t get_reserved_block_count() const = 0;
  virtual int check_space_full(const int64_t required_size) const = 0;
  virtual int check_write_limited() const = 0;

public:
  ObStorageType device_type_;
  int64_t media_id_;
};

extern ObIODevice *THE_IO_DEVICE;

}
}

#endif /* DEPS_OBLIB_SRC_COMMON_STORAGE_OB_IO_DEVICE_H_ */
