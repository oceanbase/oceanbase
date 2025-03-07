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

#ifndef OCEANBASE_LIB_STORAGE_IO_DEFINE
#define OCEANBASE_LIB_STORAGE_IO_DEFINE

#include "common/storage/ob_io_device.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_rbtree.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/restore/ob_storage.h"
#include "lib/tc/ob_tc.h"
#include "lib/worker.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/ob_ss_fd_cache_struct.h"
#endif

namespace oceanbase
{
namespace share
{
class ObUnitConfig;
}
namespace backup
{
class ObBackupDeviceHelper;
}

namespace common
{

class ObObjectDevice;

// the timestamp adjustment will not adjust until the queue is idle for more than this time
static constexpr int64_t CLOCK_IDLE_THRESHOLD_US = 100 * 1000L;  // 100ms
static constexpr int64_t DEFAULT_IO_WAIT_TIME_MS = 5000L;        // 5s
static constexpr int64_t MAX_IO_WAIT_TIME_MS = 300L * 1000L;     // 5min
static constexpr int64_t GROUP_START_NUM = 3L;
static constexpr int64_t DEFAULT_IO_WAIT_TIME_US = 5000L * 1000L;  // 5s
static constexpr int64_t MAX_DETECT_READ_WARN_TIMES = 10L;
static constexpr int64_t MAX_DETECT_READ_ERROR_TIMES = 100L;
static constexpr int64_t DEFAULT_OBJECT_STORAGE_IO_TIMEOUT_MS = 20 * 1000L;

enum class ObIOMode : uint8_t { READ = 0, WRITE = 1, MAX_MODE };

enum class ObIOGroupMode : uint8_t { LOCALREAD = 0, LOCALWRITE = 1, REMOTEREAD = 2, REMOTEWRITE = 3, MODECNT };

enum class ObIOPriority : uint8_t {
  EMERGENT = 0,  // 预留
  HIGH = 1,      // 转储写、中间层索引读取
  MIDDLE = 2,    // 合并写、临时文件写
  LOW = 3        // 后台任务读写，例如CRC校验
};

const char *get_io_mode_string(const ObIOMode mode);
const char *get_io_mode_string(const ObIOGroupMode mode);
int transform_usage_index_to_group_config_index(const uint64_t &usage_index, uint64_t &group_config_index);
ObIOMode get_io_mode_enum(const char *mode_string);

enum ObIOModule {
  SYS_MODULE_START_ID = 1,
  SLOG_IO = SYS_MODULE_START_ID,
  CALIBRATION_IO,
  DETECT_IO,
  DIRECT_LOAD_IO,
  SHARED_BLOCK_RW_IO,
  SSTABLE_WHOLE_SCANNER_IO,
  INSPECT_BAD_BLOCK_IO,
  SSTABLE_INDEX_BUILDER_IO,
  BACKUP_READER_IO,
  BLOOM_FILTER_IO,
  SHARED_MACRO_BLOCK_MGR_IO,
  INDEX_BLOCK_TREE_CURSOR_IO,
  MICRO_BLOCK_CACHE_IO,
  ROOT_BLOCK_IO,
  TMP_PAGE_CACHE_IO,
  INDEX_BLOCK_MICRO_ITER_IO,
  HA_COPY_MACRO_BLOCK_IO,
  LINKED_MACRO_BLOCK_IO,
  HA_MACRO_BLOCK_WRITER_IO,
  TMP_TENANT_MEM_BLOCK_IO,
  SSTABLE_MACRO_BLOCK_WRITE_IO,
  CLOG_WRITE_IO,
  CLOG_READ_IO,
  // end
  SYS_MODULE_END_ID
};

const int64_t SYS_MODULE_CNT = SYS_MODULE_END_ID - SYS_MODULE_START_ID;

const char *get_io_sys_group_name(ObIOModule module);
struct ObIOFlag final
{
public:
  ObIOFlag();
  ~ObIOFlag();
  ObIOFlag &operator=(const ObIOFlag &other)
  {
    this->flag_ = other.flag_;
    this->func_type_ = other.func_type_;
    this->group_id_ = other.group_id_;
    this->sys_module_id_ = other.sys_module_id_;
    return *this;
  }
  void reset();
  bool is_valid() const;
  void set_mode(ObIOMode mode);
  ObIOMode get_mode() const;
  void set_wait_event(int64_t wait_event_id);
  uint64_t get_resource_group_id() const;
  void set_sys_module_id(const uint64_t sys_module_id);
  uint64_t get_sys_module_id() const;
  bool is_sys_module() const;
  uint8_t get_func_type() const;
  int64_t get_wait_event() const;
  void set_read();
  bool is_read() const;
  void set_write();
  bool is_write() const;
  void set_sync();
  void set_async();
  bool is_sync() const;
  void set_unlimited(const bool is_unlimited = true);
  bool is_unlimited() const;
  void set_detect(const bool is_detect = true);
  void set_time_detect(const bool is_time_detect = true);
  bool is_time_detect() const;
  bool is_detect() const;
  void set_priority(const ObIOPriority priority);
  ObIOPriority get_priority() const;
  // user layer please do not call this function! only allow underlying io layer call this function!
  void set_write_through(const bool is_write_through = true);
  bool is_write_through() const;
  void set_unsealed();
  void set_sealed();
  bool is_sealed() const;
  void set_dirty();
  void set_clean();
  bool is_dirty() const;
  void set_need_close_dev_and_fd();
  void set_no_need_close_dev_and_fd();
  bool is_need_close_dev_and_fd() const;
  TO_STRING_KV("mode", common::get_io_mode_string(static_cast<ObIOMode>(mode_)), K(group_id_), K(func_type_),
      K(wait_event_id_), K(is_sync_), K(is_unlimited_), K(is_detect_), K(is_write_through_), K(is_sealed_),
      K(is_time_detect_), K(need_close_dev_and_fd_), K(reserved_));

private:
  friend struct ObIOResult;
  void set_func_type(const uint8_t func_type);
  void set_resource_group_id(const uint64_t group_id);
  static constexpr int64_t IO_MODE_BIT = 4; // read, write, append
  static constexpr int64_t IO_FUNC_TYPE_BIT = 8;
  static constexpr int64_t IO_WAIT_EVENT_BIT = 32; // for performance monitor
  static constexpr int64_t IO_SYNC_FLAG_BIT = 1; // indicate if the caller is waiting io finished
  static constexpr int64_t IO_DETECT_FLAG_BIT = 1; // notify a retry task
  static constexpr int64_t IO_UNLIMITED_FLAG_BIT = 1; // indicate if the io is unlimited
  static constexpr int64_t IO_WRITE_THROUGH_BIT = 1; // indicate write mode of the io
  // indicate if the block io is sealed. only sealed block can be flushed to object storage.
  // e.g., for block append write, the first n writes should set is_sealed_ to false, and the
  // last write should set is_sealed to true. only after the last write set is_sealed to true,
  // the block can be flushed to object storage.
  static constexpr int64_t IO_SEALED_FLAG_BIT = 1;
  static constexpr int64_t IO_TIME_DETECT_FLAG_BIT = 1; // indicate if the io is unlimited
  // designed for block io under share storage mode. indicate if need to close device and fd
  // when the block io finishes. for example, local cache block io uses ObLocalCacheDevice, which
  // does not need to close device and fd. object storage block io uses ObObjectDevice, which
  // needs to close device and fd.
  static constexpr int64_t IO_CLOSE_DEV_AND_FD_BIT = 1;
  static constexpr int64_t IO_RESERVED_BIT = 64 - IO_MODE_BIT
                                                - IO_WAIT_EVENT_BIT
                                                - IO_SYNC_FLAG_BIT
                                                - IO_DETECT_FLAG_BIT
                                                - IO_UNLIMITED_FLAG_BIT
                                                - IO_WRITE_THROUGH_BIT
                                                - IO_SEALED_FLAG_BIT
                                                - IO_TIME_DETECT_FLAG_BIT
                                                - IO_CLOSE_DEV_AND_FD_BIT;

  union { // FARM COMPAT WHITELIST
    int64_t flag_;
    struct {
      int64_t mode_ : IO_MODE_BIT;
      uint8_t func_type_ : IO_FUNC_TYPE_BIT;
      int64_t wait_event_id_ : IO_WAIT_EVENT_BIT;
      bool is_sync_ : IO_SYNC_FLAG_BIT;
      bool is_unlimited_ : IO_UNLIMITED_FLAG_BIT;
      bool is_detect_ : IO_DETECT_FLAG_BIT;
      bool is_write_through_ : IO_WRITE_THROUGH_BIT;
      bool is_sealed_ : IO_SEALED_FLAG_BIT;
      bool is_time_detect_ : IO_TIME_DETECT_FLAG_BIT;
      bool need_close_dev_and_fd_ : IO_CLOSE_DEV_AND_FD_BIT;
      int64_t reserved_ : IO_RESERVED_BIT;
    };
  };
  uint64_t group_id_;
  uint64_t sys_module_id_;
};

template <typename T, typename U>
class ObIOObjectPool final
{
public:
  ObIOObjectPool();
  ~ObIOObjectPool();
  int init(const int64_t count, ObIAllocator &allocator);
  void destroy();
  int alloc(T *&ptr);
  int recycle(T *ptr);
  bool contain(T *ptr);
  int64_t get_block_size() const
  {
    return obj_size_;
  }
  int64_t get_capacity() const
  {
    return capacity_;
  }
  int64_t get_free_cnt() const
  {
    return free_count_;
  }
  TO_STRING_KV(K(is_inited_), K(obj_size_), K(capacity_), K(free_count_), KP(allocator_), KP(start_ptr_));

private:
  bool is_inited_;
  int64_t obj_size_;
  int64_t capacity_;
  int64_t free_count_;
  ObIAllocator *allocator_;
  ObFixedQueue<T> pool_;
  char *start_ptr_;
};

// different io callback types enqueue different io callback thread queue
enum class ObIOCallbackType : uint8_t {
  ATOMIC_WRITE_CALLBACK = 0,
  ASYNC_SINGLE_MICRO_BLOCK_CALLBACK = 1,
  MULTI_DATA_BLOCK_CALLBACK = 2,
  SYNC_SINGLE_MICRO_BLOCK_CALLBACK = 3,
  SS_CACHE_LOAD_FROM_REMOTE_CALLBACK = 4,
  SS_CACHE_LOAD_FROM_LOCAL_CALLBACK = 5,
  SS_MC_PREWARM_CALLBACK = 6,
  STORAGE_META_CALLBACK = 7,
  TMP_PAGE_CALLBACK = 8,
  TMP_MULTI_PAGE_CALLBACK = 9,
  TMP_DIRECT_READ_PAGE_CALLBACK = 10,
  TEST_CALLBACK = 11, // just for unittest
  SS_TMP_FILE_CALLBACK = 12,
  TMP_CACHED_READ_CALLBACK = 13,
  MAX_CALLBACK_TYPE = 14
};

bool is_atomic_write_callback(const ObIOCallbackType type);

class ObIOCallback
{
public:
  static const int64_t CALLBACK_BUF_SIZE = 1024;
  ObIOCallback(const ObIOCallbackType type);
  virtual ~ObIOCallback();

  int process(const char *data_buffer, const int64_t size);
  virtual ObIAllocator *get_allocator() = 0;
  virtual const char *get_data() = 0;
  virtual int64_t size() const = 0;
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) = 0;
  virtual int inner_process(const char *data_buffer, const int64_t size) = 0;
  ObIOCallbackType get_type() const
  {
    return type_;
  }
  DECLARE_PURE_VIRTUAL_TO_STRING;

protected:
  static int alloc_and_copy_data(
      const char *io_data_buffer, const int64_t data_size, common::ObIAllocator *allocator, char *&data_buffer);

protected:
  ObIOCallbackType type_;

private:
  friend class ObIOResult;
  lib::Worker::CompatMode compat_mode_;
};

template <typename T>
void free_io_callback(ObIOCallback *&io_callback)
{
  if (OB_NOT_NULL(io_callback) && OB_NOT_NULL(io_callback->get_allocator())) {
    ObIAllocator *allocator = io_callback->get_allocator();
    T *specific_callback = static_cast<T *>(io_callback);
    specific_callback->~T();
    allocator->free(specific_callback);
    specific_callback = nullptr;
    io_callback = nullptr;
  }
}

struct ObSNIOInfo
{
public:
  ObSNIOInfo();
  explicit ObSNIOInfo(const ObSNIOInfo &other);
  virtual ~ObSNIOInfo();
  virtual void reset();
  bool is_valid() const;
  ObSNIOInfo &operator=(const ObSNIOInfo &other);
  TO_STRING_KV(K(tenant_id_), K(fd_), K(offset_), K(size_), K(timeout_us_), K(flag_), KP(callback_), KP(buf_),
      KP(user_data_buf_), K_(part_id));

public:
  uint64_t tenant_id_;
  ObIOFd fd_;
  int64_t offset_;
  int64_t size_;
  int64_t timeout_us_;
  ObIOFlag flag_;
  ObIOCallback *callback_;
  const char *buf_;
  char *user_data_buf_;  // actual data buf without cb, allocated by the calling layer
  int64_t part_id_;      // multipart upload's part id
};

#ifdef OB_BUILD_SHARED_STORAGE
struct ObSSIOInfo : public ObSNIOInfo
{
public:
  ObSSIOInfo();
  ObSSIOInfo(const ObSSIOInfo &other);
  virtual ~ObSSIOInfo();
  virtual void reset() override;
  ObSSIOInfo &operator=(const ObSSIOInfo &other);

public:
  storage::ObSSPhysicalBlockHandle phy_block_handle_;  // hold ref_cnt
  storage::ObSSFdCacheHandle fd_cache_handle_;
  int64_t tmp_file_valid_length_;

  INHERIT_TO_STRING_KV("SNIOInfo", ObSNIOInfo, K_(phy_block_handle), K_(fd_cache_handle),  K_(tmp_file_valid_length));
};
#endif

#ifdef OB_BUILD_SHARED_STORAGE
#define ObIOInfo ObSSIOInfo
#else
#define ObIOInfo ObSNIOInfo
#endif

template <typename T>
class ObRefHolder final
{
public:
  explicit ObRefHolder() : ptr_(nullptr)
  {}
  explicit ObRefHolder(T *ptr) : ptr_(nullptr)
  {
    hold(ptr);
  }
  ~ObRefHolder()
  {
    reset();
  }
  T *get_ptr()
  {
    return ptr_;
  }
  void hold(T *ptr)
  {
    if (nullptr != ptr && ptr != ptr_) {
      ptr->inc_ref();
      reset();  // reset previous ptr, must after ptr->inc_ref()
      ptr_ = ptr;
    }
  }
  void reset()
  {
    if (nullptr != ptr_) {
      ptr_->dec_ref();
      ptr_ = nullptr;
    }
  }
  TO_STRING_KV(KP_(ptr));

private:
  T *ptr_;
};

struct ObIOTimeLog final
{
public:
  ObIOTimeLog();
  ~ObIOTimeLog();
  void reset();
  // Note: -1 represents invalid value
  TO_STRING_KV(K_(begin_ts), "enqueue_used", (enqueue_ts_ > 0 ? enqueue_ts_ - begin_ts_ : -1), "dequeue_used",
      (dequeue_ts_ > 0 ? dequeue_ts_ - enqueue_ts_ : -1), "submit_used",
      (submit_ts_ > 0 ? submit_ts_ - dequeue_ts_ : -1), "return_used", (return_ts_ > 0 ? return_ts_ - submit_ts_ : -1),
      "callback_enqueue_used", (callback_enqueue_ts_ > 0 ? callback_enqueue_ts_ - return_ts_ : -1),
      "callback_dequeue_used", (callback_dequeue_ts_ > 0 ? callback_dequeue_ts_ - callback_enqueue_ts_ : -1),
      "callback_finish_used", (callback_finish_ts_ > 0 ? callback_finish_ts_ - callback_dequeue_ts_ : -1), "end_used",
      (end_ts_ > 0 ? end_ts_ - callback_finish_ts_ : -1));

public:
  int64_t begin_ts_;
  int64_t enqueue_ts_;
  int64_t dequeue_ts_;
  int64_t submit_ts_;
  int64_t return_ts_;
  volatile int64_t callback_enqueue_ts_;
  volatile int64_t callback_dequeue_ts_;
  volatile int64_t callback_finish_ts_;
  int64_t end_ts_;
};

int64_t get_io_interval(const int64_t &end_time, const int64_t &begin_time);

struct ObIORetCode final
{
public:
  ObIORetCode();
  ObIORetCode(int io_ret, int fs_errno = 0);
  ~ObIORetCode();
  void reset();
  TO_STRING_KV(K(io_ret_), K(fs_errno_));

public:
  int io_ret_;
  int fs_errno_;
};

class ObTenantIOManager;
class ObIOChannel;
class ObIOSender;
class ObIOUsage;
class ObIORequest;

struct ObIOGroupKey
{
  ObIOGroupKey() : group_id_(0), mode_(ObIOMode::MAX_MODE)
  {}
  ObIOGroupKey(uint64_t group_id, ObIOMode mode) : group_id_(group_id), mode_(mode)
  {}
  int hash(uint64_t &res) const
  {
    res = ((uint64_t)mode_) << 56 | group_id_;
    return OB_SUCCESS;
  }
  uint64_t hash() const
  {
    return ((uint64_t)mode_) << 56 | group_id_;
  }
  bool operator==(const ObIOGroupKey &that) const
  {
    return group_id_ == that.group_id_ && mode_ == that.mode_;
  }
  ObIOGroupKey& operator=(const ObIOGroupKey &other)
  {
    if (this != &other) {
      group_id_ = other.group_id_;
      mode_ = other.mode_;
    }
    return *this;
  }
  uint64_t group_id_;
  ObIOMode mode_;
  TO_STRING_KV(K(group_id_), K(mode_));
};

struct ObIOSSGrpKey
{
  ObIOSSGrpKey() : tenant_id_(0), group_key_()
  {}
  ObIOSSGrpKey(const int64_t tenant_id, const ObIOGroupKey group_key) : tenant_id_(tenant_id), group_key_(group_key)
  {}
  uint64_t hash() const
  {
    uint64_t hash_val = static_cast<uint64_t>(tenant_id_);
    uint64_t hash_val_2 = static_cast<uint64_t>(group_key_.hash());
    hash_val = common::murmurhash(&hash_val_2, sizeof(hash_val_2), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return common::OB_SUCCESS;
  }
  bool operator==(const ObIOSSGrpKey &that) const
  {
    return tenant_id_ == that.tenant_id_ && group_key_ == that.group_key_;
  }
  ObIOSSGrpKey& operator=(const ObIOSSGrpKey &other)
  {
    if (this != &other) {
      tenant_id_ = other.tenant_id_;
      group_key_ = other.group_key_;
    }
    return *this;
  }
  ObIOMode get_mode() const { return group_key_.mode_; };
  int64_t tenant_id_;
  ObIOGroupKey group_key_;
  TO_STRING_KV(K(tenant_id_), K(group_key_));
};


class ObIOResult final
{
public:
  ObIOResult();
  ~ObIOResult();
  bool is_valid() const;
  int basic_init();  // for pool
  int init(const ObIOInfo &info);
  void reset();
  void destroy();
  void cancel();
  int wait(int64_t wait_ms);
  void finish(const ObIORetCode &ret_code, ObIORequest *req = nullptr);
  ObIOMode get_mode() const; // 2 mode : read and write
  ObIOGroupKey get_group_key() const;
  uint64_t get_sys_module_id() const;
  bool is_sys_module() const;
  int64_t get_data_size() const;
  uint64_t get_io_usage_index();
  uint64_t get_tenant_id() const;
  void inc_ref(const char *msg = nullptr);
  void dec_ref(const char *msg = nullptr);
  void inc_out_ref();
  void dec_out_ref();
  void finish_without_accumulate(const ObIORetCode &ret_code);
  void set_complete_size(const int64_t complete_size) { complete_size_ = complete_size; }
  int cal_delay_us(int64_t &prepare_delay, int64_t &schedule_delay, int64_t &submit_delay, int64_t &device_delay, int64_t &total_delay);
  int transform_group_config_index_to_usage_index(const ObIOGroupKey &key, uint64_t &usage_index);
  ObThreadCond &get_cond() { return cond_; }

  TO_STRING_KV(K(is_inited_), K(is_finished_), K(is_canceled_), K(has_estimated_), K(complete_size_), K(offset_), K(size_),
               K(timeout_us_), K(result_ref_cnt_), K(out_ref_cnt_), K(flag_), K(ret_code_), K(tenant_id_), K(tenant_io_mgr_),
               KP(user_data_buf_), KP(buf_), KP(io_callback_), K_(time_log));
  DISALLOW_COPY_AND_ASSIGN(ObIOResult);
private:
#ifdef OB_BUILD_SHARED_STORAGE
  friend class ObSSIORequest;
#endif
  friend class ObIORequest;
  friend class ObIOHandle;
  friend class ObIOFaultDetector;
  friend class ObTenantIOManager;
  friend class ObAsyncIOChannel;
  friend class ObSyncIOChannel;
  friend class ObIORunner;
  friend class backup::ObBackupDeviceHelper;
  bool is_inited_;
  bool is_finished_;
  bool is_canceled_;
  bool has_estimated_;
  bool is_object_device_req_;
  volatile int32_t result_ref_cnt_; //for io_result and io_handle
  volatile int32_t out_ref_cnt_; //for io_handle
  int64_t complete_size_;
  int64_t offset_;
  int64_t size_;
  int64_t timeout_us_;
  uint64_t tenant_id_;
  int64_t aligned_size_;
  ObRefHolder<ObTenantIOManager> tenant_io_mgr_;
  const char *buf_;
  char *user_data_buf_; //actual data buf without cb, allocated by thpe calling layer
  ObIOCallback *io_callback_;
  ObIOFlag flag_;
  ObThreadCond cond_;
public:
  ObIOTimeLog time_log_;
  ObIORetCode ret_code_;
};

class ObIORequest : public common::ObDLinkBase<ObIORequest>
{
public:
  ObIORequest();
  virtual ~ObIORequest();
  bool is_valid() const;
  int init(const ObIOInfo &info, ObIOResult *result);
  int basic_init();  // for pool
  virtual void destroy();
  virtual void reset();
  void free();
  void set_result(ObIOResult &io_result);
  bool is_canceled();
  int64_t timeout_ts() const;
  int64_t get_data_size() const;
  ObIOGroupKey get_group_key() const;
  uint64_t get_sys_module_id() const;
  bool is_sys_module() const;
  bool is_object_device_req() const;
  char *calc_io_buf();  // calc the aligned io_buf of raw_buf_, which interact with the operating system
  const ObIOFlag &get_flag() const;
  ObIOMode get_mode() const; // 2 mode
  ObIOGroupMode get_group_mode() const; // 4 mode
  ObIOCallback *get_callback() const;
  int alloc_io_buf(char *&io_buf);
  int get_buf_size() const;
  int64_t get_align_size() const;
  int64_t get_align_offset() const;
  int prepare(char *next_buffer = nullptr, int64_t next_size = 0, int64_t next_offset = 0);
  int recycle_buffer();
  int re_prepare();
  int retry_io();
  int try_alloc_buf_until_timeout(char *&io_buf);
  bool can_callback() const;
  void free_io_buffer();
  void inc_ref(const char *msg = nullptr);
  void dec_ref(const char *msg = nullptr);

  int64_t get_remained_io_timeout_us();

  TO_STRING_KV(K(is_inited_), K(tenant_id_), KP(control_block_), K(ref_cnt_), KP(raw_buf_), K(fd_), K(is_object_device_req()),
               K(trace_id_), K(retry_count_), K(tenant_io_mgr_), K_(storage_accesser),
               KPC(io_result_), K_(part_id));
private:
  friend class ObTenantIOSchedulerV2;
  friend class ObDeviceChannel;
  friend class ObIOManager;
  friend class ObIOResult;
  friend class ObIOSender;
  friend class ObIORunner;
  friend class ObMClockQueue;
  friend class ObAsyncIOChannel;
  friend class ObSyncIOChannel;
  friend class ObIOFaultDetector;
  friend class ObTenantIOManager;
  friend class ObIOUsage;
  friend class ObIOScheduler;
  friend class ObTenantIOClock;
  friend class ObTrafficControl;
  friend class backup::ObBackupDeviceHelper;
  const char *get_io_data_buf();  // get data buf for MEMCPY before io_buf recycle
  int alloc_aligned_io_buf(char *&io_buf);
  virtual int set_block_handle(const ObIOInfo &info);
  virtual int set_fd_cache_handle(const ObIOInfo &io_info);
  int hold_storage_accesser(const ObIOFd &fd, ObObjectDevice &object_device);
  int calc_io_offset_and_size_();
public:
  ObIOResult *io_result_;
  TCRequest qsched_req_;
protected:
  bool is_inited_;
  int8_t retry_count_;
  volatile int32_t ref_cnt_;
  void *raw_buf_; //actual allocated io buf
  int buf_size_;  //raw buf size
  int64_t align_size_; // align io size, use this and don't use calc_io_offset_and_size_()
  int64_t align_offset_;
  ObIOCB *control_block_;
  uint64_t tenant_id_;
  ObRefHolder<ObTenantIOManager> tenant_io_mgr_;
  ObRefHolder<ObStorageAccesser> storage_accesser_;
  ObIOFd fd_;
  ObCurTraceId::TraceId trace_id_;
  int64_t part_id_;   // multipart upload's part id
};

typedef common::ObDList<ObIORequest> IOReqList;
class ObPhyQueue final
{
public:
  ObPhyQueue();
  ~ObPhyQueue();
  bool is_stop_accept()
  {
    return stop_accept_;
  }
  int init(const int64_t index);
  void destroy();
  void reset_time_info();
  void reset_queue_info();
  void set_stop_accept()
  {
    stop_accept_ = true;
  }
  bool reach_adjust_interval();

public:
  TO_STRING_KV(K_(reservation_ts), K_(limitation_ts), K_(stop_accept), K_(last_empty_ts));
  bool is_inited_;
  bool stop_accept_;
  int64_t reservation_ts_;
  int64_t limitation_ts_;
  int64_t proportion_ts_;
  int64_t last_empty_ts_;
  int64_t queue_index_;  // index in array, INT64_MAX means other
  int64_t reservation_pos_;
  int64_t limitation_pos_;
  int64_t proportion_pos_;
  IOReqList req_list_;
};

class ObIOHandle final
{
public:
  ObIOHandle();
  ~ObIOHandle();
  ObIOHandle(const ObIOHandle &other);
  ObIOHandle &operator=(const ObIOHandle &other);
  int set_result(ObIOResult &result);
  bool is_empty() const;
  bool is_valid() const;
  OB_INLINE bool is_finished() const
  {
    return nullptr != result_ && result_->is_finished_;
  }

  int wait(const int64_t wait_timeout_ms = UINT64_MAX);
  const char *get_buffer();
  int64_t get_data_size() const;
  int64_t get_rt() const;
  int get_fs_errno(int &io_errno) const;
  void reset();
  void cancel();
  int get_io_ret() const;
  int get_io_flag(ObIOFlag &flag) const;
  int get_io_time_us(int64_t &io_time_us) const;
  int check_is_finished(bool &is_finished);
  void clear_io_callback();
  ObIOCallback *get_io_callback();
  TO_STRING_KV("io_result", to_cstring(result_));

private:
  void estimate();

private:
  ObIOResult *result_;
  bool is_traced_;
};

struct ObTenantIOConfig final
{
public:
  struct UnitConfig
  {
    UnitConfig();
    bool is_valid() const;
    TO_STRING_KV(K_(min_iops), K_(max_iops), K_(weight), K_(max_net_bandwidth), K_(net_bandwidth_weight));
    int64_t min_iops_;
    int64_t max_iops_;
    int64_t weight_;
    int64_t max_net_bandwidth_;
    int64_t net_bandwidth_weight_;
  };

  struct GroupConfig
  {
  public:
    GroupConfig();
    ~GroupConfig();
    bool is_valid() const;
    TO_STRING_KV(K_(mode), K_(deleted), K_(cleared), K_(min_percent), K_(max_percent), K_(weight_percent));

  public:
    bool deleted_;  // group被删除的标记
    bool cleared_;  // group被清零的标记，以后有新的directive就会重置
    char group_name_[common::OB_MAX_RESOURCE_PLAN_NAME_LENGTH];
    int64_t min_percent_;
    int64_t max_percent_;
    int64_t weight_percent_;
    int64_t group_id_;
    ObIOMode mode_;
  };

public:
  ObTenantIOConfig();
  explicit ObTenantIOConfig(const share::ObUnitConfig &unit_config);
  ~ObTenantIOConfig();
  void destroy();
  static const ObTenantIOConfig &default_instance();
  bool is_valid() const;
  int deep_copy(const ObTenantIOConfig &other_config);
  int parse_group_config(const char *config_str);
  int add_single_group_config(const uint64_t tenant_id, const ObIOGroupKey &key, const char *group_name,
      int64_t min_percent, int64_t max_percent, int64_t weight_percent);
  int get_group_config(const uint64_t index, int64_t &min, int64_t &max, int64_t &weight) const;
  int64_t get_callback_thread_count() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  int64_t memory_limit_;
  int64_t callback_thread_count_;
  UnitConfig unit_config_;
  ObSEArray<GroupConfig, GROUP_START_NUM * 3> group_configs_;
  bool group_config_change_;
  bool enable_io_tracer_;
  int64_t object_storage_io_timeout_ms_;
};

struct ObAtomIOClock final
{
  ObAtomIOClock() : iops_(0), last_ns_(0)
  {}
  void atom_update_reserve(const int64_t current_ts, const double iops_scale, int64_t &deadline_ts);
  void atom_update(const int64_t current_ts, const double iops_scale, int64_t &deadline_ts);
  void compare_and_update(const int64_t current_ts, const double iops_scale, int64_t &deadline_ts);
  void reset();
  TO_STRING_KV(K_(iops), K_(last_ns));
  int64_t iops_;
  int64_t last_ns_;  // the unit is nano sescond for max iops of 1 billion
};

class ObIOQueue
{
public:
  ObIOQueue()
  {}
  virtual ~ObIOQueue()
  {}
  virtual int init() = 0;
  virtual void destroy() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObMClockQueue : public ObIOQueue
{
public:
  ObMClockQueue();
  virtual ~ObMClockQueue();
  virtual int init() override;
  virtual void destroy() override;
  int get_time_info(int64_t &reservation_ts, int64_t &limitation_ts, int64_t &proportion_ts);
  int push_phyqueue(ObPhyQueue *phy_queue);
  int pop_phyqueue(ObIORequest *&req, int64_t &deadline_ts);
  TO_STRING_KV(K(is_inited_), K(r_heap_.count()), K(l_heap_.count()), K(ready_heap_.count()));

  int remove_from_heap(ObPhyQueue *phy_queue);

private:
  int pop_with_ready_queue(const int64_t current_ts, ObIORequest *&req, int64_t &deadline_ts);

  template <typename T, int64_t T::*member_ts, IOReqList T::*list>
  struct HeapCompare
  {
    int get_error_code()
    {
      return OB_SUCCESS;
    }
    bool operator()(const T *left, const T *right) const
    {
      return left->*member_ts != right->*member_ts ? left->*member_ts > right->*member_ts
             : (left->*list).get_size() != (right->*list).get_size()
                 ? (left->*list).get_size() < (right->*list).get_size()
                 : (int64_t)left > (int64_t)right;
    }
  };

private:
  static const int64_t POP_MORE_PHY_QUEUE_USEC = 1000;  // 1ms
private:
  bool is_inited_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::reservation_ts_, &ObPhyQueue::req_list_> r_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::limitation_ts_, &ObPhyQueue::req_list_> l_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::proportion_ts_, &ObPhyQueue::req_list_> p_cmp_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::reservation_ts_, &ObPhyQueue::req_list_>,
      &ObPhyQueue::reservation_pos_>
      r_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::limitation_ts_, &ObPhyQueue::req_list_>,
      &ObPhyQueue::limitation_pos_>
      l_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::proportion_ts_, &ObPhyQueue::req_list_>,
      &ObPhyQueue::proportion_pos_>
      ready_heap_;
};

template <typename T, typename U>
ObIOObjectPool<T, U>::ObIOObjectPool()
    : is_inited_(false), obj_size_(0), capacity_(0), free_count_(0), allocator_(nullptr), start_ptr_(nullptr)
{
  static_assert(std::is_base_of<T, U>::value, "U must be T or subclass of T");
}

template <typename T, typename U>
ObIOObjectPool<T, U>::~ObIOObjectPool()
{
  static_assert(std::is_base_of<T, U>::value, "U must be T or subclass of T");
  destroy();
}

template <typename T, typename U>
int ObIOObjectPool<T, U>::init(const int64_t count, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_base_of<T, U>::value, "U must be T or subclass of T");
  int64_t size = sizeof(U);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(count));
  } else {
    obj_size_ = size;
    allocator_ = &allocator;
    capacity_ = count;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(pool_.init(capacity_, allocator_))) {
    COMMON_LOG(WARN, "fail to init memory pool", K(ret));
  } else if (OB_ISNULL(start_ptr_ = reinterpret_cast<char *>(allocator_->alloc(capacity_ * size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "fail to allocate IOobj memory", K(ret), K(capacity_), K(size));
  } else {
    void *buf = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < capacity_; ++i) {
      buf = start_ptr_ + i * size;
      U *u = nullptr;
      u = new (reinterpret_cast<U *>(buf)) U;
      if (OB_FAIL(u->basic_init())) {
        COMMON_LOG(WARN, "basic init failed", K(ret), K(i));
      } else if (OB_FAIL(pool_.push(u))) {
        COMMON_LOG(WARN, "fail to push IOobj block to pool", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    free_count_ = capacity_;
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

template <typename T, typename U>
void ObIOObjectPool<T, U>::destroy()
{
  static_assert(std::is_base_of<T, U>::value, "U must be T or subclass of T");
  is_inited_ = false;
  free_count_ = 0;
  pool_.destroy();

  if (nullptr != allocator_) {
    if (nullptr != start_ptr_) {
      allocator_->free(start_ptr_);
      start_ptr_ = nullptr;
    }
  }
  allocator_ = nullptr;
}

template <typename T, typename U>
int ObIOObjectPool<T, U>::alloc(T *&ptr)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_base_of<T, U>::value, "U must be T or subclass of T");
  ptr = nullptr;
  T *ret_ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(pool_.pop(ret_ptr))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to pop IOobj", K(ret), K(capacity_), K(obj_size_));
    }
  } else {
    ATOMIC_DEC(&free_count_);
    ptr = ret_ptr;
  }
  return ret;
}

template <typename T, typename U>
int ObIOObjectPool<T, U>::recycle(T *ptr)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_base_of<T, U>::value, "U must be T or subclass of T");
  const int64_t idx = ((char *)ptr - start_ptr_) / obj_size_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (nullptr == ptr || idx < 0 || idx >= capacity_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(idx), KP(ptr));
  } else {
    if (OB_FAIL(pool_.push(ptr))) {
      COMMON_LOG(WARN, "fail to push IOobj", K(ret), K(capacity_), K(obj_size_));
    } else {
      ATOMIC_INC(&free_count_);
    }
  }
  return ret;
}

template <typename T, typename U>
bool ObIOObjectPool<T, U>::contain(T *ptr)
{
  static_assert(std::is_base_of<T, U>::value, "U must be T or subclass of T");
  bool bret = (uint64_t)ptr > 0 && ((char *)ptr >= start_ptr_) &&
              ((char *)ptr < start_ptr_ + (capacity_ * obj_size_)) && (((char *)ptr - start_ptr_) % obj_size_ == 0);
  return bret;
}

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_LIB_STORAGE_IO_DEFINE
