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

#ifndef OB_IO_REQUEST_H
#define OB_IO_REQUEST_H

#include "lib/profile/ob_trace_id.h"
#include "lib/io/ob_io_common.h"
#include "lib/container/ob_bit_set.h"
#include "lib/io/ob_io_disk.h"

namespace oceanbase {
namespace common {

class ObIOMaster;
class ObDisk;
class ObIOChannel;
class ObIOResourceManager;
class ObIIOErrorHandler;

struct ObIORequestDesc {
public:
  ObIORequestDesc() : fd_(), io_size_(0), io_buf_(NULL), io_offset_(0)
  {}
  ~ObIORequestDesc()
  {}
  void reset()
  {
    *this = ObIORequestDesc();
  }
  bool is_valid() const;
  TO_STRING_KV(K_(desc), K_(fd), K_(io_size), KP_(io_buf), K_(io_offset));

public:
  ObIODesc desc_;
  ObDiskFd fd_;
  int32_t io_size_;
  char* io_buf_;
  int64_t io_offset_;
};

class ObIORequest : public ObDLinkBase<ObIORequest> {
public:
  ObIORequest()
  {
    destroy();
  }
  virtual ~ObIORequest()
  {}
  int init()
  {
    return common::OB_SUCCESS;
  }
  void destroy();
  int open(const ObIOMode mode, const ObIORequestDesc& req_param, ObIOMaster& master, ObDisk& disk);
  int finish(const int io_ret, const int sys_errno);
  void report_diagnose_info();
  ObDisk* get_disk()
  {
    return disk_guard_.get_disk();
  }
  bool can_retry() const;
  TO_STRING_KV(K_(inited), K_(need_submit), K_(finished), K_(desc), K_(fd), KP_(io_buf), K_(io_offset), K_(io_size),
      K_(deadline_time), K_(io_time), K_(ret_code), KP_(master), K_(disk_guard), KP_(channel), K_(retry_cnt));

public:
  struct TimeLog {
  public:
    TimeLog() : begin_time_(0), enqueue_time_(0), dequeue_time_(0), os_submit_time_(0), os_return_time_(0), end_time_(0)
    {}
    void reset()
    {
      *this = TimeLog();
    }
    TO_STRING_KV(
        K_(begin_time), K_(enqueue_time), K_(dequeue_time), K_(os_submit_time), K_(os_return_time), K_(end_time));

  public:
    int64_t begin_time_;
    int64_t enqueue_time_;
    int64_t dequeue_time_;
    int64_t os_submit_time_;
    int64_t os_return_time_;
    int64_t end_time_;
  };

public:
  bool inited_;
  bool need_submit_;
  bool finished_;
  struct iocb iocb_;
  ObIODesc desc_;
  ObDiskFd fd_;
  char* io_buf_;
  int64_t io_offset_;
  int32_t io_size_;
  int64_t deadline_time_;
  TimeLog io_time_;
  ObIORetCode ret_code_;
  ObIOMaster* master_;
  ObDiskGuard disk_guard_;
  ObIOChannel* channel_;
  int64_t disk_id_;
  int16_t retry_cnt_;
};

// general control
class ObIOMaster {
public:
  ObIOMaster();
  virtual ~ObIOMaster();
  int init();
  int open(const ObIOMode mode, const ObIOInfo& info, ObIOCallback* callback, ObIOResourceManager* resource_mgr,
      ObIAllocator* allocator);
  int open(const ObIOMode mode, const ObIOInfo& info, ObIOCallback* read_callback);
  void cancel();
  void reset();
  int send_request();
  int recv_request();
  int notify_finished();
  bool can_callback() const
  {
    return NULL != callback_ && NULL != buf_;
  }
  void inc_ref();
  void dec_ref();
  void inc_out_ref();
  void dec_out_ref();
  const ObCurTraceId::TraceId& get_trace_id() const
  {
    return trace_id_;
  }
  int send_callback();
  TO_STRING_KV(K_(inited), K_(need_callback), K_(has_finished), K_(has_estimated), K_(io_info), KP_(callback), KP_(buf),
      K_(buf_size), K_(aligned_offset), K_(io_ret), K_(finish_count), K_(io_ret), K_(finish_count), K_(time),
      K_(ref_cnt), K_(out_ref_cnt), KP_(resource_mgr), KP_(io_error_handler), K_(parent_io_master_holder),
      K_(recover_io_master_holder), K_(callback), K(ObArrayWrap<ObIORequest*>(requests_, MAX_IO_BATCH_NUM)));

private:
  int alloc_read_buf(ObIOCallback* callback);
  int alloc_write_buf();
  int alloc_write_buf(ObIOCallback* callback);
  int prepare_request(const ObIOMode mode);
  int alloc_io_error_handler(const ObIIOErrorHandler* io_error_handler);
  int init_recover_io_master();

public:
  struct TimeLog {
  public:
    TimeLog()
        : begin_time_(0),
          prepare_delay_(0),
          send_time_(0),
          recv_time_(0),
          callback_enqueue_time_(0),
          callback_dequeue_time_(0),
          callback_delay_(0),
          end_time_(0)
    {}
    void reset()
    {
      *this = TimeLog();
    }
    TO_STRING_KV(K_(begin_time), K_(prepare_delay), K_(send_time), K_(recv_time), K_(callback_enqueue_time),
        K_(callback_dequeue_time), K_(callback_delay), K_(end_time));

  public:
    int64_t begin_time_;
    int64_t prepare_delay_;
    int64_t send_time_;
    int64_t recv_time_;
    int64_t callback_enqueue_time_;
    int64_t callback_dequeue_time_;
    int64_t callback_delay_;
    int64_t end_time_;
  };

public:
  bool inited_;
  bool need_callback_;
  bool has_finished_;
  bool has_estimated_;
  ObIOInfo io_info_;
  ObIOCallback* callback_;
  char* buf_;               // buf include align
  int64_t buf_size_;        // size of buf
  int64_t aligned_offset_;  // aligned offset in macro block
  int io_ret_;              // return code
  int32_t finish_count_;
  ObIORequest* requests_[MAX_IO_BATCH_NUM];
  TimeLog time_;
  volatile int64_t ref_cnt_;
  volatile int64_t out_ref_cnt_;  // only for ObIOHandle, when handle reset, cancel IOMaster.
  ObThreadCond cond_;
  ObIOResourceManager* resource_mgr_;
  ObCurTraceId::TraceId trace_id_;
  ObIAllocator* allocator_;
  ObIIOErrorHandler* io_error_handler_;
  ObPointerHolder<ObIOMaster> parent_io_master_holder_;
  ObPointerHolder<ObIOMaster> recover_io_master_holder_;
  ObIOMode mode_;
  char callback_buf_[ObIOCallback::CALLBACK_BUF_SIZE] __attribute__((aligned(16)));
};

typedef ObPointerHolder<ObIOMaster> MasterHolder;

class ObIIOErrorHandler {
public:
  virtual ~ObIIOErrorHandler()
  {}
  virtual int64_t get_deep_copy_size() const = 0;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIIOErrorHandler*& handler) const = 0;
  virtual int init_recover_io_master(
      const ObBitSet<OB_MAX_DISK_NUMBER>& recover_index_set, ObIAllocator* allocator, ObIOMaster* io_master) = 0;
  virtual int set_read_io_buf(char* io_buf, const int64_t io_buf_size, const int64_t aligned_offset) = 0;
  virtual int get_recover_request_num(int64_t& recover_request_num) const = 0;
};
} /* namespace common */
} /* namespace oceanbase */
#endif
