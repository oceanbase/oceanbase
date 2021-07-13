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

#ifndef OCEANBASE_CLOG_OB_LOG_COMMON_
#define OCEANBASE_CLOG_OB_LOG_COMMON_

#include <stdint.h>
#include <libaio.h>
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/oblog/ob_base_log_writer.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
class ObICLogItem : public common::ObIBaseLogItem {
public:
  ObICLogItem()
  {}
  virtual ~ObICLogItem()
  {}
  virtual bool is_valid() const = 0;
  virtual char* get_buf() = 0;
  virtual const char* get_buf() const = 0;
  virtual int64_t get_data_len() const = 0;
  virtual int after_flushed(
      const file_id_t file_id, const offset_t offset, const int error_code, const ObLogWritePoolType type) = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
};

// The new file will write ObNewLogFileBuf in the file header to avoid reading the old block when reusing log files
class ObNewLogFileBuf {
public:
  ObNewLogFileBuf()
  {}
  ~ObNewLogFileBuf()
  {}
  static char buf_[CLOG_DIO_ALIGN_SIZE] __attribute__((aligned(CLOG_DIO_ALIGN_SIZE)));

private:
  DISALLOW_COPY_AND_ASSIGN(ObNewLogFileBuf);
};

template <typename T>
struct TSI {
public:
  TSI() : create_err_(0), key_()
  {
    create_err_ = pthread_key_create(&key_, release);
  }
  ~TSI()
  {
    if (0 == create_err_) {
      (void)pthread_key_delete(key_);
    }
  }
  T* get()
  {
    int err = 0;
    T* val = NULL;
    if (create_err_ != 0) {
      CLOG_LOG(WARN, "pthread_key create error before", K(create_err_));
    } else if (NULL != (val = (T*)pthread_getspecific(key_))) {
      // good
    } else if (NULL == (val = new (std::nothrow) T())) {
      CLOG_LOG(WARN, "alloc value error", KP(val));
    } else if (0 != (err = pthread_setspecific(key_, val))) {
      CLOG_LOG(WARN, "pthead set specific error", K(err), K(key_));
    }
    if (0 != err) {
      release(val);
      val = NULL;
    }
    return val;
  }

private:
  static void release(void* arg)
  {
    if (NULL != arg) {
      delete (T*)arg;
    }
  }
  int create_err_;
  pthread_key_t key_;
};

inline common::ObQSync& get_log_file_qs()
{
  // wait on log_file_qs before reuse
  static common::ObQSync log_file_qs;
  return log_file_qs;
}

int close_fd(const int fd);

class ObLogMeta {
public:
  ObLogMeta() : log_id_(common::OB_INVALID_ID), submit_timestamp_(common::OB_INVALID_TIMESTAMP), proposal_id_()
  {}
  ~ObLogMeta()
  {}
  void reset()
  {
    log_id_ = common::OB_INVALID_ID;
    submit_timestamp_ = common::OB_INVALID_TIMESTAMP;
    proposal_id_.reset();
  }
  int set(const uint64_t log_id, const int64_t submit_timestamp, const common::ObProposalID& proposal_id)
  {
    int ret = common::OB_SUCCESS;
    if (common::OB_INVALID_ID == log_id || common::OB_INVALID_TIMESTAMP == submit_timestamp ||
        !proposal_id.is_valid()) {
      ret = common::OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(ret), K(log_id), K(submit_timestamp), K(proposal_id));
    } else {
      log_id_ = log_id;
      submit_timestamp_ = submit_timestamp;
      proposal_id_ = proposal_id;
    }
    return ret;
  }
  bool is_valid() const
  {
    return common::OB_INVALID_ID != log_id_ && common::OB_INVALID_TIMESTAMP != submit_timestamp_ &&
           proposal_id_.is_valid();
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  int64_t get_submit_timestamp() const
  {
    return submit_timestamp_;
  }
  const common::ObProposalID& get_proposal_id() const
  {
    return proposal_id_;
  }

public:
  TO_STRING_KV(K(log_id_), K(submit_timestamp_), K(proposal_id_));

private:
  uint64_t log_id_;
  int64_t submit_timestamp_;
  common::ObProposalID proposal_id_;
};

class ObLogInfo {
  OB_UNIS_VERSION(1);

public:
  ObLogInfo()
  {
    reset();
  }
  ~ObLogInfo()
  {
    reset();
  }

public:
  void reset()
  {
    buff_ = NULL;
    size_ = 0;
    log_id_ = common::OB_INVALID_ID;
    submit_timestamp_ = common::OB_INVALID_TIMESTAMP;
    proposal_id_.reset();
    // true on purpose, if no need reset externally
    need_flushed_ = true;
  }
  int set(const char* buff, const int64_t size, const uint64_t log_id, const int64_t submit_timestamp,
      const common::ObProposalID& proposal_id, const bool need_flushed)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buff) || size <= 0 || common::OB_INVALID_ID == log_id ||
        common::OB_INVALID_TIMESTAMP == submit_timestamp || !proposal_id.is_valid()) {
      ret = common::OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(ret), KP(buff), K(size), K(log_id), K(submit_timestamp), K(proposal_id));
    } else {
      buff_ = buff;
      size_ = size;
      log_id_ = log_id;
      submit_timestamp_ = submit_timestamp;
      proposal_id_ = proposal_id;
      need_flushed_ = need_flushed;
    }
    return ret;
  }
  int set(const char* buff, const int64_t size, const ObLogMeta& log_meta, const bool need_flushed)
  {
    return set(
        buff, size, log_meta.get_log_id(), log_meta.get_submit_timestamp(), log_meta.get_proposal_id(), need_flushed);
  }
  const char* get_buf() const
  {
    return buff_;
  }
  int64_t get_size() const
  {
    return size_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  int64_t get_submit_timestamp() const
  {
    return submit_timestamp_;
  }
  const common::ObProposalID& get_proposal_id() const
  {
    return proposal_id_;
  }
  bool is_valid() const
  {
    return NULL != buff_ && size_ > 0 && common::OB_INVALID_ID != log_id_ &&
           common::OB_INVALID_TIMESTAMP != submit_timestamp_ && proposal_id_.is_valid();
  }
  void reset_need_flushed()
  {
    need_flushed_ = false;
  }
  bool is_need_flushed() const
  {
    return need_flushed_;
  }
  TO_STRING_KV(K(size_), K(log_id_), K(submit_timestamp_), K(proposal_id_), K(need_flushed_));

private:
  const char* buff_;
  int64_t size_;
  uint64_t log_id_;
  int64_t submit_timestamp_;
  common::ObProposalID proposal_id_;
  bool need_flushed_;
};

typedef common::ObSEArray<ObLogInfo, 10> ObLogInfoArray;
typedef common::ObSEArray<bool, 16> ObBatchAckArray;
typedef common::ObSEArray<int32_t, 10> ObLogPersistSizeArray;
// used for one phase commit transactions's rpc arg
#define DUMMY_PARTITION_ID common::ObPartitionKey(1000, 1000, 1000)

int64_t aio_write(const int fd, void* buf, const int64_t count, const int64_t offset, const int64_t timeout, iocb& iocb,
    io_context_t& ctx, io_event& ioevent);
}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_COMMON_
