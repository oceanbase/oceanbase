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

#ifndef OCEANBASE_CLOG_OB_I_SUBMIT_LOG_CB_H_
#define OCEANBASE_CLOG_OB_I_SUBMIT_LOG_CB_H_
#include "common/ob_partition_key.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
class ObISubmitLogCb {
public:
  ObISubmitLogCb() : next_(NULL), submit_timestamp_(0), need_post_cb_(false), is_high_priority_(false)
  {}
  virtual ~ObISubmitLogCb()
  {
    next_ = NULL;
    submit_timestamp_ = 0;
    need_post_cb_ = false;
    is_high_priority_ = false;
  }

public:
  // batch_committed:
  //   For distributed transactions of batch commit type, this value is used to notify the upper-layer transaction
  //   whether the commit is successful batch_committed = true
  //     The transaction commits successfully and the transaction context can be released.
  //   batch_committed = false
  //    The transaction is not submitted successfully, only this log synchronization is successful, and the transaction
  //    needs to continue to advance the two-phase submission according to the 2PC process.
  //
  // batch_last_succeed:
  //   For distributed transactions of the batchcommit type, this value is used to notify the upper transaction whether
  //   it is the last partition callback, and the on_success of all previous partitions are called successfully.
  virtual int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type,
      const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed) = 0;
  virtual int on_success_post(const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t version,
      const bool batch_committed, const bool batch_last_succeed)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(partition_key);
    UNUSED(log_id);
    UNUSED(version);
    UNUSED(batch_committed);
    UNUSED(batch_last_succeed);
    return ret;
  }
  virtual int on_finished(const common::ObPartitionKey& partition_key, const uint64_t log_id)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(partition_key);
    UNUSED(log_id);
    return ret;
  }
  virtual void reset()
  {
    base_class_reset();
  }
  void base_class_reset()
  {
    next_ = NULL;
    submit_timestamp_ = 0;
    need_post_cb_ = false;
  }
  void set_need_post_cb()
  {
    need_post_cb_ = true;
  }
  bool need_post_cb() const
  {
    return need_post_cb_;
  }
  void set_high_priority()
  {
    is_high_priority_ = true;
  }
  bool is_high_priority() const
  {
    return is_high_priority_;
  }

public:
  ObISubmitLogCb* next_;
  int64_t submit_timestamp_;
  bool need_post_cb_;
  bool is_high_priority_;
  TO_STRING_EMPTY();
};

class ObSubmitLogDummyCb : public ObISubmitLogCb {
public:
  ObSubmitLogDummyCb()
  {}
  ~ObSubmitLogDummyCb()
  {}

public:
  int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed)
  {
    UNUSED(partition_key);
    UNUSED(log_type);
    UNUSED(log_id);
    UNUSED(version);
    UNUSED(batch_committed);
    UNUSED(batch_last_succeed);
    return common::OB_SUCCESS;
  }
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_I_SUBMIT_LOG_CB_H_
