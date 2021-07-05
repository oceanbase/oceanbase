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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_SUBMIT_LOG_CB_
#define OCEANBASE_TRANSACTION_OB_TRANS_SUBMIT_LOG_CB_

#include "lib/list/ob_dlist.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "common/ob_partition_key.h"
#include "clog/ob_partition_log_service.h"
#include "storage/ob_storage_log_type.h"
#include "share/config/ob_server_config.h"
#include "ob_trans_define.h"
#include "ob_trans_log.h"
#include "ob_trans_event.h"

namespace oceanbase {
namespace clog {
class ObISubmitLogCb;
}

namespace common {
class ObPartitionKey;
}

namespace transaction {
class ObTransService;
class ObCoordTransCtx;
class ObPartTransCtx;
}  // namespace transaction

namespace transaction {

class ObITransSubmitLogCb : public clog::ObISubmitLogCb {
public:
  virtual int on_submit_log_success(
      const bool with_need_update_version, const uint64_t cur_log_id, const int64_t cur_log_timestamp) = 0;
  virtual int on_submit_log_fail(const int retcode) = 0;
  virtual int set_real_submit_timestamp(const int64_t timestamp) = 0;
  virtual int64_t get_log_type() const = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
  virtual void reset()
  {
    clog::ObISubmitLogCb::reset();
  }
};

class ObTransSubmitLogCb : public ObITransSubmitLogCb {
public:
  ObTransSubmitLogCb()
  {
    reset();
  }
  ~ObTransSubmitLogCb()
  {
    destroy();
  }
  int init(ObTransService* ts, const common::ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx* ctx);
  void reset();
  void destroy()
  {
    reset();
  }
  int set_log_type(const int64_t log_type);
  int64_t get_log_type() const
  {
    return log_type_;
  };
  int set_real_submit_timestamp(const int64_t timestamp);
  int set_have_prev_trans(const bool have_prev_trans)
  {
    have_prev_trans_ = have_prev_trans;
    return common::OB_SUCCESS;
  }
  ObTransCtx* get_ctx()
  {
    return ctx_;
  }

public:
  int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType clog_log_type,
      const uint64_t log_id, const int64_t timestamp, const bool batch_committed, const bool batch_first_participant);
  int on_submit_log_success(
      const bool with_need_update_version, const uint64_t cur_log_id, const int64_t cur_log_timestamp);
  int on_submit_log_fail(const int retcode);
  bool is_callbacking() const
  {
    return is_callbacking_;
  }

private:
  int retry_redo_sync_task_(uint64_t log_id);

public:
  TO_STRING_KV(KP_(ts), K_(partition), K_(trans_id), K_(log_type), K_(submit_timestamp), KP_(ctx), K_(have_prev_trans));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransSubmitLogCb);

private:
  bool is_inited_;
  ObTransService* ts_;
  common::ObPartitionKey partition_;
  ObTransID trans_id_;
  int64_t log_type_;
  int64_t submit_timestamp_;
  ObTransCtx* ctx_;
  bool have_prev_trans_;
  bool is_callbacking_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_SUBMIT_LOG_CB_
