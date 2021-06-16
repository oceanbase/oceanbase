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

#ifndef OCEANBASE_CLOG_OB_LOG_CALLBACK_ENGINE_
#define OCEANBASE_CLOG_OB_LOG_CALLBACK_ENGINE_

#include "lib/allocator/ob_allocator.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "ob_log_callback_task.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace common {
class ObSimpleThreadPool;
}
namespace clog {
class ObILogCallbackEngine {
public:
  ObILogCallbackEngine()
  {}
  virtual ~ObILogCallbackEngine()
  {}

public:
  virtual void destroy() = 0;
  virtual int submit_member_change_success_cb_task(const common::ObPartitionKey& partition_key,
      const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list,
      const common::ObProposalID& ms_proposal_id) = 0;
  virtual int submit_pop_task(const common::ObPartitionKey& partition_key) = 0;
};

class ObLogCallbackEngine : public ObILogCallbackEngine, public common::ObICallbackHandler {
public:
  ObLogCallbackEngine() : is_inited_(false), clog_tg_id_(-1), sp_tg_id_(-1)
  {}
  virtual ~ObLogCallbackEngine()
  {
    destroy();
  }

public:
  virtual int init(int clog_tg_id, int sp_tg_id);
  virtual void destroy();
  virtual int handle_callback(common::ObICallback* callback);
  virtual int submit_member_change_success_cb_task(const common::ObPartitionKey& partition_key,
      const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list,
      const common::ObProposalID& ms_proposal_id);
  virtual int submit_pop_task(const common::ObPartitionKey& partition_key);

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogCallbackEngine);

private:
  bool is_inited_;
  int clog_tg_id_;
  int sp_tg_id_;
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_CALLBACK_ENGINE_
