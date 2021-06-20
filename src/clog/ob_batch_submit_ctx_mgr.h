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

#ifndef OCEANBASE_CLOG_OB_BATCH_SUBMIT_CTX_MGR_H_
#define OCEANBASE_CLOG_OB_BATCH_SUBMIT_CTX_MGR_H_
#include "lib/hash/ob_link_hashmap.h"
#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "share/ob_thread_pool.h"
#include "ob_batch_submit_ctx.h"
#include "ob_log_common.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
}
namespace transaction {
class ObTransID;
}
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObILogEngine;
class ObIBatchSubmitCtxMgr {
public:
  ObIBatchSubmitCtxMgr()
  {}
  virtual ~ObIBatchSubmitCtxMgr()
  {}

public:
  virtual int alloc_ctx(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObLogPersistSizeArray& size_array,
      const ObISubmitLogCbArray& cb_array, const common::ObMemberList& member_list, const int64_t replica_num,
      const common::ObAddr& leader, common::ObILogAllocator* alloc_mgr, ObBatchSubmitCtx*& ctx) = 0;
  virtual int alloc_ctx(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObLogPersistSizeArray& size_array, const common::ObAddr& leader,
      common::ObILogAllocator* alloc_mgr, ObBatchSubmitCtx*& ctx) = 0;
  virtual int get_ctx(const transaction::ObTransID& trans_id, ObBatchSubmitCtx*& ctx) = 0;
  virtual int revert_ctx(ObBatchSubmitCtx* ctx) = 0;
  virtual int free_ctx(const transaction::ObTransID& trans_id) = 0;
};

class ObBatchSubmitCtxMgr : public ObIBatchSubmitCtxMgr, public share::ObThreadPool {
public:
  ObBatchSubmitCtxMgr()
  {
    reset();
  }
  virtual ~ObBatchSubmitCtxMgr()
  {
    destroy();
  }

public:
  int init(storage::ObPartitionService* partition_service, ObILogEngine* log_engine, const common::ObAddr& self);
  int start();
  void stop();
  void wait();

  void reset();
  void destroy();

public:
  virtual int alloc_ctx(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObLogPersistSizeArray& size_array,
      const ObISubmitLogCbArray& cb_array, const common::ObMemberList& member_list, const int64_t replica_num,
      const common::ObAddr& leader, common::ObILogAllocator* alloc_mgr, ObBatchSubmitCtx*& ctx);
  virtual int alloc_ctx(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObLogPersistSizeArray& size_array, const common::ObAddr& leader,
      common::ObILogAllocator* alloc_mgr, ObBatchSubmitCtx*& ctx);
  virtual int get_ctx(const transaction::ObTransID& trans_id, ObBatchSubmitCtx*& ctx);
  virtual int revert_ctx(ObBatchSubmitCtx* ctx);
  virtual int free_ctx(const transaction::ObTransID& trans_id);

  void run1();

private:
  typedef common::ObLinkHashMap<transaction::ObTransID, ObBatchSubmitCtx, ObBatchSubmitCtxAlloc> CtxMap;
  class RemoveIfFunctor;
  static const int64_t LOOP_INTERVAL = 200 * 1000;  // 200ms
  bool is_inited_;
  bool is_running_;
  CtxMap ctx_map_;
  storage::ObPartitionService* partition_service_;
  ObILogEngine* log_engine_;
  common::ObAddr self_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchSubmitCtxMgr);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_BATCH_SUBMIT_CTX_MGR_H_
