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

#ifndef OCEANBASE_CLOG_OB_BATCH_SUBMIT_CTX_H_
#define OCEANBASE_CLOG_OB_BATCH_SUBMIT_CTX_H_
#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "ob_log_common.h"
#include "ob_log_define.h"
#include "ob_simple_member_list.h"
#include "ob_i_submit_log_cb.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
}
namespace storage {
class ObPartitionService;
}
namespace clog {
typedef common::ObSEArray<ObISubmitLogCb*, 10> ObISubmitLogCbArray;
typedef common::ObSEArray<ObLogCursor, 16> ObLogCursorArray;
class ObILogEngine;
typedef common::LinkHashNode<transaction::ObTransID> BatchSubmitCtxHashNode;
typedef common::LinkHashValue<transaction::ObTransID> BatchSubmitCtxHashValue;

class ObBatchSubmitCtx : public BatchSubmitCtxHashValue {
public:
  ObBatchSubmitCtx()
  {
    reset();
  }
  ~ObBatchSubmitCtx()
  {
    destroy();
  }

public:
  int init(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObLogPersistSizeArray& size_array,
      const ObISubmitLogCbArray& cb_array, const common::ObMemberList& member_list, const int64_t replica_num,
      const common::ObAddr& leader, const common::ObAddr& self, storage::ObPartitionService* partition_service,
      ObILogEngine* log_engine, common::ObILogAllocator* alloc_mgr);
  int init(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObLogPersistSizeArray& size_array, const common::ObAddr& leader,
      const common::ObAddr& self, storage::ObPartitionService* partition_service, ObILogEngine* log_engine,
      common::ObILogAllocator* alloc_mgr);
  void reset();
  void destroy();
  int try_split();
  int flush_cb(const ObLogCursor& base_log_cursor);
  int ack_log(const common::ObAddr& server, const ObBatchAckArray& batch_ack_array);
  TO_STRING_KV(K(trans_id_), K(partition_array_), K(log_info_array_), K(log_persist_size_array_), K(member_list_),
      K(replica_num_), K(leader_), K(self_));

private:
  int make_cursor_array_(const ObLogCursor& base_log_cursor);
  int backfill_log_(const bool is_leader, ObBatchAckArray& batch_ack_array);
  int backfill_log_(const common::ObPartitionKey& partition_key, const ObLogInfo& log_info,
      const ObLogCursor& log_cursor, const bool is_leader, ObISubmitLogCb* submit_cb);
  bool is_leader_() const;
  bool is_majority_() const;
  int backfill_confirmed_();
  int backfill_confirmed_(
      const common::ObPartitionKey& partition_key, const ObLogInfo& log_info, const bool batch_first_participant);
  int split_();
  int split_with_lock_();
  int split_(const common::ObPartitionKey& partition_key, const ObLogInfo& log_info, ObISubmitLogCb* submit_cb);
  bool need_split_() const;
  bool ack_need_split_() const;
  int handle_ack_array_(const common::ObAddr& server, const ObBatchAckArray& batch_ack_array);

private:
  static const int64_t SPLIT_INTERVAL = 500 * 1000;  // 500ms
  bool is_inited_;
  mutable lib::ObMutex lock_;
  transaction::ObTransID trans_id_;
  common::ObPartitionArray partition_array_;
  ObLogInfoArray log_info_array_;
  ObISubmitLogCbArray cb_array_;
  common::ObMemberList member_list_;
  int64_t replica_num_;
  common::ObAddr leader_;
  common::ObAddr self_;
  storage::ObPartitionService* partition_service_;
  ObILogEngine* log_engine_;
  common::ObILogAllocator* alloc_mgr_;

  bool is_splited_;
  bool is_flushed_;
  ObSimpleMemberListArray ack_list_array_;
  int64_t ack_cnt_;
  ObLogCursorArray log_cursor_array_;
  int64_t create_ctx_ts_;
  ObLogPersistSizeArray log_persist_size_array_;  // record clog size in file
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchSubmitCtx);
};

class ObBatchSubmitCtxFactory {
public:
  static ObBatchSubmitCtx* alloc(common::ObILogAllocator* alloc_mgr);
  static void free(ObBatchSubmitCtx* ctx);
  static void statistics();

private:
  static int64_t alloc_cnt_;
  static int64_t free_cnt_;
};

class ObBatchSubmitCtxAlloc {
public:
  ObBatchSubmitCtx* alloc_value()
  {
    return NULL;
  }
  void free_value(ObBatchSubmitCtx* ctx)
  {
    if (NULL != ctx) {
      ctx->destroy();
      ObBatchSubmitCtxFactory::free(ctx);
      ctx = NULL;
    }
  }
  BatchSubmitCtxHashNode* alloc_node(ObBatchSubmitCtx* ctx)
  {
    UNUSED(ctx);
    return op_reclaim_alloc(BatchSubmitCtxHashNode);
  }
  void free_node(BatchSubmitCtxHashNode* node)
  {
    if (NULL != node) {
      op_reclaim_free(node);
      node = NULL;
    }
  }
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_BATCH_SUBMIT_CTX_H_
