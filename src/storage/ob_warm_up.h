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

#ifndef OCEANBASE_STORAGE_OB_WARM_UP_H_
#define OCEANBASE_STORAGE_OB_WARM_UP_H_

#include "storage/ob_dml_param.h"
#include "common/ob_partition_key.h"
#include "lib/container/ob_array.h"
#include "lib/random/ob_random.h"
#include "lib/atomic/ob_atomic.h"
#include "ob_partition_service_rpc.h"
#include "ob_warm_up_request.h"

namespace oceanbase {
namespace share {
class ObAliveServerTracer;
}

namespace transaction {
class ObTransDesc;
}

namespace storage {
class ObWarmUpCtx {
public:
  ObWarmUpCtx();
  ~ObWarmUpCtx();
  void record_exist_check(const common::ObPartitionKey& pkey, const int64_t table_id,
      const common::ObStoreRowkey& rowkey, const common::ObIArray<share::schema::ObColDesc>& column_ids);
  void record_get(const ObTableAccessParam& param, const ObTableAccessContext& ctx, const ObExtStoreRowkey& rowkey);
  void record_multi_get(const ObTableAccessParam& param, const ObTableAccessContext& ctx,
      const common::ObIArray<ObExtStoreRowkey>& rowkeys);
  void record_scan(const ObTableAccessParam& param, const ObTableAccessContext& ctx, const ObExtStoreRange& range);
  void record_multi_scan(const ObTableAccessParam& param, const ObTableAccessContext& ctx,
      const common::ObIArray<ObExtStoreRange>& ranges);
  void reuse();
  inline const ObWarmUpRequestList& get_requests() const
  {
    return request_list_;
  }
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline void inc_ref()
  {
    ATOMIC_AAF(&ref_cnt_, 1);
  }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_cnt_, 1);
  }
  TO_STRING_KV(K_(request_list));

private:
  const static int64_t MAX_WARM_UP_REQUESTS_SIZE = 32 * 1024;
  common::ObArenaAllocator allocator_;
  ObWarmUpRequestList request_list_;
  uint64_t tenant_id_;
  int64_t ref_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObWarmUpCtx);
};

class ObWarmUpService;
class ObSendWarmUpTask : public common::IObDedupTask {
public:
  ObSendWarmUpTask();
  virtual ~ObSendWarmUpTask();
  void assign(ObWarmUpService& warm_service, ObWarmUpCtx& warm_ctx, int64_t task_create_time);
  virtual int64_t hash() const
  {
    return reinterpret_cast<int64_t>(this);
  }
  virtual bool operator==(const IObDedupTask& task) const
  {
    return this == &task;
  }
  virtual int64_t get_deep_copy_size() const;
  virtual IObDedupTask* deep_copy(char* buffer, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();
  TO_STRING_KV(K_(warm_ctx));

private:
  const static int64_t TASK_EXPIRE_TIME = 1000 * 1000;  // 1s
  ObWarmUpService* warm_service_;
  ObWarmUpCtx* warm_ctx_;
  int64_t task_create_time_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSendWarmUpTask);
};

class ObWarmUpService {
public:
  ObWarmUpService();
  ~ObWarmUpService();
  void stop();
  int init(ObPartitionServiceRpc& pts_rpc, share::ObAliveServerTracer& server_tracer,
      common::ObInOutBandwidthThrottle& bandwidth_throttle);
  int register_warm_up_ctx(transaction::ObTransDesc& trans_desc);
  int deregister_warm_up_ctx(transaction::ObTransDesc& trans_desc);
  int send_warm_up_request(const ObWarmUpCtx& warm_up_ctx);

private:
  int check_need_warm_up(bool& is_need);
  int get_members(const common::ObPartitionKey& pkey, common::ObMemberList& members);

private:
  typedef common::hash::ObHashMap<ObPartitionKey, ObMemberList*, common::hash::NoPthreadDefendMode> MemberMap;
  typedef common::hash::ObHashMap<ObAddr, obrpc::ObWarmUpRequestArg*, common::hash::NoPthreadDefendMode> ServerMap;
  static const int32_t MAX_SEND_TASK_THREAD_CNT = 1;
  static const int64_t SEND_TASK_QUEUE_SIZE = 10000;
  static const int64_t SEND_TASK_QUEUE_RESERVE_COUNT = 1000;
  static const int64_t SEND_TASK_MAP_SIZE = 10000;
  static const int64_t PRINT_INTERVAL = 1L * 1000L * 1000L;
  bool is_inited_;
  ObPartitionServiceRpc* rpc_;
  share::ObAliveServerTracer* server_tracer_;
  common::ObRandom rand_;
  common::ObDedupQueue send_task_queue_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObWarmUpService);
};
}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_WARM_UP_H_
