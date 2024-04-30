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

#ifndef OCEANBASE_TRANSACTION_OB_GTS_SOURCE_
#define OCEANBASE_TRANSACTION_OB_GTS_SOURCE_

#include "lib/net/ob_addr.h"
#include "lib/utility/utility.h"
#include "ob_gts_msg.h"
#include "ob_gts_local_cache.h"
#include "ob_gts_task_queue.h"
//#include "ob_ts_worker.h"
#include "ob_i_ts_source.h"

namespace oceanbase
{
namespace obrpc
{
class ObGtsRpcResult;
}
namespace transaction
{
class ObILocationAdapter;
class ObTsCbTask;
class ObIGtsRequestRpc;
}
namespace transaction
{

class ObGtsStatistics
{
  static const int64_t STAT_INTERVAL = 5 * 1000 * 1000;
public:
  ObGtsStatistics() { reset(); }
  ~ObGtsStatistics() {}
  int init(const uint64_t tenant_id);
  void reset();
  void inc_gts_rpc_cnt() { ATOMIC_INC(&gts_rpc_cnt_); }
  void inc_get_gts_cache_cnt() { ATOMIC_INC(&get_gts_cache_cnt_); }
  void inc_get_gts_with_stc_cnt() { ATOMIC_INC(&get_gts_with_stc_cnt_); }
  void inc_try_get_gts_cache_cnt() { ATOMIC_INC(&try_get_gts_cache_cnt_); }
  void inc_try_get_gts_with_stc_cnt() { ATOMIC_INC(&try_get_gts_with_stc_cnt_); }
  void inc_wait_gts_elapse_cnt() { ATOMIC_INC(&wait_gts_elapse_cnt_); }
  void inc_try_wait_gts_elapse_cnt() { ATOMIC_INC(&try_wait_gts_elapse_cnt_); }
  void statistics();
private:
  uint64_t tenant_id_;
  int64_t last_stat_ts_;
  int64_t gts_rpc_cnt_;

  int64_t get_gts_cache_cnt_;
  int64_t get_gts_with_stc_cnt_;
  int64_t try_get_gts_cache_cnt_;
  int64_t try_get_gts_with_stc_cnt_;

  int64_t wait_gts_elapse_cnt_;
  int64_t try_wait_gts_elapse_cnt_;
};

class ObGtsSource
{
public:
  ObGtsSource() : log_interval_(3 * 1000 * 1000), refresh_location_interval_(100 * 1000) { reset(); }
  ~ObGtsSource() { destroy(); }
  int init(const uint64_t tenant_id, const common::ObAddr &server, ObIGtsRequestRpc *gts_request_rpc,
           ObILocationAdapter *location_adapter);
  void destroy();
  void reset();
  uint64_t get_tenant_id() const { return tenant_id_; }
  int handle_gts_err_response(const ObGtsErrResponse &msg);
  int handle_gts_result(const uint64_t tenant_id, const int64_t queue_index);
  int update_gts(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts, bool &update);
  int get_srr(MonotonicTs &srr);
  int get_latest_srr(MonotonicTs &latest_srr);
  int64_t get_task_count() const;
  int gts_callback_interrupted(const int errcode, const share::ObLSID ls_id);
public:
  int update_gts(const int64_t gts, bool &update);
  int get_gts(const MonotonicTs stc, ObTsCbTask *task, int64_t &gts, MonotonicTs &receive_gts_ts);
  int get_gts(ObTsCbTask *task, int64_t &gts);
  int wait_gts_elapse(const int64_t ts, ObTsCbTask *task, bool &need_wait);
  int wait_gts_elapse(const int64_t ts);
  int refresh_gts(const bool need_refresh);
  bool is_external_consistent() { return true; }
  int refresh_gts_location() { return refresh_gts_location_(); }
  TO_STRING_KV(K_(tenant_id), K_(gts_local_cache), K_(server), K_(gts_cache_leader));
private:
  int get_gts_leader_(common::ObAddr &leader);
  int refresh_gts_location_();
  int refresh_gts_(const bool need_refresh);
  int query_gts_(const common::ObAddr &leader);
  void statistics_();
  int get_gts_from_local_timestamp_service_(common::ObAddr &leader,
                                            int64_t &gts,
                                            MonotonicTs &receive_gts_ts);
  int get_gts_from_local_timestamp_service_(common::ObAddr &leader,
                                            int64_t &gts);
public:
  static const int64_t GET_GTS_QUEUE_COUNT = 1;
  static const int64_t WAIT_GTS_QUEUE_COUNT = 1;
  static const int64_t WAIT_GTS_QUEUE_START_INDEX = GET_GTS_QUEUE_COUNT;
  static const int64_t TOTAL_GTS_QUEUE_COUNT = GET_GTS_QUEUE_COUNT + WAIT_GTS_QUEUE_COUNT;
private:
  bool is_inited_;
  int64_t tenant_id_;
  ObGTSLocalCache gts_local_cache_;
  ObGTSTaskQueue queue_[TOTAL_GTS_QUEUE_COUNT];
  common::ObAddr server_;
  ObIGtsRequestRpc *gts_request_rpc_;
  ObILocationAdapter *location_adapter_;
  // statistics
  ObGtsStatistics gts_statistics_;
  common::ObTimeInterval log_interval_;
  common::ObAddr gts_cache_leader_;
  common::ObTimeInterval refresh_location_interval_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_RANSACTION_OB_GTS_SOURCE_
