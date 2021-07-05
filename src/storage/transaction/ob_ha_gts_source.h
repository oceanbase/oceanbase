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

#ifndef OCEANBASE_TRANSACTION_HA_GTS_SOURCE_
#define OCEANBASE_TRANSACTION_HA_GTS_SOURCE_

#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/utility.h"
#include "ob_gts_local_cache.h"
#include "ob_gts_task_queue.h"
#include "ob_gts_worker.h"
#include "ob_i_ts_source.h"
#include "ob_gts_source.h"

namespace oceanbase {
namespace transaction {
class ObILocationAdapter;
class ObTsCbTask;
class ObIGlobalTimestampService;
class ObIGtsRequestRpc;
}  // namespace transaction

namespace transaction {
class ObHaGtsSource : public ObITsSource {
public:
  ObHaGtsSource() : log_interval_(3 * 1000 * 1000)
  {
    reset();
  }
  ~ObHaGtsSource()
  {
    destroy();
  }
  int init(const uint64_t tenant_id, const common::ObAddr& server, ObILocationAdapter* location_adapter);
  void destroy();
  void reset();
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int handle_gts_result(const uint64_t tenant_id, const int64_t queue_index);
  int update_gts(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts, bool& update);
  int get_srr(MonotonicTs& srr);
  int get_latest_srr(MonotonicTs& latest_srr);
  int64_t get_task_count() const;

public:
  int update_gts(const int64_t gts, bool& update);
  int get_gts(const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts);
  int get_gts(ObTsCbTask* task, int64_t& gts);
  int get_local_trans_version(ObTsCbTask* task, int64_t& gts);
  int get_local_trans_version(const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts);
  int update_local_trans_version(const int64_t version, bool& update);
  int wait_gts_elapse(const int64_t ts, ObTsCbTask* task, bool& need_wait);
  int wait_gts_elapse(const int64_t ts);
  int refresh_gts(const bool need_refresh);
  int update_base_ts(const int64_t base_ts, const int64_t publish_version);
  int get_base_ts(int64_t& base_ts, int64_t& publish_version);
  bool is_external_consistent()
  {
    return true;
  }
  int update_publish_version(const int64_t publish_version);
  int get_publish_version(int64_t& publish_version);
  TO_STRING_KV(K_(tenant_id), K_(gts_local_cache), K_(server));

private:
  void statistics_();
  int refresh_gts_(const bool need_refresh);

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
  ObILocationAdapter* location_adapter_;
  common::ObTimeInterval log_interval_;
  // statistics
  ObGtsStatistics gts_statistics_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_HA_GTS_SOURCE_
