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

#ifndef OCEANBASE_ROOTSERVER_OB_STARTUP_PROGRESS_TRACKER_H_
#define OCEANBASE_ROOTSERVER_OB_STARTUP_PROGRESS_TRACKER_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_partition_key.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}  // namespace obrpc
namespace share {
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
}  // end namespace schema
}  // end namespace share
namespace rootserver {
class ObServerManager;
class ObStartupProgressTracker {
public:
  ObStartupProgressTracker();
  virtual ~ObStartupProgressTracker();
  int init(ObServerManager& server_manager, obrpc::ObSrvRpcProxy& rpc_proxy,
      share::schema::ObMultiVersionSchemaService& schema_service);
  int track_startup_progress(bool& is_finished);

private:
  typedef common::hash::ObHashMap<common::ObPartitionKey, obrpc::ObPartitionStat::PartitionStat> LeaderStatusMap;

  static const int64_t LEADER_STATUS_MAP_BUCKT_NUM = 1024 * 512;
  static const int64_t MAX_NOT_WRITABLE_PARTITION_LOG_CNT = 32;
  static const int64_t PERCENTAGE_BUF_LEN = 256;

  int fetch_startup_progress();
  int calculate_startup_progress(bool& is_finished);
  int log_not_writable_partition(
      const common::ObPartitionKey& partition, const bool without_leader, int64_t& log_count);
  int log_startup_progress_summary(
      const int64_t writable_count, const int64_t recovering_count, const int64_t without_leader_count);
  int check_cancel() const;

private:
  bool inited_;
  ObServerManager* server_manager_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  LeaderStatusMap leader_status_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStartupProgressTracker);
};
}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_STARTUP_PROGRESS_TRACKER_H_
