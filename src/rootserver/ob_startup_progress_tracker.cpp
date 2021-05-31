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

#define USING_LOG_PREFIX RS

#include "ob_startup_progress_tracker.h"

#include "common/ob_zone.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_iter.h"
#include "share/schema/ob_part_mgr_util.h"
#include "rootserver/ob_server_manager.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver {
ObStartupProgressTracker::ObStartupProgressTracker()
    : inited_(false), server_manager_(NULL), rpc_proxy_(NULL), schema_service_(NULL), leader_status_map_()
{}

ObStartupProgressTracker::~ObStartupProgressTracker()
{}

int ObStartupProgressTracker::init(
    ObServerManager& server_manager, ObSrvRpcProxy& rpc_proxy, ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(
                 leader_status_map_.create(LEADER_STATUS_MAP_BUCKT_NUM, ObModIds::OB_HASH_BUCKET_LEADER_STATUS_MAP))) {
    LOG_WARN(
        "leader_status_map_ create failed", "bucket num", static_cast<int64_t>(LEADER_STATUS_MAP_BUCKT_NUM), K(ret));
  } else {
    server_manager_ = &server_manager;
    rpc_proxy_ = &rpc_proxy;
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

int ObStartupProgressTracker::track_startup_progress(bool& is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(fetch_startup_progress())) {
    LOG_WARN("fetch startup progress failed", K(ret));
  } else if (OB_FAIL(calculate_startup_progress(is_finished))) {
    LOG_WARN("calculate startup progress failed", K(ret));
  } else {
    LOG_INFO("tracking startup progress", K(is_finished));
  }
  return ret;
}

int ObStartupProgressTracker::fetch_startup_progress()
{
  int ret = OB_SUCCESS;
  const ObZone zone;  // empty zone means all zones
  ObServerManager::ObServerStatusArray server_statuses;
  leader_status_map_.clear();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_manager_->get_server_statuses(zone, server_statuses))) {
    LOG_WARN("server manager get server statuses failed", K(zone), K(ret));
  } else {
    ObPartitionStatList leader_statuses;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses.count(); ++i) {
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", K(ret));
      } else if (server_statuses[i].is_alive()) {
        leader_statuses.reuse();
        if (OB_FAIL(rpc_proxy_->to(server_statuses.at(i).server_).get_partition_stat(leader_statuses))) {
          LOG_WARN("get partition stat failed, maybe server is not really serving",
              "server",
              server_statuses.at(i).server_,
              K(ret));
          // ignore rpc call get_partition_stat error
          ret = OB_SUCCESS;
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < leader_statuses.count(); ++j) {
            const int overwrite_flag = 1;
            if (OB_FAIL(leader_status_map_.set_refactored(
                    leader_statuses.at(j).partition_key_, leader_statuses.at(j).stat_, overwrite_flag))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN(
                  "leader_status_map_ set failed", "leader status", leader_statuses.at(j), K(overwrite_flag), K(ret));
            } else {
              // if leader change when we do fetch_startup_progress, it's possible that
              // hash_ret be HASH_OVERWIRTE_SUCC
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObStartupProgressTracker::calculate_startup_progress(bool& is_finished)
{
  int ret = OB_SUCCESS;
  int64_t recovering_count = 0;
  int64_t writable_count = 0;
  int64_t without_leader_count = 0;
  int64_t log_count = 0;
  ObSchemaGetterGuard schema_guard;
  ObTableIterator table_iterator;
  is_finished = false;
  // we do startup progress track after rs do restart task successfully, so we have
  // already successfully refreshed schema
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(table_iterator.init(&schema_guard))) {
    LOG_WARN("table iterator init failed", K(ret));
  } else {
    uint64_t table_id = OB_INVALID_ID;
    ObPartitionKey key;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", K(ret));
      } else if (OB_FAIL(table_iterator.next(table_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("table iterator iterate failed", K(ret), K(table_iterator));
        }
      } else {
        const ObTableSchema* table_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
          LOG_WARN("get table schema failed", KT(table_id), K(ret));
        } else if (NULL == table_schema) {
          LOG_INFO("table schema is NULL", KT(table_id));
        } else {
          if (table_schema->has_partition()) {
            bool check_dropped_schema = false;
            ObTablePartitionKeyIter pkey_iter(*table_schema, check_dropped_schema);
            for (int64_t i = 0; OB_SUCC(ret) && i < pkey_iter.get_partition_num(); ++i) {
              key.reset();
              ObPartitionStat::PartitionStat status = ObPartitionStat::RECOVERING;
              if (OB_SUCC(ret)) {
                if (OB_FAIL(check_cancel())) {
                  LOG_WARN("check_cancel failed", K(ret));
                } else if (OB_FAIL(pkey_iter.next_partition_key_v2(key))) {
                  LOG_WARN("Failed to get next partition id", K(ret));
                } else if (OB_FAIL(leader_status_map_.get_refactored(key, status))) {
                  if (OB_HASH_NOT_EXIST != ret) {
                    LOG_WARN("get partition status from PartitionStatusMap failed", K(key), K(ret));
                  } else {
                    ret = OB_SUCCESS;
                    ++without_leader_count;
                    if (log_count < MAX_NOT_WRITABLE_PARTITION_LOG_CNT) {
                      const bool without_leader = true;
                      if (OB_FAIL(log_not_writable_partition(key, without_leader, log_count))) {
                        LOG_WARN("log_not_writable_partition failed", K(key), K(without_leader), K(ret));
                      }
                    }
                  }
                } else {
                  if (ObPartitionStat::WRITABLE == status) {
                    ++writable_count;
                  } else if (ObPartitionStat::RECOVERING == status) {
                    ++recovering_count;
                    if (log_count < MAX_NOT_WRITABLE_PARTITION_LOG_CNT) {
                      const bool without_leader = false;
                      if (OB_FAIL(log_not_writable_partition(key, without_leader, log_count))) {
                        LOG_WARN("log_not_writable_partition failed", K(key), K(without_leader), K(ret));
                      }
                    }
                  }
                }
              }
            }  // for end
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (recovering_count + without_leader_count == 0) {
      is_finished = true;
    }
    if (OB_FAIL(check_cancel())) {
      LOG_WARN("check_cancel failed", K(ret));
    } else if (OB_FAIL(log_startup_progress_summary(writable_count, recovering_count, without_leader_count))) {
      LOG_WARN("log startup progress summary failed",
          K(writable_count),
          K(recovering_count),
          K(without_leader_count),
          K(ret));
    }
  } else {
    LOG_WARN(
        "calculate startup progress failed", K(recovering_count), K(writable_count), K(without_leader_count), K(ret));
  }
  return ret;
}

int ObStartupProgressTracker::log_not_writable_partition(
    const ObPartitionKey& partition, const bool without_leader, int64_t& log_count)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition", K(partition), K(ret));
  } else if (log_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log_count small than zero", K(log_count), K(ret));
  } else {
    if (without_leader) {
      LOG_INFO("partition without leader", K(partition));
    } else {
      LOG_INFO("partition is recovering", K(partition));
    }
    ++log_count;
    if (MAX_NOT_WRITABLE_PARTITION_LOG_CNT == log_count) {
      LOG_INFO("only log the front MAX_NOT_WRITABLE_PARTITION_LOG_CNT partitions",
          "MAX_NOT_WRITABLE_PARTITION_LOG_CNT",
          static_cast<int64_t>(MAX_NOT_WRITABLE_PARTITION_LOG_CNT));
    }
  }
  return ret;
}

int ObStartupProgressTracker::log_startup_progress_summary(
    const int64_t writable_count, const int64_t recovering_count, const int64_t without_leader_count)
{
  int ret = OB_SUCCESS;
  const int64_t with_leader_count = recovering_count + writable_count;
  const int64_t total_count = with_leader_count + without_leader_count;
  // schema related tables are readable, so total_count is greater than 0
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (writable_count < 0 || recovering_count < 0 || without_leader_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(writable_count), K(recovering_count), K(without_leader_count), K(ret));
  } else if (total_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected total_count", K(total_count), K(ret));
  } else {
    const double with_leader_percentage =
        static_cast<double>(with_leader_count) / static_cast<double>(total_count) * 100;
    const double writable_percentage = static_cast<double>(writable_count) / static_cast<double>(total_count) * 100;
    char writable_percentage_buf[PERCENTAGE_BUF_LEN];
    char with_leader_percentage_buf[PERCENTAGE_BUF_LEN];
    int n = 0;
    if ((n = snprintf(writable_percentage_buf, PERCENTAGE_BUF_LEN, "%.2f%c", writable_percentage, '%')) >=
            PERCENTAGE_BUF_LEN ||
        n < 0) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf writable_percentage_buf failed", K(n), K(ret));
    } else if ((n = snprintf(with_leader_percentage_buf, PERCENTAGE_BUF_LEN, "%.2f%c", with_leader_percentage, '%')) >=
                   PERCENTAGE_BUF_LEN ||
               n < 0) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf with_leader_percentage_buf failed", K(n), K(ret));
    } else {
      LOG_INFO("calculate startup progress succeed",
          "writable_percentage",
          writable_percentage_buf,
          "with_leader_percentage",
          with_leader_percentage_buf,
          K(recovering_count),
          K(writable_count),
          K(without_leader_count));
    }
  }
  return ret;
}

int ObStartupProgressTracker::check_cancel() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // @todo support cancel
    ret = OB_SUCCESS;
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
