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

#include "ob_partition_table_util.h"
#include "ob_root_utils.h"

#include "lib/container/ob_se_array_iterator.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_global_stat_proxy.h"
#include "ob_server_manager.h"
#include "ob_zone_manager.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {

using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver {

template <typename T>
T first_param_percnetage(const T first, const T second)
{
  T percentage = 100;
  const T sum = first + second;
  if (0 != sum) {
    percentage = first * 100 / sum;
  }
  return percentage;
}

int64_t ObPartitionTableUtil::MergeProgress::get_merged_partition_percentage() const
{
  return first_param_percnetage(merged_partition_cnt_, unmerged_partition_cnt_);
}

int64_t ObPartitionTableUtil::MergeProgress::get_merged_data_percentage() const
{
  return first_param_percnetage(merged_data_size_, unmerged_data_size_);
}

ObPartitionTableUtil::ObPartitionTableUtil()
    : inited_(false), zone_mgr_(NULL), schema_service_(NULL), pt_(NULL), server_mgr_(NULL), sql_proxy_(NULL)
{}

ObPartitionTableUtil::~ObPartitionTableUtil()
{}

int ObPartitionTableUtil::init(ObZoneManager& zone_mgr, ObMultiVersionSchemaService& schema_service,
    ObPartitionTableOperator& pt, ObServerManager& server_mgr, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    zone_mgr_ = &zone_mgr;
    schema_service_ = &schema_service;
    pt_ = &pt;
    server_mgr_ = &server_mgr;
    sql_proxy_ = &sql_proxy;
    inited_ = true;
  }
  return ret;
}

int ObPartitionTableUtil::check_merge_progress(
    const volatile bool& stop, const int64_t version, ObZoneMergeProgress& all_progress, bool& all_majority_merged)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  ObSEArray<ObZone, MAX_ZONE_NUM> all_zones;
  int64_t standby_snapshot_frozen_version = 0;
  int64_t standby_snapshot_schema_version = 0;
  ObClusterType cluster_type;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition table util not inited", K(ret));
  } else if (stop) {
    ret = OB_CANCELED;
  } else if (version <= 0 || all_progress.error()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(version), "array_error", all_progress.error());
  } else if (FALSE_IT(cluster_type = ObClusterInfoGetter::get_cluster_type_v2())) {
    // nothing todo
  } else {
    all_majority_merged = true;
    all_progress.reset();
    if (OB_FAIL(zone_mgr_->get_zone(all_zones))) {
      LOG_WARN("get zone failed", K(ret));
    } else {
      // init %all_progress
      MergeProgress zone_progress;
      for (int64_t i = 0; OB_SUCC(ret) && i < all_zones.count(); ++i) {
        zone_progress.zone_ = all_zones.at(i);
        if (OB_FAIL(all_progress.push_back(zone_progress))) {
          LOG_WARN("add zone merge progress to array failed", K(ret));
        }
      }
      std::sort(all_progress.begin(), all_progress.end());
    }
  }
  bool need_check_root_table_integrated = true;
  if (OB_SUCC(ret)) {
    ObGlobalStatProxy global_proxy(*sql_proxy_);
    if (STANDBY_CLUSTER != cluster_type) {
      // nothing todo
    } else if (OB_FAIL(global_proxy.get_schema_snapshot_version(
                   standby_snapshot_schema_version, standby_snapshot_frozen_version))) {
      LOG_WARN("fail to get schema snapshot versoin", KR(ret));
    } else if (standby_snapshot_frozen_version == 0 && version <= 1) {
      need_check_root_table_integrated = false;
    } else if (version <= standby_snapshot_frozen_version) {
      need_check_root_table_integrated = false;
    }
  }
  ObSchemaGetterGuard schema_guard;
  ObPartitionTableIterator iter;
  bool ignore_row_checksum = true;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(iter.init(*pt_, *schema_service_, ignore_row_checksum))) {
    LOG_WARN("partition table iterator init failed", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_only_alive_server(*server_mgr_))) {
    LOG_WARN("set filter failed", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_replica_status(REPLICA_STATUS_NORMAL))) {
    LOG_WARN("set filter failed", K(ret));
  } else if (OB_FAIL(iter.get_filters().filter_rebuild_replica())) {
    LOG_WARN("fail to set filter", K(ret));
  } else if (OB_FAIL(iter.get_filters().filter_restore_replica())) {
    LOG_WARN("fail to set filter", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else {
    int64_t majority = 0;
    int64_t full_replica_num = OB_INVALID_COUNT;
    int64_t all_replica_num = OB_INVALID_COUNT;
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    uint64_t last_partition_entity_id = OB_INVALID_ID;
    ObPartitionInfo partition;
    bool need_check = true;
    while (!stop && OB_SUCC(ret) && OB_SUCC(iter.next(partition))) {
      if (OB_INVALID_ID == last_partition_entity_id || last_partition_entity_id != partition.get_table_id()) {
        const uint64_t this_partition_entity_id = partition.get_table_id();
        bool is_restore = false;
        const uint64_t tenant_id = partition.get_tenant_id();
        if (OB_FAIL(schema_guard.check_tenant_is_restore(tenant_id, is_restore))) {
          LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
        } else if (is_restore && !is_inner_table(this_partition_entity_id)) {
          need_check = false;  // user table do not suspend merge during restore
        } else if (!is_tablegroup_id(this_partition_entity_id)) {
          const share::schema::ObTableSchema* table_schema = nullptr;
          if (OB_FAIL(schema_guard.get_table_schema(this_partition_entity_id, table_schema))) {
            LOG_WARN("fail to get table schema", K(ret), "table_id", this_partition_entity_id);
          } else if (nullptr == table_schema) {
            need_check = false;  // ignore the table does not exist
          } else if (OB_FAIL(get_associated_replica_num(schema_guard,
                         *table_schema,
                         paxos_replica_num,
                         full_replica_num,
                         all_replica_num,
                         majority))) {
            LOG_WARN("fail to get associated replica num", K(ret));
          } else {
            last_partition_entity_id = this_partition_entity_id;
            need_check = !(table_schema->is_index_table() && table_schema->is_global_index_table() &&
                           !table_schema->can_read_index());
          }
        } else {
          const share::schema::ObTablegroupSchema* tg_schema = nullptr;
          if (OB_FAIL(schema_guard.get_tablegroup_schema(this_partition_entity_id, tg_schema))) {
            LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", this_partition_entity_id);
          } else if (nullptr == tg_schema) {
            need_check = false;  // ignore the tablegroup does not exist
          } else if (OB_FAIL(get_associated_replica_num(
                         schema_guard, *tg_schema, paxos_replica_num, full_replica_num, all_replica_num, majority))) {
            LOG_WARN("fail to get associated replica num", K(ret));
          } else {
            last_partition_entity_id = this_partition_entity_id;
            need_check = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        // check the integrity for every replica
        // at lease one full replica is required, number of paxos replicas shall no more than majority
        int64_t full_cnt = 0;
        int64_t paxos_cnt = 0;
        FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCC(ret))
        {
          if (OB_ISNULL(r)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid replica", K(ret), K(r));
          } else if (!r->is_in_service()) {
            // noting todo
          } else if (ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
            paxos_cnt++;
            if (REPLICA_TYPE_FULL == r->replica_type_) {
              full_cnt++;
            }
          }
        }  // end FOREACH_CNT_X(
        if (OB_SUCC(ret) && need_check_root_table_integrated && need_check) {
          // ignore the replica integrity check for unavailable index
          // since replicas in standby cluster are not copied until index is available
          if (full_cnt <= 0 || paxos_cnt < majority) {
            const bool is_cluster_private_table =
                !GCTX.is_standby_cluster() || ObMultiClusterUtil::is_cluster_private_table(partition.get_table_id());
            ret = OB_ROOT_NOT_INTEGRATED;
            if (is_cluster_private_table) {
              LOG_ERROR("partition does not have enough alive normal replicas",
                  K(majority),
                  K(partition),
                  K(paxos_cnt),
                  K(paxos_cnt));
            } else {
              LOG_WARN("partition does not have enough alive normal replicas",
                  K(majority),
                  K(partition),
                  K(paxos_cnt),
                  K(paxos_cnt));
            }
          }
        }
      }  // end if
      if (OB_FAIL(ret)) {
      } else if (!need_check) {
        // no need to check when schema not exist, or unreadable global index
      } else {
        // check all replica merged to %version
        int64_t merged_cnt = 0;
        int64_t full_merged_cnt = 0;
        FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCCESS == ret)
        {
          ObZoneMergeProgress::iterator p = std::lower_bound(all_progress.begin(), all_progress.end(), r->zone_);
          if (p != all_progress.end() && p->zone_ == r->zone_) {
            if (REPLICA_TYPE_LOGONLY == r->replica_type_) {
              // nothing todo
            } else {
              if (p->smallest_data_version_ <= 0 || p->smallest_data_version_ > r->data_version_) {
                p->smallest_data_version_ = r->data_version_;
                // only log the first old version replica
                if (r->data_version_ < version - 1) {
                  LOG_WARN("replica data version is too old", K(version), "replica", *r);
                }
              }
              if (r->data_version_ < version) {
                // only log the first replica not merged
                if (p->unmerged_partition_cnt_ == 0) {
                  LOG_INFO("replica not merged to version", K(version), "replica", *r);
                }
                p->unmerged_partition_cnt_++;
                p->unmerged_data_size_ += r->data_size_;
              } else {
                if (REPLICA_TYPE_FULL == r->replica_type_) {
                  full_merged_cnt++;
                }
                p->merged_partition_cnt_++;
                p->merged_data_size_ += r->data_size_;
                merged_cnt++;
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          // standard for most merged:
          // 1 the majority of full replicas merge finished
          // 2 the majority of f+r replicas merge finished, ignore readonly@all locality
          int64_t replica_count_with_sstable = all_replica_num - (paxos_replica_num - full_replica_num);
          if (all_majority_merged && (full_merged_cnt < rootserver::majority(full_replica_num) ||
                                         merged_cnt < rootserver::majority(replica_count_with_sstable))) {
            all_majority_merged = false;
            // only log the first partition which majority replicas not merged
            LOG_INFO("partition's majority replicas not merged", K(version), K(majority), K(merged_cnt), K(partition));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (stop) {
      ret = OB_CANCELED;
      LOG_WARN("stop", K(ret));
    }
  }
  const int64_t end_time = ObTimeUtility::current_time();
  if (OB_SUCC(ret)) {
    LOG_INFO("check merge progress success",
        K(version),
        K(all_majority_merged),
        K(all_progress),
        "checking_time_us",
        end_time - start_time);
  } else {
    LOG_WARN("check merge progress failed", K(ret), K(version));
  }

  return ret;
}

/*
int ObPartitionTableUtil::set_leader_backup_flag(
    const volatile bool &stop,
    const bool flag,
    ObLeaderInfoArray *leader_info_array)
{
  int ret = OB_SUCCESS;
  ObPartitionTableIterator iter;
  bool ignore_row_checksum = true;

  if (NULL != leader_info_array) {
    leader_info_array->reset();
  }
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_FAIL(iter.init(*pt_, *schema_service_, ignore_row_checksum))) {
    LOG_WARN("partition table iterator init failed", K(ret));
  } else {
    ObPartitionInfo partition;
    while (!stop && OB_SUCCESS == ret) {
      if (OB_FAIL(iter.next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("iterator partition table failed", K(ret));
        }
        break;
      }

      bool exist_flag = false;
      FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCCESS == ret && !exist_flag) {
        if (r->is_original_leader_) {
          exist_flag = true;
        }
      } //end foreach
      if (OB_SUCC(ret)) {
        const ObPartitionReplica *r = NULL;
        if (OB_FAIL(partition.find_leader_v2(r))) {
          LOG_WARN("fail to find leader", K(ret), K(partition));
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          }
        } else if (OB_ISNULL(r)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid leader", K(ret), K(r), K(partition));
        } else if (NULL != leader_info_array) {
          bool found = false;
          for (int64_t i = 0; OB_SUCC(ret) && i < leader_info_array->count(); ++i) {
            if (leader_info_array->at(i).zone_ == r->zone_) {
              found = true;
              ++leader_info_array->at(i).leader_count_;
              break;
            }
          } //end for

          if (OB_SUCC(ret) && !found) {
            ObZoneLeaderInfo info;
            info.zone_ = r->zone_;
            info.leader_count_ = 1;
            if (OB_FAIL(leader_info_array->push_back(info))) {
              LOG_WARN("failed to add zone leader info", K(ret));
            }
          }
        }
      }
      if (flag != exist_flag){
        if (OB_FAIL(pt_->set_original_leader(
                    partition.get_table_id(), partition.get_partition_id(), flag))) {
          LOG_WARN("set leader backup failed", K(ret), K(partition));
        }
      }
    }
    if (stop) {
      ret = OB_CANCELED;
      LOG_WARN("stop", K(ret));
    }
  }

  return ret;
}*/

}  // end namespace rootserver
}  // end namespace oceanbase
