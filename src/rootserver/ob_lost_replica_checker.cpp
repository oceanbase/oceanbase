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

#include "ob_lost_replica_checker.h"

#include "lib/time/ob_time_utility.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/container/ob_se_array.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_multi_cluster_util.h"
#include "rootserver/ob_server_manager.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_pg_partition_meta_table_updater.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_file_system_router.h"
#include "rootserver/ob_root_service.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
namespace rootserver {

ObLostReplicaChecker::ObLostReplicaChecker()
    : inited_(false), server_manager_(NULL), pt_operator_(NULL), schema_service_(NULL)
{}

ObLostReplicaChecker::~ObLostReplicaChecker()
{}

int ObLostReplicaChecker::check_cancel()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLostReplicaChecker not inited", KR(ret), K(inited_));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service is null", KR(ret));
  } else if (!GCTX.root_service_->in_service()) {
    ret = OB_CANCELED;
    LOG_WARN("root service is stop", KR(ret));
  } else {
    // nothing todo
  }
  return ret;
}

int ObLostReplicaChecker::init(
    ObServerManager& server_manager, ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    server_manager_ = &server_manager;
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

int ObLostReplicaChecker::check_lost_replicas()
{
  int ret = OB_SUCCESS;
  ObPartitionInfo partition_info;
  // Traversing meta table, not schema
  ObPartitionTableIdIterator pt_part_iter;
  uint64_t pt_table_id = OB_INVALID_ID;
  int64_t pt_partition_id = OB_INVALID_INDEX;
  LOG_INFO("start checking lost replicas");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(pt_part_iter.init(*schema_service_))) {
    LOG_INFO("pt_part_iterator init failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(check_cancel())) {
        if (OB_CANCELED != ret) {
          LOG_WARN("fail to check cancel", KR(ret));
        } else {
          LOG_INFO("rs is stopped, no need check lost replicas", KR(ret));
        }
      } else if (OB_FAIL(pt_part_iter.get_next_partition(pt_table_id, pt_partition_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("pt_part_iterator next failed", K(ret));
        }
      } else if (OB_SUCCESS != (tmp_ret = check_lost_replica_by_pt(pt_table_id, pt_partition_id))) {
        // check lost_replica by pt, and ignore the error, as far as possible to generate tasks
        LOG_WARN("fail to check_lost_replica_by_pt", K(tmp_ret), K(pt_table_id), K(pt_partition_id));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLostReplicaChecker::delete_pg_partition_meta_table_item_(const uint64_t tg_id, const common::ObAddr& svr_ip)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(tg_id);
  const int64_t version = 0;
  share::schema::ObSchemaGetterGuard schema_guard;
  common::ObArray<const ObSimpleTableSchemaV2*> table_schema_array;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tg_id || !svr_ip.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tg_id), K(svr_ip));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id), K(tg_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tg_id, table_schema_array))) {
    LOG_WARN("fail to get table schemas in tablegroup", K(ret), K(tenant_id), K(tg_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema_array.count(); ++i) {
      const ObSimpleTableSchemaV2* schema = table_schema_array.at(i);
      if (NULL == schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("schema is null, unexpected error", K(ret), KP(schema), K(tg_id));
      } else if (!schema->has_partition()) {
        // do nothing
        // has not parition, no need to process, like local index
      } else {
        bool check_dropped_schema = true;
        ObTablePgKeyIter iter(*schema, tg_id, check_dropped_schema);
        ObPartitionKey pkey;
        ObPGKey pg_key;
        if (OB_FAIL(iter.init())) {
          LOG_WARN("fail to iter init", K(ret));
        } else {
          while (OB_SUCC(ret) && OB_SUCC(iter.next(pkey, pg_key))) {
            if (!pkey.is_valid() || !pg_key.is_valid()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret), K(pkey), K(pg_key));
            } else if (OB_FAIL(observer::ObPGPartitionMTUpdater::get_instance().add_task(
                           pkey, observer::ObPGPartitionMTUpdateType::DELETE, version, svr_ip))) {
              LOG_WARN("fail to async update pg partition meta table", K(ret), K(pkey), K(pg_key));
            } else {
              pkey.reset();
              pg_key.reset();
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
  }

  return ret;
}

int ObLostReplicaChecker::check_lost_replica_by_pt(uint64_t pt_table_id, int64_t pt_partition_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionInfo partition_info;
  ObPTPartPartitionIterator pt_iter;
  int64_t lost_count = 0;
  bool is_lost_replica = false;
  LOG_INFO("start checking lost replicas by pt", K(pt_table_id), K(pt_partition_id));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pt_table_id || OB_INVALID_INDEX == pt_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_table_id or pt_partition_id", K(ret), K(pt_table_id), K(pt_partition_id));
  } else if (OB_FAIL(pt_iter.init(*pt_operator_, pt_table_id, pt_partition_id))) {
    LOG_INFO("partition table iterator init failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      // Ignore the error code, and there is no influence between different partitions
      if (OB_FAIL(check_cancel())) {
        if (OB_CANCELED != ret) {
          LOG_WARN("fail to check cancel", KR(ret));
        } else {
          LOG_INFO("rs is stopped, no need check lost replica by pt", KR(ret));
        }
      } else if (FALSE_IT(partition_info.reuse())) {
        // never be here
      } else if (OB_FAIL(pt_iter.next(partition_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("pt_iter next failed", K(ret));
        } else {
        }  // nothing todo
      } else {
        const ObPartitionInfo::ReplicaArray& replicas = partition_info.get_replicas_v2();
        FOREACH_CNT_X(replica, replicas, OB_SUCCESS == ret)
        {
          is_lost_replica = false;
          if (OB_FAIL(check_lost_replica(partition_info, *replica, is_lost_replica))) {
            LOG_WARN("check_lost_replica failed", K(partition_info), "replica", *replica, KR(ret));
          } else if (is_lost_replica) {
            ++lost_count;
            // There is a risk here
            // the reporting mechanism of meta table and partition meta table are independent,
            // There is a possible condition where the meta table has deleted, but due to the machine is down,
            // partition meta table has not beed deleted and has remained records.
            // Later, we need to improve the reporting and management of the two tables
            ObPartitionKey pkey(replica->table_id_, replica->partition_id_, replica->partition_cnt_);
            // delete the record of standalone partition from partition meta table
            if (!pkey.is_pg()) {
              // not pg
              const int64_t version = 0;
              if (OB_SUCCESS != (tmp_ret = observer::ObPGPartitionMTUpdater::get_instance().add_task(
                                     pkey, observer::ObPGPartitionMTUpdateType::DELETE, version, replica->server_))) {
                LOG_WARN("fail to async update pg partition meta table", K(tmp_ret), K(pkey));
              }
              // delete record of all partitions under pg from partition meta table
            } else {
              // if is pg, replica->table_id_ is tg_id
              if (OB_FAIL(delete_pg_partition_meta_table_item_(replica->table_id_, replica->server_))) {
                LOG_WARN("delete pg partition meta table item error", K(ret), K(replica));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(pt_operator_->remove(replica->table_id_, replica->partition_id_, replica->server_))) {
                LOG_WARN("pt_operator remove replica failed", "replica", *replica, K(ret));
              } else {
                LOG_INFO("lost replica checker remove lost replica finish", "replica", *replica, KR(ret), K(tmp_ret));
              }
            }
          } else {
            // do nothing
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  int64_t replica_safe_remove_time_us = GCONF.replica_safe_remove_time;
  LOG_INFO("finish checking lost replicas by pt, lost_count means count of replicas "
           "that on server offline long enough",
      K(lost_count),
      K(replica_safe_remove_time_us),
      K(pt_table_id),
      K(pt_partition_id),
      K(ret));
  return ret;
}

int ObLostReplicaChecker::check_lost_replica(
    const ObPartitionInfo& partition_info, const ObPartitionReplica& replica, bool& is_lost_replica) const
{
  int ret = OB_SUCCESS;
  is_lost_replica = false;
  bool is_lost_server = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition_info.is_valid() || !replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition_info or invalid replica", K(partition_info), K(replica), K(ret));
  } else if (replica.modify_time_us_ + GCONF.replica_safe_remove_time < ObTimeUtility::current_time() ||
             replica.modify_time_us_ + GCONF.server_permanent_offline_time < ObTimeUtility::current_time()) {
    if (OB_FAIL(check_lost_server(replica.server_, is_lost_server))) {
      LOG_WARN("check lost server failed", "server", replica.server_, K(ret));
    } else if (is_lost_server) {
      /*
       * The following logic deals with three kinds of downtime:
       * 1. Paxos replica is not in the member list and is permanently offline
       * 2. Nonpaxos replica is permanently offline
       * 3. Non private table of standby cluster is permanently offline
       *
       * Before permanently offline, all replicas have migrated to other server,
       * the corresponding records in inner table have beed deleted,
       * but there maybe expection:
       * 1. Failed to delete the records in inner table.
       * 2. The replica has not migrated before permanently offline
       *
       * In order to deal with these two kinds of exceptions,
       * it is necessary to recycle and clean the inner table records here.
       *
       * Knonw Issue:If the migration fails and the replica is recycled here,
       * there will be less replicas,
       * The logic for supplementing the replica will be triggered later.
       *
       */
      /*
       * It is used to determine the record of replica can be deleted, the premise is that
       * the server is permanently offline:
       * 1. First, according to whether it is in the leader's member_List,
       *    if not, then the replica needs to be deleted.
       *    non paxos replica or not in_service replica can be deleted directly
       * 2. If the replica in leader's member_List, it is need to determine whether the schema exist
       *    If schema exist:
       *      Can not delete the records, the replica should remove member first
       *      (in ObRereplication::remove_permanent_offline_replicas).
       *      To ensure that the member is removed first, and then delete the meta table information,
       *      so as to avoid the occurrence of only_in_member_List
       *    If schema not exist:
       *      Because the meta information is not reliable, it can be deleted directly.
       *      At this time, there may be two situations:
       *        a.Schema is not exist, it is no problem to delete the record after remove member.
       *        b.Schema is exist, but misreport not exist(because of the lag of refresh schema and now
       *          schema module can not provide the deletion information of non tenant schema),
       *          delete record before remove member, it will cause the occurrence of only_in_member_list(badcase)
       * 3. Use get_partition_schema to determine whether the partition_schema exist or not
       */
      if (partition_info.in_physical_restore()) {
        is_lost_replica = true;
      } else if (!replica.is_in_service() || !ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_)) {
        is_lost_replica = true;
      } else {
        // go on check schema
      }
      if (OB_SUCC(ret) && !is_lost_replica) {
        /*
         * Badcase: Tenant_schema has refreshed without the table, and server is in permanently offline.
         *          There is a very small probability that only_in_member_list will happen.
         *          Manual method to reduce the probability of occurrence:
         *            1. Always use the newest tenant_guard.
         *            2. If the permanently_offline_time is enough, schema will be refreshed;
         *          If the replica is only in member list, it can not rereplication until remove member.
         */
        const ObPartitionSchema* partition_schema = NULL;
        share::schema::ObSchemaGetterGuard schema_guard;
        bool is_part_exist = true;
        const uint64_t schema_id = replica.table_id_;
        const uint64_t tenant_id = extract_tenant_id(schema_id);
        const ObSchemaType schema_type =
            is_new_tablegroup_id(schema_id) ? ObSchemaType::TABLEGROUP_SCHEMA : ObSchemaType::TABLE_SCHEMA;
        const int64_t partition_id = replica.partition_id_;
        bool check_dropped_partition = true;
        bool is_dropped = true;
        // check tenant exist
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(schema_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema service is null", K(ret));
          } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
            LOG_WARN("fail to get sys tenant schema guard", KR(ret));
          } else if (OB_SYS_TENANT_ID == tenant_id) {
            // nothing todo
          } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
            LOG_WARN("fail to check tenant has dropped", KR(ret), K(schema_id), K(tenant_id), K(partition_id));
          } else if (!is_dropped) {
            if (is_sys_table(schema_id)) {
              // During the physical recovery of the sys table, it will be failed to get tenant_guard
              // just get sys tenant guard as long as tenant is not dropped
            } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
              LOG_WARN("fail to get schema gurad", KR(ret), K(schema_id), K(tenant_id), K(partition_id));
            }
          } else {
            is_lost_replica = true;
          }
        }
        if (OB_SUCC(ret) && !is_dropped) {
          // 2. check table/tablegroup exist
          if (OB_FAIL(ObPartMgrUtils::get_partition_schema(schema_guard, schema_id, schema_type, partition_schema))) {
            if (OB_TABLE_NOT_EXIST != ret && OB_TABLEGROUP_NOT_EXIST != ret) {
              LOG_WARN("fail to get partition schema", KR(ret), K(schema_id), K(tenant_id), K(partition_id));
            } else {
              ret = OB_SUCCESS;
              // ignore failed to get schema
              is_dropped = true;
              is_lost_replica = true;
            }
          }
          // 3. check partition exist
          if (OB_SUCC(ret) && !is_dropped) {
            if (OB_ISNULL(partition_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("partition schema is null", KR(ret), K(schema_id), K(tenant_id), K(partition_id));
              is_lost_replica = true;  // schema has been dropped
            } else if (OB_FAIL(ObPartMgrUtils::check_part_exist(
                           *partition_schema, partition_id, check_dropped_partition, is_part_exist))) {
              LOG_WARN("check part exist failed", KR(ret), K(schema_id), K(partition_id));
            } else if (!is_part_exist) {
              is_lost_replica = true;  // partition has been dropped
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLostReplicaChecker::check_lost_server(const ObAddr& server, bool& is_lost_server) const
{
  int ret = OB_SUCCESS;
  is_lost_server = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (!server_manager_->has_build()) {
    is_lost_server = false;
  } else {
    ObServerStatus status;
    ret = server_manager_->get_server_status(server, status);
    if (OB_ENTRY_NOT_EXIST != ret && OB_SUCCESS != ret) {
      LOG_WARN("get_server_status failed", K(server), K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_lost_server = true;
      LOG_INFO("server not exist", K(server));
    } else if (ObServerStatus::OB_SERVER_ADMIN_DELETING == status.admin_status_) {
      // The server in deleting status may also be down or manually killed,
      // Permanent offline after downtime is lost server
      if (status.is_permanent_offline()) {
        is_lost_server = true;
      } else {
        is_lost_server = false;
      }
    } else {
      const int64_t now = ObTimeUtility::current_time();
      if (now - status.last_hb_time_ >= GCONF.replica_safe_remove_time ||
          now - status.last_hb_time_ >= GCONF.server_permanent_offline_time) {
        is_lost_server = true;
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
