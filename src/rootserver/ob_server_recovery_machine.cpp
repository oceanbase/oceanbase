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
#include "ob_server_recovery_machine.h"
#include "share/ob_define.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/queue/ob_dedup_queue.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_server_manager.h"
#include "ob_zone_manager.h"
#include "ob_root_service.h"
#include "ob_rs_event_history_table_operator.h"
namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
using namespace sql;
namespace rootserver {

int64_t UpdateFileRecoveryStatusTask::hash() const
{
  int64_t ret_v = 0;
  int64_t s1 = server_.hash();
  int64_t s2 = dest_server_.hash();
  ret_v = murmurhash(&s1, sizeof(s1), ret_v);
  ret_v = murmurhash(&s2, sizeof(s2), ret_v);
  ret_v = murmurhash(&tenant_id_, sizeof(tenant_id_), ret_v);
  ret_v = murmurhash(&file_id_, sizeof(file_id_), ret_v);
  return ret_v;
}

bool UpdateFileRecoveryStatusTask::operator==(const IObDedupTask& o) const
{
  const UpdateFileRecoveryStatusTask& that = static_cast<const UpdateFileRecoveryStatusTask&>(o);
  return server_ == that.server_ && dest_server_ == that.dest_server_ && tenant_id_ == that.tenant_id_ &&
         file_id_ == that.file_id_;
}

int64_t UpdateFileRecoveryStatusTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

common::IObDedupTask* UpdateFileRecoveryStatusTask::deep_copy(char* buf, const int64_t buf_size) const
{
  UpdateFileRecoveryStatusTask* task = nullptr;
  if (nullptr == buf || buf_size < get_deep_copy_size()) {
    LOG_WARN("invalid argument", KP(buf), K(buf_size), "need_size", get_deep_copy_size());
  } else {
    task = new (buf) UpdateFileRecoveryStatusTask(
        server_, dest_server_, tenant_id_, file_id_, pre_status_, cur_status_, is_stopped_, host_);
  }
  return task;
}

int UpdateFileRecoveryStatusTask::process()
{
  int ret = OB_SUCCESS;
  if (is_stopped_) {
    LOG_INFO("ObServerRecoveryMachine stopped");
  } else if (OB_FAIL(host_.update_file_recovery_status(
                 server_, dest_server_, tenant_id_, file_id_, pre_status_, cur_status_))) {
    LOG_WARN("fail to update file recovery status", K(ret));
  }
  return ret;
}

int64_t CheckPgRecoveryFinishedTask::hash() const
{
  return server_.hash();
}

bool CheckPgRecoveryFinishedTask::operator==(const IObDedupTask& o) const
{
  const CheckPgRecoveryFinishedTask& that = static_cast<const CheckPgRecoveryFinishedTask&>(o);
  return this->server_ == that.server_;
}

int64_t CheckPgRecoveryFinishedTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

common::IObDedupTask* CheckPgRecoveryFinishedTask::deep_copy(char* buf, const int64_t buf_size) const
{
  CheckPgRecoveryFinishedTask* task = nullptr;
  if (nullptr == buf || buf_size < get_deep_copy_size()) {
    LOG_WARN("invalid argument", KP(buf), K(buf_size), "need_size", get_deep_copy_size());
  } else {
    task =
        new (buf) CheckPgRecoveryFinishedTask(server_, is_stopped_, host_, pt_operator_, schema_service_, mysql_proxy_);
  }
  return task;
}

int CheckPgRecoveryFinishedTask::process()
{
  int ret = OB_SUCCESS;
  bool is_finished = false;
  if (is_stopped_) {
    LOG_INFO("host ObServerRecoveryMachine stopped");
    host_.on_check_pg_recovery_finished(server_, OB_CANCELED);
  } else {
    ret = do_check_pg_recovery_finished(is_finished);
    if (OB_SUCCESS != ret) {
      LOG_WARN("check pg recovery finished failed", K(ret));
      host_.on_check_pg_recovery_finished(server_, OB_EAGAIN);
    } else if (!is_finished) {
      LOG_INFO("pg recovery still not finished", "server", server_);
      host_.on_check_pg_recovery_finished(server_, OB_EAGAIN);
    } else {
      host_.on_check_pg_recovery_finished(server_, OB_SUCCESS);
    }
  }
  return ret;
}

int CheckPgRecoveryFinishedTask::do_check_pg_recovery_finished(bool& is_finished)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  if (is_stopped_) {
    LOG_INFO("host ObServerRecoveryMachine stopped");
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(&schema_service_, tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else {
    is_finished = true;
    LOG_INFO("do check pg recovery finished", K(server_));
    for (int64_t i = 0; is_finished && OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      share::schema::ObSchemaGetterGuard schema_guard;
      const uint64_t tenant_id = tenant_ids.at(i);
      ObRefreshSchemaStatus schema_status;
      schema_status.tenant_id_ = tenant_id;
      int64_t version_in_inner_table = OB_INVALID_VERSION;
      int64_t local_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_ids.at(i), schema_guard))) {
        LOG_WARN("fail to get schema guard", "tenant_id", tenant_ids.at(i));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
        LOG_WARN("fail to get schema version from guard", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service_.get_schema_version_in_inner_table(
                     mysql_proxy_, schema_status, version_in_inner_table))) {
        LOG_WARN("fail to get version in inner table", K(ret));
      } else if (version_in_inner_table > local_schema_version) {
        is_finished = false;  // this server may not get the newest schema, wait
      } else {
        ObTenantPartitionIterator iter;
        ObPartitionInfo partition_info;
        if (OB_FAIL(iter.init(pt_operator_, schema_service_, tenant_id, true /*ignore row checksum*/))) {
          LOG_WARN("fail to init partition table iterator", K(ret));
        } else if (OB_FAIL(iter.get_filters().set_replica_status(REPLICA_STATUS_NORMAL))) {
          LOG_WARN("fail to set filter", K(ret));
        } else if (OB_FAIL(iter.get_filters().filter_restore_replica())) {
          LOG_WARN("fail to set filter", K(ret));
        } else {
          while (is_finished && OB_SUCC(ret) && OB_SUCC(iter.next(partition_info))) {
            const share::ObPartitionReplica* leader_replica = nullptr;
            if (is_stopped_) {
              is_finished = false;
              LOG_INFO("host ObServerRecoveryMachine stopped");
            } else if (is_inner_table(partition_info.get_table_id())) {
              int tmp_ret = partition_info.find_leader_v2(leader_replica);
              if (OB_ENTRY_NOT_EXIST == tmp_ret) {
                is_finished = false;
                LOG_INFO("partition has no leader",
                    "table_id",
                    partition_info.get_table_id(),
                    "partition_id",
                    partition_info.get_partition_id());
              } else if (OB_SUCCESS == tmp_ret) {
                for (int64_t i = 0; is_finished && OB_SUCC(ret) && i < leader_replica->member_list_.count(); ++i) {
                  const common::ObAddr& this_server = leader_replica->member_list_.at(i).server_;
                  if (server_ == this_server) {
                    is_finished = false;
                    LOG_INFO("still has replica on server",
                        "table_id",
                        partition_info.get_table_id(),
                        "partition_id",
                        partition_info.get_partition_id());
                  } else {
                  }  // go on to check next
                }
              } else {
                ret = tmp_ret;
                LOG_WARN("fail to find leader",
                    K(ret),
                    "table_id",
                    partition_info.get_table_id(),
                    "partition_id",
                    partition_info.get_partition_id());
              }
            } else {
              int tmp_ret = partition_info.find_leader_v2(leader_replica);
              if (OB_ENTRY_NOT_EXIST == tmp_ret) {
                is_finished = false;
                LOG_INFO("partition has no leader",
                    "table_id",
                    partition_info.get_table_id(),
                    "partition_id",
                    partition_info.get_partition_id());
              } else if (OB_SUCCESS == tmp_ret) {
                if (nullptr == leader_replica) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("leader replica ptr is null",
                      "table_id",
                      partition_info.get_table_id(),
                      "partition_id",
                      partition_info.get_partition_id());
                } else if (leader_replica->server_ == server_) {
                  is_finished = false;
                  LOG_INFO("still has replica leader on server",
                      "tabld_id",
                      partition_info.get_table_id(),
                      "partition_id",
                      partition_info.get_partition_id());
                } else {
                }  // leader not on this server, good
              } else {
                ret = tmp_ret;
                LOG_WARN("fail to find leader",
                    K(ret),
                    "table_id",
                    partition_info.get_table_id(),
                    "partition_id",
                    partition_info.get_partition_id());
              }
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

int RefugeeInfo::assign(const RefugeeInfo& that)
{
  int ret = OB_SUCCESS;
  server_ = that.server_;
  tenant_id_ = that.tenant_id_;
  file_id_ = that.file_id_;
  dest_server_ = that.dest_server_;
  file_recovery_status_ = that.file_recovery_status_;
  return ret;
}

ObServerRecoveryTask::ObServerRecoveryTask()
    : server_(),
      rescue_server_(),
      progress_(ServerRecoveryProgress::SRP_INVALID),
      refugee_infos_(),
      split_server_log_status_(SplitServerLogStatus::SSLS_INVALID),
      recover_file_task_gen_status_(RecoverFileTaskGenStatus::RFTGS_INVALID),
      check_pg_recovery_finished_status_(CheckPgRecoveryFinishedStatus::CPRFS_INVALID),
      last_drive_ts_(0),
      lock_()
{}

ObServerRecoveryTask::~ObServerRecoveryTask()
{}

int ObServerRecoveryTask::get_refugee_info(
    const uint64_t tenant_id, const int64_t file_id, const common::ObAddr& dest_server, RefugeeInfo*& refugee_info)
{
  int ret = OB_SUCCESS;
  refugee_info = nullptr;
  bool found = false;
  for (int64_t i = 0; !found && i < refugee_infos_.count(); ++i) {
    RefugeeInfo& this_refugee_info = refugee_infos_.at(i);
    if (tenant_id == this_refugee_info.tenant_id_ && file_id == this_refugee_info.file_id_ &&
        dest_server == this_refugee_info.dest_server_) {
      found = true;
      refugee_info = &this_refugee_info;
    } else {
      // go on checking next
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

ObServerRecoveryMachine::ObServerRecoveryMachine()
    : inited_(false),
      loaded_(false),
      rpc_proxy_(nullptr),
      mysql_proxy_(nullptr),
      server_mgr_(nullptr),
      schema_service_(nullptr),
      pt_operator_(nullptr),
      rebalance_task_mgr_(nullptr),
      zone_mgr_(nullptr),
      unit_mgr_(nullptr),
      empty_server_checker_(nullptr),
      task_allocator_(),
      task_map_(),
      task_map_lock_(),
      idling_(stop_),
      rescue_server_counter_(),
      check_pg_recovery_finished_task_queue_(),
      datafile_recovery_status_task_queue_(),
      seq_generator_()
{}

ObServerRecoveryMachine::~ObServerRecoveryMachine()
{
  check_pg_recovery_finished_task_queue_.destroy();
  datafile_recovery_status_task_queue_.destroy();
}

int ObServerRecoveryMachine::init(obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* mysql_proxy,
    rootserver::ObServerManager* server_mgr, share::schema::ObMultiVersionSchemaService* schema_service,
    share::ObPartitionTableOperator* pt_operator, rootserver::ObRebalanceTaskMgr* rebalance_task_mgr,
    rootserver::ObZoneManager* zone_mgr, rootserver::ObUnitManager* unit_mgr,
    rootserver::ObEmptyServerChecker* empty_server_checker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy || nullptr == mysql_proxy || nullptr == server_mgr ||
                         nullptr == schema_service || nullptr == pt_operator || nullptr == rebalance_task_mgr ||
                         nullptr == unit_mgr || nullptr == zone_mgr || nullptr == empty_server_checker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        KP(rpc_proxy),
        KP(mysql_proxy),
        KP(server_mgr),
        KP(schema_service),
        KP(pt_operator),
        KP(rebalance_task_mgr),
        K(zone_mgr),
        KP(unit_mgr));
  } else if (OB_FAIL(task_map_.create(TASK_MAP_BUCKET_NUM, ObModIds::OB_SERVER_RECOVERY_MACHINE))) {
    LOG_WARN("fail to create task map", K(ret));
  } else if (OB_FAIL(rescue_server_counter_.create(TASK_MAP_BUCKET_NUM, ObModIds::OB_SERVER_RECOVERY_MACHINE))) {
    LOG_WARN("fail to create rescue server counter map", K(ret));
  } else if (OB_FAIL(ObRsReentrantThread::create(THREAD_CNT))) {
    LOG_WARN("fail to create server recovery machine", K(ret));
  } else if (OB_FAIL(check_pg_recovery_finished_task_queue_.init(CHECK_PG_RECOVERY_FINISHED_THREAD_CNT))) {
    LOG_WARN("fail to init check pg recovery finished task queue", K(ret));
  } else if (OB_FAIL(datafile_recovery_status_task_queue_.init(DATAFILE_RECOVERY_STATUS_THREAD_CNT))) {
    LOG_WARN("fail to init datafile recovery status task queue", K(ret));
  } else {
    check_pg_recovery_finished_task_queue_.set_label(ObModIds::OB_SERVER_RECOVERY_MACHINE);
    datafile_recovery_status_task_queue_.set_label(ObModIds::OB_SERVER_RECOVERY_MACHINE);
    rpc_proxy_ = rpc_proxy;
    mysql_proxy_ = mysql_proxy;
    server_mgr_ = server_mgr;
    schema_service_ = schema_service;
    pt_operator_ = pt_operator;
    rebalance_task_mgr_ = rebalance_task_mgr;
    zone_mgr_ = zone_mgr;
    unit_mgr_ = unit_mgr;
    empty_server_checker_ = empty_server_checker;
    inited_ = true;
  }
  return ret;
}

int ObServerRecoveryMachine::reload_server_recovery_status()
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = nullptr;
    ObTimeoutCtx ctx;

    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ServerRecoveryMachine not init", K(ret));
    } else if (OB_UNLIKELY(nullptr == mysql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql proxy ptr is null", K(ret));
    } else if (0 != task_map_.size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task map should be empty before reload", K(ret));
    } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql_string.assign_fmt("SELECT * FROM %s ", OB_ALL_SERVER_RECOVERY_STATUS_TNAME))) {
      LOG_WARN("fail to append sql", K(ret));
    } else if (OB_FAIL(mysql_proxy_->read(res, sql_string.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql_string));
    } else if (OB_UNLIKELY(nullptr == (result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        ObServerRecoveryTask* task_ptr = task_allocator_.alloc();
        if (OB_UNLIKELY(nullptr == task_ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_FAIL(fill_server_recovery_task_result(result, task_ptr))) {
          LOG_WARN("fail to fill server recovery task sresult", K(ret));
        } else if (OB_FAIL(task_map_.set_refactored(task_ptr->server_, task_ptr))) {
          LOG_WARN("fail to set refactored", K(ret));
        } else {
          int64_t cnt = -1;
          int tmp_ret = rescue_server_counter_.get_refactored(task_ptr->rescue_server_, cnt);
          if (OB_HASH_NOT_EXIST == tmp_ret) {
            int32_t overwrite = 0;
            if (OB_FAIL(rescue_server_counter_.set_refactored(task_ptr->rescue_server_, 1 /* set to 1 */, overwrite))) {
              LOG_WARN("fail to set refactored", K(ret));
            }
          } else if (OB_SUCCESS == tmp_ret) {
            int32_t overwrite = 1;
            if (OB_FAIL(rescue_server_counter_.set_refactored(task_ptr->rescue_server_, cnt + 1, overwrite))) {
              LOG_WARN("fail to set refactored", K(ret));
            }
          } else {
            ret = tmp_ret;
            LOG_WARN("fail to get from hash map", K(ret));
          }
        }
        if (OB_FAIL(ret) && nullptr != task_ptr) {
          task_allocator_.free(task_ptr);
          task_ptr = nullptr;
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::fill_server_file_recovery_status(
    common::sqlclient::ObMySQLResult* result, ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryMachine not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == result || nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumemnt", K(ret), KP(result), KP(task));
  } else {
    RefugeeInfo refugee_info;
    ObMySQLResult& res = *result;
    int64_t str_len1 = 0;
    int64_t str_len2 = 0;
    char svr_ip[OB_IP_STR_BUFF] = "";
    int64_t svr_port = 0;
    char dest_svr_ip[OB_IP_STR_BUFF] = "";
    int64_t dest_svr_port = 0;
    uint64_t dest_unit_id = 0;
    uint64_t tenant_id = OB_INVALID_ID;
    int64_t file_id = -1;
    int64_t status = 0;
    EXTRACT_STRBUF_FIELD_MYSQL(res, "svr_ip", svr_ip, OB_IP_STR_BUFF, str_len1);
    EXTRACT_INT_FIELD_MYSQL(res, "svr_port", svr_port, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "tenant_id", tenant_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "file_id", file_id, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(res, "dest_svr_ip", dest_svr_ip, OB_IP_STR_BUFF, str_len2);
    EXTRACT_INT_FIELD_MYSQL(res, "dest_svr_port", dest_svr_port, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "dest_unit_id", dest_unit_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "status", status, int64_t);
    if (!refugee_info.server_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set ip addr unexpected", K(ret));
    } else if (!refugee_info.dest_server_.set_ip_addr(dest_svr_ip, static_cast<int32_t>(dest_svr_port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set ip addr unexpected", K(ret));
    } else {
      refugee_info.tenant_id_ = tenant_id;
      refugee_info.file_id_ = file_id;
      refugee_info.dest_unit_id_ = dest_unit_id;
      refugee_info.file_recovery_status_ = static_cast<FileRecoveryStatus>(status);
      if (OB_FAIL(task->refugee_infos_.push_back(refugee_info))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::reload_file_recovery_status_by_server(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = nullptr;
    char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    ObTimeoutCtx ctx;

    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ServerRecoveryMachine not init", K(ret));
    } else if (OB_UNLIKELY(nullptr == task)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_UNLIKELY(nullptr == mysql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql proxy ptr is null", K(ret));
    } else if (ServerRecoveryProgress::SRP_IN_PG_RECOVERY != task->progress_) {
      /* bypass, no need to reload file recovery status,
       * since task progress not in pg recovery
       */
    } else if (!task->server_.ip_to_string(svr_ip, sizeof(svr_ip))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to convert ip to string", K(ret));
    } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql_string.assign_fmt("SELECT * FROM %s WHERE "
                                             "svr_ip = '%s' AND svr_port = %ld",
                   OB_ALL_DATAFILE_RECOVERY_STATUS_TNAME,
                   svr_ip,
                   static_cast<int64_t>(task->server_.get_port())))) {
      LOG_WARN("fail to append sql", K(ret));
    } else if (OB_FAIL(mysql_proxy_->read(res, sql_string.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql_string));
    } else if (OB_UNLIKELY(nullptr == (result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        if (OB_FAIL(fill_server_file_recovery_status(result, task))) {
          LOG_WARN("fail to fill server file recovery status", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        if (task->refugee_infos_.count() > 0) {
          task->recover_file_task_gen_status_ = RecoverFileTaskGenStatus::RFTGS_TASK_GENERATED;
        }
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::reload_file_recovery_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryMachine not init", K(ret));
  } else {
    for (task_iterator iter = task_map_.begin(); OB_SUCC(ret) && iter != task_map_.end(); ++iter) {
      ObServerRecoveryTask* task = iter->second;
      if (OB_UNLIKELY(nullptr == task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task ptr is null", K(ret));
      } else if (OB_FAIL(reload_file_recovery_status_by_server(task))) {
        LOG_WARN("fail to reload file recovery status by server", K(ret));
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::reload_server_recovery_machine()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryMachine not init", K(ret));
  } else if (OB_FAIL(reset_run_condition())) {
    LOG_WARN("fail to reset run condition", K(ret));
  } else {
    SpinWLockGuard guard(task_map_lock_);
    if (OB_FAIL(reload_server_recovery_status())) {
      LOG_WARN("fail to reload server recovery status", K(ret));
    } else if (OB_FAIL(reload_file_recovery_status())) {
      LOG_WARN("fail to reload file recovery status", K(ret));
    } else {
      loaded_ = true;
    }
  }
  return ret;
}

int ObServerRecoveryMachine::fill_server_recovery_task_result(ObMySQLResult* result, ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ServerRecoveryMachine not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == result || nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(result), KP(task));
  } else {
    ObMySQLResult& res = *result;
    int64_t str_len1 = 0;
    int64_t str_len2 = 0;
    char svr_ip[OB_IP_STR_BUFF] = "";
    int64_t svr_port = 0;
    char rescue_svr_ip[OB_IP_STR_BUFF] = "";
    int64_t rescue_svr_port = 0;
    int64_t progress = 0;
    EXTRACT_STRBUF_FIELD_MYSQL(res, "svr_ip", svr_ip, OB_IP_STR_BUFF, str_len1);
    EXTRACT_INT_FIELD_MYSQL(res, "svr_port", svr_port, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(res, "rescue_svr_ip", rescue_svr_ip, OB_IP_STR_BUFF, str_len2);
    EXTRACT_INT_FIELD_MYSQL(res, "rescue_svr_port", rescue_svr_port, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "rescue_progress", progress, int64_t);
    if (!task->server_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set ip addr unexpected", K(ret));
    } else if (!task->rescue_server_.set_ip_addr(rescue_svr_ip, static_cast<int32_t>(rescue_svr_port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set rescue ip add unexpected", K(ret));
    } else {
      task->progress_ = static_cast<ServerRecoveryProgress>(progress);
    }
  }
  return ret;
}

int ObServerRecoveryMachine::reset_run_condition()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SpinWLockGuard guard(task_map_lock_);
    for (task_iterator iter = task_map_.begin(); iter != task_map_.end(); ++iter) {
      ObServerRecoveryTask* task = iter->second;
      if (nullptr != task) {
        task_allocator_.free(task);  // will deconstructing in free
        task = nullptr;
      }
    }
    task_map_.reuse();
    loaded_ = false;
  }
  return ret;
}

void ObServerRecoveryMachine::wakeup()
{
  idling_.wakeup();
}

void ObServerRecoveryMachine::run3()
{
  LOG_INFO("server recovery machine starts");
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    int ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else {
    while (!stop_) {
      {
        idling_.idle(IDLING_US);
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = discover_new_server_recovery_task())) {
        LOG_WARN("fail to discover new server recovery task", K(tmp_ret));
      }
      if (OB_SUCCESS != (tmp_ret = drive_existing_server_recovery_task())) {
        LOG_WARN("fail to drive existing server recovery task", K(tmp_ret));
      }
      if (OB_SUCCESS != (tmp_ret = clean_finished_server_recovery_task())) {
        LOG_WARN("fail to clean finished server recovery task", K(tmp_ret));
      }
    }
    if (OB_SUCCESS != reset_run_condition()) {
      LOG_WARN("fail to reset run condition");
    }
  }
  LOG_INFO("server recovery machine exits");
}

int ObServerRecoveryMachine::discover_new_server_recovery_task()
{
  int ret = OB_SUCCESS;
  common::ObZone empty_zone;
  common::ObArray<common::ObAddr> servers_takenover_by_rs;
  ObTimeoutCtx ctx;

  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr null", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine is stopped");
  } else if (OB_FAIL(server_mgr_->get_servers_takenover_by_rs(empty_zone, servers_takenover_by_rs))) {
    LOG_WARN("fail to get servers taken over by rs", K(ret));
  } else if (servers_takenover_by_rs.count() <= 0) {
    // bypass, since there is no server taken over by rs
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_takenover_by_rs.count(); ++i) {
      ObMySQLTransaction trans;
      ObSqlString sql_string;
      char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
      ObServerRecoveryTask* new_task = nullptr;
      const common::ObAddr& this_server = servers_takenover_by_rs.at(i);
      int64_t affected_rows = 0;
      ObTimeoutCtx ctx;
      bool does_task_exist = false;
      {
        SpinRLockGuard read_guard(task_map_lock_);
        const ObServerRecoveryTask* const* task_ptr = task_map_.get(this_server);
        does_task_exist = (nullptr != task_ptr);
      }
      if (does_task_exist) {
        // task already exist, no need to process
      } else if (!this_server.ip_to_string(ip, sizeof(ip))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to convert ip to string", K(ret));
      } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
      } else if (OB_UNLIKELY(nullptr == (new_task = task_allocator_.alloc()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (OB_FAIL(sql_string.assign_fmt("INSERT INTO %s "
                                               "(SVR_IP, SVR_PORT, RESCUE_SVR_IP, RESCUE_SVR_PORT, RESCUE_PROGRESS) "
                                               "VALUES ('%s', %ld, '%s', %ld, %ld)",
                     OB_ALL_SERVER_RECOVERY_STATUS_TNAME,
                     ip,
                     static_cast<int64_t>(this_server.get_port()),
                     "" /* rescue ip */,
                     0L /*rescue port*/,
                     static_cast<int64_t>(ServerRecoveryProgress::SRP_SPLIT_SERVER_LOG)))) {
        LOG_WARN("fail to assign format", K(ret));
      } else if (OB_FAIL(trans.start(mysql_proxy_))) {
        LOG_WARN("fail to start trans");
      } else {
        if (OB_FAIL(trans.write(OB_SYS_TENANT_ID, sql_string.ptr(), affected_rows))) {
          LOG_WARN("fail to execute write sql", K(ret));
        } else if (1 != affected_rows) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected rows unexpected", K(ret), K(sql_string), K(affected_rows));
        } else {
          new_task->server_ = this_server;
          // --->new_task->rescue_server_ no need to set
          new_task->progress_ = ServerRecoveryProgress::SRP_SPLIT_SERVER_LOG;
          // --->refugee_pg_infos_ no need to set
          new_task->last_drive_ts_ = 0;
          // hashmap's insertion is thread safe without locking
          if (OB_FAIL(task_map_.set_refactored(new_task->server_, new_task))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("fail to end trans", K(tmp_ret), "is_commit", OB_SUCC(ret));
          ret = (OB_SUCCESS == ret ? tmp_ret : ret);
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("submit server recovery task", "server", new_task->server_);
        }
      }
      if (OB_FAIL(ret) && nullptr != new_task) {
        SpinWLockGuard write_guard(task_map_lock_);
        task_map_.erase_refactored(this_server);
        task_allocator_.free(new_task);
        new_task = nullptr;
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::drive_existing_server_recovery_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (stop_) {
    LOG_INFO("server mgr ptr null", K(ret));
  } else {
    SpinRLockGuard guard(task_map_lock_);
    if (task_map_.size() > 0) {
      for (task_iterator iter = task_map_.begin(); !stop_ && iter != task_map_.end(); ++iter) {
        ObServerRecoveryTask* task = iter->second;
        int tmp_ret = OB_SUCCESS;
        if (nullptr == task) {
          // bypass
        } else if (task->progress_ < ServerRecoveryProgress::SRP_SPLIT_SERVER_LOG ||
                   task->progress_ > ServerRecoveryProgress::SRP_CHECK_PG_RECOVERY_FINISHED) {
          // do nothing
        } else if (OB_SUCCESS != (tmp_ret = try_drive(task))) {
          LOG_WARN("fail to drive task status", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::clean_finished_server_recovery_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (stop_) {
    LOG_INFO("server mgr ptr null", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_ || nullptr == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr is null", K(ret), KP(server_mgr_));
  } else if (OB_UNLIKELY(nullptr == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mysql proxy ptr is null", K(ret));
  } else if (OB_UNLIKELY(nullptr == empty_server_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty server checker ptr is null", K(ret));
  } else {
    SpinWLockGuard guard(task_map_lock_);
    if (task_map_.size() > 0) {
      for (task_iterator iter = task_map_.begin(); !stop_ && iter != task_map_.end(); ++iter) {
        bool server_empty = false;
        share::ObServerStatus server_status;
        ObServerRecoveryTask* task = iter->second;
        if (nullptr == task) {
          // bypass
        } else if (ServerRecoveryProgress::SRP_SERVER_RECOVERY_FINISHED != task->progress_) {
          // bypass
        } else if (FALSE_IT(empty_server_checker_->notify_check())) {
          // shall never be here
        } else if (OB_FAIL(unit_mgr_->check_server_empty(iter->first, server_empty))) {
          LOG_WARN("fail to check server empty", K(ret), "server", iter->first);
        } else if (!server_empty) {
          LOG_INFO("server not empty, may have unit on", "server", iter->first);
        } else if (FALSE_IT((void)server_mgr_->set_force_stop_hb(iter->first, true /*stop*/))) {
          // shall never be here
        } else if (FALSE_IT(ret = server_mgr_->get_server_status(iter->first, server_status))) {
          // shall never be here
        } else if (OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret) {
          char svr_ip_str[OB_IP_STR_BUFF] = "";
          ObMySQLTransaction trans;
          ObSqlString sql_string;
          ObSqlString sql_string2;
          int64_t affected_rows = 0;
          ObTimeoutCtx ctx;
          bool clean_result = true;
          if (OB_SUCCESS == ret) {
            if (server_status.with_partition_) {
              clean_result = false;  // still has partition
            } else if (OB_FAIL(server_mgr_->finish_server_recovery(iter->first))) {
              LOG_WARN("fail to finish server recovery", "server", iter->first);
            }
          } else {  // OB_ENTRY_NOT_EXIST
            clean_result = true;
            // it has beed deleted successfully, not exist in server manager
            ret = OB_SUCCESS;
          }

          if (OB_FAIL(ret)) {
          } else if (!clean_result) {
            // bypass, since server not empty
          } else {
            if (!task->server_.ip_to_string(svr_ip_str, sizeof(svr_ip_str))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to convert ip to string", K(ret));
            } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
              LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
            } else if (OB_FAIL(sql_string.assign_fmt("DELETE FROM %s WHERE SVR_IP = '%s' AND SVR_PORT = %ld",
                           OB_ALL_SERVER_RECOVERY_STATUS_TNAME,
                           svr_ip_str,
                           static_cast<int64_t>(task->server_.get_port())))) {
              LOG_WARN("fail to assign fmt", K(ret));
            } else if (OB_FAIL(sql_string2.assign_fmt("DELETE FROM %s WHERE SVR_IP = '%s' AND SVR_PORT = %ld",
                           OB_ALL_DATAFILE_RECOVERY_STATUS_TNAME,
                           svr_ip_str,
                           static_cast<int64_t>(task->server_.get_port())))) {
              LOG_WARN("fail to assign fmt", K(ret));
            } else if (OB_FAIL(trans.start(mysql_proxy_))) {
              LOG_WARN("fail to start trans", K(ret));
            } else if (OB_FAIL(trans.write(sql_string.ptr(), affected_rows))) {
              LOG_WARN("fail to execute sql", K(ret), K(sql_string));
            } else if (OB_FAIL(trans.write(sql_string2.ptr(), affected_rows))) {
              LOG_WARN("fail to execute sql", K(ret), K(sql_string));
            }
            if (trans.is_started()) {
              int tmp_ret = OB_SUCCESS;
              if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
                LOG_WARN("fail to end trans", K(tmp_ret));
                ret = OB_SUCC(ret) ? tmp_ret : ret;
              }
            }
            if (OB_SUCC(ret)) {
              ROOTSERVICE_EVENT_ADD("server_recovery_machine",
                  "finish_recovery",
                  "server",
                  task->server_,
                  "rescue_server",
                  task->rescue_server_);
              if (OB_FAIL(task_map_.erase_refactored(iter->first))) {
                LOG_WARN("fail to erase item from hash map", K(ret), "server", iter->first);
              } else {
                task_allocator_.free(task);
                task = nullptr;
              }
            }
          }
        } else {
          ret = OB_SUCCESS;  // rewrite, and go on check next
          LOG_WARN("fail to get server status", K(ret), "server", iter->first);
        }
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::try_drive(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else {
    ServerRecoveryProgress recovery_progress = task->progress_;
    switch (recovery_progress) {
      case ServerRecoveryProgress::SRP_SPLIT_SERVER_LOG:
        // split slog/clog
        if (OB_FAIL(try_split_server_log(task))) {
          LOG_WARN("fail to try split server log", K(ret));
        }
        break;
      case ServerRecoveryProgress::SRP_IN_PG_RECOVERY:
        // recovery pg
        if (OB_FAIL(try_recover_pg(task))) {
          LOG_WARN("fail to try recover pg", K(ret));
        }
        break;
      case ServerRecoveryProgress::SRP_CHECK_PG_RECOVERY_FINISHED:
        // check meta_table has beed update complete after recovery pg
        if (OB_FAIL(try_check_pg_recovery_finished(task))) {
          LOG_WARN("fail to try check pg recovery finished", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected server recovery status", K(ret), K(recovery_progress));
        break;
    }
  }
  return ret;
}

int ObServerRecoveryMachine::try_split_server_log(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else {
    SpinWLockGuard guard(task->lock_);
    if (ServerRecoveryProgress::SRP_SPLIT_SERVER_LOG != task->progress_) {
      // bypass, task progress may be pushed forward by other routine
    } else if (task->last_drive_ts_ + SPLIT_SERVER_LOG_TIMEOUT > ObTimeUtility::current_time()) {
      if (OB_FAIL(drive_this_split_server_log(task))) {
        LOG_WARN("fail to drive this split server log", K(ret));
      }
    } else {
      if (OB_FAIL(launch_new_split_server_log(task))) {
        LOG_WARN("fail to launch new split server log", K(ret));
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::drive_this_split_server_log(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else if (SplitServerLogStatus::SSLS_INVALID == task->split_server_log_status_) {
    // go on wait
  } else if (SplitServerLogStatus::SSLS_SUCCEED == task->split_server_log_status_) {
    (void)switch_state(task, ServerRecoveryProgress::SRP_IN_PG_RECOVERY);
  } else if (SplitServerLogStatus::SSLS_FAILED == task->split_server_log_status_) {
    task->last_drive_ts_ = 0;  // retry immediately
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("revovery machine split server log status error", K(ret), "server", task->server_);
  }
  return ret;
}

int ObServerRecoveryMachine::launch_new_split_server_log(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr is null", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else {
    bool allocate_new_rescue_server = false;
    // check need to distribute new rescue server
    bool is_active = false;
    common::ObAddr& rescue_server = task->rescue_server_;
    common::ObAddr new_rescue_server;
    if (!rescue_server.is_valid()) {
      allocate_new_rescue_server = true;
    } else if (OB_FAIL(server_mgr_->check_server_active(rescue_server, is_active))) {
      LOG_WARN("fail to check server active", K(ret), K(rescue_server));
    } else if (!is_active) {
      allocate_new_rescue_server = true;
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!allocate_new_rescue_server) {
      new_rescue_server = rescue_server;
    } else {
      if (OB_FAIL(pick_new_rescue_server(new_rescue_server))) {
        LOG_WARN("fail to pick new rescue server", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_launch_new_split_server_log(task->server_, new_rescue_server, allocate_new_rescue_server))) {
        LOG_WARN("fail to do launch new split server log",
            K(ret),
            "server",
            task->server_,
            "rescue_server",
            new_rescue_server,
            K(allocate_new_rescue_server));
      } else {
        task->rescue_server_ = new_rescue_server;
        task->split_server_log_status_ = SplitServerLogStatus::SSLS_INVALID;
        task->last_drive_ts_ = common::ObTimeUtility::current_time();
        LOG_INFO("launch new split server log succeed", "task", *task);
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::pick_new_rescue_server(common::ObAddr& new_rescue_server)
{
  int ret = OB_SUCCESS;
  common::ObZone empty_zone;
  common::ObArray<common::ObAddr> server_array;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr ptr null", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine is stopped");
  } else if (OB_FAIL(server_mgr_->get_servers_of_zone(empty_zone, server_array))) {
    LOG_WARN("fail to get servers taken over by rs", K(ret));
  } else {
    common::ObAddr server_with_min_cnt;
    int64_t min_cnt = 0;
    bool no_active_server = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_array.count(); ++i) {
      bool is_active = false;
      const common::ObAddr& this_server = server_array.at(i);
      int64_t this_cnt = -1;
      if (OB_FAIL(server_mgr_->check_server_active(this_server, is_active))) {
        LOG_WARN("fail to check server active", K(ret));
      } else if (!is_active) {
        // not active
      } else {
        no_active_server = false;
        int tmp_ret = rescue_server_counter_.get_refactored(this_server, this_cnt);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          server_with_min_cnt = this_server;
          min_cnt = 0;
        } else if (OB_SUCCESS == tmp_ret) {
          if (this_cnt < min_cnt) {
            min_cnt = this_cnt;
            server_with_min_cnt = this_server;
          }
        } else {
          ret = tmp_ret;
          LOG_WARN("fail to get refactored", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (no_active_server) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no active server found to execute split log task", K(ret));
    } else {
      new_rescue_server = server_with_min_cnt;
      LOG_INFO("pick new rescue server", K(new_rescue_server));
    }
  }
  return ret;
}

int ObServerRecoveryMachine::do_launch_new_split_server_log(
    const common::ObAddr& server, const common::ObAddr& rescue_server, const bool is_new_rescue_server)
{
  int ret = OB_SUCCESS;
  // obrpc::ObSplitServerLogArg arg;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(rescue_server));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy ptr is nullptr", K(ret));
    //  } else if (OB_FAIL(arg.init(server, rescue_server))) {
    //    LOG_WARN("fail to init rpc arg", K(ret));
  } else {
    ObMySQLTransaction trans;
    ObSqlString sql_string;
    char rescue_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    int64_t affected_rows = 0;
    ObTimeoutCtx ctx;
    if (!server.ip_to_string(ip, sizeof(ip))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to convert ip to string", K(ret));
    } else if (!rescue_server.ip_to_string(rescue_ip, sizeof(rescue_ip))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to convert ip to string", K(ret));
    } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql_string.assign_fmt("UPDATE %s SET RESCUE_SVR_IP = '%s', RESCUE_SVR_PORT = %ld "
                                             "WHERE SVR_IP = '%s' AND SVR_PORT = %ld ",
                   OB_ALL_SERVER_RECOVERY_STATUS_TNAME,
                   rescue_ip,
                   static_cast<int64_t>(rescue_server.get_port()),
                   ip,
                   static_cast<int64_t>(server.get_port())))) {
      LOG_WARN("fail to assign format", K(ret));
    } else if (OB_FAIL(trans.start(mysql_proxy_))) {
      LOG_WARN("fail to start trans");
    } else if (OB_FAIL(trans.write(OB_SYS_TENANT_ID, sql_string.ptr(), affected_rows))) {
      LOG_WARN("fail to execute write sql", K(ret));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to end trans", K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else {
      //(void)rpc_proxy_->to(rescue_server).split_server_log(arg);
      if (is_new_rescue_server) {
        // try update rescue_server_counter, ignore ret
        int64_t cnt = -1;
        int tmp_ret = rescue_server_counter_.get_refactored(rescue_server, cnt);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          int32_t overwrite = 0;
          if (OB_SUCCESS !=
              (tmp_ret = rescue_server_counter_.set_refactored(rescue_server, 1 /* set to 1 */, overwrite))) {
            LOG_WARN("fail to set refactored", K(tmp_ret));
          }
        } else if (OB_SUCCESS == tmp_ret) {
          int32_t overwrite = 1;
          if (OB_SUCCESS != (tmp_ret = rescue_server_counter_.set_refactored(rescue_server, cnt + 1, overwrite))) {
            LOG_WARN("fail to set refactored", K(tmp_ret));
          }
        } else {
          LOG_WARN("fail to get from hash map", K(tmp_ret));
        }
      }
    }
    LOG_INFO("split server log", K(ret));
  }
  return ret;
}

int ObServerRecoveryMachine::on_split_server_log_reply(
    const common::ObAddr& server, const common::ObAddr& rescue_server, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (stop_) {
    LOG_INFO("ObServerRecoveryMachine stopped");
  } else if (OB_UNLIKELY(!server.is_valid() || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(rescue_server));
  } else {
    int64_t cnt = 0;
    LOG_INFO("on split server log succeed", K(server), K(rescue_server), K(ret_code));
    ObServerRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(server, task))) {
      LOG_WARN("fail to get from map", K(ret), K(server));
    } else if (OB_UNLIKELY(nullptr == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(server));
    } else {
      SpinWLockGuard item_guard(task->lock_);
      if (ServerRecoveryProgress::SRP_SPLIT_SERVER_LOG != task->progress_) {
        LOG_INFO("state not match, maybe previous task response", K(server), K(rescue_server));
      } else if (rescue_server != task->rescue_server_) {
        LOG_INFO("rescue server not match",
            K(server),
            "local_rescue_server",
            task->rescue_server_,
            "incoming_rescue_server",
            rescue_server);
      } else if (OB_SUCCESS == ret_code) {
        task->split_server_log_status_ = SplitServerLogStatus::SSLS_SUCCEED;
      } else {
        task->split_server_log_status_ = SplitServerLogStatus::SSLS_FAILED;
      }
    }

    int tmp_ret = rescue_server_counter_.get_refactored(rescue_server, cnt);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // ignore
    } else if (OB_SUCCESS == tmp_ret) {
      int32_t overwrite = 1;
      if (OB_SUCCESS !=
          (tmp_ret = rescue_server_counter_.set_refactored(rescue_server, (cnt > 0 ? cnt - 1 : 0), overwrite))) {
        LOG_WARN("fail to set refactored", K(tmp_ret));
      }
    } else {
      LOG_WARN("fail to get from hash map", K(tmp_ret));
    }
  }
  idling_.wakeup();
  return ret;
}

int ObServerRecoveryMachine::update_file_recovery_status(const common::ObAddr& server,
    const common::ObAddr& dest_server, const uint64_t tenant_id, const int64_t file_id, FileRecoveryStatus pre_status,
    FileRecoveryStatus cur_status)
{
  int ret = OB_SUCCESS;
  char svr_ip_string[OB_MAX_SERVER_ADDR_SIZE] = "";
  char dest_ip_string[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObTimeoutCtx ctx;

  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid()
                         // !dest_server.is_valid() dest_serve may not has a valid value while delete tenant
                         || OB_INVALID_ID == tenant_id || file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
  } else if (stop_) {
    LOG_INFO("ObServerRecoveryMachine stopped");
  } else if (OB_UNLIKELY(nullptr == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy ptr is null", K(ret));
  } else if (!server.ip_to_string(svr_ip_string, sizeof(svr_ip_string))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (!dest_server.ip_to_string(dest_ip_string, sizeof(dest_ip_string))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else {
    ObSqlString sql_string;
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    ObServerRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(server, task))) {
      LOG_WARN("fail to get from map", K(ret));
    } else if (OB_UNLIKELY(nullptr == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(server));
    } else {
      LOG_INFO("update file status", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
      SpinWLockGuard guard(task->lock_);
      RefugeeInfo* refugee_info = nullptr;
      if (ServerRecoveryProgress::SRP_IN_PG_RECOVERY != task->progress_) {
        LOG_INFO("state not match, maybe previous task response",
            K(server),
            K(dest_server),
            K(tenant_id),
            K(file_id),
            K(pre_status),
            K(cur_status));
      } else if (OB_FAIL(task->get_refugee_info(tenant_id, file_id, dest_server, refugee_info))) {
        LOG_WARN("fail to get refugee info", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
      } else if (OB_UNLIKELY(nullptr == refugee_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("refugee info ptr is null", K(ret));
      } else if (pre_status != refugee_info->file_recovery_status_) {
        LOG_INFO("state not match, maybe pushed by others",
            K(server),
            K(dest_server),
            K(tenant_id),
            K(file_id),
            K(pre_status),
            K(cur_status));
      } else if (OB_FAIL(sql_string.assign_fmt("UPDATE %s SET STATUS = %ld "
                                               "WHERE svr_ip = '%s' AND svr_port = %ld "
                                               "AND tenant_id = %ld AND file_id = %ld "
                                               "AND dest_svr_ip = '%s' AND dest_svr_port = %ld ",
                     OB_ALL_DATAFILE_RECOVERY_STATUS_TNAME,
                     static_cast<int64_t>(cur_status),
                     svr_ip_string,
                     static_cast<int64_t>(server.get_port()),
                     tenant_id,
                     file_id,
                     dest_ip_string,
                     static_cast<int64_t>(dest_server.get_port())))) {
        LOG_WARN("fail to assign fmt", K(ret));
      } else if (OB_FAIL(trans.start(mysql_proxy_))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else if (OB_FAIL(trans.write(sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql_string));
      } else if (1 != affected_rows) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("state not match", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCCESS == ret))) {
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
          LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
        }
      }
      if (OB_SUCC(ret)) {
        refugee_info->file_recovery_status_ = cur_status;
      } else {
        refugee_info->file_recovery_status_ = FileRecoveryStatus::FRS_FAILED;
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::try_recover_pg(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else {
    SpinWLockGuard guard(task->lock_);
    if (ServerRecoveryProgress::SRP_IN_PG_RECOVERY != task->progress_) {
      // bypass, task progress may be pushed forward by other routine
    } else if (RecoverFileTaskGenStatus::RFTGS_TASK_GENERATED == task->recover_file_task_gen_status_) {
      if (OB_FAIL(drive_this_recover_pg(task))) {
        LOG_WARN("fail to drive this recover pg", K(ret));
      }
    } else if (RecoverFileTaskGenStatus::RFTGS_INVALID == task->recover_file_task_gen_status_) {
      if (OB_FAIL(launch_new_recover_pg(task))) {
        LOG_WARN("fail to launch new recover pg", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task recover file task gen status unexpected", K(ret), "task", *task);
    }
  }
  return ret;
}

int ObServerRecoveryMachine::pick_dest_server_for_refugee(RefugeeInfo& refugee_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> unit_infos;
  ObArray<ObUnitInfo> available_unit_infos;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else if (OB_UNLIKELY(nullptr == unit_mgr_ || nullptr == server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr or server mgr ptr is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_active_unit_infos_by_tenant(refugee_info.tenant_id_, unit_infos))) {
    LOG_WARN("fail to get active unit infos by tenant", K(ret), "tenant_id", refugee_info.tenant_id_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_infos.count(); ++i) {
      bool is_active = false;
      const common::ObAddr& this_server = unit_infos.at(i).unit_.server_;
      if (OB_UNLIKELY(!this_server.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this server addr invalid", K(ret), "unit_info", unit_infos.at(i));
      } else if (OB_FAIL(server_mgr_->check_server_active(this_server, is_active))) {
        LOG_WARN("check server active", K(ret), K(this_server));
      } else if (!is_active) {
        // bypass
      } else if (OB_FAIL(available_unit_infos.push_back(unit_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (available_unit_infos.count() <= 0) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("no available unit to distribute datafile", K(ret), K(refugee_info));
      } else {
        const int64_t seq_num = seq_generator_.next_seq();
        const int64_t idx = seq_num % available_unit_infos.count();
        refugee_info.dest_server_ = available_unit_infos.at(idx).unit_.server_;
        refugee_info.dest_unit_id_ = available_unit_infos.at(idx).unit_.unit_id_;
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::launch_recover_file_task(ObServerRecoveryTask* task, RefugeeInfo& refugee_info)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  char svr_ip_string[OB_MAX_SERVER_ADDR_SIZE] = "";
  char dest_svr_ip_string[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObTimeoutCtx ctx;

  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else if (!refugee_info.server_.ip_to_string(svr_ip_string, sizeof(svr_ip_string))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (!refugee_info.dest_server_.ip_to_string(dest_svr_ip_string, sizeof(dest_svr_ip_string))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert dest ip to string", K(ret));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else {
    ObSqlString sql_string;
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_string.assign_fmt("UPDATE %s SET dest_svr_ip = '%s', dest_svr_port = %ld, dest_unit_id = %ld "
                                      "WHERE svr_ip = '%s' AND svr_port = %ld "
                                      "AND tenant_id = %ld AND file_id = %ld",
            OB_ALL_DATAFILE_RECOVERY_STATUS_TNAME,
            dest_svr_ip_string,
            static_cast<int64_t>(refugee_info.dest_server_.get_port()),
            refugee_info.dest_unit_id_,
            svr_ip_string,
            static_cast<int64_t>(refugee_info.server_.get_port()),
            refugee_info.tenant_id_,
            refugee_info.file_id_))) {
      LOG_WARN("fail to assign fmt", K(ret));
    } else if (OB_FAIL(trans.start(mysql_proxy_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(trans.write(sql_string.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql_string));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCCESS == ret))) {
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
    }
    if (OB_SUCC(ret)) {
      ObFastRecoveryTaskInfo recovery_task_info;
      ObFastRecoveryTask recovery_task;
      int64_t task_cnt = 0;
      if (OB_FAIL(recovery_task_info.build(refugee_info.server_,
              refugee_info.dest_server_,
              task->rescue_server_,
              refugee_info.tenant_id_,
              refugee_info.file_id_))) {
        LOG_WARN("fail to build recovery task info", K(ret));
      } else if (OB_FAIL(recovery_task.build(
                     recovery_task_info, refugee_info.dest_unit_id_, balancer::FAST_RECOVERY_TASK))) {
        LOG_WARN("fail to build recovery task", K(ret));
      } else if (OB_FAIL(rebalance_task_mgr_->add_task(recovery_task, task_cnt))) {
        LOG_WARN("fail to add task", K(ret));
      } else {
        refugee_info.file_recovery_status_ = FileRecoveryStatus::FRS_IN_PROGRESS;
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::trigger_recover_file_task(ObServerRecoveryTask* task, RefugeeInfo& refugee_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else if (OB_UNLIKELY(nullptr == rebalance_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebalance task mgr ptr is null", K(ret));
  } else {
    ObFastRecoveryTaskInfo recovery_task_info;
    ObFastRecoveryTask recovery_task;
    int64_t task_cnt = 0;
    if (OB_FAIL(recovery_task_info.build(refugee_info.server_,
            refugee_info.dest_server_,
            task->rescue_server_,
            refugee_info.tenant_id_,
            refugee_info.file_id_))) {
      LOG_WARN("fail to build recovery task info", K(ret));
    } else if (OB_FAIL(
                   recovery_task.build(recovery_task_info, refugee_info.dest_unit_id_, balancer::FAST_RECOVERY_TASK))) {
      LOG_WARN("fail to build recovery task", K(ret));
    } else if (OB_FAIL(rebalance_task_mgr_->add_task(recovery_task, task_cnt))) {
      LOG_WARN("fail to add task", K(ret));
    } else {
      refugee_info.file_recovery_status_ = FileRecoveryStatus::FRS_IN_PROGRESS;
    }
  }
  return ret;
}

int ObServerRecoveryMachine::drive_this_recover_pg(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(nullptr == server_mgr_ || nullptr == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server mgr or schema service  ptr is null", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else {
    bool finished = true;
    int64_t processed_task_cnt = 0;
    const int64_t PROCESSED_TASK_LIMIT = 100;
    for (int64_t i = 0; OB_SUCC(ret) && i < task->refugee_infos_.count() && processed_task_cnt < PROCESSED_TASK_LIMIT;
         ++i) {
      RefugeeInfo& this_refugee_info = task->refugee_infos_.at(i);
      if (FileRecoveryStatus::FRS_IN_PROGRESS == this_refugee_info.file_recovery_status_) {
        finished = false;
      } else if (FileRecoveryStatus::FRS_SUCCEED == this_refugee_info.file_recovery_status_) {
        // good, this refugee finished
      } else if (FileRecoveryStatus::FRS_INVALID == this_refugee_info.file_recovery_status_ ||
                 FileRecoveryStatus::FRS_FAILED == this_refugee_info.file_recovery_status_) {
        bool need_distribute_dest = false;
        finished = false;
        processed_task_cnt++;
        bool is_active = true;
        bool is_tenant_exist = false;
        share::schema::ObSchemaGetterGuard schema_guard;
        if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
          LOG_WARN("fail to get schema guard", K(ret));
        } else if (OB_FAIL(schema_guard.check_tenant_exist(this_refugee_info.tenant_id_, is_tenant_exist))) {
          LOG_WARN("fail to check tenant exist", K(ret));
        } else if (!is_tenant_exist) {
          this_refugee_info.file_recovery_status_ = FileRecoveryStatus::FRS_IN_PROGRESS;
          UpdateFileRecoveryStatusTask task(this_refugee_info.server_,
              this_refugee_info.dest_server_,
              this_refugee_info.tenant_id_,
              this_refugee_info.file_id_,
              FileRecoveryStatus::FRS_IN_PROGRESS,
              FileRecoveryStatus::FRS_SUCCEED,
              stop_,
              *this);
          if (OB_FAIL(datafile_recovery_status_task_queue_.add_task(task))) {
            LOG_WARN("fail to add task", K(ret), "refugee", this_refugee_info);
          }
        } else {
          if (!this_refugee_info.dest_server_.is_valid()) {
            need_distribute_dest = true;
          } else if (OB_FAIL(server_mgr_->check_server_active(this_refugee_info.dest_server_, is_active))) {
            LOG_WARN("fail to check server active", K(ret), "server", this_refugee_info.dest_server_);
          } else if (is_active) {
            // server still active, no need to distribute
          } else {
            need_distribute_dest = true;
          }
          if (OB_FAIL(ret)) {
            // failed
          } else if (need_distribute_dest) {
            if (OB_FAIL(pick_dest_server_for_refugee(this_refugee_info))) {
              LOG_WARN("fail to pick dest server for refugee", K(ret));
            } else if (OB_FAIL(launch_recover_file_task(task, this_refugee_info))) {
              LOG_WARN("fail to launch recover file task", K(ret));
            }
          } else {
            if (OB_FAIL(trigger_recover_file_task(task, this_refugee_info))) {
              LOG_WARN("fail to trigger recover file task", K(ret));
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!finished) {
      // not finish yet
    } else {
      void(switch_state(task, ServerRecoveryProgress::SRP_CHECK_PG_RECOVERY_FINISHED));
    }
  }
  return ret;
}

int ObServerRecoveryMachine::launch_new_recover_pg(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = nullptr;
    char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    ObTimeoutCtx ctx;

    if (OB_UNLIKELY(!inited_ || !loaded_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObServerRecoveryMacine state not match", K(ret));
    } else if (OB_UNLIKELY(nullptr == task)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (stop_) {
      LOG_INFO("server recovery machine stop");
    } else if (RecoverFileTaskGenStatus::RFTGS_TASK_GENERATED == task->recover_file_task_gen_status_) {
      // bypass
    } else if (OB_UNLIKELY(nullptr == mysql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql proxy ptr is null", K(ret));
    } else if (!task->server_.ip_to_string(svr_ip, sizeof(svr_ip))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to convert ip to string", K(ret));
    } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql_string.assign_fmt("SELECT * FROM %s WHERE "
                                             "svr_ip = '%s' AND svr_port = %ld",
                   OB_ALL_DATAFILE_RECOVERY_STATUS_TNAME,
                   svr_ip,
                   static_cast<int64_t>(task->server_.get_port())))) {
      LOG_WARN("fail to append sql", K(ret));
    } else if (OB_FAIL(mysql_proxy_->read(res, sql_string.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql_string));
    } else if (OB_UNLIKELY(nullptr == (result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        if (OB_FAIL(fill_server_file_recovery_status(result, task))) {
          LOG_WARN("fail to fill server file recovery status", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        task->recover_file_task_gen_status_ = RecoverFileTaskGenStatus::RFTGS_TASK_GENERATED;
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::on_recover_pg_file_reply(
    const ObRebalanceTask& rebalance_task, const common::ObIArray<int>& ret_code_array)
{
  int ret = OB_SUCCESS;
  if (ObRebalanceTaskType::FAST_RECOVERY_TASK != rebalance_task.get_rebalance_task_type()) {
    // bypass
  } else if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (stop_) {
    LOG_INFO("ObServerRecoveryMachine stopped");
  } else {
    const ObFastRecoveryTask& this_task = static_cast<const ObFastRecoveryTask&>(rebalance_task);
    const ObFastRecoveryTaskInfo* task_info = nullptr;
    if (1 != this_task.get_sub_task_count() || 1 != ret_code_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array count unexpected",
          K(ret),
          "first_array_cnt",
          this_task.get_sub_task_count(),
          "second_array_cnt",
          ret_code_array.count());
    } else if (nullptr == (task_info = static_cast<const ObFastRecoveryTaskInfo*>(this_task.get_sub_task(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task info ptr", K(ret));
    } else {
      const int ret_code = ret_code_array.at(0);
      const common::ObAddr& server = task_info->get_src();
      const common::ObAddr& dest_server = task_info->get_dst();
      const uint64_t tenant_id = task_info->get_tenant_id();
      const int64_t file_id = task_info->get_file_id();
      LOG_INFO("on recover pg file succeed", K(server), K(dest_server), K(tenant_id), K(file_id), K(ret_code));
      ObServerRecoveryTask* task = nullptr;
      SpinRLockGuard guard(task_map_lock_);
      if (OB_FAIL(task_map_.get_refactored(server, task))) {
        LOG_WARN("fail to get from map", K(ret), K(server));
      } else if (OB_UNLIKELY(nullptr == task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task ptr is null", K(ret), K(server));
      } else {
        SpinWLockGuard item_guard(task->lock_);
        RefugeeInfo* refugee_info = nullptr;
        if (ServerRecoveryProgress::SRP_IN_PG_RECOVERY != task->progress_) {
          LOG_INFO("state not match, maybe previous task response", K(server));
        } else if (OB_FAIL(task->get_refugee_info(tenant_id, file_id, dest_server, refugee_info))) {
          LOG_WARN("fail to get refugee info", K(ret), K(tenant_id), K(file_id), K(dest_server));
        } else if (OB_UNLIKELY(nullptr == refugee_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("refugee info ptr is null", K(ret), K(server), K(dest_server), K(tenant_id), K(file_id));
        } else if (FileRecoveryStatus::FRS_INVALID == refugee_info->file_recovery_status_ ||
                   FileRecoveryStatus::FRS_FAILED == refugee_info->file_recovery_status_ ||
                   FileRecoveryStatus::FRS_SUCCEED == refugee_info->file_recovery_status_) {
          // ignore this reply, state not match
          LOG_INFO("recover pg file state not match",
              K(server),
              K(dest_server),
              K(tenant_id),
              K(file_id),
              K(ret_code),
              "recovery_status",
              refugee_info->file_recovery_status_);
        } else if (FileRecoveryStatus::FRS_IN_PROGRESS == refugee_info->file_recovery_status_) {
          if (ret_code != OB_SUCCESS) {
            refugee_info->file_recovery_status_ = FileRecoveryStatus::FRS_FAILED;
          } else {
            UpdateFileRecoveryStatusTask task(refugee_info->server_,
                refugee_info->dest_server_,
                refugee_info->tenant_id_,
                refugee_info->file_id_,
                FileRecoveryStatus::FRS_IN_PROGRESS,
                FileRecoveryStatus::FRS_SUCCEED,
                stop_,
                *this);
            if (OB_FAIL(datafile_recovery_status_task_queue_.add_task(task))) {
              LOG_WARN("fail to add task", K(ret), "refugee", *refugee_info);
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("refugee info unexpected", K(ret), "refugee_info", *refugee_info);
        }
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::try_check_pg_recovery_finished(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else {
    SpinWLockGuard guard(task->lock_);
    if (ServerRecoveryProgress::SRP_CHECK_PG_RECOVERY_FINISHED != task->progress_) {
      // bypass, task progress may be push forward by other routine
    } else if (task->last_drive_ts_ + CHECK_PG_RECOVERY_FINISHED_TIMEOUT > ObTimeUtility::current_time()) {
      if (OB_FAIL(drive_this_check_pg_recovery_finished(task))) {
        LOG_WARN("fail to drive this recover pg", K(ret));
      }
    } else {
      if (OB_FAIL(launch_new_check_pg_recovery_finished(task))) {
        LOG_WARN("fail to launch new check pg recovery finished", K(ret));
      }
    }
  }
  return ret;
}

int ObServerRecoveryMachine::drive_this_check_pg_recovery_finished(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else if (CheckPgRecoveryFinishedStatus::CPRFS_INVALID == task->check_pg_recovery_finished_status_) {
    // go on wait
  } else if (CheckPgRecoveryFinishedStatus::CPRFS_SUCCEED == task->check_pg_recovery_finished_status_) {
    (void)switch_state(task, ServerRecoveryProgress::SRP_SERVER_RECOVERY_FINISHED);
  } else if (CheckPgRecoveryFinishedStatus::CPRFS_EAGAIN == task->check_pg_recovery_finished_status_) {
    task->last_drive_ts_ = 0;  // retry check pg recovery finished immediately
  } else if (CheckPgRecoveryFinishedStatus::CPRFS_FAILED == task->check_pg_recovery_finished_status_) {
    task->last_drive_ts_ = 0;  // retry check pg recovery finished immediately
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("recovery machine check pg recovery finished status error", K(ret), "server", task->server_);
  }
  return ret;
}

int ObServerRecoveryMachine::launch_new_check_pg_recovery_finished(ObServerRecoveryTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else if (OB_FAIL(submit_check_pg_recovery_finished_task(task->server_))) {
    LOG_WARN("fail to submit check pg recovery finished task", K(ret), "server", task->server_);
  } else {
    task->check_pg_recovery_finished_status_ = CheckPgRecoveryFinishedStatus::CPRFS_INVALID;
    task->last_drive_ts_ = common::ObTimeUtility::current_time();
  }
  return ret;
}

int ObServerRecoveryMachine::submit_check_pg_recovery_finished_task(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_UNLIKELY(nullptr == pt_operator_ || nullptr == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator_ or schema_service_ ptr is null", K(ret));
  } else if (stop_) {
    LOG_INFO("server recovery machine stop");
  } else {
    CheckPgRecoveryFinishedTask task(server, stop_, *this, *pt_operator_, *schema_service_, *mysql_proxy_);
    if (OB_FAIL(check_pg_recovery_finished_task_queue_.add_task(task))) {
      LOG_WARN("fail to add task", K(ret), K(server));
    }
  }
  return ret;
}

int ObServerRecoveryMachine::on_check_pg_recovery_finished(const common::ObAddr& server, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (stop_) {
    LOG_INFO("ObServerRecoveryMachine stopped");
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    LOG_INFO("on check pg recovery finished", K(server), K(ret_code));
    ObServerRecoveryTask* task = nullptr;
    SpinRLockGuard guard(task_map_lock_);
    if (OB_FAIL(task_map_.get_refactored(server, task))) {
      LOG_WARN("fail to get from map", K(ret), K(server));
    } else if (OB_UNLIKELY(nullptr == task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task ptr is null", K(ret), K(server));
    } else {
      SpinWLockGuard item_guard(task->lock_);
      if (ServerRecoveryProgress::SRP_CHECK_PG_RECOVERY_FINISHED != task->progress_) {
        LOG_INFO("state not match, maybe previous task response", K(server));
      } else if (CheckPgRecoveryFinishedStatus::CPRFS_INVALID != task->check_pg_recovery_finished_status_) {
        LOG_INFO("state not match, maybe previous task response",
            K(server),
            "check_pg_finished_stat",
            task->check_pg_recovery_finished_status_);
      } else if (OB_SUCCESS == ret_code) {
        task->check_pg_recovery_finished_status_ = CheckPgRecoveryFinishedStatus::CPRFS_SUCCEED;
      } else if (OB_EAGAIN == ret_code) {
        task->check_pg_recovery_finished_status_ = CheckPgRecoveryFinishedStatus::CPRFS_EAGAIN;
      } else {
        task->check_pg_recovery_finished_status_ = CheckPgRecoveryFinishedStatus::CPRFS_FAILED;
      }
    }
  }
  idling_.wakeup();
  return ret;
}

int ObServerRecoveryMachine::switch_state(ObServerRecoveryTask* task, const ServerRecoveryProgress new_progress)
{
  int ret = OB_SUCCESS;
  char svr_ip_string[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObTimeoutCtx ctx;

  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerRecoveryMachine state not match", K(ret));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(nullptr == mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy ptr is null", K(ret));
  } else if (!task->server_.ip_to_string(svr_ip_string, sizeof(svr_ip_string))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ip to string", K(ret));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else {
    ObMySQLTransaction trans;
    ServerRecoveryProgress cur_progress = task->progress_;
    LOG_INFO("task switch state",
        K(ret),
        K(cur_progress),
        K(new_progress),
        "server",
        task->server_,
        "rescue_server",
        task->rescue_server_);
    if (OB_FAIL(trans.start(mysql_proxy_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else {
      ObSqlString sql_string;
      int64_t affected_rows;
      if (OB_FAIL(sql_string.assign_fmt("UPDATE %s SET RESCUE_PROGRESS = %ld "
                                        "WHERE svr_ip = '%s' "
                                        "AND svr_port = %ld ",
              OB_ALL_SERVER_RECOVERY_STATUS_TNAME,
              static_cast<int64_t>(new_progress),
              svr_ip_string,
              static_cast<int64_t>(task->server_.get_port())))) {
        LOG_WARN("fail to format sql", K(ret), K(sql_string));
      } else if (OB_FAIL(trans.write(sql_string.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql_string));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCCESS == ret))) {
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        LOG_WARN("fail to end trans", K(ret), K(tmp_ret));
      }
      if (OB_SUCC(ret)) {
        ROOTSERVICE_EVENT_ADD("server_recovery_machine",
            "switch_state",
            "server",
            task->server_,
            "rescue_server",
            task->rescue_server_,
            "cur_progress",
            task->progress_,
            "next_progress",
            new_progress);
        task->progress_ = new_progress;
        // set last drive ts = 0 after switch status successfully.
        // it will be executed immdiately
        task->last_drive_ts_ = 0;
      }
    }
  }
  return ret;
}

void ObServerRecoveryMachine::stop()
{
  if (!inited_) {
    int ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!stop_) {
    ObReentrantThread::stop();
    idling_.wakeup();
  }
}

}  // end namespace rootserver
}  // end namespace oceanbase
