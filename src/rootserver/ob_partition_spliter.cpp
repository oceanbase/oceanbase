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
#include "ob_partition_spliter.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "rootserver/ob_ddl_help.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace rootserver {
int64_t ObPartitionSpliterIdling::get_idle_interval_us()
{
  int64_t idle_time = 0;
  if (host_.table_in_splitting_) {
    idle_time = MIN_CHECK_SPLIT_INTERVAL_US;
  } else {
    idle_time = MAX_CHECK_SPLIT_INTERVAL_US;
  }
  return idle_time;
}

ObPartitionSpliter::~ObPartitionSpliter()
{
  if (inited_) {
    stop();
    wait();
  }
}

int ObPartitionSpliter::init(ObCommonRpcProxy& rpc_proxy, const ObAddr& self_addr,
    share::schema::ObMultiVersionSchemaService* schema_service, ObRootService* root_service)
{
  int ret = OB_SUCCESS;
  static const int64_t thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(schema_service) || OB_ISNULL(root_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_service), K(root_service));
  } else if (OB_FAIL(create(thread_cnt, "PtSpliter"))) {
    LOG_WARN("create major freeze launcher thread failed", K(ret), K(thread_cnt));
  } else {
    rpc_proxy_ = &rpc_proxy;
    schema_service_ = schema_service;
    root_service_ = root_service;
    self_addr_ = self_addr;
    inited_ = true;
  }
  return ret;
}

void ObPartitionSpliter::run3()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("partition spliter start");
    while (!stop_) {
      if (PRIMARY_CLUSTER == ObClusterInfoGetter::get_cluster_type_v2()) {
        table_in_splitting_ = false;
        if (OB_FAIL(try_split_partition())) {
          LOG_WARN("try_launch_major_freeze failed", K(ret));
        }
      }
      if (!stop_) {
        idling_.idle();
      }
    }
    LOG_INFO("partition spliter stop");
  }
}

void ObPartitionSpliter::wakeup()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    idling_.wakeup();
  }
}

void ObPartitionSpliter::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

int ObPartitionSpliter::try_split_partition()
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2*> table_schemas;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas(table_schemas))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else {
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      const ObSimpleTableSchemaV2* schema = table_schemas.at(i);
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid schema", K(ret), K(schema), K(i));
      } else if (!schema->is_in_splitting() || OB_INVALID_ID != schema->get_tablegroup_id()) {
        // nothing todo
      } else {
        LOG_INFO("table schema in splitting", K(ret), "table_id", schema->get_table_id());
        if (schema->is_in_logical_split()) {
          table_in_splitting_ = true;
        }
        ObRootSplitPartitionArg arg;
        arg.table_id_ = schema->get_table_id();
        arg.exec_tenant_id_ = schema->get_tenant_id();
        int tmp_ret = rpc_proxy_->to(self_addr_).root_split_partition(arg);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to split partition", K(ret), K(arg), K(tmp_ret), K(self_addr_));
        }
      }
    }
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;  // ignore ret
    }
    if (OB_SUCC(ret)) {
      ObArray<uint64_t> tenant_ids;
      ObArray<const ObTablegroupSchema*> schemas;
      if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
        LOG_WARN("fail to get tenant ids", K(ret));
      } else {
        for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); i++) {
          uint64_t tenant_id = tenant_ids.at(i);
          schemas.reset();
          if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, schemas))) {
            LOG_WARN("fail to get tablegorup schemas", K(ret), K(tenant_id));
          } else {
            for (int64_t j = 0; j < schemas.count() && OB_SUCC(ret); j++) {
              if (OB_ISNULL(schemas.at(j))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("schema is null", K(ret), K(j));
              } else if (!schemas.at(j)->is_in_splitting()) {
                // nothing todo
              } else {
                LOG_INFO("tablegroup schema in splitting", K(ret), "tablegroup_id", schemas.at(j)->get_tablegroup_id());
                if (schemas.at(j)->is_in_logical_split()) {
                  table_in_splitting_ = true;
                }
                ObRootSplitPartitionArg arg;
                arg.table_id_ = schemas.at(j)->get_tablegroup_id();
                arg.exec_tenant_id_ = tenant_id;
                int tmp_ret = rpc_proxy_->to(self_addr_).root_split_partition(arg);
                if (OB_SUCCESS != tmp_ret) {
                  LOG_WARN("fail to split partiton", K(ret), K(arg), K(tmp_ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

/////////////////////////////////////////////

int ObPartitionSplitExecutor::split_table_partition(const share::schema::ObTableSchema& schema,
    share::ObPartitionTableOperator& pt_operator, obrpc::ObSrvRpcProxy& rpc_proxy, ObSplitPartitionArg& arg,
    ObSplitProgress& split_process)
{
  int ret = OB_SUCCESS;
  if (!schema.is_in_splitting()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is not split", K(ret), K(schema));
  } else if (OB_FAIL(get_split_status(schema, pt_operator, rpc_proxy, arg, split_process))) {
    LOG_WARN("failed to get split result", K(ret), K(schema));
  }
  return ret;
}

int ObPartitionSplitExecutor::split_binding_tablegroup_partition(const share::schema::ObTablegroupSchema& schema,
    share::ObPartitionTableOperator& pt_operator, obrpc::ObSrvRpcProxy& rpc_proxy, ObSplitPartitionArg& arg,
    ObSplitProgress& split_process)
{
  int ret = OB_SUCCESS;
  if (!schema.is_in_splitting() || !schema.get_binding()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is not split or binding", K(ret), K(schema));
  } else if (OB_FAIL(get_split_status(schema, pt_operator, rpc_proxy, arg, split_process))) {
    LOG_WARN("failed to get split result", K(ret), K(schema));
  }
  return ret;
}

int ObPartitionSplitExecutor::get_split_status(const share::schema::ObPartitionSchema& schema,
    share::ObPartitionTableOperator& pt_operator, obrpc::ObSrvRpcProxy& rpc_proxy, ObSplitPartitionArg& arg,
    ObSplitProgress& split_status)
{
  int ret = OB_SUCCESS;
  if (!schema.is_in_splitting()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is null", K(ret), K(schema));
  } else {
    ObSplitPartitionResult res;
    ObPartitionInfo partition;
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    partition.set_allocator(&allocator);
    const ObPartitionReplica* leader = NULL;
    int64_t timeout = 0;
    // For the time being, it only supports splitting one partition at a time;
    if (OB_FAIL(ObPartitionSplitHelper::build_split_info(schema, arg.split_info_))) {
      LOG_WARN("fail to build split arg", K(ret));
    } else if (arg.split_info_.get_spp_array().count() != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support split multi_partition", K(ret), K(arg));
    } else if (OB_FAIL(pt_operator.get(arg.split_info_.get_spp_array().at(0).get_source_pkey().get_table_id(),
                   arg.split_info_.get_spp_array().at(0).get_source_pkey().get_partition_id(),
                   partition))) {
      LOG_WARN("fail to get partition", K(ret), K(arg));
    } else if (OB_FAIL(partition.find_leader_v2(leader))) {
      LOG_WARN("fail to find leader", K(ret), K(partition));
    } else if (OB_ISNULL(leader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid leader", K(ret));
    }
    if (OB_SUCC(ret)) {
      const int64_t per_partition_process_time = 5 * 1000;
      timeout = GCONF.rpc_timeout +
                arg.split_info_.get_spp_array().at(0).get_dest_array().count() * per_partition_process_time;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rpc_proxy.to(leader->server_).timeout(timeout).split_partition(arg, res))) {
      LOG_WARN("fail to split partition", K(ret), K(timeout), K(arg));
#ifdef ERRSIM
    } else if (OB_FAIL(E(EventTable::EN_BLOCK_SPLIT_PROGRESS_RESPONSE) OB_SUCCESS)) {
      if (REACH_TIME_INTERVAL(1000000)) {
        LOG_WARN("ERRSIM: EN_BLOCK_SPLIT_PROGRESS_RESPONSE", K(ret));
      }
#endif
    } else if (OB_FAIL(ObPartitionSplitHelper::check_split_result(arg, res, split_status))) {
      LOG_WARN("fail to check split result", K(ret), K(arg), K(res));
    }
  }
  return ret;
}
int ObPartitionSplitExecutor::split_tablegroup_partition(ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTablegroupSchema& tablegroup_schema, ObISQLClient& client, ObSplitProgress& split_status)
{
  int ret = OB_SUCCESS;
  split_status = IN_SPLITTING;
  uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  ObArray<const ObSimpleTableSchemaV2*> table_schemas;
  obrpc::ObSplitPartitionArg args;
  ObArray<ObAddr> servers;
  ObSqlString sql;
  args.split_info_.set_schema_version(tablegroup_schema.get_partition_schema_version());
  if (tablegroup_schema.get_binding()) {
    // The split of PG is processed in the upper layer, do not go to this branch for processing
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablegroup is binding", K(ret), K(tablegroup_schema));
  } else {
    if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
      LOG_WARN("fail to get table schemas", K(ret), K(tenant_id), K(tablegroup_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (table_schemas.count() <= 0) {
    // tablegroup has no tables
    split_status = PHYSICAL_SPLIT_FINISH;
  } else if (OB_FAIL(sql.assign_fmt("SELECT table_id, partition_id, svr_ip, svr_port FROM %s WHERE ROLE = 1 AND (",
                 OB_ALL_TENANT_META_TABLE_TNAME))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else {
    obrpc::ObSplitPartitionArg arg;
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      const ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
      arg.reset();
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid schema", K(ret), K(i), K(tablegroup_id));
      } else if (!table_schema->has_partition()) {
        // do nothing
      } else if (OB_FAIL(ObPartitionSplitHelper::build_split_info(*table_schema, arg.split_info_))) {
        LOG_WARN("fail to build split arg", K(ret), K(i));
      } else if (OB_FAIL(add_split_arg(args, arg))) {
        LOG_WARN("fail to add split_arg", K(ret), K(arg));
      } else if (0 == i) {
        if (OB_FAIL(sql.append_fmt("(TENANT_ID = %ld AND TABLE_ID = %ld AND PARTITION_ID IN (",
                tenant_id,
                table_schema->get_table_id()))) {
          LOG_WARN("fail to append sql", K(ret));
        }
      } else {
        if (OB_FAIL(sql.append_fmt(" OR (TENANT_ID = %ld AND TABLE_ID = %ld AND PARTITION_ID IN (",
                tenant_id,
                table_schema->get_table_id()))) {
          LOG_WARN("fail to append sql", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        for (int64_t j = 0; j < arg.split_info_.get_spp_array().count() && OB_SUCC(ret); j++) {
          const ObPartitionKey& source_key = arg.split_info_.get_spp_array().at(j).get_source_pkey();
          if (j == 0) {
            if (OB_FAIL(sql.append_fmt("%ld", source_key.get_partition_id()))) {
              LOG_WARN("fail to append fmt", K(ret), K(source_key));
            }
          } else {
            if (OB_FAIL(sql.append_fmt(", %ld", source_key.get_partition_id()))) {
              LOG_WARN("fail to append fmt", K(ret), K(source_key));
            }
          }
        }  // end for
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(sql.append_fmt("))"))) {
          LOG_WARN("append_fmt failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        common::sqlclient::ObMySQLResult* result = NULL;
        if (OB_FAIL(sql.append_fmt(")"))) {
          LOG_WARN("append_fmt failed", K(ret));
        } else if (OB_FAIL(client.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get result fail", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END != ret) {
                LOG_WARN("result next failed", K(ret));
              } else {
                ret = OB_SUCCESS;
                break;
              }
            } else {
              char svr_ip[OB_IP_STR_BUFF] = "";
              int64_t svr_port = 0;
              int64_t tmp_real_str_len = 0;
              int64_t table_id = 0;
              int64_t partition_id = 0;
              ObAddr server_addr;
              UNUSED(tmp_real_str_len);
              EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", svr_ip, OB_IP_PORT_STR_BUFF, tmp_real_str_len);
              EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", partition_id, int64_t);
              server_addr.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port));
              if (OB_FAIL(ret)) {
                LOG_WARN("fail to extract result", K(ret));
              } else if (OB_FAIL(accumulate_split_info(server_addr, table_id, partition_id, args))) {
                LOG_WARN("fail to add split info", K(ret), K(server_addr), K(table_id), K(partition_id), K(args));
              }
            }  // end else
          }    // end while
        }
      }
    }
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (result_row_ != args.split_info_.get_spp_array().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid leader count, maybe no leader", K(result_row_), K(args));
    } else if (OB_FAIL(send_rpc())) {
      LOG_WARN("fail to send rpc");
    } else if (OB_FAIL(check_result(split_status))) {
      LOG_WARN("fail to check result", K(ret));
    }
  }
  if (OB_SUCC(ret) && PHYSICAL_SPLIT_FINISH == split_status) {
    LOG_INFO("finish split tablegroup", K(ret), K(tablegroup_schema));
  }
  return ret;
}

int ObPartitionSplitExecutor::check_result(ObSplitProgress& split_status)
{
  int ret = OB_SUCCESS;
  split_status = PHYSICAL_SPLIT_FINISH;
  ObSplitProgress table_split_status;
  for (int64_t i = 0; i < proxy_batch_.get_results().count() && OB_SUCC(ret); i++) {
    const obrpc::ObSplitPartitionResult* batch_res = proxy_batch_.get_results().at(i);
    const obrpc::ObSplitPartitionArg& batch_arg = proxy_batch_.get_args().at(i);
    if (OB_ISNULL(batch_res)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch split partition response is null", K(ret), K(batch_res));
    } else if (OB_FAIL(ObPartitionSplitHelper::check_split_result(batch_arg, *batch_res, table_split_status))) {
      LOG_WARN("fail to check split result", K(ret), K(batch_arg));
    } else if (split_status > table_split_status) {
      split_status = table_split_status;
    }
  }
  return ret;
}

int ObPartitionSplitExecutor::send_rpc()
{
  int ret = OB_SUCCESS;
  if (dest_addr_.count() != split_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid count", K(ret), K(dest_addr_), K(split_infos_));
  } else if (0 < split_infos_.count()) {
    const int64_t per_partition_process_time = 5 * 1000;
    const ObSplitPartitionArg& split_arg = split_infos_.at(0);
    if (0 < split_arg.split_info_.get_spp_array().count()) {
      const ObSplitPartitionPair& split_pair = split_arg.split_info_.get_spp_array().at(0);
      const int64_t timeout = GCONF.rpc_timeout + split_pair.get_dest_array().count() * per_partition_process_time;
      ObArray<int> return_array;
      for (int64_t i = 0; i < dest_addr_.count() && OB_SUCC(ret); i++) {
        if (OB_FAIL(proxy_batch_.call(dest_addr_.at(i), timeout, split_infos_.at(i)))) {
          LOG_WARN("fail to call async batch rpc", K(ret), K(timeout), K(i), K(dest_addr_), K(split_infos_));
        }
      }
      if (OB_FAIL(ret)) {
        // nothing todo
      } else if (OB_FAIL(proxy_batch_.wait_all(return_array))) {
        LOG_WARN("fail to wait all response", K(ret));
      } else if (return_array.count() != dest_addr_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected batch return count", K(ret), K(dest_addr_), K(return_array));
      }
    }
  }
  return ret;
}

int ObPartitionSplitExecutor::accumulate_split_info(
    const ObAddr& addr, const int64_t table_id, const int64_t partition_id, const obrpc::ObSplitPartitionArg& args)
{
  int ret = OB_SUCCESS;
  if (!addr.is_valid() || OB_INVALID_ID == table_id || OB_INVALID_ID == partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr), K(table_id), K(partition_id));
  } else if (dest_addr_.count() != split_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inner stat", K(ret), K(dest_addr_), K(split_infos_));
  } else {
    int64_t server_index = OB_INVALID_INDEX;
    int64_t arg_index = OB_INVALID_INDEX;
    for (int64_t i = 0; i < dest_addr_.count() && OB_SUCC(ret); i++) {
      if (dest_addr_.at(i) == addr) {
        server_index = i;
        break;
      }
    }
    for (int64_t i = 0; i < args.split_info_.get_spp_array().count() && OB_SUCC(ret); i++) {
      const ObPartitionKey& source_pkey = args.split_info_.get_spp_array().at(i).get_source_pkey();
      if (table_id == source_pkey.get_table_id() && partition_id == source_pkey.get_partition_id()) {
        arg_index = i;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_INDEX == arg_index) {
      // Get an invalid leader information and don't process it
    } else {
      if (OB_INVALID_INDEX == server_index) {
        ObSplitPartitionArg arg;
        arg.split_info_.set_schema_version(args.split_info_.get_schema_version());
        if (OB_FAIL(dest_addr_.push_back(addr))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(arg.split_info_.get_spp_array().push_back(args.split_info_.get_spp_array().at(arg_index)))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(split_infos_.push_back(arg))) {
          LOG_WARN("fail to push back", K(ret), K(arg_index), K(arg), K(split_infos_));
        } else {
          result_row_++;
        }
      } else if (OB_FAIL(split_infos_.at(server_index)
                             .split_info_.get_spp_array()
                             .push_back(args.split_info_.get_spp_array().at(arg_index)))) {
        LOG_WARN("fail to push back", K(ret), K(server_index), K(arg_index), K(args), K(split_infos_));
      } else {
        result_row_++;
        LOG_DEBUG("add pair to arg", K(ret), K(result_row_), K(server_index), K(arg_index), K(args));
      }  // end else if
    }
  }
  return ret;
}

int ObPartitionSplitExecutor::add_split_arg(
    obrpc::ObSplitPartitionArg& args, const obrpc::ObSplitPartitionArg& arg) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < arg.split_info_.get_spp_array().count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(args.split_info_.get_spp_array().push_back(arg.split_info_.get_spp_array().at(i)))) {
      LOG_WARN("fail to add split arg", K(ret), K(arg), K(args));
    }
  }
  return ret;
}
}  // namespace rootserver
}  // namespace oceanbase
