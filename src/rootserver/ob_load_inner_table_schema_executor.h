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

#ifndef OCEANBASE_ROOTSERVER_OB_LOAD_INNER_TABLE_SCHEMA_EXECUTOR_H_
#define OCEANBASE_ROOTSERVER_OB_LOAD_INNER_TABLE_SCHEMA_EXECUTOR_H_

#include "share/ob_rpc_struct.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace share
{
class ObLoadInnerTableSchemaInfo;
}
namespace rootserver
{

class ObLoadInnerTableSchemaExecutor
{
public:
  static int load_inner_table_schema(const obrpc::ObLoadTenantTableSchemaArg &arg);
  static int load_core_schema_version(const uint64_t tenant_id, common::ObISQLClient &client);
private:
  static int load_inner_table_schema(const obrpc::ObLoadTenantTableSchemaArg &arg,
      const share::ObLoadInnerTableSchemaInfo &info);
  static int get_extra_header(const obrpc::ObLoadTenantTableSchemaArg &arg, ObSqlString &header);
  static int get_extra_value(
      const obrpc::ObLoadTenantTableSchemaArg &arg,
      const uint64_t table_id,
      ObSqlString &value);

public:
  ObLoadInnerTableSchemaExecutor() : tenant_id_(OB_INVALID_TENANT_ID), rpc_proxy_(nullptr), inited_(false),
    args_(), next_arg_index_(0), load_rpc_timeout_(0), parallel_count_(0) {}
  int init(const ObIArray<ObTableSchema> &table_schemas, const uint64_t tenant_id,
      const int64_t max_cpu, obrpc::ObSrvRpcProxy *rpc_proxy);
  int execute();
private:
  int init_args_(const ObIArray<ObTableSchema> &table_schemas);
  int append_arg(const ObIArray<int64_t> &insert_idx, const share::ObLoadInnerTableSchemaInfo &info);
  int call_next_arg_(ObLoadTenantTableSchemaProxy& proxy);

private:
  static const int64_t LOAD_ROWS_PER_BATCH = 1000;
  static const int64_t LOAD_ROWS_PER_INSERT = 100;
  static const int64_t WAIT_THREAD_FREE_TIME = 10_ms;
  static const int64_t THREAD_PER_CPU = 4; // should equal to the default value of parameter cpu_quota_concurrency
  static const share::ObLoadInnerTableSchemaInfo *ALL_LOAD_SCHEMA_INFO[];
private:
  uint64_t tenant_id_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
private:
  bool inited_;
  ObArray<obrpc::ObLoadTenantTableSchemaArg> args_;
  int64_t next_arg_index_;
  int64_t load_rpc_timeout_;
  int64_t parallel_count_;
};
}
}

#endif // OCEANBASE_ROOTSERVER_OB_LOAD_INNER_TABLE_SCHEMA_EXECUTOR_H_
