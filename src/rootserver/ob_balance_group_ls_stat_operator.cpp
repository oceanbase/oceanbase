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

#define USING_LOG_PREFIX SHARE

#include "share/schema/ob_schema_mgr.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "lib/hash/ob_hashset.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_timeout_ctx.h"
#include "observer/ob_server_struct.h" // for GCTX
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_share_util.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_srv_rpc_proxy.h" // ObSrvRpcProxy
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/ls/ob_ls_operator.h" // ObLSAttrOperator
#include "share/balance/ob_balance_task_table_operator.h" // ObBalanceTaskTableOperator
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_debug_sync.h" // DEBUG_SYNC
#include "storage/tablelock/ob_lock_utils.h" // ObLSObjLockUtil
#include "share/ls/ob_ls_table.h" // ObLSTable
#include "share/ls/ob_ls_table_operator.h" // ObLSTableOperator
#include "share/location_cache/ob_location_service.h" // ObLocationService
#include "share/ob_rpc_struct.h" // ObCreateDupLSArg & ObCreateDupLSResult
#include "rootserver/ob_root_service.h"
#include "rootserver/parallel_ddl/ob_tablet_balance_allocator.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace transaction::tablelock;

namespace rootserver
{


int64_t ObNewTableTabletAllocator::alloc_tablet_ls_offset_ = 0;

int ObBalanceGroupLSStat::build(
    const uint64_t tenant_id,
    const ObBalanceGroupID &balance_group_id,
    const share::ObLSID &ls_id,
    const int64_t tablet_group_count,
    const ObBalanceGroupName &balance_group_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                  || !balance_group_id.is_valid()
                  || !ls_id.is_valid()
                  || tablet_group_count < 0
                  || balance_group_name.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(tenant_id),
             K(balance_group_id),
             K(ls_id),
             K(tablet_group_count),
             K(balance_group_name));
  } else {
    tenant_id_ = tenant_id;
    balance_group_id_= balance_group_id;
    ls_id_ = ls_id;
    tablet_group_count_ = tablet_group_count;
    balance_group_name_ = balance_group_name;
  }
  return ret;
}

ObBalanceGroupLSStatOperator::ObBalanceGroupLSStatOperator()
  : inited_(false),
    sql_proxy_(nullptr)
{
}

ObBalanceGroupLSStatOperator::~ObBalanceGroupLSStatOperator()
{
}

int ObBalanceGroupLSStatOperator::init(
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(nullptr == sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy));
  } else {
    sql_proxy_ = sql_proxy;
    inited_ = true;
  }
  return ret;
}

int ObBalanceGroupLSStatOperator::get_balance_group_ls_stat(
    const int64_t timeout,
    const uint64_t tenant_id,
    const ObBalanceGroupID &balance_group_id,
    common::ObIArray<ObBalanceGroupLSStat> &balance_group_ls_stat_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(timeout <= 0
                         || OB_INVALID_ID == tenant_id
                         || !balance_group_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(timeout),
             K(tenant_id),
             K(balance_group_id));
  } else if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ ptr is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(get_balance_group_ls_stat(
          timeout,
          *sql_proxy_,
          tenant_id,
          balance_group_id,
          false, /* for update */
          balance_group_ls_stat_array))) {
    LOG_WARN("fail to get balance group ls stat", KR(ret),
             K(tenant_id),
             K(balance_group_id));
  }
  return ret;
}

int ObBalanceGroupLSStatOperator::get_balance_group_ls_stat(
    const int64_t timeout,
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObBalanceGroupID &balance_group_id,
    const bool for_update,
    common::ObIArray<ObBalanceGroupLSStat> &balance_group_ls_stat_array)
{
  int ret = OB_SUCCESS;
  common::ObTimeoutCtx timeout_ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(timeout <= 0
                         || OB_INVALID_ID == tenant_id
                         || !balance_group_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(timeout),
             K(tenant_id),
             K(balance_group_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(
          timeout_ctx,
          timeout))) {
    LOG_WARN("fail to set timeout", KR(ret), K(timeout));
  } else {
    common::ObSqlString sql;
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    balance_group_ls_stat_array.reset();
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.append_fmt(
              "SELECT * FROM %s WHERE "
              "tenant_id = %ld AND "
              "balance_group_id_high = %ld AND "
              "balance_group_id_low = %ld%s",
              OB_ALL_BALANCE_GROUP_LS_STAT_TNAME,
              tenant_id,
              balance_group_id.id_high_,
              balance_group_id.id_low_,
              (for_update ? " FOR UPDATE" : "")))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_client.read(res, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql_tenant_id), K(sql));
      } else if (OB_UNLIKELY(nullptr == (result = res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql res failed", KR(ret), K(sql));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ObBalanceGroupLSStat tmp_bg_ls_stat;
          uint64_t tenant_id = OB_INVALID_ID;
          uint64_t id_high = OB_INVALID_ID;
          uint64_t id_low = OB_INVALID_ID;
          int64_t ls_id = 0;
          int64_t tablet_group_count = -1;
          ObString balance_group_name;
          EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "balance_group_id_high", id_high, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "balance_group_id_low", id_low, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "tablet_group_count", tablet_group_count, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "balance_group_name", balance_group_name);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(tmp_bg_ls_stat.build(
                    tenant_id,
                    ObBalanceGroupID(id_high, id_low),
                    share::ObLSID(ls_id),
                    tablet_group_count,
                    balance_group_name))) {
              LOG_WARN("fail to build balance group ls stat", KR(ret),
                       K(tenant_id),
                       K(id_high),
                       K(id_low),
                       K(ls_id),
                       K(tablet_group_count),
                       K(balance_group_name));
            } else if (OB_FAIL(balance_group_ls_stat_array.push_back(
                    tmp_bg_ls_stat))) {
              LOG_WARN("fail to push back", KR(ret));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObBalanceGroupLSStatOperator::insert_update_balance_group_ls_stat(
    const int64_t timeout,
    const uint64_t tenant_id,
    const ObBalanceGroupID &balance_group_id,
    const common::ObIArray<ObBalanceGroupLSStat> &balance_group_ls_stat_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(timeout <= 0
                         || OB_INVALID_ID == tenant_id
                         || !balance_group_id.is_valid()
                         || balance_group_ls_stat_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(timeout),
             K(tenant_id),
             K(balance_group_id),
             K(balance_group_ls_stat_array));
  } else if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ ptr is null", KR(ret), KP(sql_proxy_));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(
            sql_proxy_,
            gen_meta_tenant_id(tenant_id)))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else {
      if (OB_FAIL(insert_update_balance_group_ls_stat(
              timeout,
              trans,
              tenant_id,
              balance_group_id,
              balance_group_ls_stat_array))) {
        LOG_WARN("fail to insert update balance group ls stat", KR(ret));
      }
      // commit/abort
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", K(tmp_ret), "is_commit", OB_SUCCESS == ret);
        ret = (OB_SUCCESS == ret ? tmp_ret : ret);
      }
    }
  }
  return ret;
}

int ObBalanceGroupLSStatOperator::insert_update_balance_group_ls_stat(
    const int64_t timeout,
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObBalanceGroupID &balance_group_id,
    const common::ObIArray<ObBalanceGroupLSStat> &balance_group_ls_stat_array)
{
  int ret = OB_SUCCESS;
  common::ObTimeoutCtx timeout_ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(timeout <= 0
                         || OB_INVALID_ID == tenant_id
                         || !balance_group_id.is_valid()
                         || balance_group_ls_stat_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(tenant_id),
             K(balance_group_id),
             K(balance_group_ls_stat_array));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(
          timeout_ctx,
          timeout))) {
    LOG_WARN("fail to set timeout", KR(ret), K(timeout));
  } else {
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    for (int64_t i = 0; OB_SUCC(ret) && i < balance_group_ls_stat_array.count(); ++i) {
      int64_t affected_rows = -1;
      common::ObSqlString insert_update_sql;
      const ObBalanceGroupLSStat &balance_group_ls_stat = balance_group_ls_stat_array.at(i);
      if (OB_FAIL(generate_insert_update_sql(
              balance_group_ls_stat,
              insert_update_sql))) {
        LOG_WARN("fail to generate insert update sql", KR(ret),
                 K(balance_group_ls_stat),
                 K(insert_update_sql));
      } else if (OB_FAIL(sql_client.write(
              sql_tenant_id,
              insert_update_sql.ptr(),
              affected_rows))) {
        LOG_WARN("fail to insert update", KR(ret),
                 K(sql_tenant_id),
                 K(insert_update_sql));
      } else if (affected_rows > 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", KR(ret),
                 K(sql_tenant_id),
                 K(insert_update_sql),
                 K(affected_rows));
      }
    }
  }
  return ret;
}

int ObBalanceGroupLSStatOperator::inc_balance_group_ls_stat(
    const int64_t timeout,
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObBalanceGroupLSStat &ls_stat)
{
  int ret = OB_SUCCESS;
  common::ObTimeoutCtx timeout_ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(
             timeout <= 0
             || OB_INVALID_TENANT_ID == tenant_id
             || !ls_stat.get_balance_group_id().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_stat));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(
             timeout_ctx, timeout))) {
    LOG_WARN("fail to set timeout", KR(ret), K(timeout));
  } else {
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    common::ObSqlString inc_sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(generate_inc_sql_(ls_stat, inc_sql))) {
      LOG_WARN("fail to generate inc sql", KR(ret),
               K(tenant_id), K(ls_stat), K(inc_sql));
    } else if (OB_FAIL(sql_client.write(
               sql_tenant_id, inc_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to insert update", KR(ret),
               K(tenant_id), K(inc_sql));
    } else if (OB_UNLIKELY(affected_rows > 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", KR(ret),
               K(tenant_id), K(inc_sql), K(affected_rows));
    }
  }
  return ret;
}

int ObBalanceGroupLSStatOperator::delete_balance_group_ls_stat(
    const int64_t timeout,
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id= %ld", OB_ALL_BALANCE_GROUP_LS_STAT_TNAME, tenant_id))) {
    LOG_WARN("fail to format sql", KR(ret));
  } else if (OB_FAIL(sql_client.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to delete inner table", KR(ret), K(sql));
  }
  return ret;
}

int ObBalanceGroupLSStatOperator::generate_inc_sql_(
    const ObBalanceGroupLSStat &bg_ls_stat,
    common::ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!bg_ls_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_ls_stat));
  } else if (OB_FAIL(sql_string.append_fmt(
             "INSERT INTO %s ("
             "tenant_id, "
             "balance_group_id_high, "
             "balance_group_id_low, "
             "ls_id, "
             "tablet_group_count, "
             "balance_group_name)"
             " VALUES ("
             "%ld, %ld, %ld, %ld, %ld, '%s') "
             "ON DUPLICATE KEY UPDATE "
             "tablet_group_count = tablet_group_count + %ld, "
             "balance_group_name = '%s'",
             OB_ALL_BALANCE_GROUP_LS_STAT_TNAME,
             bg_ls_stat.get_tenant_id(),
             bg_ls_stat.get_balance_group_id().id_high_,
             bg_ls_stat.get_balance_group_id().id_low_,
             bg_ls_stat.get_ls_id().id(),
             bg_ls_stat.get_tablet_group_count(),
             to_cstring(ObHexEscapeSqlStr(bg_ls_stat.get_balance_group_name().str())),
             bg_ls_stat.get_tablet_group_count(),
             to_cstring(ObHexEscapeSqlStr(bg_ls_stat.get_balance_group_name().str()))))) {
    LOG_WARN("fail to append fmt", KR(ret), K(bg_ls_stat));
  } else {
    LOG_INFO("balance group ls inc sql", K(sql_string));
  }
  return ret;
}

int ObBalanceGroupLSStatOperator::generate_insert_update_sql(
    const ObBalanceGroupLSStat &bg_ls_stat,
    common::ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!bg_ls_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_ls_stat));
  } else {
    if (OB_FAIL(sql_string.append_fmt(
            "INSERT INTO %s ("
            "tenant_id, "
            "balance_group_id_high, "
            "balance_group_id_low, "
            "ls_id, "
            "tablet_group_count, "
            "balance_group_name)"
            " VALUES ("
            "%ld, %ld, %ld, %ld, %ld, '%s') "
            "ON DUPLICATE KEY UPDATE "
            "tablet_group_count = %ld, "
            "balance_group_name = '%s'",
            OB_ALL_BALANCE_GROUP_LS_STAT_TNAME,
            bg_ls_stat.get_tenant_id(),
            bg_ls_stat.get_balance_group_id().id_high_,
            bg_ls_stat.get_balance_group_id().id_low_,
            bg_ls_stat.get_ls_id().id(),
            bg_ls_stat.get_tablet_group_count(),
            to_cstring(ObHexEscapeSqlStr(bg_ls_stat.get_balance_group_name().str())),
            bg_ls_stat.get_tablet_group_count(),
            to_cstring(ObHexEscapeSqlStr(bg_ls_stat.get_balance_group_name().str()))))) {
      LOG_WARN("fail to append fmt", KR(ret), K(bg_ls_stat));
    } else {
      LOG_INFO("balance group ls update sql", K(sql_string));
    }
  }
  return ret;
}

ObNewTableTabletAllocator::ObNewTableTabletAllocator(
    const uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObMySQLProxy *sql_proxy,
    const bool use_parallel_ddl /*= false*/)
  : tenant_id_(tenant_id),
    schema_guard_(schema_guard),
    sql_proxy_(sql_proxy),
    bg_ls_stat_operator_(),
    status_(MyStatus::INVALID),
    ls_id_array_(),
    inited_(false),
    is_add_partition_(false),
    use_parallel_ddl_(use_parallel_ddl)
{
}

ObNewTableTabletAllocator::~ObNewTableTabletAllocator()
{
}

int ObNewTableTabletAllocator::init()
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy ptr is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(bg_ls_stat_operator_.init(sql_proxy_))) {
    LOG_WARN("fail to init bg_ls_stat_operator_", KR(ret));
  } else {
    status_ = MyStatus::WAIT_TO_PREPARE;
    is_add_partition_ = false;
    inited_ = true;
  }
  return ret;
}

int ObNewTableTabletAllocator::prepare(
    ObMySQLTransaction &trans,
    const share::schema::ObTableSchema &table_schema,
    bool is_add_partition)
{
  int ret = OB_SUCCESS;
  is_add_partition_ = is_add_partition;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(!table_schema.has_tablet())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet has not tablet", KR(ret), K(table_schema));
  } else if (MyStatus::WAIT_TO_PREPARE != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("NewTableTabletAllocator state not match", KR(ret), K(status_));
  } else if ((is_meta_tenant(table_schema.get_tenant_id()))
             || (is_sys_tenant(table_schema.get_tenant_id()))) {
    if (OB_FAIL(alloc_ls_for_meta_or_sys_tenant_tablet(table_schema))) {
      LOG_WARN("fail to alloc ls for meta or sys tenant tablet", KR(ret));
    }
  } else if (table_schema.is_duplicate_table()) {
    if (OB_FAIL(alloc_ls_for_duplicate_table_(table_schema))) {
      LOG_WARN("fail to alloc ls for duplicate tablet", KR(ret), K(table_schema));
    }
  } else {
    if (table_schema.is_index_table()) {
      if (table_schema.is_index_local_storage()) {
        // local index or global index with local storage
        if (OB_FAIL(alloc_ls_for_local_index_tablet(table_schema))) {
          LOG_WARN("fail to alloc ls for local index tablet", KR(ret));
        }
      } else {
        // global index
        if (OB_FAIL(alloc_ls_for_global_index_tablet(table_schema))) {
          LOG_WARN("fail to alloc ls for global index tablet", KR(ret));
        }
      }
    } else {
      if (OB_INVALID_ID != table_schema.get_tablegroup_id()) {
        if (OB_FAIL(alloc_ls_for_in_tablegroup_tablet(table_schema))) {
          LOG_WARN("fail to alloc ls for in tablegroup tablet", KR(ret));
        }
      } else {
        if (OB_FAIL(alloc_ls_for_normal_table_tablet(table_schema))) {
          LOG_WARN("fail to alloc ls for normal table tablet", KR(ret));
        }
      }
    }

    DEBUG_SYNC(BEFORE_LOCK_LS_WHEN_CREATE_TABLE);
    // If ls status is not normal or is blocking tablet in, choose new ls for tablet creating.
    if (OB_FAIL(ret)) {
    } else if (is_related_table(table_schema.get_table_type(), table_schema.get_index_type())) {
      // skip lock ls
    } else if (OB_FAIL(check_and_replace_ls_(trans, table_schema.get_tenant_id()))) {
      LOG_WARN("lock user ls failed", KR(ret),
               "tenant_id", table_schema.get_tenant_id(), K_(ls_id_array));
    }
  }

  if (OB_SUCC(ret)) {
    status_ = MyStatus::WAIT_TO_OUTPUT;
  }
  is_add_partition_ = false;
  return ret;
}

int ObNewTableTabletAllocator::prepare_like(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(!table_schema.has_tablet())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet has not tablet", KR(ret), K(table_schema));
  } else if (MyStatus::WAIT_TO_PREPARE != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("NewTableTabletAllocator state not match", KR(ret), K(status_));
  } else if ((is_meta_tenant(table_schema.get_tenant_id()))
             || (is_sys_tenant(table_schema.get_tenant_id()))) {
    if (OB_FAIL(alloc_ls_for_meta_or_sys_tenant_tablet(table_schema))) {
      LOG_WARN("fail to alloc ls for meta or sys tenant tablet", KR(ret));
    }
  } else if (OB_FAIL(alloc_tablet_by_primary_schema(table_schema))) {
    LOG_WARN("fail to alloc tablet by primary schema", KR(ret), K(table_schema));
  }
  if (OB_SUCC(ret)) {
    status_ = MyStatus::WAIT_TO_OUTPUT;
  }
  return ret;
}

int ObNewTableTabletAllocator::get_ls_id_array(
    common::ObIArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (MyStatus::WAIT_TO_OUTPUT != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("NewTableTabletAllocator state not match", KR(ret), K(status_));
  } else {
    ls_id_array.reset();
    if (OB_FAIL(ls_id_array.assign(ls_id_array_))) {
      LOG_WARN("fail to assign ls id array", KR(ret));
    } else {
      ls_id_array_.reset();
      status_ = MyStatus::WAIT_TO_PREPARE;
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::finish(
    const bool commit)
{
  UNUSED(commit);
  return OB_SUCCESS;
}

int ObNewTableTabletAllocator::get_tablet_id_array(
    const share::schema::ObTableSchema &table_schema,
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  tablet_id_array.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    schema::ObPartitionSchemaIter iter(table_schema, schema::CHECK_PARTITION_MODE_NORMAL);
    schema::ObPartitionSchemaIter::Info info;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next_partition_info(info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(tablet_id_array.push_back(info.tablet_id_))) {
        LOG_WARN("fail to push tablet_id to array", KR(ret), K(info.tablet_id_));
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_by_primary_schema(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc tablet by primary schema",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ ptr is null", KR(ret));
  } else {
    common::ObArray<common::ObTabletID> tablet_id_array;
    if (OB_FAIL(get_tablet_id_array(table_schema, tablet_id_array))) {
      LOG_WARN("fail to get tablet id array", KR(ret));
    } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(
            *sql_proxy_,
            tenant_id_,
            tablet_id_array,
            ls_id_array_))) {
      LOG_WARN("fail to batch get ls", KR(ret));
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::get_available_ls(
    common::ObIArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy ptr is null", KR(ret));
  } else {
    share::ObLSAttrOperator ls_attr_operator(tenant_id_, sql_proxy_);
    ObLSAttrArray ls_attr_array;
    if (OB_FAIL(ls_attr_operator.get_all_ls_by_order(ls_attr_array))) {
      LOG_WARN("fail to load all ls", KR(ret), K_(tenant_id));
    } else {
      ARRAY_FOREACH(ls_attr_array, idx) {
        share::ObLSAttr &ls_attr = ls_attr_array.at(idx);
        if (ls_attr.ls_is_normal()
            && SYS_LS != ls_attr.get_ls_id()
            && !ls_attr.get_ls_flag().is_block_tablet_in()
            && !ls_attr.get_ls_flag().is_duplicate_ls()) {
          if (OB_FAIL(ls_id_array.push_back(ls_attr.get_ls_id()))) {
            LOG_WARN("fail to push back", KR(ret), K(ls_attr), K(ls_id_array));
          }
        }
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_create_balance_group(
    const ObBalanceGroupName &bg_name,
    const ObBalanceGroupID &bg_id,
    const common::ObIArray<share::ObLSID> &ls_id_array,
    const int64_t part_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(bg_name.is_empty()
                         || !bg_id.is_valid()
                         || ls_id_array.count() <= 0
                         || part_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(bg_name),
             K(bg_id),
             K(ls_id_array),
             K(part_num));
  } else {
    const int64_t bucket_num = ls_id_array.count();
    const int64_t min_itl = part_num / bucket_num;
    const int64_t max_itl = ((min_itl * bucket_num == part_num) ? (min_itl) : (min_itl + 1));
    const int64_t min_cnt = max_itl * bucket_num - part_num;
    const int64_t max_cnt = bucket_num - min_cnt;
    common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
    int64_t start_idx = fetch_ls_offset();
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array.count(); ++i) {
      const share::ObLSID &ls_id = ls_id_array.at((start_idx + i) % ls_id_array.count());
      const int64_t tablet_cnt = ((i < min_cnt) ? min_itl : max_itl);
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_cnt; ++j) {
        if (OB_FAIL(ls_id_array_.push_back(ls_id))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObBalanceGroupLSStat bg_ls_stat;
        if (OB_FAIL(bg_ls_stat.build(
                tenant_id_,
                bg_id,
                ls_id,
                tablet_cnt,
                bg_name))) {
          LOG_WARN("fail to build bg ls stat", KR(ret));
        } else if (OB_FAIL(bg_ls_stat_array.push_back(
                bg_ls_stat))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(bg_ls_stat_operator_.insert_update_balance_group_ls_stat(
              THIS_WORKER.get_timeout_remain(),
              *sql_proxy_,
              tenant_id_,
              bg_id,
              bg_ls_stat_array))) {
        LOG_WARN("fail to insert update balance group ls stat", KR(ret));
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_add_balance_group(
    const common::ObIArray<ObBalanceGroupLSStat> &bg_ls_stat_array,
    const ObBalanceGroupName &bg_name,
    const ObBalanceGroupID &bg_id,
    const common::ObIArray<share::ObLSID> &ls_id_array,
    const int64_t partition_num)
{
  int ret = OB_SUCCESS;
  // suppose bg_ls_stat_array can be empty
  common::hash::ObHashSet<share::ObLSID> ls_id_set;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(bg_name.is_empty()
                         || !bg_id.is_valid()
                         || ls_id_array.count() <= 0
                         || partition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(bg_name),
             K(bg_id),
             K(ls_id_array),
             K(partition_num));
  } else if (OB_FAIL(ls_id_set.create(MAX_TENANT_LS_CNT))) {
    LOG_WARN("fail to create ls id set", KR(ret));
  } else {
    common::ObArray<ObBalanceGroupLSStat> final_ls_stat_array;
    int64_t total_alloc_num = partition_num;
    int64_t valid_bg_cnt = total_alloc_num;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array.count(); ++i) {
      const share::ObLSID &ls_id = ls_id_array.at(i);
      if (OB_FAIL(ls_id_set.set_refactored(ls_id, 0/*not overwrite*/))) {
        LOG_WARN("fail to set refactored", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < bg_ls_stat_array.count(); ++i) {
      const share::ObLSID &ls_id = bg_ls_stat_array.at(i).get_ls_id();
      int tmp_ret = ls_id_set.exist_refactored(ls_id);
      LOG_INFO("balance group ls stat", "bg_ls_stat", bg_ls_stat_array.at(i));
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        // ls not available
      } else if (OB_HASH_EXIST == tmp_ret) {
        if (OB_FAIL(final_ls_stat_array.push_back(bg_ls_stat_array.at(i)))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(ls_id_set.erase_refactored(ls_id))) {
          LOG_WARN("fail to erase refactored", KR(ret));
        } else {
          valid_bg_cnt += bg_ls_stat_array.at(i).get_tablet_group_count();
        }
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to check exist", KR(ret), K(ls_id));
      }
    }
    for (common::hash::ObHashSet<share::ObLSID>::iterator iter = ls_id_set.begin();
         OB_SUCC(ret) && iter != ls_id_set.end();
         ++iter) {
      ObBalanceGroupLSStat bg_ls_stat;
      if (OB_FAIL(bg_ls_stat.build(
              tenant_id_,
              bg_id,
              iter->first, /*ls_id*/
              0,/*bg cnt*/
              bg_name))) {
        LOG_WARN("fail to build bg ls stat", KR(ret),
                 K(tenant_id_),
                 K(bg_id),
                 K(bg_name),
                 "ls_id", iter->first);
      } else if (OB_FAIL(final_ls_stat_array.push_back(bg_ls_stat))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }

    if (OB_FAIL(ret)) {
      // bypass
    } else if (OB_UNLIKELY(final_ls_stat_array.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("final ls stat array count unexpected", KR(ret), K(final_ls_stat_array));
    } else {
      lib::ob_sort(final_ls_stat_array.begin(), final_ls_stat_array.end());
      for (int64_t alloc_seq = 0; OB_SUCC(ret) && alloc_seq < total_alloc_num; alloc_seq++) {
        int64_t min_ls_tg_idx = 0;
        int64_t min_ls_tg_cnt = final_ls_stat_array.at(0).get_tablet_group_count();
        // find min
        for (int64_t i = 1; OB_SUCC(ret) && i < final_ls_stat_array.count(); ++i) {
          ObBalanceGroupLSStat &bg_ls_stat = final_ls_stat_array.at(i);
          if (bg_ls_stat.get_tablet_group_count() < min_ls_tg_cnt) {
            min_ls_tg_idx = i;
            min_ls_tg_cnt = bg_ls_stat.get_tablet_group_count();
          }
        }
        if (OB_SUCC(ret)) {
          final_ls_stat_array.at(min_ls_tg_idx).add_tablet_group_count(1);
          if (OB_FAIL(ls_id_array_.push_back(final_ls_stat_array.at(min_ls_tg_idx).get_ls_id()))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(bg_ls_stat_operator_.insert_update_balance_group_ls_stat(
                THIS_WORKER.get_timeout_remain(),
                *sql_proxy_,
                tenant_id_,
                bg_id,
                final_ls_stat_array))) {
          LOG_WARN("fail to insert update balance group ls stat", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_one_level_partitioned_balance_group(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc tablet for one level partitioned balance group",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  common::ObArray<share::ObLSID> ls_id_array;
  ObBalanceGroup bg;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(PARTITION_LEVEL_ONE != table_schema.get_part_level())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "part_level", table_schema.get_part_level());
  } else if (OB_FAIL(bg.init_by_table(table_schema, NULL/*partition*/))) {
    LOG_WARN("fail to get one level partitioned bg info", KR(ret));
  } else if (OB_FAIL(get_available_ls(ls_id_array))) {
    LOG_WARN("fail to get available ls", KR(ret));
  } else {
    if (!is_add_partition_) {
      if (OB_FAIL(alloc_tablet_for_create_balance_group(
          bg.name(),
          bg.id(),
          ls_id_array,
          table_schema.get_partition_num()))) {
        LOG_WARN("fail to alloc tablet for create balance group", KR(ret));
      }
    } else {
      common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
      if (OB_FAIL(bg_ls_stat_operator_.get_balance_group_ls_stat(
              THIS_WORKER.get_timeout_remain(),
              *sql_proxy_,
              tenant_id_,
              bg.id(),
              false, /*for update*/
              bg_ls_stat_array))) {
        LOG_WARN("fail to get balance group ls stat", KR(ret),
                 K(tenant_id_), K(bg));
      } else if (OB_FAIL(alloc_tablet_for_add_balance_group(
          bg_ls_stat_array,
          bg.name(),
          bg.id(),
          ls_id_array,
          table_schema.get_partition_num()))) {
        LOG_WARN("fail to alloc tablet for add balance group", KR(ret), K(bg));
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_two_level_partitioned_balance_group(
    const share::schema::ObTableSchema &table_schema,
    const int64_t part_idx)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc tablet for two level partitioned balance group",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  common::ObArray<share::ObLSID> ls_id_array;
  ObBalanceGroup bg;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(PARTITION_LEVEL_TWO != table_schema.get_part_level())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "part_level", table_schema.get_part_level());
  } else if (OB_UNLIKELY(part_idx >= table_schema.get_partition_num())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part idx", KR(ret), K(part_idx),
             "part_num", table_schema.get_partition_num());
  } else {
    const schema::ObPartition *partition = NULL;
    if (OB_FAIL(table_schema.get_partition_by_partition_index(part_idx, schema::CHECK_PARTITION_MODE_NORMAL, partition))) {
      LOG_WARN("get_partition_by_partition_index fail", KR(ret), K(part_idx), K(table_schema));
    } else if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part ptr is null", KR(ret), K(part_idx), K(table_schema));
    } else if (OB_FAIL(bg.init_by_table(table_schema, partition))) {
      LOG_WARN("fail to init two level partitioned bg info", KR(ret), K(table_schema), K(partition));
    } else if (OB_FAIL(get_available_ls(ls_id_array))) {
      LOG_WARN("fail to get available ls", KR(ret));
    } else {
      if (!is_add_partition_) {
        if (OB_FAIL(alloc_tablet_for_create_balance_group(
            bg.name(),
            bg.id(),
            ls_id_array,
            partition->get_subpartition_num()))) {
          LOG_WARN("fail to alloc tablet for create balance group", KR(ret));
        }
      } else {
        common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
        if (OB_FAIL(bg_ls_stat_operator_.get_balance_group_ls_stat(
                THIS_WORKER.get_timeout_remain(),
                *sql_proxy_,
                tenant_id_,
                bg.id(),
                false, /*for update*/
                bg_ls_stat_array))) {
          LOG_WARN("fail to get balance group ls stat", KR(ret),
                   K(tenant_id_), K(bg));
        } else if (OB_FAIL(alloc_tablet_for_add_balance_group(
            bg_ls_stat_array,
            bg.name(),
            bg.id(),
            ls_id_array,
            partition->get_subpartition_num()))) {
          LOG_WARN("fail to alloc tablet for add balance group", KR(ret), K(bg));
        }
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_non_partitioned_balance_group(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc tablet for non partitioned balance group",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  ObBalanceGroup bg;
  common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
  common::ObArray<share::ObLSID> ls_id_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(PARTITION_LEVEL_ZERO != table_schema.get_part_level())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             "part_num", table_schema.get_all_part_num(),
             "part_level", table_schema.get_part_level(),
             K(table_schema));
  } else if (OB_FAIL(bg.init_by_table(table_schema, NULL/*partition*/))) {
    LOG_WARN("fail to init non partitioned bg info", KR(ret), K(bg), K(table_schema));
  } else if (OB_FAIL(get_available_ls(ls_id_array))) {
    LOG_WARN("fail to get available ls", KR(ret));
  } else if (OB_FAIL(bg_ls_stat_operator_.get_balance_group_ls_stat(
          THIS_WORKER.get_timeout_remain(),
          *sql_proxy_,
          tenant_id_,
          bg.id(),
          false, /*for update*/
          bg_ls_stat_array))) {
    LOG_WARN("fail to get balance group ls stat", KR(ret), K(tenant_id_), K(bg));
  } else if (OB_FAIL(alloc_tablet_for_add_balance_group(
          bg_ls_stat_array,
          bg.name(),
          bg.id(),
          ls_id_array,
          table_schema.get_all_part_num()))) {
    LOG_WARN("fail to alloc tablet for add balance group", KR(ret), K(bg), K(bg_ls_stat_array),
        K(ls_id_array), K(table_schema.get_all_part_num()));
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_non_partitioned_balance_group_by_cache_(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc tablet for non partitioned balance group by cache",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  common::ObArray<share::ObLSID> ls_id_array;
  share::ObLSID ls_id;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(PARTITION_LEVEL_ZERO != table_schema.get_part_level())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             "part_num", table_schema.get_all_part_num(),
             "part_level", table_schema.get_part_level(),
             K(table_schema));
  } else if (OB_FAIL(get_available_ls(ls_id_array))) {
    LOG_WARN("fail to get available ls", KR(ret));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", KR(ret));
  } else if (OB_FAIL(GCTX.root_service_->get_ddl_service()
                     .get_non_partitioned_tablet_allocator()
                     .alloc_tablet(tenant_id_, ls_id_array, ls_id))) {
    LOG_WARN("fail to alloc tablet by cache", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ls_id_array_.push_back(ls_id))) {
    LOG_WARN("fail to push back ls id", KR(ret), K_(tenant_id), K(ls_id));
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_partitioned_balance_group(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else {
    if (PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
      if (OB_FAIL(alloc_tablet_for_one_level_partitioned_balance_group(
              table_schema))) {
        LOG_WARN("fail to alloc tablet for one level partitioned bg", KR(ret));
      }
    } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_partition_num(); ++i) {
        if (OB_FAIL(alloc_tablet_for_two_level_partitioned_balance_group(
                table_schema, i))) {
          LOG_WARN("fail to alloc tablet for two level partitioned bg", KR(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part level unexpected", KR(ret),
               "part_level", table_schema.get_part_level());
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_by_count_balance(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (table_schema.get_all_part_num() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema part num unexpected", KR(ret),
             "part_num", table_schema.get_all_part_num(),
             "table_schema", table_schema);
  } else if (is_sys_table(table_schema.get_table_id())
             || is_sys_tenant(table_schema.get_tenant_id())) {
    for (int64_t i = 0; i < table_schema.get_all_part_num() && OB_SUCC(ret); i++) {
      if (OB_FAIL(ls_id_array_.push_back(ObLSID(SYS_LS)))) {
        LOG_WARN("failed to push_back", KR(ret), K(i));
      }
    }
  } else if (PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    if (!use_parallel_ddl_) {
      if (OB_FAIL(alloc_tablet_for_non_partitioned_balance_group(table_schema))) {
        LOG_WARN("fail to alloc tablet by non partitioned balance group", KR(ret));
      }
    } else {
      if (OB_FAIL(alloc_tablet_for_non_partitioned_balance_group_by_cache_(table_schema))) {
        LOG_WARN("fail to alloc tablet by non partitioned balance group by cache", KR(ret));
      }
    }
  } else {
    if (OB_FAIL(alloc_tablet_for_partitioned_balance_group(table_schema))) {
      LOG_WARN("fail to alloc tablet by partitioned balance group", KR(ret));
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_ls_for_meta_or_sys_tenant_tablet(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc ls for meta or sys tenant tablet",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if ((!is_meta_tenant(table_schema.get_tenant_id())
             && (!is_sys_tenant(table_schema.get_tenant_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table schema", KR(ret),
             "tenant_id", table_schema.get_tenant_id(), K(table_schema));
  } else {
    for (int64_t i = 0; i < table_schema.get_all_part_num() && OB_SUCC(ret); i++) {
      if (OB_FAIL(ls_id_array_.push_back(ObLSID(SYS_LS)))) {
        LOG_WARN("failed to push_back", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_ls_for_local_index_tablet(
    const share::schema::ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc ls for local index tablet",
           "tenant_id", index_schema.get_tenant_id(),
           "index_id", index_schema.get_table_id());
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else {
    const uint64_t tenant_id = index_schema.get_tenant_id();
    const uint64_t data_table_id = index_schema.get_data_table_id();
    const share::schema::ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(schema_guard_.get_table_schema(
        tenant_id, data_table_id, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(data_table_id));
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(data_table_id));
    } else if (OB_FAIL(alloc_tablet_by_primary_schema(
            *table_schema))) {
      LOG_WARN("fail to alloc tablet by guard", KR(ret), K(data_table_id));
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_ls_for_global_index_tablet(
    const share::schema::ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc ls for global index tablet",
           "tenant_id", index_schema.get_tenant_id(),
           "index_id", index_schema.get_table_id());
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_FAIL(alloc_tablet_by_count_balance(
          index_schema))) {
    LOG_WARN("fail to alloc tablet by count balance", KR(ret));
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_ls_for_in_tablegroup_tablet(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc ls for in tablegroup tablet",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_schema.get_tablegroup_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shall not be here for a table without tablegroup", KR(ret), K(table_schema));
  } else if (is_sys_table(table_schema.get_table_id())
             || is_sys_tenant(table_schema.get_tenant_id())) {
    for (int64_t i = 0; i < table_schema.get_all_part_num() && OB_SUCC(ret); i++) {
      if (OB_FAIL(ls_id_array_.push_back(ObLSID(SYS_LS)))) {
        LOG_WARN("failed to push_back", KR(ret), K(i));
      }
    }
  } else {
    common::ObArray<const share::schema::ObTableSchema *> table_schema_array;
    const share::schema::ObSimpleTablegroupSchema *tablegroup_schema = NULL;
    if (OB_FAIL(schema_guard_.get_table_schemas_in_tablegroup(
            tenant_id_,
            table_schema.get_tablegroup_id(),
            table_schema_array))) {
      LOG_WARN("fail to get table schemas in tablegroup", KR(ret),
               "tenant_id", tenant_id_,
               "tablegroup_id", table_schema.get_tablegroup_id());
    } else if (OB_FAIL(schema_guard_.get_tablegroup_schema(tenant_id_, table_schema.get_tablegroup_id(), tablegroup_schema))) {
      LOG_WARN("fail to get tablegroup_schema", KR(ret), K(table_schema.get_tablegroup_id()));
    } else if (OB_ISNULL(tablegroup_schema) || !tablegroup_schema->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablegroup_schema invalid", KR(ret), K(tablegroup_schema));
    } else if (table_schema_array.count() > 0) {
      if (OB_UNLIKELY(nullptr == table_schema_array.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", KR(ret), K(table_schema_array));
      } else if (!is_add_partition_ || tablegroup_schema->get_sharding() == OB_PARTITION_SHARDING_NONE) {
        if (OB_FAIL(alloc_tablet_for_tablegroup(*table_schema_array.at(0), table_schema, *tablegroup_schema))) {
          LOG_WARN("fail to alloc tablet for tablegroup", KR(ret), K(is_add_partition_), K(tablegroup_schema), K(*table_schema_array.at(0)), K(table_schema));
        }
      } else if (tablegroup_schema->get_sharding() == OB_PARTITION_SHARDING_ADAPTIVE) {
        // add partition for tablegroup table may break the constraint of sharding ADAPTIVE
        // so alloc tablet as new table
        if (OB_FAIL(alloc_tablet_for_tablegroup(table_schema, *tablegroup_schema))) {
          LOG_WARN("fail to alloc tablet for tablegroup", KR(ret), K(table_schema), K(tablegroup_schema));
        }
      } else if (tablegroup_schema->get_sharding() == OB_PARTITION_SHARDING_PARTITION) {
        /* add partition for tablegroup sharding=PARTITION, we process only add subpart binding to existing one level partition
         * otherwise alloc tablet as new table
         */
        const ObTableSchema *origin_table_schema = NULL;
        if (OB_FAIL(schema_guard_.get_table_schema(table_schema.get_tenant_id(), table_schema.get_table_id(), origin_table_schema))) {
          LOG_WARN("fail to get origin table_schema", KR(ret), K(table_schema.get_table_id()));
        } else if (OB_ISNULL(origin_table_schema)) {
          ret = OB_ERR_UNDEFINED;
          LOG_WARN("origin_table_schema is null", KR(ret), K(table_schema.get_table_id()));
        } else if (OB_FAIL(alloc_tablet_for_add_part_in_tablegroup_sharding_partition(table_schema, *origin_table_schema))) {
          LOG_WARN("fail to alloc_tablet_for_tablegroup_add_part", KR(ret), K(table_schema), K(origin_table_schema));
        }
      }
    } else {
      if (OB_FAIL(alloc_tablet_for_tablegroup(table_schema, *tablegroup_schema))) {
        LOG_WARN("fail to alloc tablet for tablegroup", KR(ret), K(table_schema));
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_add_part_in_tablegroup_sharding_partition(
    const schema::ObTableSchema &table_schema,
    const schema::ObTableSchema &origin_table_schema)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObLSID> origin_ls_id_array;
  common::ObArray<share::ObLSID> pre_ls_id_array;
  common::ObArray<share::ObLSID> avail_ls_id_array;
  if (table_schema.get_table_id() != origin_table_schema.get_table_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema not match", KR(ret), K(table_schema), K(origin_table_schema));
  } else if (OB_FAIL(generate_ls_array_by_primary_schema(origin_table_schema, origin_ls_id_array))) {
    LOG_WARN("fail to generate_ls_array_by_primary_schema", KR(ret), K(origin_table_schema));
  } else if (OB_FAIL(extract_one_level_ls_array_by_primary_schema(origin_table_schema, origin_ls_id_array, pre_ls_id_array))) {
    LOG_WARN("fail to extract_one_level_ls_array_by_primary_schema", KR(ret), K(origin_table_schema));
  } else if (OB_FAIL(get_available_ls(avail_ls_id_array))) {
    LOG_WARN("fail get_available_ls", KR(ret));
  } else if (avail_ls_id_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no available ls", KR(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < table_schema.get_partition_num(); i++) {
      const schema::ObPartition *partition = NULL;
      if (OB_FAIL(table_schema.get_partition_by_partition_index(i, schema::CHECK_PARTITION_MODE_NORMAL, partition))) {
        LOG_WARN("get_partition_by_partition_index fail", KR(ret), K(i), K(table_schema));
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part ptr is null", KR(ret), K(i), K(table_schema));
      } else {
        int64_t origin_part_index = OB_INVALID_INDEX;
        ObLSID dest_ls_id;
        int64_t need_ls_count = 1;
        if (schema::PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
          need_ls_count = partition->get_sub_part_num();
        }
        if (OB_FAIL(origin_table_schema.get_partition_index_by_id(partition->get_part_id(), schema::CHECK_PARTITION_MODE_NORMAL, origin_part_index))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            int64_t dest_idx = (fetch_ls_offset() % avail_ls_id_array.count());
            // table_group can't use count balance because no partition value assign ls rule
            dest_ls_id = avail_ls_id_array.at(dest_idx);
          }
        } else {
          dest_ls_id = pre_ls_id_array.at(origin_part_index);
        }
        for (int c = 0; OB_SUCC(ret) && c < need_ls_count; c++) {
          if (OB_FAIL(ls_id_array_.push_back(dest_ls_id))) {
            LOG_WARN("fail to push ls_id to array", KR(ret), K(pre_ls_id_array), K(origin_part_index));
          }
        }
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_tablegroup(
    const schema::ObTableSchema &table_schema,
    const schema::ObSimpleTablegroupSchema &tablegroup_schema)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObLSID> ls_id_array;
  if (OB_FAIL(get_available_ls(ls_id_array))) {
    LOG_WARN("fail to get available ls", KR(ret), K(tenant_id_));
  } else if (ls_id_array.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("empty ls to alloc", KR(ret), K(tenant_id_));
  } else if (tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_NONE || schema::PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    int64_t start_idx = fetch_ls_offset();
    ObLSID dest_ls_id = ls_id_array.at(start_idx % ls_id_array.count());
    for (int64_t i = 0; i < table_schema.get_all_part_num() && OB_SUCC(ret); i++) {
      if (OB_FAIL(ls_id_array_.push_back(dest_ls_id))) {
        LOG_WARN("failed to push_back", KR(ret), K(i));
      }
    }
  } else if (tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_PARTITION || schema::PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
    int64_t start_idx = fetch_ls_offset();
    for (int64_t i = 0; i < table_schema.get_partition_num() && OB_SUCC(ret); i++) {
      ObLSID dest_ls_id = ls_id_array.at((start_idx + i) % ls_id_array.count());
      if (schema::PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
        if (OB_FAIL(ls_id_array_.push_back(dest_ls_id))) {
          LOG_WARN("failed to push_back", KR(ret), K(i));
        }
      } else if (schema::PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
        const schema::ObPartition *partition = NULL;
        if (OB_FAIL(table_schema.get_partition_by_partition_index(i, schema::CHECK_PARTITION_MODE_NORMAL, partition))) {
          LOG_WARN("get_partition_by_partition_index fail", KR(ret), K(i), K(table_schema));
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part ptr is null", KR(ret), K(i), K(table_schema));
        } else {
          for (int64_t sp = 0; OB_SUCC(ret) && sp < partition->get_subpartition_num(); sp++) {
            if (OB_FAIL(ls_id_array_.push_back(dest_ls_id))) {
              LOG_WARN("failed to push ls_id to array", KR(ret), K(dest_ls_id));
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part_level", KR(ret), K(table_schema.get_part_level()));
      }
    }
  } else if (tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_ADAPTIVE) {
    for (int64_t i = 0; i < table_schema.get_partition_num() && OB_SUCC(ret); i++) {
      int64_t start_idx = fetch_ls_offset();
      if (schema::PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
        const schema::ObPartition *partition = NULL;
        if (OB_FAIL(table_schema.get_partition_by_partition_index(i, schema::CHECK_PARTITION_MODE_NORMAL, partition))) {
          LOG_WARN("get_partition_by_partition_index fail", KR(ret), K(i), K(table_schema));
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part ptr is null", KR(ret), K(i), K(table_schema));
        } else {
          for (int64_t sp = 0; OB_SUCC(ret) && sp < partition->get_subpartition_num(); sp++) {
            ObLSID dest_ls_id = ls_id_array.at((start_idx + sp) % ls_id_array.count());
            if (OB_FAIL(ls_id_array_.push_back(dest_ls_id))) {
              LOG_WARN("failed to push ls_id to array", KR(ret), K(dest_ls_id));
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part_level", KR(ret), K(table_schema.get_part_level()));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknow sharding option", KR(ret), K(tablegroup_schema.get_sharding()));
  }
  return ret;
}

int ObNewTableTabletAllocator::generate_ls_array_by_primary_schema(
    const schema::ObTableSchema &primary_schema,
    common::ObArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;

  ls_id_array.reuse();
  common::ObArray<common::ObTabletID> tablet_id_array;
  if (OB_FAIL(get_tablet_id_array(primary_schema, tablet_id_array))) {
    LOG_WARN("fail to get tablet id array", KR(ret), K(primary_schema));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(
          *sql_proxy_,
          tenant_id_,
          tablet_id_array,
          ls_id_array))) {
    LOG_WARN("fail to batch get ls", KR(ret), K(tenant_id_), K(tablet_id_array), K(primary_schema));
  } else if (ls_id_array.count() != primary_schema.get_all_part_num()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty pre_ls_id_array", KR(ret), K(tenant_id_), K(tablet_id_array), K(primary_schema));
  }

  return ret;
}

int ObNewTableTabletAllocator::extract_one_level_ls_array_by_primary_schema(
    const schema::ObTableSchema &primary_schema,
    common::ObArray<share::ObLSID> &all_ls_id_array,
    common::ObArray<share::ObLSID> &pre_ls_id_array)
{
  int ret = OB_SUCCESS;
  pre_ls_id_array.reuse();
  if (primary_schema.get_all_part_num() != all_ls_id_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part count not match", KR(ret), K(primary_schema.get_all_part_num()), K(all_ls_id_array.count()));
  } else {
    int64_t primary_partition_offset = 0;
    for (int64_t part_idx = 0; OB_SUCC(ret) && part_idx < primary_schema.get_partition_num(); part_idx++) {
      if (OB_FAIL(pre_ls_id_array.push_back(all_ls_id_array.at(primary_partition_offset)))) {
        LOG_WARN("push ls_id to array", KR(ret), K(primary_partition_offset), K(all_ls_id_array));
      } else if (schema::PARTITION_LEVEL_ONE == primary_schema.get_part_level()) {
        primary_partition_offset++;
      } else if (schema::PARTITION_LEVEL_TWO == primary_schema.get_part_level()) {
        const schema::ObPartition *partition = NULL;
        if (OB_FAIL(primary_schema.get_partition_by_partition_index(part_idx, schema::CHECK_PARTITION_MODE_NORMAL, partition))) {
          LOG_WARN("get_partition_by_partition_index fail", KR(ret), K(part_idx), K(primary_schema));
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part ptr is null", KR(ret), K(part_idx), K(primary_schema));
        } else {
          primary_partition_offset += partition->get_subpartition_num();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknow table part_level", KR(ret), K(primary_schema));
      }
    }
  }

  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_tablegroup(
    const schema::ObTableSchema &primary_schema,
    const schema::ObTableSchema &table_schema,
    const schema::ObSimpleTablegroupSchema &tablegroup_schema)
{
  int ret = OB_SUCCESS;
  if (tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_NONE || table_schema.get_part_level() == schema::PARTITION_LEVEL_ZERO) {
    common::ObArray<share::ObLSID> pre_ls_id_array;
    if (OB_FAIL(generate_ls_array_by_primary_schema(primary_schema, pre_ls_id_array))) {
      LOG_WARN("fail to generate_ls_array_by_primary_schema", KR(ret), K(primary_schema));
    } else {
      // first tablet location ls
      ObLSID dest_ls_id = pre_ls_id_array.at(0);
      for (int64_t i = 0; i < table_schema.get_all_part_num() && OB_SUCC(ret); i++) {
        if (OB_FAIL(ls_id_array_.push_back(dest_ls_id))) {
          LOG_WARN("failed to push_back", KR(ret), K(i), K(tenant_id_), K(table_schema));
        }
      }
    }
  } else if (tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_PARTITION) {
    common::ObArray<share::ObLSID> all_ls_id_array;
    common::ObArray<share::ObLSID> pre_ls_id_array;
    if (primary_schema.get_partition_num() != table_schema.get_partition_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mismatch partition num in tablegroup", KR(ret), K(primary_schema), K(table_schema));
    } else if (OB_FAIL(generate_ls_array_by_primary_schema(primary_schema, all_ls_id_array))) {
      LOG_WARN("fail to generate_ls_array_by_primary_schema", KR(ret), K(primary_schema));
    } else if (OB_FAIL(extract_one_level_ls_array_by_primary_schema(primary_schema, all_ls_id_array, pre_ls_id_array))) {
      LOG_WARN("fail to extract_one_level_ls_array_by_primary_schema", KR(ret), K(primary_schema));
    } else {
      /*
       * keep align with one level partition
       */
      for (int64_t i = 0; i < table_schema.get_partition_num() && OB_SUCC(ret); i++) {
        ObLSID dest_ls_id = pre_ls_id_array.at(i);
        if (schema::PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
          if (OB_FAIL(ls_id_array_.push_back(dest_ls_id))) {
            LOG_WARN("failed to push_back", KR(ret), K(i));
          }
        } else if (schema::PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
          const schema::ObPartition *partition = NULL;
          if (OB_FAIL(table_schema.get_partition_by_partition_index(i, schema::CHECK_PARTITION_MODE_NORMAL, partition))) {
            LOG_WARN("get_partition_by_partition_index fail", KR(ret), K(i), K(table_schema));
          } else if (OB_ISNULL(partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part ptr is null", KR(ret), K(i), K(table_schema));
          } else {
            for (int64_t sp = 0; OB_SUCC(ret) && sp < partition->get_subpartition_num(); sp++) {
              if (OB_FAIL(ls_id_array_.push_back(dest_ls_id))) {
                LOG_WARN("failed to push ls_id to array", KR(ret), K(dest_ls_id));
              }
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknow table part_level", KR(ret), K(table_schema));
        }
      }
    }
  } else if (tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_ADAPTIVE) {
    if (primary_schema.get_all_part_num() != table_schema.get_all_part_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mismatch partition in tablegroup", KR(ret), K(table_schema), K(primary_schema));
    } else if (OB_FAIL(alloc_tablet_by_primary_schema(primary_schema))) {
      LOG_WARN("fail to alloc tablet by primary_schema", KR(ret), K(primary_schema));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknow sharding option", KR(ret), K(tablegroup_schema.get_sharding()));
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_ls_for_normal_table_tablet(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  LOG_INFO("alloc ls for normal table tablet",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_FAIL(alloc_tablet_by_count_balance(table_schema))) {
    LOG_WARN("fail to alloc tablet by count balance", KR(ret));
  }
  return ret;
}

int ObNewTableTabletAllocator::wait_ls_elect_leader_(
    const uint64_t tenant_id,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(GCTX.location_service_)
             || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    bool has_leader = false;
    ObAddr ls_leader;
    while (OB_SUCC(ret) && !has_leader) {
      int tmp_ret = OB_SUCCESS;
      ls_leader.reset();
      const share::ObLSReplica *leader_replica = nullptr;
      if (0 > ctx.get_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait ls elect leader timeout", KR(ret));
      } else if (OB_TMP_FAIL(GCTX.location_service_->nonblock_get_leader(GCONF.cluster_id, tenant_id, ls_id, ls_leader))) {
        LOG_WARN("fail to get ls leader", KR(ret), K(tenant_id), K(ls_id), K(ls_leader));
      } else {
        has_leader = true;
      }
      if (OB_SUCC(ret) && !has_leader) {
        LOG_WARN("fail to wait log stream elect leader, need retry", K(tenant_id), K(ls_id), K(ls_leader));
        ob_usleep(WAIT_INTERVAL_US);
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_ls_for_duplicate_table_(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema.get_tenant_id();
  LOG_INFO("alloc ls for duplicate table tablet",
           "tenant_id", table_schema.get_tenant_id(),
           "table_id", table_schema.get_table_id());
  share::ObLSStatusOperator ls_status_operator;
  share::ObLSStatusInfo duplicate_ls_status_info;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)
             || OB_ISNULL(GCTX.location_service_)
             || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    obrpc::ObCreateDupLSArg arg;
    obrpc::ObCreateDupLSResult result;
    while (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      duplicate_ls_status_info.reset();
      if (0 > ctx.get_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait creating duplicate log stream timeout", KR(ret));
      } else if (OB_TMP_FAIL(ls_status_operator.get_duplicate_ls_status_info(
                             tenant_id,
                             *GCTX.sql_proxy_,
                             duplicate_ls_status_info,
                             share::OBCG_DEFAULT/*group_id*/))) {
        if (OB_ENTRY_NOT_EXIST == tmp_ret) {
          LOG_INFO("duplicate log stream not exist, should create one duplicate log stream");
          tmp_ret = OB_SUCCESS;
          // create duplicate ls
          ObAddr leader;
          const int64_t timeout = ctx.get_timeout();
          if (OB_TMP_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id, tenant_id,
                                                             SYS_LS, FALSE, leader))) {
            LOG_WARN("failed to get leader", KR(tmp_ret), K(tenant_id));
          } else if (OB_TMP_FAIL(arg.init(tenant_id))) {
            LOG_WARN("failed to init arg", KR(ret), K(tenant_id));
          } else if (OB_TMP_FAIL(GCTX.srv_rpc_proxy_->to(leader).timeout(timeout).notify_create_duplicate_ls(arg, result))) {
            LOG_WARN("failed to create tenant duplicate ls", KR(tmp_ret), K(tenant_id), K(leader), K(arg), K(timeout));
            if (OB_CONFLICT_WITH_CLONE == tmp_ret) {
              ret = tmp_ret;
              LOG_WARN("tenant is in clone procedure, can not create new log stream for now", KR(ret), K(tenant_id), K(arg));
            }
          }
        } else {
          LOG_WARN("fail to get duplicate log stream from table", KR(tmp_ret), K(tenant_id));
        }
      } else if (!duplicate_ls_status_info.ls_is_normal()) {
        LOG_TRACE("duplicate log stream is not in normal status", K(duplicate_ls_status_info));
      } else if (OB_FAIL(wait_ls_elect_leader_(
                             duplicate_ls_status_info.tenant_id_,
                             duplicate_ls_status_info.ls_id_))) {
        LOG_WARN("fail to wait duplicate ls elect leader", KR(ret), K(duplicate_ls_status_info));
      } else {
        for (int64_t i = 0; i < table_schema.get_all_part_num() && OB_SUCC(ret); i++) {
          if (OB_FAIL(ls_id_array_.push_back(duplicate_ls_status_info.ls_id_))) {
            LOG_WARN("failed to push_back", KR(ret), K(i), K(duplicate_ls_status_info));
          }
        }
        break;
      }
      if (OB_SUCC(ret)) {
        LOG_WARN("fail to get duplicate log stream, need retry", K(tenant_id), K(duplicate_ls_status_info));
        ob_usleep(WAIT_INTERVAL_US);
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::check_and_replace_ls_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSID> locked_ls_id_array;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_FAIL(locked_ls_id_array.reserve(ls_id_array_.count()))) {
    LOG_WARN("reserve failed", KR(ret), K(tenant_id), K_(ls_id_array));
  } else {
    ARRAY_FOREACH(ls_id_array_, idx) {
      ObLSID prev_ls_id;
      ObLSID new_ls_id;
      ObLSAttr new_ls_attr;
      ObLSAttr curr_ls_attr;
      const ObLSID curr_ls_id = ls_id_array_.at(idx);
      if (idx > 0) {
        int64_t index = OB_INVALID_INDEX;
        find_last_user_ls_(locked_ls_id_array, index);
        if (index >= 0 && index < locked_ls_id_array.count()) {
          prev_ls_id = locked_ls_id_array.at(index);
        }
      }
      if (curr_ls_id.is_sys_ls()) { // do not lock sys ls
        new_ls_id = curr_ls_id;
      } else if (OB_FAIL(lock_and_check_ls_(
          trans,
          tenant_id,
          locked_ls_id_array,
          curr_ls_id,
          curr_ls_attr))) {
        if (OB_STATE_NOT_MATCH == ret) {
          if (OB_FAIL(choose_new_ls_(tenant_id, curr_ls_attr, prev_ls_id, new_ls_id))) {
            LOG_WARN("choose new ls failed", KR(ret),
                K(tenant_id), K(curr_ls_attr), K(prev_ls_id), K(new_ls_id));
          } else if (OB_FAIL(lock_and_check_ls_(
              trans,
              tenant_id,
              locked_ls_id_array,
              new_ls_id,
              new_ls_attr))) {
            // new ls should not be OB_STATE_NOT_MATCH
            LOG_WARN("check and lock ls failed", KR(ret),
                K(tenant_id), K(locked_ls_id_array), K(new_ls_id), K(new_ls_attr));
          } else {
            LOG_INFO("the ls allocated for tablet creating has changed",
                KR(ret), K(tenant_id), "old_ls_id", curr_ls_id, K(new_ls_id));
          }
        } else {
          LOG_WARN("check and lock ls failed", KR(ret),
              K(tenant_id), K(locked_ls_id_array), K(curr_ls_id), K(curr_ls_attr));
        }
      } else { // lock user ls successfully
        new_ls_id = curr_ls_id;
      }
      if (FAILEDx(locked_ls_id_array.push_back(new_ls_id))){
        LOG_WARN("push back failed", KR(ret), K(new_ls_id), K(locked_ls_id_array));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (locked_ls_id_array.count() != ls_id_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_id_array count not match", KR(ret), K(tenant_id),
          "tmp_ls_id_arry count", locked_ls_id_array.count(),
          "ls_id_array_ count", ls_id_array_.count(), K(locked_ls_id_array), K_(ls_id_array));
    } else if (OB_FAIL(ls_id_array_.assign(locked_ls_id_array))) {
      LOG_WARN("assign failed", KR(ret), K(locked_ls_id_array), K_(ls_id_array));
    }
  }
  return ret;
}

void ObNewTableTabletAllocator::find_last_user_ls_(
    const ObIArray<ObLSID> &ls_id_array,
    int64_t &index)
{
  index = OB_INVALID_INDEX;
  for (int64_t i = ls_id_array.count() - 1; i >= 0; --i) {
    const ObLSID &curr_ls = ls_id_array.at(i);
    if (curr_ls.id() > ObLSID::MIN_USER_LS_ID) {
      index = i;
      break;
    }
  }
}

int ObNewTableTabletAllocator::lock_and_check_ls_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const ObIArray<ObLSID> &locked_ls_id_array,
    const ObLSID &ls_id,
    ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  ls_attr.reset();
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (!ls_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(ls_id));
  } else if (common::has_exist_in_array(locked_ls_id_array, ls_id)) {
    // ls has been locked
  } else {
    ObLSAttrOperator ls_operator(tenant_id, sql_proxy_);
    if (OB_FAIL(ObLSObjLockUtil::lock_ls_in_trans(
        trans,
        tenant_id,
        ls_id,
        SHARE))) {
      LOG_WARN("lock ls in trans failed", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(ls_operator.get_ls_attr(ls_id, false/*for_update*/, trans, ls_attr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ls_attr.reset();
        ret = OB_STATE_NOT_MATCH;
        LOG_INFO("ls has been deleted when creating tablet", KR(ret), K(ls_id));
      } else {
        LOG_WARN("get ls attr failed", KR(ret), K(ls_id), K(ls_attr));
      }
    } else if (!ls_attr.ls_is_normal() || ls_attr.get_ls_flag().is_block_tablet_in()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_TRACE("can not create tablet on this ls beacuse it is not in normal status or is block tablet in",
          KR(ret), K(tenant_id), K(ls_id), K(ls_attr));
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::choose_new_ls_(
    const uint64_t tenant_id,
    const ObLSAttr &old_ls_attr,
    const ObLSID &prev_ls_id,
    ObLSID &new_ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (!old_ls_attr.is_valid() || !old_ls_attr.ls_is_normal()) {
    if (prev_ls_id.is_valid()) {
      new_ls_id = prev_ls_id;
    } else {
      ObLSAttrOperator ls_operator(tenant_id, sql_proxy_);
      if (OB_FAIL(ls_operator.get_random_normal_user_ls(new_ls_id))) {
        LOG_WARN("get random normal user ls failed", KR(ret), K(tenant_id), K(new_ls_id));
      }
    }
  } else if (old_ls_attr.get_ls_flag().is_block_tablet_in()) {
    //only in 4200 canbe block tablet in, no need process data_version
    if (OB_FAIL(ObBalanceTaskTableOperator::get_merge_task_dest_ls_by_src_ls(
        *sql_proxy_,
        tenant_id,
        old_ls_attr.get_ls_id(),
        new_ls_id))) {
      LOG_WARN("get dest ls by src ls failed", KR(ret), K(tenant_id), K(old_ls_attr), K(new_ls_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ls_attr", KR(ret), K(old_ls_attr));
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
