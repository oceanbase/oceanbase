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

#include "ob_balance_group_ls_stat_operator.h"

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
#include "share/ls/ob_ls_table.h" // ObLSTable
#include "share/ls/ob_ls_table_operator.h" // ObLSTableOperator
#include "share/location_cache/ob_location_service.h" // ObLocationService
#include "share/ob_rpc_struct.h" // ObCreateDupLSArg & ObCreateDupLSResult

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace rootserver
{

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
    common::ObMySQLProxy *sql_proxy)
  : tenant_id_(tenant_id),
    schema_guard_(schema_guard),
    sql_proxy_(sql_proxy),
    trans_(),
    bg_ls_stat_operator_(),
    status_(MyStatus::INVALID),
    ls_id_array_(),
    inited_(false)
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
  } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(meta_tenant_id));
  } else {
    status_ = MyStatus::WAIT_TO_PREPARE;
    inited_ = true;
  }
  return ret;
}

int ObNewTableTabletAllocator::prepare(
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
  }
  if (OB_SUCC(ret)) {
    status_ = MyStatus::WAIT_TO_OUTPUT;
  }
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
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    // by pass, maybe invoked with !inited_
  } else if (!trans_.is_started()) {
    // bypass
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans_.end(commit))) {
      LOG_WARN("fail to end trans", KR(ret), KR(tmp_ret), K(commit));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
  }
  return ret;
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
  } else if (PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    if (OB_FAIL(tablet_id_array.push_back(table_schema.get_tablet_id()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(table_schema));
    }
  } else if (PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
    ObPartition **part_array = table_schema.get_part_array();
    if (OB_UNLIKELY(nullptr == part_array)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part array is null", K(table_schema), KR(ret));
    } else {
      for (int64_t i = 0; i < table_schema.get_partition_num() && OB_SUCC(ret); ++i) {
        ObPartition *this_part = part_array[i];
        if (OB_UNLIKELY(nullptr == this_part)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("NULL ptr", K(i), K(table_schema), KR(ret));
        } else if (OB_FAIL(tablet_id_array.push_back(this_part->get_tablet_id()))) {
          LOG_WARN("fail to append fmt", KR(ret), K(table_schema));
        }
      }
    }
  } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
    ObPartition **part_array = table_schema.get_part_array();
    if (OB_UNLIKELY(nullptr == part_array)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part array is null", K(table_schema), KR(ret));
    } else {
      for (int64_t i = 0; i < table_schema.get_partition_num() && OB_SUCC(ret); ++i) {
        ObPartition *this_part = nullptr;
        ObSubPartition **sub_part_array = nullptr;
        if (OB_UNLIKELY(nullptr == (this_part = part_array[i])
                        || nullptr == (sub_part_array = this_part->get_subpart_array()))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("NULL ptr", K(i), K(table_schema), KR(ret));
        } else {
          for (int64_t j = 0; j < this_part->get_subpartition_num() && OB_SUCC(ret); j++) {
            ObSubPartition *this_sub_part = sub_part_array[j];
            if (OB_UNLIKELY(nullptr == this_sub_part)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("NULL ptr", K(j), K(table_schema), KR(ret));
            } else if (OB_FAIL(tablet_id_array.push_back(this_sub_part->get_tablet_id()))) {
              LOG_WARN("fail to append fmt", KR(ret), K(table_schema));
            }
          }
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part level invalid", KR(ret), K(table_schema));
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
    common::ObIArray<share::ObLSStatusInfo> &ls_status_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(nullptr == sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy ptr is null", KR(ret));
  } else {
    share::ObLSStatusOperator ls_status_operator;
    common::ObArray<share::ObLSStatusInfo> my_ls_array;
    if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order(
            tenant_id_,
            my_ls_array,
            *sql_proxy_))) {
      LOG_WARN("fail to get all ls status by order", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < my_ls_array.count(); ++i) {
        share::ObLSStatusInfo &ls_status_info = my_ls_array.at(i);
        if (ls_status_info.ls_is_normal() && SYS_LS != ls_status_info.ls_id_ && !ls_status_info.is_duplicate_ls()) {
          if (OB_FAIL(ls_status_info_array.push_back(ls_status_info))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::get_balance_group_primary_schema(
    const share::schema::ObTableSchema &table_schema,
    const share::schema::ObPartitionSchema *&primary_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_INVALID_ID == table_schema.get_tablegroup_id()) {
    primary_schema = &table_schema;
  } else {
    const share::schema::ObTablegroupSchema *tg_schema = nullptr;
    if (OB_FAIL(schema_guard_.get_tablegroup_schema(
            table_schema.get_tenant_id(),
            table_schema.get_tablegroup_id(),
            tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", KR(ret), K(table_schema));
    } else if (OB_UNLIKELY(nullptr == tg_schema)) {
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_WARN("tablegroup not exist", KR(ret), "tg_id", table_schema.get_tablegroup_id());
    } else {
      primary_schema = tg_schema;
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::alloc_tablet_for_create_balance_group(
    const ObBalanceGroupName &bg_name,
    const ObBalanceGroupID &bg_id,
    const common::ObIArray<share::ObLSStatusInfo> &ls_status_info_array,
    const int64_t part_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(bg_name.is_empty()
                         || !bg_id.is_valid()
                         || ls_status_info_array.count() <= 0
                         || part_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(bg_name),
             K(bg_id),
             K(ls_status_info_array),
             K(part_num));
  } else {
    const int64_t bucket_num = ls_status_info_array.count();
    const int64_t min_itl = part_num / bucket_num;
    const int64_t max_itl = ((min_itl * bucket_num == part_num) ? (min_itl) : (min_itl + 1));
    const int64_t min_cnt = max_itl * bucket_num - part_num;
    const int64_t max_cnt = bucket_num - min_cnt;
    common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_info_array.count(); ++i) {
      const share::ObLSID &ls_id = ls_status_info_array.at(i).ls_id_;
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
              trans_,
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
    const common::ObIArray<share::ObLSStatusInfo> &ls_status_info_array,
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
                         || ls_status_info_array.count() <= 0
                         || partition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(bg_name),
             K(bg_id),
             K(ls_status_info_array),
             K(partition_num));
  } else if (OB_FAIL(ls_id_set.create(MAX_TENANT_LS_CNT))) {
    LOG_WARN("fail to create ls id set", KR(ret));
  } else {
    common::ObArray<ObBalanceGroupLSStat> final_ls_stat_array;
    int64_t total_alloc_num = partition_num;
    int64_t valid_bg_cnt = total_alloc_num;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_info_array.count(); ++i) {
      const share::ObLSID &ls_id = ls_status_info_array.at(i).ls_id_;
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
      const int64_t valid_ls_num = final_ls_stat_array.count();
      const int64_t min_itl = valid_bg_cnt / valid_ls_num;
      const int64_t max_itl = (min_itl * valid_ls_num == valid_bg_cnt) ? min_itl : min_itl + 1;
      int64_t min_cnt = max_itl * valid_ls_num - valid_bg_cnt;
      int64_t max_cnt = valid_ls_num - min_cnt;
      std::sort(final_ls_stat_array.begin(), final_ls_stat_array.end());
      for (int64_t i = 0;
           OB_SUCC(ret) && i < final_ls_stat_array.count() && total_alloc_num > 0;
           ++i) {
        int64_t alloc_num = 0;
        ObBalanceGroupLSStat &bg_ls_stat = final_ls_stat_array.at(i);
        if (bg_ls_stat.get_tablet_group_count() >= max_itl) {
          max_cnt--;
        } else if (max_cnt > 0) {
          max_cnt--;
          alloc_num = max_itl - bg_ls_stat.get_tablet_group_count();
        } else {
          alloc_num = min_itl - bg_ls_stat.get_tablet_group_count();
        }
        alloc_num = total_alloc_num > alloc_num ? alloc_num : total_alloc_num;
        total_alloc_num -= alloc_num;
        bg_ls_stat.add_tablet_group_count(alloc_num);
        for (int64_t j = 0; OB_SUCC(ret) && j < alloc_num; ++j) {
          if (OB_FAIL(ls_id_array_.push_back(bg_ls_stat.get_ls_id()))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(bg_ls_stat_operator_.insert_update_balance_group_ls_stat(
                THIS_WORKER.get_timeout_remain(),
                trans_,
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
  ObBalanceGroupName bg_name;
  ObBalanceGroupID bg_id;
  common::ObArray<share::ObLSStatusInfo> ls_status_info_array;
  const share::schema::ObPartitionSchema *partition_schema = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(PARTITION_LEVEL_ONE != table_schema.get_part_level())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "part_level", table_schema.get_part_level());
  } else if (OB_FAIL(get_balance_group_primary_schema(
          table_schema,
          partition_schema))) {
    LOG_WARN("fail to get balance group partition schema", KR(ret));
  } else if (OB_UNLIKELY(nullptr == partition_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition schema ptr is null", KR(ret), KP(partition_schema));
  } else if (OB_FAIL(get_one_level_partitioned_bg_info(
          *partition_schema,
          bg_name,
          bg_id))) {
    LOG_WARN("fail to get one level partitioned bg info", KR(ret));
  } else if (OB_FAIL(get_available_ls(ls_status_info_array))) {
    LOG_WARN("fail to get available ls", KR(ret));
  } else if (OB_FAIL(alloc_tablet_for_create_balance_group(
          bg_name,
          bg_id,
          ls_status_info_array,
          table_schema.get_partition_num()))) {
    LOG_WARN("fail to alloc tablet for create balance group", KR(ret));
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
  ObBalanceGroupName bg_name;
  ObBalanceGroupID bg_id;
  common::ObArray<share::ObLSStatusInfo> ls_status_info_array;
  const share::schema::ObPartitionSchema *partition_schema = nullptr;
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
  } else if (OB_FAIL(get_balance_group_primary_schema(
          table_schema,
          partition_schema))) {
    LOG_WARN("fail to get balance group partition schema", KR(ret));
  } else if (OB_UNLIKELY(nullptr == partition_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition schema ptr is null", KR(ret), KP(partition_schema));
  } else {
    ObPartition **part_array = nullptr;
    ObPartition *this_part = nullptr;
    if (OB_UNLIKELY(nullptr == (part_array = table_schema.get_part_array())
                    || nullptr == (this_part = part_array[part_idx]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part ptr is null", KR(ret), KP(part_array), KP(this_part));
    } else if (OB_FAIL(get_two_level_partitioned_bg_info(
            *partition_schema,
            *this_part,
            bg_name,
            bg_id))) {
      LOG_WARN("fail to get two level partitioned bg info", KR(ret));
    } else if (OB_FAIL(get_available_ls(ls_status_info_array))) {
      LOG_WARN("fail to get available ls", KR(ret));
    } else if (OB_FAIL(alloc_tablet_for_create_balance_group(
            bg_name,
            bg_id,
            ls_status_info_array,
            this_part->get_subpartition_num()))) {
      LOG_WARN("fail to alloc tablet for create balance group", KR(ret));
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
  ObBalanceGroupName bg_name;
  ObBalanceGroupID bg_id;
  common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
  common::ObArray<share::ObLSStatusInfo> ls_status_info_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObNewTableTabletAllocator not init", KR(ret));
  } else if (OB_UNLIKELY(table_schema.get_all_part_num() > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             "part_num", table_schema.get_all_part_num(),
             K(table_schema));
  } else if (OB_FAIL(get_non_partitioned_bg_info(
          tenant_id_,
          bg_name,
          bg_id))) {
    LOG_WARN("fail to get non partitioned bg info", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(get_available_ls(ls_status_info_array))) {
    LOG_WARN("fail to get available ls", KR(ret));
  } else if (OB_FAIL(bg_ls_stat_operator_.get_balance_group_ls_stat(
          THIS_WORKER.get_timeout_remain(),
          trans_,
          tenant_id_,
          bg_id,
          true, /*for update*/
          bg_ls_stat_array))) {
    LOG_WARN("fail to get balance group ls stat", KR(ret),
             K(tenant_id_), K(bg_id));
  } else if (OB_FAIL(alloc_tablet_for_add_balance_group(
          bg_ls_stat_array,
          bg_name,
          bg_id,
          ls_status_info_array,
          table_schema.get_all_part_num()))) {
    LOG_WARN("fail to alloc tablet for add balance group", KR(ret));
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
  } else if (1 == table_schema.get_all_part_num()) {
    if (OB_FAIL(alloc_tablet_for_non_partitioned_balance_group(table_schema))) {
      LOG_WARN("fail to alloc tablet by non partitioned balance group", KR(ret));
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
    if (OB_FAIL(schema_guard_.get_table_schemas_in_tablegroup(
            tenant_id_,
            table_schema.get_tablegroup_id(),
            table_schema_array))) {
      LOG_WARN("fail to get table schemas in tablegroup", KR(ret),
               "tenant_id", tenant_id_,
               "tablegroup_id", table_schema.get_tablegroup_id());
    } else if (table_schema_array.count() > 0) {
      if (OB_UNLIKELY(nullptr == table_schema_array.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", KR(ret), K(table_schema_array));
      } else if (OB_FAIL(alloc_tablet_by_primary_schema(*table_schema_array.at(0)))) {
        LOG_WARN("fail to alloc tablet by guard", KR(ret));
      }
    } else {
      if (OB_FAIL(alloc_tablet_by_count_balance(table_schema))) {
        LOG_WARN("fail to alloc tablet by count balance", KR(ret), K(table_schema));
      }
    }
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
                             duplicate_ls_status_info))) {
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

int ObNewTableTabletAllocator::get_non_partitioned_bg_info(
    const uint64_t tenant_id,
    ObBalanceGroupName &bg_name,
    ObBalanceGroupID &bg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    common::ObSqlString bg_name_str;
    if (OB_FAIL(bg_name_str.append_fmt(
            "BG_TENANT_%ld",
            tenant_id))) {
      LOG_WARN("fail to append fmt", KR(ret));
    } else if (OB_FAIL(bg_name.assign(
            bg_name_str.ptr()))) {
      LOG_WARN("fail to assign bg name", KR(ret), K(bg_name_str));
    } else {
      bg_id = ObBalanceGroupID(tenant_id,
                               0);
      LOG_INFO("get non partitioned bg info",
               K(tenant_id),
               K(bg_name),
               K(bg_id));
    }
  }
  return ret;
}

int ObNewTableTabletAllocator::get_one_level_partitioned_bg_info(
    const share::schema::ObPartitionSchema &entity_schema,
    ObBalanceGroupName &bg_name,
    ObBalanceGroupID &bg_id)
{
  int ret = OB_SUCCESS;
  common::ObSqlString bg_name_str;
  if (OB_FAIL(bg_name_str.append_fmt(
          "BG_ONE_LEVEL_PART_%s_%ld_%s",
          (OB_INVALID_ID != entity_schema.get_tablegroup_id()
           ? "TG" : "TB"),
          entity_schema.get_tenant_id(),
          entity_schema.get_entity_name()))) {
    LOG_WARN("fail to append fmt", KR(ret));
  } else if (OB_FAIL(bg_name.assign(
          bg_name_str.ptr()))) {
    LOG_WARN("fail to assign bg name", KR(ret), K(bg_name_str));
  } else {
    bg_id = ObBalanceGroupID(entity_schema.get_table_id(),
                             0);
    LOG_INFO("get non partitioned bg info",
             "tenant_id", entity_schema.get_tenant_id(),
             K(bg_name),
             K(bg_id));
  }
  return ret;
}

int ObNewTableTabletAllocator::get_two_level_partitioned_bg_info(
    const share::schema::ObPartitionSchema &entity_schema,
    const share::schema::ObPartition &partition_schema,
    ObBalanceGroupName &bg_name,
    ObBalanceGroupID &bg_id)
{
  int ret = OB_SUCCESS;
  common::ObSqlString bg_name_str;
  if (OB_FAIL(bg_name_str.append_fmt(
          "BG_TWO_LEVEL_PART_%s_%ld_%s_%s",
          (OB_INVALID_ID != entity_schema.get_tablegroup_id()
           ? "TG" : "TB"),
          entity_schema.get_tenant_id(),
          entity_schema.get_entity_name(),
          partition_schema.get_part_name().ptr()))) {
    LOG_WARN("fail to append fmt", KR(ret));
  } else if (OB_FAIL(bg_name.assign(
          bg_name_str.ptr()))) {
    LOG_WARN("fail to assign bg name", KR(ret), K(bg_name_str));
  } else {
    bg_id = ObBalanceGroupID(entity_schema.get_table_id(),
                             partition_schema.get_part_id());
    LOG_INFO("get non partitioned bg info",
             "tenant_id", entity_schema.get_tenant_id(),
             K(bg_name),
             K(bg_id));
  }
  return ret;
}
}//end namespace rootserver
}//end namespace oceanbase
