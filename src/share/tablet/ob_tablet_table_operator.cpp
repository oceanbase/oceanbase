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

#include "share/tablet/ob_tablet_table_operator.h"
#include "share/ob_errno.h" // KR(ret)
#include "share/inner_table/ob_inner_table_schema.h" // OB_ALL_TABLET_META_TABLE_TNAME
#include "share/ob_dml_sql_splicer.h" // ObDMLSqlSplicer
#include "lib/string/ob_sql_string.h" // ObSqlString
#include "lib/mysqlclient/ob_mysql_transaction.h" // ObMySQLTransaction
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObMySqlProxy
#include "share/ob_ls_id.h" // ObLSID
#include "observer/omt/ob_tenant_timezone_mgr.h" // for OTTZ_MGR.get_tenant_tz

namespace oceanbase
{
using namespace common;

namespace share
{
ObTabletTableOperator::ObTabletTableOperator()
    : inited_(false), sql_proxy_(NULL), batch_size_(MAX_BATCH_COUNT), group_id_(0)
{
}

ObTabletTableOperator::~ObTabletTableOperator()
{
  reset();
}

int ObTabletTableOperator::init(ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    batch_size_ = MAX_BATCH_COUNT;
    group_id_ = 0; /*OBCG_DEFAULT*/
    inited_ = true;
  }
  return ret;
}

int ObTabletTableOperator::init(const int32_t group_id, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table operator get invalid argument", K(ret), K(group_id));
  } else {
    sql_proxy_ = &sql_proxy;
    batch_size_ = MAX_BATCH_COUNT;
    group_id_ = group_id;
    inited_ = true;
  }
  return ret;
}

void ObTabletTableOperator::reset()
{
  inited_ = false;
  sql_proxy_ = NULL;
  batch_size_ = 0;
}

int ObTabletTableOperator::get(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const common::ObAddr &addr,
    ObTabletReplica &tablet_replica)
{
  int ret = OB_SUCCESS;
  ObTabletInfo tablet_info;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(get(tenant_id, tablet_id, ls_id, tablet_info))) {
    LOG_WARN("failed to get tablet info", K(ret), K(tenant_id), K(tablet_id), K(ls_id));
  } else {
    bool find = false;
    const ObArray<ObTabletReplica> &replicas = tablet_info.get_replicas();
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
      tablet_replica = replicas.at(i);
      if (tablet_replica.get_server() == addr) {
        find = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("not find entry", K(tablet_info));
    }
  }
  return ret;
}

// will fill empty tablet_info when tablet not exist
int ObTabletTableOperator::get(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const ObLSID &ls_id,
    ObTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObSEArray<ObTabletLSPair, 1> tablet_ls_pairs;
    ObSEArray<ObTabletInfo, 1> tablet_infos;
    if (OB_FAIL(tablet_ls_pairs.push_back(ObTabletLSPair(tablet_id, ls_id)))) {
      LOG_WARN("fail to push back tablet ls pair", KR(ret), K(tablet_id), K(ls_id));
    } else if (OB_FAIL(batch_get(tenant_id, tablet_ls_pairs, tablet_infos))) {
      LOG_WARN("fail to get tablet info", KR(ret), K(tenant_id), K(tablet_ls_pairs));
    } else if (1 != tablet_infos.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet_infos count should be one", KR(ret), "count", tablet_infos.count());
    } else if (OB_FAIL(tablet_info.assign(tablet_infos.at(0)))) {
      LOG_WARN("fail to assign tablet info", KR(ret), K(tablet_infos));
    }
  }
  return ret;
}

int ObTabletTableOperator::batch_get_tablet_info(
    common::ObISQLClient *sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<compaction::ObTabletCheckInfo> &tablet_ls_infos,
    const int32_t group_id,
    ObIArray<ObTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sql_proxy));
  } else {
    int64_t pairs_count = tablet_ls_infos.count();
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, pairs_count);
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      if (OB_FAIL(inner_batch_get_tablet_by_sql_(
          *sql_proxy,
          tenant_id,
          tablet_ls_infos,
          start_idx,
          end_idx,
          group_id,
          tablet_infos))) {
        LOG_WARN("fail to inner batch get by sql",
            KR(ret), K(tenant_id), K(tablet_ls_infos), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, pairs_count);
      }
    }
  }
  if (OB_SUCC(ret) && tablet_ls_infos.count() != tablet_infos.count()) {
    LOG_WARN("tablet_infos count is not same as tablet_ls_pairs count ", KR(ret), K(tablet_infos.count()), K(tablet_ls_infos.count()));
  }
  return ret;
}

int ObTabletTableOperator::inner_batch_get_tablet_by_sql_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObIArray<compaction::ObTabletCheckInfo> &tablet_ls_infos,
    const int64_t start_idx,
    const int64_t end_idx,
    const int32_t group_id,
    ObIArray<ObTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_ls_infos.empty()
      || OB_INVALID_TENANT_ID == tenant_id
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > tablet_ls_infos.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ls_infos), K(start_idx), K(end_idx));
  } else {
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    ObSqlString sql;
    ObSqlString part_sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.append_fmt(
          "SELECT * FROM %s WHERE tenant_id = %ld and (tablet_id,ls_id) IN (",
          OB_ALL_TABLET_META_TABLE_TNAME, tenant_id))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else {
        for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
          const ObTabletID &tablet_id = tablet_ls_infos.at(idx).get_tablet_id();
          const ObLSID &ls_id = tablet_ls_infos.at(idx).get_ls_id();
          if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id)
              || !ls_id.is_valid_with_tenant(tenant_id))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid tablet_id with tenant", KR(ret), K(tenant_id), K(tablet_id), K(ls_id));
          } else if (OB_FAIL(sql.append_fmt(
              "%s (%lu,%ld)",
              start_idx == idx ? "" : ",",
              tablet_id.id(),
              ls_id.id()))) {
            LOG_WARN("fail to assign sql", KR(ret), K(tablet_id));
          } else if (OB_FAIL(part_sql.append_fmt(
              ",%lu",
              tablet_id.id()))) {
            LOG_WARN("fail to assign sql", KR(ret), K(tablet_id));
          }
        }
      }
      if (FAILEDx(sql.append_fmt(") ORDER BY FIELD(tablet_id%s)", part_sql.string().ptr()))) {
        LOG_WARN("assign sql string failed", KR(ret));
      } else if (OB_FAIL(sql_client.read(result, sql_tenant_id, sql.ptr(), group_id))) {
        LOG_WARN("execute sql failed", KR(ret),
            K(tenant_id), K(sql_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), "sql", sql.ptr());
      } else if (OB_FAIL(construct_tablet_infos(*result.get_result(), tablet_infos))) {
        LOG_WARN("construct tablet info failed", KR(ret), K(tablet_infos));
      }
    }
  }
  return ret;
}

int ObTabletTableOperator::batch_get(
    const uint64_t tenant_id,
    const ObIArray<ObTabletLSPair> &tablet_ls_pairs,
    ObIArray<ObTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  tablet_infos.reset();
  const int64_t pairs_cnt = tablet_ls_pairs.count();
  hash::ObHashMap<ObTabletLSPair, bool> pairs_map;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(pairs_cnt < 1 || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pairs_cnt));
  }
  // Step 1: check duplicates by hash map
  if (FAILEDx(pairs_map.create(
      hash::cal_next_prime(pairs_cnt * 2),
      ObModIds::OB_HASH_BUCKET))) {
    LOG_WARN("fail to create pairs_map", KR(ret), K(pairs_cnt));
  } else {
    ARRAY_FOREACH_N(tablet_ls_pairs, idx, cnt) {
      // if same talet_id exist, return error
      if (OB_FAIL(pairs_map.set_refactored(tablet_ls_pairs.at(idx), false))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tablet_ls_pairs have duplicates", KR(ret), K(tablet_ls_pairs), K(idx));
        } else {
          LOG_WARN("fail to set refactored", KR(ret), K(tablet_ls_pairs), K(idx));
        }
      }
    } // end for
    if (OB_FAIL(ret)) {
    } else if (pairs_map.size() != pairs_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid pairs_map size", "size", pairs_map.size(), K(pairs_cnt));
    }
  }
  // Step 2: cut tablet_ls_pairs into small batches
  if (FAILEDx(tablet_infos.reserve(pairs_cnt))) {
    LOG_WARN("fail to reserve tablet_infos", KR(ret), K(pairs_cnt));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(batch_size_, pairs_cnt);
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      if (OB_FAIL(inner_batch_get_by_sql_(
          *sql_proxy_,
          tenant_id,
          tablet_ls_pairs,
          start_idx,
          end_idx,
          tablet_infos))) {
        LOG_WARN("fail to inner batch get by sql",
            KR(ret), K(tenant_id), K(tablet_ls_pairs), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + batch_size_, pairs_cnt);
      }
    }
  }
  // Step 3: check tablet_infos and push back empty tablet_info for tablets not exist
  if (OB_SUCC(ret) && (tablet_infos.count() < pairs_cnt)) {
    // check tablet infos and set flag in map
    int overwrite_flag = 1;
    ARRAY_FOREACH_N(tablet_infos, idx, cnt) {
      const ObTabletID &tablet_id = tablet_infos.at(idx).get_tablet_id();
      const ObLSID &ls_id = tablet_infos.at(idx).get_ls_id();
      if (OB_FAIL(pairs_map.set_refactored(ObTabletLSPair(tablet_id, ls_id), true, overwrite_flag))) {
        LOG_WARN("fail to set_fefactored", KR(ret), K(tablet_id), K(ls_id));
      }
    }
    // push back empty tablet_info
    if (OB_SUCC(ret)) {
      FOREACH_X(iter, pairs_map, OB_SUCC(ret)) {
        if (!iter->second) {
          ObArray<ObTabletReplica> replica; // empty replica
          ObTabletInfo tablet_info(
              tenant_id,
              iter->first.get_tablet_id(),
              iter->first.get_ls_id(),
              replica);
          if (OB_FAIL(tablet_infos.push_back(tablet_info))) {
            LOG_WARN("fail to push back tablet info", KR(ret), K(tablet_info));
          }
          LOG_TRACE("tablet not exist in meta table",
              KR(ret), K(tenant_id), "tablet_id", iter->first);
        }
      }
    }
  }
  return ret;
}

int ObTabletTableOperator::inner_batch_get_by_sql_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObIArray<ObTabletLSPair> &tablet_ls_pairs,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<ObTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_ls_pairs.empty()
      || OB_INVALID_TENANT_ID == tenant_id
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > tablet_ls_pairs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ls_pairs), K(start_idx), K(end_idx));
  } else {
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    ObSqlString sql;
    ObSqlString order_by_sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.append_fmt(
          "SELECT * FROM %s WHERE tenant_id = %ld and (tablet_id,ls_id) IN (",
          OB_ALL_TABLET_META_TABLE_TNAME, tenant_id))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else {
        for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
          const ObTabletID &tablet_id = tablet_ls_pairs.at(idx).get_tablet_id();
          const ObLSID &ls_id = tablet_ls_pairs.at(idx).get_ls_id();
          if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id)
              || !ls_id.is_valid_with_tenant(tenant_id))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid tablet_id with tenant", KR(ret), K(tenant_id), K(tablet_id), K(ls_id));
          } else if (OB_FAIL(sql.append_fmt(
              "%s (%lu,%lu)",
              start_idx == idx ? "" : ",",
              tablet_id.id(),
              ls_id.id()))) {
            LOG_WARN("fail to assign sql", KR(ret), K(tablet_id));
          } else  if (OB_FAIL(order_by_sql.append_fmt(
              ",%ld",
              tablet_id.id()))) {
            LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(tablet_id));
          }
        }
      }
      if (FAILEDx(sql.append_fmt(") ORDER BY FIELD(tablet_id %s)", order_by_sql.string().ptr()))) {
        LOG_WARN("assign sql string failed", KR(ret));
      } else if (OB_FAIL(sql_client.read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret),
            K(tenant_id), K(sql_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), "sql", sql.ptr());
      } else if (OB_FAIL(construct_tablet_infos(*result.get_result(), tablet_infos))) {
        LOG_WARN("construct tablet info failed", KR(ret), K(tablet_infos));
      }
    }
  }
  return ret;
}

int ObTabletTableOperator::construct_tablet_infos(
    sqlclient::ObMySQLResult &res,
    ObIArray<ObTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  ObTabletInfo tablet_info;
  ObTabletReplica replica;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
      break;
    } else {
      replica.reset();
      if (OB_FAIL(construct_tablet_replica_(res, replica))) {
        LOG_WARN("fail to construct tablet replica", KR(ret));
      } else if (OB_UNLIKELY(!replica.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("construct invalid replica", KR(ret), K(replica));
      } else if (tablet_info.is_self_replica(replica)) {
        if (OB_FAIL(tablet_info.add_replica(replica))) {
          LOG_WARN("fail to add replica", KR(ret), K(replica));
        }
      } else {
        if (tablet_info.is_valid()) {
          if (OB_FAIL(tablet_infos.push_back(tablet_info))) {
            LOG_WARN("fail to push back", KR(ret), K(tablet_info));
          }
        }
        tablet_info.reset();
        if (FAILEDx(tablet_info.init_by_replica(replica))) {
          LOG_WARN("fail to init tablet_info by replica", KR(ret), K(replica));
        }
      }
    }
  } // end while
  if (OB_SUCC(ret) && tablet_info.is_valid()) {
    // last tablet info
    if (OB_FAIL(tablet_infos.push_back(tablet_info))) {
      LOG_WARN("fail to push back", KR(ret), K(tablet_info));
    }
  }
  return ret;
}

int ObTabletTableOperator::construct_tablet_replica_(
    sqlclient::ObMySQLResult &res,
    ObTabletReplica &replica)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
  common::ObAddr server;
  ObString ip;
  int64_t port = OB_INVALID_INDEX;
  int64_t ls_id = OB_INVALID_ID;
  uint64_t uint_compaction_scn = 0;
  int64_t compaction_scn = 0;
  int64_t data_size = 0;
  int64_t required_size = 0;
  uint64_t uint_report_scn = 0;
  int64_t status_in_table = 0;
  ObTabletReplica::ScnStatus status = ObTabletReplica::SCN_STATUS_IDLE;
  bool skip_null_error = false;
  bool skip_column_error = true;

  (void) GET_COL_IGNORE_NULL(res.get_int, "tenant_id", tenant_id);
  (void) GET_COL_IGNORE_NULL(res.get_int, "tablet_id", tablet_id);
  (void) GET_COL_IGNORE_NULL(res.get_int, "ls_id", ls_id);
  (void) GET_COL_IGNORE_NULL(res.get_varchar, "svr_ip", ip);
  (void) GET_COL_IGNORE_NULL(res.get_int, "svr_port", port);
  (void) GET_COL_IGNORE_NULL(res.get_uint, "compaction_scn", uint_compaction_scn);
  (void) GET_COL_IGNORE_NULL(res.get_int, "data_size", data_size);
  (void) GET_COL_IGNORE_NULL(res.get_int, "required_size", required_size);

  EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(res, "report_scn", uint_report_scn, uint64_t, skip_null_error, skip_column_error, 0);
  EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(res, "status", status_in_table, int64_t, skip_null_error, skip_column_error, ObTabletReplica::SCN_STATUS_IDLE);

  status = (ObTabletReplica::ScnStatus)status_in_table;
  compaction_scn = static_cast<int64_t>(uint_compaction_scn);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!server.set_ip_addr(ip, static_cast<int32_t>(port)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", KR(ret), K(ip), K(port));
  } else if (OB_UNLIKELY(!ObTabletReplica::is_status_valid(status))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid status", K(ret), K(status_in_table));
  } else if (OB_FAIL(
      replica.init(
          tenant_id,
          ObTabletID(tablet_id),
          share::ObLSID(ls_id),
          server,
          compaction_scn,
          data_size,
          required_size,
          (int64_t)uint_report_scn,
          status))) {
    LOG_WARN("fail to init replica", KR(ret),
        K(tenant_id), K(tablet_id), K(server), K(ls_id), K(data_size), K(required_size));
  }
  LOG_TRACE("construct tablet replica", KR(ret), K(replica));
  return ret;
}

int ObTabletTableOperator::update(const ObTabletReplica &replica)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletReplica, 1> replicas;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet replica", KR(ret), K(replica));
  } else if (OB_FAIL(replicas.push_back(replica))) {
    LOG_WARN("fail to push back replcia", KR(ret), K(replica));
  } else if (OB_FAIL(batch_update(replica.get_tenant_id(), replicas))) {
    LOG_WARN("fail to batch update", KR(ret), K(replicas));
  }
  return ret;
}

int ObTabletTableOperator::batch_update(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplica> &replicas)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    common::ObMySQLTransaction trans;
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(trans.start(sql_proxy_, sql_tenant_id))) {
      LOG_WARN("start transaction failed", KR(ret), K(sql_tenant_id));
    } else if (OB_FAIL(batch_update(trans, tenant_id, replicas))) {
      LOG_WARN("fail to batch update", KR(ret), K(tenant_id));
    }

    if (trans.is_started()) {
      int trans_ret = trans.end(OB_SUCCESS == ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("end transaction failed", KR(trans_ret));
        ret = OB_SUCCESS == ret ? trans_ret : ret;
      }
    }
  }
  return ret;
}

int ObTabletTableOperator::batch_update(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplica> &replicas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || replicas.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "replicas count", replicas.count());
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(batch_size_, replicas.count());
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      if (OB_FAIL(inner_batch_update_by_sql_(tenant_id, replicas, start_idx, end_idx, sql_client))) {
        LOG_WARN("fail to inner batch update", KR(ret), K(tenant_id), K(replicas), K(start_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + batch_size_, replicas.count());
      }
    }
  }
  return ret;
}

int ObTabletTableOperator::inner_batch_update_by_sql_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplica> &replicas,
    const int64_t start_idx,
    const int64_t end_idx,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || replicas.count() <= 0
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > replicas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(tenant_id), "replicas count", replicas.count(), K(start_idx), K(end_idx));
  } else {
    int64_t affected_rows = 0;
    ObSqlString sql;
    ObDMLSqlSplicer dml;
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObTabletReplica &replica = replicas.at(idx);
      if (OB_UNLIKELY(!replica.is_valid() || tenant_id != replica.get_tenant_id())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid replica", KR(ret), K(tenant_id), K(replica));
      } else if (OB_FAIL(fill_dml_splicer_(replica, dml))) {
        LOG_WARN("fail to fill dml splicer", KR(ret), K(replica));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(replica));
      }
    }
    if (FAILEDx(dml.splice_batch_insert_update_sql(OB_ALL_TABLET_META_TABLE_TNAME, sql))) {
      LOG_WARN("fail to splice batch insert update sql", KR(ret), K(sql));
    } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql.ptr(), group_id_, affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObTabletTableOperator::fill_dml_splicer_(
    const ObTabletReplica &replica,
    ObDMLSqlSplicer &dml_splicer)
{
  int ret = OB_SUCCESS;
  const uint64_t snapshot_version = replica.get_snapshot_version() < 0 ? 0 : replica.get_snapshot_version();
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(!replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", KR(ret), K(replica));
  } else if (OB_UNLIKELY(!replica.get_server().ip_to_string(ip, sizeof(ip)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("convert server ip to string failed", KR(ret), "server", replica.get_server());
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", replica.get_tenant_id()))
      || OB_FAIL(dml_splicer.add_pk_column("tablet_id", replica.get_tablet_id().id()))
      || OB_FAIL(dml_splicer.add_pk_column("svr_ip", ip))
      || OB_FAIL(dml_splicer.add_pk_column("svr_port", replica.get_server().get_port()))
      || OB_FAIL(dml_splicer.add_pk_column("ls_id", replica.get_ls_id().id()))
      || OB_FAIL(dml_splicer.add_uint64_column("compaction_scn", snapshot_version))
      || OB_FAIL(dml_splicer.add_column("data_size", replica.get_data_size()))
      || OB_FAIL(dml_splicer.add_column("required_size", replica.get_required_size()))) {
    LOG_WARN("add column failed", KR(ret), K(replica));
  }
  return ret;
}

int ObTabletTableOperator::range_get(
    const uint64_t tenant_id,
    const common::ObTabletID &start_tablet_id,
    const int64_t range_size,
    ObIArray<ObTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  tablet_infos.reset();
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(
      !is_valid_tenant_id(tenant_id)
      || range_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(start_tablet_id), K(range_size));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
      ObSqlString sql;
      if (OB_FAIL(sql.append_fmt(
          "SELECT * FROM %s WHERE tenant_id = %lu and tablet_id > %lu LIMIT %ld",
          OB_ALL_TABLET_META_TABLE_TNAME,
          tenant_id,
          start_tablet_id.id(),
          range_size))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_proxy_->read(result, sql_tenant_id, sql.ptr(), group_id_))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_tablet_infos(*result.get_result(), tablet_infos))) {
        LOG_WARN("construct tablet info failed", KR(ret), K(sql), K(tablet_infos));
      } else if (OB_UNLIKELY(tablet_infos.count() > range_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get too much tablets", KR(ret), K(sql),
            K(range_size), "tablet_infos count", tablet_infos.count());
      }
    }
  }
  return ret;
}

int ObTabletTableOperator::batch_remove(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplica> &replicas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    common::ObMySQLTransaction trans;
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(trans.start(sql_proxy_, sql_tenant_id))) {
      LOG_WARN("start transaction failed", KR(ret), K(sql_tenant_id));
    } else if (OB_FAIL(batch_remove(trans, tenant_id, replicas))) {
      LOG_WARN("fail to batch remove", KR(ret));
    }

    if (trans.is_started()) {
      int trans_ret = trans.end(OB_SUCCESS == ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("end transaction failed", KR(trans_ret));
        ret = OB_SUCCESS == ret ? trans_ret : ret;
      }
    }
  }
  return ret;
}

int ObTabletTableOperator::batch_remove(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplica> &replicas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || replicas.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "replicas count", replicas.count());
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(batch_size_, replicas.count());
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      if (OB_FAIL(inner_batch_remove_by_sql_(tenant_id, replicas, start_idx, end_idx, sql_client))) {
        LOG_WARN("fail to inner batch remove", KR(ret), K(tenant_id), K(replicas), K(start_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + batch_size_, replicas.count());
      }
    }
  }
  return ret;
}

int ObTabletTableOperator::inner_batch_remove_by_sql_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletReplica> &replicas,
    const int64_t start_idx,
    const int64_t end_idx,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || replicas.count() <= 0
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > replicas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(tenant_id), "replicas count", replicas.count(), K(start_idx), K(end_idx));
  } else {
    int64_t affected_rows = 0;
    ObSqlString sql;
    ObDMLSqlSplicer dml;
    const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObTabletReplica &replica = replicas.at(idx);
      if (OB_UNLIKELY(!replica.primary_keys_are_valid() || tenant_id != replica.get_tenant_id())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid replica", KR(ret), K(tenant_id), K(replica));
      } else if (OB_FAIL(fill_remove_dml_splicer_(replica, dml))) {
        LOG_WARN("fail to fill dml splicer", KR(ret), K(replica));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(replica));
      }
    }
    if (FAILEDx(dml.splice_batch_delete_sql(OB_ALL_TABLET_META_TABLE_TNAME, sql))) {
      LOG_WARN("fail to splice batch delete sql", KR(ret), K(sql));
    } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql.ptr(), group_id_, affected_rows))) {
      LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql_tenant_id), K(sql));
    }
  }
  return ret;
}

// remove tablet replica by primary key
int ObTabletTableOperator::fill_remove_dml_splicer_(
    const ObTabletReplica &replica,
    ObDMLSqlSplicer &dml_splicer)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(!replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", KR(ret), K(replica));
  } else if (OB_UNLIKELY(!replica.get_server().ip_to_string(ip, sizeof(ip)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("convert server ip to string failed", KR(ret), "server", replica.get_server());
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", replica.get_tenant_id()))
      || OB_FAIL(dml_splicer.add_pk_column("tablet_id", replica.get_tablet_id().id()))
      || OB_FAIL(dml_splicer.add_pk_column("svr_ip", ip))
      || OB_FAIL(dml_splicer.add_pk_column("svr_port", replica.get_server().get_port()))
      || OB_FAIL(dml_splicer.add_pk_column("ls_id", replica.get_ls_id().id()))) {
    LOG_WARN("add column failed", KR(ret), K(replica));
  }
  return ret;
}

int ObTabletTableOperator::remove_residual_tablet(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObAddr &server,
    const int64_t limit,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObSqlString sql;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(
      !is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id)
      || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(server));
  } else if (OB_UNLIKELY(!server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", KR(ret), K(server));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE FROM %s WHERE tenant_id = %lu AND svr_ip = '%s' AND svr_port = %d limit %ld",
      OB_ALL_TABLET_META_TABLE_TNAME,
      tenant_id,
      ip,
      server.get_port(),
      limit))) {
    LOG_WARN("assign sql string failed", KR(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql.ptr(), group_id_, affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql), K(sql_tenant_id));
  } else if (affected_rows > 0) {
    LOG_INFO("finish to remove residual tablet", KR(ret), K(tenant_id), K(affected_rows));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
