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
#include "share/ob_tablet_reorganize_history_table_operator.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_server_struct.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{

int ObTabletReorganizeHistoryTableOperator::check_tablet_has_reorganized(
    common::ObMySQLProxy &proxy,
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    share::ObLSID &ls_id,
    bool &reorganized)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  reorganized = false;
  ObSqlString sql;

  if (OB_INVALID_TENANT_ID == tenant_id || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablet_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt(
        "select * from %s where tenant_id = %lu and src_tablet_id = %ld",
        OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, tenant_id, tablet_id.id()))) {
        LOG_WARN("failed to assign sql", K(ret), K(sql), K(tenant_id), K(tablet_id));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          reorganized = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next row", K(ret), K(sql));
        }
      } else {
        int64_t tmp_ls_id = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", tmp_ls_id, int64_t);
        ls_id = ObLSID(tmp_ls_id);
        reorganized = true;
      }
    }
  }

  return ret;
}

int ObTabletReorganizeHistoryTableOperator::get_all_split_tablet_pairs(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObIArray<ReorganizeTabletPair> &tablet_pairs)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql_string;
  // clear tablet pairs array
  tablet_pairs.reuse();
  if (tenant_id == common::OB_INVALID_TENANT_ID || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_string.assign_fmt("SELECT src_tablet_id, dest_tablet_id FROM %s WHERE tenant_id = %ld "
              "AND ls_id = %ld", OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, tenant_id, ls_id.id()))) {
        LOG_WARN("assign sql string failed", K(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("read tablet ids from all tablet reorganize history table failed", K(ret), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next row", K(ret));
            }
          } else {
            int64_t src_tablet_id = common::ObTabletID::INVALID_TABLET_ID;
            int64_t dest_tablet_id = common::ObTabletID::INVALID_TABLET_ID;
            EXTRACT_INT_FIELD_MYSQL(*result, "src_tablet_id", src_tablet_id, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "dest_tablet_id", dest_tablet_id, int64_t);
            ReorganizeTabletPair tablet_pair(src_tablet_id, dest_tablet_id);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(tablet_pairs.push_back(tablet_pair))) {
              LOG_WARN("failed to push back tablet pair", K(ret), K(src_tablet_id), K(dest_tablet_id));
            }
          }
        } // while
      }
    } // smart var
  }
  return ret;
}

int ObTabletReorganizeHistoryTableOperator::get_split_tablet_pairs_by_src(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObIArray<ReorganizeTabletPair> &tablet_pairs)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql_string;
  // clear tablet pairs array
  tablet_pairs.reuse();
  if (tenant_id == common::OB_INVALID_TENANT_ID || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_string.assign_fmt("SELECT src_tablet_id, dest_tablet_id FROM %s WHERE tenant_id = %ld "
              "AND ls_id = %ld and src_tablet_id = %ld", OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, tenant_id, ls_id.id(), tablet_id.id()))) {
        LOG_WARN("assign sql string failed", K(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("read tablet ids from all tablet reorganize history table failed", K(ret), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next row", K(ret));
            }
          } else {
            int64_t src_tablet_id = common::ObTabletID::INVALID_TABLET_ID;
            int64_t dest_tablet_id = common::ObTabletID::INVALID_TABLET_ID;
            EXTRACT_INT_FIELD_MYSQL(*result, "src_tablet_id", src_tablet_id, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "dest_tablet_id", dest_tablet_id, int64_t);
            ReorganizeTabletPair tablet_pair(src_tablet_id, dest_tablet_id);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(tablet_pairs.push_back(tablet_pair))) {
              LOG_WARN("failed to push back tablet pair", K(ret), K(src_tablet_id), K(dest_tablet_id));
            }
          }
        } // while
      }
    } // smart var
  }
  return ret;
}

int ObTabletReorganizeHistoryTableOperator::get_split_tablet_pairs_by_dest(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    ReorganizeTabletPair &tablet_pair)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_INVALID_TENANT_ID == tenant_id || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablet_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt(
        "select * from %s where tenant_id = %lu and dest_tablet_id = %ld",
        OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, tenant_id, tablet_id.id()))) {
        LOG_WARN("failed to assign sql", K(ret), K(sql), K(tenant_id), K(tablet_id));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("failed to get next row", K(ret), K(sql));
        }
      } else {
        int64_t src_tablet_id = common::ObTabletID::INVALID_TABLET_ID;
        int64_t dest_tablet_id = common::ObTabletID::INVALID_TABLET_ID;
        EXTRACT_INT_FIELD_MYSQL(*result, "src_tablet_id", src_tablet_id, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "dest_tablet_id", dest_tablet_id, int64_t);
        tablet_pair = ReorganizeTabletPair(src_tablet_id, dest_tablet_id);
      }
    }
  }
  return ret;
}

int ObTabletReorganizeHistoryTableOperator::insert(
    ObISQLClient &sql_proxy,
    const ObTabletReorganizeRecord &record)
{
  int ret = OB_SUCCESS;
  common::ObSqlString insert_sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(insert_sql.assign_fmt("INSERT INTO %s (tenant_id, ls_id, src_tablet_id, dest_tablet_id, "
          "type, create_time, finish_time) VALUES (%lu, %ld, %ld, %ld, %ld, usec_to_time(%lu), usec_to_time(%lu))"
          " ON DUPLICATE KEY UPDATE finish_time = usec_to_time(%lu)",
          OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, record.tenant_id_, record.ls_id_.id(), record.src_tablet_id_.id(),
          record.dest_tablet_id_.id(), int64_t(record.type_), record.create_time_, record.finish_time_, record.finish_time_))) {
    LOG_WARN("failed to assign fmt", K(ret), K(record.tenant_id_), K(record.ls_id_), K(record.src_tablet_id_),
        K(record.dest_tablet_id_), K(record.type_), K(record.create_time_), K(record.finish_time_));
  } else if (OB_FAIL(sql_proxy.write(record.tenant_id_, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(insert_sql));
  } else {
    LOG_INFO("insert tabler reorganize history table success", K(ret), K(record.tenant_id_), K(record.ls_id_),
        K(record.src_tablet_id_), K(record.dest_tablet_id_), K(record.type_), K(record.create_time_),
        K(record.finish_time_));
  }
  return ret;
}

int ObTabletReorganizeHistoryTableOperator::batch_insert(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const obrpc::ObPartitionSplitArg &split_arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(&sql_proxy, tenant_id))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(inner_batch_insert_(trans, tenant_id, split_arg.src_tablet_id_, split_arg.dest_tablet_ids_))) {
      LOG_WARN("failed to inner batch insert", K(ret));
    } else if (split_arg.src_local_index_tablet_ids_.count() != split_arg.dest_local_index_tablet_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("split arg src local index count not equal to dest", K(ret), K(split_arg));
    } else {
      ARRAY_FOREACH_X(split_arg.dest_local_index_tablet_ids_, idx, cnt, OB_SUCC(ret)) {
        const ObTabletID &src_tablet_id = split_arg.src_local_index_tablet_ids_.at(idx);
        const ObSArray<ObTabletID> &dest_tablet_ids = split_arg.dest_local_index_tablet_ids_.at(idx);
        if (OB_FAIL(inner_batch_insert_(trans, tenant_id, src_tablet_id, dest_tablet_ids))) {
          LOG_WARN("failed to inner batch insert", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (split_arg.src_lob_tablet_ids_.count() != split_arg.dest_lob_tablet_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("split arg src local index count not equal to dest", K(ret), K(split_arg));
    } else {
      ARRAY_FOREACH_X(split_arg.dest_lob_tablet_ids_, idx, cnt, OB_SUCC(ret)) {
        const ObTabletID &src_tablet_id = split_arg.src_lob_tablet_ids_.at(idx);
        const ObSArray<ObTabletID> &dest_tablet_ids = split_arg.dest_lob_tablet_ids_.at(idx);
        if (OB_FAIL(inner_batch_insert_(trans, tenant_id, src_tablet_id, dest_tablet_ids))) {
          LOG_WARN("failed to inner batch insert", K(ret));
        }
      }
    }
  }
  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObTabletReorganizeHistoryTableOperator::inner_batch_insert_(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const ObSArray<ObTabletID> &dest_tablet_ids)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(dest_tablet_ids, idx, cnt, OB_SUCC(ret)) {
    const ObTabletID &src_tablet_id = tablet_id;
    const ObTabletID &dest_tablet_id = dest_tablet_ids.at(idx);
    ObLocationService *location_service = nullptr;
    share::ObLSID ls_id;
    ObAddr leader_addr;
    const int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
    if (OB_ISNULL(location_service = GCTX.location_service_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("location_cache is null", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(location_service,
            tenant_id, tablet_id, rpc_timeout, ls_id, leader_addr))) {
      LOG_WARN("get tablet leader addr failed", K(ret));
    } else {
      ObTabletReorganizeRecord record = ObTabletReorganizeRecord(tenant_id,
                                                                 ls_id,
                                                                 src_tablet_id,
                                                                 dest_tablet_id,
                                                                 ObTabletReorganizeType::SPLIT,
                                                                 0/*create_time*/,
                                                                 0/*finish_time*/);
      // TODO(yanfeng): do real batch later
      if (OB_FAIL(insert(sql_proxy, record))) {
        LOG_WARN("failed to insert record", K(ret), K(record));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
