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
#include "share/location_cache/ob_location_service.h"

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
      sqlclient::ObMySQLResult *result = NULL;
      const uint64_t zero_tenant_id = 0;
      if (OB_FAIL(sql.assign_fmt(
        "select * from %s where tenant_id = %lu and src_tablet_id = %ld",
        OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, zero_tenant_id, tablet_id.id()))) {
        LOG_WARN("failed to assign sql", K(ret), K(sql), K(tablet_id));
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
      const uint64_t zero_tenant_id = 0;
      if (OB_FAIL(sql_string.assign_fmt("SELECT src_tablet_id, dest_tablet_id FROM %s WHERE "
              " tenant_id = %lu and ls_id = %ld", OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, zero_tenant_id, ls_id.id()))) {
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
      const uint64_t zero_tenant_id = 0;
      if (OB_FAIL(sql_string.assign_fmt("SELECT src_tablet_id, dest_tablet_id FROM %s WHERE "
              " tenant_id = %lu and ls_id = %ld and src_tablet_id = %ld", OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, zero_tenant_id, ls_id.id(), tablet_id.id()))) {
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
      sqlclient::ObMySQLResult *result = NULL;
      const uint64_t zero_tenant_id = 0;
      if (OB_FAIL(sql.assign_fmt(
        "select * from %s where tenant_id = %lu and dest_tablet_id = %ld",
        OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME, zero_tenant_id, tablet_id.id()))) {
        LOG_WARN("failed to assign sql", K(ret), K(sql), K(tablet_id));
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

int ObTabletReorganizeHistoryTableOperator::insert_(
    ObISQLClient &sql_proxy,
    const ObTabletReorganizeRecord &incomplete_record,
    const ObIArray<ObTabletID> &dest_tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dest_tablet_ids.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_tablet_ids));
  } else {
    common::ObSqlString insert_sql;
    ObTabletReorganizeRecord complete_record(incomplete_record.tenant_id_,
                                             incomplete_record.ls_id_,
                                             incomplete_record.src_tablet_id_,
                                             incomplete_record.dest_tablet_id_,
                                             incomplete_record.type_,
                                             incomplete_record.create_time_,
                                             incomplete_record.finish_time_);
    if (OB_FAIL(insert_sql.assign_fmt("INSERT INTO %s (tenant_id, ls_id, src_tablet_id, dest_tablet_id, "
        "type, create_time, finish_time) VALUES ", OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
    int64_t affected_rows = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablet_ids.count(); ++i) {
      complete_record.dest_tablet_id_ = dest_tablet_ids.at(i);
      if (OB_FAIL(insert_sql.append_fmt("(%lu, %ld, %ld, %ld, %ld, usec_to_time(%lu), usec_to_time(%lu))",
          ObSchemaUtils::get_extract_tenant_id(complete_record.tenant_id_, complete_record.tenant_id_), complete_record.ls_id_.id(), complete_record.src_tablet_id_.id(),
          complete_record.dest_tablet_id_.id(), int64_t(complete_record.type_), complete_record.create_time_, complete_record.finish_time_))) {
        LOG_WARN("failed to append fmt", K(ret), K(complete_record));
      } else if ((i%ObDDLUtil::MAX_BATCH_COUNT) == ObDDLUtil::MAX_BATCH_COUNT - 1 || i == dest_tablet_ids.count() - 1) {
        if (OB_FAIL(insert_sql.append_fmt("ON DUPLICATE KEY UPDATE finish_time = usec_to_time(VALUES(finish_time));"))) {
          LOG_WARN("failed to append insert_sql", K(ret), K(insert_sql));
        } else if (OB_FAIL(sql_proxy.write(complete_record.tenant_id_, insert_sql.ptr(), affected_rows))) {
          LOG_WARN("failed to write sql", K(ret), K(insert_sql));
        } else {
          LOG_INFO("insert tablet reorganize history table success", K(ret), K(affected_rows), K(complete_record.tenant_id_));
          insert_sql.reuse();
          if (OB_FAIL(insert_sql.assign_fmt("INSERT INTO %s (tenant_id, ls_id, src_tablet_id, dest_tablet_id, "
              "type, create_time, finish_time) VALUES ", OB_ALL_TABLET_REORGANIZE_HISTORY_TNAME))) {
            LOG_WARN("failed to assign fmt", K(ret));
          }
        }
      } else if (OB_FAIL(insert_sql.append(", "))) {
        LOG_WARN("failed to append insert_sql", K(ret), K(insert_sql));
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
  ObSEArray<ObTabletID, 1> dest_tablet_id;
  if (OB_FAIL(dest_tablet_id.push_back(record.dest_tablet_id_))) {
    LOG_WARN("failed to push back into dest tablet id", K(ret));
  } else if (OB_FAIL(insert_(sql_proxy, record, dest_tablet_id))) {
    LOG_WARN("failed to do insert insert_", K(ret), K(record));
  }
  return ret;
}

int ObTabletReorganizeHistoryTableOperator::batch_insert(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const obrpc::ObPartitionSplitArg &split_arg,
    const int64 start_time,
    const int64 finish_time)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(&sql_proxy, tenant_id))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(inner_batch_insert_(trans, tenant_id, split_arg.src_tablet_id_, split_arg.dest_tablet_ids_, start_time, finish_time))) {
      LOG_WARN("failed to inner batch insert", K(ret));
    } else if (split_arg.src_local_index_tablet_ids_.count() != split_arg.dest_local_index_tablet_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("split arg src local index count not equal to dest", K(ret), K(split_arg));
    } else {
      ARRAY_FOREACH_X(split_arg.dest_local_index_tablet_ids_, idx, cnt, OB_SUCC(ret)) {
        const ObTabletID &src_tablet_id = split_arg.src_local_index_tablet_ids_.at(idx);
        const ObSArray<ObTabletID> &dest_tablet_ids = split_arg.dest_local_index_tablet_ids_.at(idx);
        if (OB_FAIL(inner_batch_insert_(trans, tenant_id, src_tablet_id, dest_tablet_ids, start_time, finish_time))) {
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
        if (OB_FAIL(inner_batch_insert_(trans, tenant_id, src_tablet_id, dest_tablet_ids, start_time, finish_time))) {
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
    const ObSArray<ObTabletID> &dest_tablet_ids,
    const int64 start_time,
    const int64 finish_time)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletReorganizeRecord> records;
  share::ObLSID ls_id;
  ObAddr leader_addr;
  ObLocationService *location_service = nullptr;
  const int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
  // it's guranteed that all dest_tablets_ids are located at the same server with src
  // so all the records can share one single record instance
  if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("location_cache is null", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(location_service,
            tenant_id, tablet_id, rpc_timeout, ls_id, leader_addr))) {
    LOG_WARN("get tablet leader addr failed", K(ret));
  }
  ObTabletID invalid_des_tablet_id;
  ObTabletReorganizeRecord incomplete_record = ObTabletReorganizeRecord(tenant_id,
                                                                        ls_id,
                                                                        tablet_id,
                                                                        invalid_des_tablet_id,
                                                                        ObTabletReorganizeType::SPLIT,
                                                                        start_time/*create_time*/,
                                                                        finish_time/*finish_time*/);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(insert_(sql_proxy, incomplete_record, dest_tablet_ids))) {
    LOG_WARN("failed to insert record", K(ret), K(records));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
