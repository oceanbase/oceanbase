/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_zone_merge_table_operator.h"

#include "share/ob_zone_merge_info.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

int ObZoneMergeTableOperator::load_zone_merge_infos(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<ObZoneMergeInfo> &infos,
    const bool print_sql)
{
  return inner_load_zone_merge_infos_(sql_client, tenant_id, infos, print_sql);
}

int ObZoneMergeTableOperator::insert_zone_merge_infos(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObIArray<ObZoneMergeInfo> &infos)
{
  return inner_insert_zone_merge_infos_(sql_client, tenant_id, InsertMode::INSERT, infos);
}

int ObZoneMergeTableOperator::update_tenant_all_zone_merge_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObZoneMergeInfo &info)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_id != info.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(info));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, meta_tenant_id);

    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
      LOG_WARN("fail to add pk column", KR(ret), K(tenant_id), K(info));
    } else {
      bool need_update = false;
      const ObMergeInfoItem *it = info.list_.get_first();
      while (OB_SUCC(ret) && (it != info.list_.get_header())) {
        if (NULL == it) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null item", KR(ret), KP(it), K(tenant_id), K(info));
        } else {
          if (it->need_update_) {
            if (it->is_scn_) {
              if (OB_FAIL(dml.add_uint64_column(it->name_, it->get_scn_val()))) {
                LOG_WARN("fail to add scn column", KR(ret), K(tenant_id), K(info), K(*it));
              } else if (0 == STRCMP(it->name_, "all_merged_scn")) {
                // do not add extra condition for all_merged_scn
                //
              } else if (dml.get_extra_condition().empty()) {
                if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s < %ld", it->name_, it->get_scn_val()))) {
                  LOG_WARN("fail to assign extra_condition", KR(ret), K(tenant_id));
                }
              } else {
                if (OB_FAIL(dml.get_extra_condition().append_fmt(" AND %s < %ld", it->name_, it->get_scn_val()))) {
                  LOG_WARN("fail to append extra_condition", KR(ret), K(tenant_id));
                }
              }
            } else {
              if (OB_FAIL(dml.add_uint64_column(it->name_, it->value_))) {
                LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(info), K(*it));
              }
            }
            need_update = true;
          }
          it = it->get_next();
        }
      }

      if (need_update) {
        if (FAILEDx(exec.exec_update(OB_ALL_ZONE_MERGE_INFO_TNAME, dml, affected_rows))) {
          LOG_WARN("fail to exec_update zone_merge_info", KR(ret), K(meta_tenant_id), K(info));
        } else if (is_zero_row(affected_rows)) {
          LOG_WARN("no row updated, maybe because of scn revert, check it", KR(ret), K(tenant_id), K(info));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("actual no need to update zone merge info", KR(ret), K(tenant_id), K(info));
      }
    }
  }
  return ret;
}

int ObZoneMergeTableOperator::sync_zone_merge_info_with_zone_list(
    ObMySQLTransaction &trans, const uint64_t tenant_id,
    const ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(delete_zone_merge_info_not_in_zones(trans, tenant_id, zone_list))) {
    LOG_WARN("fail to delete zone merge info not in zones", KR(ret), K(tenant_id), K(zone_list));
  } else if (OB_FAIL(insert_ignore_zone_merge_infos(trans, tenant_id, zone_list))) {
    LOG_WARN("fail to insert ignore zone merge infos", KR(ret), K(tenant_id), K(zone_list));
  }
  return ret;
}

int ObZoneMergeTableOperator::delete_zone_merge_info_not_in_zones(
    ObISQLClient &sql_client, const uint64_t tenant_id,
    const ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t zone_cnt = zone_list.count();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (zone_cnt < 1) {
    // zone_list is empty, delete all rows for this tenant
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu",
        OB_ALL_ZONE_MERGE_INFO_TNAME, tenant_id))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
    }
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND zone NOT IN(",
      OB_ALL_ZONE_MERGE_INFO_TNAME, tenant_id))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < zone_cnt && OB_SUCC(ret); ++i) {
      if (OB_FAIL(sql.append_fmt("'%s'%s", zone_list.at(i).ptr(),
          (i == zone_cnt - 1) ? ")" : ", "))) {
        LOG_WARN("fail to append sql", KR(ret), K(i), "zone", zone_list.at(i));
      }
    }
  }

  if (FAILEDx(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
  } else {
    LOG_INFO("succ to delete zone_merge_info not in zone_list", K(tenant_id), K(sql), K(affected_rows));
  }
  return ret;
}

int ObZoneMergeTableOperator::insert_ignore_zone_merge_infos(
    ObISQLClient &sql_client, const uint64_t tenant_id,
    const ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  const int64_t zone_cnt = zone_list.count();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (zone_cnt > 0) {
    ObSEArray<ObZoneMergeInfo, 5> to_insert_infos;
    for (int64_t i = 0; i < zone_cnt && OB_SUCC(ret); ++i) {
      ObZoneMergeInfo tmp_info;
      tmp_info.tenant_id_ = tenant_id;
      tmp_info.zone_ = zone_list.at(i);
      if (OB_FAIL(to_insert_infos.push_back(tmp_info))) {
        LOG_WARN("fail to push back", KR(ret), K(tenant_id), K(tmp_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(inner_insert_zone_merge_infos_(
              sql_client, tenant_id, InsertMode::INSERT_IGNORE, to_insert_infos))) {
        LOG_WARN("fail to insert ignore zone merge infos", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObZoneMergeTableOperator::inner_insert_zone_merge_infos_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const InsertMode mode,
    const ObIArray<ObZoneMergeInfo> &infos)
{
  int ret = OB_SUCCESS;

  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  const int64_t info_cnt = infos.count();
  if (OB_UNLIKELY(info_cnt < 1 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(info_cnt));
  } else {
    for (int64_t i = 0; (i < info_cnt) && OB_SUCC(ret); ++i) {
      if (!infos.at(i).is_valid() || infos.at(i).tenant_id_ != tenant_id) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(i), K(tenant_id), "merge_info", infos.at(i));
      }
    }
  }

  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && (i < info_cnt); ++i) {
    const ObZoneMergeInfo &cur_info = infos.at(i);
    const uint64_t all_merged_scn_val = cur_info.all_merged_scn_.get_scn_val();
    const uint64_t broadcast_scn_val = cur_info.broadcast_scn_.get_scn_val();
    const uint64_t frozen_scn_val = cur_info.frozen_scn_.get_scn_val();
    const uint64_t last_merged_scn_val = cur_info.last_merged_scn_.get_scn_val();
    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
        || OB_FAIL(dml.add_pk_column("zone", cur_info.zone_.ptr()))
        || OB_FAIL(dml.add_uint64_column("all_merged_scn", all_merged_scn_val))
        || OB_FAIL(dml.add_uint64_column("broadcast_scn", broadcast_scn_val))
        || OB_FAIL(dml.add_uint64_column("frozen_scn", frozen_scn_val))
        || OB_FAIL(dml.add_uint64_column("is_merging", cur_info.is_merging_.value_))
        || OB_FAIL(dml.add_uint64_column("last_merged_time", cur_info.last_merged_time_.value_))
        || OB_FAIL(dml.add_uint64_column("last_merged_scn", last_merged_scn_val))
        || OB_FAIL(dml.add_uint64_column("merge_start_time", cur_info.merge_start_time_.value_))
        || OB_FAIL(dml.add_uint64_column("merge_status", cur_info.merge_status_.value_))) {
      LOG_WARN("fail to add column", KR(ret), K(cur_info));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("fail to finish row", KR(ret), K(i), K(cur_info));
    }
  }

  if (InsertMode::REPLACE == mode) {
    if (FAILEDx(dml.splice_batch_replace_sql(OB_ALL_ZONE_MERGE_INFO_TNAME, sql))) {
      LOG_WARN("fail to splice batch replace sql", KR(ret), K(sql));
    }
  } else if (InsertMode::INSERT_IGNORE == mode) {
    if (FAILEDx(dml.splice_batch_insert_ignore_sql(OB_ALL_ZONE_MERGE_INFO_TNAME, sql))) {
      LOG_WARN("fail to splice insert ignore sql", KR(ret), K(sql));
    }
  } else {
    if (FAILEDx(dml.splice_batch_insert_sql(OB_ALL_ZONE_MERGE_INFO_TNAME, sql))) {
      LOG_WARN("fail to splice batch insert sql", KR(ret), K(sql));
    }
  }
  if (FAILEDx(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
  }
  
  return ret;
}

int ObZoneMergeTableOperator::inner_load_zone_merge_infos_(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<ObZoneMergeInfo> &infos,
    const bool print_sql)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      const int64_t info_cnt = infos.count();
      if (info_cnt > 0) {
        if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE (tenant_id, zone) IN (", 
            OB_ALL_ZONE_MERGE_INFO_TNAME))) {
          LOG_WARN("fail to assign sql", KR(ret), K(info_cnt));
        } else {
          for (int64_t i = 0; (i < info_cnt) && OB_SUCC(ret); ++i) {
            const ObZoneMergeInfo &cur_info = infos.at(i);
            if (OB_UNLIKELY(cur_info.tenant_id_ != tenant_id)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(cur_info));
            } else if (OB_FAIL(sql.append_fmt("('%lu', '%s')%s", tenant_id, cur_info.zone_.ptr(),
                ((i == info_cnt - 1) ? ")" : ", ")))) {
              LOG_WARN("fail to assign sql", KR(ret), K(i), K(tenant_id), K(cur_info));
            }
          }
        }
      } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = '%lu'", 
                 OB_ALL_ZONE_MERGE_INFO_TNAME, tenant_id))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
      }

      const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
      if (FAILEDx(sql_client.read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
      } else {
        const bool need_check = (info_cnt > 0) ? true : false;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", KR(ret), K(tenant_id), K(sql));
            }
          } else if (OB_FAIL(construct_zone_merge_info_(*result, need_check, infos))) {
            LOG_WARN("fail to construct zone merge info", KR(ret), K(tenant_id), K(need_check));
          } 
        } // end while loop

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (print_sql) {
      LOG_INFO("finish load_zone_merge_info", KR(ret), K(tenant_id), K(sql));
    }
  }
  return ret;
}

int ObZoneMergeTableOperator::construct_zone_merge_info_(
    sqlclient::ObMySQLResult &result,
    const bool need_check,
    ObIArray<ObZoneMergeInfo> &infos)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  int64_t tmp_real_str_len = 0; // only used for output parameter
  int64_t tenant_id = 0;
  char zone_buf[MAX_ZONE_LENGTH] = "";
  ObZoneMergeInfo tmp_merge_info;
  uint64_t all_merged_scn = 0;
  uint64_t broadcast_scn = 0;
  uint64_t frozen_scn = 0;
  uint64_t last_merged_scn = 0;

  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", tmp_merge_info.tenant_id_, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, "all_merged_scn", all_merged_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, "broadcast_scn", broadcast_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, "frozen_scn", frozen_scn, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_merging", tmp_merge_info.is_merging_.value_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "last_merged_time", tmp_merge_info.last_merged_time_.value_, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, "last_merged_scn", last_merged_scn, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "merge_start_time", tmp_merge_info.merge_start_time_.value_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "merge_status", tmp_merge_info.merge_status_.value_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, "zone", zone_buf, static_cast<int64_t>(sizeof(zone_buf)), tmp_real_str_len);

  if (FAILEDx(tmp_merge_info.all_merged_scn_.set_scn(all_merged_scn))) {
    LOG_WARN("fail to set scn val", KR(ret), K(all_merged_scn));
  } else if (OB_FAIL(tmp_merge_info.broadcast_scn_.set_scn(broadcast_scn))) {
    LOG_WARN("fail to set scn val", KR(ret), K(broadcast_scn));
  } else if (OB_FAIL(tmp_merge_info.frozen_scn_.set_scn(frozen_scn))) {
    LOG_WARN("fail to set scn val", KR(ret), K(frozen_scn));
  } else if (OB_FAIL(tmp_merge_info.last_merged_scn_.set_scn(last_merged_scn))) {
    LOG_WARN("fail to set scn val", KR(ret), K(last_merged_scn));
  }

  if (OB_SUCC(ret)) {
    tmp_merge_info.zone_ = zone_buf;

    if (need_check) {
      for (int64_t i = 0; (i < infos.count()) && OB_SUCC(ret) && !exist; ++i) {
        ObZoneMergeInfo &cur_info = infos.at(i);
        if ((cur_info.tenant_id_ == tmp_merge_info.tenant_id_) &&
            strncasecmp(cur_info.zone_.ptr(), tmp_merge_info.zone_.ptr(), OB_MAX_TZ_NAME_LEN) == 0) {
          if (OB_FAIL(cur_info.assign_value(tmp_merge_info))) {
            LOG_WARN("fail to assign value of zone merge info", KR(ret), K(tmp_merge_info), K(cur_info));
          } else {
            exist = true;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!exist) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("fail to find the zone merge info", KR(ret), K(tmp_merge_info));
      }
    } else {
      if (OB_FAIL(infos.push_back(tmp_merge_info))) {
        LOG_WARN("fail to push back", KR(ret), K(tmp_merge_info));
      }
    }
  }
  
  return ret;
}

} // end namespace share
} // end namespace oceanbase
