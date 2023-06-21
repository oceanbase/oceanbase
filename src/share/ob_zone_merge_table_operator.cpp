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

#include "share/ob_zone_merge_table_operator.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_zone_merge_info.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

int ObZoneMergeTableOperator::load_zone_merge_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObZoneMergeInfo &info,
    const bool print_sql)
{
  int ret = OB_SUCCESS;
  ObArray<ObZoneMergeInfo> infos;
  if (OB_FAIL(infos.push_back(info))) {
    LOG_WARN("fail to push back zone merge info", KR(ret), K(info));
  } else if (OB_FAIL(load_zone_merge_infos(sql_client, tenant_id, infos, print_sql))) {
    LOG_WARN("fail to load zone merge infos", KR(ret), K(info));
  } else if (OB_UNLIKELY(infos.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err about info count", KR(ret), K(tenant_id), K(info));
  } else if (OB_FAIL(info.assign_value(infos.at(0)))) {
    LOG_WARN("fail to assign zone merge info value", KR(ret), K(info));
  }
  return ret;
}

int ObZoneMergeTableOperator::load_zone_merge_infos(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<ObZoneMergeInfo> &infos,
    const bool print_sql)
{
  return inner_load_zone_merge_infos_(sql_client, tenant_id, infos, print_sql);
}

int ObZoneMergeTableOperator::insert_zone_merge_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObZoneMergeInfo &info)
{
  int ret = OB_SUCCESS;
  ObArray<ObZoneMergeInfo> infos;
  if (OB_FAIL(infos.push_back(info))) {
    LOG_WARN("fail to push back zone merge info", KR(ret), K(tenant_id), K(info));
  } else if (OB_FAIL(insert_zone_merge_infos(sql_client, tenant_id, infos))) {
    LOG_WARN("fail to insert zone merge infos", KR(ret), K(tenant_id), K(info));
  }
  return ret;
}

int ObZoneMergeTableOperator::insert_zone_merge_infos(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObIArray<ObZoneMergeInfo> &infos)
{
  return inner_insert_or_update_zone_merge_infos_(sql_client, tenant_id, false, infos);
}

int ObZoneMergeTableOperator::update_partial_zone_merge_info(
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

    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
        || OB_FAIL(dml.add_pk_column("zone", info.zone_.ptr()))) {
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
                  LOG_WARN("fail to assign extra_condition", KR(ret), K(tenant_id));
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
        } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected affected rows", KR(ret), K(affected_rows), K(meta_tenant_id), K(info));
        } else if (is_zero_row(affected_rows)) {
          if (OB_FAIL(check_scn_revert(sql_client, tenant_id, info))) {
            LOG_WARN("fail to check scn revert", KR(ret), K(tenant_id));
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("actual no need to update zone merge info", KR(ret), K(tenant_id), K(info));
      }
    }
  }
  return ret;
}

int ObZoneMergeTableOperator::update_zone_merge_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObZoneMergeInfo &info)
{
  int ret = OB_SUCCESS;
  ObArray<ObZoneMergeInfo> infos;
  if (OB_FAIL(infos.push_back(info))) {
    LOG_WARN("fail to push back zone merge info", KR(ret), K(info));
  } else if (OB_FAIL(update_zone_merge_infos(sql_client, tenant_id, infos))) {
    LOG_WARN("fail to update zone merge infos", KR(ret), K(tenant_id), K(info));
  }
  return ret;
}

int ObZoneMergeTableOperator::update_zone_merge_infos(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObIArray<ObZoneMergeInfo> &infos)
{
  return inner_insert_or_update_zone_merge_infos_(sql_client, tenant_id, true, infos);
}

int ObZoneMergeTableOperator::inner_insert_or_update_zone_merge_infos_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const bool is_update,
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

  if (is_update) {
    if (FAILEDx(dml.splice_batch_replace_sql(OB_ALL_ZONE_MERGE_INFO_TNAME, sql))) {
      LOG_WARN("fail to splice batch update sql", KR(ret), K(sql));
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

int ObZoneMergeTableOperator::delete_tenant_merge_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu",
      OB_ALL_ZONE_MERGE_INFO_TNAME, tenant_id))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
  } else {
    LOG_TRACE("succ to delete tenant_merge_info", K(tenant_id), K(sql), K(affected_rows));
  }
  return ret;
}

int ObZoneMergeTableOperator::delete_tenant_merge_info_by_zone(
    ObISQLClient &sql_client, 
    const uint64_t tenant_id,
    const common::ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t zone_cnt = zone_list.count();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (!is_valid_tenant_id(tenant_id) || zone_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(zone_cnt));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND zone in(",
      OB_ALL_ZONE_MERGE_INFO_TNAME, tenant_id))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < zone_cnt && OB_SUCC(ret); ++i) {
      if (OB_FAIL(sql.append_fmt("'%s'%s", zone_list.at(i).ptr(), (i == zone_cnt - 1) ? ")" : ", "))) {
        LOG_WARN("fail to append sql", KR(ret), K(i), "zone", zone_list.at(i));
      }
    }

    if (FAILEDx(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
    } else {
      LOG_INFO("succ to delete zone_merge_info", K(tenant_id), K(sql), K(affected_rows));
    }
  }
  return ret;
}

int ObZoneMergeTableOperator::get_zone_list(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    zone_list.reset();
    ObZone zone;
    ObMySQLResult *result = nullptr;
    char sql[OB_SHORT_SQL_LENGTH];
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    int n = snprintf(sql, sizeof(sql), "SELECT zone FROM %s WHERE tenant_id = '%lu'",
                     OB_ALL_ZONE_MERGE_INFO_TNAME, tenant_id);
    if (n < 0 || n >= OB_SHORT_SQL_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("sql buf not enough", KR(ret), K(tenant_id), K(n));
    } else if (OB_FAIL(sql_client.read(res, meta_tenant_id, sql))) {
      LOG_WARN("fail to do read", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result", KR(ret), K(tenant_id), K(sql));
    } else {
      int64_t tmp_real_str_len = 0; // only used for output parameter
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "zone", zone.ptr(), MAX_ZONE_LENGTH, tmp_real_str_len);
        (void) tmp_real_str_len; // make compiler happy
        if (OB_FAIL(zone_list.push_back(zone))) {
          LOG_WARN("fail to add zone list", KR(ret), K(tenant_id));
        }
      }

      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get zone list", KR(ret), K(sql));
      } else {
        ret = OB_SUCCESS;
      }
    }
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
  char zone_buf[OB_MAX_TZ_NAME_LEN] = "";
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

int ObZoneMergeTableOperator::check_scn_revert(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const share::ObZoneMergeInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(info));
  } else {
    HEAP_VAR(ObZoneMergeInfo, zone_merge_info) {
      zone_merge_info.tenant_id_ = tenant_id;
      zone_merge_info.zone_ = info.zone_;
      if (OB_FAIL(ObZoneMergeTableOperator::load_zone_merge_info(sql_client, tenant_id,
                                                                 zone_merge_info))) {
        LOG_WARN("fail to load zone merge info", KR(ret), K(tenant_id));
      } else {
        const ObMergeInfoItem *it = info.list_.get_first();
        while (OB_SUCC(ret) && (it != info.list_.get_header())) {
          if (NULL == it) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null item", KR(ret), KP(it), K(tenant_id), K(info));
          } else {
            if (it->need_update_ && it->is_scn_) {
              if (0 == STRCMP(it->name_, "frozen_scn")) {
                if (it->get_scn() < zone_merge_info.frozen_scn_.get_scn()) {
                  LOG_WARN("frozen_scn revert", K(tenant_id), "new_frozen_scn", it->get_scn(),
                    "origin_frozen_scn", zone_merge_info.frozen_scn_.get_scn());
                }
              } else if (0 == STRCMP(it->name_, "broadcast_scn")) {
                if (it->get_scn() < zone_merge_info.broadcast_scn_.get_scn()) {
                  LOG_WARN("broadcast_scn revert", K(tenant_id), "new_broadcast_scn",
                    it->get_scn(), "origin_broadcast_scn", zone_merge_info.broadcast_scn_.get_scn());
                }
              } else if (0 == STRCMP(it->name_, "last_merged_scn")) {
                if (it->get_scn() < zone_merge_info.last_merged_scn_.get_scn()) {
                  LOG_WARN("last_merged_scn revert", K(tenant_id), "new_last_merged_scn",
                    it->get_scn(), "origin_last_merged_scn", zone_merge_info.last_merged_scn_.get_scn());
                }
              } else if (0 == STRCMP(it->name_, "all_merged_scn")) {
                if (it->get_scn() < zone_merge_info.all_merged_scn_.get_scn()) {
                  LOG_WARN("all_merged_scn revert", K(tenant_id), "new_all_merged_scn",
                    it->get_scn(), "origin_all_merged_scn", zone_merge_info.all_merged_scn_.get_scn());
                }
              }
            }
            it = it->get_next();
          }
        }
      }
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
