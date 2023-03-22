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

#include "share/ob_global_merge_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
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

int ObGlobalMergeTableOperator::load_global_merge_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObGlobalMergeInfo &info,
    const bool print_sql)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = '%lu'", OB_ALL_MERGE_INFO_TNAME, tenant_id))) {
        LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_client.read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(tenant_id), K(sql));
      } else {
        bool exist = false;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          uint64_t frozen_scn_val = UINT64_MAX;
          uint64_t global_broadcast_scn_val = UINT64_MAX;
          uint64_t last_merged_scn_val = UINT64_MAX;
          info.tenant_id_ = tenant_id;
          EXTRACT_INT_FIELD_MYSQL(*result, "cluster", info.cluster_.value_, int64_t);
          EXTRACT_UINT_FIELD_MYSQL(*result, "frozen_scn", frozen_scn_val, uint64_t);
          EXTRACT_UINT_FIELD_MYSQL(*result, "global_broadcast_scn", global_broadcast_scn_val, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "is_merge_error", info.is_merge_error_.value_, int64_t);
          EXTRACT_UINT_FIELD_MYSQL(*result, "last_merged_scn", last_merged_scn_val, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "merge_status", info.merge_status_.value_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "error_type", info.error_type_.value_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "suspend_merging", info.suspend_merging_.value_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "merge_start_time", info.merge_start_time_.value_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "last_merged_time", info.last_merged_time_.value_, int64_t);
          if (FAILEDx(info.frozen_scn_.set_scn(frozen_scn_val))) {
            LOG_WARN("fail to set frozen scn val", KR(ret), K(frozen_scn_val));
          } else if (OB_FAIL(info.global_broadcast_scn_.set_scn(global_broadcast_scn_val))) {
            LOG_WARN("fail to set global broadcast scn val", KR(ret), K(global_broadcast_scn_val));
          } else if (OB_FAIL(info.last_merged_scn_.set_scn(last_merged_scn_val))) {
            LOG_WARN("fail to set last merged scn val", KR(ret), K(last_merged_scn_val));
          } else {
            exist = true;
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret) && !exist) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("fail to find global merge info", KR(ret), K(tenant_id), K(meta_tenant_id));
        }
      }
    }
    if (print_sql) {
      LOG_INFO("finish load_gloal_merge_info", KR(ret), K(tenant_id), K(sql));
    }
  }
  return ret;
}

int ObGlobalMergeTableOperator::insert_global_merge_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObGlobalMergeInfo &info)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, meta_tenant_id);
  if (!info.is_valid() || !is_valid_tenant_id(tenant_id) ||
      (tenant_id != info.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(info));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
            || OB_FAIL(dml.add_uint64_column("cluster", info.cluster_.value_))
            || OB_FAIL(dml.add_uint64_column("frozen_scn", info.frozen_scn_.get_scn_val()))
            || OB_FAIL(dml.add_uint64_column("global_broadcast_scn", info.global_broadcast_scn_.get_scn_val()))
            || OB_FAIL(dml.add_uint64_column("is_merge_error", info.is_merge_error_.value_))
            || OB_FAIL(dml.add_uint64_column("last_merged_scn", info.last_merged_scn_.get_scn_val()))
            || OB_FAIL(dml.add_uint64_column("merge_status", info.merge_status_.value_))
            || OB_FAIL(dml.add_uint64_column("error_type", info.error_type_.value_))
            || OB_FAIL(dml.add_uint64_column("suspend_merging", info.suspend_merging_.value_))
            || OB_FAIL(dml.add_uint64_column("merge_start_time", info.merge_start_time_.value_))
            || OB_FAIL(dml.add_uint64_column("last_merged_time", info.last_merged_time_.value_))) {
      LOG_WARN("fail to add pk column", KR(ret), K(tenant_id), K(info));
  } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_MERGE_INFO_TNAME, dml, affected_rows))) {
    LOG_WARN("fail to splice exec_insert_update", KR(ret), K(meta_tenant_id), K(info));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected affected rows", KR(ret), K(affected_rows), K(meta_tenant_id), K(info));
  }
  return ret;
}

int ObGlobalMergeTableOperator::update_partial_global_merge_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObGlobalMergeInfo &info)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  if (!is_valid_tenant_id(tenant_id) || tenant_id != info.tenant_id_) {
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
        if (FAILEDx(exec.exec_update(OB_ALL_MERGE_INFO_TNAME, dml, affected_rows))) {
          LOG_WARN("fail to exec_update global_merge_info", KR(ret), K(tenant_id), K(info));
        } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected affected rows", KR(ret), K(affected_rows), K(meta_tenant_id));
        } else if (is_zero_row(affected_rows)) {
          if (OB_FAIL(check_scn_revert(sql_client, tenant_id, info))) {
            LOG_WARN("fail to check scn revert", KR(ret), K(tenant_id));
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("actual no need to update global merge info", KR(ret), K(tenant_id), K(info));
      }
    } 
  }
  return ret;
}

int ObGlobalMergeTableOperator::check_scn_revert(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObGlobalMergeInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(info));
  } else {
    HEAP_VAR(ObGlobalMergeInfo, global_merge_info) {
      if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(sql_client, tenant_id,
                                                                     global_merge_info))) {
        LOG_WARN("fail to load global merge info", KR(ret), K(tenant_id));
      } else {
        const ObMergeInfoItem *it = info.list_.get_first();
        while (OB_SUCC(ret) && (it != info.list_.get_header())) {
          if (NULL == it) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null item", KR(ret), KP(it), K(tenant_id), K(info));
          } else {
            if (it->need_update_ && it->is_scn_) {
              if (0 == STRCMP(it->name_, "frozen_scn")) {
                if (it->get_scn() < global_merge_info.frozen_scn_.get_scn()) {
                  LOG_WARN("frozen_scn revert", K(tenant_id), "new_frozen_scn", it->get_scn(),
                    "origin_frozen_scn", global_merge_info.frozen_scn_.get_scn());
                }
              } else if (0 == STRCMP(it->name_, "global_broadcast_scn")) {
                if (it->get_scn() < global_merge_info.global_broadcast_scn_.get_scn()) {
                  LOG_WARN("global_broadcast_scn revert", K(tenant_id), "new_global_broadcast_scn",
                    it->get_scn(), "origin_global_broadcast_scn", global_merge_info.global_broadcast_scn_.get_scn());
                }
              } else if (0 == STRCMP(it->name_, "last_merged_scn")) {
                if (it->get_scn() < global_merge_info.last_merged_scn_.get_scn()) {
                  LOG_WARN("last_merged_scn revert", K(tenant_id), "new_last_merged_scn",
                    it->get_scn(), "origin_last_merged_scn", global_merge_info.last_merged_scn_.get_scn());
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
