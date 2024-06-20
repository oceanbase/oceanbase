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

#define USING_LOG_PREFIX SERVER

#include "ob_all_virtual_archive_dest_status.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "ob_all_virtual_ls_archive_stat.h"
#include "logservice/archiveservice/ob_archive_service.h"
#include "share/backup/ob_archive_struct.h"

using namespace oceanbase::share;
using namespace oceanbase::common::sqlclient;

namespace oceanbase
{
namespace observer
{

ObVirtualArchiveDestStatus::ObVirtualArchiveDestStatus() :
  is_inited_(false),
  ls_end_map_inited_(false),
  ls_checkpoint_map_inited_(false),
  sql_proxy_(NULL),
  table_schema_(NULL),
  tenant_array_(),
  ls_end_map_(),
  ls_checkpoint_map_()
{}

ObVirtualArchiveDestStatus::~ObVirtualArchiveDestStatus()
{
  destroy();
}

ObVirtualArchiveDestStatus::ObArchiveDestStatusInfo::ObArchiveDestStatusInfo()
{
  reset();
}

void ObVirtualArchiveDestStatus::ObArchiveDestStatusInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  dest_id_ = OB_INVALID_DEST_ID;
  status_.reset();
  path_.reset();
  checkpoint_scn_ = OB_INVALID_SCN_VAL;
  synchronized_.reset();
  comment_.reset();
}

bool ObVirtualArchiveDestStatus::ObArchiveDestStatusInfo::is_valid()
{
  return tenant_id_ != OB_INVALID_TENANT_ID
         && dest_id_ != OB_INVALID_DEST_ID
         && !status_.is_empty()
         && checkpoint_scn_ != OB_INVALID_SCN_VAL
         && !synchronized_.is_empty();
}

int ObVirtualArchiveDestStatus::init(ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "sql proxy is NULL", K(ret));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", K(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schema(effective_tenant_id_,
    OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_TID, table_schema_))) {
    SERVER_LOG(WARN, "failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table schema is NULL", KP_(table_schema), K(ret));
  } else if (OB_FAIL(ls_end_map_.init("LSEndSCNMap"))){
    SERVER_LOG(WARN, "ls end map init failed", K(ret));
  } else if (OB_FAIL(ls_checkpoint_map_.init("LSCkptSCNMap"))){
    SERVER_LOG(WARN, "ls checkpoint map init failed", K(ret));
  } else {
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
    tenant_array_.reset();
    ls_array_.reset();
    ls_end_map_inited_ = true;
    ls_checkpoint_map_inited_ = true;
  }
  return ret;
}

int ObVirtualArchiveDestStatus::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited" , K(ret));
  } else if (!start_to_read_) {
    if (OB_FAIL(get_all_tenant_())) {
      SERVER_LOG(WARN, "get all tenant failed", K(ret));
    } else if (OB_UNLIKELY(tenant_array_.size() == 0)) {
      ret = OB_ITER_END;
      SERVER_LOG(WARN, "tenant array is empty", K(ret));
    } else {
      for (int64_t tenant_idx = 0; OB_SUCC(ret) && tenant_idx < tenant_array_.count(); tenant_idx++) {
        const uint64_t curr_tenant = tenant_array_.at(tenant_idx);
        ObArchivePersistHelper persist_helper;
        common::ObSEArray<std::pair<int64_t, int64_t>, 1> dest_array;

        // reset ls_array_ for each tenant
        if (ls_array_.count() != 0 ) {
          ls_array_.reset();
        }

        if (OB_FAIL(get_all_tenant_ls_(curr_tenant))) {
          SERVER_LOG(WARN, "get all tenant ls failed", K(curr_tenant), K(ret));
        } else if (OB_FAIL(persist_helper.init(curr_tenant))) {
          SERVER_LOG(WARN, "init persist_helper failed", K(curr_tenant), K(ret));
        } else if (OB_FAIL(persist_helper.get_valid_dest_pairs(*sql_proxy_, dest_array))) {
          SERVER_LOG(WARN, "get valid dest pair failed", K(curr_tenant), K(ret));
        } else {
          if (dest_array.count() == 0) {
            SERVER_LOG(INFO, "no archive dest exist, just skip", K(ret), K(curr_tenant));
          } else {
            for (int64_t dest_idx = 0; OB_SUCC(ret) && dest_idx < dest_array.count(); dest_idx++) {
              const int64_t curr_dest = dest_array.at(dest_idx).second;
              ObArchiveDestStatusInfo dest_status_info;
              ObArray<Column> columns;

              if (ls_checkpoint_map_inited_ && ls_checkpoint_map_.count() != 0) {
                ls_checkpoint_map_.reset();
              }
              if (OB_FAIL(get_ls_checkpoint_scn_(curr_tenant, curr_dest))) {
                SERVER_LOG(WARN, "no archive ls exist", K(ret));
              } else if (OB_FAIL(get_status_info_(curr_tenant, curr_dest, dest_status_info))) {
                SERVER_LOG(WARN, "get status info failed", K(ret));
              } else {
                if (ls_end_map_inited_ && ls_end_map_.size() != 0) {
                  ls_end_map_.reset();
                }
                // get ls max scn via tenant_id
                if (OB_FAIL(get_ls_max_scn_(curr_tenant))) {
                  SERVER_LOG(WARN, "get ls max scn failed", K(curr_tenant), K(ret));
                } else if (ls_checkpoint_map_.count() == 0 || ls_end_map_.count() == 0 || ls_checkpoint_map_.count() != ls_end_map_.count()) {
                  SERVER_LOG(WARN, "map may be empty", K(curr_tenant), K(curr_dest), K(ls_end_map_.count()), K(ls_checkpoint_map_.count()));
                  continue;
                } else if (OB_FAIL(compare_scn_map_())) {
                  SERVER_LOG(WARN, "compare scn map failed", K(ret));
                } else if (is_synced_) {
                  if (OB_FAIL(check_if_switch_piece_(curr_tenant, curr_dest))) {
                    SERVER_LOG(WARN, "fail to check if switch piece", K(ret), K(curr_tenant), K(curr_dest));
                  } else if (is_synced_) {
                    dest_status_info.synchronized_.reset();
                    if (OB_FAIL(dest_status_info.synchronized_.assign("YES"))) {
                      SERVER_LOG(WARN, "fail to assign synchronized YES", K(ret));
                    } else {
                      SERVER_LOG(INFO, "success to assign dest status synchronized YES", K(dest_status_info.synchronized_));
                    }
                  }
                }

                if (OB_FAIL(ret)) {
                  SERVER_LOG(WARN, "fail to get archive dest status", K(curr_tenant), K(curr_dest), KR(ret));
                } else if (!dest_status_info.is_valid()) {
                  SERVER_LOG(WARN, "dest status info is invalid", K(curr_tenant), K(curr_dest), K(dest_status_info));
                } else if (OB_FAIL(get_full_row_(table_schema_, dest_status_info, columns))) {
                  SERVER_LOG(WARN, "failed to get full row", "table_schema", *table_schema_, K(dest_status_info), K(ret));
                } else if (OB_FAIL(project_row(columns, cur_row_))) {
                  SERVER_LOG(WARN, "failed to project row", K(ret));
                } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
                  SERVER_LOG(WARN, "fail to add row", K(cur_row_), KR(ret));
                }
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  } // start to read
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else {
      row = &cur_row_;
    }
  }

  return ret;
}

void ObVirtualArchiveDestStatus::destroy()
{
  if (is_inited_) {
    tenant_array_.reset();
    ls_array_.reset();
    ls_end_map_.destroy();
    ls_checkpoint_map_.destroy();
  }
}

void ObVirtualArchiveDestStatus::ObArchiveSCNValue::get(uint64_t &scn)
{
  scn = scn_;
}

int ObVirtualArchiveDestStatus::ObArchiveSCNValue::set(const uint64_t scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(scn == OB_INVALID_SCN_VAL)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(scn));
  } else {
    scn_ = scn;
  }
  return ret;
}

int ObVirtualArchiveDestStatus::get_all_tenant_()
{

  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);
  ObArray<uint64_t> all_tenant_array_;

  if (is_sys_tenant(effective_tenant_id_)) { // sys tenant
    if (OB_FAIL(schema_guard_->get_available_tenant_ids(all_tenant_array_))) {
      SERVER_LOG(WARN, "get tenant ids failed", K(ret));
    } else {
      for (int64_t tenant_idx = 0; OB_SUCC(ret) && tenant_idx < all_tenant_array_.count(); tenant_idx++) {
        uint64_t curr_tenant = all_tenant_array_.at(tenant_idx);
        if (is_user_tenant(curr_tenant) && OB_FAIL(tenant_array_.push_back(curr_tenant))) {
          SERVER_LOG(WARN, "failed to push back", K(curr_tenant), K(ret));
        }
      }
    }
  } else { // user tenant
    if (OB_FAIL(tenant_array_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "failed to push back", K(effective_tenant_id_), K(ret));
    }
  }

  SERVER_LOG(INFO, "get all tenant success", K(tenant_array_));
  return ret;
}

int ObVirtualArchiveDestStatus::get_all_tenant_ls_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;

    const static char *SELECT_ALL_LS = "SELECT ls_id FROM %s WHERE tenant_id = %ld and status not in "
    "('CREATING', 'CREATED', 'TENANT_DROPPING', 'CREATE_ABORT', 'PRE_TENANT_DROPPING')";
    if (OB_FAIL(sql.append_fmt(SELECT_ALL_LS, OB_ALL_VIRTUAL_LS_STATUS_TNAME, tenant_id))){
      SERVER_LOG(WARN, "failed to append table name", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
      SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        int64_t ls_id;
        EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ls_array_.push_back(ls_id))) {
            SERVER_LOG(WARN, "failed to push back ls_id", K(ls_id), K(ret));
          }
        }
      }

      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "failed to get all tenant", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObVirtualArchiveDestStatus::get_ls_max_scn_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;

      const static char *SELECT_LS_BY_TENANT = "SELECT ls_id, max_scn FROM %s WHERE tenant_id=%ld and role='LEADER'";
      if (OB_FAIL(sql.append_fmt(SELECT_LS_BY_TENANT, OB_ALL_VIRTUAL_LOG_STAT_TNAME, tenant_id))) {
        SERVER_LOG(WARN, "failed to append table name", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t ls_id;
          uint64_t max_scn;
          ObArchiveSCNValue *scn_value = NULL;

          EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
          EXTRACT_UINT_FIELD_MYSQL(*result, "max_scn", max_scn, uint64_t);

          if (OB_SUCC(ret)) {
            if (OB_FAIL(ls_end_map_.alloc_value(scn_value))) {
              SERVER_LOG(WARN, "alloc_value fail", K(ret), K(ls_id), K(max_scn));
            } else if (OB_ISNULL(scn_value)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "scn_value is NULL", K(ret), K(ls_id), K(scn_value));
            } else if (OB_FAIL(scn_value->set(max_scn))) {
              SERVER_LOG(WARN, "scn_value set failed", K(ret), K(ls_id), K(max_scn));
            } else if (OB_FAIL(ls_end_map_.insert_and_get(ObLSID(ls_id), scn_value))) {
              SERVER_LOG(WARN, "ls_end_map insert and get failed", K(ret), K(ls_id), K(scn_value));
            } else {
              SERVER_LOG(INFO, "get ls max_scn succ", K(tenant_id), K(ls_id), K(max_scn));
              ls_end_map_.revert(scn_value);
              scn_value = NULL;
            }

            if (OB_FAIL(ret) && NULL != scn_value) {
              ls_end_map_.del(ObLSID(ls_id));
              ls_end_map_.free_value(scn_value);
              scn_value = NULL;
            }
          }
        }
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to get ls max scn", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObVirtualArchiveDestStatus::get_ls_checkpoint_scn_(const uint64_t tenant_id, const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;

      const static char *SELECT_LS_CHECKPOINT = "select ls_id, max(checkpoint_scn) as checkpoint_scn from %s "
      "where ls_id in (select ls_id from %s where tenant_id=%ld) and dest_id=%ld and tenant_id=%ld group by ls_id";
      if (OB_FAIL(sql.append_fmt(SELECT_LS_CHECKPOINT, OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
                                 OB_ALL_VIRTUAL_LS_STATUS_TNAME, tenant_id, dest_id, tenant_id))) {
        SERVER_LOG(WARN, "failed to append table name", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t ls_id;
          uint64_t checkpoint_scn;
          ObArchiveSCNValue *scn_value = NULL;

          EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
          EXTRACT_UINT_FIELD_MYSQL(*result, "checkpoint_scn", checkpoint_scn, uint64_t);

          if (OB_SUCC(ret)) {
             if (OB_FAIL(ls_checkpoint_map_.alloc_value(scn_value))) {
              SERVER_LOG(WARN, "alloc_value fail", K(ret), K(ls_id), K(checkpoint_scn));
            } else if (OB_ISNULL(scn_value)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "scn_value is NULL", K(ret), K(ls_id), K(scn_value));
            } else if (OB_FAIL(scn_value->set(checkpoint_scn))) {
              SERVER_LOG(WARN, "scn_value set failed", K(ret), K(ls_id), K(checkpoint_scn));
            } else if (OB_FAIL(ls_checkpoint_map_.insert_and_get(ObLSID(ls_id), scn_value))) {
              SERVER_LOG(WARN, "ls_end_map insert and get failed", K(ret), K(ls_id), K(scn_value));
            } else {
              SERVER_LOG(INFO, "get archive checkpoint_scn success", K(tenant_id), K(dest_id), K(checkpoint_scn));
              ls_checkpoint_map_.revert(scn_value);
              scn_value = NULL;
            }

            if (OB_FAIL(ret) && NULL != scn_value) {
              ls_checkpoint_map_.del(ObLSID(ls_id));
              ls_checkpoint_map_.free_value(scn_value);
              scn_value = NULL;
            }
          }
        }
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to get ls checkpoint scn", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObVirtualArchiveDestStatus::get_full_row_(const share::schema::ObTableSchema *table,
                                              const ObArchiveDestStatusInfo &dest_status,
                                              ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited" , K(ret));
  } else if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "table is null", K(ret));
  } else {
    ADD_COLUMN(set_int, table, "tenant_id", dest_status.tenant_id_, columns);
    ADD_COLUMN(set_int, table, "dest_id", dest_status.dest_id_, columns);
    ADD_COLUMN(set_varchar, table, "path", dest_status.path_.str(), columns);
    ADD_COLUMN(set_varchar, table, "status", dest_status.status_.str(), columns);
    ADD_COLUMN(set_uint64, table, "checkpoint_scn", dest_status.checkpoint_scn_, columns);
    ADD_COLUMN(set_varchar, table, "synchronized", dest_status.synchronized_.str(), columns);
    ADD_COLUMN(set_varchar, table, "comment", dest_status.comment_.str(), columns);
  }
  return ret;
}

int ObVirtualArchiveDestStatus::get_status_info_(const uint64_t tenant_id,
                                                 const int64_t dest_id,
                                                 ObArchiveDestStatusInfo &dest_status_info)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;

      const static char *SELECT_LOG_ARCHIVE_PROGRESS = "SELECT status,path,checkpoint_scn,comment from %s "
      "where tenant_id=%ld and dest_id=%ld";
      if (OB_FAIL(sql.append_fmt(SELECT_LOG_ARCHIVE_PROGRESS, OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TNAME,
                                 tenant_id, dest_id))) {
        SERVER_LOG(WARN, "failed to append table name", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ObString temp_path;
          ObString temp_status;
          ObString temp_comment;

          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "path", temp_path);
          if (OB_SUCC(ret) && OB_FAIL(dest_status_info.path_.assign(temp_path))) {
            SERVER_LOG(WARN, "fail to assign dest_status_info.path", K(ret));
          }
          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "status", temp_status);
          if (OB_SUCC(ret) && OB_FAIL(dest_status_info.status_.assign(temp_status))) {
            SERVER_LOG(WARN, "fail to assign dest_status_info.status", K(ret));
          }
          EXTRACT_UINT_FIELD_MYSQL(*result, "checkpoint_scn", dest_status_info.checkpoint_scn_, uint64_t);

          EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "comment", temp_comment);
          if (OB_SUCC(ret) && OB_FAIL(dest_status_info.comment_.assign(temp_comment))) {
            SERVER_LOG(WARN, "fail to assign dest_status_info.comment", K(ret));
          }
          dest_status_info.tenant_id_ = tenant_id;
          dest_status_info.dest_id_ = dest_id;
          if (OB_FAIL(dest_status_info.synchronized_.assign("NO"))) {
            SERVER_LOG(WARN, "fail to assign default synchronized NO", K(ret));
          } else {
            SERVER_LOG(INFO, "success to assign default synchronized NO");
          }
        }
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to get dest status info", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObVirtualArchiveDestStatus::compare_scn_map_()
{
  int ret = OB_SUCCESS;
  ObArchiveSCNValue *max_scn_val = NULL;
  ObArchiveSCNValue *ckpt_scn_val = NULL;
  uint64_t max_scn;
  uint64_t ckpt_scn;
  is_synced_ = true;

  for (auto ls_id : ls_array_) {
    if (OB_FAIL(ls_end_map_.get(ObLSID(ls_id), max_scn_val))) {
      is_synced_ = false;
      SERVER_LOG(WARN, "get ls max scn from ls_end_map failed", K(ls_id), K(ret));
      break;
    } else {
      ls_end_map_.revert(max_scn_val);
      if (OB_FAIL(ls_checkpoint_map_.get(ObLSID(ls_id), ckpt_scn_val))) {
        is_synced_ = false;
        SERVER_LOG(WARN, "get ls checkpoint scn from ls_checkpoint_map failed", K(ls_id), K(ret));
        break;
      } else {
        ls_checkpoint_map_.revert(ckpt_scn_val);
        max_scn_val->get(max_scn);
        ckpt_scn_val->get(ckpt_scn);

        if (max_scn > ckpt_scn) {
          is_synced_ = false;
          break;
        } else if (max_scn < ckpt_scn){
          SERVER_LOG(ERROR, "it is wrong that archive checkpoint_scn is bigger than max_scn", K(max_scn), K(ckpt_scn));
        }
      }
    }
  }
  return ret;
}

int ObVirtualArchiveDestStatus::check_if_switch_piece_(const uint64_t tenant_id, const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);
  int64_t used_piece_id = OB_BACKUP_INVALID_PIECE_ID;

  if (OB_UNLIKELY(!is_inited_)) {
    SERVER_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(get_log_archive_used_piece_id_(tenant_id, dest_id, used_piece_id))){
    SERVER_LOG(WARN, "get log archive used piece id failed", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      int64_t tmp_ls_id = share::ObLSID::INVALID_LS_ID;
      int64_t tmp_piece_id = OB_BACKUP_INVALID_PIECE_ID;
      const static char *SELECT_PIECE_ID_SQL = "select ls_id, piece_id FROM %s "
          "WHERE piece_id > %ld and tenant_id=%ld and dest_id=%ld";
      if (OB_FAIL(sql.append_fmt(SELECT_PIECE_ID_SQL, OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
          used_piece_id, tenant_id, dest_id))) {
        SERVER_LOG(WARN, "failed to append table name", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "get piece result failed", K(sql.ptr()), K(ret));
      } else if (OB_FAIL(result->next()) && (OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        is_synced_ = true;
        SERVER_LOG(INFO, "no ls piece_id is bigger than archive used_piece_id", K(sql.ptr()), K(ret));
      } else {
        is_synced_ = false;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", tmp_ls_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "piece_id", tmp_piece_id, int64_t);
          SERVER_LOG(INFO, "ls piece_id is bigger than archive used_piece_id",
           K(tenant_id), K(dest_id), K(tmp_ls_id), K(tmp_piece_id), K(used_piece_id), K(sql.ptr()));
        }
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to used piece id", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObVirtualArchiveDestStatus::get_log_archive_used_piece_id_(const uint64_t tenant_id, const int64_t dest_id, int64_t &piece_id)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);

  if (OB_UNLIKELY(!is_inited_)) {
    SERVER_LOG(WARN, "not inited", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;

      const static char *SELECT_RS_PIECE_ID_SQL = "SELECT used_piece_id FROM %s WHERE tenant_id=%ld and dest_id=%ld";
      if (OB_FAIL(sql.append_fmt(SELECT_RS_PIECE_ID_SQL, OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TNAME, tenant_id, dest_id))) {
        SERVER_LOG(WARN, "failed to append table name", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          EXTRACT_INT_FIELD_MYSQL(*result, "used_piece_id", piece_id, int64_t);
          SERVER_LOG(INFO, "get used_piece_id success", K(piece_id));
        }
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to get used piece id", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}
}// end namespace observer
}// end namespace oceanbase
