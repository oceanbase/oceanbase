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

#include "observer/virtual_table/ob_all_virtual_session_ps_info.h"

#include "sql/resolver/ob_resolver_utils.h"

#include "observer/ob_server_utils.h"

using namespace oceanbase;
using namespace sql;
using namespace observer;
using namespace common;

int ObAllVirtualSessionPsInfo::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool is_filled = false;
  ObSQLSessionInfo *sess_info = nullptr;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    for (; tenant_id_array_idx_ < tenant_id_array_.count() &&
            is_filled == false && OB_SUCC(ret);) {
      int64_t tenant_id = tenant_id_array_.at(tenant_id_array_idx_);
      MTL_SWITCH(tenant_id) {
        if (OB_FAIL(get_next_row_from_specified_tenant(tenant_id, is_filled))) {
          if (ret == OB_ITER_END) {
            if (tenant_id_array_idx_ < tenant_id_array_.count() - 1) {
              ++tenant_id_array_idx_;
              ret = OB_SUCCESS;
            }
          } else {
            SERVER_LOG(WARN, "get_rows_from_specified_tenant failed", K(ret),
                        K(tenant_id));
          }
        }
      } else {
        // failed to switch
        ret = OB_SUCCESS;
        ++tenant_id_array_idx_;
        if (tenant_id_array_idx_ == tenant_id_array_.count()) {
          is_iter_end_ = true;
          ret = OB_ITER_END;
        }
      }
    }
  }
  if (ret == OB_ITER_END && !is_iter_end_) {
    is_iter_end_ = true;
  }
  return ret;
}

int ObAllVirtualSessionPsInfo::inner_open()
{
  int ret = OB_SUCCESS;
  ps_client_stmt_ids_.set_tenant_id(effective_tenant_id_);
  fetcher_.set_tenant_id(effective_tenant_id_);

  ObMemAttr attr(effective_tenant_id_, lib::ObLabel("BucketAlloc"));
  if (OB_FAIL(tenant_session_id_map_.create(BUCKET_COUNT,
                                         attr,
                                         attr))) {
    SERVER_LOG(WARN, "failed to init tenant_session_id_map_", K(ret));
  } else if (is_sys_tenant(effective_tenant_id_)) {
    // sys tenant show all tenant infos
    if (OB_ISNULL(GCTX.omt_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "GCTX.omt_ is NULL", K(ret));
    } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_id_array_))) {
      SERVER_LOG(WARN, "failed to add tenant id", K(ret));
    }
  } else {
    // user tenant show self tenant info
    if (OB_FAIL(tenant_id_array_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "tenant id array fail to push back", KR(ret),
                  K(effective_tenant_id_));
    }
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < tenant_id_array_.count();
        ++idx) {
    ObArray<SessionID> *tenant_sessions = nullptr;
    if (OB_ISNULL(tenant_sessions =
                      OB_NEWx(ObArray<SessionID>, allocator_))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN,
                  "failed to init tenant_session_id_map_, allocate memory failed",
                  K(ret));
    } else if (OB_FAIL(tenant_session_id_map_.set_refactored(tenant_id_array_.at(idx),
                                          *tenant_sessions))) {
      SERVER_LOG(WARN,
                  "failed to add item to tenant_session_id_map_",
                  K(ret), K(tenant_id_array_.at(idx)));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(GCTX.session_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "GCTX.session_mgr_ is NULL", K(ret));
    } else if (OB_FAIL(GCTX.session_mgr_->for_each_session(all_sql_session_iterator_))) {
      SERVER_LOG(WARN, "failed to read each session", K(ret));
    }
  }

  return ret;
}

int format_param_types(const ObIArray<obmysql::EMySQLFieldType> &param_types,
                       ObIAllocator *allocator, const char *&ptr,
                       uint64_t &len)
{
  int ret = OB_SUCCESS;
  ptr = nullptr;
  len = 0;
  ObStringBuffer str_buf(allocator);
  for (int64_t idx = 0; OB_SUCC(ret) && idx < param_types.count(); ++idx) {
    std::string str = std::to_string(param_types.at(idx));
    const char *charPtr = str.c_str();
    if (OB_FAIL(str_buf.append(charPtr))) {
      SERVER_LOG(WARN, "failed to format param_types", K(ret));
    } else if (idx < param_types.count()-1 && OB_FAIL(str_buf.append(", "))) {
      SERVER_LOG(WARN, "failed to format param_types", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ptr = str_buf.ptr();
    len = str_buf.length();
  }
  return ret;
}

int ObAllVirtualSessionPsInfo::fill_cells(uint64_t tenant_id,
                                          ObPsStmtId ps_client_stmt_id,
                                          bool &is_filled)
{
  int ret = OB_SUCCESS;
  int64_t col_count = output_column_ids_.count();
  is_filled = false;
  fetcher_.reuse();
  ObObj *cells = cur_row_.cells_;
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells pointer is NULL", K(ret));
  } else {
    if (OB_ISNULL(cur_session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur_session_info_ pointer is NULL", K(ret));
    } else if (OB_FAIL(cur_session_info_->visit_ps_session_info(ps_client_stmt_id,
                                                     fetcher_))) {
      if (ret == OB_EER_UNKNOWN_STMT_HANDLER) {
        SERVER_LOG(DEBUG, "can not find the ps_session_info, may be released",
                  K(ret), K(ps_client_stmt_id));
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "cannot get ps_session_info", K(ret),
                  K(ps_client_stmt_id));
      }
    } else if (OB_FAIL(fetcher_.get_error_code())) {
      SERVER_LOG(WARN, "failed to deep copy ps session info", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::TENANT_ID: {
          cells[i].set_int(tenant_id);
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::PROXY_SESSION_ID: {
          cells[i].set_uint64(cur_session_info_->get_proxy_sessid());
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::SESSION_ID: {
          cells[i].set_uint64(cur_session_info_->get_sessid());
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::PS_CLIENT_STMT_ID: {
          cells[i].set_int(ps_client_stmt_id);
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::PS_INNER_STMT_ID: {
          cells[i].set_int(fetcher_.get_inner_stmt_id());
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::SVR_IP: {
          ObString ipstr;
          if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
            SERVER_LOG(WARN, "get server ip failed", K(ret));
          } else {
            cells[i].set_varchar(ipstr);
            cells[i].set_collation_type(ObCharset::get_default_collation(
                ObCharset::get_default_charset()));
          }
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::SVR_PORT: {
          cells[i].set_int(GCTX.self_addr().get_port());
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::STMT_TYPE: {
          ObString stmt_type_str;
          ObString stmt_type_tmp =
              ObResolverUtils::get_stmt_type_string(fetcher_.get_stmt_type());
          if (OB_FAIL(
                  ob_write_string(*allocator_, stmt_type_tmp, stmt_type_str))) {
            SERVER_LOG(WARN, "ob write string failed", K(ret));
          } else {
            cells[i].set_varchar(stmt_type_str);
            cells[i].set_collation_type(ObCharset::get_default_collation(
                ObCharset::get_default_charset()));
          }
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::PARAM_COUNT: {
          cells[i].set_int(fetcher_.get_param_count());
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::PARAM_TYPES: {
          const char *types_str = nullptr;
          uint64_t len = 0;
          if (OB_FAIL(format_param_types(fetcher_.get_param_types(),
                                        allocator_, types_str, len))) {
            SERVER_LOG(WARN, "format_param_types failed", K(ret),
                      K(fetcher_.get_param_types()));
          } else {
            cells[i].set_lob_value(ObLongTextType, types_str,
                                  static_cast<int32_t>(len));
            cells[i].set_collation_type(ObCharset::get_default_collation(
                ObCharset::get_default_charset()));
          }
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::REF_COUNT: {
          cells[i].set_int(fetcher_.get_ref_count());
          break;
        }
        case share::ALL_VIRTUAL_SESSION_PS_INFO_CDE::CHECKSUM: {
          cells[i].set_int(fetcher_.get_ps_stmt_checksum());
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(i),
                    K(output_column_ids_), K(col_id));
          break;
        }
        }
      }
      if (OB_SUCC(ret)) {
        is_filled = true;
      }
    }
  }
  return ret;
}

int ObAllVirtualSessionPsInfo::get_next_row_from_specified_tenant(
    uint64_t tenant_id, bool &is_filled)
{
  int ret = OB_SUCCESS;
  is_filled = false;
  do {
    if (ps_client_stmt_ids_.count() == 0) {
      if (OB_FAIL(all_sql_session_iterator_.next(tenant_id, cur_session_info_))) {
        if (ret == OB_ITER_END) {
          // do nothing
        } else {
          SERVER_LOG(WARN, "get next session failed", K(ret), K(tenant_id));
        }
      } else {
        if (OB_NOT_NULL(cur_session_info_)) {
          if (OB_FAIL(cur_session_info_->for_each_ps_session_info(*this))) {
            SERVER_LOG(WARN, "failed to read each ps session info", K(ret), K(tenant_id));
          }
        } else {
          SERVER_LOG(WARN, "cur_session_info_ is nullptr", K(ret), K(tenant_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObPsStmtId ps_client_stmt_id = 0;
      if (ps_client_stmt_ids_.count() == 0) {
      } else if (OB_FAIL(ps_client_stmt_ids_.pop_back(ps_client_stmt_id))) {
        SERVER_LOG(WARN, "get client stmt id failed", K(ret));
      } else if (OB_FAIL(fill_cells(tenant_id, ps_client_stmt_id,
                                    is_filled))) {
        SERVER_LOG(WARN, "fill cells failed", K(ret));
      }
    }
  } while (!is_filled && OB_SUCC(ret));
  if (ret != OB_SUCCESS && ret != OB_ITER_END) {
    SERVER_LOG(WARN, "generate rows failed", K(ret), K(tenant_id),
               K(output_column_ids_));
  }
  return ret;
}

void ObAllVirtualSessionPsInfo::reset()
{
  ObAllPlanCacheBase::reset();
  fetcher_.reset();
  tenant_session_id_map_.reuse();
  all_sql_session_iterator_.reset();
  cur_session_info_ = nullptr;
  tenant_id_array_idx_ = 0;
  ps_client_stmt_ids_.reset();
  is_iter_end_ = false;
}

int ObAllVirtualSessionPsInfo::operator()(
    common::hash::HashMapPair<uint64_t, ObPsSessionInfo *> &entry)
{
  ObPsStmtId ps_client_stmt_id = entry.first;
  return ps_client_stmt_ids_.push_back(ps_client_stmt_id);
}

bool ObAllVirtualSessionPsInfo::ObTenantSessionInfoIterator::operator()(
    ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNDEFINED;
    SERVER_LOG(WARN, "sess_info is NULL", K(ret));
  } else {
    if (sess_info->is_shadow()) {
    } else {
      ObArray<SessionID> *session_id_list =
          const_cast<ObArray<SessionID> *>(
              tenant_session_id_map_.get(sess_info->get_priv_tenant_id()));
      if (OB_ISNULL(session_id_list)) {
      } else if (OB_FAIL(session_id_list->push_back(sess_info->get_sessid()))) {
        SERVER_LOG(WARN, "failed to push session id into session_id_list", K(ret),
               K(sess_info->get_sessid()));
      }
    }
  }
  return ret == OB_SUCCESS;
}

int ObAllVirtualSessionPsInfo::ObTenantSessionInfoIterator::next(
    uint64_t tenant_id, ObSQLSessionInfo *&sess_info)
{
  int ret = OB_SUCCESS;
  sess_info = nullptr;
  SessionID session_id = 0;
  if (OB_NOT_NULL(last_attach_session_info_)) {
    if (OB_ISNULL(GCTX.session_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "GCTX.session_mgr_ is NULL", K(ret));
    } else {
      GCTX.session_mgr_->revert_session(last_attach_session_info_);
      last_attach_session_info_ = nullptr;
    }
  }
  if (OB_SUCC(ret) && (OB_ISNULL(cur_session_id_list_) || tenant_id != cur_tenant_id_)) {
    cur_session_id_list_ = tenant_session_id_map_.get(tenant_id);
    if (OB_NOT_NULL(cur_session_id_list_)) {
      cur_tenant_id_ = tenant_id;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_SUCC(ret)) {
    do {
      if (0 == cur_session_id_list_->count()) {
        cur_session_id_list_ = nullptr;
        ret = OB_ITER_END;
      } else {
        if (OB_FAIL(cur_session_id_list_->pop_back(session_id))) {
          SERVER_LOG(WARN, "failed to get session id", K(ret));
        } else {
          if (OB_ISNULL(GCTX.session_mgr_)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "GCTX.session_mgr_ is NULL", K(ret));
          } else if (OB_FAIL(GCTX.session_mgr_->get_session(session_id, sess_info))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            }
          } else {
            last_attach_session_info_ = sess_info;
            break;
          }
        }
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

void ObAllVirtualSessionPsInfo::ObTenantSessionInfoIterator::reset()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(last_attach_session_info_)) {
    if (OB_ISNULL(GCTX.session_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "GCTX.session_mgr_ is NULL", K(ret));
    } else {
      GCTX.session_mgr_->revert_session(last_attach_session_info_);
      last_attach_session_info_ = nullptr;
    }
  }
  cur_session_id_list_ = nullptr;
  cur_tenant_id_ = 0;
}

int ObAllVirtualSessionPsInfo::ObPsSessionInfoFetcher::operator()(
    common::hash::HashMapPair<uint64_t, ObPsSessionInfo *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(entry.second)) {
    ObPsSessionInfo *ps_session_info =
        static_cast<ObPsSessionInfo *>(entry.second);
    inner_stmt_id_ = ps_session_info->get_inner_stmt_id();
    stmt_type_ = ps_session_info->get_stmt_type();
    param_count_ = ps_session_info->get_param_count();
    ref_count_ = ps_session_info->get_ref_cnt();
    checksum_ = ps_session_info->get_ps_stmt_checksum();
    if (OB_FAIL(param_types_.assign(ps_session_info->get_param_types()))) {
      SERVER_LOG(WARN, "failed to copy param types", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ps session info pointer is NULL", K(ret));
  }
  error_code_ = ret;
  return ret;
}
