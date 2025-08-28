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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_PS_INFO_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_PS_INFO_ 1

#include "observer/virtual_table/ob_all_plan_cache_stat.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase {
using common::ObPsStmtId;
namespace observer {
class ObAllVirtualSessionPsInfo : public ObAllPlanCacheBase {
public:
  static const int64_t BUCKET_COUNT = 128;
  using SessionID = uint32_t;
  class ObTenantSessionInfoIterator {
  public:
    ObTenantSessionInfoIterator(
        common::hash::ObHashMap<uint64_t, common::ObArray<SessionID>> &tenant_session_map)
        : last_attach_session_info_(nullptr),
          tenant_session_id_map_(tenant_session_map),
          cur_tenant_id_(OB_INVALID_TENANT_ID), cur_session_id_list_(nullptr) {}
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
    int next(uint64_t tenant_id, sql::ObSQLSessionInfo *&sess_info);
    void reset();
  private:
    ObSQLSessionInfo *last_attach_session_info_;
    common::hash::ObHashMap<uint64_t, common::ObArray<SessionID>> &tenant_session_id_map_;
    uint64_t cur_tenant_id_;
    common::ObArray<SessionID> *cur_session_id_list_;
  };

  class ObPsSessionInfoFetcher {
    public:
      ObPsSessionInfoFetcher():
        inner_stmt_id_(OB_INVALID_ID),
        stmt_type_(stmt::T_NONE),
        param_count_(-1),
        ref_count_(-1),
        checksum_(0),
        param_types_(),
        error_code_(OB_SUCCESS)
      {}
      // deep copy ObPsSessionInfo obj
      int operator() (common::hash::HashMapPair<uint64_t, ObPsSessionInfo *> &entry);
      inline ObPsStmtId get_inner_stmt_id() const { return inner_stmt_id_; }
      inline stmt::StmtType get_stmt_type() const { return stmt_type_; }
      inline int64_t get_param_count() const { return param_count_; }
      inline int64_t get_ref_count() const { return ref_count_; }
      inline uint64_t get_ps_stmt_checksum() const { return checksum_; }
      inline int get_error_code() const { return error_code_; }
      inline common::ObArray<obmysql::EMySQLFieldType> get_param_types() const { return param_types_; }
      void set_tenant_id(uint64_t tenant_id) {
        param_types_.set_tenant_id(tenant_id);
      }
      void reuse() {
        inner_stmt_id_ = OB_INVALID_ID;
        stmt_type_ = stmt::T_NONE;
        param_count_ = -1;
        ref_count_ = -1;
        checksum_ = 0;
        error_code_ = OB_SUCCESS;
        param_types_.reuse();
      }
      void reset() {
        reuse();
      }
      
    private:
      ObPsStmtId inner_stmt_id_;
      stmt::StmtType stmt_type_;
      int64_t param_count_;
      int64_t ref_count_;
      uint64_t checksum_;
      common::ObArray<obmysql::EMySQLFieldType> param_types_;
      int error_code_;
  };
  
  ObAllVirtualSessionPsInfo()
      : ObAllPlanCacheBase(), 
        fetcher_(),
        tenant_session_id_map_(),
        all_sql_session_iterator_(tenant_session_id_map_),
        cur_session_info_(nullptr),
        ps_client_stmt_ids_(),
        is_iter_end_(false) {}
  virtual ~ObAllVirtualSessionPsInfo() { reset(); }
  virtual int inner_get_next_row() override;
  virtual int inner_open() override;
  virtual void reset() override;
  int operator()(common::hash::HashMapPair<uint64_t, ObPsSessionInfo *> &entry);
private:
  int fill_cells(uint64_t tenant_id, ObPsStmtId ps_client_stmt_id,
                 bool &is_filled);
  int get_next_row_from_specified_tenant(uint64_t tenant_id, bool &is_filled);
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSessionPsInfo);
private:
  ObPsSessionInfoFetcher fetcher_;
  common::hash::ObHashMap<uint64_t, common::ObArray<SessionID>>
      tenant_session_id_map_;
  ObTenantSessionInfoIterator all_sql_session_iterator_;
  ObSQLSessionInfo *cur_session_info_;
  common::ObArray<ObPsStmtId> ps_client_stmt_ids_;
  bool is_iter_end_;
};
} // namespace observer
} // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_PS_INFO_ */
