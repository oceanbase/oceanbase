/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/mysqlclient/ob_single_connection_proxy.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace storage {

class ObVectorRefreshIdxTransaction : public common::ObSingleConnectionProxy {
  friend class ObVectorRefreshIdxTxnInnerMySQLGuard;

public:
  ObVectorRefreshIdxTransaction();
  virtual ~ObVectorRefreshIdxTransaction();
  DISABLE_COPY_ASSIGN(ObVectorRefreshIdxTransaction);

  int start(sql::ObSQLSessionInfo *session_info, ObISQLClient *sql_client);
  int end(const bool commit);
  bool is_started() const { return in_trans_; }
  sql::ObSQLSessionInfo *get_session_info() const { return session_info_; }
  ObCompatibilityMode get_compatibility_mode() const {
    return nullptr != session_info_ ? session_info_->get_compatibility_mode()
                                    : ObCompatibilityMode::OCEANBASE_MODE;
  }

protected:
  int connect(sql::ObSQLSessionInfo *session_info, ObISQLClient *sql_client);
  int start_transaction(uint64_t tenant_id);
  int end_transaction(const bool commit);

protected:
  class ObSessionParamSaved {
  public:
    ObSessionParamSaved();
    ~ObSessionParamSaved();
    DISABLE_COPY_ASSIGN(ObSessionParamSaved);

    int save(sql::ObSQLSessionInfo *session_info);
    int restore();

  private:
    sql::ObSQLSessionInfo *session_info_;
    bool is_inner_;
    bool autocommit_;
  };

private:
  sql::ObSQLSessionInfo *session_info_;
  ObSessionParamSaved session_param_saved_;
  int64_t start_time_;
  bool in_trans_;
};

} // namespace storage
} // namespace oceanbase