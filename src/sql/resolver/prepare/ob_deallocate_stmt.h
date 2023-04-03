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

#ifndef OCEANBASE_SQL_RESOVLER_PREPARE_DEALLOCATE_STMT_H_
#define OCEANBASE_SQL_RESOVLER_PREPARE_DEALLOCATE_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
class ObDeallocateStmt : public ObCMDStmt
{
public:
  ObDeallocateStmt() : ObCMDStmt(stmt::T_DEALLOCATE), prepare_name_(), prepare_id_(OB_INVALID_ID) {}
  virtual ~ObDeallocateStmt() {}

  inline void set_prepare_name(const common::ObString &name) { prepare_name_ = name; }
  const common::ObString &get_prepare_name() const { return prepare_name_; }
  inline void set_prepare_id(ObPsStmtId id) { prepare_id_ = id; }
  inline ObPsStmtId get_prepare_id() const { return prepare_id_; }

  TO_STRING_KV(N_STMT_NAME, prepare_name_, N_SQL_ID, prepare_id_);

private:
  common::ObString  prepare_name_;
  ObPsStmtId prepare_id_;
  DISALLOW_COPY_AND_ASSIGN(ObDeallocateStmt);
};

}//namespace sql
}//namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_PREPARE_DEALLOCATE_STMT_H_
