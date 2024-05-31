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

#include "share/schema/ob_mlog_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/scn.h"
#include "storage/mview/ob_mview_transaction.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{
struct ObMLogPurgeParam
{
public:
  ObMLogPurgeParam()
    : tenant_id_(OB_INVALID_TENANT_ID),
      master_table_id_(OB_INVALID_ID),
      purge_log_parallel_(0)
  {
  }
  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_TENANT_ID && master_table_id_ != OB_INVALID_ID;
  }
  TO_STRING_KV(K_(tenant_id), K_(master_table_id), K_(purge_log_parallel));

public:
  uint64_t tenant_id_;
  uint64_t master_table_id_;
  int64_t purge_log_parallel_;
};

class ObMLogPurger
{
public:
  ObMLogPurger();
  ~ObMLogPurger();
  DISABLE_COPY_ASSIGN(ObMLogPurger);

  int init(sql::ObExecContext &exec_ctx, const ObMLogPurgeParam &purge_param);
  int purge();

private:
  int prepare_for_purge();
  int do_purge();

private:
  sql::ObExecContext *ctx_;
  ObMLogPurgeParam purge_param_;
  ObMViewTransaction trans_;
  share::schema::ObMLogInfo mlog_info_;
  bool is_oracle_mode_;
  bool need_purge_;
  share::SCN purge_scn_;
  ObSqlString purge_sql_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
