// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <yuya.yu@oceanbase.com>

#pragma once

#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObTableLoadTableCtx;

class ObTableLoadRedefTable
{
public:
  ObTableLoadRedefTable();
  ~ObTableLoadRedefTable();
  void reset();
  int init(ObTableLoadTableCtx *ctx, sql::ObSQLSessionInfo *session_info);
  int start();
  int finish();
  int abort();
  OB_INLINE int64_t get_ddl_task_id() const { return ddl_task_id_; }
private:
  ObTableLoadTableCtx * ctx_;
  sql::ObSQLSessionInfo *session_info_;
  int64_t ddl_task_id_;
  int64_t schema_version_;
  bool is_finish_or_abort_called_;
  bool is_inited_;
};
}  // namespace observer
}  // namespace oceanbase
