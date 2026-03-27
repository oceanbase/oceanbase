/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_TABLE_OB_TIMEZONE_IMPORTER_H_
#define OCEANBASE_SHARE_TABLE_OB_TIMEZONE_IMPORTER_H_

#include "share/table/redis/ob_redis_common.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/table/ob_table_mode_control.h"
#include "share/table/ob_redis_importer.h"

namespace oceanbase
{
namespace table
{

class ObTimezoneImporter
{
public:
  explicit ObTimezoneImporter(uint64_t tenant_id, sql::ObExecContext& exec_ctx)
      : tenant_id_(tenant_id), exec_ctx_(exec_ctx), affected_rows_(0)
  {}
  virtual ~ObTimezoneImporter() {}
  int exec_op(table::ObModuleDataArg op_arg);
  OB_INLINE int64_t get_affected_rows() { return affected_rows_; }

private:
  int import_timezone_info(const ObString &file_path);

  uint64_t tenant_id_;
  sql::ObExecContext& exec_ctx_;
  int64_t affected_rows_;
};

}  // namespace table
}  // namespace oceanbase
#endif
