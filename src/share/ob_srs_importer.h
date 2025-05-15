/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_TABLE_OB_SRS_IMPORTER_H_
#define OCEANBASE_SHARE_TABLE_OB_SRS_IMPORTER_H_

#include "share/table/redis/ob_redis_common.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/table/ob_table_mode_control.h"
#include "share/table/ob_redis_importer.h"

namespace oceanbase
{
namespace table
{

class ObSRSImporter
{
public:
  explicit ObSRSImporter(uint64_t tenant_id, sql::ObExecContext& exec_ctx)
      : tenant_id_(tenant_id), exec_ctx_(exec_ctx), affected_rows_(0)
  {}
  virtual ~ObSRSImporter() {}
  int exec_op(table::ObModuleDataArg op_arg);
  OB_INLINE int64_t get_affected_rows() { return affected_rows_; }

private:
  int import_srs_info(const ObString &file_path);

  uint64_t tenant_id_;
  sql::ObExecContext& exec_ctx_;
  int64_t affected_rows_;
};

}  // namespace table
}  // namespace oceanbase
#endif
