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

#ifndef OCEANBASE_SHARE_TABLE_OB_REDIS_IMPORTER_H_
#define OCEANBASE_SHARE_TABLE_OB_REDIS_IMPORTER_H_

#include "share/table/redis/ob_redis_common.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/table/ob_table_mode_control.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace table
{
struct ObModuleDataArg
{
public:
  enum ObInfoOpType {
    INVALID_OP = -1,
    LOAD_INFO,
    CHECK_INFO,
    MAX_OP
  };
  enum ObExecModule {
    INVALID_MOD = -1,
    REDIS,
    TIMEZONE,
    GIS,
    MAX_MOD
  };
  ObModuleDataArg() :
    op_(ObInfoOpType::INVALID_OP),
    target_tenant_id_(OB_INVALID_TENANT_ID),
    module_(ObExecModule::INVALID_MOD),
    file_path_()
  {}
  virtual ~ObModuleDataArg() {}
  int assign(const ObModuleDataArg &other);
  bool is_valid() const;
  TO_STRING_KV(K_(op), K_(target_tenant_id), K_(module), K_(file_path));

  ObInfoOpType op_; // enum ObInfoOpType
  uint64_t target_tenant_id_;
  ObExecModule module_; // ObExecModule
  ObString file_path_;
};

class ObRedisImporter
{
public:
  explicit ObRedisImporter(uint64_t tenant_id, sql::ObExecContext& exec_ctx)
      : tenant_id_(tenant_id), exec_ctx_(exec_ctx), affected_rows_(0)
  {}
  virtual ~ObRedisImporter() {}
  int exec_op(table::ObModuleDataArg::ObInfoOpType op);
  OB_INLINE int64_t get_affected_rows() { return affected_rows_; }

private:
  int get_kv_mode(ObKvModeType &kv_mode_type);
  int get_tenant_memory_size(uint64_t &memory_size);
  int check_basic_info(bool &need_import);
  int get_sql_uint_result(const char *sql, const char *col_name, uint64_t &sql_res);
  int import_redis_info();
  int check_redis_info();

  uint64_t tenant_id_;
  sql::ObExecContext& exec_ctx_;
  int64_t affected_rows_;
};

}  // namespace table
}  // namespace oceanbase
#endif
