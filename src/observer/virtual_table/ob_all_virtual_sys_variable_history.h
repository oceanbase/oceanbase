/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SYS_VARIABLE_HISTORY_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SYS_VARIABLE_HISTORY_H_

#include "share/ob_virtual_table_iterator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{

// __all_virtual_sys_variable_history: exposes __all_sys_variable_history across all tenants
// with value column converted from raw storage encoding to human-readable display format.
class ObAllVirtualSysVariableHistory : public common::ObVirtualTableIterator
{
public:
  ObAllVirtualSysVariableHistory();
  virtual ~ObAllVirtualSysVariableHistory() = default;
  int init(common::ObMySQLProxy *sql_proxy);
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual int inner_close() override;

private:
  struct SysVarRow {
    int64_t tenant_id_;
    common::ObString zone_;
    common::ObString name_;
    int64_t schema_version_;
    int64_t gmt_create_;     // microseconds since epoch
    int64_t gmt_modified_;   // microseconds since epoch
    int64_t is_deleted_;
    int64_t data_type_;
    common::ObString value_;
    bool value_is_null_;
    common::ObString info_;
    int64_t flags_;
    common::ObString min_val_;
    common::ObString max_val_;
    SysVarRow()
      : tenant_id_(0), schema_version_(0), gmt_create_(0), gmt_modified_(0),
        is_deleted_(0), data_type_(0), value_is_null_(false), flags_(0) {}
    TO_STRING_KV(K_(tenant_id), K_(zone), K_(name), K_(schema_version));
  };

  static bool is_tenant_in_range(uint64_t tenant_id, const common::ObNewRange &range);
  static int raw_sys_var_value_to_display(
      common::ObIAllocator &allocator,
      const sql::ObSQLSessionInfo *session,
      const common::ObString &var_name,
      const common::ObString &raw_value,
      common::ObString &out_display,
      bool &result_is_null);
  int fetch_rows_for_tenant(uint64_t tenant_id);
  int fill_cur_row(const SysVarRow &r);

  enum COLUMN_ID {
    TENANT_ID      = common::OB_APP_MIN_COLUMN_ID,
    ZONE,
    NAME,
    SCHEMA_VERSION,
    GMT_CREATE,
    GMT_MODIFIED,
    IS_DELETED,
    DATA_TYPE,
    VALUE,
    INFO,
    FLAGS,
    MIN_VAL,
    MAX_VAL,
  };

  common::ObMySQLProxy *sql_proxy_;
  common::ObArenaAllocator row_alloc_;
  common::ObArray<SysVarRow> rows_;
  int64_t row_idx_;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSysVariableHistory);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SYS_VARIABLE_HISTORY_H_
