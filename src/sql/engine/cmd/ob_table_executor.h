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

#ifndef OCEANBASE_SQL_OB_TABLE_EXECUTOR_
#define OCEANBASE_SQL_OB_TABLE_EXECUTOR_
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_rpc_struct.h"
#include "sql/ob_sql_context.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObPartition;
struct ObSubPartition;
struct ObBasePartition;
class ObMultiVersionSchemaService;
}
}
namespace obrpc
{
struct ObAlterTableArg;
}
namespace common
{
class ObIAllocator;
struct ObExprCtx;
class ObNewRow;
// namespace sqlclient
// {
class ObMySQLProxy;
// }
}
namespace sql
{
class ObExecContext;
class ObRawExpr;
class ObCreateTableStmt;
class ObTableStmt;

class ObCreateTableExecutor
{
  class ObInsSQLPrinter : public ObISqlPrinter
  {
  public:
    ObInsSQLPrinter(ObCreateTableStmt *stmt,
                    ObSchemaGetterGuard *schema_guard,
                    ObObjPrintParams print_params,
                    const ParamStore *param_store,
                    bool do_osg) :
      stmt_(stmt),
      schema_guard_(schema_guard),
      print_params_(print_params),
      param_store_(param_store),
      do_osg_(do_osg)
      {}
    virtual int inner_print(char *buf, int64_t buf_len, int64_t &res_len) override;
  private:
    ObCreateTableStmt *stmt_;
    ObSchemaGetterGuard *schema_guard_;
    ObObjPrintParams print_params_;
    const ParamStore *param_store_;
    bool do_osg_;
  };
public:
  ObCreateTableExecutor();
  virtual ~ObCreateTableExecutor();
  int execute(ObExecContext &ctx, ObCreateTableStmt &stmt);
  int set_index_arg_list(ObExecContext &ctx, ObCreateTableStmt &stmt);
  int execute_ctas(ObExecContext &ctx, ObCreateTableStmt &stmt, obrpc::ObCommonRpcProxy *common_rpc_proxy);
private:
  int prepare_stmt(ObCreateTableStmt &stmt, const ObSQLSessionInfo &my_session, ObString &create_table_name);
  int prepare_ins_arg(ObCreateTableStmt &stmt,
                      const ObSQLSessionInfo *my_session,
                      ObSchemaGetterGuard *schema_guard,
                      const ParamStore *param_store,
                      ObSqlString &ins_sql);
  int prepare_alter_arg(ObCreateTableStmt &stmt, const ObSQLSessionInfo *my_session, const ObString &create_table_name, obrpc::ObAlterTableArg &alter_table_arg);
  int prepare_drop_arg(const ObCreateTableStmt &stmt, const ObSQLSessionInfo *my_session, obrpc::ObTableItem &table_item, obrpc::ObDropTableArg &drop_table_arg);
};

class ObAlterTableStmt;
class ObAlterTableExecutor
{
public:
  ObAlterTableExecutor();
  virtual ~ObAlterTableExecutor();
  int execute(ObExecContext &ctx, ObAlterTableStmt &stmt);

  static int update_external_file_list(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const common::ObString &location,
      const common::ObString &access_info,
      const common::ObString &pattern,
      ObExecContext &ctx);
  static int collect_local_files_on_servers(
      const uint64_t tenant_id,
      const common::ObString &location,
      common::ObIArray<common::ObAddr> &all_servers,
      common::ObIArray<common::ObString> &file_urls,
      common::ObIArray<int64_t> &file_sizes,
      common::ObIAllocator &allocator);
  static int flush_external_file_cache(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const common::ObIArray<common::ObAddr> &all_servers);
private:
  static const int64_t TIME_INTERVAL_PER_PART_US = 50 * 1000; // 50ms
  static const int64_t MAX_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US = 120LL * 1000000LL; // 120s
  static const int64_t MIN_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US = 20LL * 1000000LL; // 20s
  static const int64_t WAIT_US = 500 * 1000; // 500ms
  static const int64_t GET_ASSOCIATED_SNAPSHOT_TIMEOUT = 9000000LL; // 9s
  int check_constraint_validity(ObExecContext &ctx,
      obrpc::ObAlterTableArg &alter_table_arg,
      common::ObIAllocator &allocator,
      obrpc::ObAlterTableRes &res,
      obrpc::ObCommonRpcProxy &common_rpc_proxy,
      ObString first_stmt,
      const bool need_modify_notnull_validate);

  int alter_table_rpc_v2(
      obrpc::ObAlterTableArg &alter_table_arg,
      obrpc::ObAlterTableRes &res,
      common::ObIAllocator &allocator,
      obrpc::ObCommonRpcProxy *common_rpc_proxy,
      ObSQLSessionInfo *my_session,
      const bool is_sync_ddl_user);

  int need_check_constraint_validity(obrpc::ObAlterTableArg &alter_table_arg, bool &need_check);

  int set_alter_col_nullable_ddl_stmt_str(
      obrpc::ObAlterTableArg &alter_table_arg,
      common::ObIAllocator &allocator);

  int check_alter_partition(
      ObExecContext &ctx,
      ObAlterTableStmt &stmt,
      const obrpc::ObAlterTableArg &arg);
  int resolve_alter_column_partition_expr(
      const share::schema::ObColumnSchemaV2 &col_schema,
      const share::schema::ObTableSchema &table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObSQLSessionInfo &session_info,
      common::ObIAllocator &allocator,
      const bool is_sub_part,
      ObExprResType &dst_res_type);
  template<class T>
  int calc_range_part_high_bound(
      const ObPartitionFuncType part_func_type,
      const ObString &col_name,
      const ObExprResType &dst_res_type,
      T &part,
      ObExecContext &ctx);
  int calc_range_values_exprs(
      const share::schema::ObColumnSchemaV2 &col_schema,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObSQLSessionInfo &session_info,
      common::ObIAllocator &allocator,
      ObExecContext &ctx,
      const bool is_subpart);
  template<class T>
  int calc_list_part_rows(
    const ObPartitionFuncType part_func_type,
    const ObString &col_name,
    const ObExprResType &dst_res_type,
    const T &orig_part,
    T &new_part,
    ObExecContext &ctx,
    common::ObIAllocator &allocator);
  int calc_list_values_exprs(
      const share::schema::ObColumnSchemaV2 &col_schema,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObSQLSessionInfo &session_info,
      common::ObIAllocator &allocator,
      ObExecContext &ctx,
      const bool is_subpart);
  int check_alter_part_key(ObExecContext &ctx,
                           obrpc::ObAlterTableArg &arg);

  int set_index_arg_list(ObExecContext &ctx, ObAlterTableStmt &stmt);

  int refresh_schema_for_table(const uint64_t tenant_id);
  int execute_alter_external_table(ObExecContext &ctx, ObAlterTableStmt &stmt);
  static int get_external_file_list(
    const ObString &location,
    common::ObIArray<common::ObString> &file_urls,
    common::ObIArray<int64_t> &file_sizes,
    const common::ObString &access_info,
    common::ObIAllocator &allocator,
    common::ObStorageType &storage_type);
  static int filter_and_sort_external_files(const ObString &pattern,
                                            ObExecContext &exec_ctx,
                                            ObIArray<ObString> &file_urls,
                                            ObIArray<int64_t> &file_sizes);
private:
  //DISALLOW_COPY_AND_ASSIGN(ObAlterTableExecutor);
};

class ObDropTableStmt;
class ObDropTableExecutor
{
public:
  ObDropTableExecutor();
  virtual ~ObDropTableExecutor();
  int execute(ObExecContext &ctx, ObDropTableStmt &stmt);
private:
};

class ObRenameTableStmt;
class ObRenameTableExecutor
{
public:
  ObRenameTableExecutor();
  virtual ~ObRenameTableExecutor();
  int execute(ObExecContext &ctx, ObRenameTableStmt &stmt);
private:

};

class ObTruncateTableStmt;
class ObTruncateTableExecutor
{
public:
  ObTruncateTableExecutor();
  virtual ~ObTruncateTableExecutor();
  int execute(ObExecContext &ctx, ObTruncateTableStmt &stmt);
private:
  int check_use_parallel_truncate(const obrpc::ObTruncateTableArg &arg, bool &use_parallel_truncate);

};

class ObCreateTableLikeStmt;
class ObCreateTableLikeExecutor
{
public:
  ObCreateTableLikeExecutor();
  virtual ~ObCreateTableLikeExecutor();
  int execute(ObExecContext &ctx, ObCreateTableLikeStmt &stmt);
private:

};

class ObFlashBackTableFromRecyclebinStmt;
class ObFlashBackTableFromRecyclebinExecutor
{
public:
  ObFlashBackTableFromRecyclebinExecutor() {}
  virtual ~ObFlashBackTableFromRecyclebinExecutor() {}
  int execute(ObExecContext &ctx, ObFlashBackTableFromRecyclebinStmt &stmt);
private:
};

class ObFlashBackTableToScnStmt;
class ObFlashBackTableToScnExecutor
{
public:
  ObFlashBackTableToScnExecutor() {}
  virtual ~ObFlashBackTableToScnExecutor() {}
  int execute(ObExecContext &ctx, ObFlashBackTableToScnStmt &stmt);
private:
};

class ObPurgeTableStmt;
class ObPurgeTableExecutor
{
public:
  ObPurgeTableExecutor() {}
  virtual ~ObPurgeTableExecutor() {}
  int execute(ObExecContext &ctx, ObPurgeTableStmt &stmt);
private:
};

class ObOptimizeTableStmt;
class ObOptimizeTableExecutor
{
public:
  ObOptimizeTableExecutor() = default;
  virtual ~ObOptimizeTableExecutor() = default;
  int execute(ObExecContext &ctx, ObOptimizeTableStmt &stmt);
};

class ObOptimizeTenantStmt;
class ObOptimizeTenantExecutor
{
public:
  ObOptimizeTenantExecutor() = default;
  virtual ~ObOptimizeTenantExecutor() = default;
  int execute(ObExecContext &ctx, ObOptimizeTenantStmt &stmt);
  static int optimize_tenant(const obrpc::ObOptimizeTenantArg &arg,
      const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService &schema_service,
      obrpc::ObCommonRpcProxy *common_rpc_proxy);
};

class ObOptimizeAllStmt;
class ObOptimizeAllExecutor
{
public:
  ObOptimizeAllExecutor() = default;
  virtual ~ObOptimizeAllExecutor() = default;
  int execute(ObExecContext &ctx, ObOptimizeAllStmt &stmt);
};

} //end namespace sql
} //end namespace oceanbase


#endif //OCEANBASE_SQL_OB_TABLE_EXECUTOR_
