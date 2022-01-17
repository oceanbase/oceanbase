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
namespace oceanbase {
namespace share {
namespace schema {
class ObPartition;
class ObSubPartition;
class ObBasePartition;
class ObMultiVersionSchemaService;
}  // namespace schema
}  // namespace share
namespace obrpc {
class ObAlterTableArg;
}
namespace common {
class ObIAllocator;
class ObExprCtx;
class ObNewRow;
namespace sqlclient {
class ObMySQLProxy;
}
}  // namespace common
namespace sql {
class ObExecContext;
class ObRawExpr;
class ObCreateTableStmt;
class ObTableStmt;

class ObTableExecutorUtils {
public:
  static int get_first_stmt(
      const common::ObString& stmt, common::ObString& first_stmt, ObSQLMode sql_mode = DEFAULT_OCEANBASE_MODE);
};

class ObCreateTableExecutor {
public:
  ObCreateTableExecutor();
  virtual ~ObCreateTableExecutor();
  int execute(ObExecContext& ctx, ObCreateTableStmt& stmt);
  int set_index_arg_list(ObExecContext& ctx, ObCreateTableStmt& stmt);
  int execute_ctas(ObExecContext& ctx, ObCreateTableStmt& stmt, obrpc::ObCommonRpcProxy* common_rpc_proxy);

private:
  int prepare_ins_arg(ObCreateTableStmt& stmt, const ObSQLSessionInfo* my_session, ObSqlString& ins_sql);
  int prepare_alter_arg(
      ObCreateTableStmt& stmt, const ObSQLSessionInfo* my_session, obrpc::ObAlterTableArg& alter_table_arg);
  int prepare_drop_arg(const ObCreateTableStmt& stmt, const ObSQLSessionInfo* my_session,
      obrpc::ObTableItem& table_item, obrpc::ObDropTableArg& drop_table_arg);
};

class ObAlterTableStmt;
class ObAlterTableExecutor {
public:
  ObAlterTableExecutor();
  virtual ~ObAlterTableExecutor();
  int execute(ObExecContext& ctx, ObAlterTableStmt& stmt);

private:
  struct PartitionServer {
    PartitionServer() : pkey_(), server_()
    {}
    void reset()
    {
      pkey_.reset();
      server_.reset();
    }
    TO_STRING_KV(K(pkey_), K(server_));
    int set_server(const common::ObAddr& server)
    {
      int ret = common::OB_SUCCESS;
      if (!server.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid argument", K(ret), K(server));
      } else {
        server_ = server;
      }
      return ret;
    }
    int set_partition_key(const common::ObPartitionKey& pkey)
    {
      int ret = common::OB_SUCCESS;
      if (!pkey.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid argument", K(ret), K(pkey));
      } else {
        pkey_ = pkey;
      }
      return ret;
    }
    int set(const common::ObAddr& server, common::ObPartitionKey& pkey)
    {
      int ret = common::OB_SUCCESS;
      if (!server.is_valid() || !pkey.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid argument", K(ret), K(server), K(pkey));
      } else {
        server_ = server;
        pkey_ = pkey;
      }
      return ret;
    }
    int set(
        const common::ObAddr& server, const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt)
    {
      int ret = common::OB_SUCCESS;
      if (!server.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid argument", K(ret), K(server));
      } else if (OB_FAIL(pkey_.init(table_id, partition_id, partition_cnt))) {
        RS_LOG(WARN, "fail to init pkey", K(ret), K(table_id), K(partition_id), K(partition_cnt));
      } else {
        server_ = server;
      }
      return ret;
    }
    common::ObPartitionKey pkey_;
    common::ObAddr server_;
  };
  static const int64_t TIME_INTERVAL_PER_PART_US = 50 * 1000;                          // 50ms
  static const int64_t MAX_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US = 120LL * 1000000LL;  // 120s
  static const int64_t MIN_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US = 20LL * 1000000LL;   // 20s
  static const int64_t WAIT_US = 500 * 1000;                                           // 500ms
  static const int64_t GET_ASSOCIATED_SNAPSHOT_TIMEOUT = 9000000LL;                    // 9s

  int alter_table_rpc_v1(obrpc::ObAlterTableArg& alter_table_arg, obrpc::ObAlterTableRes& res,
      common::ObIAllocator& allocator, obrpc::ObCommonRpcProxy* common_rpc_proxy, ObSQLSessionInfo* my_session,
      const bool is_sync_ddl_user);

  int alter_table_rpc_v2(obrpc::ObAlterTableArg& alter_table_arg, obrpc::ObAlterTableRes& res,
      common::ObIAllocator& allocator, obrpc::ObCommonRpcProxy* common_rpc_proxy, ObSQLSessionInfo* my_session,
      const bool is_sync_ddl_user);

  int set_alter_col_nullable_ddl_stmt_str(obrpc::ObAlterTableArg& alter_table_arg, common::ObIAllocator& allocator);

  int set_drop_constraint_ddl_stmt_str(obrpc::ObAlterTableArg& alter_table_arg, common::ObIAllocator& allocator);

  int set_alter_constraint_ddl_stmt_str_for_check(
      obrpc::ObAlterTableArg& alter_table_arg, common::ObIAllocator& allocator);

  int set_alter_constraint_ddl_stmt_str_for_fk(
      obrpc::ObAlterTableArg& alter_table_arg, common::ObIAllocator& allocator);

  int init_build_snapshot_ctx(const common::ObIArray<PartitionServer>& partition_leader_array,
      common::ObIArray<int64_t>& invalid_snapshot_id_array, common::ObIArray<int64_t>& snapshot_array);

  int generate_original_table_partition_leader_array(ObExecContext& ctx,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema* data_schema,
      common::ObIArray<PartitionServer>& partition_leader_array);

  int update_partition_leader_array(common::ObIArray<PartitionServer>& partition_leader_array,
      const common::ObIArray<int>& ret_code_array, const common::ObIArray<int64_t>& invalid_snapshot_id_array);

  int pick_build_snapshot(const common::ObIArray<int64_t>& snapshot_array, int64_t& snapshot);

  template <typename PROXY>
  int update_build_snapshot_ctx(PROXY& proxy, const common::ObIArray<int>& ret_code_array,
      common::ObIArray<int64_t>& invalid_snapshot_id_array, common::ObIArray<int64_t>& snapshot_array);

  template <typename PROXY, typename ARG>
  int do_get_associated_snapshot(PROXY& rpc_proxy, ARG& rpc_arg, int64_t schema_version,
      const share::schema::ObTableSchema* data_schema, common::ObIArray<PartitionServer>& partition_leader_array,
      int64_t& snapshot);

  int get_constraint_check_snapshot(ObExecContext& ctx, int64_t schema_version,
      share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t table_id, int64_t& snapshot);

  int check_data_validity_for_check_by_inner_sql(const share::schema::AlterTableSchema& alter_table_schema,
      ObCommonSqlProxy* sql_proxy, const ObString& check_expr_str, bool& is_data_valid);

  int check_check_constraint_data_validity(ObExecContext& ctx, const obrpc::ObAlterTableArg& alter_table_arg,
      ObCommonSqlProxy* sql_proxy, int64_t schema_version, const ObString& check_expr_str, bool& is_data_valid);

  int check_data_validity_for_fk_by_inner_sql(const share::schema::AlterTableSchema& alter_table_schema,
      const obrpc::ObCreateForeignKeyArg& fk_arg, ObCommonSqlProxy* sql_proxy, bool& is_data_valid);

  int check_fk_constraint_data_validity(ObExecContext& ctx, const obrpc::ObAlterTableArg& alter_table_arg,
      ObCommonSqlProxy* sql_proxy, int64_t schema_version, bool& is_data_valid);

  int check_alter_partition(ObExecContext& ctx, ObAlterTableStmt& stmt, const obrpc::ObAlterTableArg& arg);

  int set_index_arg_list(ObExecContext& ctx, ObAlterTableStmt& stmt);

  int refresh_schema_for_table(const uint64_t tenant_id);

private:
  // DISALLOW_COPY_AND_ASSIGN(ObAlterTableExecutor);
};

class ObDropTableStmt;
class ObDropTableExecutor {
public:
  ObDropTableExecutor();
  virtual ~ObDropTableExecutor();
  int execute(ObExecContext& ctx, ObDropTableStmt& stmt);

private:
};

class ObRenameTableStmt;
class ObRenameTableExecutor {
public:
  ObRenameTableExecutor();
  virtual ~ObRenameTableExecutor();
  int execute(ObExecContext& ctx, ObRenameTableStmt& stmt);

private:
};

class ObTruncateTableStmt;
class ObTruncateTableExecutor {
public:
  ObTruncateTableExecutor();
  virtual ~ObTruncateTableExecutor();
  int execute(ObExecContext& ctx, ObTruncateTableStmt& stmt);

private:
};

class ObCreateTableLikeStmt;
class ObCreateTableLikeExecutor {
public:
  ObCreateTableLikeExecutor();
  virtual ~ObCreateTableLikeExecutor();
  int execute(ObExecContext& ctx, ObCreateTableLikeStmt& stmt);

private:
};

class ObFlashBackTableFromRecyclebinStmt;
class ObFlashBackTableFromRecyclebinExecutor {
public:
  ObFlashBackTableFromRecyclebinExecutor()
  {}
  virtual ~ObFlashBackTableFromRecyclebinExecutor()
  {}
  int execute(ObExecContext& ctx, ObFlashBackTableFromRecyclebinStmt& stmt);

private:
};

class ObFlashBackTableToScnStmt;
class ObFlashBackTableToScnExecutor {
public:
  ObFlashBackTableToScnExecutor()
  {}
  virtual ~ObFlashBackTableToScnExecutor()
  {}
  int execute(ObExecContext& ctx, ObFlashBackTableToScnStmt& stmt);

private:
};

class ObPurgeTableStmt;
class ObPurgeTableExecutor {
public:
  ObPurgeTableExecutor()
  {}
  virtual ~ObPurgeTableExecutor()
  {}
  int execute(ObExecContext& ctx, ObPurgeTableStmt& stmt);

private:
};

class ObOptimizeTableStmt;
class ObOptimizeTableExecutor {
public:
  ObOptimizeTableExecutor() = default;
  virtual ~ObOptimizeTableExecutor() = default;
  int execute(ObExecContext& ctx, ObOptimizeTableStmt& stmt);
};

class ObOptimizeTenantStmt;
class ObOptimizeTenantExecutor {
public:
  ObOptimizeTenantExecutor() = default;
  virtual ~ObOptimizeTenantExecutor() = default;
  int execute(ObExecContext& ctx, ObOptimizeTenantStmt& stmt);
  static int optimize_tenant(const obrpc::ObOptimizeTenantArg& arg, const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService& schema_service, obrpc::ObCommonRpcProxy* common_rpc_proxy);
};

class ObOptimizeAllStmt;
class ObOptimizeAllExecutor {
public:
  ObOptimizeAllExecutor() = default;
  virtual ~ObOptimizeAllExecutor() = default;
  int execute(ObExecContext& ctx, ObOptimizeAllStmt& stmt);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_OB_TABLE_EXECUTOR_
