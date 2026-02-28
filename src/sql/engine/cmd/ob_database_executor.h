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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DATABASE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DATABASE_EXECUTOR_H_
#include "share/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateDatabaseStmt;
class ObDropDatabaseStmt;
class ObUseDatabaseStmt;
class ObAlterDatabaseStmt;
class ObFlashBackDatabaseStmt;
class ObPurgeDatabaseStmt;
class ObCreateDatabaseExecutor
{
public:
  ObCreateDatabaseExecutor();
  virtual ~ObCreateDatabaseExecutor();
  int execute(ObExecContext &ctx, ObCreateDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateDatabaseExecutor);
};

///////////////////////
class ObUseDatabaseExecutor
{
public:
  ObUseDatabaseExecutor();
  virtual ~ObUseDatabaseExecutor();
  int execute(ObExecContext &ctx, ObUseDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObUseDatabaseExecutor);
};

///////////////////////
class ObAlterDatabaseExecutor
{
public:
  ObAlterDatabaseExecutor();
  virtual ~ObAlterDatabaseExecutor();
  int execute(ObExecContext &ctx, ObAlterDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterDatabaseExecutor);
};

/////////////////////
class ObDropDatabaseExecutor
{
public:
  ObDropDatabaseExecutor();
  virtual ~ObDropDatabaseExecutor();
  int execute(ObExecContext &ctx, ObDropDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropDatabaseExecutor);
};


/* *
 *
 * */
class ObFlashBackDatabaseExecutor
{
public:
  ObFlashBackDatabaseExecutor() {}
  virtual ~ObFlashBackDatabaseExecutor() {}
  int execute(ObExecContext &ctx, ObFlashBackDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackDatabaseExecutor);
};

class ObPurgeDatabaseExecutor
{
public:
  ObPurgeDatabaseExecutor() {}
  virtual ~ObPurgeDatabaseExecutor() {}
  int execute(ObExecContext &ctx, ObPurgeDatabaseStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPurgeDatabaseExecutor);
};

class ObDropTableBatchScheduler
{
public:
  ObDropTableBatchScheduler();
  ~ObDropTableBatchScheduler();
  int init(const uint64_t tenant_id,
           obrpc::ObCommonRpcProxy &rpc_proxy,
           const ObString &database_name,
           const common::ObIArray<ObString> &table_names);
  int execute();
public:
  static const int64_t MAX_TABLES_PER_RPC = 1;
private:
  struct ProxyCtx
  {
  public:
    ProxyCtx();
    ~ProxyCtx();
    int init(obrpc::ObCommonRpcProxy &base_rpc_proxy, const uint64_t tenant_id);
    void reset_round();
  public:
    ObArenaAllocator allocator_;
    rootserver::ObNonAtomicDropTableInDBProxy *proxy_;
    TO_STRING_KV(KP(proxy_));
  private:
    void reset();
    DISALLOW_COPY_AND_ASSIGN(ProxyCtx);
  };
private:
  int wait_proxy(ProxyCtx &ctx);
  int send_one_rpc(ProxyCtx &ctx);
  int cleanup_pending_proxy();
  bool has_pending_proxy() const;
  bool has_pending_rpc(const ProxyCtx &ctx) const;
  bool has_sent_rpc(const ProxyCtx &ctx) const;
  bool has_finished_rpc(const ProxyCtx &ctx) const;
private:
  uint64_t tenant_id_;
  int64_t table_count_;
  int64_t next_table_idx_;
  ObString database_name_;
  common::ObArray<ObString> table_names_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  common::ObArray<ProxyCtx *> proxy_ctxs_;
  ObArenaAllocator allocator_;
  DISABLE_COPY_ASSIGN(ObDropTableBatchScheduler);
};

int non_atomic_drop_table_in_database(const uint64_t tenant_id, const ObString &database_name, obrpc::ObCommonRpcProxy &common_rpc_proxy);

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DATABASE_EXECUTOR_H_ */
