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

#ifndef OCEANBASE_SQL_ENGINE_CMD_ROLE_CMD_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_ROLE_CMD_EXECUTOR_
#include "lib/string/ob_string.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
struct ObCreateRoleArg;
class ObDropUserArg;
struct ObAlterRoleArg;
}
namespace sql
{
class ObExecContext;
class ObCreateRoleStmt;
class ObDropRoleStmt;
class ObAlterRoleStmt;
class ObCreateRoleExecutor
{
public:
  ObCreateRoleExecutor() {}
  virtual ~ObCreateRoleExecutor() {}
  int execute(ObExecContext &ctx, ObCreateRoleStmt &stmt);
  static int encrypt_passwd(const common::ObString& passwd,
                            common::ObString& encrypted_passwd,
                            char *enc_buf,
                            int64_t buf_len);
private:
  int create_role(obrpc::ObCommonRpcProxy *rpc_proxy,
                  const obrpc::ObCreateRoleArg &arg) const;
  int drop_role(obrpc::ObCommonRpcProxy *rpc_proxy,
                  const obrpc::ObDropUserArg &arg) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateRoleExecutor);
};

class ObDropRoleExecutor
{
public:
  ObDropRoleExecutor() {}
  virtual ~ObDropRoleExecutor() {}
  int execute(ObExecContext &ctx, ObDropRoleStmt &stmt);
  static int encrypt_passwd(const common::ObString& passwd,
                            common::ObString& encrypted_passwd,
                            char *enc_buf,
                            int64_t buf_len);
private:
  int drop_role(obrpc::ObCommonRpcProxy *rpc_proxy,
                  const obrpc::ObDropUserArg &arg) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropRoleExecutor);
};

class ObAlterRoleExecutor
{
public:
  ObAlterRoleExecutor() {}
  virtual ~ObAlterRoleExecutor() {}
  int execute(ObExecContext &ctx, ObAlterRoleStmt &stmt);
  static int encrypt_passwd(const common::ObString& passwd,
                            common::ObString& encrypted_passwd,
                            char *enc_buf,
                            int64_t buf_len);
private:
  int alter_role(obrpc::ObCommonRpcProxy *rpc_proxy,
                 const obrpc::ObAlterRoleArg &arg) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterRoleExecutor);
};

}
}
#endif //OCEANBASE_SQL_ENGINE_CMD_USER_CMD_EXECUTOR_
