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

#ifndef _OB_MODULE_DATA_RESOLVER_H
#define _OB_MODULE_DATA_RESOLVER_H 1

#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObModuleDataResolver : public ObSystemCmdResolver
{
public:
  ObModuleDataResolver(ObResolverParams &params) : ObSystemCmdResolver(params) {}
  virtual ~ObModuleDataResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  static const int32_t CHILD_NUM = 4;
  static const int32_t TYPE_IDX = 0;
  static const int32_t MODULE_IDX = 1;
  static const int32_t TENANT_IDX = 2;
  static const int32_t FILE_IDX = 3;
  int resolve_exec_type(const ParseNode *node, table::ObModuleDataArg::ObInfoOpType &type);
  int resolve_module(const ParseNode *node, table::ObModuleDataArg::ObExecModule &mod);
  int resolve_target_tenant_id(const ParseNode *node, uint64_t &target_tenant_id);
  int resolve_file_path(const ParseNode *node, ObString &abs_path);
};
}//namespace sql
}//namespace oceanbase
#endif // _OB_MODULE_DATA_RESOLVER_H