/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ADMIN_DUMP_ENUM_VALUE_EXECUTOR_H_
#define OB_ADMIN_DUMP_ENUM_VALUE_EXECUTOR_H_
#include "../ob_admin_executor.h"

namespace oceanbase
{
namespace tools
{

class ObAdminDumpEnumValueExecutor : public ObAdminExecutor
{
public:
  ObAdminDumpEnumValueExecutor();
  virtual ~ObAdminDumpEnumValueExecutor();
  virtual int execute(int argc, char *argv[]);
private:
  void print_rpc_code();
};

}
}

#endif /* OB_ADMIN_DUMP_ENUM_VALUE_EXECUTOR_H_ */
