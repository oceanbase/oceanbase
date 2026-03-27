/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_admin_dump_enum_value_executor.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace tools
{

ObAdminDumpEnumValueExecutor::ObAdminDumpEnumValueExecutor()
{
}

ObAdminDumpEnumValueExecutor::~ObAdminDumpEnumValueExecutor()
{
}

int ObAdminDumpEnumValueExecutor::execute(int argc, char *argv[])
{
  UNUSEDx(argc, argv);
  int ret = OB_SUCCESS;

  printf("MODULE ID SEMANTICS\n");

  print_rpc_code();

  return ret;
}

void ObAdminDumpEnumValueExecutor::print_rpc_code()
{
#define PCODE_DEF(name, id) printf("RPC_CODE %d %s\n", id, #name);
#include "rpc/obrpc/ob_rpc_packet_list.h"
#undef PCODE_DEF
}

}
}
