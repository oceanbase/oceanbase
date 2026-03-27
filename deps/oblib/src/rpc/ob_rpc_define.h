/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEABASE_RPC_OB_RPC_DEFINE_H_
#define _OCEABASE_RPC_OB_RPC_DEFINE_H_

#include "io/easy_io.h"

namespace oceanbase
{
namespace rpc
{

static inline bool is_io_thread()
{
  return NULL != EASY_IOTH_SELF;
}

} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_OB_RPC_DEFINE_H_ */
