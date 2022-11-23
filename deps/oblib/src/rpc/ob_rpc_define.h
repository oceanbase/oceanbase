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
