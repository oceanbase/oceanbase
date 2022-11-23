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

#ifndef OCEANBASE_LOGSERVICE_LOG_SERVICE_GUARD_
#define OCEANBASE_LOGSERVICE_LOG_SERVICE_GUARD_
#include "lib/utility/ob_print_utils.h"                 // TO_STRING_KV
#include "lib/utility/ob_macro_utils.h"                 // DISALLOW_COPY_AND_ASSIGN
#include "palf_handle_impl.h"                           // PalfHandleImpl
#include "palf_env_impl.h"                              // PalfHandleImplMap

namespace oceanbase
{
namespace palf
{
struct PalfHandleImplGuard
{
  PalfHandleImplGuard();
  ~PalfHandleImplGuard();
  int set_palf_handle_impl(const int64_t palf_id,
                      PalfHandleImplMap *palf_handle_impl_map);
  int set_palf_handle_impl(const int64_t palf_id,
                      PalfHandleImpl *palf_handle_impl,
                      PalfHandleImplMap *palf_handle_impl_map);
  bool is_valid() const;
  IPalfHandleImpl *get_palf_handle_impl() const { return static_cast<IPalfHandleImpl*>(palf_handle_impl_); }
  TO_STRING_KV(K_(palf_id), KP_(palf_handle_impl), KP_(palf_handle_impl_map));

  int64_t  palf_id_;
  PalfHandleImpl *palf_handle_impl_;
  PalfHandleImplMap *palf_handle_impl_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(PalfHandleImplGuard);
};
} // end namespace palf
} // end namespace oceanbase

#endif
