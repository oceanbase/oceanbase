/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_SERVICE_GUARD_
#define OCEANBASE_LOGSERVICE_LOG_SERVICE_GUARD_
#include "lib/utility/ob_print_utils.h"                 // TO_STRING_KV
#include "lib/utility/ob_macro_utils.h"                 // DISALLOW_COPY_AND_ASSIGN

namespace oceanbase
{
namespace palf
{
class IPalfHandleImpl;
class IPalfEnvImpl;
class PalfHandleImpl;
class PalfEnvImpl;

struct IPalfHandleImplGuard
{
  IPalfHandleImplGuard();
  ~IPalfHandleImplGuard();
  bool is_valid() const;
  void reset();
  IPalfHandleImpl *get_palf_handle_impl() const { return palf_handle_impl_; }
  TO_STRING_KV(K_(palf_id), KP_(palf_handle_impl), KP_(palf_env_impl));

  int64_t  palf_id_;
  IPalfHandleImpl *palf_handle_impl_;
  IPalfEnvImpl *palf_env_impl_;
private:
  DISALLOW_COPY_AND_ASSIGN(IPalfHandleImplGuard);
};
} // end namespace palf
} // end namespace oceanbase

#endif
