/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_COMMON_ERRSIM_OB_TENANT_ERRSIM_MODULE_MGR_H_
#define SRC_COMMON_ERRSIM_OB_TENANT_ERRSIM_MODULE_MGR_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_errsim_module_type.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/utility/ob_backtrace.h"

namespace oceanbase
{
namespace common
{

struct ObTenantErrsimEvent final
{
  ObTenantErrsimEvent();
  ~ObTenantErrsimEvent() = default;
  void reset();
  bool is_valid() const;
  void build_event(const int32_t result);

  typedef ObFixedLengthString<LBT_BUFFER_LENGTH> Lbt;

  TO_STRING_KV(K_(timestamp), K_(type), K_(errsim_error), K_(backtrace));
  int64_t timestamp_;
  ObErrsimModuleType type_;
  int32_t errsim_error_;
  Lbt backtrace_;
};

} // namespace common
} // namespace oceanbase
#endif
