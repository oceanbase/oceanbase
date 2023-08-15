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
