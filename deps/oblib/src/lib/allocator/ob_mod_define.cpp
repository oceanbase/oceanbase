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

#include "lib/allocator/ob_mod_define.h"

#include <malloc.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/thread_local/ob_tsi_factory.h"

namespace oceanbase {
namespace common {

#define LABEL_ITEM_DEF(name, ...) constexpr const char ObModIds::name[];
#include "lib/allocator/ob_mod_define.h"
#undef LABEL_ITEM_DEF

int ObModSet::get_mod_id(const common::ObString& mod_name) const
{
  int id = -1;
  bool found = false;
  for (int i = 0; !found && i < MOD_COUNT_LIMIT; i++) {
    if (mod_names_[i] != NULL) {
      if ((strlen(mod_names_[i]) == (uint64_t)(mod_name.length())) &&
          (0 == strncasecmp(mod_names_[i], mod_name.ptr(), mod_name.length()))) {
        id = i;
        found = true;
      }
    }
  }
  return id;
}

};  // end namespace common
};  // end namespace oceanbase
