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

#include "lib/allocator/ob_page_manager.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase::lib;
namespace oceanbase
{
namespace common
{
void ObPageManager::reset()
{
  ctx_id_ = ObCtxIds::GLIBC;
  bs_.reset();
  used_ = 0;
  is_inited_ = false;
}

_RLOCAL(ObPageManager *, ObPageManager::tl_instance_);

} // end of namespace common
} // end of namespace oceanbase
