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

#include "lib/thread_local/ob_tsi_factory.h"

namespace oceanbase {
namespace common {
TSIFactory& get_tsi_fatcory()
{
  static TSIFactory instance;
  return instance;
}

void tsi_factory_init()
{
  get_tsi_fatcory().init();
}

void tsi_factory_destroy()
{
  get_tsi_fatcory().destroy();
}
}  // namespace common
}  // namespace oceanbase
