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

#include "lib/wait_event/ob_wait_class.h"
namespace oceanbase {
namespace common {
const ObWaitClass OB_WAIT_CLASSES[] = {
#define WAIT_CLASS_DEF(name, wait_class, wait_class_id) \
  {wait_class, wait_class_id},

#include "lib/wait_event/ob_wait_class.h"

#undef WAIT_CLASS_DEF
};
}
}
