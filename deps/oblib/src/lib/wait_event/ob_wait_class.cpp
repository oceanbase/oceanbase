/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
