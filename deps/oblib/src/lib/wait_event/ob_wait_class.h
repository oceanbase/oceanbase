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

#ifdef WAIT_CLASS_DEF
WAIT_CLASS_DEF(OTHER, "OTHER", 100)
WAIT_CLASS_DEF(APPLICATION, "APPLICATION", 101)
WAIT_CLASS_DEF(CONFIGURATION, "CONFIGURATION", 102)
WAIT_CLASS_DEF(ADMINISTRATIVE, "ADMINISTRATIVE", 103)
WAIT_CLASS_DEF(CONCURRENCY, "CONCURRENCY", 104)
WAIT_CLASS_DEF(COMMIT, "COMMIT", 105)
WAIT_CLASS_DEF(IDLE, "IDLE", 106)
WAIT_CLASS_DEF(NETWORK, "NETWORK", 107)
WAIT_CLASS_DEF(USER_IO, "USER_IO", 108)
WAIT_CLASS_DEF(SYSTEM_IO, "SYSTEM_IO", 109)
WAIT_CLASS_DEF(SCHEDULER, "SCHEDULER", 110)
WAIT_CLASS_DEF(CLUSTER, "CLUSTER", 111)
WAIT_CLASS_DEF(QUEUEING, "QUEUEING", 112)

#endif

#ifndef OB_WAIT_CLASS_H_
#define OB_WAIT_CLASS_H_
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

static const int64_t MAX_WAIT_CLASS_NAME_LENGTH = 64;

struct ObWaitClassIds
{
  enum ObWaitClassIdEnum
  {
#define WAIT_CLASS_DEF(name, wait_class, wait_class_id) name,
#include "lib/wait_event/ob_wait_class.h"
#undef WAIT_CLASS_DEF
  };
};

struct ObWaitClass
{
  char wait_class_[MAX_WAIT_CLASS_NAME_LENGTH];
  int64_t wait_class_id_;
};

extern const ObWaitClass OB_WAIT_CLASSES[];

}
}


#endif /* OB_WAIT_CLASS_H_ */
