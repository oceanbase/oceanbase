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

#ifdef STAT_CLASS_DEF
STAT_CLASS_DEF(NETWORK, 1)
STAT_CLASS_DEF(QUEUE, 2)
STAT_CLASS_DEF(TRANS, 4)
STAT_CLASS_DEF(SQL, 8)
STAT_CLASS_DEF(CACHE, 16)
STAT_CLASS_DEF(STORAGE, 32)
STAT_CLASS_DEF(RESOURCE, 64)
STAT_CLASS_DEF(DEBUG, 128)
STAT_CLASS_DEF(CLOG, 256)
STAT_CLASS_DEF(ELECT, 512)
STAT_CLASS_DEF(OBSERVER, 1024)
STAT_CLASS_DEF(RS, 2048)
STAT_CLASS_DEF(SYS, 3072)
STAT_CLASS_DEF(TABLEAPI, 4096)
STAT_CLASS_DEF(WR, 8192)
#endif

#ifndef OB_STAT_CLASS_H_
#define OB_STAT_CLASS_H_
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

static const int64_t MAX_STAT_CLASS_NAME_LENGTH = 64;

struct ObStatClassIds
{
  enum ObStatClassIdEnum
  {
#define STAT_CLASS_DEF(name, num) name = num,
#include "lib/statistic_event/ob_stat_class.h"
#undef STAT_CLASS_DEF
  };
};

struct ObStatClass
{
  char stat_class_[MAX_STAT_CLASS_NAME_LENGTH];
};


static const ObStatClass OB_STAT_CLASSES[] = {
#define STAT_CLASS_DEF(name, num) \
  {#name},
#include "lib/statistic_event/ob_stat_class.h"
#undef STAT_CLASS_DEF
};

}
}


#endif /* OB_WAIT_CLASS_H_ */
