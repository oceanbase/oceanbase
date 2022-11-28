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

#ifndef OCEANBASE_SRC_PL_DEBUG_OB_PL_DEBUGGER_MANAGER_H
#define OCEANBASE_SRC_PL_DEBUG_OB_PL_DEBUGGER_MANAGER_H

#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_queue_thread.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/container/ob_id_map.h"
#include "ob_pl_debugger.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace pl
{
namespace debugger
{
class ObPLDebugger;
}
class ObPLDebuggerGuard
{
public:
  ObPLDebuggerGuard(uint32_t id)
    : id_(id), pl_debugger_(NULL) {}

  virtual ~ObPLDebuggerGuard();

  int get(debugger::ObPLDebugger *& pl_debugger);

private:
  uint32_t id_;
  debugger::ObPLDebugger *pl_debugger_;
};

class ObPDBManager
{
public:
  ObPDBManager() : init_(false) {}
  static int build_instance();
  static ObPDBManager *get_instance();

  int alloc(pl::debugger::ObPLDebugger *& pl_debugger, sql::ObSQLSessionInfo *info);
  void free(pl::debugger::ObPLDebugger *pl_debugger);
  int get(uint32_t id, pl::debugger::ObPLDebugger *& pl_debugger);
  void release(pl::debugger::ObPLDebugger *pl_debugger);

  TO_STRING_KV(K_(init));

private:
  int init();
  int check(uint64_t tenant_id);

private:
  static ObPDBManager *instance_;
  static const int64_t MAX_DEBUGGER_MAP_SIZE = 128;
private:
  bool init_;
  common::ObSpinLock lock_;
  common::ObIDMap<debugger::ObPLDebugger, uint32_t> debuggers_;
  common::ObArenaAllocator allocator_;
};

}
}

#endif