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

#ifndef OB_DI_CACHE_H_
#define OB_DI_CACHE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "lib/ob_define.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_di_list.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class ObDISessionCollect : public ObDINode<ObDISessionCollect>
{
public:
  ObDISessionCollect();
  virtual ~ObDISessionCollect();
  void clean();
  TO_STRING_KV(K_(session_id), "di_session_info", (uint64_t)&base_value_, K(lock_.get_lock()));
  uint64_t session_id_;
  ObDiagnoseSessionInfo base_value_;
  DIRWLock lock_;
};

class ObDITenantCollect : public ObDINode<ObDITenantCollect>
{
public:
  ObDITenantCollect(ObIAllocator *allocator = NULL);
  virtual ~ObDITenantCollect();
  void clean();
  uint64_t tenant_id_;
  uint64_t last_access_time_;
  ObDiagnoseTenantInfo base_value_;
};

struct AddWaitEvent
{
  AddWaitEvent() {}
  void operator()(ObDiagnoseTenantInfo &base_info, const ObDiagnoseTenantInfo &info)
  {
    base_info.add_wait_event(info);
  }
};

struct AddStatEvent
{
  AddStatEvent() {}
  void operator()(ObDiagnoseTenantInfo &base_info, const ObDiagnoseTenantInfo &info)
  {
    base_info.add_stat_event(info);
  }
};

struct AddLatchStat
{
  AddLatchStat() {}
  void operator()(ObDiagnoseTenantInfo &base_info, const ObDiagnoseTenantInfo &info)
  {
    base_info.add_latch_stat(info);
  }
};

}// end namespace common
}
#endif /* OB_DI_CACHE_H_ */
