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

#include "share/deadlock/ob_deadlock_detector_rpc.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"

namespace oceancase
{
namespace unittest
{

using namespace oceanbase::share::detector;

class MockDeadLockRpc : public oceanbase::share::detector::ObDeadLockDetectorRpc
{
public:
  int post_lcl_message(const ObAddr &dest_addr, const ObLCLMessage &lcl_msg) override
  {
    UNUSED(dest_addr);
    MTL(oceanbase::share::detector::ObDeadLockDetectorMgr*)->process_lcl_message(lcl_msg);
    return OB_SUCCESS;
  }
  int post_collect_info_message(const ObAddr &dest_addr,
                                const ObDeadLockCollectInfoMessage &collect_info_msg) override
  {
    UNUSED(dest_addr);
    MTL(oceanbase::share::detector::ObDeadLockDetectorMgr*)->process_collect_info_message(collect_info_msg);
    return OB_SUCCESS;
  }
  int post_notify_parent_message(const ObAddr &dest_addr,
                                 const ObDeadLockNotifyParentMessage &notify_msg) override
  {
    UNUSED(dest_addr);
    MTL(oceanbase::share::detector::ObDeadLockDetectorMgr*)->process_notify_parent_message(notify_msg);
    return OB_SUCCESS;
  }
};

}
}
