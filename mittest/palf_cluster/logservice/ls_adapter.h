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

#ifndef OCEANBASE_PALF_CLUSTER_LS_ADAPTER_H_
#define OCEANBASE_PALF_CLUSTER_LS_ADAPTER_H_

#include <stdint.h>
#include "share/ob_ls_id.h"
#include "logservice/ob_ls_adapter.h"

namespace oceanbase
{
namespace logservice
{
class ObLogReplayTask;
};

namespace palfcluster
{

class MockLSAdapter : public logservice::ObLSAdapter
{
public:
  MockLSAdapter();
  ~MockLSAdapter();
  int init();
  void destroy();
public:
  int replay(logservice::ObLogReplayTask *replay_task) override final;
  int wait_append_sync(const share::ObLSID &ls_id) override final;
private:
  bool is_inited_;
};

} // logservice
} // oceanbase

#endif
