/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
