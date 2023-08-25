/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_SERVICE_H_
#define OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_SERVICE_H_

#include "logservice/ob_log_base_type.h"
#include "share/wr/ob_wr_task.h"

namespace oceanbase
{
namespace share
{

class ObWorkloadRepositoryService : public logservice::ObIReplaySubHandler,
                                    public logservice::ObIRoleChangeSubHandler,
                                    public logservice::ObICheckpointSubHandler
{
public:
  ObWorkloadRepositoryService();
  virtual ~ObWorkloadRepositoryService() {};
  DISABLE_COPY_ASSIGN(ObWorkloadRepositoryService);
  // used for ObIRoleChangeSubHandler
  virtual void switch_to_follower_forcedly() override final;
  virtual int switch_to_leader() override final;
  virtual int switch_to_follower_gracefully() override final;
  virtual int resume_leader() override final;
  // for replay, do nothing
  virtual int replay(const void *buffer,
                     const int64_t nbytes,
                     const palf::LSN &lsn,
                     const share::SCN &scn) override final;
  // for checkpoint, do nothing
  virtual share::SCN get_rec_scn() override final;
  virtual int flush(share::SCN &scn) override final;
  int cancel_current_task();
  int schedule_new_task(const int64_t interval);
  bool is_running_task() const {return wr_timer_task_.is_running_task();};
  int64_t get_snapshot_interval() const {return wr_timer_task_.get_snapshot_interval();};
  WorkloadRepositoryTask& get_wr_timer_task() {return wr_timer_task_;};
  INHERIT_TO_STRING_KV("ObIRoleChangeSubHandler", ObIRoleChangeSubHandler,
                        K_(is_inited),
                        K_(wr_timer_task));
  // used for ObServer class
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

private:
  int inner_switch_to_leader();
  int inner_switch_to_follower();
  bool is_inited_;
  WorkloadRepositoryTask wr_timer_task_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif //OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_SERVICE_H_
