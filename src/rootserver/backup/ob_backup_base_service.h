// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//         http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_BASE_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_BASE_SERVICE_H_

#include "lib/thread/thread_mgr_interface.h"
#include "rootserver/ob_tenant_thread_helper.h"
#include "lib/lock/ob_thread_cond.h"
#include "logservice/ob_log_base_type.h"
#include "storage/ls/ob_ls.h"
namespace oceanbase
{

namespace rootserver
{

class ObBackupBaseService : public lib::TGRunnable,
                            public logservice::ObIReplaySubHandler,
                            public logservice::ObIRoleChangeSubHandler,
                            public logservice::ObICheckpointSubHandler
{
public:
  static const int64_t OB_MAX_IDLE_TIME = 60 * 1000 * 1000; // 1min
  static const int64_t OB_MIDDLE_IDLE_TIME = 30 * 1000 * 1000; // 30s
  static const int64_t OB_FALST_IDLE_TIME = 10 * 1000 * 1000; // 1s
public:
  ObBackupBaseService();
  virtual ~ObBackupBaseService();

public:
  int create(const char* thread_name, ObBackupBaseService &tenant_thread, int32_t event_no);
  virtual void run1() override final;
  virtual void run2() = 0;
  virtual void destroy();
  int start();
  void stop();
  void wait();
  void idle();
  void wakeup();
  void mtl_thread_stop();
  void mtl_thread_wait();
  void set_idle_time(const int64_t interval_time_us) { interval_idle_time_us_ = interval_time_us; }
  int64_t get_idle_time() const { return interval_idle_time_us_; }
  // role change
  int check_leader();
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;

  // backup service no need to use the above func
  virtual int replay(const void *buffer,
                     const int64_t nbytes,
                     const palf::LSN &lsn,
                     const share::SCN &scn) override final;
  virtual share::SCN get_rec_scn() override final { return share::SCN::max_scn(); }
  virtual int flush(share::SCN &scn) override final;
private:
  bool is_created_;
  int tg_id_;
  int64_t proposal_id_;
  int64_t wakeup_cnt_;
  int64_t interval_idle_time_us_;
  common::ObThreadCond thread_cond_;
  const char* thread_name_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBaseService);
};

}

}

#endif /* !OCEANBASE_ROOTSERVER_OB_BACKUP_BASE_SERVICE_H_ */