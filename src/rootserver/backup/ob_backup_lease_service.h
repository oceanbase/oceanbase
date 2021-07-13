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

#ifndef SRC_ROOTSERVER_BACKUP_OB_BACKUP_LEASE_SERVICE_H_
#define SRC_ROOTSERVER_BACKUP_OB_BACKUP_LEASE_SERVICE_H_

#include "share/backup/ob_backup_lease_info_mgr.h"
#include "rootserver/ob_thread_idling.h"

namespace oceanbase {
namespace rootserver {
class ObRsReentrantThread;

class ObBackupLeaseService : public share::ObIBackupLeaseService, public lib::ThreadPool {
public:
  ObBackupLeaseService();
  virtual ~ObBackupLeaseService();

  int init(const common::ObAddr& addr, common::ObMySQLProxy& sql_proxy);
  int register_scheduler(ObRsReentrantThread& scheduler);
  int schedule_renew_task();
  // start_lease/stop_lease control lease service's status: is_stop/expect_round
  int start_lease();
  int stop_lease();
  void wait_lease();

  virtual int start() override;
  virtual void stop() override;
  void destroy();
  void wakeup();
  virtual int check_lease() override;
  virtual int get_lease_status(bool& is_lease_valid) override;
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(can_be_leader_ts), K_(expect_round), K_(lease_info));

private:
  int start_backup_scheduler_();
  void stop_backup_scheduler_();
  void wait_backup_scheduler_stop_();
  virtual void run1() final override;
  void do_idle(const int32_t result);
  int renew_lease_();
  int clean_backup_lease_info_(const int64_t next_round);
  int set_backup_lease_info_(const share::ObBackupLeaseInfo& lease_info, const char* msg);
  OB_INLINE bool can_be_leader_() const
  {
    return 0 != can_be_leader_ts_;
  }
  int check_sys_backup_info_();

  class ObBackupLeaseIdle : public rootserver::ObThreadIdling {
  public:
    static const int64_t DEFAULT_IDLE_US = 60 * 1000 * 1000;  // 60s
    static const int64_t FAST_IDLE_US = 1 * 1000 * 1000;      // 1s
    explicit ObBackupLeaseIdle(volatile bool& stop) : ObThreadIdling(stop)
    {}
    virtual ~ObBackupLeaseIdle()
    {}
    virtual int64_t get_idle_interval_us() override;
  };

private:
  bool is_inited_;
  int64_t can_be_leader_ts_;  // > 0 means can do backup scheduler
  int64_t expect_round_;      // usually, start/stop makes round+1. Or if rs epoch is changed, round will inc 1 also.
  common::SpinRWLock lock_;
  share::ObBackupLeaseInfoMgr backup_lease_info_mgr_;
  share::ObBackupLeaseInfo lease_info_;  // Only single thread will change lease_info_
  ObMySQLProxy* sql_proxy_;              // This is the sql proxy of the observer and is not controlled by rs stop
  common::ObSEArray<ObRsReentrantThread*, 8> backup_schedulers_;  // There are less than 8 at present, can be adjusted
                                                                  // larger as needed
  ObBackupLeaseIdle idle_;
  common::ObAddr local_addr_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLeaseService);
};

}  // namespace rootserver
}  // namespace oceanbase
#endif /* SRC_ROOTSERVER_BACKUP_OB_BACKUP_LEASE_SERVICE_H_ */
