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

#ifndef OCEANBASE_ROOTSERVER_OB_RS_REENTRANT_THREAD_
#define OCEANBASE_ROOTSERVER_OB_RS_REENTRANT_THREAD_

#include "lib/thread/ob_reentrant_thread.h"
#include "share/rc/ob_context.h"
namespace oceanbase {
namespace rootserver {

// can set Rootserver Thread properties,
// before real running and after running
class CheckThreadSet;
class ObRsReentrantThread : public share::ObReentrantThread {
public:
  ObRsReentrantThread();
  explicit ObRsReentrantThread(bool need_check);
  virtual ~ObRsReentrantThread();

  virtual void run2() override
  {
    int ret = common::OB_SUCCESS;
    CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, common::OB_RS_TENANT_ID)
    {
      run3();
    }
  }
  virtual void run3() = 0;

  // Set RS thread properties
  virtual int before_blocking_run() override
  {
    common::ObThreadFlags::set_rs_flag();
    return common::OB_SUCCESS;
  }

  virtual int after_blocking_run() override
  {
    common::ObThreadFlags::cancel_rs_flag();
    return common::OB_SUCCESS;
  }

  // check thread
  static CheckThreadSet check_thread_set_;
  static void check_alert(const ObRsReentrantThread& thread);

  virtual bool need_monitor_check() const;
  virtual int64_t get_schedule_interval() const
  {
    return -1;
  }

  int64_t get_last_run_timestamp() const;
  void update_last_run_timestamp();

  int create(const int64_t thread_cnt, const char* name = nullptr);
  int destroy();
  void stop() override;
  void wait() override;
  TO_STRING_KV("name", get_thread_name());

private:
  // >0 :last run timestamp;
  // =0 :pause check thread;
  // =-1 :close check thread;
  int64_t last_run_timestamp_;
#ifdef ERRSIM  // for obtest
  static const int64_t MAX_THREAD_SCHEDULE_OVERRUN_TIME = 5LL * 1000LL * 1000LL;
#else
  static const int64_t MAX_THREAD_SCHEDULE_OVERRUN_TIME = 10LL * 60LL * 1000LL * 1000LL;
#endif
};

class CheckThreadSet {
public:
  CheckThreadSet();
  virtual ~CheckThreadSet();
  void reset();
  int remove(ObRsReentrantThread* thread);
  int add(ObRsReentrantThread* thread);
  void loop_operation(void (*func)(const ObRsReentrantThread&));

private:
  static const int MAX_LEN = 16;
  const ObRsReentrantThread* arr_[MAX_LEN];
  common::SpinRWLock rwlock_;
  int len_;
};

}  // namespace rootserver
}  // namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_RS_REENTRANT_THREAD_
