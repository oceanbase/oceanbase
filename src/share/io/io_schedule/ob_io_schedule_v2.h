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

#ifndef OCEANBASE_IO_SCHEDULE_OB_IO_SCHEDULE_V2_H_
#define OCEANBASE_IO_SCHEDULE_OB_IO_SCHEDULE_V2_H_
#include "lib/lock/ob_drw_lock.h"
#include "share/io/ob_io_define.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
const int64_t STANDARD_IOPS_SIZE = 16 * (1<<10);
class QSchedCallback: public ITCHandler
{
public:
  QSchedCallback(): stop_submit_(false) {}
  virtual ~QSchedCallback() {}
  int start() { stop_submit_ = false; return OB_SUCCESS; }
  void set_stop() { stop_submit_ = true; }
  virtual int handle(TCRequest* req);
private:
  bool stop_submit_;
};

class ObIOManagerV2
{
public:
  enum { N_SUB_ROOT = 3 };
  ObIOManagerV2();
  ~ObIOManagerV2() {}
  int get_root_qid() { return root_qid_; }
  int get_sub_root_qid(int idx) {
    return (idx >= 0 && idx < N_SUB_ROOT)? sub_roots_[idx]: -1;
  }
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
private:
  QSchedCallback io_submitter_;
  int root_qid_;
  int sub_roots_[N_SUB_ROOT];
  int set_default_net_tc_limits();
};

extern ObIOManagerV2 g_io_mgr2;
#define OB_IO_MANAGER_V2 g_io_mgr2

class ObTenantIOSchedulerV2
{
public:
  enum { N_SUB_ROOT = ObIOManagerV2::N_SUB_ROOT };
  enum { GROUP_START_NUM = 3 * 3 };
  ObTenantIOSchedulerV2();
  ~ObTenantIOSchedulerV2() { destroy(); }
public:
  int init(const uint64_t tenant_id, const ObTenantIOConfig &io_config);
  void destroy();
  int update_config(const ObTenantIOConfig &io_config);
  int schedule_request(ObIORequest &req);
private:
  static int64_t get_qindex(ObIORequest& req);
  int get_qid(int64_t index, ObIORequest& req, bool& is_default_q);
private:
  DRWLock rwlock_;
  uint64_t tenant_id_;
  int top_qid_[N_SUB_ROOT];
  int default_qid_[N_SUB_ROOT];
  ObSEArray <int, GROUP_START_NUM> qid_;
};

}; // end namespace common
}; // end namespace oceanbase
#endif /* OCEANBASE_IO_SCHEDULE_OB_IO_SCHEDULE_V2_H_ */
