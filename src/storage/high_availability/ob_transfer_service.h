/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEABASE_STORAGE_TRANSFER_SERVICE_
#define OCEABASE_STORAGE_TRANSFER_SERVICE_

#include "lib/thread/thread_pool.h"
#include "lib/thread/ob_reentrant_thread.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/container/ob_se_array.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
class ObTransferService : public lib::ThreadPool
{
public:
  ObTransferService();
  virtual ~ObTransferService();
  static int mtl_init(ObTransferService *&transfer_service);

  int init(ObLSService *ls_service);
  void destroy();
  void run1() final;
  void wakeup();
  void stop();
  void wait();
  int start();

private:
  int get_ls_id_array_();
  int scheduler_transfer_handler_();
  int do_transfer_handler_(const share::ObLSID &ls_id);

private:
  static const int64_t SCHEDULER_WAIT_TIME_MS = 5 * 60 * 1000L; // 5min
  bool is_inited_;
  common::ObThreadCond thread_cond_;
  int64_t wakeup_cnt_;
  ObLSService *ls_service_;
  ObArray<share::ObLSID> ls_id_array_;
  DISALLOW_COPY_AND_ASSIGN(ObTransferService);
};



}
}
#endif
