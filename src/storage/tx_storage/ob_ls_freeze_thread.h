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

#ifndef OCEANBASE_STORAGE_OB_LS_FREEZE_THREAD_
#define OCEANBASE_STORAGE_OB_LS_FREEZE_THREAD_

#include "lib/lock/ob_spin_lock.h"
#include "lib/thread/thread_mgr_interface.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace lib;
using namespace common;
namespace storage
{
namespace checkpoint {
  class ObDataCheckpoint;
}

class ObLSFreezeThread;

// Traverse ls_frozen_list
class ObLSFreezeTask
{
public:
  void set_task(ObLSFreezeThread *host,
                checkpoint::ObDataCheckpoint *data_checkpoint,
                share::SCN rec_scn);

  void handle();

private:
  share::SCN rec_scn_;
  ObLSFreezeThread *host_;
  checkpoint::ObDataCheckpoint *data_checkpoint_;
};

class ObLSFreezeThread : public TGTaskHandler
{
  friend class ObLSFreezeTask;

public:
  static const int64_t QUEUE_THREAD_NUM = 3;
  static const int64_t MINI_MODE_QUEUE_THREAD_NUM = 1;
  static const int64_t MAX_FREE_TASK_NUM = 5;

  ObLSFreezeThread();
  virtual ~ObLSFreezeThread();

  int init(int64_t tenant_id, int tg_id);
  void destroy();

  int add_task(checkpoint::ObDataCheckpoint *data_checkpoint, share::SCN rec_scn);
  void handle(void *task);
  int get_tg_id() { return tg_id_; }

private:
  int push_back_(ObLSFreezeTask *task);

  bool inited_;
  int tg_id_;  // thread group id
  ObLSFreezeTask *task_array_[MAX_FREE_TASK_NUM];
  int64_t available_index_;
  ObSpinLock lock_;
};

}  // namespace storage
}  // namespace oceanbase
#endif
