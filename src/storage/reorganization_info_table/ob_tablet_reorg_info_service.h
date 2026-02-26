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

#ifndef OCEABASE_STORAGE_TABLET_REORGANIZATION_INFO_SERVICE
#define OCEABASE_STORAGE_TABLET_REORGANIZATION_INFO_SERVICE

#include "lib/thread/thread_pool.h"
#include "lib/thread/ob_reentrant_thread.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/container/ob_se_array.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scn.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{

class ObTabletReorgInfoTableService final : public lib::ThreadPool
{
public:
  ObTabletReorgInfoTableService();
  ~ObTabletReorgInfoTableService();
  static int mtl_init(ObTabletReorgInfoTableService *&reorg_info_service);

  int init(ObLSService *ls_service);
  void destroy();
  void run1() final;
  void wakeup();
  void stop();
  void wait();
  int start();
private:
  int calc_max_recycle_scn_();
  int calc_ls_max_recycle_scn_(
      const share::ObLSID &ls_id);
  int check_can_recycle_(
      ObLS &ls,
      const ObTabletReorgInfoData &data,
      bool &can_recycle);
  int check_transfer_tablet_(
      ObLS &ls,
      const ObTabletReorgInfoData &data,
      bool &can_recycle);
  int check_transfer_in_tablet_finish_(
      ObLS &ls,
      const ObTabletReorgInfoData &data,
      const ObTransferDataValue &data_value,
      bool &is_finish);
  int check_transfer_out_tablet_finish_(
      ObLS &ls,
      const ObTabletReorgInfoData &data,
      const ObTransferDataValue &data_value,
      bool &is_finish);

private:
  static const int64_t SCHEDULER_WAIT_TIME_MS = 5 * 60 * 1000L; // 5min
  static const int64_t WAIT_SERVER_IN_SERVICE_TIME_MS = 1000; //1s

  bool is_inited_;
  common::ObThreadCond thread_cond_;
  int64_t wakeup_cnt_;
  ObLSService *ls_service_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletReorgInfoTableService);
};


}
}
#endif
