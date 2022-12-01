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

#ifndef OCEABASE_STORAGE_TRANSFER_HANDLER_
#define OCEABASE_STORAGE_TRANSFER_HANDLER_

#include "ob_storage_ha_struct.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace storage
{

enum ObTransferStatus
{
  TRANSFER_NONE = 0,
  TRANSFER_DATA_COPY_ITERM1 = 1,
  TRANSFER_DATA_COPY_ITERM2 = 2,
  TRANFER_FAIL = 3,
  MAX,
};

struct ObTabletTransferInfo
{
  ObTabletTransferInfo();
  virtual ~ObTabletTransferInfo() = default;
  int update_snapshot_log_ts();
  void reset();
  bool is_valid() const;

  VIRTUAL_TO_STRING_KV(K_(tablet_id), K_(snapshot_log_ts), K_(status));

  common::ObTabletID tablet_id_;
  int64_t snapshot_log_ts_;
  ObTransferStatus status_;
};

struct ObTablesTransferTask
{
  ObTablesTransferTask();
  virtual ~ObTablesTransferTask();
  bool is_valid() const;
  void reset();
  VIRTUAL_TO_STRING_KV(K_(tablet_transfer_info_array), K_(src_ls_id), K_(dst_ls_id), K_(task_id));

  ObArray<ObTabletTransferInfo> tablet_transfer_info_array_;
  int result_;
  share::ObLSID src_ls_id_;
  share::ObLSID dst_ls_id_;
  share::ObTaskId task_id_;
};


class ObTransferHandler
{
public:
  ObTransferHandler();
  virtual ~ObTransferHandler();
  int init();
  virtual int process();
  int add_tablets();
  int add_tablets_for_relay();
  int check_task_exist(const share::ObTaskId &task_id, bool &is_exist);
private:
  int try_transfer_in_start_();
  int generate_tablet_task_();
  int create_tablets_();
  int scheduler_tablets_task_();
  int do_with_trans_status_(const ObTablesTransferTask &transfer_task);
  int do_data_copy_item1_();
  int do_data_copy_item2_();
  int change_transfer_status_();
  int get_tablets_effective_member_();
  int genreate_tablet_task_info_();
  int do_check_follower_finish_();
  int transfer_in_();
  int transfer_in_finish_();
  int transfer_in_abort_();
  int transfer_out_();
  int transfer_out_finish_();
private:
  bool is_inited_;
//  common::SpinRWLock lock_; not used yet
  ObSEArray<ObTablesTransferTask, 1> tablets_transfer_array_;
  DISALLOW_COPY_AND_ASSIGN(ObTransferHandler);
};


}
}
#endif
