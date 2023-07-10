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

#ifndef OCEANBASE_STORAGE_OB_TABLET_BINDING_REPLAY_EXECUTOR
#define OCEANBASE_STORAGE_OB_TABLET_BINDING_REPLAY_EXECUTOR

#include "common/ob_tablet_id.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"

namespace oceanbase
{

namespace storage
{

class ObTabletBindingReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletBindingReplayExecutor();

  int init(
      mds::BufferCtx &user_ctx,
      ObTabletBindingMdsUserData &user_data,
      const share::SCN &scn,
      const bool for_old_mds);

protected:
  bool is_replay_update_tablet_status_() const override
  {
     // TODO (jiahua.cjh): binding is pre barrier that doesn't
     // need call ObLS::replay_get_tablet. Consider refactor base execuator interface
    return true;
  }

  int do_replay_(ObTabletHandle &tablet_handle) override;

  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }

private:
  mds::BufferCtx *user_ctx_;
  ObTabletBindingMdsUserData *user_data_;
  share::SCN scn_;
  bool for_old_mds_;
};


}
}

#endif
