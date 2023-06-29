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

#ifndef OCEANBASE_STORAGE_OB_TABLET_DELETE_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_DELETE_MDS_HELPER

#include <stdint.h>
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace share
{
class SCN;
}

namespace obrpc
{
struct ObBatchRemoveTabletArg;
}

namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObTabletHandle;
class ObLSTabletService;

class ObTabletDeleteMdsHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      mds::BufferCtx &ctx);
  static int register_process(
      obrpc::ObBatchRemoveTabletArg &arg,
      mds::BufferCtx &ctx);
  static int on_commit_for_old_mds(
      const char* buf,
      const int64_t len,
      const transaction::ObMulSourceDataNotifyArg &notify_arg);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
  static int replay_process(
      obrpc::ObBatchRemoveTabletArg &arg,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
private:
  static int delete_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      mds::BufferCtx &ctx);
  static int replay_delete_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
  static int set_tablet_deleted_status(
    ObLSTabletService *ls_tablet_service,
    ObTabletHandle &tablet_handle,
    mds::BufferCtx &ctx);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_DELETE_MDS_HELPER
