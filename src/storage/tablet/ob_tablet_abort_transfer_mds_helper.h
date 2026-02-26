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

#ifndef OCEANBASE_STORAGE_OB_TABLET_ABORT_TRANSFER_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_ABORT_TRANSFER_MDS_HELPER

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace share
{
class SCN;
class ObLSID;
struct ObTransferTabletInfo;
}
namespace storage
{

namespace mds
{
struct BufferCtx;
}

class ObLS;
class ObTablet;
class ObTabletHandle;
class ObTabletCreateDeleteMdsUserData;
class ObTXTransferInAbortedInfo;

class ObTabletAbortTransferHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      mds::BufferCtx &ctx);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
  static bool check_can_do_tx_end(
       const bool is_willing_to_commit,
       const bool for_replay,
       const share::SCN &log_scn,
       const char *buf,
       const int64_t buf_len,
       mds::BufferCtx &ctx,
       const char *&can_not_do_reason);
private:
  static int on_register_success_(
      const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
      mds::BufferCtx &ctx);
  static int on_replay_success_(
      const share::SCN &scn,
      const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
      mds::BufferCtx &ctx);
  static int check_transfer_in_tablet_aborted_(
      const share::SCN &scn,
      const bool for_replay,
      const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
      ObLS *ls);
  static int do_tx_end_before_commit_(
      const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
      const share::SCN &abort_redo_scn,
      const char *&can_not_do_reason);
  static int check_can_skip_replay_(
      const share::SCN &scn,
      const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
      bool &skip_replay);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletAbortTransferHelper);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_ABORT_TRANSFER_MDS_HELPER
