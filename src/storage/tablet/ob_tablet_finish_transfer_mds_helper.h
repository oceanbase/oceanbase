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

#ifndef OCEANBASE_STORAGE_OB_TABLET_FINISH_TRANSFER_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_FINISH_TRANSFER_MDS_HELPER

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"
#include "src/storage/high_availability/ob_storage_ha_struct.h"

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
class ObTabletHandle;
struct ObTXFinishTransferInInfo;
struct ObTXFinishTransferOutInfo;

class ObTabletFinishTransferUtil
{
public:
  static int check_transfer_table_replaced(
      ObTabletHandle &tablet_handle,
      bool &all_replaced);
  static int can_skip_check_transfer_tablets(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id /* transfer dest ls */,
    const share::SCN &scn /* finish transfer in log scn */,
    bool &can_skip_check);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletFinishTransferUtil);
};

class ObTabletFinishTransferOutHelper
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
private:
  static int on_register_success_(
      const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
      mds::BufferCtx &ctx);
  static int check_transfer_out_tablets_validity_(
      const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
      storage::ObLS *ls);
  static int inner_check_transfer_out_tablet_validity_(
      const share::ObTransferTabletInfo &tablet_info,
      storage::ObLS *ls);
  static int update_transfer_tablets_deleted_(
      const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
      storage::ObLS *ls,
      mds::BufferCtx &ctx);
  static int update_transfer_tablet_deleted_(
      const share::ObTransferTabletInfo &tablet_info,
      storage::ObLS *ls,
      mds::BufferCtx &ctx);

  static int on_replay_success_(
      const share::SCN &scn,
      const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
      mds::BufferCtx &ctx);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletFinishTransferOutHelper);
};

class ObTabletFinishTransferInHelper
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
private:
  static int on_register_success_(
      const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
      mds::BufferCtx &ctx);
  static int check_ls_replay_scn_(
      const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
      storage::ObLS *ls);

  static int check_transfer_in_tablets_validity_(
      const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
      storage::ObLS *ls);
  static int inner_check_transfer_in_tablet_validity_(
      const share::ObTransferTabletInfo &tablet_info,
      storage::ObLS *ls);
  static int update_transfer_tablets_normal_(
      const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
      storage::ObLS *ls,
      mds::BufferCtx &ctx);
  static int update_transfer_tablet_normal_(
      const share::ObTransferTabletInfo &tablet_info,
      storage::ObLS *ls,
      mds::BufferCtx &ctx);
  static int on_replay_success_(
      const share::SCN &scn,
      const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
      mds::BufferCtx &ctx);
  static int can_skip_check_transfer_tablets_(
      ObLS &ls,
      bool &can_skip_check);


private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletFinishTransferInHelper);
};


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_FINISH_TRANSFER_MDS_HELPER
