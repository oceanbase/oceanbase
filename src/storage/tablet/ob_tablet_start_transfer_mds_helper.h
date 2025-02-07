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

#ifndef OCEANBASE_STORAGE_OB_TABLET_START_TRANSFER_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_START_TRANSFER_MDS_HELPER

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_tablet_id.h"
#include "storage/high_availability/ob_storage_ha_struct.h"

namespace oceanbase
{
namespace share
{
class SCN;
class ObLSID;
struct ObTransferTabletInfo;
class ObLSRestoreStatus;
}
namespace transaction {
enum class ObTxDataSourceType : int64_t;
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
class ObTXStartTransferOutInfo;
class ObTXStartTransferInInfo;
class ObMigrationTabletParam;

class ObTabletStartTransferOutCommonHelper
{
public:
  ObTabletStartTransferOutCommonHelper(transaction::ObTxDataSourceType &mds_op_type)
    : mds_op_type_(mds_op_type) {}
  ~ObTabletStartTransferOutCommonHelper() {}
  int update_tablets_transfer_out_(
      const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
      ObLS *ls,
      mds::BufferCtx &ctx);
  int update_tablet_transfer_out_(
      const share::ObLSID &dest_ls_id,
      const share::ObTransferTabletInfo &tablet_info,
      ObLS *ls,
      mds::BufferCtx &ctx);
  int set_transfer_tablets_freeze_flag_(const ObTXStartTransferOutInfo &tx_start_transfer_out_info);
  int try_enable_dest_ls_clog_replay(
      const share::SCN &scn,
      const share::ObLSID &dest_ls_id);
  int on_replay_success_(
      const share::SCN &scn,
      const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
      mds::BufferCtx &ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletStartTransferOutCommonHelper);
  transaction::ObTxDataSourceType &mds_op_type_;
};

class ObTabletStartTransferOutHelper
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
      const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
      mds::BufferCtx &ctx);
  static int prepare_src_transfer_tablets_(
      const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
      ObLS *ls);
  static int prepare_src_transfer_tablet_(
      const share::ObTransferTabletInfo &tablet_info,
      ObLS *ls);
  static int check_src_transfer_tablet_(
      const share::ObLSID &ls_id,
      const share::ObTransferTabletInfo &tablet_info,
      ObTablet *tablet);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletStartTransferOutHelper);
};

class ObTabletStartTransferOutPrepareHelper
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
};

class ObTabletStartTransferOutV2Helper
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
  DISALLOW_COPY_AND_ASSIGN(ObTabletStartTransferOutV2Helper);
};

class ObTabletStartTransferInHelper
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
  static bool check_can_replay_commit(
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
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      mds::BufferCtx &ctx);
  static int check_can_skip_replay_(
      const share::SCN &scn,
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      bool &skip_replay);
  static int check_transfer_dest_tablets_(
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      const bool for_replay);
  static int check_transfer_dest_tablet_(
      const ObMigrationTabletParam &tablet_meta,
      const bool for_replay,
      ObLS *dest_ls);
  // TODO:(muwei) donot pass parameter 'for_replay'
  static int check_transfer_src_tablets_(
      const share::SCN &scn,
      const bool for_replay,
      const ObTXStartTransferInInfo &tx_start_transfer_in_info);
  static int check_transfer_src_tablet_(
      const share::SCN &scn,
      const bool for_replay,
      const ObMigrationTabletParam &tablet_meta,
      ObLS *src_ls);
  static int create_transfer_in_tablets_(
      const share::SCN &scn,
      const bool for_replay,
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      mds::BufferCtx &ctx);
  static int create_transfer_in_tablet_(
      const share::SCN &scn,
      const bool for_replay,
      const ObMigrationTabletParam &tablet_meta,
      const share::ObLSID &src_ls_id,
      ObLS *dest_ls,
      mds::BufferCtx &ctx,
      common::ObIArray<common::ObTabletID> &tablet_id_array);
  static int rollback_transfer_in_tablets_(
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      ObLS *dest_ls);
  static int rollback_transfer_in_tablet_(
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_start_scn,
      ObLS *dest_ls);

  static int inner_create_transfer_in_tablet_(
    const share::SCN &scn,
    const bool for_replay,
    const ObMigrationTabletParam &tablet_meta,
    ObLS *dest_ls,
    ObTabletHandle &tablet_handle);

  static int on_replay_success_(
      const share::SCN &scn,
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      mds::BufferCtx &ctx);
  static int do_for_replay_(
      const share::SCN &scn,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const share::ObLSID &ls_id,
      const  share::SCN &transfer_start_scn,
      ObTabletHandle &tablet_handle,
      mds::BufferCtx &ctx);
  static int check_can_skip_check_transfer_src_tablet_(
      const share::SCN &scn,
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      bool &can_skip);
  static int check_gts_(
      const share::SCN &scn,
      bool &can_skip);
  static int check_transfer_dest_ls_status_(
      const share::SCN &scn,
      const share::ObLSID &ls_id,
      bool &can_skip);
  static int check_transfer_dest_ls_restore_status_(
      const share::SCN &scn,
      const share::ObLSID &ls_id,
      bool &can_skip);
  static int check_transfer_dest_tablets_ready_(
      const share::SCN &scn,
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      bool &can_skip);
  static int check_transfer_dest_tablet_ready_(
      const ObMigrationTabletParam &tablet_meta,
      bool &can_skip);
  static int set_dest_ls_rebuild_(
      const share::ObLSID &dest_ls_id,
      const share::SCN &scn,
      const bool for_replay);
  static int do_tx_end_before_commit_(
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      const share::SCN &scn,
      const bool for_replay,
      const char *&can_not_do_reason);
  static int do_tx_end_before_abort_(
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      const char *&can_not_do_reason);
  static int get_migration_and_restore_status_(
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      ObMigrationStatus &migration_status,
      share::ObLSRestoreStatus &ls_restore_status);
  static int check_can_replay_redo_log_(
      const ObTXStartTransferInInfo &tx_start_transfer_in_info,
      const share::SCN &scn,
      const ObMigrationStatus &migration_status,
      const share::ObLSRestoreStatus &ls_restore_status);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletStartTransferInHelper);
};


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_START_TRANSFER_MDS_HELPER
