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

#ifndef OCEANBASE_STORAGE_OB_TABLET_CREATE_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_CREATE_MDS_HELPER

#include <stdint.h>
#include "lib/worker.h"
#include "lib/container/ob_iarray.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/ob_storage_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}

class ObLSID;
class SCN;
}

namespace common
{
class ObTabletID;
}

namespace obrpc
{
struct ObBatchCreateTabletArg;
struct ObCreateTabletInfo;
struct ObCreateTabletExtraInfo;
}

namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObLSHandle;
class ObTabletHandle;
class ObLSTabletService;

enum class ObTabletCreateThrottlingLevel : uint8_t
{
    STRICT = 0, // throttling by config like 1G2W, used in leader creation
    SOFT = 1,   // adaptive, could break config to 1G3W, used in HA scene
    FREE = 2,   // most free, 1G4W is the max creation speed without influcing stability
    MAX
};

class ObTabletCreateMdsHelper
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
  static int on_commit_for_old_mds(
    const char* buf,
    const int64_t len,
    const transaction::ObMulSourceDataNotifyArg &arg);
  static int register_process(
      const obrpc::ObBatchCreateTabletArg &arg,
      mds::BufferCtx &ctx);
  static int replay_process(
      const obrpc::ObBatchCreateTabletArg &arg,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
  static int check_create_new_tablets(const int64_t inc_tablet_cnt, const ObTabletCreateThrottlingLevel level);
private:
  static int check_create_new_tablets(const obrpc::ObBatchCreateTabletArg &arg, const bool is_replay = false);
  static int check_create_arg(
      const obrpc::ObBatchCreateTabletArg &arg,
      bool &valid);
  static int create_tablets(
    const obrpc::ObBatchCreateTabletArg &arg,
    const bool for_replay,
    const share::SCN &scn,
    mds::BufferCtx &ctx,
    common::ObIArray<common::ObTabletID> &tablet_id_array);
  static int get_table_schema_index(
      const common::ObTabletID &tablet_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      int64_t &index);
  static bool is_pure_data_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_mixed_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_pure_aux_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_bind_hidden_tablets(const obrpc::ObCreateTabletInfo &info);
  static int check_pure_data_or_mixed_tablets_info(
      const share::ObLSID &ls_id,
      const obrpc::ObCreateTabletInfo &info,
      bool &valid);
  static int check_pure_aux_tablets_info(
      const share::ObLSID &ls_id,
      const obrpc::ObCreateTabletInfo &info,
      bool &valid);
  static int check_hidden_tablets_info(
      const share::ObLSID &ls_id,
      const obrpc::ObCreateTabletInfo &hidden_info,
      const obrpc::ObCreateTabletInfo *aux_info,
      bool &valid);
  static bool find_aux_info_for_hidden_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const common::ObTabletID &tablet_id,
      int64_t &aux_info_idx);
  static int build_pure_data_tablet(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const bool for_replay,
      const share::SCN &scn,
      mds::BufferCtx &ctx,
      common::ObIArray<common::ObTabletID> &tablet_id_array);
  static int build_mixed_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const bool for_replay,
      const share::SCN &scn,
      mds::BufferCtx &ctx,
      common::ObIArray<common::ObTabletID> &tablet_id_array);
  static int build_pure_aux_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const bool for_replay,
      const share::SCN &scn,
      mds::BufferCtx &ctx,
      common::ObIArray<common::ObTabletID> &tablet_id_array);
  static int build_bind_hidden_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const bool for_replay,
      const share::SCN &scn,
      mds::BufferCtx &ctx,
      common::ObIArray<common::ObTabletID> &tablet_id_array);
  static int rollback_remove_tablets(
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &tablet_id_array);
  static int get_ls(
      const share::ObLSID &ls_id,
      ObLSHandle &ls_handle);
  static int set_tablet_normal_status(
      ObLSTabletService *ls_tablet_service,
      ObTabletHandle &tablet_handle,
      const bool for_replay,
      const share::SCN &scn,
      mds::BufferCtx &ctx,
      const bool for_old_mds);
  static void handle_ret_for_replay(int &ret);
  static int convert_schemas(obrpc::ObBatchCreateTabletArg &arg);
  static int check_and_get_create_tablet_schema_info(
      const ObSArray<ObCreateTabletSchema*> &create_tablet_schemas,
      const ObSArray<obrpc::ObCreateTabletExtraInfo> &create_tablet_extra_infos,
      const obrpc::ObCreateTabletInfo &info,
      const int64_t index,
      const ObCreateTabletSchema *&create_tablet_schema,
      bool &need_create_empty_major_sstable);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_MDS_HELPER
