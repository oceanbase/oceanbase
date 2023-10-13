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

#ifndef OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER

#include <stdint.h>
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/hash/ob_hashset.h"
#include "common/ob_tablet_id.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/scn.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_mds_data_cache.h"

namespace oceanbase
{
namespace obrpc
{
struct ObBatchCreateTabletArg;
}

namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace storage
{
class ObTabletMapKey;
class ObTabletCreateSSTableParam;
class ObTableHandleV2;
class ObTablet;
class ObTabletCreateDeleteMdsUserData;

class ObTabletCreateDeleteHelper
{
public:
  static int get_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle,
      const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US);

  // snapshot version is used for multi source data reading,
  // tablet's multi source data will infect its visibility.
  // if snapshot version is MAX_TRANS_VERSION, it means we'll ignore
  // tablet creation/deletion transaction commit version,
  // and the tablet is fully visible as long as it really exists.
  static int check_and_get_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle,
      const int64_t timeout_us,
      const ObMDSGetTabletMode mode,
      const int64_t snapshot_version);
  static int check_status_for_new_mds(
      ObTablet &tablet,
      const int64_t snapshot_version,
      const int64_t timeout_us,
      ObTabletStatusCache &tablet_status_cache);
  static int check_read_snapshot_by_commit_version(
      ObTablet &tablet,
      const int64_t create_commit_version,
      const int64_t delete_commit_version,
      const int64_t snapshot_version,
      const ObTabletStatus &tablet_status);
  static int check_read_snapshot_for_normal(
      ObTablet &tablet,
      const int64_t snapshot_version,
      const int64_t timeout_us,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const bool is_committed);
  static int check_read_snapshot_for_deleted(
      ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const bool is_committed);
  static int check_read_snapshot_for_transfer_in(
      ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const bool is_committed);
  static int check_read_snapshot_for_transfer_out(
      ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const bool is_committed);
  static int check_read_snapshot_for_transfer_out_deleted(
      ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const bool is_committed);
  static int check_read_snapshot_by_commit_version(
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data);
  static int create_tmp_tablet(
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &handle);
  static int prepare_create_msd_tablet();
  static int create_msd_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle);
  static int acquire_msd_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle);
  static int acquire_tmp_tablet(
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &handle);
  static int acquire_tablet_from_pool(
      const ObTabletPoolType &type,
      const ObTabletMapKey &key,
      ObTabletHandle &handle);
  static int check_need_create_empty_major_sstable(
      const share::schema::ObTableSchema &table_schema,
      bool &need_create_sstable);
  static int build_create_sstable_param(
      const share::schema::ObTableSchema &table_schema,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      ObTabletCreateSSTableParam &param);
  static int create_sstable_for_migrate( // TODO: @jinzhu remove me later.
      const ObTabletCreateSSTableParam &param,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &table_handle);
  static int create_sstable(
      const ObTabletCreateSSTableParam &param,
      common::ObArenaAllocator &allocator,
      blocksstable::ObSSTable &sstable);
  // ObTabletBindingHelper usage
  static bool is_pure_data_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_mixed_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_pure_aux_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_pure_hidden_tablets(const obrpc::ObCreateTabletInfo &info);
  template<typename Arg, typename Helper>
  static int process_for_old_mds(
             const char *buf,
             const int64_t len,
             const transaction::ObMulSourceDataNotifyArg &notify_arg);
};

template<typename Arg, typename Helper>
int ObTabletCreateDeleteHelper::process_for_old_mds(
    const char *buf,
    const int64_t len,
    const transaction::ObMulSourceDataNotifyArg &notify_arg)
{
  int ret = OB_SUCCESS;
  Arg arg;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), KP(buf), K(len));
  } else {
    do {
      int64_t pos = 0;
      if (OB_FAIL(arg.deserialize(buf, len, pos))) {
        TRANS_LOG(WARN, "failed to deserialize", KR(ret), K(notify_arg), K(pos));
        if (notify_arg.for_replay_) {
          ret = OB_EAGAIN;
        } else {
          usleep(100 * 1000);
        }
      }
    } while (OB_FAIL(ret) && !notify_arg.for_replay_);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "arg is invalid", K(ret), K(arg));
  } else if (arg.is_old_mds_) {
    mds::MdsCtx mds_ctx;
    mds_ctx.set_binding_type_id(mds::TupleTypeIdx<mds::BufferCtxTupleHelper, mds::MdsCtx>::value);
    mds_ctx.set_writer(mds::MdsWriter(notify_arg.tx_id_));

    if (notify_arg.for_replay_) {
      if (OB_FAIL(Helper::replay_process(arg, notify_arg.scn_, mds_ctx))) {
        ret = OB_EAGAIN;
        TRANS_LOG(WARN, "failed to replay_process", K(ret), K(notify_arg), K(arg));
      }
    } else {
      do {
        if (OB_FAIL(Helper::register_process(arg, mds_ctx))) {
          TRANS_LOG(ERROR, "fail to register_process, retry", K(ret), K(arg), K(notify_arg));
          usleep(100 * 1000);
        }
      } while (OB_FAIL(ret));
    }

    if (OB_FAIL(ret)) {
    } else {
      mds_ctx.single_log_commit(notify_arg.trans_version_, notify_arg.scn_);
      TRANS_LOG(INFO, "replay create commit for old_mds", KR(ret), K(arg));
    }
  }
  return ret;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER
