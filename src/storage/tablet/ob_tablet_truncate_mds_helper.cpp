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

#include <utility>
#include "storage/tablet/ob_tablet_truncate_mds_helper.h"
#include "storage/multi_data_source/ob_tablet_truncate_mds_ctx.h"
#include "storage/tablet/ob_tablet_truncate_mds_replay_executor.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/memtable/ob_memtable.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
OB_DEF_SERIALIZE_SIZE(ObTabletTruncateMdsArg)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, ls_id_, tablet_id_, truncate_data_, schema_version_, table_schema_);
  return len;
}

OB_DEF_SERIALIZE(ObTabletTruncateMdsArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              ls_id_,
              tablet_id_,
              truncate_data_,
              schema_version_,
              table_schema_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletTruncateMdsArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              ls_id_,
              tablet_id_,
              truncate_data_,
              schema_version_);
  if (FAILEDx(table_schema_.deserialize(allocator_, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize table schema", K(ret), K(data_len), K(pos));
  }
  return ret;
}

int ObTabletTruncateMdsArg::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const lib::Worker::CompatMode compat_mode,
    const share::schema::ObTableSchema &input_schema,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !tablet_id.is_valid()
                  || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(schema_version));
  } else if (OB_FAIL(table_schema_.init(allocator_,
                                        input_schema,
                                        compat_mode,
                                        false/*skip_column_info*/,
                                        DATA_CURRENT_VERSION))) {
    LOG_WARN("fail to assign table schema", K(ret), K(input_schema));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    schema_version_ = schema_version;
    truncate_data_.schema_version_ = input_schema.get_schema_version();
  }
  return ret;
}

int ObTabletTruncateMdsHelper::on_register(
    const char *buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTabletTruncateMdsArg arg;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else if (CLICK_FAIL(register_process(arg, ctx))) {
    LOG_WARN("fail to register_process", K(ret), K(arg));
  }
  return ret;
}

int ObTabletTruncateMdsHelper::on_replay(
    const char *buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTabletTruncateMdsArg arg;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0) || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K(scn));
  } else if (CLICK_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else if (CLICK_FAIL(replay_process(arg, scn, ctx))) {
    LOG_WARN("fail to replay_process", K(ret), K(arg), K(scn));
  }
  return ret;
}

int ObTabletTruncateMdsHelper::register_process(
    const ObTabletTruncateMdsArg &arg,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  mds::ObTabletTruncateMdsCtx &mds_ctx = static_cast<mds::ObTabletTruncateMdsCtx&>(ctx);

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid truncate arg", K(ret), K(arg));
  } else if (FALSE_IT(mds_ctx.set_ls_id(arg.ls_id_))) {
  } else if (FALSE_IT(mds_ctx.set_tablet_id(arg.tablet_id_))) {
  } else if (FALSE_IT(mds_ctx.set_nop(true/*nop*/))) {
  } else if (CLICK_FAIL(truncate_tablet_(
      arg.ls_id_, arg.tablet_id_, arg.table_schema_, arg.schema_version_, false/*for_replay*/,
      share::SCN::invalid_scn(), arg.truncate_data_, ctx))) {
    LOG_WARN("failed to truncate tablet", K(ret), K(arg));
  }
  LOG_INFO("[TRUNCATE TABLET] register", KR(ret), K(arg), K(ctx), K(&ctx));
  return ret;
}

int ObTabletTruncateMdsHelper::replay_process(
    const ObTabletTruncateMdsArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  mds::ObTabletTruncateMdsCtx &mds_ctx = static_cast<mds::ObTabletTruncateMdsCtx&>(ctx);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  share::SCN tablet_change_checkpoint_scn;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid truncate arg", K(ret), K(arg));
  } else if (FALSE_IT(mds_ctx.set_ls_id(arg.ls_id_))) {
  } else if (FALSE_IT(mds_ctx.set_tablet_id(arg.tablet_id_))) {
  } else if (FALSE_IT(mds_ctx.set_nop(true/*nop*/))) {
  } else if (CLICK_FAIL(get_ls_(arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(arg));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(arg));
  } else if (FALSE_IT(tablet_change_checkpoint_scn = ls->get_tablet_change_checkpoint_scn())) {
  } else if (scn <= tablet_change_checkpoint_scn) {
    LOG_INFO("[TRUNCATE TABLET] scn is smaller than tablet change checkpoint, skip replay",
        K(ret), K(scn), K(tablet_change_checkpoint_scn), K(arg));
  } else if (CLICK_FAIL(truncate_tablet_(
      arg.ls_id_, arg.tablet_id_, arg.table_schema_, arg.schema_version_, true/*for_replay*/, scn,
      arg.truncate_data_, ctx))) {
    LOG_WARN("failed to truncate tablet in replay", K(ret), K(arg), K(scn));
  }
  LOG_INFO("[TRUNCATE TABLET] replay", KR(ret), K(scn), K(arg), K(ctx), KP(&ctx));
  return ret;
}

int ObTabletTruncateMdsHelper::truncate_tablet_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const storage::ObCreateTabletSchema &table_schema,
    const int64_t schema_version,
    const bool for_replay,
    const share::SCN &scn,
    const ObTabletTruncateMdsUserData &truncate_data,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  mds::ObTabletTruncateMdsCtx &mds_ctx = static_cast<mds::ObTabletTruncateMdsCtx&>(ctx);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSTabletService *ls_tablet_service = nullptr;
  ObTabletHandle tablet_handle;
  bool need_skip = false;
  bool need_replay_mds_only = false;

  if (OB_FAIL(get_ls_(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet service is null", K(ret), K(ls_id));
  } else if (for_replay) {
    if (OB_FAIL(replay_get_tablet_(*ls, tablet_id, scn, tablet_handle, need_skip))) {
      LOG_WARN("failed to replay get tablet", K(ret), K(ls_id), K(tablet_id), K(scn));
    }
  } else {
    const ObTabletMapKey key(ls_id, tablet_id);
    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id));
    }
  }
  //   1. Disable CAS on tablet pointer
  //   2. Create new Full Tablet from old Tiny Tablet (empty Major SSTable)
  //   3. Set need_memtable_filter_after_truncate_tablet_ = true
  //   4. Put new tablet into t3m
  bool rollback_if_failed = false;
  if (OB_FAIL(ret)
      || need_skip) {
    // do nothing
  } else if (OB_FAIL(ls_tablet_service->start_tablet_truncate_mds(tablet_id, table_schema, schema_version, for_replay,
          scn, need_skip, need_replay_mds_only))) {
    if (for_replay && OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("[TRUNCATE TABLET] empty shell tablet should skip replay", K(ls_id), K(tablet_id), K(schema_version), K(scn));
    } else {
      LOG_WARN("ls_tablet_service failed to start tablet truncate mds", K(ret), K(tablet_id), K(schema_version), K(scn), K(for_replay));
    }
  } else if (need_skip) {
    LOG_INFO("skip covered tablet truncate replay after acquiring current tablet", K(tablet_id),
      K(schema_version), K(for_replay), K(scn));
  } else if (FALSE_IT(rollback_if_failed = !need_replay_mds_only)) {
    // pending truncate tablet has been acquired above; any failure below MUST rollback
  } else if (!need_replay_mds_only && FALSE_IT(mds_ctx.set_nop(false/*nop*/))) {
  } else if (OB_FAIL(set_tablet_truncate_mds_(ls_tablet_service,
                                              tablet_handle,
                                              for_replay,
                                              scn,
                                              truncate_data,
                                              ctx))) {
    LOG_WARN("failed to set tablet truncate mds", K(ret), K(ls_id), K(tablet_id));
  }
  if (OB_FAIL(ret) && rollback_if_failed) {
    int tmp_ret = OB_SUCCESS;
    // release pending truncate tablet
    if (OB_TMP_FAIL(ls_tablet_service->end_tablet_truncate_mds(tablet_id, false/*is_commit*/))) {
      LOG_ERROR("failed to rollback truncate tablet", K(ret), K(tmp_ret), K(tablet_id));
    } else {
      mds_ctx.set_nop(true/*nop*/);
    }
  }
  return ret;
}

int ObTabletTruncateMdsHelper::set_tablet_truncate_mds_(
    ObLSTabletService *ls_tablet_service,
    ObTabletHandle &tablet_handle,
    const bool for_replay,
    const share::SCN &scn,
    const ObTabletTruncateMdsUserData &truncate_data,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx &mds_ctx = static_cast<mds::MdsCtx&>(ctx);
  ObTablet *tablet = tablet_handle.get_obj();

  if (OB_ISNULL(tablet) || OB_ISNULL(ls_tablet_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet or ls_tablet_service is null", K(ret), KP(tablet), KP(ls_tablet_service));
  } else if (for_replay) {
    ObTabletTruncateMdsReplayExecutor replay_executor;
    if (OB_FAIL(replay_executor.init(ctx, scn, truncate_data))) {
      LOG_WARN("failed to init replay executor", K(ret), K(scn));
    } else if (OB_FAIL(replay_executor.execute(scn, tablet->get_ls_id(), tablet->get_tablet_id()))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to execute replay", K(ret), K(scn));
      }
    }
  } else if (OB_FAIL(ls_tablet_service->set_truncate_mds_data(tablet->get_tablet_id(),
                                                              truncate_data,
                                                              mds_ctx,
                                                              0/*lock_timeout_us*/))) {
    LOG_WARN("failed to set tablet truncate mds data", K(ret));
  }
  return ret;
}

int ObTabletTruncateMdsHelper::get_ls_(
    const share::ObLSID &ls_id,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService*);
  if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  }
  return ret;
}

int ObTabletTruncateMdsHelper::replay_get_tablet_(
    ObLS &ls,
    const common::ObTabletID &tablet_id,
    const share::SCN &scn,
    ObTabletHandle &tablet_handle,
    bool &skip)
{
  int ret = OB_SUCCESS;
  skip = false;
  const ObLSID ls_id = ls.get_ls_id();
  if (OB_UNLIKELY(!tablet_id.is_valid()) || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(scn));
  } else if (OB_FAIL(ls.replay_get_tablet_no_check(tablet_id,
                                                   scn,
                                                   true/*replay_allow_tablet_not_exist*/,
                                                   tablet_handle))) {
    if (OB_OBSOLETE_CLOG_NEED_SKIP == ret) {
      ret = OB_NO_NEED_UPDATE;
      LOG_INFO("[TRUNCATE TABLET] clog is obsolete, should skip replay",
        K(ret), K(ls_id), K(tablet_id), K(scn));
    } else if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("[TRUNCATE TABLET] tablet not exist yet during replay, will retry",
        K(ret), K(ls_id), K(tablet_id), K(scn));
    } else {
      LOG_WARN("failed to replay get tablet", K(ret), K(ls_id), K(tablet_id), K(scn));
    }
  } else {
    ObTablet *tablet = tablet_handle.get_obj();
    share::SCN truncate_commit_scn;
    int64_t unused_truncate_version = OB_INVALID_VERSION;
    if (OB_ISNULL(tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(ls_id), K(tablet_id));
    } else if (tablet->is_empty_shell()) {
      skip = true;
      LOG_INFO("[TRUNCATE TABLET] tablet is empty shell, should skip replay",
          K(ls_id), K(tablet_id), K(scn));
    } else if (OB_FAIL(tablet->get_tablet_truncate_scn_and_version(truncate_commit_scn,
                                                                   unused_truncate_version))) {
      LOG_WARN("failed to get tablet truncate scn and version",
          K(ret), K(ls_id), K(tablet_id));
    } else if (OB_UNLIKELY(!truncate_commit_scn.is_valid_and_not_min())) {
      // tablet has never been truncated, no need to skip
    } else if (truncate_commit_scn >= scn) {
      skip = true;
      LOG_INFO("[TRUNCATE TABLET] tablet truncate_commit_scn is already >= replay scn, skip",
          K(ls_id), K(tablet_id), K(scn), K(truncate_commit_scn));
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
