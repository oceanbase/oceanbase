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

#include "storage/tablet/ob_tablet_create_mds_helper.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_batch_create_tablet_pretty_arg.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_create_replay_executor.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

#define USING_LOG_PREFIX MDS
#define PRETTY_ARG(arg) (ObBatchCreateTabletPrettyArg(arg))

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(EN_CREATE_TABLET_FAILED);

int ObTabletCreateMdsHelper::on_commit_for_old_mds(
    const char* buf,
    const int64_t len,
    const transaction::ObMulSourceDataNotifyArg &notify_arg)
{
  mds::TLOCAL_MDS_INFO.reset();// disable runtime check
  return ObTabletCreateDeleteHelper::process_for_old_mds<ObBatchCreateTabletArg, ObTabletCreateMdsHelper>(buf, len, notify_arg);
}

int ObTabletCreateMdsHelper::register_process(
    const ObBatchCreateTabletArg &arg,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  bool valid = false;
  common::ObSArray<ObTabletID> tablet_id_array;
  if (CLICK_FAIL(tablet_id_array.reserve(arg.get_tablet_count()))) {
    LOG_WARN("failed to reserve memory", K(ret), "capacity", arg.get_tablet_count());
  } else if (CLICK_FAIL(check_create_arg(arg, valid))) {
    LOG_WARN("failed to check tablet arg", K(ret), K(arg));
  } else if (OB_UNLIKELY(!valid)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, arg is not valid", K(ret), K(arg));
  } else if (CLICK_FAIL(create_tablets(arg, false/*for_replay*/, share::SCN::invalid_scn(), ctx, tablet_id_array))) {
    LOG_WARN("failed to create tablets", K(ret), K(arg));
  } else if (!arg.set_binding_info_outside_create() && CLICK_FAIL(ObTabletBindingHelper::modify_tablet_binding_for_new_mds_create(arg, SCN::invalid_scn(), ctx))) {
    LOG_WARN("failed to modify tablet binding", K(ret));
  }

  if (OB_FAIL(ret)) {
    // roll back
    int tmp_ret = OB_SUCCESS;
    if (CLICK_TMP_FAIL(rollback_remove_tablets(arg.id_, tablet_id_array))) {
      LOG_ERROR("failed to roll back remove tablets", K(tmp_ret));
      ob_usleep(1 * 1000 * 1000);
      ob_abort();
    }
  } else if (CLICK_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_gc_trigger(arg.id_))) {
    LOG_WARN("failed to set tablet gc trigger", K(ret));
  }
  LOG_INFO("create tablet register", KR(ret), "arg", PRETTY_ARG(arg));
  return ret;
}

int ObTabletCreateMdsHelper::on_register(
    const char* buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObBatchCreateTabletArg arg;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), "arg", PRETTY_ARG(arg));
  } else if (arg.is_old_mds_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, arg is old mds", K(ret), K(arg));
  } else if (OB_FAIL(convert_schemas(arg))) {
    LOG_WARN("failed to convert_schemas", K(ret), "arg", PRETTY_ARG(arg));
  } else if (CLICK_FAIL(check_create_new_tablets(arg, false/*is_replay*/))) {
    LOG_WARN("failed to check crate new tablets", K(ret), "arg", PRETTY_ARG(arg));
  } else if (CLICK_FAIL(register_process(arg, ctx))) {
    LOG_WARN("fail to register_process", K(ret), "arg", PRETTY_ARG(arg));
  }
  return ret;
}

int ObTabletCreateMdsHelper::replay_process(
    const ObBatchCreateTabletArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  common::ObSArray<ObTabletID> tablet_id_array;
  const ObLSID &ls_id = arg.id_;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  share::SCN tablet_change_checkpoint_scn;
  if (CLICK_FAIL(tablet_id_array.reserve(arg.get_tablet_count()))) {
    LOG_WARN("failed to reserve memory", K(ret), "capacity", arg.get_tablet_count());
  } else if (CLICK_FAIL(get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  } else if (FALSE_IT(tablet_change_checkpoint_scn = ls->get_tablet_change_checkpoint_scn())) {
  } else if (scn <= tablet_change_checkpoint_scn) {
    LOG_INFO("current scn is smaller than ls tablet change check point scn, log replaying can be skipped",
        K(ret), K(scn), K(tablet_change_checkpoint_scn));
  } else if (CLICK_FAIL(create_tablets(arg, true/*for_replay*/, scn, ctx, tablet_id_array))) {
    LOG_WARN("failed to create tablets", K(ret), K(arg), K(scn));
  } else if (!arg.set_binding_info_outside_create() && CLICK_FAIL(ObTabletBindingHelper::modify_tablet_binding_for_new_mds_create(arg, scn, ctx))) {
    LOG_WARN("failed to modify tablet binding", K(ret));
  } else if (CLICK_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_gc_trigger(ls_id))) {
    LOG_WARN("failed to trigger tablet gc task", K(ret));
  }

  if (CLICK_FAIL(ret)) {
    // roll back
    int tmp_ret = OB_SUCCESS;
    if (CLICK() && OB_TMP_FAIL(rollback_remove_tablets(arg.id_, tablet_id_array))) {
      LOG_ERROR("failed to roll back remove tablets", K(tmp_ret));
      ob_usleep(1 * 1000 * 1000);
      ob_abort();
    }
  }
  LOG_INFO("create tablet replay", KR(ret), K(scn), "arg", PRETTY_ARG(arg));
  return ret;
}

int ObTabletCreateMdsHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObBatchCreateTabletArg arg;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0) || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K(scn));
  } else if (CLICK_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), "arg", PRETTY_ARG(arg));
  } else if (arg.is_old_mds_) {
    LOG_INFO("skip replay create tablet for old mds", K(ret), K(scn), "arg", PRETTY_ARG(arg));
  } else if (OB_FAIL(convert_schemas(arg))) {
    LOG_WARN("failed to convert_schemas", K(ret), "arg", PRETTY_ARG(arg));
  } else if (CLICK_FAIL(check_create_new_tablets(arg, true/*is_replay*/))) {
    LOG_WARN("failed to check create new tablets", K(ret));
  } else if (CLICK_FAIL(replay_process(arg, scn, ctx))) {
    LOG_WARN("fail to replay_process", K(ret), "arg", PRETTY_ARG(arg));
  }

  if (OB_FAIL(ret)) {
    handle_ret_for_replay(ret);
  }

  return ret;
}

int ObTabletCreateMdsHelper::check_create_new_tablets(
  const int64_t inc_tablet_cnt,
  const ObTabletCreateThrottlingLevel level)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObUnitInfoGetter::ObTenantConfig unit;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  int64_t tablet_cnt_per_gb = 20000; // default value

  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get invalid tenant config", K(ret));
    } else {
      tablet_cnt_per_gb = tenant_config->_max_tablet_cnt_per_gb;
      switch (level) {
        case ObTabletCreateThrottlingLevel::SOFT:
          tablet_cnt_per_gb = MAX(tablet_cnt_per_gb, 30000);
          break;
        case ObTabletCreateThrottlingLevel::FREE:
          tablet_cnt_per_gb = MAX(tablet_cnt_per_gb, 40000);
          break;
        default:
          // do nothing
          break;
      }
    }
  }

  if (FAILEDx(GCTX.omt_->get_tenant_unit(tenant_id, unit))) {
    if (OB_TENANT_NOT_IN_SERVER != ret) {
      LOG_WARN("failed to get tenant unit", K(ret), K(tenant_id));
    } else {
      // during restart, tenant unit not ready, skip check
      ret = OB_SUCCESS;
    }
  } else {
    const double memory_limit = unit.config_.memory_size();
    const int64_t max_tablet_cnt = memory_limit / (1 << 30) * tablet_cnt_per_gb;
    const int64_t cur_tablet_cnt = t3m->get_total_tablet_cnt();

    if (OB_UNLIKELY(cur_tablet_cnt + inc_tablet_cnt > max_tablet_cnt)) {
      ret = OB_TOO_MANY_PARTITIONS_ERROR;
      LOG_WARN("too many partitions of tenant", K(ret), K(tenant_id), K(level), K(memory_limit), K(tablet_cnt_per_gb),
          K(max_tablet_cnt), K(cur_tablet_cnt), K(inc_tablet_cnt));
    }
  }
  return ret;
}

int ObTabletCreateMdsHelper::check_create_new_tablets(const obrpc::ObBatchCreateTabletArg &arg, const bool is_replay)
{
  int ret = OB_SUCCESS;
  bool skip_check = !arg.need_check_tablet_cnt_;
  bool is_truncate = false;
  // skip hidden tablet creation
  for (int64_t i = 0; OB_SUCC(ret) && !skip_check && i < arg.create_tablet_schemas_.count(); ++i) {
    if (OB_ISNULL(arg.create_tablet_schemas_[i])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(i), K(arg));
    } else if (arg.create_tablet_schemas_[i]->is_user_hidden_table()) {
      skip_check = true;
    } else if (OB_INVALID_VERSION != arg.create_tablet_schemas_[i]->get_truncate_version()) {
      is_truncate = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (skip_check) {
  } else if (is_truncate) {
    // for leader, we should use timeout from ddl request
    // for follower, quick failure to release replay thread and other resource like lock
    const int64_t timeout = is_replay ? 0 : THIS_WORKER.get_timeout_remain();
    const int64_t start_time = ObTimeUtility::fast_current_time();
    do {
      ret = check_create_new_tablets(arg.get_tablet_count(),
                  is_replay ? ObTabletCreateThrottlingLevel::FREE : ObTabletCreateThrottlingLevel::SOFT);
      if (OB_SUCC(ret)) {
        // do nothing
      } else if (OB_UNLIKELY(OB_TOO_MANY_PARTITIONS_ERROR == ret)) {
        if (ObTimeUtility::fast_current_time() - start_time >= timeout) {
          ret = OB_TIMEOUT;
          LOG_WARN("too many partitions, retry timeout", K(ret));
        } else {
          ob_usleep(1000 * 1000L); // sleep 1s
        }
      } else {
        LOG_WARN("fail to check create new tablets", K(ret));
      }
    } while (OB_TOO_MANY_PARTITIONS_ERROR == ret);
  } else if (OB_FAIL(check_create_new_tablets(arg.get_tablet_count(),
              is_replay ? ObTabletCreateThrottlingLevel::FREE : ObTabletCreateThrottlingLevel::STRICT))) {
    LOG_WARN("fail to create new tablets", K(ret));
  }
  return ret;
}

int ObTabletCreateMdsHelper::check_create_arg(
    const obrpc::ObBatchCreateTabletArg &arg,
    bool &valid)
{
  int ret = OB_SUCCESS;
  valid = true;
  const ObLSID &ls_id = arg.id_;
  int64_t aux_info_idx = -1;
  ObSArray<int64_t> aux_info_idx_array;

  for (int64_t i = 0; OB_SUCC(ret) && valid && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    const ObTabletID &data_tablet_id = info.data_tablet_id_;
    if (is_contain(aux_info_idx_array, i)) {
      // do nothing
    } else if (is_pure_data_tablets(info) || is_mixed_tablets(info)) {
      if (OB_FAIL(check_pure_data_or_mixed_tablets_info(ls_id, info, valid))) {
        LOG_WARN("failed to check create tablet info", K(ret), K(ls_id), K(info));
      }
    } else if (is_pure_aux_tablets(info)) {
      if (OB_FAIL(check_pure_aux_tablets_info(ls_id, info, valid))) {
        LOG_WARN("failed to check create tablet info", K(ret), K(ls_id), K(info));
      }
    } else if (is_bind_hidden_tablets(info)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < info.tablet_ids_.count(); ++i) {
        const ObTabletID &tablet_id = info.tablet_ids_.at(i);
        bool has_related_aux_info = find_aux_info_for_hidden_tablets(arg, tablet_id, aux_info_idx);
        if (has_related_aux_info) {
          const ObCreateTabletInfo &aux_info = arg.tablets_.at(aux_info_idx);
          if (OB_FAIL(check_hidden_tablets_info(ls_id, info, &aux_info, valid))) {
            LOG_WARN("failed to check create tablet info", K(ret), K(ls_id), K(info), K(aux_info));
          } else if (OB_FAIL(aux_info_idx_array.push_back(aux_info_idx))) {
            LOG_WARN("failed to push back aux info idx", K(ret));
          }
        } else if (OB_FAIL(check_hidden_tablets_info(ls_id, info, nullptr/*aux_info*/, valid))) {
          LOG_WARN("failed to check create tablet info", K(ret), K(ls_id), K(info));
        }
      }
    }
  }

  return ret;
}

int ObTabletCreateMdsHelper::create_tablets(
    const obrpc::ObBatchCreateTabletArg &arg,
    const bool for_replay,
    const share::SCN &scn,
    mds::BufferCtx &ctx,
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    if (is_pure_data_tablets(info)) {
      if (CLICK_FAIL(build_pure_data_tablet(arg, info, for_replay, scn, ctx, tablet_id_array))) {
        LOG_WARN("failed to build pure data tablet", K(ret), K(info));
      }
    } else if (is_mixed_tablets(info)) {
      if (CLICK_FAIL(build_mixed_tablets(arg, info, for_replay, scn, ctx, tablet_id_array))) {
        LOG_WARN("failed to build mixed tablets", K(ret), K(info));
      }
    } else if (is_pure_aux_tablets(info)) {
      if (CLICK_FAIL(build_pure_aux_tablets(arg, info, for_replay, scn, ctx, tablet_id_array))) {
        LOG_WARN("failed to build pure aux tablets", K(ret), K(info));
      }
    } else if (is_bind_hidden_tablets(info)) {
      if (CLICK_FAIL(build_bind_hidden_tablets(arg, info, for_replay, scn, ctx, tablet_id_array))) {
        LOG_WARN("failed to build hidden tablets", K(ret), K(info));
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_CREATE_TABLET_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("inject EN_CREATE_TABLET_FAILED", K(ret));
    }
  }
#endif

  return ret;
}

int ObTabletCreateMdsHelper::get_table_schema_index(
    const common::ObTabletID &tablet_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    int64_t &index)
{
  int ret = OB_SUCCESS;
  bool match = false;

  for (int64_t i = 0; !match && i < tablet_ids.count(); ++i) {
    if (tablet_ids.at(i) == tablet_id) {
      index = i;
      match = true;
    }
  }

  if (OB_UNLIKELY(!match)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot find target tablet id in array", K(ret), K(tablet_id));
  }

  return ret;
}

bool ObTabletCreateMdsHelper::is_pure_data_tablets(const obrpc::ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() == 1 && is_contain(tablet_ids, data_tablet_id) && !info.is_create_bind_hidden_tablets_;
}

bool ObTabletCreateMdsHelper::is_mixed_tablets(const obrpc::ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && is_contain(tablet_ids, data_tablet_id) && !info.is_create_bind_hidden_tablets_;
}

bool ObTabletCreateMdsHelper::is_pure_aux_tablets(const obrpc::ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && !is_contain(tablet_ids, data_tablet_id) && !info.is_create_bind_hidden_tablets_;
}

bool ObTabletCreateMdsHelper::is_bind_hidden_tablets(const obrpc::ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && !is_contain(tablet_ids, data_tablet_id) && info.is_create_bind_hidden_tablets_;
}

int ObTabletCreateMdsHelper::check_pure_data_or_mixed_tablets_info(
    const share::ObLSID &ls_id,
    const obrpc::ObCreateTabletInfo &info,
    bool &valid)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletMapKey key;
  key.ls_id_ = ls_id;

  for (int64_t i = 0; OB_SUCC(ret) && !exist && i < info.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = info.tablet_ids_[i];
    key.tablet_id_ = tablet_id;
    if (OB_FAIL(t3m->has_tablet(key, exist))) {
      LOG_WARN("failed to check tablet existence", K(ret), K(key));
    } else if (OB_UNLIKELY(exist)) {
      LOG_WARN("unexpected tablet existence", K(ret), K(key), K(exist));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    valid = !exist;
  }

  return ret;
}

int ObTabletCreateMdsHelper::check_pure_aux_tablets_info(
    const share::ObLSID &ls_id,
    const obrpc::ObCreateTabletInfo &info,
    bool &valid)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletMapKey key;
  key.ls_id_ = ls_id;

  for (int64_t i = 0; OB_SUCC(ret) && !exist && i < info.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = info.tablet_ids_[i];
    key.tablet_id_ = tablet_id;
    if (OB_FAIL(t3m->has_tablet(key, exist))) {
      LOG_WARN("failed to check tablet existence", K(ret), K(key));
    } else if (OB_UNLIKELY(exist)) {
      LOG_WARN("unexpected tablet existence", K(ret), K(key), K(exist));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(exist)) {
    valid = false;
  } else {
    key.tablet_id_ = info.data_tablet_id_;
    if (OB_FAIL(t3m->has_tablet(key, exist))) {
      LOG_WARN("failed to check tablet existence", K(ret), K(key));
    } else if (OB_UNLIKELY(!exist)) {
      ret = OB_ERR_PARALLEL_DDL_CONFLICT;
      LOG_WARN("data tablet does not exist, maybe transferred out", K(ret), K(key));
    } else {
      valid = true;
    }
  }

  return ret;
}

int ObTabletCreateMdsHelper::check_hidden_tablets_info(
    const share::ObLSID &ls_id,
    const obrpc::ObCreateTabletInfo &hidden_info,
    const obrpc::ObCreateTabletInfo *aux_info,
    bool &valid)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletMapKey key;
  key.ls_id_ = ls_id;

  for (int64_t i = 0; OB_SUCC(ret) && !exist && i < hidden_info.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = hidden_info.tablet_ids_[i];
    key.tablet_id_ = tablet_id;
    if (OB_FAIL(t3m->has_tablet(key, exist))) {
      LOG_WARN("failed to check tablet existence", K(ret), K(key));
    } else if (OB_UNLIKELY(exist)) {
      LOG_WARN("unexpected tablet existence", K(ret), K(key), K(exist));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(exist)) {
    valid = false;
  } else {
    key.tablet_id_ = hidden_info.data_tablet_id_;
    if (OB_FAIL(t3m->has_tablet(key, exist))) {
      LOG_WARN("failed to check tablet existence", K(ret), K(key));
    } else if (OB_UNLIKELY(!exist)) {
      valid = false;
      LOG_WARN("data tablet does not exist", K(ret), K(key));
    } else {
      valid = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!valid) {
  } else if (nullptr != aux_info) {
    exist = false;
    for (int64_t i = 0; OB_SUCC(ret) && !exist && i < aux_info->tablet_ids_.count(); ++i) {
      key.tablet_id_ = aux_info->tablet_ids_[i];
      if (OB_FAIL(t3m->has_tablet(key, exist))) {
        LOG_WARN("failed to check tablet existence", K(ret), K(key));
      } else if (OB_UNLIKELY(exist)) {
        LOG_WARN("unexpected tablet existence", K(ret), K(key), K(exist));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      valid = !exist;
    }
  }

  return ret;
}

bool ObTabletCreateMdsHelper::find_aux_info_for_hidden_tablets(
    const obrpc::ObBatchCreateTabletArg &arg,
    const common::ObTabletID &tablet_id,
    int64_t &aux_info_idx)
{
  bool found = false;

  for (int64_t i = 0; !found && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    if (is_pure_aux_tablets(info) && tablet_id == info.data_tablet_id_) {
      aux_info_idx = i;
      found = true;
    }
  }

  return found;
}

int ObTabletCreateMdsHelper::convert_schemas(
    obrpc::ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  if (arg.create_tablet_schemas_.count() > 0) {
  }
  // compatibility with DATA_VERSION_4_1_0_0
  else if (arg.tablets_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg));
  } else {
    lib::Worker::CompatMode compat_mode = arg.tablets_.at(0).compat_mode_;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.table_schemas_.count(); ++i) {
      ObTableSchema &table_schema = arg.table_schemas_[i];
      ObCreateTabletSchema *create_tablet_schema = NULL;
      void *create_tablet_schema_ptr = arg.allocator_.alloc(sizeof(ObCreateTabletSchema));
      if (OB_ISNULL(create_tablet_schema_ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate storage schema", KR(ret), K(table_schema));
      } else if (FALSE_IT(create_tablet_schema = new (create_tablet_schema_ptr)ObCreateTabletSchema())) {
      } else if (OB_FAIL(create_tablet_schema->init(arg.allocator_, table_schema, compat_mode,
           false/*skip_column_info*/, ObCreateTabletSchema::STORAGE_SCHEMA_VERSION_V3))) {
        LOG_WARN("failed to init storage schema", KR(ret), K(table_schema));
      } else if (OB_FAIL(arg.create_tablet_schemas_.push_back(create_tablet_schema))) {
        LOG_WARN("failed to push back table schema", KR(ret), K(create_tablet_schema));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // defensive check.
    if (OB_UNLIKELY(arg.tablet_extra_infos_.count() != arg.create_tablet_schemas_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, mismatched number", K(ret), K(arg));
    }
  }
  return ret;
}

int ObTabletCreateMdsHelper::check_and_get_create_tablet_schema_info(
    const ObSArray<ObCreateTabletSchema*> &create_tablet_schemas,
    const ObSArray<obrpc::ObCreateTabletExtraInfo> &create_tablet_extra_infos,
    const obrpc::ObCreateTabletInfo &info,
    const int64_t index,
    const ObCreateTabletSchema *&create_tablet_schema,
    bool &need_create_empty_major_sstable)
{
  int ret = OB_SUCCESS;
  create_tablet_schema = nullptr;
  need_create_empty_major_sstable = true;
  if (OB_UNLIKELY(create_tablet_extra_infos.count() != create_tablet_schemas.count()
        || index < 0
        || index >= info.table_schema_index_.count()
        || info.table_schema_index_[index] < 0
        || info.table_schema_index_[index] >= create_tablet_schemas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(index), K(info), "table_schema_index_cnt", info.table_schema_index_.count(),
      "info_index", info.table_schema_index_[index], K(create_tablet_schemas), K(create_tablet_extra_infos));
  } else if (OB_ISNULL(create_tablet_schema = create_tablet_schemas[info.table_schema_index_[index]])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(index), K(info), K(create_tablet_schemas));
  } else {
    const obrpc::ObCreateTabletExtraInfo &extra_info = create_tablet_extra_infos[info.table_schema_index_[index]];
    if (DATA_VERSION_4_3_0_0 <= extra_info.tenant_data_version_) {
      need_create_empty_major_sstable = extra_info.need_create_empty_major_;
      // TODO: @jinzhu, please remove me later, after hanxuan implement fts ddl task for post-creating index.
      if (create_tablet_schema->is_fts_index() && !create_tablet_schema->can_read_index()) {
        need_create_empty_major_sstable = true;
      }
    } else {
      need_create_empty_major_sstable =
        !(create_tablet_schema->is_user_hidden_table() || (create_tablet_schema->is_index_table() && !create_tablet_schema->can_read_index()));
    }
  }
  return ret;
}

int ObTabletCreateMdsHelper::build_pure_data_tablet(
    const obrpc::ObBatchCreateTabletArg &arg,
    const obrpc::ObCreateTabletInfo &info,
    const bool for_replay,
    const share::SCN &scn,
    mds::BufferCtx &ctx,
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  MDS_TG(5_ms);
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObCreateTabletSchema *create_tablet_schema = nullptr;
  const ObSArray<ObCreateTabletSchema*> &create_tablet_schemas = arg.create_tablet_schemas_;
  const ObSArray<obrpc::ObCreateTabletExtraInfo> &create_tablet_extra_infos = arg.tablet_extra_infos_;
  const lib::Worker::CompatMode &compat_mode = info.compat_mode_;
  const int64_t snapshot_version = arg.major_frozen_scn_.get_val_for_tx();
  ObTabletHandle tablet_handle;
  bool exist = false;
  int64_t index = -1;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool need_create_empty_major_sstable = true;

  if (for_replay) {
    const ObTabletMapKey key(ls_id, data_tablet_id);
    if (CLICK_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(data_tablet_id));
      }
    } else {
      exist = true;
    }
  }

  if (CLICK_FAIL(ret)) {
  } else if (for_replay && exist) {
    LOG_INFO("create pure data tablet is already exist, skip it", K(ret), K(for_replay), K(exist),
        K(ls_id), K(data_tablet_id));
  } else if (CLICK_FAIL(get_table_schema_index(data_tablet_id, info.tablet_ids_, index))) {
    LOG_WARN("failed to get table schema index", K(ret), K(ls_id), K(data_tablet_id));
  } else if (OB_UNLIKELY(index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema index is invalid", K(ret), K(ls_id), K(data_tablet_id), K(index));
  } else if (CLICK_FAIL(get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  } else if (CLICK_FAIL(tablet_id_array.push_back(data_tablet_id))) {
    LOG_WARN("failed to push back tablet id", K(ret), K(ls_id), K(data_tablet_id));
  } else if (OB_FAIL(check_and_get_create_tablet_schema_info(create_tablet_schemas, create_tablet_extra_infos, info, index,
      create_tablet_schema, need_create_empty_major_sstable))) {
    LOG_WARN("check and get create tablet schema_info failed", K(ret));
  } else if (CLICK_FAIL(ls->get_tablet_svr()->create_tablet(ls_id, data_tablet_id, data_tablet_id,
      scn, snapshot_version, *create_tablet_schema, compat_mode,
      need_create_empty_major_sstable, tablet_handle))) {
    LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(data_tablet_id), "arg", PRETTY_ARG(arg));
  }

  if (OB_FAIL(ret)) {
  } else if (CLICK_FAIL(set_tablet_normal_status(ls->get_tablet_svr(), tablet_handle, for_replay, scn, ctx, arg.is_old_mds_))) {
    LOG_WARN("failed to set tablet normal status", K(ret), K(ls_id), K(data_tablet_id));
  }

  return ret;
}

int ObTabletCreateMdsHelper::build_mixed_tablets(
    const obrpc::ObBatchCreateTabletArg &arg,
    const obrpc::ObCreateTabletInfo &info,
    const bool for_replay,
    const share::SCN &scn,
    mds::BufferCtx &ctx,
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  const ObSArray<ObCreateTabletSchema*> &create_tablet_schemas = arg.create_tablet_schemas_;
  const lib::Worker::CompatMode &compat_mode = info.compat_mode_;
  const ObSArray<obrpc::ObCreateTabletExtraInfo> &create_tablet_extra_infos = arg.tablet_extra_infos_;
  const int64_t snapshot_version = arg.major_frozen_scn_.get_val_for_tx();
  ObTabletHandle data_tablet_handle;
  ObTabletHandle tablet_handle;
  ObTabletID lob_meta_tablet_id;
  ObTabletID lob_piece_tablet_id;
  bool exist = false;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (CLICK_FAIL(get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    MDS_TG(5_ms);
    exist = false;
    const ObTabletID &tablet_id = tablet_ids[i];
    const ObCreateTabletSchema *create_tablet_schema = nullptr;
    bool need_create_empty_major_sstable = true;
    if (OB_FAIL(check_and_get_create_tablet_schema_info(create_tablet_schemas, create_tablet_extra_infos, info, i,
        create_tablet_schema, need_create_empty_major_sstable))) {
      LOG_WARN("check and get create tablet schema_info failed", K(ret));
    } else if (create_tablet_schema->is_aux_lob_meta_table()) {
      lob_meta_tablet_id = tablet_id;
    } else if (create_tablet_schema->is_aux_lob_piece_table()) {
      lob_piece_tablet_id = tablet_id;
    }

    if (OB_FAIL(ret)) {
    } else if (for_replay) {
      const ObTabletMapKey key(ls_id, tablet_id);
      if (CLICK_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(data_tablet_id), K(tablet_id));
        }
      } else {
        exist = true;
      }
    }


    if (OB_FAIL(ret)) {
    } else if (for_replay && exist) {
      LOG_INFO("tablet already exists in replay procedure, skip it", K(ret), K(ls_id), K(tablet_id));
    } else if (CLICK_FAIL(tablet_id_array.push_back(tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(ls_id), K(tablet_id));
    } else if (CLICK_FAIL(ls->get_tablet_svr()->create_tablet(ls_id, tablet_id, data_tablet_id,
        scn, snapshot_version, *create_tablet_schema, compat_mode,
        need_create_empty_major_sstable, tablet_handle))) {
      LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id), "arg", PRETTY_ARG(arg));
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(set_tablet_normal_status(ls->get_tablet_svr(), tablet_handle, for_replay, scn, ctx, arg.is_old_mds_))) {
      LOG_WARN("failed to set tablet normal status", K(ret), K(ls_id), K(tablet_id));
    }

    if (OB_FAIL(ret)) {
    } else if (tablet_id == data_tablet_id) {
      data_tablet_handle = tablet_handle;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (lob_meta_tablet_id.is_valid() || lob_piece_tablet_id.is_valid()) {
    // process lob meta/piece tablet
    ObTablet *data_tablet = data_tablet_handle.get_obj();
    if (OB_ISNULL(data_tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data tablet is null", K(ret), K(ls_id), K(data_tablet_id));
    } else {
      // binding info
    }
  }

  return ret;
}

int ObTabletCreateMdsHelper::build_pure_aux_tablets(
    const obrpc::ObBatchCreateTabletArg &arg,
    const obrpc::ObCreateTabletInfo &info,
    const bool for_replay,
    const share::SCN &scn,
    mds::BufferCtx &ctx,
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  const ObSArray<ObCreateTabletSchema*> &create_tablet_schemas = arg.create_tablet_schemas_;
  const lib::Worker::CompatMode &compat_mode = info.compat_mode_;
  const ObSArray<obrpc::ObCreateTabletExtraInfo> &create_tablet_extra_infos = arg.tablet_extra_infos_;
  const int64_t snapshot_version = arg.major_frozen_scn_.get_val_for_tx();
  ObTabletHandle tablet_handle;
  bool exist = false;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (CLICK_FAIL(get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    MDS_TG(5_ms);
    exist = false;
    const ObTabletID &tablet_id = tablet_ids[i];
    const ObCreateTabletSchema *create_tablet_schema = nullptr;
    bool need_create_empty_major_sstable = true;
    if (for_replay) {
      const ObTabletMapKey key(ls_id, tablet_id);
      if (CLICK_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(data_tablet_id));
        }
      } else {
        exist = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (for_replay && exist) {
      LOG_INFO("create pure aux tablet is already exist, skip it", K(ret), K(for_replay), K(exist),
          K(ls_id), K(data_tablet_id), K(tablet_id));
    } else if (CLICK_FAIL(tablet_id_array.push_back(tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(check_and_get_create_tablet_schema_info(create_tablet_schemas, create_tablet_extra_infos, info, i,
        create_tablet_schema, need_create_empty_major_sstable))) {
      LOG_WARN("check and get create tablet schema_info failed", K(ret));
    } else if (CLICK_FAIL(ls->get_tablet_svr()->create_tablet(ls_id, tablet_id, data_tablet_id,
        scn, snapshot_version, *create_tablet_schema, compat_mode,
        need_create_empty_major_sstable, tablet_handle))) {
      LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id), "arg", PRETTY_ARG(arg));
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(set_tablet_normal_status(ls->get_tablet_svr(), tablet_handle, for_replay, scn, ctx, arg.is_old_mds_))) {
      LOG_WARN("failed to set tablet normal status", K(ret), K(ls_id), K(tablet_id));
    }
  }

  // process lob meta/piece tablet

  return ret;
}

int ObTabletCreateMdsHelper::build_bind_hidden_tablets(
    const obrpc::ObBatchCreateTabletArg &arg,
    const obrpc::ObCreateTabletInfo &info,
    const bool for_replay,
    const share::SCN &scn,
    mds::BufferCtx &ctx,
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  const ObTabletID &orig_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  const ObSArray<ObCreateTabletSchema*> &create_tablet_schemas = arg.create_tablet_schemas_;
  const lib::Worker::CompatMode &compat_mode = info.compat_mode_;
  const ObSArray<obrpc::ObCreateTabletExtraInfo> &create_tablet_extra_infos = arg.tablet_extra_infos_;
  const int64_t snapshot_version = arg.major_frozen_scn_.get_val_for_tx();
  ObTabletHandle tablet_handle;
  int64_t aux_info_idx = -1;
  ObTabletID lob_meta_tablet_id;
  ObTabletID lob_piece_tablet_id;
  bool exist = false;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (CLICK_FAIL(get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    MDS_TG(5_ms);
    exist = false;
    lob_meta_tablet_id.reset();
    lob_piece_tablet_id.reset();
    const ObTabletID &tablet_id = tablet_ids[i];
    const ObCreateTabletSchema *create_tablet_schema = nullptr;
    bool has_related_aux_info = find_aux_info_for_hidden_tablets(arg, tablet_id, aux_info_idx);
    bool need_create_empty_major_sstable = true;
    if (OB_FAIL(check_and_get_create_tablet_schema_info(create_tablet_schemas, create_tablet_extra_infos, info, i,
        create_tablet_schema, need_create_empty_major_sstable))) {
      LOG_WARN("check and get create tablet schema_info failed", K(ret));
    } else if (has_related_aux_info) {
      const ObCreateTabletInfo &aux_info = arg.tablets_.at(aux_info_idx);
      for (int64_t j = 0; j < aux_info.tablet_ids_.count(); ++j) {
        const int64_t table_schema_index = aux_info.table_schema_index_.at(j);
        const ObCreateTabletSchema *aux_table_schema = create_tablet_schemas[table_schema_index];
        if (OB_ISNULL(aux_table_schema)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid args", K(ret), K(j));
        } else if (aux_table_schema->is_aux_lob_meta_table()) {
          lob_meta_tablet_id = aux_info.tablet_ids_.at(j);
        } else if (aux_table_schema->is_aux_lob_piece_table()) {
          lob_piece_tablet_id = aux_info.tablet_ids_.at(j);
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (for_replay) {
      const ObTabletMapKey key(ls_id, tablet_id);
      if (CLICK_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(orig_tablet_id), K(tablet_id));
        }
      } else {
        exist = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (for_replay && exist) {
      LOG_INFO("create hidden tablet is already exist, skip it", K(ret), K(for_replay), K(exist),
          K(ls_id), K(orig_tablet_id), K(tablet_id));
    } else if (CLICK_FAIL(tablet_id_array.push_back(tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(ls_id), K(tablet_id));
    } else if (CLICK_FAIL(ls->get_tablet_svr()->create_tablet(ls_id, tablet_id, tablet_id,
        scn, snapshot_version, *create_tablet_schema, compat_mode,
        need_create_empty_major_sstable, tablet_handle))) {
      LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(tablet_id), K(orig_tablet_id), "arg", PRETTY_ARG(arg));
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(set_tablet_normal_status(ls->get_tablet_svr(), tablet_handle, for_replay, scn, ctx, arg.is_old_mds_))) {
      LOG_WARN("failed to set tablet normal status", K(ret), K(ls_id), K(tablet_id));
    }

    // process lob meta/piece tablet
  }

  return ret;
}

int ObTabletCreateMdsHelper::rollback_remove_tablets(
    const share::ObLSID &ls_id,
    const common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const share::SCN transfer_start_scn(share::SCN::min_scn());

  if (CLICK_FAIL(get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
      MDS_TG(10_ms);
      const common::ObTabletID &tablet_id = tablet_id_array.at(i);
      if (CLICK_FAIL(ls->get_tablet_svr()->rollback_remove_tablet(ls_id, tablet_id, transfer_start_scn))) {
        LOG_WARN("failed to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObTabletCreateMdsHelper::get_ls(
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

int ObTabletCreateMdsHelper::set_tablet_normal_status(
    ObLSTabletService *ls_tablet_service,
    ObTabletHandle &tablet_handle,
    const bool for_replay,
    const share::SCN &scn,
    mds::BufferCtx &ctx,
    const bool for_old_mds)
{
  MDS_TG(5_ms);
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
  ObTabletCreateDeleteMdsUserData data(ObTabletStatus::NORMAL, ObTabletMdsUserDataType::CREATE_TABLET);

  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (OB_UNLIKELY(for_replay && !scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scn is invalid", K(ret),
        "ls_id", tablet->get_tablet_meta().ls_id_,
        "tablet_id", tablet->get_tablet_meta().tablet_id_,
        K(for_replay), K(scn));
  } else if (for_replay) {
    ObTabletCreateReplayExecutor replay_executor;
    const share::ObLSID &ls_id = tablet->get_tablet_meta().ls_id_;
    const common::ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    if (CLICK_FAIL(replay_executor.init(ctx, scn, for_old_mds))) {
      LOG_WARN("failed to init replay executor", K(ret));
    } else if (CLICK_FAIL(replay_executor.execute(scn, ls_id, tablet_id))) {
      LOG_WARN("failed to replay mds data", K(ret));
    }
  } else if (CLICK_FAIL(ls_tablet_service->set_tablet_status(tablet->get_tablet_meta().tablet_id_, data, user_ctx))) {
    LOG_WARN("failed to set mds data", K(ret));
  }

  return ret;
}

void ObTabletCreateMdsHelper::handle_ret_for_replay(int &ret)
{
  if (OB_TIMEOUT == ret || OB_TOO_MANY_PARTITIONS_ERROR == ret) {
    int origin_ret = ret;
    ret = OB_EAGAIN;
    LOG_INFO("rewrite failure to OB_EAGAIN to retry clog replay", K(ret), K(origin_ret));
  }
}
} // namespace storage
} // namespace oceanbase
