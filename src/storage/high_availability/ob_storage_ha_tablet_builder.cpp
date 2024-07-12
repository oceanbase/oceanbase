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

#define USING_LOG_PREFIX STORAGE
#include "ob_storage_ha_tablet_builder.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "share/scn.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/ob_storage_schema_util.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

/******************ObStorageHATabletsBuilderParam*********************/
ObStorageHATabletsBuilderParam::ObStorageHATabletsBuilderParam()
  : tenant_id_(OB_INVALID_ID),
    ls_(nullptr),
    tablet_id_array_(),
    src_info_(),
    local_rebuild_seq_(-1),
    need_check_seq_(false),
    is_leader_restore_(false),
    need_keep_old_tablet_(false),
    ha_table_info_mgr_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    restore_base_info_(nullptr),
    restore_action_(ObTabletRestoreAction::MAX),
    meta_index_store_(nullptr)
{
}

void ObStorageHATabletsBuilderParam::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_ = nullptr;
  tablet_id_array_.reset();
  src_info_.reset();
  local_rebuild_seq_ = -1;
  need_check_seq_ = false;
  is_leader_restore_ = false;
  need_keep_old_tablet_ = false;
  ha_table_info_mgr_ = nullptr;
  bandwidth_throttle_ = nullptr;
  svr_rpc_proxy_ = nullptr;
  storage_rpc_ = nullptr;
  restore_base_info_ = nullptr;
  restore_action_ = ObTabletRestoreAction::MAX;
  meta_index_store_ = nullptr;
}

bool ObStorageHATabletsBuilderParam::is_valid() const
{
  bool bool_ret = false;
  bool_ret = OB_INVALID_ID != tenant_id_
      && OB_NOT_NULL(ls_)
      && ((need_check_seq_ && local_rebuild_seq_ >= 0) || !need_check_seq_)
      && OB_NOT_NULL(ha_table_info_mgr_);
  if (bool_ret) {
    if (!is_leader_restore_) {
      bool_ret = src_info_.is_valid() && OB_NOT_NULL(bandwidth_throttle_)
          && OB_NOT_NULL(svr_rpc_proxy_) && OB_NOT_NULL(storage_rpc_);
    } else {
      bool_ret = OB_NOT_NULL(restore_base_info_)
         && ObTabletRestoreAction::is_valid(restore_action_)
         && OB_NOT_NULL(meta_index_store_);
    }
  }
  return bool_ret;
}

int ObStorageHATabletsBuilderParam::assign(const ObStorageHATabletsBuilderParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage ha tablets builder param is not valid", K(ret), K(param));
  } else if (OB_FAIL(tablet_id_array_.assign(param.tablet_id_array_))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(param));
  } else {
    tenant_id_ = param.tenant_id_;
    ls_ = param.ls_;
    src_info_ = param.src_info_;
    local_rebuild_seq_ = param.local_rebuild_seq_;
    need_check_seq_ = param.need_check_seq_;
    is_leader_restore_ = param.is_leader_restore_;
    need_keep_old_tablet_ = param.need_keep_old_tablet_;
    ha_table_info_mgr_ = param.ha_table_info_mgr_;
    bandwidth_throttle_ = param.bandwidth_throttle_;
    svr_rpc_proxy_ = param.svr_rpc_proxy_;
    storage_rpc_ = param.storage_rpc_;
    restore_base_info_ = param.restore_base_info_;
    restore_action_ = param.restore_action_;
    meta_index_store_ = param.meta_index_store_;
  }
  return ret;
}

/******************ObStorageHATabletsBuilder*********************/
ObStorageHATabletsBuilder::ObStorageHATabletsBuilder()
  : is_inited_(false),
    param_()
{
}

ObStorageHATabletsBuilder::~ObStorageHATabletsBuilder()
{
}

int ObStorageHATabletsBuilder::init(const ObStorageHATabletsBuilderParam &param)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUCKET_NUM = 1024;
  int64_t bucket_num = 0;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha tablets builder init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init storage ha tablets builder get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign storage ha tablets builder param", K(ret), K(param));
  } else if (FALSE_IT(bucket_num = std::max(MAX_BUCKET_NUM, param.tablet_id_array_.count()))) {
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStorageHATabletsBuilder::create_or_update_tablets()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObICopyTabletInfoReader *reader = nullptr;
  obrpc::ObCopyTabletInfo tablet_info;
  const int overwrite = 1;
  const bool need_check_tablet_limit = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (OB_ISNULL(ls = param_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(param_));
  } else if (OB_FAIL(get_tablet_info_reader_(reader))) {
    LOG_WARN("failed to get tablet info reader", K(ret), K(param_));
  } else {
    while (OB_SUCC(ret)) {
      tablet_info.reset();
      if (OB_FAIL(reader->fetch_tablet_info(tablet_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to fetch tablet info", K(ret));
        }
      } else if (OB_FAIL(modified_tablet_info_(tablet_info))) {
        LOG_WARN("failed to modified tablet info", K(ret), K(tablet_info));
      } else if (OB_FAIL(create_or_update_tablet_(tablet_info, need_check_tablet_limit, ls))) {
        LOG_WARN("failed to create or update tablet", K(ret), K(tablet_info));
      }
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        if (GCONF.errsim_migration_tablet_id == tablet_info.tablet_id_.id()) {
          SERVER_EVENT_SYNC_ADD("storage_ha", "after_migration_fetch_tablet_info",
                                "tablet_id", tablet_info.tablet_id_);
          DEBUG_SYNC(AFTER_MIGRATION_FETCH_TABLET_INFO);
        }
      }
#endif
    }
  }

  if (OB_NOT_NULL(reader)) {
    free_tablet_info_reader_(reader);
  }

  return ret;
}

int ObStorageHATabletsBuilder::create_all_tablets(
    const bool need_check_tablet_limit,
    ObICopyLSViewInfoReader *reader,
    common::ObIArray<common::ObTabletID> &sys_tablet_id_list,
    common::ObIArray<common::ObTabletID> &data_tablet_id_list,
    CopyTabletSimpleInfoMap &simple_info_map)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  obrpc::ObCopyTabletInfo tablet_info;
  ObCopyTabletSimpleInfo tablet_simple_info;
  const int overwrite = 1;
  sys_tablet_id_list.reset();
  data_tablet_id_list.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create all tablets get invalid argument", K(ret), KP(reader));
  } else if (OB_ISNULL(ls = param_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(param_));
  } else {
    static const int64_t CREATE_TABLETS_WARN_THRESHOLD = 60 * 1000 * 1000; //60s
    common::ObTimeGuard timeguard("tablets_builder_create_all_tablets", CREATE_TABLETS_WARN_THRESHOLD);
    while (OB_SUCC(ret)) {
      tablet_info.reset();
      tablet_simple_info.reset();
      if (OB_FAIL(reader->get_next_tablet_info(tablet_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to fetch tablet info", K(ret));
        }
      } else if (OB_FAIL(modified_tablet_info_(tablet_info))) {
        LOG_WARN("failed to modified tablet info", K(ret), K(tablet_info));
      } else if (OB_FAIL(create_or_update_tablet_(tablet_info, need_check_tablet_limit, ls))) {
        LOG_WARN("failed to create or update tablet", K(ret), K(tablet_info));
      } else if (tablet_info.tablet_id_.is_ls_inner_tablet()) {
        if (OB_FAIL(sys_tablet_id_list.push_back(tablet_info.tablet_id_))) {
          LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_info));
        }
      } else {
        if (OB_FAIL(data_tablet_id_list.push_back(tablet_info.tablet_id_))) {
          LOG_WARN("failed to push tablet id into data tablet id list", K(ret), K(tablet_info));
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        tablet_simple_info.tablet_id_ = tablet_info.tablet_id_;
        tablet_simple_info.status_ = tablet_info.status_;
        tablet_simple_info.data_size_ = tablet_info.data_size_;
        if (OB_FAIL(simple_info_map.set_refactored(tablet_info.tablet_id_, tablet_simple_info, overwrite))) {
          LOG_WARN("failed to set tablet status info into map", K(ret), K(tablet_simple_info), K(tablet_info));
        }
      }
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        if (GCONF.errsim_migration_tablet_id == tablet_info.tablet_id_.id()) {
          SERVER_EVENT_SYNC_ADD("storage_ha", "after_migration_fetch_tablet_info",
                                "tablet_id", tablet_info.tablet_id_);
          DEBUG_SYNC(AFTER_MIGRATION_FETCH_TABLET_INFO);
        }
      }
#endif
    }
    LOG_INFO("create all tablets finish", K(ret), "sys_tablet_count", sys_tablet_id_list.count(),
                                                  "data_tablet_count", data_tablet_id_list.count());
  }
  return ret;
}

int ObStorageHATabletsBuilder::create_all_tablets_with_4_1_rpc(
    CopyTabletSimpleInfoMap &simple_info_map)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObICopyTabletInfoReader *reader = nullptr;
  obrpc::ObCopyTabletInfo tablet_info;
  const int overwrite = 1;
  ObCopyTabletSimpleInfo tablet_simple_info;
  const bool need_check_tablet_limit = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (OB_ISNULL(ls = param_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(param_));
  } else if (OB_FAIL(get_tablet_info_reader_(reader))) {
    LOG_WARN("failed to get tablet info reader", K(ret), K(param_));
  } else {
    while (OB_SUCC(ret)) {
      tablet_info.reset();
      if (OB_FAIL(reader->fetch_tablet_info(tablet_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to fetch tablet info", K(ret));
        }
      } else if (OB_FAIL(modified_tablet_info_(tablet_info))) {
        LOG_WARN("failed to modified tablet info", K(ret), K(tablet_info));
      } else if (OB_FAIL(create_or_update_tablet_(tablet_info, need_check_tablet_limit, ls))) {
        LOG_WARN("failed to create or update tablet", K(ret), K(tablet_info));
      } else {
        tablet_simple_info.tablet_id_ = tablet_info.tablet_id_;
        tablet_simple_info.status_ = tablet_info.status_;
        tablet_simple_info.data_size_ = tablet_info.data_size_;
        if (OB_FAIL(simple_info_map.set_refactored(tablet_info.tablet_id_, tablet_simple_info, overwrite))) {
          LOG_WARN("failed to set tablet status info into map", K(ret), K(tablet_simple_info), K(tablet_info));
        }
      }
    }
  }

  if (OB_NOT_NULL(reader)) {
    free_tablet_info_reader_(reader);
  }
  return ret;
}

int ObStorageHATabletsBuilder::update_pending_tablets_with_remote()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObICopyTabletInfoReader *reader = nullptr;
  const bool need_check_tablet_limit = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (OB_ISNULL(ls = param_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(param_));
  } else if (OB_FAIL(get_tablet_info_reader_(reader))) {
    LOG_WARN("failed to get tablet info reader", K(ret), K(param_));
  } else {
    obrpc::ObCopyTabletInfo tablet_info;
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    while (OB_SUCC(ret)) {
      tablet_info.reset();
      if (OB_FAIL(reader->fetch_tablet_info(tablet_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to fetch tablet info", K(ret));
        }
        break;
      }

      const ObTabletID tablet_id = tablet_info.tablet_id_;
      if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          LOG_INFO("tablet is not exist", K(tablet_id));
          ret = OB_SUCCESS;
          continue;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id));
      } else if (!tablet->get_tablet_meta().ha_status_.is_restore_status_pending()) {
        // has been renewed before, skip.
        LOG_INFO("local tablet is not PENDING, skip it", K(tablet_id),
          "ha_status", tablet->get_tablet_meta().ha_status_);
        continue;
      } else if (ObCopyTabletStatus::TABLET_EXIST == tablet_info.status_) {
        if (tablet_info.param_.ha_status_.is_restore_status_pending()) {
          // This may happen when leader switch. The old leader sent the restored
          // tablet id to follower. Then, the follower will try to restore these meta.
          // However, the meta from new leader is still PENDING.
          ret = OB_TABLET_NOT_EXIST;
          LOG_WARN("remote tablet is PENDING", K(ret), K(tablet_info));
        } else if (tablet->get_tablet_meta().transfer_info_.transfer_seq_ != tablet_info.param_.transfer_info_.transfer_seq_) {
          // If remote tablet's transfer sequence is not equal with
          // local tablet, it is also considered as same as the indicated
          // tablet is not exist.
          LOG_INFO("transfer sequence not equal, reset tablet not exist", K(tablet_id),
            "remote_transfer_info", tablet_info.param_.transfer_info_,
            "local_transfer_info", tablet->get_tablet_meta().transfer_info_);
          tablet_info.status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
        }
      }


      if (OB_FAIL(ret)) {
      } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == tablet_info.status_) {
        // If remote tablet is not exist, update local tablet from PENDING to
        // UNDEFINED.
        if (OB_FAIL(ls->update_tablet_restore_status(tablet_info.tablet_id_,
                                                     ObTabletRestoreStatus::STATUS::UNDEFINED,
                                                     true/* need reset transfer flag */))) {
          LOG_WARN("failed to update tablet restore status to UNDEFINED", K(ret), K(tablet_info));
        } else {
          LOG_INFO("update tablet restore status to UNDEFINED", K(tablet_info));
        }
      } else if (OB_FAIL(create_or_update_tablet_(tablet_info, need_check_tablet_limit, ls))) {
        LOG_WARN("failed to create or update tablet", K(ret), K(tablet_info));
      } else {
        LOG_INFO("success to replace PENDING tablet with a newer meta", K(tablet_id));
      }
    }
  }

  if (OB_NOT_NULL(reader)) {
    free_tablet_info_reader_(reader);
  }

  return ret;
}

int ObStorageHATabletsBuilder::get_tablet_info_reader_(
    ObICopyTabletInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  void *buf = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets buidler do not init", K(ret));
  } else if (param_.is_leader_restore_) {
    if (OB_FAIL(get_tablet_info_restore_reader_(reader))) {
      LOG_WARN("failed to get tablet info restore reader", K(ret), K(param_));
    }
  } else {
    if (OB_FAIL(get_tablet_info_ob_reader_(reader))) {
      LOG_WARN("failed to get tablet info ob reader", K(ret), K(param_));
    }
  }
  return ret;
}

int ObStorageHATabletsBuilder::get_tablet_info_restore_reader_(ObICopyTabletInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  void *buf = nullptr;
  ObCopyTabletInfoRestoreReader *restore_reader = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!param_.is_leader_restore_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tablet info restore reader get invalid argument", K(ret), K(param_));
  } else if (FALSE_IT(buf = ob_malloc(sizeof(ObCopyTabletInfoRestoreReader), "TabletReader"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(restore_reader = new (buf) ObCopyTabletInfoRestoreReader())) {
  } else if (FALSE_IT(reader = restore_reader)) {
  } else if (OB_FAIL(restore_reader->init(*param_.restore_base_info_, param_.tablet_id_array_, *param_.meta_index_store_))) {
    LOG_WARN("failed to init tablet restore reader", K(ret), K(param_));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(reader)) {
      free_tablet_info_reader_(reader);
    }
  }
  return ret;
}

int ObStorageHATabletsBuilder::get_tablet_info_ob_reader_(
    ObICopyTabletInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  void *buf = nullptr;
  obrpc::ObCopyTabletInfoArg arg;
  ObCopyTabletInfoObReader *ob_reader = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage tablets restore task do not init", K(ret));
  } else if (param_.is_leader_restore_ || !param_.need_check_seq_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get follower tablet info reader get invalid argument", K(ret), K(param_));
  } else if (FALSE_IT(buf = ob_malloc(sizeof(ObCopyTabletInfoObReader), "TabletObReader"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(ob_reader = new (buf) ObCopyTabletInfoObReader())) {
  } else if (FALSE_IT(reader = ob_reader)) {
  } else if (OB_FAIL(arg.tablet_id_list_.assign(param_.tablet_id_array_))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(param_));
  } else if (OB_FAIL(ObStorageHAUtils::get_server_version(arg.version_))) {
    LOG_WARN("failed to get server version", K(ret), K_(param));
  } else {
    arg.tenant_id_ = param_.tenant_id_;
    arg.ls_rebuild_seq_ = param_.local_rebuild_seq_;
    arg.ls_id_ = param_.ls_->get_ls_id();
    arg.need_check_seq_ = param_.need_check_seq_;
    if (OB_FAIL(ob_reader->init(param_.src_info_, arg, *param_.svr_rpc_proxy_, *param_.bandwidth_throttle_))) {
      LOG_WARN("failed to init copy tablet info ob reader", K(ret), K(param_));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(reader)) {
      free_tablet_info_reader_(reader);
    }
  }

  return ret;
}

void ObStorageHATabletsBuilder::free_tablet_info_reader_(ObICopyTabletInfoReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    reader->~ObICopyTabletInfoReader();
    ob_free(reader);
    reader = nullptr;
  }
}

int ObStorageHATabletsBuilder::create_or_update_tablet_(
    const obrpc::ObCopyTabletInfo &tablet_info,
    const bool need_check_tablet_limit,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  const bool keep_old = param_.need_keep_old_tablet_;
  ObArenaAllocator allocator("HATabBuilder");
  ObTabletHandle local_tablet_hdl;
  ObTablesHandleArray major_tables;
  ObBatchUpdateTableStoreParam param;
  ObStorageSchema storage_schema;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!tablet_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create or update tablet get invalid argument", K(ret), K(tablet_info), KP(ls));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == tablet_info.status_ && tablet_info.tablet_id_.is_ls_inner_tablet()) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("src ls inner tablet is not exist, src ls is maybe deleted", K(ret), K(tablet_info));
  } else if (need_check_tablet_limit && OB_FAIL(ObTabletCreateMdsHelper::check_create_new_tablets(1LL, ObTabletCreateThrottlingLevel::SOFT))) {
    if (OB_TOO_MANY_PARTITIONS_ERROR == ret) {
      LOG_ERROR("too many partitions, failed to check create new tablet", K(ret), K(tablet_info));
    } else {
      LOG_WARN("failed to check create new tablet", K(ret), K(tablet_info));
    }
  } else if (OB_FAIL(hold_local_reuse_sstable_(tablet_info.tablet_id_, local_tablet_hdl, major_tables, storage_schema, allocator))) {
    LOG_WARN("failed to hold local reuse sstable", K(ret), K(tablet_info));
  } else if (OB_FAIL(ls->rebuild_create_tablet(tablet_info.param_, keep_old))) {
    LOG_WARN("failed to create or update tablet", K(ret), K(tablet_info));
  } else if (tablet_info.param_.is_empty_shell() || tablet_info.param_.ha_status_.is_restore_status_undefined()) {
    // empty shell or UNDEFINED tablet does not need to reuse any sstable.
  } else {
    if (tablet_info.param_.transfer_info_.has_transfer_table_) {
      //do nothing
    } else if (OB_FAIL(create_tablet_with_major_sstables_(ls, tablet_info, major_tables, storage_schema))) {
      LOG_WARN("failed to create tablet with major sstables", K(ret), KPC(ls), K(tablet_info), K(major_tables));
    } else {
      LOG_INFO("succeed build ha table new table store", K(tablet_info), K(major_tables));
    }
  }
  return ret;
}

int ObStorageHATabletsBuilder::build_tablets_sstable_info()
{
  int ret = OB_SUCCESS;
  ObICopySSTableInfoReader *reader = nullptr;
  obrpc::ObCopyTabletSSTableInfo sstable_info;
  obrpc::ObCopyTabletSSTableHeader copy_header;
  ObLS *ls = nullptr;
  ObArray<ObTabletHandle> tablet_handle_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (OB_ISNULL(ls = param_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(hold_local_tablet_(tablet_handle_array))) {
    LOG_WARN("failed to hold local tablet", K(ret), KP(ls));
  } else if (OB_FAIL(get_tablets_sstable_reader_(tablet_handle_array, reader))) {
    LOG_WARN("failed to get tablets sstable reader", K(ret), K(param_));
  } else {
    while (OB_SUCC(ret)) {
      sstable_info.reset();
      copy_header.reset();

      if (OB_FAIL(reader->get_next_tablet_sstable_header(copy_header))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next tablet sstable header", K(ret), K(param_));
        }
      } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == copy_header.status_
          && copy_header.tablet_id_.is_ls_inner_tablet()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls inner tablet should be exist", K(ret), K(copy_header));
      } else if (OB_FAIL(param_.ha_table_info_mgr_->init_tablet_info(copy_header))) {
        LOG_WARN("failed to init tablet info", K(ret), K(copy_header));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < copy_header.sstable_count_; ++i) {
          if (OB_FAIL(reader->get_next_sstable_info(sstable_info))) {
            LOG_WARN("failed to get next sstable info", K(copy_header), K(param_));
          } else if (OB_FAIL(build_tablets_sstable_info_(sstable_info))) {
            LOG_WARN("failed to create tablet sstable", K(ret), K(sstable_info));
          }
        }
      }
    }
  }

  if (OB_NOT_NULL(reader)) {
    free_sstable_info_reader_(reader);
  }
  return ret;
}

int ObStorageHATabletsBuilder::build_tablets_sstable_info_(
    const obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start create sstable", K(sstable_info));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!sstable_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create tablet sstable get invalid argument", K(ret), K(sstable_info));
  } else if (sstable_info.table_key_.is_memtable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table should not be MEMTABLE", K(ret), K(sstable_info));
  } else if (OB_FAIL(param_.ha_table_info_mgr_->add_table_info(sstable_info.tablet_id_, sstable_info))) {
    LOG_WARN("failed to add table info", K(ret), K(sstable_info));
  } else {
    LOG_DEBUG("add table info", K(sstable_info.tablet_id_), K(sstable_info));
  }
  return ret;
}

int ObStorageHATabletsBuilder::get_tablets_sstable_reader_(
    const common::ObIArray<ObTabletHandle> &tablet_handle_array,
    ObICopySSTableInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (param_.is_leader_restore_) {
    if (OB_FAIL(get_tablets_sstable_restore_reader_(tablet_handle_array ,reader))) {
      LOG_WARN("failed to get tablets sstable restore reader", K(ret), K(param_));
    }
  } else {
    if (OB_FAIL(get_tablets_sstable_ob_reader_(tablet_handle_array, reader))) {
      LOG_WARN("failed to get tablets sstable ob reader", K(ret), K(param_));
    }
  }
  return ret;
}

int ObStorageHATabletsBuilder::get_tablets_sstable_restore_reader_(
    const common::ObIArray<ObTabletHandle> &tablet_handle_array,
    ObICopySSTableInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  void *buf = nullptr;
  ObCopySSTableInfoRestoreReader *restore_reader = nullptr;
  ObArray<ObTabletID> tablet_id_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!param_.is_leader_restore_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tablets sstable restore reader get invalid argument", K(ret), K(param_));
  } else if (FALSE_IT(buf = ob_malloc(sizeof(ObCopySSTableInfoRestoreReader), "TabletReader"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(restore_reader = new (buf) ObCopySSTableInfoRestoreReader())) {
  } else if (FALSE_IT(reader = restore_reader)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_handle_array.count(); ++i) {
      const ObTabletHandle &tablet_handle = tablet_handle_array.at(i);
      if (!tablet_handle.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tablet handle should be valid", K(ret), K(tablet_handle));
      } else if (OB_FAIL(tablet_id_array.push_back(tablet_handle.get_obj()->get_tablet_meta().tablet_id_))) {
        LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_handle));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(restore_reader->init(param_.ls_->get_ls_id(),
        *param_.restore_base_info_, param_.restore_action_,
        tablet_id_array, *param_.meta_index_store_))) {
      LOG_WARN("failed to init restore reader", K(ret), K(param_));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(reader)) {
      free_sstable_info_reader_(reader);
    }
  }
  return ret;
}

int ObStorageHATabletsBuilder::get_tablets_sstable_ob_reader_(
    const common::ObIArray<ObTabletHandle> &tablet_handle_array,
    ObICopySSTableInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  obrpc::ObCopyTabletsSSTableInfoArg arg;
  ObCopySSTableInfoObReader *ob_reader = nullptr;
  void *buf = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (FALSE_IT(buf = mtl_malloc(sizeof(ObCopySSTableInfoObReader), "SSTableObReader"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(ob_reader = new (buf) ObCopySSTableInfoObReader())) {
  } else if (FALSE_IT(reader = ob_reader)) {
  } else if (OB_FAIL(build_copy_tablets_sstable_info_arg_(tablet_handle_array, arg))) {
    LOG_WARN("failed to build copy tablets sstable info arg", K(ret), K(arg));
  } else if (OB_FAIL(ob_reader->init(param_.src_info_, arg, *param_.svr_rpc_proxy_, *param_.bandwidth_throttle_))) {
    LOG_WARN("failed to init copy tablet info ob reader", K(ret), K(param_));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(reader)) {
      free_sstable_info_reader_(reader);
    }
  }

  return ret;
}

void ObStorageHATabletsBuilder::free_sstable_info_reader_(
    ObICopySSTableInfoReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    reader->~ObICopySSTableInfoReader();
    mtl_free(reader);
    reader = nullptr;
  }
}

int ObStorageHATabletsBuilder::build_copy_tablets_sstable_info_arg_(
    const common::ObIArray<ObTabletHandle> &tablet_handle_array,
    obrpc::ObCopyTabletsSSTableInfoArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!param_.need_check_seq_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K_(param));
  } else if (OB_FAIL(ObStorageHAUtils::get_server_version(arg.version_))) {
    LOG_WARN("failed to get server version", K(ret), K_(param));
  } else {
    arg.tenant_id_ = param_.tenant_id_;
    arg.ls_rebuild_seq_ = param_.local_rebuild_seq_;
    arg.ls_id_ = param_.ls_->get_ls_id();
    arg.need_check_seq_ = param_.need_check_seq_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_handle_array.count(); ++i) {
      const ObTabletHandle &tablet_handle = tablet_handle_array.at(i);
      ObCopyTabletSSTableInfoArg sstable_info_arg;
      if (OB_FAIL(build_copy_tablet_sstable_info_arg_(tablet_handle, sstable_info_arg))) {
        LOG_WARN("failed to build copy tablet sstable info arg", K(ret), K(tablet_handle), K(param_));
      } else if (OB_FAIL(arg.tablet_sstable_info_arg_list_.push_back(sstable_info_arg))) {
        LOG_WARN("failed to push sstable info arg into array", K(ret), K(sstable_info_arg), K(param_));
      }
    }
  }
  return ret;
}

int ObStorageHATabletsBuilder::build_copy_tablet_sstable_info_arg_(
    const ObTabletHandle &tablet_handle,
    obrpc::ObCopyTabletSSTableInfoArg &arg)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  arg.reset();
  ObTabletID tablet_id;

#ifdef ERRSIM
  const int64_t errsim_tablet_id = GCONF.errsim_migration_tablet_id;
  if (errsim_tablet_id == tablet_id.id()) {
    SERVER_EVENT_SYNC_ADD("storage_ha", "before_copy_ddl_sstable",
                          "tablet_id", tablet_id);
    DEBUG_SYNC(BEFORE_COPY_DDL_SSTABLE);
  }
#endif

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build copy tablet sstable info arg get invalid argument", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(ls = param_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle));
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    arg.tablet_id_ = tablet_id;
    const ObSSTableArray &major_sstable_array = table_store_wrapper.get_member()->get_major_sstables();
    const ObSSTableArray &minor_sstable_array = table_store_wrapper.get_member()->get_minor_sstables();
    const ObSSTableArray &ddl_sstable_array = table_store_wrapper.get_member()->get_ddl_sstables();

    //major
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_major_sstable_max_snapshot_(major_sstable_array, arg.max_major_sstable_snapshot_))) {
        LOG_WARN("failed to get sstable max snapshot", K(ret), K(tablet_id), K(param_));
      }
    }

    //minor
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_minor_scn_range_(minor_sstable_array, tablet, arg.minor_sstable_scn_range_))) {
        LOG_WARN("failed to get sstable max end log ts", K(ret), K(tablet_id), K(param_));
      }
    }

    //ddl
    if (OB_SUCC(ret)) {
      //TODO(muwei.ym) now do not reuse ddl sstable, will reuse it in 4.3
      if (OB_FAIL(get_need_copy_ddl_sstable_range_(tablet, ddl_sstable_array, arg.ddl_sstable_scn_range_))) {
        LOG_WARN("failed to get need copy ddl sstable range", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("succeed build copy sstable arg", K(tablet_id), K(arg));
    }
  }
  return ret;
}

int ObStorageHATabletsBuilder::get_major_sstable_max_snapshot_(
    const ObSSTableArray &major_sstable_array,
    int64_t &max_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSTableWrapper> sstables;

  max_snapshot_version = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (major_sstable_array.count() > 0 && OB_FAIL(major_sstable_array.get_all_table_wrappers(sstables))) {
    LOG_WARN("failed to get all tables", K(ret), K(param_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); ++i) {
      const ObITable *table = sstables.at(i).get_sstable();

      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable should not be NULL", K(ret), KP(table), K(param_));
      } else if (!table->is_major_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable type is unexpected", K(ret), KP(table), K(param_));
      } else {
        max_snapshot_version = std::max(max_snapshot_version, table->get_key().get_snapshot_version());
      }
    }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COPY_MAJOR_SNAPSHOT_VERSION) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      max_snapshot_version = 0;
      ret = OB_SUCCESS;
      STORAGE_LOG(ERROR, "fake EN_COPY_MAJOR_SNAPSHOT_VERSION", K(ret), K(max_snapshot_version));
    }
  }
#endif

  }
  return ret;
}

int ObStorageHATabletsBuilder::get_minor_scn_range_(
    const ObSSTableArray &minor_sstable_array,
    ObTablet *tablet,
    ObScnRange &scn_range)
{
  int ret = OB_SUCCESS;
  scn_range.reset();
  ObArray<ObSSTableWrapper> sstables;
  scn_range.start_scn_ = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;
  scn_range.end_scn_ = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tables builder do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get remote logical minor scn range get invalid argument", K(ret), KP(tablet));
  } else if (minor_sstable_array.count() > 0 && OB_FAIL(minor_sstable_array.get_all_table_wrappers(sstables))) {
    LOG_WARN("failed to get all tables", K(ret), K(param_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); ++i) {
      const ObITable *table = sstables.at(i).get_sstable();

      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable should not be NULL", K(ret), KP(table), K(param_));
      } else if (table->is_remote_logical_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not has remote logical minor sstable, unexpected", K(ret), KPC(tablet), KPC(table));
      }
    }

    if (OB_SUCC(ret)) {
      //need copy src all minor sstables for tablet meta merge, do not need calculate sstable version range.
      //here set end scn just for compatible
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_2_0) {
        scn_range.start_scn_ = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;
        scn_range.end_scn_ = sstables.empty() ? tablet->get_tablet_meta().clog_checkpoint_scn_ : sstables.at(0).get_sstable()->get_start_scn();
      } else {
        scn_range.start_scn_.set_base();
        scn_range.end_scn_.set_max();
      }
    }
  }
  return ret;
}

// the tablet meta if the one copied from the source server
// ddl_sstable_array is the sstable of the destination server
// the first ddl sstable is an empty one with scn range: (ddl_start_scn - 1, ddl_start_scn]
// the scn range of ddl_sstable_array is continuous, so get the min ddl start scn as the end scn of need_copy_scn_range
int ObStorageHATabletsBuilder::get_need_copy_ddl_sstable_range_(
    const ObTablet *tablet,
    const ObSSTableArray &ddl_sstable_array,
    share::ObScnRange &need_copy_scn_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be null", K(ret));
  } else if (tablet->get_tablet_meta().table_store_flag_.with_major_sstable()) {
    need_copy_scn_range.start_scn_.set_min();
    need_copy_scn_range.end_scn_.set_min();
  } else {
    const SCN ddl_start_scn = tablet->get_tablet_meta().ddl_start_scn_;
    const SCN ddl_checkpoint_scn = tablet->get_tablet_meta().ddl_checkpoint_scn_;
    need_copy_scn_range.start_scn_ = tablet->get_tablet_meta().get_ddl_sstable_start_scn();
    if (ddl_start_scn > ddl_checkpoint_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("checkpoint ts should be greater than start ts",
        K(ret), "tablet_meta", tablet->get_tablet_meta());
    } else {
      if (!ddl_sstable_array.empty()) {
        if (OB_FAIL(get_ddl_sstable_min_start_scn_(ddl_sstable_array, need_copy_scn_range.end_scn_))) {
          LOG_WARN("failed to get ddl sstable min start scn", K(ret));
        }
      } else {
        need_copy_scn_range.end_scn_ = ddl_checkpoint_scn;
      }
#ifdef ERRSIM
      LOG_INFO("get_need_copy_ddl_sstable_range", K(ddl_sstable_array), K(ddl_start_scn), K(ddl_checkpoint_scn));
      SERVER_EVENT_SYNC_ADD("storage_ha", "get_need_copy_ddl_sstable_range",
                            "tablet_id", tablet->get_tablet_meta().tablet_id_,
                            "dest_ddl_sstable_count", ddl_sstable_array.count(),
                            "start_scn", need_copy_scn_range.start_scn_,
                            "end_scn", need_copy_scn_range.end_scn_);
#endif
    }
  }
  return ret;
}

int ObStorageHATabletsBuilder::get_ddl_sstable_min_start_scn_(
    const ObSSTableArray &ddl_sstable_array,
    SCN &max_start_scn)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSTableWrapper> sstables;
  max_start_scn = SCN::max_scn();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tables builder do not init", K(ret));
  } else if (ddl_sstable_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl sstable should not be empty", K(ret));
  } else if (OB_FAIL(ddl_sstable_array.get_all_table_wrappers(sstables))) {
    LOG_WARN("failed to get all tables", K(ret), K(param_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); ++i) {
      const ObITable *table = sstables.at(i).get_sstable();

      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable should not be NULL", K(ret), KP(table), K(param_));
      } else if (!table->is_ddl_dump_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable type is unexpected", K(ret), KP(table), K(param_));
      } else {
        SCN start_scn = table->get_key().scn_range_.start_scn_.is_valid() ? (table->get_key().scn_range_.start_scn_) : SCN::max_scn();
        max_start_scn = std::min(max_start_scn, start_scn);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (SCN::max_scn() == max_start_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max start scn must not be equal to palf::max_scn", K(ret), K(max_start_scn));
    }

  }
  return ret;
}

int ObStorageHATabletsBuilder::hold_local_reuse_sstable_(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &local_tablet_hdl,
    ObTablesHandleArray &tables_handle,
    ObStorageSchema &storage_schema,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  tables_handle.reset();
  ObTablet *tablet = nullptr;
  ObArenaAllocator arena_allocator;
  ObStorageSchema *tablet_storage_schema = nullptr;
  const compaction::ObMediumCompactionInfoList *tablet_medium_list = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hold local reuse sstable get invalid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(param_.ls_->ha_get_tablet(tablet_id, local_tablet_hdl))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(tablet = local_tablet_hdl.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id), KP(tablet));
  } else if (OB_FAIL(tablet->load_storage_schema(arena_allocator, tablet_storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (tablet->get_tablet_meta().has_next_tablet_) {
        //TODO(muwei.ym) In this condition can work without L replica. 4.3
        //With L replica inner tablet should keep multi version tablet
        if (OB_FAIL(remove_uncomplete_tablet_(tablet_id))) {
          LOG_WARN("failed to remove uncomplete tablet", K(ret), K(tablet_id));
        }
      } else if (OB_FAIL(hold_local_complete_tablet_sstable_(tablet, tables_handle))) {
        LOG_WARN("failed to hold local complete tablet sstable", K(ret), KP(tablet));
      } else {
        if (!storage_schema.is_inited()) {
          if (OB_FAIL(storage_schema.init(allocator, *tablet_storage_schema))) {
            LOG_WARN("failed to init storage schema", K(ret), KPC(tablet));
          }
        } else if (storage_schema.compare_schema_newer(*tablet_storage_schema)) {
          if (OB_FAIL(ObStorageSchemaUtil::update_storage_schema(allocator, *tablet_storage_schema, storage_schema/*dst*/))) {
            LOG_WARN("failed to init storage schema", K(ret), KPC(tablet));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!tablet->get_tablet_meta().has_next_tablet_) {
        break;
      } else {
        tablet = tablet->get_next_tablet_guard().get_obj();
        if (OB_ISNULL(tablet)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_id));
        }
      }
    } // end of while
  }
  ObTabletObjLoadHelper::free(arena_allocator, tablet_storage_schema);
  return ret;
}

int ObStorageHATabletsBuilder::hold_local_complete_tablet_sstable_(
    ObTablet *tablet,
    ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  //TODO(muwwei.ym) here do not reuse andy ddl sstables and minor sstables
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hold local complete tablet sstable get invalid argument", K(ret));
  } else if (tablet->get_tablet_meta().tablet_id_.is_ls_inner_tablet()) {
    LOG_INFO("ls inner tablet do not reuse any sstable", K(ret), KPC(tablet));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
    //TODO(muwei.ym) ls inner tablet now do not reuse any sstable, will reuse in 4.3
  } else {
    const ObSSTableArray &major_sstable = table_store_wrapper.get_member()->get_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < major_sstable.count(); ++i) {
      ObITable *table = major_sstable.at(i);
      bool is_exist = false;

      if (OB_ISNULL(table) || !table->is_major_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KP(table), KPC(tablet));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < tables_handle.get_count(); ++j) {
          ObITable *tmp_table = tables_handle.get_table(j);
          if (OB_ISNULL(tmp_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table should not be NULL", K(ret), KP(tmp_table), K(j), KPC(tablet));
          } else if (tmp_table->get_key() == table->get_key()) {
            is_exist = true;
            break;
          }
        }

        if (OB_SUCC(ret)) {
          if (!is_exist && OB_FAIL(tables_handle.add_sstable(table, table_store_wrapper.get_meta_handle()))) {
            LOG_WARN("failed to add table into tables handle", K(ret), KPC(tablet));
          }
        }
      }
    }
    LOG_INFO("succeed to get reuse sstable handle", K(ret), K(tables_handle), KPC(tablet));
  }
  return ret;
}

int ObStorageHATabletsBuilder::remove_uncomplete_tablet_(
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const bool is_rollback = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove uncomplete tablet get invalid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(param_.ls_->trim_rebuild_tablet(tablet_id, is_rollback))) {
    LOG_WARN("failed to trim tablet tablet with rollback", K(ret), K(tablet_id));
  } else {
    LOG_INFO("succeed to remove uncomplete tablet", K(ret), K(tablet_id));
  }
  return ret;
}

int ObStorageHATabletsBuilder::modified_tablet_info_(
    obrpc::ObCopyTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (!tablet_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("modified tablet info get invalid argument", K(ret), K(tablet_info));
  } else if (tablet_info.param_.is_empty_shell()) {
    // do nothing
  } else if (tablet_info.param_.ha_status_.is_restore_status_full()
      && !tablet_info.param_.ha_status_.is_data_status_complete()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet info ha status is unexpected", K(ret), K(tablet_info));
  } else if (ObTabletRestoreAction::is_restore_none(param_.restore_action_)  // restore process doesn't consider data state
          && OB_FAIL(tablet_info.param_.ha_status_.set_data_status(ObTabletDataStatus::INCOMPLETE))) {
    LOG_WARN("failed to set data status", K(ret), K(tablet_info));
  }
  return ret;
}

int ObStorageHATabletsBuilder::create_tablet_with_major_sstables_(
    ObLS *ls,
    const obrpc::ObCopyTabletInfo &tablet_info,
    const ObTablesHandleArray &major_tables,
    const ObStorageSchema &storage_schema)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (major_tables.empty()) {
    //do nothing
  } else if (OB_FAIL(ObStorageHATabletBuilderUtil::build_tablet_with_major_tables(ls,
      tablet_info.tablet_id_, major_tables, storage_schema))) {
    LOG_WARN("failed to build tablet with major tables", K(ret), K(tablet_info), KPC(ls));
  }
  return ret;
}

int ObStorageHATabletsBuilder::hold_local_tablet_(
    common::ObIArray<ObTabletHandle> &tablet_handle_array)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;

  tablet_handle_array.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablets builder do not init", K(ret));
  } else if (OB_ISNULL(ls = param_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(param_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = param_.tablet_id_array_.at(i);
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
        }
      } else if (OB_FAIL(tablet_handle_array.push_back(tablet_handle))) {
        LOG_WARN("failed to push tablet handle into array", K(ret), K(tablet_handle));
      }
    }
  }
  return ret;
}

/******************ObStorageHATabletTableInfoMgr*********************/
ObStorageHATableInfoMgr::ObStorageHATabletTableInfoMgr::ObStorageHATabletTableInfoMgr()
  : is_inited_(false),
    tablet_id_(),
    status_(ObCopyTabletStatus::MAX_STATUS),
    allocator_("HATableInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    copy_table_info_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
    tablet_meta_()
{
}

ObStorageHATableInfoMgr::ObStorageHATabletTableInfoMgr::~ObStorageHATabletTableInfoMgr()
{
}

int ObStorageHATableInfoMgr::ObStorageHATabletTableInfoMgr::init(
    const ObTabletID &tablet_id,
    const storage::ObCopyTabletStatus::STATUS &status,
    const ObMigrationTabletParam &tablet_meta)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha tablet table info mgr init twice", K(ret), K(tablet_id));
  } else if (!tablet_id.is_valid() || !ObCopyTabletStatus::is_valid(status)
      || (ObCopyTabletStatus::TABLET_EXIST == status && !tablet_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init storage ha tablet table info mgr get invalid argument", K(ret), K(tablet_id),
        K(status), K(tablet_meta));
  } else if (ObCopyTabletStatus::TABLET_EXIST == status && OB_FAIL(tablet_meta_.assign(tablet_meta))) {
    LOG_WARN("failed to assign tablet meta", K(ret), K(tablet_meta));
  } else {
    tablet_id_ = tablet_id;
    status_ = status;
    is_inited_ = true;
  }
  return ret;
}

int ObStorageHATableInfoMgr::ObStorageHATabletTableInfoMgr::get_copy_table_info(
    const ObITable::TableKey &table_key,
    const blocksstable::ObMigrationSSTableParam *&copy_table_info)
{
  int ret = OB_SUCCESS;
  bool found = false;
  copy_table_info  = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet table info mgr do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get copy table info get invalid argument", K(ret), K(table_key));
  } else {
    for (int64_t i = 0; i < copy_table_info_array_.count() && !found; ++i) {
      const ObMigrationSSTableParam &tmp_copy_table_info = copy_table_info_array_.at(i);
      if (table_key == tmp_copy_table_info.table_key_) {
        copy_table_info = &copy_table_info_array_.at(i);
        found = true;
      }
    }

    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get copy table key info", K(ret), K(table_key));
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::ObStorageHATabletTableInfoMgr::add_copy_table_info(
    const blocksstable::ObMigrationSSTableParam &copy_table_info)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  bool found = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet table info mgr do not init", K(ret));
  } else if (!copy_table_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add copy table key get invalid argument", K(ret), K(copy_table_info));
  } else{
    for (int64_t i = 0; i < copy_table_info_array_.count() && !found; ++i) {
      const ObMigrationSSTableParam &tmp_copy_table_info = copy_table_info_array_.at(i);
      if (copy_table_info.table_key_ == tmp_copy_table_info.table_key_) {
        found = true;
      }
    }

    if (!found) {
      if (OB_FAIL(copy_table_info_array_.push_back(copy_table_info))) {
        LOG_WARN("failed to push copy table key info into array", K(ret), K(copy_table_info));
      }
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::ObStorageHATabletTableInfoMgr::get_table_keys(
    common::ObIArray<ObITable::TableKey> &table_keys)
{
  int ret = OB_SUCCESS;
  table_keys.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet table info mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; i < copy_table_info_array_.count(); ++i) {
      const ObMigrationSSTableParam &tmp_copy_table_info = copy_table_info_array_.at(i);
      if (OB_FAIL(table_keys.push_back(tmp_copy_table_info.table_key_))) {
        LOG_WARN("failed to push table key into array", K(ret), K(tmp_copy_table_info));
      }
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::ObStorageHATabletTableInfoMgr::check_copy_tablet_exist(bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet table info mgr do not init", K(ret));
  } else {
    is_exist = ObCopyTabletStatus::TABLET_EXIST == status_;
  }
  return ret;
}

int ObStorageHATableInfoMgr::ObStorageHATabletTableInfoMgr::get_tablet_meta(const ObMigrationTabletParam *&tablet_meta)
{
  int ret = OB_SUCCESS;
  tablet_meta = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet table info mgr do not init", K(ret));
  } else if (ObCopyTabletStatus::TABLET_EXIST != status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet do not exist, cannot get tablet meta", K(ret), K(status_));
  } else {
    tablet_meta = &tablet_meta_;
  }
  return ret;
}

/******************ObStorageHATableInfoMgr*********************/
ObStorageHATableInfoMgr::ObStorageHATableInfoMgr()
  : is_inited_(false),
    lock_(),
    table_info_mgr_map_()
{
}

ObStorageHATableInfoMgr::~ObStorageHATableInfoMgr()
{
  reuse();
}

int ObStorageHATableInfoMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha table info mgr init twice", K(ret));
  } else if (OB_FAIL(table_info_mgr_map_.create(MAX_BUCEKT_NUM, "HATableInfoMgr"))) {
    LOG_WARN("failed to create tablet table key mgr", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStorageHATableInfoMgr::get_table_info(
    const common::ObTabletID &tablet_id,
    const ObITable::TableKey &table_key,
    const blocksstable::ObMigrationSSTableParam *&copy_table_info)
{
  int ret = OB_SUCCESS;
  copy_table_info = nullptr;
  ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha table info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get table key info get invalid argument", K(ret), K(tablet_id), K(table_key));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(table_info_mgr_map_.get_refactored(tablet_id, tablet_table_info_mgr))) {
      LOG_WARN("failed to get tablet table key mgr", K(ret), K(tablet_id));
    } else if (OB_ISNULL(tablet_table_info_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table key mgr should not be NULL", K(ret), KP(tablet_table_info_mgr));
    } else if (OB_FAIL(tablet_table_info_mgr->get_copy_table_info(table_key, copy_table_info))) {
      LOG_WARN("failed to get copy table key info", K(ret), K(tablet_id), K(table_key));
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::add_table_info(
    const common::ObTabletID &tablet_id,
    const obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha table info mgr do not init", K(ret), K(tablet_id));
  } else if (!tablet_id.is_valid() || !sstable_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add table info get invalid argument", K(ret), K(tablet_id), K(sstable_info));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(table_info_mgr_map_.get_refactored(tablet_id, tablet_table_info_mgr))) {
      LOG_WARN("failed to get tablet table info mgr", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_table_info_mgr->add_copy_table_info(sstable_info.param_))) {
      LOG_WARN("failed to add copy table key info", K(ret), K(tablet_id), K(sstable_info));
    }
  }
  return ret;
}

void ObStorageHATableInfoMgr::reuse()
{
  common::SpinWLockGuard guard(lock_);
  if (!table_info_mgr_map_.created()) {
  } else {
    for (TabletTableInfoMgr::iterator iter = table_info_mgr_map_.begin(); iter != table_info_mgr_map_.end(); ++iter) {
      ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = iter->second;
      tablet_table_info_mgr->~ObStorageHATabletTableInfoMgr();
      mtl_free(tablet_table_info_mgr);
      tablet_table_info_mgr = nullptr;
    }
    table_info_mgr_map_.reuse();
  }
}

int ObStorageHATableInfoMgr::remove_tablet_table_info(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha table info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove tablet table key mgr get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(table_info_mgr_map_.erase_refactored(tablet_id, &tablet_table_info_mgr))) {
      LOG_WARN("failed to erase tablet table key mgr", K(ret), K(tablet_id));
    } else if (nullptr == tablet_table_info_mgr) {
      //do nothing
    } else {
      tablet_table_info_mgr->~ObStorageHATabletTableInfoMgr();
      mtl_free(tablet_table_info_mgr);
      tablet_table_info_mgr = nullptr;
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::get_table_keys(
    const common::ObTabletID &tablet_id,
    common::ObIArray<ObITable::TableKey> &table_keys)
{
  int ret = OB_SUCCESS;
  table_keys.reset();
  ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get table keys get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(table_info_mgr_map_.get_refactored(tablet_id, tablet_table_info_mgr))) {
      LOG_WARN("failed to get tablet table info mgr", K(ret), K(tablet_id));
    } else if (OB_ISNULL(tablet_table_info_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table info mgr should not be NULL", K(ret), K(tablet_id), KP(tablet_table_info_mgr));
    } else if (OB_FAIL(tablet_table_info_mgr->get_table_keys(table_keys))) {
      LOG_WARN("failed to get table keys", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::init_tablet_info(
    const obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage hs tablet info mgr do not init", K(ret));
  } else if (!copy_header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet info get invalid argument", K(ret), K(copy_header));
  } else {
    common::SpinWLockGuard guard(lock_);
    int32_t hash_ret = table_info_mgr_map_.get_refactored(copy_header.tablet_id_, tablet_table_info_mgr);
    if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret == OB_SUCCESS ? OB_ERR_UNEXPECTED : hash_ret;
      LOG_WARN("tablet table info mgr already init", K(ret), K(copy_header));
    } else {
      void *buf = NULL;
      tablet_table_info_mgr = nullptr;

      if (FALSE_IT(buf = mtl_malloc(sizeof(ObStorageHATabletTableInfoMgr), "HATabletInfoMgr"))) {
      } else if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KP(buf));
      } else if (FALSE_IT(tablet_table_info_mgr = new (buf) ObStorageHATabletTableInfoMgr())) {
      } else if (OB_FAIL(tablet_table_info_mgr->init(copy_header.tablet_id_, copy_header.status_, copy_header.tablet_meta_))) {
        LOG_WARN("failed to init tablet table key mgr", K(ret), K(copy_header));
      } else if (OB_FAIL(table_info_mgr_map_.set_refactored(copy_header.tablet_id_, tablet_table_info_mgr))) {
        LOG_WARN("failed to set tablet table key mgr into map", K(ret), K(copy_header));
      }

      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(tablet_table_info_mgr)) {
          tablet_table_info_mgr->~ObStorageHATabletTableInfoMgr();
          mtl_free(tablet_table_info_mgr);
          tablet_table_info_mgr = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::check_copy_tablet_exist(
    const common::ObTabletID &tablet_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check copy tablet exist get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(table_info_mgr_map_.get_refactored(tablet_id, tablet_table_info_mgr))) {
      LOG_WARN("failed to get tablet table info mgr", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_table_info_mgr->check_copy_tablet_exist(is_exist))) {
      LOG_WARN("failed to check copy tablet exist", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::check_tablet_table_info_exist(
    const common::ObTabletID &tablet_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check copy tablet exist get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(table_info_mgr_map_.get_refactored(tablet_id, tablet_table_info_mgr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        is_exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tablet table info mgr", K(ret), K(tablet_id));
      }
    } else if (OB_FAIL(tablet_table_info_mgr->check_copy_tablet_exist(is_exist))) {
      LOG_WARN("failed to check copy tablet exist", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObStorageHATableInfoMgr::get_tablet_meta(
    const common::ObTabletID &tablet_id,
    const ObMigrationTabletParam *&tablet_meta)
{
  int ret = OB_SUCCESS;
  tablet_meta = nullptr;
  ObStorageHATabletTableInfoMgr *tablet_table_info_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha tablet info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check copy tablet exist get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(table_info_mgr_map_.get_refactored(tablet_id, tablet_table_info_mgr))) {
      LOG_WARN("failed to get tablet table info mgr", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_table_info_mgr->get_tablet_meta(tablet_meta))) {
      LOG_WARN("failed to get tablet meta", K(ret), K(tablet_id), KP(tablet_meta));
    }
  }
  return ret;
}

/******************ObStorageHACopySSTableParam*********************/
ObStorageHACopySSTableParam::ObStorageHACopySSTableParam()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    copy_table_key_array_(),
    src_info_(),
    local_rebuild_seq_(-1),
    need_check_seq_(false),
    is_leader_restore_(false),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr)
{
}

void ObStorageHACopySSTableParam::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  copy_table_key_array_.reset();
  src_info_.reset();
  local_rebuild_seq_ = -1;
  need_check_seq_ = false;
  is_leader_restore_ = false;
  bandwidth_throttle_ = nullptr;
  svr_rpc_proxy_ = nullptr;
  storage_rpc_ = nullptr;
  restore_base_info_ = nullptr;
  meta_index_store_ = nullptr;
  second_meta_index_store_ = nullptr;
}

bool ObStorageHACopySSTableParam::is_valid() const
{
  bool bool_ret = false;
  bool_ret = OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && tablet_id_.is_valid()
      && ((need_check_seq_ && local_rebuild_seq_ >= 0) || !need_check_seq_);
  if (bool_ret) {
    if (!is_leader_restore_) {
      bool_ret = src_info_.is_valid() && OB_NOT_NULL(bandwidth_throttle_)
          && OB_NOT_NULL(svr_rpc_proxy_) && OB_NOT_NULL(storage_rpc_);
    } else {
      bool_ret = OB_NOT_NULL(restore_base_info_)
        && OB_NOT_NULL(meta_index_store_)
        && OB_NOT_NULL(second_meta_index_store_);
    }
  }
  return bool_ret;
}

int ObStorageHACopySSTableParam::assign(const ObStorageHACopySSTableParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage ha copy sstable param is not valid", K(ret), K(param));
  } else if (OB_FAIL(copy_table_key_array_.assign(param.copy_table_key_array_))) {
    LOG_WARN("failed to assign table key info array", K(ret), K(param));
  } else {
    tenant_id_ = param.tenant_id_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    src_info_ = param.src_info_;
    local_rebuild_seq_ = param.local_rebuild_seq_;
    need_check_seq_ = param.need_check_seq_;
    is_leader_restore_ = param.is_leader_restore_;
    bandwidth_throttle_ = param.bandwidth_throttle_;
    svr_rpc_proxy_ = param.svr_rpc_proxy_;
    storage_rpc_ = param.storage_rpc_;
    restore_base_info_ = param.restore_base_info_;
    meta_index_store_ = param.meta_index_store_;
    second_meta_index_store_ = param.second_meta_index_store_;
  }
  return ret;
}

/******************ObStorageHACopySSTableInfoMgr*********************/
ObStorageHACopySSTableInfoMgr::ObStorageHACopySSTableInfoMgr()
  : is_inited_(false),
    param_(),
    allocator_("HACopySSTMgr"),
    macro_range_info_map_(),
    status_(ObCopyTabletStatus::TABLET_EXIST)
{
}

ObStorageHACopySSTableInfoMgr::~ObStorageHACopySSTableInfoMgr()
{
  if (!macro_range_info_map_.created()) {
  } else {
    for (CopySSTableMacroRangeInfoMap::iterator iter = macro_range_info_map_.begin();
        iter != macro_range_info_map_.end(); ++iter) {
      ObCopySSTableMacroRangeInfo *sstable_macro_range_info = iter->second;
      sstable_macro_range_info->~ObCopySSTableMacroRangeInfo();
      sstable_macro_range_info = nullptr;
    }
    macro_range_info_map_.reuse();
  }
  allocator_.reset();
}

int ObStorageHACopySSTableInfoMgr::init(const ObStorageHACopySSTableParam &param)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUECKT_NUM = 128;
  int64_t bucket_num = 0;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha copy sstable info mgr init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init storage ha copy sstable info mgr get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign copy sstable info param", K(ret), K(param));
  } else if (FALSE_IT(bucket_num = std::max(MAX_BUECKT_NUM, param_.copy_table_key_array_.count()))) {
  } else if (OB_FAIL(macro_range_info_map_.create(bucket_num, "MacroRangeMap"))) {
    LOG_WARN("failed to create macro range info map", K(ret), K(param_));
  } else if (OB_FAIL(build_sstable_macro_range_info_map_())) {
    LOG_WARN("failed to build sstable macro range info map", K(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStorageHACopySSTableInfoMgr::build_sstable_macro_range_info_map_()
{
  int ret = OB_SUCCESS;
  ObICopySSTableMacroInfoReader *reader = nullptr;
  ObCopySSTableMacroRangeInfo sstable_macro_range_info;
  void *buf = nullptr;
  ObCopySSTableMacroRangeInfo *sstable_macro_range_info_ptr = nullptr;

  if (!param_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param should not be invalid", K(ret), K(param_));
  } else if (param_.copy_table_key_array_.empty()) {
    LOG_INFO("tablet do not has any sstable", K(ret), K(param_));
  } else if (OB_FAIL(get_sstable_macro_range_info_reader_(reader))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_INFO("src tablet do not exist", K(param_));
      status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get sstable macro range info reader", K(ret), K(param_));
    }
  } else {
    while (OB_SUCC(ret)) {
      sstable_macro_range_info.reset();
      buf = nullptr;
      sstable_macro_range_info_ptr = nullptr;
      if (OB_FAIL(reader->get_next_sstable_range_info(sstable_macro_range_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next sstable range info", K(ret), K(param_));
        }
      } else if (FALSE_IT(buf = allocator_.alloc(sizeof(ObCopySSTableMacroRangeInfo)))) {
      } else if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KP(buf));
      } else if (FALSE_IT(sstable_macro_range_info_ptr = new (buf) ObCopySSTableMacroRangeInfo())) {
      } else if (OB_FAIL(sstable_macro_range_info_ptr->assign(sstable_macro_range_info))) {
        LOG_WARN("failed to assign sstable macro range info", K(ret), K(param_));
      } else if (OB_FAIL(macro_range_info_map_.set_refactored(
          sstable_macro_range_info_ptr->copy_table_key_, sstable_macro_range_info_ptr))) {
        LOG_WARN("failed to set sstable macro range info into map", K(ret), K(param_));
      } else {
        sstable_macro_range_info_ptr = nullptr;
      }

      if (nullptr != sstable_macro_range_info_ptr) {
        sstable_macro_range_info_ptr->~ObCopySSTableMacroRangeInfo();
      }
    }
  }

  if (OB_NOT_NULL(reader)) {
    free_sstable_macro_range_info_reader_(reader);
  }
  return ret;
}

int ObStorageHACopySSTableInfoMgr::get_sstable_macro_range_info_reader_(
    ObICopySSTableMacroInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  if (!param_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param should not be invalid", K(ret), K(param_));
  } else if (param_.is_leader_restore_) {
    if (OB_FAIL(get_sstable_macro_range_info_restore_reader_(reader))) {
      LOG_WARN("failed to get sstable macro range info restore reader", K(ret), K(param_));
    }
  } else {
    if (OB_FAIL(get_sstable_macro_range_info_ob_reader_(reader))) {
      LOG_WARN("failed to get sstable macro range info ob reader", K(ret), K(param_));
    }
  }
  return ret;
}

int ObStorageHACopySSTableInfoMgr::get_sstable_macro_range_info_ob_reader_(
    ObICopySSTableMacroInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  void *buf = nullptr;
  obrpc::ObCopySSTableMacroRangeInfoArg arg;
  ObCopySSTableMacroObReader *ob_reader = nullptr;

  if (!param_.is_valid() || param_.is_leader_restore_ || !param_.need_check_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param should not be invalid", K(ret), K(param_));
  } else if (FALSE_IT(buf = mtl_malloc(sizeof(ObCopySSTableMacroObReader), "MacroInfoObRead"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(ob_reader = new (buf) ObCopySSTableMacroObReader())) {
  } else if (FALSE_IT(reader = ob_reader)) {
  } else if (OB_FAIL(arg.copy_table_key_array_.assign(param_.copy_table_key_array_))) {
    LOG_WARN("failed to assign copy table key info", K(ret), K(param_));
  } else {
    arg.tenant_id_ = param_.tenant_id_;
    arg.ls_id_ = param_.ls_id_;
    arg.tablet_id_ = param_.tablet_id_;
    arg.macro_range_max_marco_count_ = MACRO_RANGE_MAX_MACRO_COUNT;
    arg.need_check_seq_ = param_.need_check_seq_;
    arg.ls_rebuild_seq_ = param_.local_rebuild_seq_;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_MIGRATION_COPY_MACRO_BLOCK_NUM) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      arg.macro_range_max_marco_count_ = 1;
      ret = OB_SUCCESS;
      STORAGE_LOG(ERROR, "fake MACRO_RANGE_MAX_MACRO_COUNT", K(ret), K(arg));
    }
  }
#endif

    if (OB_FAIL(ob_reader->init(param_.src_info_, arg, *param_.svr_rpc_proxy_, *param_.bandwidth_throttle_))) {
      LOG_WARN("failed to init copy sstable macro ob reader", K(ret), K(param_));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(reader)) {
      free_sstable_macro_range_info_reader_(reader);
    }
  }

  return ret;
}

void ObStorageHACopySSTableInfoMgr::free_sstable_macro_range_info_reader_(ObICopySSTableMacroInfoReader *&reader)
{
  if (nullptr != reader) {
    reader->~ObICopySSTableMacroInfoReader();
    mtl_free(reader);
    reader = nullptr;
  }
}

int ObStorageHACopySSTableInfoMgr::get_sstable_macro_range_info_restore_reader_(
    ObICopySSTableMacroInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  void *buf = nullptr;
  obrpc::ObCopySSTableMacroRangeInfoArg arg;
  ObCopySSTableMacroRestoreReader *restore_reader = nullptr;

  if (!param_.is_valid() || !param_.is_leader_restore_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param should not be invalid", K(ret), K(param_));
  } else if (FALSE_IT(buf = mtl_malloc(sizeof(ObCopySSTableMacroRestoreReader), "MacroInfoReRead"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(restore_reader = new (buf) ObCopySSTableMacroRestoreReader())) {
  } else if (FALSE_IT(reader = restore_reader)) {
  } else if (OB_FAIL(arg.copy_table_key_array_.assign(param_.copy_table_key_array_))) {
    LOG_WARN("failed to assign copy table key info", K(ret), K(param_));
  } else {
    arg.tenant_id_ = param_.tenant_id_;
    arg.ls_id_ = param_.ls_id_;
    arg.tablet_id_ = param_.tablet_id_;
    arg.macro_range_max_marco_count_ = MACRO_RANGE_MAX_MACRO_COUNT;
    arg.need_check_seq_ = false;
    arg.ls_rebuild_seq_ = -1;
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_RESTORE_COPY_MACRO_BLOCK_NUM) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      arg.macro_range_max_marco_count_ = 8;
      ret = OB_SUCCESS;
      STORAGE_LOG(ERROR, "fake restore MACRO_RANGE_MAX_MACRO_COUNT", K(ret), K(arg));
    }
  }
#endif
    if (OB_FAIL(restore_reader->init(arg, *param_.restore_base_info_,
          *param_.meta_index_store_, *param_.second_meta_index_store_))) {
      LOG_WARN("failed to init copy sstable macro restore reader", K(ret), K(param_));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(reader)) {
      free_sstable_macro_range_info_reader_(reader);
    }
  }
  return ret;
}

int ObStorageHACopySSTableInfoMgr::get_copy_sstable_maro_range_info(
    const ObITable::TableKey &copy_table_key,
    ObCopySSTableMacroRangeInfo &copy_sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  ObCopySSTableMacroRangeInfo *sstable_macro_range_info_ptr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha copy sstable info mgr do not init", K(ret));
  } else if (!copy_table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get copy sstable macro range info get invalid argument", K(ret), K(copy_table_key));
  } else if (OB_FAIL(macro_range_info_map_.get_refactored(copy_table_key, sstable_macro_range_info_ptr))) {
    LOG_WARN("failed to get macro range info map", K(ret), K(copy_table_key));
  } else if (OB_ISNULL(sstable_macro_range_info_ptr) || !sstable_macro_range_info_ptr->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable macro range info should not be NULL or invalid", K(ret), KPC(sstable_macro_range_info_ptr));
  } else if (OB_FAIL(copy_sstable_macro_range_info.assign(*sstable_macro_range_info_ptr))) {
    LOG_WARN("failed to copy sstable macro range info", K(ret), KPC(sstable_macro_range_info_ptr));
  } else {
    LOG_INFO("succeed get copy sstable macro range info", K(ret), K(copy_table_key), K(copy_sstable_macro_range_info));
  }
  return ret;
}

int ObStorageHACopySSTableInfoMgr::check_src_tablet_exist(bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha copy sstable info mgr do not init", K(ret));
  } else {
    is_exist = ObCopyTabletStatus::TABLET_EXIST == status_;
  }
  return ret;
}

/******************ObStorageHATabletBuilderUtil*********************/

int ObStorageHATabletBuilderUtil::get_tablet_(
    const common::ObTabletID &tablet_id,
    ObLS *ls,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id), KPC(ls));
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::build_tablet_with_major_tables(
    ObLS *ls,
    const common::ObTabletID &tablet_id,
    const ObTablesHandleArray &major_tables,
    const ObStorageSchema &storage_schema)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == ls || !tablet_id.is_valid() || !storage_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KP(ls), K(tablet_id), K(storage_schema));
  } else if (!storage_schema.is_row_store()) {
    if (OB_FAIL(ObStorageHATabletBuilderUtil::build_tablet_for_column_store_(ls,
        tablet_id, major_tables, storage_schema))) {
      LOG_WARN("failed to build tablet with co tables", K(ret), K(tablet_id), KPC(ls));
    }
  } else if (OB_FAIL(ObStorageHATabletBuilderUtil::build_tablet_for_row_store_(ls,
      tablet_id, major_tables, storage_schema))) {
    LOG_WARN("failed to build tablet with major tables", K(ret), K(tablet_id), KPC(ls));
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::build_tablet_for_row_store_(
    ObLS *ls,
    const common::ObTabletID &tablet_id,
    const ObTablesHandleArray &major_tables,
    const ObStorageSchema &storage_schema)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_table_array;
  int64_t multi_version_start = 0;
  int64_t transfer_seq = 0;

  if (OB_ISNULL(ls) || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tablet with major tables get invalid argument", K(ret), KP(ls), K(tablet_id));
  } else if (OB_FAIL(get_tablet_(tablet_id, ls, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id), KPC(ls));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (FALSE_IT(transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else if (OB_FAIL(calc_multi_version_start_with_major_(major_tables, tablet, multi_version_start))) {
    LOG_WARN("failed to calc multi version start with major", K(ret), KPC(tablet));
  } else if (OB_FAIL(major_tables.get_tables(major_table_array))) {
    LOG_WARN("failed to get tables", K(ret));
  } else if (OB_FAIL(ObTableStoreUtil::sort_major_tables(major_table_array))) {
    LOG_WARN("failed to sort mjaor tables", K(ret));
  } else {
    ObTableHandleV2 major_table_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < major_table_array.count(); ++i) {
      major_table_handle.reset();
      ObITable *table_ptr = major_table_array.at(i);
      if (OB_ISNULL(table_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table ptr should not be null", K(ret), KP(table_ptr));
      } else if (!table_ptr->is_major_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table ptr is not major", K(ret), KPC(table_ptr));
      } else if (OB_FAIL(major_tables.get_table(table_ptr->get_key(), major_table_handle))) {
        LOG_WARN("fail to get table handle from array by table key", K(ret), KPC(table_ptr), K(major_tables));
      } else if (OB_FAIL(inner_update_tablet_table_store_with_major_(multi_version_start, major_table_handle,
          ls, tablet, storage_schema, transfer_seq))) {
        LOG_WARN("failed to update tablet table store", K(ret), K(tablet_id), KPC(table_ptr));
      }
    }
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::build_tablet_for_column_store_(
    ObLS *ls,
    const common::ObTabletID &tablet_id,
    const ObTablesHandleArray &major_tables,
    const ObStorageSchema &storage_schema)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTablesHandleArray co_tables;
  int64_t co_table_cnt = 0;
  int64_t multi_version_start = 0;

  if (OB_UNLIKELY(NULL == ls || !tablet_id.is_valid() || major_tables.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tablet with major tables get invalid argument", K(ret), KP(ls), K(tablet_id), K(major_tables));
  } else if (OB_UNLIKELY(NULL == major_tables.get_table(0) || !major_tables.get_table(0)->is_column_store_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table type", K(ret), KPC(major_tables.get_table(0)));
  } else if (OB_FAIL(get_tablet_(tablet_id, ls, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id), KPC(ls));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(calc_multi_version_start_with_major_(major_tables, tablet, multi_version_start))) {
    LOG_WARN("failed to calc multi version start with major", K(ret), KPC(tablet));
  } else if (OB_FAIL(assemble_column_oriented_sstable_(major_tables, co_tables))) {
    LOG_WARN("assemble co tables failed", K(ret), K(major_tables));
  } else if (OB_FAIL(build_tablet_with_co_tables_( //we should assemble flattened cg sstables when updating tablet due to allocator
      ls, tablet, storage_schema, multi_version_start, co_tables))) {
    LOG_WARN("failed to build tablet with column store tables", K(ret));
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::get_column_store_tables_(
    const ObTablesHandleArray &major_tables,
    ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> &column_store_tables,
    int64_t &co_table_cnt)
{
  int ret = OB_SUCCESS;
  column_store_tables.reset();
  co_table_cnt = 0;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> cg_tables;

  ObITable *table = nullptr;
  int64_t full_co_table_cnt = 0; // just for defensive check
  for (int64_t i = 0; OB_SUCC(ret) && i < major_tables.get_count(); ++i) {
    table = major_tables.get_table(i);
    if (OB_UNLIKELY(NULL == table || !table->is_column_store_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unpected table", K(ret), KPC(table));
    } else if (table->is_co_sstable()) {
      if (OB_FAIL(column_store_tables.push_back(table))) {
        LOG_WARN("failed to add co table", K(ret), KPC(table));
      } else if (static_cast<ObCOSSTableV2 *>(table)->is_inited()) {
        ++full_co_table_cnt;
      }
    } else if (OB_FAIL(cg_tables.push_back(table))) {
      LOG_WARN("failed to add cg table", K(ret), KPC(table));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (column_store_tables.empty() || (full_co_table_cnt < column_store_tables.count() && cg_tables.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table count", K(ret), K(full_co_table_cnt),
        K(column_store_tables.count()), K(cg_tables.count()), K(major_tables));
  } else if (FALSE_IT(co_table_cnt = column_store_tables.count())) {
  } else if (OB_FAIL(ObTableStoreUtil::sort_column_store_tables(column_store_tables))) {
    LOG_WARN("failed to sort co tables", K(ret));
  } else if (OB_FAIL(ObTableStoreUtil::sort_column_store_tables(cg_tables))) {
    LOG_WARN("failed to sort cg tables", K(ret));
  } else if (OB_FAIL(append(column_store_tables, cg_tables))) {
    LOG_WARN("failed to append cg tables", K(ret));
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::assemble_column_oriented_sstable_(
    const ObTablesHandleArray &mixed_tables,
    ObTablesHandleArray &co_tables)
{
  int ret = OB_SUCCESS;
  co_tables.reset();
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> column_store_tables;
  int64_t co_table_cnt = 0;
  if (OB_FAIL(get_column_store_tables_(mixed_tables, column_store_tables, co_table_cnt))) {
    LOG_WARN("failed to get column store tables", K(ret));
  }

  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> cur_cg_tables;
  int64_t start_cg_idx = co_table_cnt;

  // [CO_1, CO_N, CG_1_1, CG_1_2, ..., CG_N_1, CG_N_2]
  for (int64_t co_idx = 0; OB_SUCC(ret) && co_idx < co_table_cnt; ++co_idx) {
    ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(column_store_tables.at(co_idx));
    const int64_t co_snapshot_version = co_sstable->get_snapshot_version();
    cur_cg_tables.reset();

    if (co_sstable->is_inited()) {
      LOG_INFO("co sstable is inited", K(co_idx), K(co_table_cnt), K(start_cg_idx), KPC(co_sstable));
      // co sstable no need to fill cg tables
    } else {
      for (int64_t cg_idx = start_cg_idx; OB_SUCC(ret) && cg_idx < column_store_tables.count(); ++cg_idx) {
        ObITable *cg_table = column_store_tables.at(cg_idx);
        if (co_snapshot_version != cg_table->get_snapshot_version()) {
          if (cur_cg_tables.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("co table mismatch cg table!", K(ret), K(co_idx), K(co_table_cnt), K(start_cg_idx), K(cg_idx),
                K(co_snapshot_version), KPC(cg_table), K(column_store_tables));
          } else {
            start_cg_idx += cur_cg_tables.count();
          }
          break;
        } else if (OB_FAIL(cur_cg_tables.push_back(cg_table))) {
          LOG_WARN("failed to add cg table", K(ret), KPC(cg_table));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(co_sstable->fill_cg_sstables(cur_cg_tables))) {
          LOG_WARN("failed to fill cg tables", K(ret), KPC(co_sstable));
        }
      }
    }

    ObTableHandleV2 co_table_handle;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mixed_tables.get_table(co_sstable->get_key(), co_table_handle))) {
      LOG_WARN("fail to get table handle from array by table key", K(ret), KPC(co_sstable), K(mixed_tables));
    } else if (OB_FAIL(co_tables.add_table(co_table_handle))) {
      LOG_WARN("failed to add table", K(ret), K(co_table_handle));
    }
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::build_tablet_with_co_tables_(
    ObLS *ls,
    ObTablet *tablet,
    const ObStorageSchema &storage_schema,
    const int64_t multi_version_start,
    const ObTablesHandleArray &co_tables)
{
  int ret = OB_SUCCESS;
  int64_t transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_;

  for (int64_t co_idx = 0; OB_SUCC(ret) && co_idx < co_tables.get_count(); ++co_idx) {
    ObTableHandleV2 major_table_handle;
    if (OB_FAIL(co_tables.get_table(co_idx, major_table_handle))) {
      LOG_WARN("get co table handle failed", K(ret), K(co_idx));
    } else if (OB_FAIL(inner_update_tablet_table_store_with_major_(multi_version_start,
        major_table_handle, ls, tablet, storage_schema, transfer_seq))) {
      LOG_WARN("failed to update tablet table store", K(ret), KPC(tablet), "major_sstable", PC(major_table_handle.get_table()));
    }
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::calc_multi_version_start_with_major_(
    const ObTablesHandleArray &major_tables,
    ObTablet *tablet,
    int64_t &multi_version_start)
{
  int ret = OB_SUCCESS;
  multi_version_start = 0;
  int64_t tmp_multi_version_start = INT64_MAX;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc multi version start with major get invalid argument", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &local_major_tables = table_store_wrapper.get_member()->get_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < local_major_tables.count(); ++i) {
      const ObITable *table = local_major_tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table), KPC(tablet));
      } else {
        tmp_multi_version_start = std::min(tmp_multi_version_start, table->get_snapshot_version());
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < major_tables.get_count(); ++i) {
      const ObITable *table = major_tables.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table), KPC(tablet));
      } else {
        tmp_multi_version_start = std::min(tmp_multi_version_start, table->get_snapshot_version());
      }
    }

    if (OB_SUCC(ret)) {
      if (INT64_MAX == tmp_multi_version_start) {
        //do nothing
      } else {
        multi_version_start = tmp_multi_version_start;
      }
    }
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::inner_update_tablet_table_store_with_major_(
    const int64_t multi_version_start,
    const ObTableHandleV2 &table_handle,
    ObLS *ls,
    ObTablet *tablet,
    const ObStorageSchema &storage_schema,
    const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  SCN tablet_snapshot_version;
  ObTenantMetaMemMgr *meta_mem_mgr = nullptr;
  ObArenaAllocator allocator;
  ObStorageSchema *tablet_storage_schema = nullptr;
  if (multi_version_start < 0 || OB_ISNULL(tablet) || OB_ISNULL(ls) || !table_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table ptr should not be null", K(ret), K(multi_version_start), KP(tablet), K(table_handle), KP(ls));
  } else if (OB_ISNULL(meta_mem_mgr = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get meta mem mgr from MTL", K(ret));
  } else if (OB_FAIL(tablet->get_snapshot_version(tablet_snapshot_version))) {
    LOG_WARN("failed to get_snapshot_version", K(ret));
  } else if (OB_FAIL(tablet->load_storage_schema(allocator, tablet_storage_schema))) {
    LOG_WARN("fail to load storage schema failed", K(ret));
  } else {
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    const ObITable *table = table_handle.get_table();
    const int64_t update_snapshot_version = MAX(tablet->get_snapshot_version(), table->get_key().get_snapshot_version());
    const int64_t update_multi_version_start = MAX(tablet->get_multi_version_start(), multi_version_start);
    ObUpdateTableStoreParam param(static_cast<const blocksstable::ObSSTable *>(table),
                            update_snapshot_version,
                            update_multi_version_start,
                            &storage_schema,
                            ls->get_rebuild_seq(),
                            true/*need_check_transfer_seq*/,
                            transfer_seq,
                            true/*need_report*/,
                            SCN::min_scn()/*clog_checkpoint_scn*/,
                            true/*need_check_sstable*/,
                            true/*allow_duplicate_sstable*/,
                            compaction::ObMergeType::MEDIUM_MERGE/*merge_type*/);
    if (tablet_storage_schema->get_schema_version() < storage_schema.get_schema_version()) {
      SERVER_EVENT_ADD("storage_ha", "schema_change_need_merge_tablet_meta",
                      "tenant_id", MTL_ID(),
                      "tablet_id", tablet_id.id(),
                      "old_schema_version", tablet_storage_schema->get_schema_version(),
                      "new_schema_version", storage_schema.get_schema_version());
    }
#ifdef ERRSIM
    SERVER_EVENT_ADD("storage_ha", "update_major_tablet_table_store",
                      "tablet_id", tablet_id.id(),
                      "old_multi_version_start", tablet->get_multi_version_start(),
                      "new_multi_version_start", update_multi_version_start,
                      "old_snapshot_version", tablet->get_snapshot_version(),
                      "new_snapshot_version", table->get_key().get_snapshot_version());
#endif
    if (FAILEDx(ls->update_tablet_table_store(tablet_id, param, tablet_handle))) {
      LOG_WARN("failed to build ha tablet new table store", K(ret), KPC(tablet), K(param));
    }
  }
  ObTabletObjLoadHelper::free(allocator, tablet_storage_schema);
  return ret;
}

int ObStorageHATabletBuilderUtil::build_table_with_minor_tables(
    ObLS *ls,
    const common::ObTabletID &tablet_id,
    const ObMigrationTabletParam *src_tablet_meta,
    const ObTablesHandleArray &mds_tables,
    const ObTablesHandleArray &minor_tables,
    const ObTablesHandleArray &ddl_tables,
    const ObTabletRestoreAction::ACTION &restore_action)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  const bool need_tablet_meta_merge = ObTabletRestoreAction::is_restore_major(restore_action) ? false : true;
  ObTablesHandleArray sstables;
  ObTablesHandleArray ddl_co_tables;

  if (OB_ISNULL(ls) || !tablet_id.is_valid() || OB_ISNULL(src_tablet_meta) || !ObTabletRestoreAction::is_valid(restore_action)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tablet with major tables get invalid argument", K(ret), KP(ls), K(tablet_id), K(restore_action));
  } else if (OB_FAIL(append_sstable_array_(sstables, mds_tables))) {
    LOG_WARN("failed to append mds tables handle into array", K(ret), K(mds_tables));
  } else if (OB_FAIL(append_sstable_array_(sstables, minor_tables))) {
    LOG_WARN("failed to append minor tables handle into array", K(ret), K(minor_tables));
  } else if (!ddl_tables.empty() && ddl_tables.get_table(0)->is_column_store_sstable()) {
    if (OB_FAIL(assemble_column_oriented_sstable_(ddl_tables, ddl_co_tables))) {
      LOG_WARN("assemble co tables failed", K(ret), K(ddl_tables));
    } else if (OB_FAIL(append_sstable_array_(sstables, ddl_co_tables))) {
      LOG_WARN("failed to append ddl tables handle", K(ret), K(ddl_co_tables));
    }
  } else if (OB_FAIL(append_sstable_array_(sstables, ddl_tables))) {
    LOG_WARN("failed to append ddl tables handle", K(ret), K(ddl_tables));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_(tablet_id, ls, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id), KPC(ls));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(inner_update_tablet_table_store_with_minor_(ls, tablet, need_tablet_meta_merge,
      src_tablet_meta, sstables))) {
    LOG_WARN("failed to update tablet table store with minor", K(ret));
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::inner_update_tablet_table_store_with_minor_(
    ObLS *ls,
    ObTablet *tablet,
    const bool &need_tablet_meta_merge,
    const ObMigrationTabletParam *src_tablet_meta,
    const ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  ObBatchUpdateTableStoreParam update_table_store_param;
  const bool is_rollback = false;
  bool need_merge = false;

  if (OB_ISNULL(ls) || OB_ISNULL(tablet) || (need_tablet_meta_merge && OB_ISNULL(src_tablet_meta))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner update tablet table store with minor get invalid argument", K(ret), KP(ls), KP(tablet));
  } else if (need_tablet_meta_merge && OB_FAIL(check_need_merge_tablet_meta_(src_tablet_meta, tablet, need_merge))) {
    LOG_WARN("failed to check remote logical sstable exist", K(ret), KPC(tablet));
  } else {
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    update_table_store_param.tablet_meta_ = need_merge ? src_tablet_meta : nullptr;
    update_table_store_param.rebuild_seq_ = ls->get_rebuild_seq();

    if (OB_FAIL(update_table_store_param.tables_handle_.assign(tables_handle))) {
      LOG_WARN("failed to assign tables handle", K(ret), K(tables_handle));
    } else if (OB_FAIL(ls->build_ha_tablet_new_table_store(tablet_id, update_table_store_param))) {
      LOG_WARN("failed to build ha tablet new table store", K(ret), K(tablet_id), KPC(tablet), KPC(src_tablet_meta), K(update_table_store_param));
    }
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::check_need_merge_tablet_meta_(
    const ObMigrationTabletParam *src_tablet_meta,
    ObTablet *tablet,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  bool is_exist = false;
  if (OB_ISNULL(tablet) || OB_ISNULL(src_tablet_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check need merge tablet meta get invalid argument", K(ret), KP(tablet), KP(src_tablet_meta));
  } else if (tablet->get_tablet_meta().has_transfer_table()) {
    // If transfer table exist, no remote logical table will be created. And, the replaced transfer table
    // must be included in the minor tables. The transfer table info of local tablet need to be cleared by
    // merging tablet meta.
    need_merge = true;
  } else if (tablet->get_tablet_meta().clog_checkpoint_scn_ >= src_tablet_meta->clog_checkpoint_scn_
      && tablet->get_tablet_meta().mds_checkpoint_scn_ >= src_tablet_meta->mds_checkpoint_scn_) {
    need_merge = false;
  } else {
    need_merge = true;
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::check_remote_logical_sstable_exist(
    ObTablet *tablet,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check remote logical sstable exist get invalid argument", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &minor_sstables = table_store_wrapper.get_member()->get_minor_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_sstables.count(); ++i) {
      const ObITable *table = minor_sstables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("minor sstable should not be NULL", K(ret), KP(table));
      } else if (table->is_remote_logical_minor_sstable()) {
        is_exist = true;
        break;
      }
    }
  }
  return ret;
}

int ObStorageHATabletBuilderUtil::append_sstable_array_(
    ObTablesHandleArray &dest_array, const ObTablesHandleArray &src_array)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  for (int64_t i = 0; OB_SUCC(ret) && i < src_array.get_count(); ++i) {
    table_handle.reset();
    if (OB_FAIL(src_array.get_table(i, table_handle))) {
      LOG_WARN("failed to get table", K(ret), K(i), K(src_array));
    } else if (OB_FAIL(dest_array.add_table(table_handle))) {
      LOG_WARN("failed to add table", K(ret), K(table_handle));
    }
  }
  return ret;
}

}
}

