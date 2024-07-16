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

#include "ob_direct_insert_sstable_ctx_new.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_ddl_common.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/px/ob_sub_trans_ctrl.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ddl/ob_ddl_inc_redo_log_writer.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

int64_t ObTenantDirectLoadMgr::generate_context_id()
{
  return ATOMIC_AAF(&context_id_generator_, 1);
}

ObTenantDirectLoadMgr::ObTenantDirectLoadMgr()
  : is_inited_(false), slice_id_generator_(0), context_id_generator_(0), last_gc_time_(0)
{
}

ObTenantDirectLoadMgr::~ObTenantDirectLoadMgr()
{
  destroy();
}

void ObTenantDirectLoadMgr::destroy()
{
  is_inited_ = false;
  int ret = OB_SUCCESS;
  bucket_lock_.destroy();
  common::ObArray<ObTabletDirectLoadMgrKey> tablet_mgr_keys;
  for (TABLET_MGR_MAP::const_iterator iter = tablet_mgr_map_.begin();
        iter != tablet_mgr_map_.end(); ++iter) {
    if (OB_FAIL(tablet_mgr_keys.push_back(iter->first))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  for (int64_t i = 0; i < tablet_mgr_keys.count(); i++) {
    // overwrite ret
    if (OB_FAIL(remove_tablet_direct_load(tablet_mgr_keys.at(i)))) {
      LOG_WARN("remove tablet mgr failed", K(ret), K(tablet_mgr_keys.at(i)));
    }
  }
  allocator_.reset();
}

int64_t ObTenantDirectLoadMgr::generate_slice_id()
{
  return ATOMIC_AAF(&slice_id_generator_, 1);
}

int ObTenantDirectLoadMgr::mtl_init(ObTenantDirectLoadMgr *&tenant_direct_load_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_FAIL(tenant_direct_load_mgr->init())) {
    LOG_WARN("init failed", K(ret));
  }
  return ret;
}

int ObTenantDirectLoadMgr::init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const int64_t bucket_num = common::hash::cal_next_prime(1000L * 100L);
  const int64_t memory_limit = 1024L * 1024L * 1024L * 10L; // 10GB
  lib::ObMemAttr attr(tenant_id, "TenantDLMgr");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE,
    attr.label_, tenant_id, memory_limit))) {
    LOG_WARN("init alloctor failed", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(bucket_num, ObLatchIds::TENANT_DIRECT_LOAD_MGR_LOCK,
      ObLabel("TenDLBucket"), tenant_id))) {
    LOG_WARN("init bucket lock failed", K(ret), K(bucket_num));
  } else if (OB_FAIL(tablet_mgr_map_.create(bucket_num, attr, attr))) {
    LOG_WARN("create context map failed", K(ret));
  } else if (OB_FAIL(tablet_exec_context_map_.create(bucket_num, attr, attr))) {
    LOG_WARN("create context map failed", K(ret));
  } else {
    allocator_.set_attr(attr);
    slice_id_generator_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

// 1. Leader create it when start tablet direct load task;
// 2. Follower create it before replaying start log;
// 3. Migrate/Rebuild create tablet/ LS online create it.
int ObTenantDirectLoadMgr::create_tablet_direct_load(
    const int64_t context_id,
    const int64_t execution_id,
    const ObTabletDirectLoadInsertParam &build_param,
    const share::SCN checkpoint_scn,
    const bool only_persisted_ddl_data)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = build_param.common_param_.ls_id_;
  const common::ObTabletID &tablet_id = build_param.common_param_.tablet_id_;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletBindingMdsUserData ddl_data;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!build_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context_id), K(build_param));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(build_param));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(build_param));
  } else if (!only_persisted_ddl_data && OB_FAIL(tablet_handle.get_obj()->get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("failed to get ddl data from tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (only_persisted_ddl_data && OB_FAIL((tablet_handle.get_obj()->get_mds_data_from_tablet<mds::DummyKey, ObTabletBindingMdsUserData>(
      mds::DummyKey(),
      share::SCN::max_scn(),
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
      ReadBindingInfoOp(ddl_data))))) {
    if (OB_SNAPSHOT_DISCARDED == ret) {
      ddl_data.set_default_value();
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ddl data from tablet", K(ret), K(ls_id), K(tablet_id));
    }
  }

  if (OB_SUCC(ret)) {
    ObTabletHandle lob_tablet_handle;
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    ObTabletMemberWrapper<ObTabletTableStore> lob_store_wrapper;
    ObTabletDirectLoadMgrHandle data_tablet_direct_load_mgr_handle;
    ObTabletDirectLoadMgrHandle lob_tablet_direct_load_mgr_handle;
    data_tablet_direct_load_mgr_handle.reset();
    lob_tablet_direct_load_mgr_handle.reset();
    const bool is_full_direct_load_task = is_full_direct_load(build_param.common_param_.direct_load_type_);
    const ObTabletID &lob_meta_tablet_id = ddl_data.lob_meta_tablet_id_;
    if (!lob_meta_tablet_id.is_valid() || checkpoint_scn.is_valid_and_not_min()) {
      // has no lob, or recover from checkpoint.
      LOG_DEBUG("do not create lob mgr handle when create data tablet mgr", K(ret), K(ls_id), K(lob_meta_tablet_id),
          K(checkpoint_scn), K(build_param));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, lob_meta_tablet_id,
        lob_tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
      LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(lob_meta_tablet_id));
    } else if (OB_FAIL(lob_tablet_handle.get_obj()->fetch_table_store(lob_store_wrapper))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_FAIL(try_create_tablet_direct_load_mgr(context_id, execution_id,
        nullptr != lob_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/),
        allocator_, ObTabletDirectLoadMgrKey(lob_meta_tablet_id, is_full_direct_load_task), true /*is lob tablet*/,
        lob_tablet_direct_load_mgr_handle))) {
      LOG_WARN("try create data tablet direct load mgr failed", K(ret), K(build_param));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fetch table store failed", K(ret));
    } else if (OB_FAIL(try_create_tablet_direct_load_mgr(context_id, execution_id,
        nullptr != table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/),
        allocator_, ObTabletDirectLoadMgrKey(build_param.common_param_.tablet_id_, is_full_direct_load_task), false /*is lob tablet*/,
        data_tablet_direct_load_mgr_handle))) {
      // Newly-allocated Lob meta tablet direct load mgr will be cleanuped when tablet gc task works.
      LOG_WARN("try create data tablet direct load mgr failed", K(ret), K(build_param));
    }

    if (OB_FAIL(ret)) {
    } else if (data_tablet_direct_load_mgr_handle.is_valid()) {
      if (OB_FAIL(data_tablet_direct_load_mgr_handle.get_obj()->update(
          lob_tablet_direct_load_mgr_handle.get_obj(), build_param))) {
        LOG_WARN("init tablet mgr failed", K(ret), K(build_param));
      }
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::replay_create_tablet_direct_load(
    const ObTabletHandle &tablet_handle,
    const int64_t execution_id,
    const ObTabletDirectLoadInsertParam &build_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid() || execution_id < 0 || !build_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle), K(execution_id), K(build_param));
  } else {
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
    const bool is_full_direct_load_task = is_full_direct_load(build_param.common_param_.direct_load_type_);
    const int64_t unused_context_id = -1;
    if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fetch table store failed", K(ret));
    } else if (OB_FAIL(try_create_tablet_direct_load_mgr(
            unused_context_id,
            execution_id,
            nullptr != table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/),
            allocator_,
            ObTabletDirectLoadMgrKey(build_param.common_param_.tablet_id_, is_full_direct_load_task),
            false /*is lob tablet*/,
            direct_load_mgr_handle))) {
      // Newly-allocated Lob meta tablet direct load mgr will be cleanuped when tablet gc task works.
      LOG_WARN("try create data tablet direct load mgr failed", K(ret), K(build_param));
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::try_create_tablet_direct_load_mgr(
    const int64_t context_id,
    const int64_t execution_id,
    const bool major_sstable_exist,
    ObIAllocator &allocator,
    const ObTabletDirectLoadMgrKey &mgr_key,
    const bool is_lob_tablet_mgr,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle)
{
  int ret = OB_SUCCESS;
  direct_load_mgr_handle.reset();
  ObTabletDirectLoadExecContextId exec_id;
  ObTabletDirectLoadExecContext exec_context;
  exec_id.tablet_id_ = mgr_key.tablet_id_;
  exec_id.context_id_ = context_id;
  ObSArray<uint64_t> all_hash_array;
  ObMultiBucketLockGuard lock_guard(bucket_lock_, true/*is_write_lock*/);
  const bool need_set_exec_ctx = !is_lob_tablet_mgr && context_id >= 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!mgr_key.is_valid()) || execution_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(mgr_key), K(execution_id));
  } else if (OB_FAIL(all_hash_array.push_back(mgr_key.hash()))) {
    LOG_WARN("push back failed", K(ret), K(mgr_key));
  } else if (need_set_exec_ctx && OB_FAIL(all_hash_array.push_back(exec_id.hash()))) {
    LOG_WARN("push back failed", K(ret), K(exec_id));
  } else if (OB_FAIL(lock_guard.lock_multi_buckets(all_hash_array))) {
    LOG_WARN("lock mult buckets failed", K(ret));
  } else {
    ObTabletDirectLoadMgr *direct_load_mgr = nullptr;
    if (OB_FAIL(get_tablet_mgr_no_lock(mgr_key, direct_load_mgr_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get refactored failed", K(ret), K(is_full_direct_load), K(mgr_key));
      }
    } else if (OB_ISNULL(direct_load_mgr = direct_load_mgr_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(mgr_key));
    } else if (!mgr_key.is_full_direct_load_) {
      // For incremental direct load, different task cannot use the same ObTabletDirectLoadMgr, remove old ObTabletDirectLoadMgr
      if (OB_FAIL(remove_tablet_direct_load_nolock(mgr_key))) {
        LOG_WARN("fail to remove tablet direct load", K(ret), K(mgr_key));
      } else {
        LOG_INFO("remove an old tablet direct load", K(mgr_key), K(context_id), "old_context_id", direct_load_mgr->get_context_id());
        direct_load_mgr = nullptr;
        direct_load_mgr_handle.reset();
      }
    }
    if (OB_SUCC(ret) && (!mgr_key.is_full_direct_load_ || !major_sstable_exist)) {
      if (nullptr == direct_load_mgr) {
        void *buf = nullptr;
        const int64_t buf_size = mgr_key.is_full_direct_load_ ?
            sizeof(ObTabletFullDirectLoadMgr) : sizeof(ObTabletIncDirectLoadMgr);
        if (OB_ISNULL(buf = allocator.alloc(buf_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret), K(mgr_key));
        } else if (mgr_key.is_full_direct_load_) {
          direct_load_mgr = new (buf) ObTabletFullDirectLoadMgr();
        } else {
          direct_load_mgr = new (buf) ObTabletIncDirectLoadMgr(context_id);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(direct_load_mgr_handle.set_obj(direct_load_mgr))) {
          LOG_WARN("set direct load mgr failed", K(ret));
        }
        // cleanup if failed.
        if (OB_FAIL(ret)) {
          if (nullptr != direct_load_mgr) {
            direct_load_mgr->~ObTabletDirectLoadMgr();
            direct_load_mgr = nullptr;
          }
          if (buf != nullptr) {
            allocator.free(buf);
            buf = nullptr;
          }
        }
        // ownership of direct_load_mgr has been transferred to direct_load_mgr_handle
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(tablet_mgr_map_.set_refactored(mgr_key, direct_load_mgr))) {
          LOG_WARN("set tablet mgr failed", K(ret));
        } else {
          direct_load_mgr->inc_ref();
          LOG_INFO("create tablet direct load mgr", K(mgr_key), K(execution_id), K(major_sstable_exist));
        }
      }
    }
    if (OB_SUCC(ret) && need_set_exec_ctx) { // only build execution context map for data tablet
      exec_context.execution_id_ = execution_id;
      exec_context.start_scn_.reset();
      if (OB_FAIL(tablet_exec_context_map_.set_refactored(exec_id, exec_context, true /*overwrite*/))) {
        LOG_WARN("get table execution context failed", K(ret), K(exec_id));
      }
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::alloc_execution_context_id(
    int64_t &context_id)
{
  int ret = OB_SUCCESS;
  context_id = generate_context_id();
  return ret;
}

int ObTenantDirectLoadMgr::open_tablet_direct_load(
    const bool is_full_direct_load,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t context_id,
    SCN &start_scn,
    ObTabletDirectLoadMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTabletDirectLoadExecContextId exec_id;
  ObTabletDirectLoadExecContext exec_context;
  exec_id.tablet_id_ = tablet_id;
  exec_id.context_id_ = context_id;
  ObTabletDirectLoadMgrKey mgr_key(tablet_id, is_full_direct_load);
  bool is_mgr_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || context_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(context_id));
  } else if (OB_FAIL(get_tablet_mgr(tablet_id, is_full_direct_load, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_full_direct_load) {
      if (OB_FAIL(check_and_process_finished_tablet(ls_id, tablet_id))) {
        LOG_WARN("check and report checksum if need failed", K(ret), K(ls_id), K(tablet_id));
      }
    } else {
      LOG_WARN("get table mgr failed", K(ret), K(tablet_id), K(is_full_direct_load));
    }
  }
  // TODO(suzhi.yt) remove this later
  else if (!is_full_direct_load && context_id != handle.get_obj()->get_context_id()) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("tablet direct load context id not macth", K(ret), K(context_id), "current_context_id", handle.get_obj()->get_context_id());
  } else {
    is_mgr_exist = true;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_tablet_exec_context_with_rlock(exec_id, exec_context))) {
      LOG_WARN("get table execution context failed", K(ret), K(exec_id));
    }
  }

  if (OB_SUCC(ret) && is_mgr_exist) {
    if (OB_FAIL(handle.get_obj()->open(exec_context.execution_id_, start_scn))) {
      LOG_WARN("update tablet direct load failed", K(ret), K(is_full_direct_load), K(tablet_id), K(exec_context));
    }
  }

  if (OB_SUCC(ret)) {
    ObBucketHashWLockGuard guard(bucket_lock_, exec_id.hash());
    exec_context.start_scn_ = start_scn;
    if (OB_FAIL(tablet_exec_context_map_.set_refactored(exec_id, exec_context, true/*overwrite*/))) {
      LOG_WARN("get table execution context failed", K(ret), K(exec_id));
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::open_sstable_slice(
    const blocksstable::ObMacroDataSeq &start_seq,
    ObDirectLoadSliceInfo &slice_info)
{
  int ret = OB_SUCCESS;
  slice_info.slice_id_ = 0;
  ObTabletDirectLoadMgrHandle handle;
  const int64_t new_slice_id = generate_slice_id();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !start_seq.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info), K(start_seq));
  } else if (OB_FAIL(get_tablet_mgr(slice_info.data_tablet_id_, slice_info.is_full_direct_load_, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret && slice_info.is_full_direct_load_) {
      if (OB_FAIL(check_and_process_finished_tablet(slice_info.ls_id_, slice_info.data_tablet_id_))) {
        LOG_WARN("check and report checksum if need failed", K(ret), K(slice_info));
      }
    } else {
      LOG_WARN("get table mgr failed", K(ret), K(slice_info));
    }
  } else if (!slice_info.is_full_direct_load_ && slice_info.context_id_ != handle.get_obj()->get_context_id()) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("tablet direct load context id not macth", K(ret), "context_id", slice_info.context_id_, "current_context_id", handle.get_obj()->get_context_id());
  } else if (OB_FAIL(handle.get_obj()->open_sstable_slice(
    slice_info.is_lob_slice_/*is_data_tablet_process_for_lob*/, start_seq, slice_info.context_id_, new_slice_id))) {
    LOG_WARN("open sstable slice failed", K(ret), K(slice_info));
  }
  if (OB_SUCC(ret)) {
    // To simplify the logic of TabletDirectLoadMgr,
    // unique slice id is generated here.
    slice_info.slice_id_ = new_slice_id;
  }
  return ret;
}

int ObTenantDirectLoadMgr::fill_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    ObIStoreRowIterator *iter,
    int64_t &affected_rows,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  bool need_iter_part_row = false;
  ObTabletDirectLoadMgrHandle handle;
  ObTabletDirectLoadExecContext exec_context;
  ObTabletDirectLoadExecContextId exec_id;
  exec_id.tablet_id_ = slice_info.data_tablet_id_;
  exec_id.context_id_ = slice_info.context_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info));
  } else if (OB_FAIL(get_tablet_mgr(slice_info.data_tablet_id_, slice_info.is_full_direct_load_, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret && slice_info.is_full_direct_load_) {
      need_iter_part_row = true;
    } else {
      LOG_WARN("get table mgr failed", K(ret), K(slice_info));
    }
  } else if (OB_FAIL(get_tablet_exec_context_with_rlock(exec_id, exec_context))) {
    LOG_WARN("get tablet execution context failed", K(ret));
  } else if (!slice_info.is_full_direct_load_ && slice_info.context_id_ != handle.get_obj()->get_context_id()) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("tablet direct load context id not macth", K(ret), "context_id", slice_info.context_id_, "current_context_id", handle.get_obj()->get_context_id());
  } else if (OB_FAIL(handle.get_obj()->fill_sstable_slice(slice_info, exec_context.start_scn_, iter, affected_rows, insert_monitor))) {
    if (OB_TRANS_COMMITED == ret && slice_info.is_full_direct_load_) {
      need_iter_part_row = true;
    } else {
      LOG_WARN("fill sstable slice failed", K(ret), K(slice_info));
    }
  }

  if (need_iter_part_row &&
      OB_FAIL(check_and_process_finished_tablet(slice_info.ls_id_, slice_info.data_tablet_id_, iter))) {
    LOG_WARN("check and report checksum if need failed", K(ret), K(slice_info));
  }
  return ret;
}

int ObTenantDirectLoadMgr::fill_lob_sstable_slice(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info,
    share::ObTabletCacheInterval &pk_interval,
    const ObArray<int64_t> &lob_column_idxs,
    const ObArray<common::ObObjMeta> &col_types,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  ObTabletDirectLoadMgrHandle handle;
  ObTabletDirectLoadExecContext exec_context;
  ObTabletDirectLoadExecContextId exec_id;
  exec_id.tablet_id_ = slice_info.data_tablet_id_;
  exec_id.context_id_ = slice_info.context_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info));
  } else if (OB_FAIL(get_tablet_mgr(slice_info.data_tablet_id_, slice_info.is_full_direct_load_, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret && slice_info.is_full_direct_load_) {
      if (OB_FAIL(check_and_process_finished_tablet(slice_info.ls_id_, slice_info.data_tablet_id_))) {
        LOG_WARN("check and report checksum if need failed", K(ret), K(slice_info));
      }
    } else {
      LOG_WARN("get table mgr failed", K(ret), K(slice_info));
    }
  } else if (OB_FAIL(get_tablet_exec_context_with_rlock(exec_id, exec_context))) {
    LOG_WARN("get tablet execution context failed", K(ret));
  } else if (!slice_info.is_full_direct_load_ && slice_info.context_id_ != handle.get_obj()->get_context_id()) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("tablet direct load context id not macth", K(ret), "context_id", slice_info.context_id_, "current_context_id", handle.get_obj()->get_context_id());
  } else if (OB_FAIL(handle.get_obj()->fill_lob_sstable_slice(allocator, slice_info, exec_context.start_scn_, pk_interval, lob_column_idxs, col_types, datum_row))) {
    LOG_WARN("fail to fill batch sstable slice", KR(ret), K(slice_info), K(datum_row));
  }
  return ret;
}

int ObTenantDirectLoadMgr::calc_range(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t thread_cnt,
    const bool is_full_direct_load)
{
  int ret = OB_SUCCESS;
  ObTabletDirectLoadMgrHandle handle;
  bool is_major_sstable_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!is_full_direct_load)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected calc range in incremental direct load", K(ret));
  } else if (OB_FAIL(get_tablet_mgr_and_check_major(ls_id, tablet_id, is_full_direct_load, handle, is_major_sstable_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("direct load mgr not exist, but major sstable exist", K(ret), K(tablet_id));
    } else {
      LOG_WARN("get table mgr failed", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(handle.get_obj()->calc_range(thread_cnt))) {
    LOG_WARN("calc range failed", K(ret), K(thread_cnt));
  }
  return ret;
}

int ObTenantDirectLoadMgr::fill_column_group(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool is_full_direct_load,
    const int64_t thread_cnt,
    const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  ObTabletDirectLoadMgrHandle handle;
  bool is_major_sstable_exist = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || thread_cnt <= 0 || thread_id < 0 || thread_id > thread_cnt - 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(ls_id), K(tablet_id), K(thread_cnt), K(thread_id));
  } else if (OB_UNLIKELY(!is_full_direct_load)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected calc range in incremental direct load", K(ret));
  } else if (OB_FAIL(get_tablet_mgr_and_check_major(ls_id, tablet_id, is_full_direct_load, handle, is_major_sstable_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("direct load mgr not exist, but major sstable exist", K(ret), K(tablet_id));
    } else {
      LOG_WARN("get table mgr failed", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(handle.get_obj()->fill_column_group(thread_cnt, thread_id))) {
    LOG_WARN("fill sstable slice failed", K(ret), K(thread_cnt), K(thread_id));
  }
  return ret;
}

int ObTenantDirectLoadMgr::cancel(
    const int64_t context_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool is_full_direct_load)
{
  int ret = OB_SUCCESS;
  ObTabletDirectLoadMgrHandle handle;
  bool is_major_sstable_exist = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(context_id < 0 || !ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(context_id), K(ls_id), K(tablet_id));
  } else if (is_full_direct_load ) {
    if (OB_FAIL(get_tablet_mgr_and_check_major(ls_id, tablet_id, is_full_direct_load, handle, is_major_sstable_exist))) {
      if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
        ret = OB_TASK_EXPIRED;
        LOG_INFO("direct load mgr not exist, but major sstable exist", K(ret), K(tablet_id));
      } else {
        LOG_WARN("get table mgr failed", K(ret), K(tablet_id));
      }
    }
  } else {
    if (OB_FAIL(get_tablet_mgr(tablet_id, is_full_direct_load, handle))) {
      LOG_WARN("get table mgr failed", K(ret), K(tablet_id), K(is_full_direct_load));
    } else if (context_id != handle.get_obj()->get_context_id()) {
      ret = OB_TASK_EXPIRED;
      LOG_WARN("tablet direct load context id not macth", K(ret), K(context_id), "current_context_id", handle.get_obj()->get_context_id());
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(handle.get_obj()->cancel())) {
    LOG_WARN("cancel fill sstable slice failed", K(ret));
  }
  return ret;
}

int ObTenantDirectLoadMgr::close_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    ObInsertMonitor* insert_monitor,
    blocksstable::ObMacroDataSeq &next_seq)
{
  int ret = OB_SUCCESS;
  ObTabletDirectLoadMgrHandle handle;
  ObTabletDirectLoadExecContext exec_context;
  ObTabletDirectLoadExecContextId exec_id;
  exec_id.tablet_id_ = slice_info.data_tablet_id_;
  exec_id.context_id_ = slice_info.context_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info));
  } else if (OB_FAIL(get_tablet_mgr(slice_info.data_tablet_id_, slice_info.is_full_direct_load_, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret && slice_info.is_full_direct_load_) {
      if (OB_FAIL(check_and_process_finished_tablet(slice_info.ls_id_, slice_info.data_tablet_id_))) {
        LOG_WARN("check and report checksum if need failed", K(ret), K(slice_info));
      }
    } else {
      LOG_WARN("get table mgr failed", K(ret), K(slice_info));
    }
  } else if (OB_FAIL(get_tablet_exec_context_with_rlock(exec_id, exec_context))) {
    LOG_WARN("get tablet execution context failed", K(ret));
  } else if (OB_FAIL(handle.get_obj()->close_sstable_slice(
      slice_info.is_lob_slice_/*is_data_tablet_process_for_lob*/,
      slice_info,
      exec_context.start_scn_,
      exec_context.execution_id_,
      insert_monitor,
      next_seq))) {
    LOG_WARN("close sstable slice failed", K(ret), K(slice_info), "execution_start_scn", exec_context.start_scn_, "execution_id", exec_context.execution_id_);
  }
  return ret;
}

int ObTenantDirectLoadMgr::close_tablet_direct_load(
    const int64_t context_id,
    const bool is_full_direct_load,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool need_commit,
    const bool emergent_finish,
    const int64_t task_id,
    const int64_t table_id,
    const int64_t execution_id)
{
  int ret = OB_SUCCESS;
  UNUSED(emergent_finish);
  ObTabletDirectLoadMgrHandle handle;
  ObTabletDirectLoadMgrKey mgr_key(tablet_id, is_full_direct_load);
  ObTabletDirectLoadExecContextId exec_id;
  ObTabletDirectLoadExecContext exec_context;
  exec_id.tablet_id_ = tablet_id;
  exec_id.context_id_ = context_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || context_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(context_id));
  } else if (OB_FAIL(get_tablet_mgr(tablet_id, is_full_direct_load, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_full_direct_load) {
      if (OB_FAIL(check_and_process_finished_tablet(ls_id, tablet_id, nullptr/*row_iterator*/, task_id, table_id, execution_id))) {
        LOG_WARN("check and report checksum if need failed", K(ret), K(ls_id), K(tablet_id), K(task_id), K(execution_id));
      }
    } else {
      LOG_WARN("get table mgr failed", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (!is_full_direct_load && context_id != handle.get_obj()->get_context_id()) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("tablet direct load context id not macth", K(ret), K(context_id), "current_context_id", handle.get_obj()->get_context_id());
  } else if (need_commit) {
    ObTabletDirectLoadExecContext exec_context;
    if (OB_FAIL(get_tablet_exec_context_with_rlock(exec_id, exec_context))) {
      LOG_WARN("get exec context failed", K(ret), K(exec_id));
    } else if (OB_FAIL(handle.get_obj()->close(exec_context.execution_id_, exec_context.start_scn_))) {
      LOG_WARN("close failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_full_direct_load) {
      // For full direct load, the ObTabletDirectLoadMgr will be removed from MTL when,
      // 1. the direct load task abort indicated by `need_commit = false`, and we do not care about
      //    the error code triggered by the not found ObTabletDirectLoadMgr after.
      // 2. the direct load task commit and all ddl kvs persist successfully.

      // But how to notify the follower to remove it, with write commit failed log or tablet gc task ??

      if (nullptr != handle.get_full_obj()) {
        IGNORE_RETURN handle.get_full_obj()->cleanup_slice_writer(context_id); // remove slice writer of current context
      }
    } else {
      // For incremental direct load, the ObTabletDirectLoadMgr will be removed immediately
      ObTabletID lob_meta_tablet_id = handle.get_obj()->get_lob_meta_tablet_id();
      ObTabletDirectLoadMgrKey log_meta_mgr_key(lob_meta_tablet_id, is_full_direct_load);
      if (log_meta_mgr_key.is_valid() && OB_FAIL(remove_tablet_direct_load(log_meta_mgr_key, context_id))) {
        LOG_WARN("fail to remove lob meta tablet direct load", K(ret), K(log_meta_mgr_key), K(context_id));
      } else if (OB_FAIL(remove_tablet_direct_load(mgr_key, context_id))) {
        LOG_WARN("fail to remove tablet direct load", K(ret), K(mgr_key), K(context_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObBucketHashWLockGuard guard(bucket_lock_, exec_id.hash());
    if (OB_FAIL(tablet_exec_context_map_.erase_refactored(exec_id))) {
      LOG_WARN("erase tablet execution context failed", K(ret), K(exec_id));
    } else {
      LOG_INFO("erase execution context", K(exec_id), K(tablet_id));
    }
  }
  return ret;
}

// Other utils function.
int ObTenantDirectLoadMgr::get_online_stat_collect_result(
    const bool is_full_direct_load,
    const ObTabletID &tablet_id,
    const ObArray<ObOptColumnStat*> *&column_stat_array)
{
  int ret = OB_NOT_IMPLEMENT;
  // ObTableDirectLoadMgr *table_mgr = nullptr;
  // if (OB_FAIL(get_table_mgr(task_id, table_mgr))) {
  //   LOG_WARN("get context failed", K(ret));
  // } else if (OB_FAIL(table_mgr->get_online_stat_collect_result(tablet_id, column_stat_array))) {
  //   LOG_WARN("finish table context failed", K(ret));
  // }
  return ret;
}

int ObTenantDirectLoadMgr::get_tablet_cache_interval(
    const int64_t context_id,
    const ObTabletID &tablet_id,
    ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  ObTabletAutoincrementService &autoinc_service = ObTabletAutoincrementService::get_instance();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(context_id < 0 || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context_id), K(tablet_id));
  } else if (OB_FAIL(autoinc_service.get_tablet_cache_interval(MTL_ID(), interval))) {
    LOG_WARN("failed to get tablet cache intervals", K(ret));
  } else {
    ObTabletDirectLoadExecContext exec_context;
    ObTabletDirectLoadExecContextId exec_id;
    exec_id.tablet_id_ = tablet_id;
    exec_id.context_id_ = context_id;
    ObBucketHashWLockGuard guard(bucket_lock_, exec_id.hash());
    if (OB_FAIL(tablet_exec_context_map_.get_refactored(exec_id, exec_context))) {
      LOG_WARN("get tablet execution context failed", K(ret));
    } else {
      interval.task_id_ = exec_context.seq_interval_task_id_++;
      if (OB_FAIL(tablet_exec_context_map_.set_refactored(exec_id, exec_context, true/*overwrite*/))) {
        LOG_WARN("set tablet execution context map", K(ret));
      }
    }
  }

  return ret;
}

int get_co_column_checksums_if_need(
    ObTabletHandle &tablet_handle,
    const ObSSTable *sstable,
    ObIArray<int64_t> &column_checksum_array)
{
  int ret = OB_SUCCESS;
  column_checksum_array.reset();
  if (OB_UNLIKELY(!tablet_handle.is_valid() || nullptr == sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle), KP(sstable));
  } else if (!sstable->is_co_sstable()) {
    // do nothing
  } else {
    bool is_rowkey_based_co_sstable = false;
    ObStorageSchema *storage_schema = nullptr;
    ObArenaAllocator arena("co_ddl_cksm", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(arena, storage_schema))) {
      LOG_WARN("load storage schema failed", K(ret));
    } else if (OB_FAIL(ObCODDLUtil::is_rowkey_based_co_sstable(
            static_cast<const ObCOSSTableV2 *>(sstable), storage_schema, is_rowkey_based_co_sstable))) {
      LOG_WARN("check is rowkey based co sstable failed", K(ret));
    } else if (is_rowkey_based_co_sstable) {
      if (OB_FAIL(ObCODDLUtil::get_column_checksums(
                static_cast<const ObCOSSTableV2 *>(sstable),
                storage_schema,
                column_checksum_array))) {
        LOG_WARN("get column checksum from co sstable failed", K(ret));
      }
    }
    ObTabletObjLoadHelper::free(arena, storage_schema);
  }
  return ret;
}

int ObTenantDirectLoadMgr::check_and_process_finished_tablet(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObIStoreRowIterator *row_iter,
    const int64_t task_id,
    const int64_t table_id,
    const int64_t execution_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObSSTableMetaHandle sst_meta_hdl;
  const uint64_t tenant_id = MTL_ID();
  uint64_t data_format_version = 0;
  int64_t snapshot_version = 0;
  share::ObDDLTaskStatus unused_task_status = share::ObDDLTaskStatus::PREPARE;
  const ObSSTable *first_major_sstable = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const int64_t max_wait_timeout_us = 30L * 1000L * 1000L; // 30s
  ObTimeGuard tg("ddl_retry_tablet", max_wait_timeout_us);
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", K(ret), K(ls_id), K(tablet_id));
    } else if (tg.get_diff() > max_wait_timeout_us) {
      ret = OB_NEED_RETRY;
      LOG_WARN("process finished tablet timeout, need retry", K(ret), K(ls_id), K(tablet_id), K(tg));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
        tablet_id, tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
      LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_UNLIKELY(nullptr == tablet_handle.get_obj())) {
      ret = OB_ERR_SYS;
      LOG_WARN("tablet handle is null", K(ret), K(ls_id), K(tablet_id));
    } else if (task_id <= 0 || common::OB_INVALID_ID == table_id || execution_id < 0
      || tablet_handle.get_obj()->get_tablet_meta().ddl_execution_id_ > execution_id) {
      // no need to report checkksum.
      LOG_INFO("no need to report checksum", K(ret), K(task_id), K(table_id), K(execution_id),
        "tablet_meta", tablet_handle.get_obj()->get_tablet_meta());
      break;
    } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (FALSE_IT(first_major_sstable = static_cast<ObSSTable *>(
          table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
    } else if (nullptr == first_major_sstable) {
      LOG_INFO("major not exist, retry later", K(ret), K(ls_id), K(tablet_id), K(tg));
      usleep(100L * 1000L); // 100ms
    } else if (OB_FAIL(ObTabletDDLUtil::check_and_get_major_sstable(
        ls_id, tablet_id, first_major_sstable, table_store_wrapper))) {
      LOG_WARN("check if major sstable exist failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(first_major_sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_data_information(tenant_id, task_id, data_format_version, snapshot_version, unused_task_status))) {
      LOG_WARN("get ddl cluster version failed", K(ret), K(tenant_id), K(task_id));
    } else {
      const int64_t *column_checksums = sst_meta_hdl.get_sstable_meta().get_col_checksum();
      int64_t column_count = sst_meta_hdl.get_sstable_meta().get_col_checksum_cnt();
      ObArray<int64_t> co_column_checksums;
      co_column_checksums.set_attr(ObMemAttr(MTL_ID(), "TblDL_Ccc"));
      if (OB_FAIL(get_co_column_checksums_if_need(tablet_handle, first_major_sstable, co_column_checksums))) {
        LOG_WARN("get column checksum from co sstable failed", K(ret));
      } else if (OB_FAIL(ObTabletDDLUtil::report_ddl_checksum(
            ls_id,
            tablet_id,
            table_id,
            execution_id,
            task_id,
            co_column_checksums.empty() ? column_checksums : co_column_checksums.get_data(),
            co_column_checksums.empty() ? column_count : co_column_checksums.count(),
            data_format_version))) {
        LOG_WARN("report ddl column checksum failed", K(ret), K(ls_id), K(tablet_id), K(execution_id));
      } else {
        break;
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != row_iter) {
    ObDDLInsertRowIterator *ddl_row_iter = dynamic_cast<ObDDLInsertRowIterator*>(row_iter);
    ObDirectLoadInsertTableRowIterator *direct_load_row_iter = dynamic_cast<ObDirectLoadInsertTableRowIterator*>(row_iter);
    const ObDatumRow *row = nullptr;
    const bool skip_lob = true;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else {
        if (ddl_row_iter != nullptr) {
          ret = ddl_row_iter->get_next_row(skip_lob, row);
        } else if (direct_load_row_iter != nullptr) {
          ret = direct_load_row_iter->get_next_row(skip_lob, row);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrong type of row iter", K(typeid(*row_iter).name()), KP(row_iter), KR(ret));
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("iter row failed", K(ret), K(task_id), K(execution_id), K(ls_id), K(tablet_id));
        }
      }
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::get_tablet_mgr_and_check_major(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool is_full_direct_load,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    bool &is_major_sstable_exist)
{
  int ret = get_tablet_mgr(tablet_id, is_full_direct_load, direct_load_mgr_handle);
  is_major_sstable_exist = false;
  if (OB_ENTRY_NOT_EXIST == ret) {
    int tmp_ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    if (OB_TMP_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(tmp_ret), K(ls_id));
    } else if (OB_TMP_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle))) {
      LOG_WARN("get tablet handle failed", K(tmp_ret), K(ls_id), K(tablet_id));
    } else {
      is_major_sstable_exist = tablet_handle.get_obj()->get_major_table_count() > 0
        || tablet_handle.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable();
    }
    if (!is_major_sstable_exist) {
      ret = OB_TASK_EXPIRED;
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::get_tablet_mgr(
    const ObTabletID &tablet_id,
    const bool is_full_direct_load,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else {
    ObTabletDirectLoadMgrKey mgr_key(tablet_id, is_full_direct_load);
    ObBucketHashRLockGuard guard(bucket_lock_, mgr_key.hash());
    if (OB_FAIL(get_tablet_mgr_no_lock(mgr_key, direct_load_mgr_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get table mgr without lock failed", K(ret), K(mgr_key));
      }
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::get_tablet_mgr_no_lock(
    const ObTabletDirectLoadMgrKey &mgr_key,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObTabletDirectLoadMgr *tablet_mgr = nullptr;
  if (OB_UNLIKELY(!mgr_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(mgr_key));
  } else if (OB_FAIL(tablet_mgr_map_.get_refactored(mgr_key, tablet_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get refactored failed", K(ret), K(mgr_key));
    } else {
      ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    }
  } else if (OB_FAIL(direct_load_mgr_handle.set_obj(tablet_mgr))) {
    LOG_WARN("set handle failed", K(ret), K(mgr_key));
  } else if (!direct_load_mgr_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(mgr_key));
  }
  return ret;
}

int ObTenantDirectLoadMgr::get_tablet_exec_context_with_rlock(
    const ObTabletDirectLoadExecContextId &exec_id,
    ObTabletDirectLoadExecContext &exec_context)
{
  int ret = OB_SUCCESS;
  exec_context.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!exec_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exec_id));
  } else {
    ObBucketHashRLockGuard guard(bucket_lock_, exec_id.hash());
    if (OB_FAIL(tablet_exec_context_map_.get_refactored(exec_id, exec_context))) {
      LOG_WARN("get refactored failed", K(ret), K(exec_id));
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::GetGcCandidateOp::operator() (common::hash::HashMapPair<ObTabletDirectLoadMgrKey, ObTabletDirectLoadMgr *> &kv)
{
  int ret = OB_SUCCESS;
  const ObTabletDirectLoadMgrKey &key = kv.first;
  ObTabletDirectLoadMgr *tablet_direct_load_mgr = kv.second;
  if (1 == tablet_direct_load_mgr->get_ref()) {
    if (OB_FAIL(candidate_mgrs_.push_back(std::make_pair(tablet_direct_load_mgr->get_ls_id(), key)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::gc_tablet_direct_load()
{
  int ret = OB_SUCCESS;
  if (!tablet_mgr_map_.empty() && ObDDLUtil::reach_time_interval(10 * 1000 * 1000, last_gc_time_)) {
    ObSEArray<std::pair<share::ObLSID, ObTabletDirectLoadMgrKey>, 8> candidate_mgrs;
    {
      ObBucketTryRLockAllGuard guard(bucket_lock_);
      if (OB_SUCC(guard.get_ret())) {
        GetGcCandidateOp op(candidate_mgrs);
        (void)tablet_mgr_map_.foreach_refactored(op);
      }
    }

    for (int64_t i = 0; i < candidate_mgrs.count(); i++) { // overwrite ret
      const share::ObLSID &ls_id = candidate_mgrs.at(i).first;
      const ObTabletDirectLoadMgrKey &mgr_key = candidate_mgrs.at(i).second;
      ObLSService *ls_svr = MTL(ObLSService*);
      ObLS *ls = nullptr;
      ObLSHandle ls_handle;
      ObTabletHandle tablet_handle;
      if (!mgr_key.is_full_direct_load_) {
        // skip
      } else if (OB_ISNULL(ls_svr)) {
        ret = OB_ERR_SYS;
        LOG_WARN("invalid mtl ObLSService", K(ret));
      } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
        LOG_WARN("get log stream failed", K(ret), K(ls_id));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log stream not exist", K(ret));
      } else if (OB_FAIL(ls->get_tablet(mgr_key.tablet_id_, tablet_handle,
              ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(mgr_key));
      } else if (tablet_handle.get_obj()->get_major_table_count() > 0) {
        (void)remove_tablet_direct_load(mgr_key);
      }
    }
  }
  return ret;
}

int ObTenantDirectLoadMgr::remove_tablet_direct_load(const ObTabletDirectLoadMgrKey &mgr_key, int64_t context_id)
{
  ObBucketHashWLockGuard guard(bucket_lock_, mgr_key.hash());
  return remove_tablet_direct_load_nolock(mgr_key, context_id);
}

int ObTenantDirectLoadMgr::remove_tablet_direct_load_nolock(const ObTabletDirectLoadMgrKey &mgr_key, int64_t context_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!mgr_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(mgr_key));
  } else {
    ObTabletDirectLoadMgr *tablet_direct_load_mgr = nullptr;
    if (OB_FAIL(tablet_mgr_map_.get_refactored(mgr_key, tablet_direct_load_mgr))) {
      ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
      LOG_TRACE("get table mgr failed", K(ret), K(mgr_key), K(common::lbt()));
    } else if (OB_ISNULL(tablet_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(mgr_key));
    } else if (!mgr_key.is_full_direct_load_ && context_id > 0 && context_id != tablet_direct_load_mgr->get_context_id()) {
      // context_id not match, ignore
    } else if (OB_FAIL(tablet_mgr_map_.erase_refactored(mgr_key))) {
      LOG_WARN("erase from map failed", K(ret));
    } else {
      LOG_INFO("remove tablet direct load mgr from MTL", K(ret), K(mgr_key), K(common::lbt()), K(tablet_direct_load_mgr->get_ref()));
      if (0 == tablet_direct_load_mgr->dec_ref()) {
        tablet_direct_load_mgr->~ObTabletDirectLoadMgr();
        allocator_.free(tablet_direct_load_mgr);
      } else {
        // unreachable
      }
    }
  }
  return ret;
}

struct DestroySliceWriterMapFn
{
public:
  DestroySliceWriterMapFn(ObIAllocator *allocator, const int64_t context_id = -1) :allocator_(allocator), context_id_(context_id) {}
  int operator () (hash::HashMapPair<ObTabletDirectLoadBuildCtx::SliceKey, ObDirectLoadSliceWriter *> &entry) {
    int ret = OB_SUCCESS;
    if (nullptr != allocator_) {
      if (nullptr != entry.second && (-1 == context_id_ || entry.first.context_id_ == context_id_)) {
        LOG_INFO("erase a slice writer", K(&entry.second), "slice_id", entry.first, K(context_id_));
        entry.second->~ObDirectLoadSliceWriter();
        allocator_->free(entry.second);
        entry.second = nullptr;
      }
    }
    return ret;
  }

private:
  ObIAllocator *allocator_;
  int64_t context_id_;
};

ObTabletDirectLoadBuildCtx::ObTabletDirectLoadBuildCtx()
  : allocator_(), slice_writer_allocator_(), build_param_(), slice_mgr_map_(), data_block_desc_(true/*is ddl*/), index_builder_(nullptr),
    column_stat_array_(), sorted_slice_writers_(), sorted_slices_idx_(), is_task_end_(false), task_finish_count_(0), fill_column_group_finish_count_(0),
    commit_scn_(), schema_allocator_("TDL_schema", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), storage_schema_(nullptr)
{
  column_stat_array_.set_attr(ObMemAttr(MTL_ID(), "TblDL_CSA"));
  sorted_slice_writers_.set_attr(ObMemAttr(MTL_ID(), "TblDL_SSR"));
  sorted_slices_idx_.set_attr(ObMemAttr(MTL_ID(), "TblDL_IDX"));
}

ObTabletDirectLoadBuildCtx::~ObTabletDirectLoadBuildCtx()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(index_builder_)) {
    index_builder_->~ObSSTableIndexBuilder();
    allocator_.free(index_builder_);
    index_builder_ = nullptr;
  }
  ObTabletObjLoadHelper::free(schema_allocator_, storage_schema_);
  storage_schema_ = nullptr;
  schema_allocator_.reset();
  commit_scn_.reset();
  for (int64_t i = 0; i < column_stat_array_.count(); i++) {
    ObOptColumnStat *col_stat = column_stat_array_.at(i);
    col_stat->~ObOptColumnStat();
    allocator_.free(col_stat);
    col_stat = nullptr;
  }
  column_stat_array_.reset();
  sorted_slice_writers_.reset();
  sorted_slices_idx_.reset();

  if (!slice_mgr_map_.empty()) {
    DestroySliceWriterMapFn destroy_map_fn(&slice_writer_allocator_);
    slice_mgr_map_.foreach_refactored(destroy_map_fn);
    slice_mgr_map_.destroy();
  }
  allocator_.reset();
  slice_writer_allocator_.reset();
}

bool ObTabletDirectLoadBuildCtx::is_valid() const
{
  return build_param_.is_valid();
}

void ObTabletDirectLoadBuildCtx::reset_slice_ctx_on_demand()
{
  ATOMIC_STORE(&task_finish_count_, 0);
  ATOMIC_STORE(&fill_column_group_finish_count_, 0);
  ATOMIC_STORE(&task_total_cnt_, build_param_.runtime_only_param_.task_cnt_);
}

void ObTabletDirectLoadBuildCtx::cleanup_slice_writer(const int64_t context_id)
{
  if (!slice_mgr_map_.empty()) {
    DestroySliceWriterMapFn destroy_map_fn(&slice_writer_allocator_, context_id);
    slice_mgr_map_.foreach_refactored(destroy_map_fn);
  }
  LOG_INFO("cleanup slice writer of current context", K(context_id), K(build_param_));
}

ObTabletDirectLoadMgr::ObTabletDirectLoadMgr()
  : is_inited_(false), is_schema_item_ready_(false), ls_id_(), tablet_id_(), table_key_(), data_format_version_(0),
    lock_(), ref_cnt_(0), direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), sqc_build_ctx_(),
    column_items_(), lob_column_idxs_(), lob_col_types_(), schema_item_(), dir_id_(0)
{
  column_items_.set_attr(ObMemAttr(MTL_ID(), "DL_schema"));
  lob_column_idxs_.set_attr(ObMemAttr(MTL_ID(), "DL_schema"));
  lob_col_types_.set_attr(ObMemAttr(MTL_ID(), "DL_schema"));
}

ObTabletDirectLoadMgr::~ObTabletDirectLoadMgr()
{
  FLOG_INFO("deconstruct tablet direct load mgr", KP(this), KPC(this));
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  is_inited_ = false;
  ls_id_.reset();
  tablet_id_.reset();
  table_key_.reset();
  data_format_version_ = 0;
  ATOMIC_STORE(&ref_cnt_, 0);
  direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  column_items_.reset();
  lob_column_idxs_.reset();
  lob_col_types_.reset();
  schema_item_.reset();
  is_schema_item_ready_ = false;
}

bool ObTabletDirectLoadMgr::is_valid()
{
  return is_inited_ == true && ls_id_.is_valid() && tablet_id_.is_valid()
      && is_valid_direct_load(direct_load_type_);
}

int ObTabletDirectLoadMgr::update(
    ObTabletDirectLoadMgr *lob_tablet_mgr,
    const ObTabletDirectLoadInsertParam &build_param)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 97L; // 97
  const int64_t memory_limit = 1024L * 1024L * 1024L * 10L; // 10GB
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!build_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(build_param));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(build_param.common_param_.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(build_param));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               build_param.common_param_.tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(build_param));
  } else if (OB_FAIL(prepare_storage_schema(tablet_handle))) {
    LOG_WARN("fail to prepare storage schema", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(sqc_build_ctx_.storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null storage schema", K(ret));
  } else if (nullptr != lob_tablet_mgr) {
    // has lob
    ObTabletDirectLoadInsertParam lob_param;
    ObSchemaGetterGuard schema_guard;
    ObTabletBindingMdsUserData ddl_data;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(lob_param.assign(build_param))) {
      LOG_WARN("assign lob parameter failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
      LOG_WARN("get ddl data failed", K(ret));
    } else if (OB_FALSE_IT(lob_param.common_param_.tablet_id_ = ddl_data.lob_meta_tablet_id_)) {
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      MTL_ID(), schema_guard, lob_param.runtime_only_param_.schema_version_))) {
      LOG_WARN("get tenant schema failed", K(ret), K(MTL_ID()), K(lob_param));
    } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(),
              lob_param.runtime_only_param_.table_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(lob_param));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(lob_param));
    } else {
      lob_param.runtime_only_param_.table_id_ = table_schema->get_aux_lob_meta_tid();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(lob_mgr_handle_.set_obj(lob_tablet_mgr))) {
      LOG_WARN("set lob direct load mgr failed", K(ret), K(lob_param));
    } else if (OB_FAIL(lob_mgr_handle_.get_obj()->update(nullptr, lob_param))) {
      LOG_WARN("init lob failed", K(ret), K(lob_param));
    } else {
      LOG_INFO("set lob mgr handle", K(lob_param));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!build_param.is_replay_ && !sqc_build_ctx_.slice_mgr_map_.created()) {
    // 1. Create slice_mgr_map if the tablet_direct_load_mgr is created firstly.
    // 2. Create slice_mgr_map if the node is switched from follower to leader.
    const uint64_t tenant_id = MTL_ID();
    lib::ObMemAttr attr(tenant_id, "TabletDLMgr");
    lib::ObMemAttr slice_writer_attr(tenant_id, "SliceWriter");
    lib::ObMemAttr slice_writer_map_attr(tenant_id, "SliceWriterMap");
    if (OB_FAIL(sqc_build_ctx_.allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE,
      attr.label_, tenant_id, memory_limit))) {
      LOG_WARN("init alloctor failed", K(ret));
    } else if (OB_FAIL(sqc_build_ctx_.slice_writer_allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE,
      slice_writer_attr.label_, tenant_id, memory_limit))) {
      LOG_WARN("init allocator failed", K(ret));
    } else if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.create(bucket_num,
                                                      slice_writer_map_attr, slice_writer_map_attr))) {
      LOG_WARN("create slice writer map failed", K(ret));
    } else if (OB_FAIL(cond_.init(ObWaitEventIds::COLUMN_STORE_DDL_RESCAN_LOCK_WAIT))) {
      LOG_WARN("init condition failed", K(ret));
    } else {
      sqc_build_ctx_.allocator_.set_attr(attr);
      sqc_build_ctx_.slice_writer_allocator_.set_attr(slice_writer_attr);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sqc_build_ctx_.build_param_.assign(build_param))) {
      LOG_WARN("assign build param failed", K(ret));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
      LOG_WARN("alloc dir id failed", K(ret));
    } else {
      ls_id_ = build_param.common_param_.ls_id_;
      tablet_id_ = build_param.common_param_.tablet_id_;
      direct_load_type_ = build_param.common_param_.direct_load_type_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::open_sstable_slice(
    const bool is_data_tablet_process_for_lob,
    const blocksstable::ObMacroDataSeq &start_seq,
    const int64_t context_id,
    const int64_t slice_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!start_seq.is_valid() || context_id < 0 || slice_id <= 0 || !sqc_build_ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tablet_id_), K(start_seq), K(slice_id), K(sqc_build_ctx_), K(context_id));
  } else if (is_data_tablet_process_for_lob) {
    if (OB_UNLIKELY(!lob_mgr_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), KPC(this));
    } else if (OB_FAIL(lob_mgr_handle_.get_obj()->open_sstable_slice(
        false, start_seq, context_id, slice_id))) {
      LOG_WARN("open sstable slice for lob failed", K(ret), KPC(this));
    }
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_FAIL(prepare_schema_item_on_demand(sqc_build_ctx_.build_param_.runtime_only_param_.table_id_,
                                                   sqc_build_ctx_.build_param_.runtime_only_param_.parallel_))) {
    LOG_WARN("prepare table schema item on demand", K(ret), K(sqc_build_ctx_.build_param_));
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    if (OB_ISNULL(slice_writer = OB_NEWx(ObDirectLoadSliceWriter, (&sqc_build_ctx_.slice_writer_allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadSliceWriter", KR(ret));
    } else if (OB_FAIL(slice_writer->init(this, start_seq))) {
      LOG_WARN("init sstable slice writer failed", K(ret), KPC(this));
    } else if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.set_refactored(ObTabletDirectLoadBuildCtx::SliceKey(context_id, slice_id), slice_writer))) {
      LOG_WARN("set refactored failed", K(ret), K(slice_id), KPC(this));
    } else {
      LOG_INFO("add a slice writer", KP(slice_writer), K(context_id), K(slice_id), K(sqc_build_ctx_.slice_mgr_map_.size()));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(slice_writer)) {
        slice_writer->~ObDirectLoadSliceWriter();
        sqc_build_ctx_.slice_writer_allocator_.free(slice_writer);
        slice_writer = nullptr;
      }
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::prepare_schema_item_on_demand(const uint64_t table_id,
                                                         const int64_t parallel)
{
  int ret = OB_SUCCESS;
  bool is_schema_item_ready = ATOMIC_LOAD(&is_schema_item_ready_);
  if (!is_schema_item_ready) {
    ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    is_schema_item_ready = is_schema_item_ready_;
  }
  if (!is_schema_item_ready) {
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    if (is_schema_item_ready_) {
      // do nothing
    } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(table_id));
    } else {
      const uint64_t tenant_id = MTL_ID();
      ObSchemaGetterGuard schema_guard;
      const ObDataStoreDesc &data_desc = sqc_build_ctx_.data_block_desc_.get_desc();
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get tenant schema failed", K(ret), K(tenant_id), K(table_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(tenant_id), K(table_id));
      } else if (OB_FAIL(prepare_index_builder_if_need(*table_schema))) {
        LOG_WARN("prepare sstable index builder failed", K(ret), K(sqc_build_ctx_));
      } else if (OB_FAIL(table_schema->get_is_column_store(schema_item_.is_column_store_))) {
        LOG_WARN("fail to get is column store", K(ret));
      } else {
        schema_item_.is_index_table_ = table_schema->is_index_table();
        schema_item_.rowkey_column_num_ = table_schema->get_rowkey_column_num();
        schema_item_.is_unique_index_ = table_schema->is_unique_index();
        schema_item_.lob_inrow_threshold_ = table_schema->get_lob_inrow_threshold();

        if (OB_FAIL(column_items_.reserve(data_desc.get_col_desc_array().count()))) {
          LOG_WARN("reserve column schema array failed", K(ret), K(data_desc.get_col_desc_array().count()), K(column_items_));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < data_desc.get_col_desc_array().count(); ++i) {
            const ObColDesc &col_desc = data_desc.get_col_desc_array().at(i);
            const schema::ObColumnSchemaV2 *column_schema = nullptr;
            ObColumnSchemaItem column_item;
            if (i >= table_schema->get_rowkey_column_num() && i < table_schema->get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
              // skip multi version column, keep item invalid
            } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(col_desc.col_id_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema is null", K(ret), K(i), K(data_desc.get_col_desc_array()), K(col_desc.col_id_));
            } else {
              column_item.is_valid_ = true;
              column_item.col_type_ = column_schema->get_meta_type();
              column_item.col_accuracy_ = column_schema->get_accuracy();
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(column_items_.push_back(column_item))) {
                LOG_WARN("push back null column schema failed", K(ret));
              } else if (OB_NOT_NULL(column_schema) && column_schema->get_meta_type().is_lob_storage()) { // not multi version column
                if (OB_FAIL(lob_column_idxs_.push_back(i))) {
                  LOG_WARN("push back lob column idx failed", K(ret), K(i));
                } else if (OB_FAIL(lob_col_types_.push_back(column_schema->get_meta_type()))) {
                  LOG_WARN("push back lob col_type  failed", K(ret), K(i));
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        is_schema_item_ready_ = true;
      }
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::fill_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    const SCN &start_scn,
    ObIStoreRowIterator *iter,
    int64_t &affected_rows,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  share::SCN commit_scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !start_scn.is_valid_and_not_min()) || !sqc_build_ctx_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info), K(start_scn), K(sqc_build_ctx_));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (is_full_direct_load(direct_load_type_)) {
    if (sqc_build_ctx_.commit_scn_.is_valid_and_not_min()) {
      ret = OB_TRANS_COMMITED;
      FLOG_INFO("already committed", K(commit_scn), KPC(this));
    } else if (start_scn != get_start_scn()) {
      ret = OB_TASK_EXPIRED;
      LOG_WARN("task expired", K(ret), "start_scn of current execution", start_scn, "start_scn latest", get_start_scn());
    }
  }
  if (OB_SUCC(ret)) {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      LOG_WARN("get refactored failed", K(ret), K(slice_info));
    } else if (OB_ISNULL(slice_writer) || OB_UNLIKELY(!ATOMIC_LOAD(&is_schema_item_ready_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info), K(is_schema_item_ready_));
    } else if (OB_FAIL(slice_writer->fill_sstable_slice(start_scn, sqc_build_ctx_.build_param_.runtime_only_param_.table_id_, tablet_id_,
        sqc_build_ctx_.storage_schema_, iter, schema_item_, direct_load_type_, column_items_, dir_id_,
        sqc_build_ctx_.build_param_.runtime_only_param_.parallel_, affected_rows, insert_monitor))) {
      LOG_WARN("fill sstable slice failed", K(ret), KPC(this));
    }
  }
  if (OB_FAIL(ret) && (OB_TRANS_COMMITED != ret)) {
    // cleanup when failed.
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_TMP_FAIL(sqc_build_ctx_.slice_mgr_map_.erase_refactored(slice_key, &slice_writer))) {
      LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
    } else {
      LOG_INFO("erase a slice writer", KP(slice_writer), K(slice_key), K(sqc_build_ctx_.slice_mgr_map_.size()));
      slice_writer->~ObDirectLoadSliceWriter();
      sqc_build_ctx_.slice_writer_allocator_.free(slice_writer);
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::fill_lob_sstable_slice(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info,
    const SCN &start_scn,
    share::ObTabletCacheInterval &pk_interval,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  share::SCN commit_scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !sqc_build_ctx_.is_valid() || !start_scn.is_valid_and_not_min() ||
      !lob_mgr_handle_.is_valid() || !lob_mgr_handle_.get_obj()->get_sqc_build_ctx().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info), "lob_direct_load_mgr is valid", lob_mgr_handle_.is_valid(), KPC(this), K(start_scn));
  } else if (is_full_direct_load(direct_load_type_)) {
    if (sqc_build_ctx_.commit_scn_.is_valid_and_not_min()) {
      ret = OB_TRANS_COMMITED;
      FLOG_INFO("already committed", K(commit_scn), KPC(this));
    } else if (start_scn != get_start_scn()) {
      ret = OB_TASK_EXPIRED;
      LOG_WARN("task expired", K(ret), "start_scn of current execution", start_scn, "start_scn latest", get_start_scn());
    }
  }

  if (OB_SUCC(ret)) {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    const int64_t trans_version = is_full_direct_load(direct_load_type_) ? table_key_.get_snapshot_version() : INT64_MAX;
    ObBatchSliceWriteInfo info(tablet_id_, ls_id_, trans_version, direct_load_type_, sqc_build_ctx_.build_param_.runtime_only_param_.trans_id_,
        sqc_build_ctx_.build_param_.runtime_only_param_.seq_no_, slice_info.src_tenant_id_,
        sqc_build_ctx_.build_param_.runtime_only_param_.tx_desc_);

    if (OB_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      LOG_WARN("get refactored failed", K(ret), K(slice_info), K(sqc_build_ctx_.slice_mgr_map_.size()));
    } else if (OB_ISNULL(slice_writer) || OB_UNLIKELY(!ATOMIC_LOAD(&(lob_mgr_handle_.get_obj()->is_schema_item_ready_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info), K(lob_mgr_handle_.get_obj()->is_schema_item_ready_));
    } else if (OB_FAIL(slice_writer->fill_lob_sstable_slice(lob_mgr_handle_.get_obj()->sqc_build_ctx_.build_param_.runtime_only_param_.table_id_, allocator, sqc_build_ctx_.allocator_,
          start_scn, info, pk_interval, lob_column_idxs_, lob_col_types_, schema_item_.lob_inrow_threshold_, datum_row))) {
        LOG_WARN("fail to fill batch sstable slice", K(ret), K(start_scn), K(tablet_id_), K(pk_interval));
    }
  }
  if (OB_FAIL(ret) && lob_mgr_handle_.is_valid()) {
    // cleanup when failed.
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_TMP_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.erase_refactored(slice_key, &slice_writer))) {
      LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
    } else {
      LOG_INFO("erase a slice writer", KP(slice_writer), K(slice_key), K(sqc_build_ctx_.slice_mgr_map_.size()));
      slice_writer->~ObDirectLoadSliceWriter();
      lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_writer_allocator_.free(slice_writer);
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::fill_lob_sstable_slice(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info,
    const SCN &start_scn,
    share::ObTabletCacheInterval &pk_interval,
    const ObArray<int64_t> &lob_column_idxs,
    const ObArray<common::ObObjMeta> &col_types,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  share::SCN commit_scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !sqc_build_ctx_.is_valid() || !start_scn.is_valid_and_not_min() ||
      !lob_mgr_handle_.is_valid() || !lob_mgr_handle_.get_obj()->get_sqc_build_ctx().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info), "lob_direct_load_mgr is valid", lob_mgr_handle_.is_valid(), KPC(this), K(start_scn));
  } else if (is_full_direct_load(direct_load_type_)) {
    if (sqc_build_ctx_.commit_scn_.is_valid_and_not_min()) {
      ret = OB_TRANS_COMMITED;
      FLOG_INFO("already committed", K(sqc_build_ctx_.commit_scn_), KPC(this));
    } else if (start_scn != get_start_scn()) {
      ret = OB_TASK_EXPIRED;
      LOG_WARN("task expired", K(ret), "start_scn of current execution", start_scn, "start_scn latest", get_start_scn());
    }
  }

  if (OB_SUCC(ret)) {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    const int64_t trans_version = is_full_direct_load(direct_load_type_) ? table_key_.get_snapshot_version() : INT64_MAX;
    ObBatchSliceWriteInfo info(tablet_id_, ls_id_, trans_version, direct_load_type_, sqc_build_ctx_.build_param_.runtime_only_param_.trans_id_,
        sqc_build_ctx_.build_param_.runtime_only_param_.seq_no_, slice_info.src_tenant_id_,
        sqc_build_ctx_.build_param_.runtime_only_param_.tx_desc_);

    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      LOG_WARN("get refactored failed", K(ret), K(slice_info), K(sqc_build_ctx_.slice_mgr_map_.size()));
    } else if (OB_ISNULL(slice_writer) || OB_UNLIKELY(!ATOMIC_LOAD(&(lob_mgr_handle_.get_obj()->is_schema_item_ready_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info), K(lob_mgr_handle_.get_obj()->is_schema_item_ready_));
    } else if (OB_FAIL(slice_writer->fill_lob_sstable_slice(lob_mgr_handle_.get_obj()->sqc_build_ctx_.build_param_.runtime_only_param_.table_id_,
        allocator, sqc_build_ctx_.allocator_, start_scn, info,
        pk_interval, lob_column_idxs, col_types, schema_item_.lob_inrow_threshold_, datum_row))) {
      LOG_WARN("fail to fill batch sstable slice", K(ret), K(start_scn), K(tablet_id_), K(pk_interval));
    }
  }
  if (OB_FAIL(ret) && lob_mgr_handle_.is_valid()) {
    // cleanup when failed.
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_TMP_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.erase_refactored(slice_key, &slice_writer))) {
      LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
    } else {
      LOG_INFO("erase a slice writer", KP(slice_writer), K(slice_key), K(sqc_build_ctx_.slice_mgr_map_.size()));
      slice_writer->~ObDirectLoadSliceWriter();
      lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_writer_allocator_.free(slice_writer);
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::fill_lob_meta_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    const share::SCN &start_scn,
    ObIStoreRowIterator *iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  share::SCN commit_scn;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !sqc_build_ctx_.is_valid() || !start_scn.is_valid_and_not_min() ||
      !lob_mgr_handle_.is_valid() || !lob_mgr_handle_.get_obj()->get_sqc_build_ctx().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info), "lob_direct_load_mgr is valid", lob_mgr_handle_.is_valid(), KPC(this), K(start_scn));
  } else if (OB_UNLIKELY(!is_incremental_direct_load(direct_load_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected direct load type", K(ret), K(direct_load_type_));
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      LOG_WARN("get refactored failed", K(ret), K(slice_info), K(sqc_build_ctx_.slice_mgr_map_.size()));
    } else if (OB_ISNULL(slice_writer) || OB_UNLIKELY(!ATOMIC_LOAD(&(lob_mgr_handle_.get_obj()->is_schema_item_ready_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info), K(lob_mgr_handle_.get_obj()->is_schema_item_ready_));
    } else if (OB_FAIL(slice_writer->fill_lob_meta_sstable_slice(start_scn,
                                                                 lob_mgr_handle_.get_obj()->sqc_build_ctx_.build_param_.runtime_only_param_.table_id_,
                                                                 lob_mgr_handle_.get_obj()->tablet_id_,
                                                                 iter,
                                                                 affected_rows))) {
      LOG_WARN("fail to fill lob meta sstable slice", K(ret), K(start_scn), K(tablet_id_));
    }
  }
  if (OB_FAIL(ret) && lob_mgr_handle_.is_valid()) {
    // cleanup when failed.
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_TMP_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.erase_refactored(slice_key, &slice_writer))) {
      LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
    } else {
      LOG_INFO("erase a slice writer", KP(slice_writer), K(slice_key), K(sqc_build_ctx_.slice_mgr_map_.size()));
      slice_writer->~ObDirectLoadSliceWriter();
      lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_writer_allocator_.free(slice_writer);
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::wait_notify(const ObDirectLoadSliceWriter *slice_writer, const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(slice_writer) || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(slice_writer), K(start_scn));
  } else {
    while (OB_SUCC(ret)) {
      const SCN tablet_start_scn = get_start_scn();
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (start_scn != tablet_start_scn) {
        ret = OB_TASK_EXPIRED;
        LOG_WARN("task expired", K(ret), K(start_scn), K(tablet_start_scn));
      } else if (slice_writer->get_row_offset() >= 0) {
        // row offset already set
        break;
      } else {
        const int64_t wait_interval_ms = 100L;
        ObThreadCondGuard guard(cond_);
        if (OB_FAIL(cond_.wait(wait_interval_ms))) {
          if (OB_TIMEOUT != ret) {
            LOG_WARN("wait thread condition failed", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret)) {
          ObTabletDirectLoadMgrHandle handle;
          if (OB_FAIL(MTL(ObTenantDirectLoadMgr *)->get_tablet_mgr(tablet_id_, is_full_direct_load(direct_load_type_), handle))) {
            if (OB_ENTRY_NOT_EXIST == ret && is_full_direct_load(direct_load_type_)) {
              ret = OB_TASK_EXPIRED; //retry by RS
              LOG_WARN("task expired", K(ret), "start_scn of current execution", start_scn, "start_scn latest", get_start_scn());
              break;
            } else {
              LOG_WARN("get table mgr failed", K(ret), K(tablet_id_), K(direct_load_type_));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::notify_all()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(cond_.broadcast())) {
      LOG_WARN("broadcast thread condition failed", K(ret));
    }
  }
  return ret;
}

struct SliceEndkeyCompareFunctor
{
public:
  SliceEndkeyCompareFunctor(const ObStorageDatumUtils &datum_utils) : datum_utils_(datum_utils), ret_code_(OB_SUCCESS) {}
  bool operator ()(const ObDirectLoadSliceWriter *left, const ObDirectLoadSliceWriter *right)
  {
    bool bret = false;
    int ret = ret_code_;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (!left->is_empty() && !right->is_empty()) {
      const ObChunkSliceStore *left_slice_store = static_cast<const ObChunkSliceStore *>(left->get_slice_store());
      const ObChunkSliceStore *right_slice_store = static_cast<const ObChunkSliceStore *>(right->get_slice_store());
      int cmp_ret = 0;
      if (OB_FAIL(left_slice_store->endkey_.compare(right_slice_store->endkey_, datum_utils_, cmp_ret))) {
        LOG_WARN("endkey compare failed", K(ret));
      } else {
        bret = cmp_ret < 0;
      }
    } else if (left->is_empty() && right->is_empty()) {
      // both empty, compare pointer
      bret = left < right;
    } else {
      // valid formmer, empty latter
      bret = !left->is_empty();
    }
    ret_code_ = OB_SUCCESS == ret_code_ ? ret : ret_code_;
    return bret;
  }
public:
  const ObStorageDatumUtils &datum_utils_;
  int ret_code_;
};

int ObTabletDirectLoadMgr::calc_range(const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<ObDirectLoadSliceWriter *> sorted_slices;
  sorted_slices.set_attr(ObMemAttr(MTL_ID(), "DL_SortS_tmp"));
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(nullptr == sqc_build_ctx_.storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(sqc_build_ctx_.storage_schema_));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(tablet_id_));
  } else if (OB_FAIL(sorted_slices.reserve(sqc_build_ctx_.slice_mgr_map_.size()))) {
    LOG_WARN("reserve slice array failed", K(ret), K(sqc_build_ctx_.slice_mgr_map_.size()));
  } else {
    for (ObTabletDirectLoadBuildCtx::SLICE_MGR_MAP::const_iterator iter = sqc_build_ctx_.slice_mgr_map_.begin();
      OB_SUCC(ret) && iter != sqc_build_ctx_.slice_mgr_map_.end(); ++iter) {
      ObDirectLoadSliceWriter *cur_slice = iter->second;
      if (OB_ISNULL(cur_slice)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(cur_slice));
      } else if (OB_FAIL(sorted_slices.push_back(cur_slice))) {
        LOG_WARN("push back slice failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      SliceEndkeyCompareFunctor cmp(tablet_handle.get_obj()->get_rowkey_read_info().get_datum_utils());
      lib::ob_sort(sorted_slices.begin(), sorted_slices.end(), cmp);
      ret = cmp.ret_code_;
      if (OB_FAIL(ret)) {
        LOG_WARN("sort slice failed", K(ret), K(sorted_slices));
      }
    }
    int64_t offset = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_slices.count(); ++i) {
      sorted_slices.at(i)->set_row_offset(offset);
      offset += sorted_slices.at(i)->get_row_count();
    }
  }
  if (OB_SUCC(ret) && is_data_direct_load(direct_load_type_)) {
    bool is_column_store = false;
    if (OB_FAIL(ObCODDLUtil::need_column_group_store(*sqc_build_ctx_.storage_schema_, is_column_store))) {
      LOG_WARN("fail to check need column group", K(ret));
    } else if (is_column_store) {
      if (thread_cnt <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invali thread cnt", K(ret), K(thread_cnt));
      } else if (OB_FAIL(calc_cg_range(sorted_slices, thread_cnt))) {
        LOG_WARN("fail to calc cg range", K(ret), K(sorted_slices), K(thread_cnt));
      }
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::calc_cg_range(ObArray<ObDirectLoadSliceWriter *> &sorted_slices, const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  sqc_build_ctx_.sorted_slice_writers_.reset();
  if (thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invali thread cnt", K(ret), K(thread_cnt));
  } else if (OB_FAIL(sqc_build_ctx_.sorted_slice_writers_.assign(sorted_slices))) {
    LOG_WARN("copy slice array failed", K(ret), K(sorted_slices.count()));
  } else {
    common::ObArray<ObTabletDirectLoadBuildCtx::AggregatedCGInfo> sorted_slices_idx;
    int64_t slice_idx = 0;
    while (OB_SUCC(ret) && slice_idx < sorted_slices.count()) {
      int64_t tmp_row_cnt = 0;
      ObTabletDirectLoadBuildCtx::AggregatedCGInfo cur_info;
      cur_info.start_idx_ = slice_idx;
      while (OB_SUCC(ret) && slice_idx < sorted_slices.count()) {
        tmp_row_cnt += sorted_slices.at(slice_idx)->get_row_count();
        ++slice_idx;
        cur_info.last_idx_ = slice_idx;
        if (tmp_row_cnt >= EACH_MACRO_MIN_ROW_CNT) {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sorted_slices_idx.push_back(cur_info))) {
          LOG_WARN("fail to push slice info", K(ret));
        }
      }
    }

    sqc_build_ctx_.sorted_slices_idx_.reset();
    if (OB_FAIL(ret)) {
    } else if (sorted_slices_idx.count() > thread_cnt) {
      // thread_cnt cannot handle aggregated group, re_calc by thread_cnt
      LOG_INFO("[DIRECT_LOAD_FILL_CG] re_calc by thread_cnt", K(sorted_slices_idx.count()), K(thread_cnt), K(sqc_build_ctx_.sorted_slice_writers_.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < thread_cnt; ++i) {
        ObTabletDirectLoadBuildCtx::AggregatedCGInfo cur_info;
        calc_cg_idx(thread_cnt, i, cur_info.start_idx_, cur_info.last_idx_);
        if (OB_FAIL(sqc_build_ctx_.sorted_slices_idx_.push_back(cur_info))) {
          LOG_WARN("fail to push info", K(ret), K(i));
        }
      }
    } else if (OB_FAIL(sqc_build_ctx_.sorted_slices_idx_.assign(sorted_slices_idx))) {
      LOG_WARN("fail to assign array", K(ret));
    }
  }
  FLOG_INFO("[DIRECT_LOAD_FILL_CG]calc_cg_range", K(ret), K(thread_cnt), K(sqc_build_ctx_.sorted_slice_writers_.count()), K(sqc_build_ctx_.sorted_slices_idx_.count()));
  return ret;
}

struct CancelSliceWriterMapFn
{
public:
  CancelSliceWriterMapFn() {}
  int operator () (hash::HashMapPair<ObTabletDirectLoadBuildCtx::SliceKey, ObDirectLoadSliceWriter *> &entry) {
    int ret = OB_SUCCESS;
    if (nullptr != entry.second) {
      LOG_INFO("slice writer cancel", K(&entry.second), "slice_key", entry.first);
      entry.second->cancel();
    }
    return ret;
  }
};

int ObTabletDirectLoadMgr::cancel()
{
  CancelSliceWriterMapFn cancel_map_fn;
  sqc_build_ctx_.slice_mgr_map_.foreach_refactored(cancel_map_fn);
  return OB_SUCCESS;
}

int ObTabletDirectLoadMgr::close_sstable_slice(
    const bool is_data_tablet_process_for_lob,
    const ObDirectLoadSliceInfo &slice_info,
    const share::SCN &start_scn,
    const int64_t execution_id,
    ObInsertMonitor *insert_monitor,
    blocksstable::ObMacroDataSeq &next_seq)
{
  int ret = OB_SUCCESS;
  next_seq.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !start_scn.is_valid_and_not_min() || !sqc_build_ctx_.is_valid() || execution_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info), K(start_scn), K(execution_id), K(sqc_build_ctx_));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (is_data_tablet_process_for_lob) {
    if (OB_UNLIKELY(!lob_mgr_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info));
    } else if (OB_FAIL(lob_mgr_handle_.get_obj()->close_sstable_slice(
        false, slice_info, start_scn, execution_id, insert_monitor, next_seq))) {
      LOG_WARN("close lob sstable slice failed", K(ret), K(slice_info));
    }
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
      LOG_WARN("get refactored failed", K(ret), K(slice_info));
    } else if (OB_ISNULL(slice_writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info));
    } else if (OB_FAIL(slice_writer->close())) {
      LOG_WARN("close failed", K(ret), K(slice_info));
    } else if (OB_FALSE_IT(next_seq = slice_writer->get_next_block_start_seq())) {
      // block start seq after the close operation is the next availabled one.
    } else if (!slice_info.is_lob_slice_ && is_ddl_direct_load(direct_load_type_)) {
      int64_t task_finish_count = -1;
      {
        ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
        if (start_scn == get_start_scn()) {
          task_finish_count = ATOMIC_AAF(&sqc_build_ctx_.task_finish_count_, 1);
        }
      }
      LOG_INFO("inc task finish count", K(tablet_id_), K(execution_id), K(task_finish_count), K(sqc_build_ctx_.task_total_cnt_));
      bool is_column_group_store = false;
      if (OB_ISNULL(sqc_build_ctx_.storage_schema_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", K(ret), KP(sqc_build_ctx_.storage_schema_));
      } else if (OB_FAIL(ObCODDLUtil::need_column_group_store(*sqc_build_ctx_.storage_schema_, is_column_group_store))) {
        LOG_WARN("fail to check is column group store", K(ret));
      } else if (!is_column_group_store) {
        if (task_finish_count >= sqc_build_ctx_.task_total_cnt_) {
          // for ddl, write commit log when all slices ready.
          if (OB_FAIL(close(execution_id, start_scn))) {
            LOG_WARN("close sstable slice failed", K(ret), K(sqc_build_ctx_.build_param_));
          }
        }
      } else {
        if (task_finish_count < sqc_build_ctx_.task_total_cnt_) {
          if (OB_FAIL(wait_notify(slice_writer, start_scn))) {
            LOG_WARN("wait notify failed", K(ret));
          } else if (OB_FAIL(slice_writer->fill_column_group(sqc_build_ctx_.storage_schema_, start_scn, insert_monitor))) {
            LOG_WARN("slice writer fill column group failed", K(ret));
          }
        } else {
          if (OB_FAIL(calc_range(0))) {
            LOG_WARN("calc range failed", K(ret));
          } else if (OB_FAIL(notify_all())) {
            LOG_WARN("notify all failed", K(ret));
          } else if (OB_FAIL(slice_writer->fill_column_group(sqc_build_ctx_.storage_schema_, start_scn, insert_monitor))) {
            LOG_WARN("slice fill column group failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          int64_t fill_cg_finish_count = -1;
          {
            ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
            if (start_scn == get_start_scn()) {
              fill_cg_finish_count = ATOMIC_AAF(&sqc_build_ctx_.fill_column_group_finish_count_, 1);
            }
          }
          LOG_INFO("inc fill cg finish count", K(tablet_id_), K(execution_id), K(fill_cg_finish_count), K(sqc_build_ctx_.task_total_cnt_));
          if (fill_cg_finish_count >= sqc_build_ctx_.task_total_cnt_) {
            // for ddl, write commit log when all slices ready.
            if (OB_FAIL(close(execution_id, start_scn))) {
              LOG_WARN("close sstable slice failed", K(ret));
            }
          }
        }
      }
    }
    if (OB_NOT_NULL(slice_writer)) {
      if (OB_SUCC(ret) && is_data_direct_load(direct_load_type_) && slice_writer->need_column_store()) {
        //ignore, free after rescan
      } else {
        // for ddl, delete slice_writer regardless of success or failure
        int tmp_ret = OB_SUCCESS;
        ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
        if (OB_TMP_FAIL(sqc_build_ctx_.slice_mgr_map_.erase_refactored(slice_key))) {
          LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
        } else {
          LOG_INFO("erase a slice writer", K(ret), K(slice_key), KP(slice_writer), K(sqc_build_ctx_.slice_mgr_map_.size()));
          slice_writer->~ObDirectLoadSliceWriter();
          sqc_build_ctx_.slice_writer_allocator_.free(slice_writer);
          slice_writer = nullptr;
        }
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

void ObTabletDirectLoadMgr::calc_cg_idx(const int64_t thread_cnt, const int64_t thread_id, int64_t &start_idx, int64_t &end_idx)
{
  int ret = OB_SUCCESS;
  const int64_t each_thread_task_cnt = sqc_build_ctx_.sorted_slice_writers_.count() / thread_cnt;
  const int64_t need_plus_thread_cnt = sqc_build_ctx_.sorted_slice_writers_.count() % thread_cnt; // handle +1 task
  const int64_t pre_handle_cnt = need_plus_thread_cnt * (each_thread_task_cnt + 1);
  if (need_plus_thread_cnt != 0) {
    if (thread_id < need_plus_thread_cnt) {
      start_idx = (each_thread_task_cnt + 1) * thread_id;
      end_idx = start_idx + (each_thread_task_cnt + 1);
    } else {
      start_idx = pre_handle_cnt + (thread_id - need_plus_thread_cnt) * each_thread_task_cnt;
      end_idx = start_idx + each_thread_task_cnt;
      // when slice_cnt < thread_cnt, idle thread start_idx = end_idx
    }
  } else {
    start_idx = each_thread_task_cnt * thread_id;
    end_idx = start_idx + each_thread_task_cnt;
  }
}

int ObTabletDirectLoadMgr::fill_column_group(const int64_t thread_cnt, const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(thread_cnt <= 0 || thread_id < 0 || thread_id > thread_cnt - 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(thread_cnt), K(thread_id));
  } else if (sqc_build_ctx_.sorted_slice_writers_.count() == 0 || thread_id > sqc_build_ctx_.sorted_slices_idx_.count() - 1) {
    //ignore
    FLOG_INFO("[DIRECT_LOAD_FILL_CG] idle thread", K(sqc_build_ctx_.sorted_slice_writers_.count()), K(thread_id), K(sqc_build_ctx_.sorted_slices_idx_.count()));
  } else if (sqc_build_ctx_.sorted_slice_writers_.count() != sqc_build_ctx_.slice_mgr_map_.size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong slice writer num", K(ret), K(sqc_build_ctx_.sorted_slice_writers_.count()), K(sqc_build_ctx_.slice_mgr_map_.size()), K(common::lbt()));
  } else {
    const int64_t start_idx = sqc_build_ctx_.sorted_slices_idx_.at(thread_id).start_idx_;
    const int64_t last_idx = sqc_build_ctx_.sorted_slices_idx_.at(thread_id).last_idx_;

    ObArenaAllocator arena_allocator("DIRECT_RESCAN", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    int64_t fill_cg_finish_count = -1;
    int64_t row_cnt = 0;
    if (OB_ISNULL(sqc_build_ctx_.storage_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), KP(sqc_build_ctx_.storage_schema_));
    } else {
      const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = sqc_build_ctx_.storage_schema_->get_column_groups();
      FLOG_INFO("[DIRECT_LOAD_FILL_CG] start fill cg",
          "tablet_id", tablet_id_,
          "cg_cnt", cg_schemas.count(),
          "slice_cnt", sqc_build_ctx_.sorted_slice_writers_.count(),
          K(thread_cnt), K(thread_id), K(start_idx), K(last_idx));

      ObCOSliceWriter *cur_writer = nullptr;
      if (OB_ISNULL(cur_writer = OB_NEWx(ObCOSliceWriter, &arena_allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for co writer failed", K(ret));
      } else if (OB_FAIL(fill_aggregated_column_group(start_idx, last_idx, sqc_build_ctx_.storage_schema_, cur_writer, fill_cg_finish_count, row_cnt))) {
        LOG_WARN("fail to fill aggregated cg", K(ret), KPC(cur_writer));
      }
      // free writer anyhow
      if (OB_NOT_NULL(cur_writer)) {
        cur_writer->~ObCOSliceWriter();
        arena_allocator.free(cur_writer);
        cur_writer = nullptr;
      }
      arena_allocator.reset();

      // after finish all slice, free slice_writer
      if (OB_SUCC(ret)) {
        if (fill_cg_finish_count == sqc_build_ctx_.sorted_slice_writers_.count()) {
          sqc_build_ctx_.sorted_slice_writers_.reset();
          FLOG_INFO("tablet_direct_mgr finish fill column group", K(sqc_build_ctx_.slice_mgr_map_.size()), K(this), K(fill_cg_finish_count));
          if (!sqc_build_ctx_.slice_mgr_map_.empty()) {
            DestroySliceWriterMapFn destroy_map_fn(&sqc_build_ctx_.slice_writer_allocator_);
            int tmp_ret = sqc_build_ctx_.slice_mgr_map_.foreach_refactored(destroy_map_fn);
            if (tmp_ret == OB_SUCCESS) {
              sqc_build_ctx_.slice_mgr_map_.destroy();
            } else {
              ret = tmp_ret;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("[DIRECT_LOAD_FILL_CG] finish fill cg",
          "tablet_id", tablet_id_,
          "row_cnt", row_cnt,
          "slice_cnt", sqc_build_ctx_.sorted_slice_writers_.count(),
          K(thread_cnt), K(thread_id), K(start_idx), K(last_idx),  K(sqc_build_ctx_.slice_mgr_map_.size()));
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::fill_aggregated_column_group(
    const int64_t start_idx,
    const int64_t last_idx,
    const ObStorageSchema *storage_schema,
    ObCOSliceWriter *cur_writer,
    int64_t &fill_cg_finish_count,
    int64_t &fill_row_cnt)
{
  int ret = OB_SUCCESS;
  fill_cg_finish_count = -1;
  fill_row_cnt = 0;
  int64_t fill_aggregated_cg_cnt = 0;
  if (OB_ISNULL(cur_writer) || OB_ISNULL(storage_schema) || OB_UNLIKELY(start_idx < 0 || last_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cur_writer), KP(storage_schema), K(start_idx), K(last_idx));
  } else {
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_schemas.count(); ++cg_idx) {
      cur_writer->reset();
      common::ObArray<sql::ObCompactStore *> datum_stores;
      datum_stores.set_attr(ObMemAttr(MTL_ID(), "TDL_Agg_CG"));
      if (start_idx == last_idx || start_idx >= sqc_build_ctx_.sorted_slice_writers_.count() || last_idx > sqc_build_ctx_.sorted_slice_writers_.count()) {
        // skip
      } else {
        ObDirectLoadSliceWriter *first_slice_writer = sqc_build_ctx_.sorted_slice_writers_.at(start_idx);
        if (OB_ISNULL(first_slice_writer)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null slice writer", K(ret), KP(first_slice_writer));
        } else if (OB_UNLIKELY(first_slice_writer->get_row_offset() < 0)) {
          ret = OB_ERR_SYS;
          LOG_WARN("invalid row offset", K(ret), K(first_slice_writer->get_row_offset()));
        } else if (OB_FAIL(cur_writer->init(storage_schema, cg_idx, this, first_slice_writer->get_start_seq(), first_slice_writer->get_row_offset(), get_start_scn()))) {
          LOG_WARN("init co ddl writer failed", K(ret), KPC(cur_writer), K(cg_idx), KPC(this));
        } else {
          for (int64_t i = start_idx; OB_SUCC(ret) && i < last_idx; ++i) {
            ObDirectLoadSliceWriter *slice_writer = sqc_build_ctx_.sorted_slice_writers_.at(i);
            if (OB_ISNULL(slice_writer) || !slice_writer->need_column_store()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("wrong slice writer",  K(ret), KPC(slice_writer));
            } else if (OB_FAIL(slice_writer->fill_aggregated_column_group(cg_idx, cur_writer, datum_stores))) {
              LOG_WARN("slice writer rescan failed", K(ret), K(cg_idx), KPC(cur_writer));
            } else if (cg_idx == cg_schemas.count() - 1) {
              // after fill last cg, inc finish cnt
              fill_row_cnt += slice_writer->get_row_count();
              ++fill_aggregated_cg_cnt;

            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (cur_writer->is_inited() && OB_FAIL(cur_writer->close())) {
          LOG_WARN("close co ddl writer failed", K(ret));
        } else {
          for (int64_t i = 0; i < datum_stores.count(); ++i) {
            if (OB_NOT_NULL(datum_stores.at(i))) {
              datum_stores.at(i)->~ObCompactStore();
            }
          }
          datum_stores.reset();
        }
      }
      // next cg
    }
    if (OB_SUCC(ret)) {
      /*
      To avoid concurrent release of slice_writer (datum_store) with slice_mgr_map_.destroy()
       we must reset datum_store first and then increase fill_column_group_finish_count_.
      */
      fill_cg_finish_count = ATOMIC_AAF(&sqc_build_ctx_.fill_column_group_finish_count_, fill_aggregated_cg_cnt);
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::prepare_index_builder_if_need(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc index_block_desc(true/*is ddl*/);
  if (sqc_build_ctx_.index_builder_ != nullptr) {
    LOG_INFO("index builder is already prepared");
  } else if (OB_FAIL(index_block_desc.init(table_schema, ls_id_, tablet_id_,
          is_full_direct_load(direct_load_type_) ? compaction::ObMergeType::MAJOR_MERGE : compaction::ObMergeType::MINOR_MERGE,
          is_full_direct_load(direct_load_type_) ? table_key_.get_snapshot_version() : 1L,
          data_format_version_,
          is_full_direct_load(direct_load_type_) ? SCN::invalid_scn() : table_key_.get_end_scn()))) {
    LOG_WARN("fail to init data desc", K(ret));
  } else {
    void *builder_buf = nullptr;

    if (OB_ISNULL(builder_buf = sqc_build_ctx_.allocator_.alloc(sizeof(ObSSTableIndexBuilder)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (OB_ISNULL(sqc_build_ctx_.index_builder_ = new (builder_buf) ObSSTableIndexBuilder())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new ObSSTableIndexBuilder", K(ret));
    } else if (OB_FAIL(sqc_build_ctx_.index_builder_->init(
            index_block_desc.get_desc(), // index_block_desc is copied in index_builder
            nullptr, // macro block flush callback
            ObSSTableIndexBuilder::DISABLE))) {
      LOG_WARN("failed to init index builder", K(ret), K(index_block_desc));
    } else if (OB_FAIL(sqc_build_ctx_.data_block_desc_.init(table_schema, ls_id_, tablet_id_,
            is_full_direct_load(direct_load_type_) ? compaction::ObMergeType::MAJOR_MERGE : compaction::ObMergeType::MINOR_MERGE,
            is_full_direct_load(direct_load_type_) ? table_key_.get_snapshot_version() : 1L,
            data_format_version_,
            is_full_direct_load(direct_load_type_) ? SCN::invalid_scn() : table_key_.get_end_scn()))) {
      LOG_WARN("fail to init data block desc", K(ret));
    } else {
      sqc_build_ctx_.data_block_desc_.get_desc().sstable_index_builder_ = sqc_build_ctx_.index_builder_; // for build the tail index block in macro block
    }


    if (OB_FAIL(ret)) {
      if (nullptr != sqc_build_ctx_.index_builder_) {
        sqc_build_ctx_.index_builder_->~ObSSTableIndexBuilder();
        sqc_build_ctx_.index_builder_ = nullptr;
      }
      if (nullptr != builder_buf) {
        sqc_build_ctx_.allocator_.free(builder_buf);
        builder_buf = nullptr;
      }
      sqc_build_ctx_.data_block_desc_.reset();
    }
  }
  return ret;
}

int ObTabletDirectLoadMgr::wrlock(const int64_t timeout_us, uint32_t &tid)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = timeout_us + ObTimeUtility::current_time();
  if (OB_SUCC(lock_.wrlock(ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK, abs_timeout_us))) {
    tid = static_cast<uint32_t>(GETTID());
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTabletDirectLoadMgr::rdlock(const int64_t timeout_us, uint32_t &tid)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = timeout_us + ObTimeUtility::current_time();
  if (OB_SUCC(lock_.rdlock(ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK, abs_timeout_us))) {
    tid = static_cast<uint32_t>(GETTID());
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObTabletDirectLoadMgr::unlock(const uint32_t tid)
{
  if (OB_SUCCESS != lock_.unlock(&tid)) {
    ob_abort();
  }
}

int ObTabletDirectLoadMgr::prepare_storage_schema(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (nullptr != sqc_build_ctx_.storage_schema_) {
    LOG_INFO("storage schema has been prepared before", K(*sqc_build_ctx_.storage_schema_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(sqc_build_ctx_.schema_allocator_, sqc_build_ctx_.storage_schema_))) {
    LOG_WARN("load storage schema failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    sqc_build_ctx_.commit_scn_ = get_commit_scn(tablet_handle.get_obj()->get_tablet_meta());
  }
  return ret;
}

ObTabletFullDirectLoadMgr::ObTabletFullDirectLoadMgr()
  : ObTabletDirectLoadMgr(), start_scn_(share::SCN::min_scn()),
    commit_scn_(share::SCN::min_scn()), execution_id_(-1)
{
}

ObTabletFullDirectLoadMgr::~ObTabletFullDirectLoadMgr()
{
}

int ObTabletFullDirectLoadMgr::update(
    ObTabletDirectLoadMgr *lob_tablet_mgr,
    const ObTabletDirectLoadInsertParam &build_param)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  if (OB_UNLIKELY(!build_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(build_param));
  } else if (OB_FAIL(ObTabletDirectLoadMgr::update(lob_tablet_mgr, build_param))) {
    LOG_WARN("init failed", K(ret), K(build_param));
  } else {
    table_key_.reset();
    table_key_.tablet_id_ = build_param.common_param_.tablet_id_;
    bool is_column_group_store = false;
    if (OB_ISNULL(sqc_build_ctx_.storage_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null storage schema", K(ret));
    } else if (OB_FAIL(ObCODDLUtil::need_column_group_store(*sqc_build_ctx_.storage_schema_, is_column_group_store))) {
      LOG_WARN("fail to get schema is column group store", K(ret));
    } else if (is_column_group_store) {
      table_key_.table_type_ = ObITable::COLUMN_ORIENTED_SSTABLE;
      int64_t base_cg_idx = -1;
      if (OB_FAIL(ObCODDLUtil::get_base_cg_idx(sqc_build_ctx_.storage_schema_, base_cg_idx))) {
        LOG_WARN("get base cg idx failed", K(ret));
      } else {
        table_key_.column_group_idx_ = static_cast<uint16_t>(base_cg_idx);
      }
    } else {
      table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
    }
    table_key_.version_range_.snapshot_version_ = build_param.common_param_.read_snapshot_;
  }
  LOG_INFO("init tablet direct load mgr finished", K(ret), K(build_param), KPC(this));
  return ret;
}

int ObTabletFullDirectLoadMgr::open(const int64_t current_execution_id, share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  uint32_t lock_tid = 0;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletFullDirectLoadMgr *lob_tablet_mgr = nullptr;
  start_scn.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid() || !sqc_build_ctx_.is_valid() || current_execution_id < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this), K(current_execution_id));
  } else if (OB_FAIL(wrlock(TRY_LOCK_TIMEOUT, lock_tid))) {
    LOG_WARN("failed to wrlock", K(ret), KPC(this));
  } else if (lob_mgr_handle_.is_valid()
    && OB_ISNULL(lob_tablet_mgr = lob_mgr_handle_.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls service should not be null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id_, tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", K(ret), K(tablet_handle));
  } else if (current_execution_id < execution_id_
    || current_execution_id < tablet_handle.get_obj()->get_tablet_meta().ddl_execution_id_) {
    ret = OB_TASK_EXPIRED;
    LOG_INFO("receive a old execution id, don't do start", K(ret), K(current_execution_id), K(sqc_build_ctx_),
      "tablet_meta", tablet_handle.get_obj()->get_tablet_meta());
  } else if (get_commit_scn(tablet_handle.get_obj()->get_tablet_meta()).is_valid_and_not_min()) {
    // has already committed.
    start_scn = start_scn_;
    if (!start_scn.is_valid_and_not_min()) {
      start_scn = tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_;
    }
    if (!start_scn.is_valid_and_not_min()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("start scn must be valid after commit", K(ret), K(start_scn));
    }
  } else {
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    ObDDLKvMgrHandle lob_kv_mgr_handle;
    ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
    if (OB_FAIL(direct_load_mgr_handle.set_obj(this))) {
      LOG_WARN("set handle failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
      LOG_WARN("create ddl kv mgr failed", K(ret));
    } else if (nullptr != lob_tablet_mgr) {
      ObTabletHandle lob_tablet_handle;
      if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, lob_tablet_mgr->get_tablet_id(), lob_tablet_handle))) {
        LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), KPC(lob_tablet_mgr));
      } else if (OB_FAIL(lob_tablet_handle.get_obj()->get_ddl_kv_mgr(lob_kv_mgr_handle, true/*try_create*/))) {
        LOG_WARN("create ddl kv mgr failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObDDLRedoLogWriter redo_writer;
      if (OB_FAIL(redo_writer.init(ls_id_, tablet_id_))) {
        LOG_WARN("init redo writer failed", K(ret), K(ls_id_), K(tablet_id_));
      } else if (OB_FAIL(redo_writer.write_start_log(table_key_,
        current_execution_id, sqc_build_ctx_.build_param_.common_param_.data_format_version_, direct_load_type_,
        ddl_kv_mgr_handle, lob_kv_mgr_handle, direct_load_mgr_handle, lock_tid, start_scn))) {
        LOG_WARN("fail write start log", K(ret), K(table_key_), K(data_format_version_), K(sqc_build_ctx_));
      } else if (OB_UNLIKELY(!start_scn.is_valid_and_not_min())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), K(start_scn));
      } else if (nullptr != lob_tablet_mgr
        && OB_FAIL(lob_tablet_mgr->init_ddl_table_store(start_scn, table_key_.get_snapshot_version(), start_scn))) {
        LOG_WARN("clean up ddl sstable failed", K(ret), K(start_scn), K(table_key_));
      } else if (OB_FAIL(init_ddl_table_store(start_scn, table_key_.get_snapshot_version(), start_scn))) {
        LOG_WARN("clean up ddl sstable failed", K(ret), K(start_scn), K(table_key_));
      }
    }
  }
  if (lock_tid != 0) {
    unlock(lock_tid);
  }
  return ret;
}

int ObTabletFullDirectLoadMgr::close(const int64_t execution_id, const SCN &start_scn)
{
  int ret = OB_SUCCESS;
  SCN commit_scn;
  bool is_remote_write = false;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObTabletFullDirectLoadMgr *lob_tablet_mgr = nullptr;
  bool sstable_already_created = false;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(execution_id < 0 || !start_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(execution_id), K(start_scn));
  } else if (lob_mgr_handle_.is_valid()
    && OB_ISNULL(lob_tablet_mgr = lob_mgr_handle_.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls service should not be null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id_, tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", K(ret), K(tablet_handle));
  } else {
    uint32_t lock_tid = 0;
    ObDDLRedoLogWriter redo_writer;
    if (OB_FAIL(wrlock(TRY_LOCK_TIMEOUT, lock_tid))) {
      LOG_WARN("failed to wrlock", K(ret), KPC(this));
    } else if (FALSE_IT(sstable_already_created = sqc_build_ctx_.is_task_end_)) {
    } else if (sstable_already_created) {
      // Why use is_task_end_ rather than commit_scn_.
      // sqc may switch to follower, and the commit_scn will not be set.
      LOG_INFO("had already closed", K(ret));
    } else if (OB_FAIL(redo_writer.init(ls_id_, tablet_id_))) {
      LOG_WARN("init redo writer failed", K(ret), K(ls_id_), K(tablet_id_));
    } else {
      DEBUG_SYNC(AFTER_REMOTE_WRITE_DDL_PREPARE_LOG);
      ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
      if (OB_FAIL(direct_load_mgr_handle.set_obj(this))) {
        LOG_WARN("set direct load mgr handle failed", K(ret));
      } else if (OB_FAIL(redo_writer.write_commit_log_with_retry(true, table_key_,
          start_scn, direct_load_mgr_handle, tablet_handle, commit_scn, is_remote_write, lock_tid))) {
        LOG_WARN("fail write ddl commit log", K(ret), K(table_key_), K(sqc_build_ctx_));
      }
    }
    if (0 != lock_tid) {
      unlock(lock_tid);
    }
  }

  bool is_delay_build_major = false;
#ifdef ERRSIM
    is_delay_build_major = 0 != GCONF.errsim_ddl_major_delay_time;
    sqc_build_ctx_.is_task_end_ = is_delay_build_major ? true : sqc_build_ctx_.is_task_end_;  // skip report checksum
#endif
  if (OB_FAIL(ret) || sstable_already_created) {
  } else if (is_remote_write) {
    LOG_INFO("ddl commit log is written in remote, need wait replay", K(sqc_build_ctx_), K(start_scn), K(commit_scn));
  } else if (OB_UNLIKELY(!start_scn.is_valid_and_not_min()) || !commit_scn.is_valid_and_not_min()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_FAIL(commit(*tablet_handle.get_obj(), start_scn, commit_scn,
      sqc_build_ctx_.build_param_.runtime_only_param_.table_id_, sqc_build_ctx_.build_param_.runtime_only_param_.task_id_, false/*is replay*/))) {
    LOG_WARN("failed to do ddl kv commit", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
  } else if (sstable_already_created || is_delay_build_major) {
    LOG_INFO("sstable had already created, skip waiting for major generated and reporting chksum", K(start_scn), K(commit_scn),
        K(sstable_already_created), K(is_delay_build_major));
  } else if (OB_FAIL(schedule_merge_task(start_scn, commit_scn, true/*wait_major_generate*/, false/*is_replay*/))) {
    LOG_WARN("schedule merge task and wait real major generate", K(ret),
        K(is_remote_write), K(sstable_already_created), K(start_scn), K(commit_scn));
  } else if (lob_mgr_handle_.is_valid() &&
      OB_FAIL(lob_mgr_handle_.get_full_obj()->schedule_merge_task(start_scn, commit_scn, true/*wait_major_generate*/, false/*is_replay*/))) {
    LOG_WARN("schedule merge task and wait real major generate for lob failed", K(ret),
        K(is_remote_write), K(sstable_already_created), K(start_scn), K(commit_scn));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id_, new_tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
  } else {
    ObSSTableMetaHandle sst_meta_hdl;
    ObSSTable *first_major_sstable = nullptr;
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    if (OB_FAIL(new_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fetch table store failed", K(ret));
    } else if (OB_ISNULL(first_major_sstable = static_cast<ObSSTable *>
      (table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no major after wait merge success", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_UNLIKELY(first_major_sstable->get_key() != table_key_)) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("ddl major sstable dropped, snapshot holding may have bug",
        K(ret), KPC(first_major_sstable), K(table_key_), K(tablet_id_), K(sqc_build_ctx_.build_param_), K(sqc_build_ctx_.build_param_.runtime_only_param_.task_id_));
    } else if (OB_FAIL(first_major_sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else {
      const int64_t *column_checksums = sst_meta_hdl.get_sstable_meta().get_col_checksum();
      int64_t column_count = sst_meta_hdl.get_sstable_meta().get_col_checksum_cnt();
      ObArray<int64_t> co_column_checksums;
      co_column_checksums.set_attr(ObMemAttr(MTL_ID(), "TblDL_Ccc"));
      if (OB_FAIL(get_co_column_checksums_if_need(tablet_handle, first_major_sstable, co_column_checksums))) {
        LOG_WARN("get column checksum from co sstable failed", K(ret));
      } else {
        for (int64_t retry_cnt = 10; retry_cnt > 0; retry_cnt--) { // overwrite ret
          if (OB_FAIL(ObTabletDDLUtil::report_ddl_checksum(
                  ls_id_,
                  tablet_id_,
                  sqc_build_ctx_.build_param_.runtime_only_param_.table_id_,
                  execution_id,
                  sqc_build_ctx_.build_param_.runtime_only_param_.task_id_,
                  co_column_checksums.empty() ? column_checksums : co_column_checksums.get_data(),
                  co_column_checksums.empty() ? column_count : co_column_checksums.count(),
                  data_format_version_))) {
            LOG_WARN("report ddl column checksum failed", K(ret), K(ls_id_), K(tablet_id_), K(execution_id), K(sqc_build_ctx_));
          } else {
            break;
          }
          ob_usleep(100L * 1000L);
        }
      }
    }

    if (OB_SUCC(ret)) {
      sqc_build_ctx_.is_task_end_ = true;
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgr::start_with_checkpoint(
    ObTablet &tablet,
    const share::SCN &start_scn,
    const uint64_t data_format_version,
    const int64_t execution_id,
    const share::SCN &checkpoint_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!checkpoint_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(checkpoint_scn));
  } else if (OB_UNLIKELY(!table_key_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("the table key not updated", K(ret), KPC(this));
  } else {
    ObITable::TableKey table_key = table_key_;
    ret = start(tablet, table_key, start_scn, data_format_version, execution_id, checkpoint_scn);
  }
  return ret;
}

// For Leader and follower both.
// For replay start log only, migration_create_tablet and online will no call the intrface.
int ObTabletFullDirectLoadMgr::start(
    ObTablet &tablet,
    const ObITable::TableKey &table_key,
    const share::SCN &start_scn,
    const uint64_t data_format_version,
    const int64_t execution_id,
    const share::SCN &checkpoint_scn)
{
  int ret = OB_SUCCESS;
  share::SCN saved_start_scn;
  int64_t saved_snapshot_version = 0;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLKvMgrHandle lob_kv_mgr_handle;
  ddl_kv_mgr_handle.reset();
  lob_kv_mgr_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(table_key != table_key_)
    || !start_scn.is_valid_and_not_min()
    || execution_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(table_key), K(table_key_), K(start_scn), K(execution_id));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
    LOG_WARN("create tablet ddl kv mgr handle failed", K(ret));
  } else if (lob_mgr_handle_.is_valid()) {
    ObLSHandle ls_handle;
    ObTabletHandle lob_tablet_handle;
    if (OB_ISNULL(MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret));
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(ls_id_));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, lob_mgr_handle_.get_obj()->get_tablet_id(), lob_tablet_handle))) {
      LOG_WARN("get tablet failed", K(ret));
    } else if (OB_FAIL(lob_tablet_handle.get_obj()->get_ddl_kv_mgr(lob_kv_mgr_handle, true/*try_create*/))) {
      LOG_WARN("create tablet ddl kv mgr handle failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("get ls handle failed", K(ret), K(ls_id_));
    } else if (OB_ISNULL(ls_handle.get_ls()) || OB_ISNULL(ls_handle.get_ls()->get_ddl_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls or ddl log handler is null", K(ret), KPC(ls_handle.get_ls()), K(ls_id_));
    } else if (OB_FAIL(ls_handle.get_ls()->get_ddl_log_handler()->add_tablet(tablet_id_))) {
      LOG_WARN("add tablet id failed", K(ret), K(ls_id_), K(tablet_id_));
    } else if (lob_kv_mgr_handle.is_valid() && OB_FAIL(ls_handle.get_ls()->get_ddl_log_handler()->add_tablet(lob_mgr_handle_.get_obj()->get_tablet_id()))) {
      LOG_WARN("add lob tablet id failed", K(ret), "lob_tablet_id", lob_mgr_handle_.get_obj()->get_tablet_id());
    }
  }
  if (OB_SUCC(ret)) {
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    if (OB_FAIL(start_nolock(table_key, start_scn, data_format_version, execution_id, checkpoint_scn,
        ddl_kv_mgr_handle, lob_kv_mgr_handle))) {
      LOG_WARN("failed to ddl start", K(ret));
    } else {
      // save variables under lock
      saved_start_scn = start_scn_;
      saved_snapshot_version = table_key_.get_snapshot_version();
      const SCN ddl_commit_scn = get_commit_scn(tablet.get_tablet_meta());
      commit_scn_.atomic_store(ddl_commit_scn);
      if (lob_mgr_handle_.is_valid()) {
        lob_mgr_handle_.get_full_obj()->set_commit_scn_nolock(ddl_commit_scn);
      }
    }
  }
  if (OB_SUCC(ret) && !checkpoint_scn.is_valid_and_not_min()) {
    // remove ddl sstable if exists and flush ddl start log ts and snapshot version into tablet meta.
    // persist lob meta tablet before data tablet is necessary, to avoid start-loss for lob meta tablet when recovered from checkpoint.
    if (lob_mgr_handle_.is_valid() &&
      OB_FAIL(lob_mgr_handle_.get_full_obj()->init_ddl_table_store(saved_start_scn, saved_snapshot_version, saved_start_scn))) {
      LOG_WARN("clean up ddl sstable failed", K(ret));
    } else if (OB_FAIL(init_ddl_table_store(saved_start_scn, saved_snapshot_version, saved_start_scn))) {
      LOG_WARN("clean up ddl sstable failed", K(ret), K(tablet_id_));
    }
  }
  FLOG_INFO("start full direct load mgr finished", K(ret), K(start_scn), K(execution_id), KPC(this));
  return ret;
}

int ObTabletFullDirectLoadMgr::start_nolock(
    const ObITable::TableKey &table_key,
    const share::SCN &start_scn,
    const uint64_t data_format_version,
    const int64_t execution_id,
    const SCN &checkpoint_scn,
    ObDDLKvMgrHandle &ddl_kv_mgr_handle,
    ObDDLKvMgrHandle &lob_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  bool is_brand_new = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!table_key.is_valid() || !start_scn.is_valid_and_not_min() || data_format_version < 0 || execution_id < 0
      || (checkpoint_scn.is_valid_and_not_min() && checkpoint_scn < start_scn)) || !ddl_kv_mgr_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(start_scn), K(data_format_version), K(execution_id), K(checkpoint_scn),
      "kv_mgr_handle is valid", ddl_kv_mgr_handle.is_valid());
  } else if (table_key.get_tablet_id() != tablet_id_ || table_key_ != table_key) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet id not same", K(ret), K(table_key), K(table_key_), K(tablet_id_));
  } else {
    if (start_scn_.is_valid_and_not_min()) {
      if (execution_id >= execution_id_ && start_scn >= start_scn_) {
        is_brand_new = true;
        LOG_INFO("execution id changed, need cleanup", K(ls_id_), K(tablet_id_), K(execution_id_), K(execution_id), K(start_scn_), K(start_scn));
      } else {
        if (!checkpoint_scn.is_valid_and_not_min()) {
          // only return error code when not start from checkpoint.
          ret = OB_TASK_EXPIRED;
        }
        LOG_INFO("ddl start ignored", K(ls_id_), K(tablet_id_), K(execution_id_), K(execution_id), K(start_scn_), K(start_scn), K(checkpoint_scn));
      }
    } else {
      is_brand_new = true;
      FLOG_INFO("ddl start brand new", K(table_key), K(start_scn), K(execution_id), KPC(this));
    }
    if (OB_SUCC(ret) && is_brand_new) {
      if (OB_FAIL(cleanup_unlock())) {
        LOG_WARN("cleanup unlock failed", K(ret));
      } else {
        table_key_ = table_key;
        data_format_version_ = data_format_version;
        execution_id_ = execution_id;
        start_scn_.atomic_store(start_scn);
        ddl_kv_mgr_handle.get_obj()->set_max_freeze_scn(SCN::max(start_scn, checkpoint_scn));
        sqc_build_ctx_.reset_slice_ctx_on_demand();
      }
    }
  }
  if (OB_SUCC(ret) && lob_mgr_handle_.is_valid()) {
    // For lob meta tablet recover from checkpoint, execute start itself to avoid the data loss when,
    // 1. lob meta tablet recover from checkpoint;
    // 2. replay some data redo log on lob meta tablet.
    // 3. data tablet recover from checkpoint, and cleanup will be triggered if lob meta tablet
    //    execute start again.
    ObDDLKvMgrHandle unused_kv_mgr_handle;
    ObITable::TableKey lob_table_key;
    lob_table_key.tablet_id_ = lob_mgr_handle_.get_full_obj()->get_tablet_id();
    lob_table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE; // lob tablet not support column group store
    lob_table_key.version_range_ = table_key.version_range_;
    if (OB_FAIL(lob_mgr_handle_.get_full_obj()->start_nolock(lob_table_key, start_scn, data_format_version, execution_id, checkpoint_scn,
        lob_kv_mgr_handle, unused_kv_mgr_handle))) {
      LOG_WARN("start nolock for lob meta tablet failed", K(ret));
    }
  }
  FLOG_INFO("start_nolock full direct load mgr finished", K(ret), K(start_scn), K(execution_id), KPC(this));
  return ret;
}

int ObTabletFullDirectLoadMgr::commit(
    ObTablet &tablet,
    const share::SCN &start_scn,
    const share::SCN &commit_scn,
    const uint64_t table_id,
    const int64_t ddl_task_id,
    const bool is_replay)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret), KPC(this));
  } else if (start_scn < get_start_scn()) {
    ret = OB_TASK_EXPIRED;
    LOG_INFO("skip ddl commit log", K(start_scn), K(*this));
  } else if (OB_FAIL(set_commit_scn(commit_scn))) {
    LOG_WARN("failed to set commit scn", K(ret));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("create ddl kv mgr failed", K(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(
    start_scn, table_key_.get_snapshot_version(), data_format_version_, commit_scn))) {
    LOG_WARN("failed to start prepare", K(ret), K(tablet_id_), K(commit_scn));
  } else {
    ret = OB_EAGAIN;
    while (OB_EAGAIN == ret) {
      if (OB_FAIL(update_major_sstable())) {
        LOG_WARN("update ddl major sstable failed", K(ret), K(tablet_id_), K(start_scn), K(commit_scn));
      }
      if (OB_EAGAIN == ret) {
        usleep(1000L);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(schedule_merge_task(start_scn, commit_scn, false/*wait_major_generate*/, is_replay))) {
        LOG_WARN("schedule major merge task failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && lob_mgr_handle_.is_valid()) {
    const share::ObLSID &ls_id = lob_mgr_handle_.get_full_obj()->get_ls_id();
    const ObTabletID &lob_tablet_id = lob_mgr_handle_.get_full_obj()->get_tablet_id();
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    ObTabletHandle lob_tablet_handle;
    if (OB_ISNULL(MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls should not be null", K(ret));
    } else if (OB_FAIL(ls->get_tablet(lob_tablet_id, lob_tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
      LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(lob_tablet_id));
    } else if (OB_FAIL(lob_mgr_handle_.get_full_obj()->commit(*lob_tablet_handle.get_obj(), start_scn, commit_scn, table_id, ddl_task_id, is_replay))) {
      LOG_WARN("commit for lob failed", K(ret), K(start_scn), K(commit_scn));
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgr::replay_commit(
    ObTablet &tablet,
    const share::SCN &start_scn,
    const share::SCN &commit_scn,
    const uint64_t table_id,
    const int64_t ddl_task_id)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret), KPC(this));
  } else if (start_scn < get_start_scn()) {
    ret = OB_TASK_EXPIRED;
    LOG_INFO("skip ddl commit log", K(start_scn), K(*this));
  } else if (OB_FAIL(set_commit_scn(commit_scn))) {
    LOG_WARN("failed to set commit scn", K(ret));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("create ddl kv mgr failed", K(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(
    start_scn, table_key_.get_snapshot_version(), data_format_version_, commit_scn))) {
    LOG_WARN("failed to start prepare", K(ret), K(tablet_id_), K(commit_scn));
  } else {
    ret = OB_EAGAIN;
    while (OB_EAGAIN == ret) {
      if (OB_FAIL(update_major_sstable())) {
        LOG_WARN("update ddl major sstable failed", K(ret), K(tablet_id_), K(start_scn), K(commit_scn));
      }
      if (OB_EAGAIN == ret) {
        usleep(1000L);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(schedule_merge_task(start_scn, commit_scn, false/*wait_major_generate*/, true/*is_replay*/))) {
        LOG_WARN("schedule major merge task failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgr::schedule_merge_task(
    const share::SCN &start_scn,
    const share::SCN &commit_scn,
    const bool wait_major_generated,
    const bool is_replay)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!start_scn.is_valid_and_not_min() || !commit_scn.is_valid_and_not_min() || (is_replay && wait_major_generated))) {
    ret = OB_ERR_SYS;
    LOG_WARN("unknown start scn or commit snc", K(ret), K(start_scn), K(commit_scn), K(is_replay), K(wait_major_generated));
  } else {
    const int64_t wait_start_ts = ObTimeUtility::fast_current_time();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else {
        ObDDLTableMergeDagParam param;
        param.direct_load_type_    = direct_load_type_;
        param.ls_id_               = ls_id_;
        param.tablet_id_           = tablet_id_;
        param.rec_scn_             = commit_scn;
        param.is_commit_           = true;
        param.start_scn_           = start_scn;
        param.data_format_version_ = data_format_version_;
        param.snapshot_version_    = table_key_.get_snapshot_version();
        if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
          if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
            LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
          } else {
            ret = OB_SUCCESS;
            if (is_replay) {
              break;
            }
          }
        } else if (!wait_major_generated) {
          // schedule successfully and no need to wait physical major generates.
          break;
        }
      }
      if (OB_SUCC(ret)) {
        const ObSSTable *first_major_sstable = nullptr;
        ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
        if (OB_FAIL(ObTabletDDLUtil::check_and_get_major_sstable(ls_id_, tablet_id_, first_major_sstable, table_store_wrapper))) {
          LOG_WARN("check if major sstable exist failed", K(ret));
        } else if (nullptr != first_major_sstable) {
          FLOG_INFO("major has already existed", KPC(this));
          break;
        }
      }
      if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
        LOG_INFO("wait build ddl sstable", K(ret), K(ls_id_), K(tablet_id_), K(start_scn), K(commit_scn),
            "wait_elpased_s", (ObTimeUtility::fast_current_time() - wait_start_ts) / 1000000L);
      }
    }
  }
  return ret;
}

void ObTabletFullDirectLoadMgr::set_commit_scn_nolock(const share::SCN &scn)
{
  commit_scn_.atomic_store(scn);
  if (lob_mgr_handle_.is_valid()) {
    lob_mgr_handle_.get_full_obj()->set_commit_scn_nolock(scn);
  }
}

int ObTabletFullDirectLoadMgr::set_commit_scn(const share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!commit_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(commit_scn));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
                                                    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    const share::SCN old_commit_scn = get_commit_scn(tablet_handle.get_obj()->get_tablet_meta());
    if (old_commit_scn.is_valid_and_not_min() && old_commit_scn != commit_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("already committed by others", K(ret), K(commit_scn), KPC(this));
    } else {
      commit_scn_.atomic_store(commit_scn);
    }
  }
  return ret;
}

// return latest commit_scn iff tablet_meta is newer than the creation of ObTabletFullDirectLoadMgr
share::SCN ObTabletFullDirectLoadMgr::get_commit_scn(const ObTabletMeta &tablet_meta)
{
  share::SCN mgr_commit_scn = commit_scn_.atomic_load();
  share::SCN commit_scn = share::SCN::min_scn();
  if (tablet_meta.ddl_commit_scn_.is_valid_and_not_min() || mgr_commit_scn.is_valid_and_not_min()) {
    if (tablet_meta.ddl_commit_scn_.is_valid_and_not_min()) {
      commit_scn = tablet_meta.ddl_commit_scn_;
    } else {
      commit_scn = mgr_commit_scn;
    }
  } else {
    commit_scn = share::SCN::min_scn();
  }
  return commit_scn;
}

share::SCN ObTabletFullDirectLoadMgr::get_start_scn()
{
  return start_scn_.atomic_load();
}

int ObTabletFullDirectLoadMgr::can_schedule_major_compaction_nolock(
    const ObTablet &tablet,
    bool &can_schedule)
{
  int ret = OB_SUCCESS;
  can_schedule = false;
  share::SCN commit_scn;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fetch table store failed", K(ret));
  } else if (nullptr != table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)) {
    // major sstable has already existed.
  } else {
    can_schedule = get_commit_scn(tablet_meta).is_valid_and_not_min() ? true : false;
  }
  return ret;
}

int ObTabletFullDirectLoadMgr::prepare_ddl_merge_param(
    const ObTablet &tablet,
    ObDDLTableMergeDagParam &merge_param)
{
  int ret = OB_SUCCESS;
  bool can_schedule = false;
  ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  if (OB_FAIL(can_schedule_major_compaction_nolock(tablet, can_schedule))) {
    LOG_WARN("check can schedule major compaction failed", K(ret));
  } else if (can_schedule) {
    merge_param.direct_load_type_ = direct_load_type_;
    merge_param.ls_id_ = ls_id_;
    merge_param.tablet_id_ = tablet_id_;
    merge_param.rec_scn_ = get_commit_scn(tablet.get_tablet_meta());
    merge_param.is_commit_ = true;
    merge_param.start_scn_ = start_scn_;
    merge_param.data_format_version_ = data_format_version_;
    merge_param.snapshot_version_    = table_key_.get_snapshot_version();
  } else {
    merge_param.direct_load_type_ = direct_load_type_;
    merge_param.ls_id_ = ls_id_;
    merge_param.tablet_id_ = tablet_id_;
    merge_param.start_scn_ = start_scn_;
    merge_param.data_format_version_ = data_format_version_;
    merge_param.snapshot_version_    = table_key_.get_snapshot_version();
  }
  return ret;
}

int ObTabletFullDirectLoadMgr::prepare_major_merge_param(
    ObTabletDDLParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret));
  } else {
    ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    param.direct_load_type_ = direct_load_type_;
    param.ls_id_ = ls_id_;
    param.table_key_ = table_key_;
    param.start_scn_ = start_scn_;
    param.commit_scn_ = commit_scn_;
    param.snapshot_version_ = table_key_.get_snapshot_version();
    param.data_format_version_ = data_format_version_;
  }
  return ret;
}

void ObTabletFullDirectLoadMgr::cleanup_slice_writer(const int64_t context_id)
{
  sqc_build_ctx_.cleanup_slice_writer(context_id);
}

int ObTabletFullDirectLoadMgr::cleanup_unlock()
{
  int ret = OB_SUCCESS;
  LOG_INFO("cleanup expired sstables", K(*this));
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls service should not be null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id_, tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("create ddl kv mgr failed", K(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->cleanup())) {
    LOG_WARN("cleanup failed", K(ret));
  } else {
    table_key_.reset();
    data_format_version_ = 0;
    start_scn_.atomic_store(share::SCN::min_scn());
    commit_scn_.atomic_store(share::SCN::min_scn());
    execution_id_ = -1;
  }
  return ret;
}

int ObTabletFullDirectLoadMgr::init_ddl_table_store(
    const share::SCN &start_scn,
    const int64_t snapshot_version,
    const share::SCN &ddl_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObArenaAllocator tmp_arena("DDLUpdateTblTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObStorageSchema *storage_schema = nullptr;
  bool is_column_group_store = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!start_scn.is_valid_and_not_min() || snapshot_version <= 0 || !ddl_checkpoint_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_scn), K(snapshot_version), K(ddl_checkpoint_scn));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(tablet_handle));
  } else if (OB_FAIL(ObCODDLUtil::need_column_group_store(*storage_schema, is_column_group_store))) {
    LOG_WARN("fail to check schema is column group store", K(ret));
  }
  else {
    ObTableHandleV2 table_handle; // empty
    const int64_t rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    ObTableHandleV2 sstable_handle;
    ObTabletHandle new_tablet_handle;
    ObTablesHandleArray empty_cg_sstable_handles;
    ObArray<ObDDLBlockMeta> empty_meta_array;
    empty_meta_array.set_attr(ObMemAttr(MTL_ID(), "TblFDL_EMA"));

    ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    ObTabletDDLParam ddl_param;
    ddl_param.direct_load_type_ = direct_load_type_;
    ddl_param.ls_id_ = ls_id_;
    ddl_param.table_key_ = table_key_;
    ddl_param.start_scn_ = start_scn;
    ddl_param.commit_scn_ = commit_scn_;
    ddl_param.snapshot_version_ = table_key_.get_snapshot_version();
    ddl_param.data_format_version_ = data_format_version_;
    ddl_param.table_key_.table_type_ = is_column_group_store ? ObITable::DDL_MERGE_CO_SSTABLE : ObITable::DDL_DUMP_SSTABLE;
    ddl_param.table_key_.scn_range_.start_scn_ = SCN::scn_dec(start_scn);
    ddl_param.table_key_.scn_range_.end_scn_ = start_scn;

    ObUpdateTableStoreParam param(tablet_handle.get_obj()->get_snapshot_version(),
                                  ObVersionRange::MIN_VERSION, // multi_version_start
                                  storage_schema,
                                  rebuild_seq);
    param.ddl_info_.keep_old_ddl_sstable_ = false;
    param.ddl_info_.ddl_start_scn_ = start_scn;
    param.ddl_info_.ddl_snapshot_version_ = snapshot_version;
    param.ddl_info_.ddl_checkpoint_scn_ = ddl_checkpoint_scn;
    param.ddl_info_.ddl_execution_id_ = execution_id_;
    param.ddl_info_.data_format_version_ = data_format_version_;
    if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(*tablet_handle.get_obj(), ddl_param, empty_meta_array, nullptr/*first_ddl_sstable*/,
        storage_schema, tmp_arena, sstable_handle))) {
      LOG_WARN("create empty ddl sstable failed", K(ret));
    } else if (ddl_param.table_key_.is_co_sstable()) {
      // add empty cg sstables
      ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(sstable_handle.get_table());
      const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
      ObTabletDDLParam cg_ddl_param = ddl_param;
      cg_ddl_param.table_key_.table_type_ = ObITable::TableType::DDL_MERGE_CG_SSTABLE;
      for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
        ObTableHandleV2 cur_handle;
        cg_ddl_param.table_key_.column_group_idx_ = static_cast<uint16_t>(i);
        if (table_key_.get_column_group_id() == i) {
          // skip base cg idx
        } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(*tablet_handle.get_obj(), cg_ddl_param, empty_meta_array,
                nullptr/*first_ddl_sstable*/, storage_schema, tmp_arena, cur_handle))) {
          LOG_WARN("create empty cg sstable failed", K(ret), K(i), K(cg_ddl_param));
        } else if (OB_FAIL(empty_cg_sstable_handles.add_table(cur_handle))) {
          LOG_WARN("add table handle failed", K(ret), K(i), K(cur_handle));
        }
      }
      if (OB_SUCC(ret)) {
        ObArray<ObITable *> cg_sstables;
        cg_sstables.set_attr(ObMemAttr(MTL_ID(), "TblFDL_CGS"));
        if (OB_FAIL(empty_cg_sstable_handles.get_tables(cg_sstables))) {
          LOG_WARN("get cg sstables failed", K(ret));
        } else if (OB_FAIL(co_sstable->fill_cg_sstables(cg_sstables))) {
          LOG_WARN("fill empty cg sstables failed", K(ret));
        } else {
          LOG_DEBUG("fill co sstable with empty cg sstables success", K(ret), K(ddl_param), KPC(co_sstable));
        }
      }
    }
    bool is_column_group_store = false;
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(param.sstable_ = static_cast<ObSSTable *>(sstable_handle.get_table()))) {
    } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(ls_id_), K(tablet_id_), K(param));
    } else if (OB_FAIL(ObCODDLUtil::need_column_group_store(*storage_schema, is_column_group_store))) {
      LOG_WARN("failed to check storage schema is column group store", K(ret));
    } else {
      LOG_INFO("update tablet success", K(ls_id_), K(tablet_id_),
          "is_column_store", is_column_group_store, K(ddl_param),
          "column_group_schemas", storage_schema->get_column_groups(),
          "update_table_store_param", param, K(start_scn), K(snapshot_version), K(ddl_checkpoint_scn));
    }
  }
  ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
  return ret;
}

int ObTabletFullDirectLoadMgr::update_major_sstable()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObArenaAllocator tmp_arena("DDLUpdateTblTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObStorageSchema *storage_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("load storage schema failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    ObTabletHandle new_tablet_handle;
    ObUpdateTableStoreParam param(tablet_handle.get_obj()->get_snapshot_version(),
                                  ObVersionRange::MIN_VERSION, // multi_version_start
                                  storage_schema,
                                  ls_handle.get_ls()->get_rebuild_seq());
    param.ddl_info_.keep_old_ddl_sstable_ = true;
    param.ddl_info_.ddl_commit_scn_ = get_commit_scn(tablet_handle.get_obj()->get_tablet_meta()); // ddl commit scn may larger than ddl checkpoint scn
    if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(ls_id_), K(tablet_id_), K(param));
    }
  }
  ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
  return ret;
}

/**
 * ObTabletIncDirectLoadMgr
 */

ObTabletIncDirectLoadMgr::ObTabletIncDirectLoadMgr(int64_t context_id)
  : ObTabletDirectLoadMgr(),
    context_id_(context_id),
    start_scn_(share::SCN::min_scn()),
    is_closed_(false)
{
}

ObTabletIncDirectLoadMgr::~ObTabletIncDirectLoadMgr()
{
}

int ObTabletIncDirectLoadMgr::update(
    ObTabletDirectLoadMgr *lob_tablet_mgr,
    const ObTabletDirectLoadInsertParam &build_param)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  if (OB_UNLIKELY(!build_param.is_valid() ||
                  nullptr == build_param.runtime_only_param_.tx_desc_ ||
                  !build_param.runtime_only_param_.trans_id_.is_valid() ||
                  build_param.runtime_only_param_.seq_no_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(build_param));
  } else if (OB_FAIL(ObTabletDirectLoadMgr::update(lob_tablet_mgr, build_param))) {
    LOG_WARN("init failed", K(ret), K(build_param));
  } else {
    table_key_.reset();
    table_key_.tablet_id_ = build_param.common_param_.tablet_id_;
    table_key_.table_type_ = ObITable::MINI_SSTABLE;
    // set scn_range for check valid only
    table_key_.scn_range_.start_scn_.convert_for_tx(1);
    table_key_.scn_range_.end_scn_.convert_for_tx(build_param.common_param_.read_snapshot_); // for logic version
    data_format_version_ = build_param.common_param_.data_format_version_;
    sqc_build_ctx_.reset_slice_ctx_on_demand();
    start_scn_.set_min();
  }
  LOG_INFO("init tablet inc direct load mgr finished", K(ret), K(build_param), KPC(this));
  return ret;
}

int ObTabletIncDirectLoadMgr::open(
    const int64_t current_execution_id,
    share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  ObTabletIncDirectLoadMgr *lob_tablet_mgr = nullptr;
  start_scn.reset();
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid() || !sqc_build_ctx_.is_valid() || current_execution_id < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this), K(current_execution_id));
  } else if (lob_mgr_handle_.is_valid()
    && OB_ISNULL(lob_tablet_mgr = lob_mgr_handle_.get_inc_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  }
  if (OB_SUCC(ret)) {
    ObDDLIncRedoLogWriter redo_writer;
    if (OB_FAIL(redo_writer.init(ls_id_, tablet_id_))) {
      LOG_WARN("init redo writer failed", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_FAIL(redo_writer.write_inc_start_log_with_retry(
                 nullptr != lob_tablet_mgr ? lob_tablet_mgr->get_tablet_id() : ObTabletID(),
                 sqc_build_ctx_.build_param_.runtime_only_param_.tx_desc_,
                 start_scn))) {
      LOG_WARN("fail write start log", K(ret), K(data_format_version_), K(sqc_build_ctx_));
    } else if (OB_UNLIKELY(!start_scn.is_valid_and_not_min())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(start_scn));
    } else if (nullptr != lob_tablet_mgr && OB_FAIL(lob_tablet_mgr->start(current_execution_id, start_scn))) {
      LOG_WARN("fail to start lob", KR(ret));
    } else if (OB_FAIL(start(current_execution_id, start_scn))) {
      LOG_WARN("fail to start", KR(ret));
    }
  }
  return ret;
}

int ObTabletIncDirectLoadMgr::start(const int64_t execution_id, const share::SCN &start_scn)
{
  UNUSED(execution_id);
  int ret = OB_SUCCESS;
  start_scn_ = start_scn;
  return ret;
}

int ObTabletIncDirectLoadMgr::close(const int64_t current_execution_id, const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  ObTabletIncDirectLoadMgr *lob_tablet_mgr = nullptr;
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(current_execution_id < 0 || !start_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(current_execution_id), K(start_scn));
  } else if (OB_UNLIKELY(start_scn != start_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected start scn", KR(ret), K(context_id_), K(start_scn_), K(start_scn));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected close twice", KR(ret), K(context_id_));
  } else if (lob_mgr_handle_.is_valid()
    && OB_ISNULL(lob_tablet_mgr = lob_mgr_handle_.get_inc_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else {
    ObDDLIncRedoLogWriter redo_writer;
    if (OB_FAIL(redo_writer.init(ls_id_, tablet_id_))) {
      LOG_WARN("init redo writer failed", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_FAIL(redo_writer.write_inc_commit_log_with_retry(
                 true /*allow_remote_write*/,
                 nullptr != lob_tablet_mgr ? lob_tablet_mgr->get_tablet_id() : ObTabletID(),
                 sqc_build_ctx_.build_param_.runtime_only_param_.tx_desc_))) {
      LOG_WARN("fail write start log", K(ret), K(data_format_version_), K(sqc_build_ctx_));
    } else if (nullptr != lob_tablet_mgr && OB_FAIL(lob_tablet_mgr->commit(current_execution_id, start_scn))) {
      LOG_WARN("fail to commit lob", KR(ret));
    } else if (OB_FAIL(commit(current_execution_id, start_scn))) {
      LOG_WARN("fail to commit", KR(ret));
    }
  }
  return ret;
}

int ObTabletIncDirectLoadMgr::commit(const int64_t execution_id, const share::SCN &commit_scn)
{
  UNUSEDx(execution_id, commit_scn);
  int ret = OB_SUCCESS;
  is_closed_ = true;
  return ret;
}
