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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/ddl/ob_ddl_merge_helper.h"
#include "storage/ddl/ob_inc_ddl_merge_helper.h"
#include "storage/ddl/ob_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"

using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(ERRSIM_DDL_RETRY_WRITE_COMPLETE);
ERRSIM_POINT_DEF(ERRSIM_DDL_RETRY_REPORT_CHECKSUM);
ERRSIM_POINT_DEF(ERRSIM_DDL_PRINT_DDL_CHECKPOINT_SCN);

int ObIDDLMergeHelper::get_merge_helper(ObIAllocator &allocator,
                                        const ObDirectLoadType direct_load_type,
                                        ObIDDLMergeHelper *&helper)
{
  int ret = OB_SUCCESS;
  #define BUILD_MERGE_HELPER(helper_type) \
          if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(sizeof(helper_type))))) { \
            ret = OB_ALLOCATE_MEMORY_FAILED; \
            LOG_WARN("failed to alloc", K(ret), K(direct_load_type)); \
          } else { \
            helper = new (buf) helper_type(); \
          } \

  char *buf = nullptr;
  switch(direct_load_type) {
    case ObDirectLoadType::DIRECT_LOAD_INCREMENTAL:
      BUILD_MERGE_HELPER(ObIncMinDDLMergeHelper);
      break;
    #ifdef OB_BUILD_SHARED_STORAGE
    case ObDirectLoadType::DIRECT_LOAD_DDL_V2:
    case ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2:
      BUILD_MERGE_HELPER(ObSSDDLMergeHelper);
      break;
    #endif
    case ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL:
    case ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DATA:
      BUILD_MERGE_HELPER(ObSNDDLMergeHelperV2);
      break;
    #ifdef OB_BUILD_SHARED_STORAGE
    case ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DDL:
    case ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DATA:
      BUILD_MERGE_HELPER(ObSSDDLMergeHelper);
      break;
    #endif
    case ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR:
      if (!GCTX.is_shared_storage_mode()) {
        BUILD_MERGE_HELPER(ObIncMajorDDLMergeHelper);
      }
#ifdef OB_BUILD_SHARED_STORAGE
      else {
        BUILD_MERGE_HELPER(ObSSIncMajorDDLMergeHelper);
      }
#endif
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported direct load type", K(ret), K(direct_load_type));
      break;
  }
  #undef BUILD_MERGE_HELPER
  return ret;
}


int ObIDDLMergeHelper::freeze_ddl_kv(ObDDLTabletMergeDagParamV2 &param)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;

  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ObDDLMergeTaskUtils::freeze_ddl_kv(target_ls_id,
                                                        target_tablet_id,
                                                        param.direct_load_type_,
                                                        param.start_scn_,
                                                        param.ddl_task_param_.snapshot_version_,
                                                        param.ddl_task_param_.tenant_data_version_))) {
    LOG_WARN("failed to freeze ddl kv", K(ret));
  }
  return ret;
}

int ObSNDDLMergeHelperV2::set_ddl_complete(ObIDag *dag, ObTablet &tablet, ObDDLTabletMergeDagParamV2 &ddl_merge_param)
{
  int ret = OB_SUCCESS;
  ObTabletDDLCompleteArg complete_arg;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "MrgHlpArg"));
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext *tablet_context = ddl_merge_param.get_tablet_ctx();
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  /* ddl kv has already been freeze in prepare task */
  if (OB_UNLIKELY(ERRSIM_DDL_RETRY_WRITE_COMPLETE)) {
    ret = OB_EAGAIN;
    DEBUG_SYNC(AFTER_DDL_RETRY_WRITE_COMPLETE);
  } else if (OB_ISNULL(dag) || !ddl_merge_param.is_valid() || OB_ISNULL(tablet_context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), KP(dag), K(ddl_merge_param), KP(tablet_context));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle, false /* not for repaly*/))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(ddl_merge_param));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(ddl_merge_param));
    }
  } else if (OB_FAIL(ddl_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_ISNULL(tablet_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get tablet param", K(ret), K(ddl_merge_param));
  } else if (FALSE_IT(storage_schema = tablet_param ->storage_schema_)) {
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema should not be null", K(ret), K(ddl_merge_param));
  } else {
    complete_arg.has_complete_          = true;
    complete_arg.ls_id_                 = target_ls_id;
    complete_arg.tablet_id_             = target_tablet_id;
    complete_arg.direct_load_type_      = ddl_merge_param.direct_load_type_;
    complete_arg.start_scn_             = ddl_merge_param.start_scn_;
    complete_arg.data_format_version_   = ddl_merge_param.ddl_task_param_.tenant_data_version_;
    complete_arg.snapshot_version_      = ddl_merge_param.ddl_task_param_.snapshot_version_;
    complete_arg.table_key_             = ddl_merge_param.table_key_;
    const ObDDLWriteStat *write_stat = target_tablet_id == tablet_context->lob_meta_tablet_id_ ? &tablet_context->lob_write_stat_ : &tablet_context->write_stat_;
    if (OB_FAIL(complete_arg.set_write_stat(*write_stat))) {
      LOG_WARN("failed to set write stat", K(ret), KPC(write_stat));
    } else if (OB_FAIL(complete_arg.set_storage_schema(*storage_schema))) {
      LOG_WARN("failed to set storage_schema", K(ret), K(ddl_merge_param), KPC(storage_schema));
    } else if (OB_FAIL(ObTabletDDLCompleteMdsHelper::record_ddl_complete_arg_to_mds(complete_arg, allocator))) {
      LOG_WARN("failed to record ddl complete arg to mds", KR(ret), K(complete_arg));
    }
  }
  return ret;
}

int ObSNDDLMergeHelperV2::set_ddl_complete_for_direct_load(
  const ObLSID &ls_id,
  const ObTabletID &tablet_id,
  const ObDirectLoadType direct_load_type,
  const int64_t snapshot_version,
  const int64_t data_version)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DLM_CLOSE"));
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLWriteStat write_stats; // only used for empty direct load, so keep empty here
  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("MTL ObLSService is null", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(ls));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ls is nullptr", KR(ret));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("fail to get tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*try_create*/))) {
    LOG_WARN("get ddl kv mgr failed", K(ret), K(ls_id), K(tablet_id));
  } else {
    ObTabletDDLCompleteArg complete_arg;
    complete_arg.has_complete_          = true;
    complete_arg.ls_id_                 = ls_id;
    complete_arg.tablet_id_             = tablet_id;
    complete_arg.direct_load_type_      = direct_load_type;
    complete_arg.start_scn_             = SCN::base_scn();
    complete_arg.data_format_version_   = data_version;
    complete_arg.snapshot_version_      = snapshot_version;
    complete_arg.table_key_.reset();
    complete_arg.table_key_.tablet_id_ = tablet_id;
    complete_arg.table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
    complete_arg.table_key_.version_range_.snapshot_version_ = snapshot_version;

    if (OB_FAIL(complete_arg.set_write_stat(write_stats))) {
      LOG_WARN("failed to set write stat", K(ret), K(write_stats));
    } else if (OB_FAIL(complete_arg.set_storage_schema(*storage_schema))) {
      LOG_WARN("failed to set storage_schema", K(ret), KPC(storage_schema));
    } else if (OB_FAIL(ObTabletDDLCompleteMdsHelper::record_ddl_complete_arg_to_mds(complete_arg, allocator))) {
      LOG_WARN("failed to record ddl complete arg to mds", KR(ret), K(complete_arg));
    }
  }
  ObTabletObjLoadHelper::free(allocator, storage_schema);
  return ret;
}


int ObSNDDLMergeHelperV2::process_prepare_task(ObIDag *dag,
                                               ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                                               ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices)
{
  int ret = OB_SUCCESS;

  int64_t merge_slice_idx = 0;
  bool for_major = false;

  cg_slices.reset();
  hash::ObHashSet<int64_t> slice_idxes;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<const ObSSTable*> ddl_sstables;

  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  const ObSSTable *first_major_sstable = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;
  ObDDLKVQueryParam ddl_kv_query_param;
  if (!ddl_merge_param.is_valid() || nullptr == dag) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_merge_param), KPC(dag));
  } else if (OB_FAIL(ddl_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (FALSE_IT(for_major = ddl_merge_param.for_major_)) {
  } else if (OB_FAIL(slice_idxes.create(DDL_SLICE_BUCKET_NUM, ObMemAttr(MTL_ID(), "slice_idx_set")))) {
    LOG_WARN("create slice index set failed", K(ret));
  } else if (OB_FAIL(ddl_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx should not be null", K(ret), K(ddl_merge_param));
  }

  /* check major sstable exist */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FALSE_IT(first_major_sstable = static_cast<ObSSTable *>(
                                                table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
  }else if (nullptr != first_major_sstable) {          /* if major exist, do nothing */
  } else if (for_major && !ddl_merge_param.for_replay_ && OB_FAIL(set_ddl_complete(dag, *(tablet_handle.get_obj()), ddl_merge_param))) {
    LOG_WARN("failed to set ddl complete", K(ret));
  }

  /* if for major need to wait ddl complete take effect */
  if (OB_FAIL(ret)) {
  } else if (nullptr != first_major_sstable) {          /* if major exist, do nothing */
  } else if (for_major) {
    ObArenaAllocator arena(ObMemAttr(MTL_ID(), "DDL_Mrg_Pre"));
    ObTabletDDLCompleteMdsUserData user_data;
    if (OB_FAIL(tablet_handle.get_obj()->get_ddl_complete(share::SCN::max_scn(), arena, user_data))) {
      if (OB_EMPTY_RESULT == ret) {
        /* for ddl execute node, should wait take effect */
        ret = ddl_merge_param.for_replay_ ? OB_EAGAIN : OB_DAG_TASK_IS_SUSPENDED;
      }
      LOG_WARN("failed to get ddl complete mds user data", K(ret));
    } else if (!user_data.has_complete_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl complete not take effect", K(ret), K(user_data));
    }
  }

  /* freeze & get frozen ddl kvs */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(ddl_merge_param));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(ddl_merge_param));
    }
  } else if (ddl_merge_param.start_scn_ < tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("ddl task expired, skip it", K(ret), K(ddl_merge_param),
            "new_start_scn", tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_);
  } else if (OB_FALSE_IT(ddl_kv_query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_FULL)) {
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(
      true/*frozen_only*/, frozen_ddl_kvs, ddl_kv_query_param))) {
    LOG_WARN("get freezed ddl kv failed", K(ret), K(ddl_merge_param), K(ddl_kv_query_param));
  } else if (OB_FAIL(merge_ctx->ddl_kv_handles_.assign(frozen_ddl_kvs))) {
    LOG_WARN("failed to frozen ddl kv", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
      int max_retry_cnt = 10000;
      while(max_retry_cnt > 0) {
        max_retry_cnt--;
        if (OB_FAIL(frozen_ddl_kvs.at(i).get_obj()->close())) {
          if (OB_EAGAIN != ret) {
            break;
          } else {
            ob_usleep(500);
          }
        } else {
          break;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("falied to close frozen ddl kv", K(ret), K(max_retry_cnt));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (nullptr != first_major_sstable) {
    /* do nothing */
  } else if (for_major) {
    if (OB_FAIL(slice_idxes.set_refactored(0))) {
      LOG_WARN("failed to set refactored", K(ret)); // should have at least one slice in slice idx
    } else {
      merge_slice_idx = 0; // merge all slice
    }
  } else if (!for_major) {
    if (OB_FAIL(ObDDLMergeTaskUtils::get_merge_slice_idx(frozen_ddl_kvs, merge_slice_idx))) {
      LOG_WARN("failed to get merge slice idx", K(ret));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::get_ddl_memtables(frozen_ddl_kvs, ddl_sstables))) {
      LOG_WARN("get ddl memtables failed", K(ret), K(frozen_ddl_kvs));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::get_slice_indexes(ddl_sstables, slice_idxes))) { // get slice idx from ddl memtable only
      LOG_WARN("get slice indexes failed", K(ret), K(ddl_merge_param));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (nullptr != first_major_sstable) {          /* if major exist, do nothing */
  } else if (OB_FAIL(ddl_merge_param.init_cg_sstable_array(slice_idxes))) {
    LOG_WARN("fialed to init cg sstable array", K(ret));
  } else {
    int64_t cg_count = !ObITable::is_column_store_sstable(ddl_merge_param.table_key_.table_type_) ?
                       1 :tablet_param->storage_schema_->get_column_group_count();
    for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_count; cg_idx++) {
      for (hash::ObHashSet<int64_t>::const_iterator iter = slice_idxes.begin();
          OB_SUCC(ret) && iter != slice_idxes.end();
          ++iter) {
        int64_t start_slice_idx = iter->first;
        int64_t end_slice_idx   = 0 == iter->first ? merge_slice_idx : iter->first;
        if (OB_FAIL(cg_slices.push_back(ObTuple<int64_t, int64_t, int64_t>(cg_idx, start_slice_idx, end_slice_idx)))) {
          LOG_WARN("faield to push back val", K(ret), K(start_slice_idx), K(end_slice_idx));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ERRSIM_DDL_PRINT_DDL_CHECKPOINT_SCN)) {
    if (ddl_merge_param.for_major_ && !ddl_merge_param.for_replay_) {
      if (OB_FAIL(get_rec_scn(ddl_merge_param))) {
        LOG_WARN("failed to get rec scn", K(ret));
      } else {
        SERVER_EVENT_ADD("ddl", "ddl checkpoint scn",
          "tenant_id", MTL_ID(),
          "ret", ret,
          "trace_id", *ObCurTraceId::get_trace_id(),
          "tablet_id", target_tablet_id,
          "trans_id", ddl_merge_param.rec_scn_);
      }
    }
  }

  FLOG_INFO("[DDL_MERGE_TASK][PREPARE] get ddl kv", K(ret), K(frozen_ddl_kvs.count()),  K(merge_slice_idx), K(target_tablet_id));
  return ret;
}

int calc_scn_range(const ObIArray<ObDDLKVHandle> &ddl_kvs,
                   const ObArray<ObSSTable*> &ddl_sstables,
                   share::SCN &start_scn,
                   share::SCN &end_scn,
                   int64_t &snapshot_version)
{
  int ret = OB_SUCCESS;
  /* calc from dump sstalbe */
  if (ddl_kvs.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_kvs));
  } else {
    ObDDLKVHandle first_kv_handle = ddl_kvs.at(0);
    ObDDLKVHandle last_kv_handle = ddl_kvs.at(ddl_kvs.count() - 1);
    start_scn = first_kv_handle.get_obj()->get_start_scn();
    end_scn = last_kv_handle.get_obj()->get_end_scn();
    snapshot_version = first_kv_handle.get_obj()->get_ddl_snapshot_version();
  }

  /* calc from  from dump sstable */
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables.count(); ++i) {
      if (OB_ISNULL(ddl_sstables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl sstable is null", K(ret), K(i));
      } else if (ObITable::is_ddl_dump_sstable(ddl_sstables.at(i)->get_key().table_type_)) {
        start_scn = share::SCN::min(start_scn, ddl_sstables.at(i)->get_key().scn_range_.start_scn_);
      }
    }
  }
  return ret;
}

int ObSNDDLMergeHelperV2::merge_cg_slice(ObIDag *dag,
                                         ObDDLTabletMergeDagParamV2 &merge_param,
                                         const int64_t cg_idx,
                                         const int64_t start_slice_idx,
                                         const int64_t end_slice_idx)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObArray<ObSSTable*> ddl_sstables;
  ObArray<ObDDLBlockMeta> sorted_metas;
  ObArray<ObDDLBlockMeta> tmp_metas;
  ObArray<ObStorageMetaHandle> meta_handles;
  ObDDLWriteStat write_stat;

  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  ObTabletDDLParam ddl_param;

  ObArenaAllocator arena(ObMemAttr(MTL_ID(), "merge_cg_slice"));
  ObTabletDDLCompleteMdsUserData ddl_data;

  if (OB_ISNULL(dag) || cg_idx < 0 || start_slice_idx < 0 || end_slice_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid param", K(ret), K(dag), K(cg_idx), K(start_slice_idx), K(end_slice_idx));
  } else if (OB_FAIL(merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret), K(merge_param));
  } else  if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(merge_param));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(merge_param));
  } else if (OB_FAIL(prepare_ddl_param(merge_param, cg_idx, start_slice_idx, end_slice_idx, ddl_param))) {
    LOG_WARN("failed to prepare ddl_param", K(ret));
  } else if (OB_FAIL(merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret), K(merge_param));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_ctx should not be null", K(ret));
  }

  int64_t ddl_dump_table_cnt = 0;
  SMART_VAR(ObTableStoreIterator, ddl_sstable_iter) {
    const ObITableReadInfo *cg_index_read_info = nullptr;
    bool for_row_store = !ObITable::is_column_store_sstable(ddl_param.table_key_.table_type_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_sstables(ddl_sstable_iter))) {
      LOG_WARN("failed to get ddl sstable", K(ret));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::get_ddl_tables_from_dump_tables(for_row_store,
                                                                            ddl_sstable_iter,
                                                                            cg_idx,
                                                                            start_slice_idx,
                                                                            merge_param.for_major_ ? INT64_MAX : end_slice_idx,
                                                                            ddl_sstables,
                                                                            meta_handles))) {
      LOG_WARN("failed to get ddl tables from dump sstables", K(ret), K(ddl_param), K(for_row_store), K(merge_param), K(cg_idx), K(start_slice_idx), K(end_slice_idx));
    } else if (FALSE_IT(ddl_dump_table_cnt = ddl_sstables.count())) {
    } else if (OB_FAIL(ObDDLMergeTaskUtils::get_ddl_tables_from_ddl_kvs(merge_ctx->ddl_kv_handles_,
                                                   cg_idx,
                                                   start_slice_idx,
                                                   merge_param.for_major_ ? INT64_MAX : end_slice_idx,
                                                   ddl_sstables))) {
     LOG_WARN("failed to get ddl tables from  ddl kvs", K(ret));
    } else if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(cg_index_read_info))) {
      LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", K(ret));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::get_sorted_meta_array(*tablet_handle.get_obj(),
                                                                  ddl_param,
                                                                  tablet_param->storage_schema_,
                                                                  ddl_sstables,
                                                                  cg_idx == merge_param.table_key_.column_group_idx_ ? tablet_handle.get_obj()->get_rowkey_read_info() :
                                                                                                                       *cg_index_read_info,
                                                                  arena, tmp_metas))) {
      LOG_WARN("failed to get storted meta array", K(ret));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::check_idempodency(tmp_metas, sorted_metas, &write_stat))) {
      LOG_WARN("failed to check idempodency", K(ret));
    } else if (merge_param.for_major_) {
      if (OB_FAIL(tablet_handle.get_obj()->get_ddl_complete(share::SCN::max_scn(), arena, ddl_data))) {
        if (OB_EMPTY_RESULT == ret) {
          /* may read mds failed when tablet is deleted*/
          ret = OB_TASK_EXPIRED;
        }
        LOG_WARN("failed to get ddl complete", K(ret), K(ddl_data));
      } else if (!ddl_data.is_valid() || !ddl_data.has_complete_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl complete has not been set", K(ret), K(ddl_data));
      } else if (ddl_data.write_stat_.row_count_ != write_stat.row_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl row count not match", K(ret), K(ddl_data), K(write_stat));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!merge_param.for_major_ && OB_FAIL(calc_scn_range(merge_ctx->ddl_kv_handles_, ddl_sstables, ddl_param.table_key_.scn_range_.start_scn_,
                                      ddl_param.table_key_.scn_range_.end_scn_, ddl_param.snapshot_version_))) {
      LOG_WARN("failed to calc scn range", K(ret));
    }
  } // ddl_sstable_iter

  LOG_INFO("sn_ddl_merge_helper_v2 merge_cg_slice", KR(ret), K(ddl_sstables.count()), K(ddl_param));

  /* !!! notice !!!
   * sstable meta info rely on previous ddl dump sstable if exist
   * rember to using dump sstable instead of mem table as first ddl sstabe
   */
  ObTableHandleV2 sstable_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(*(tablet_handle.get_obj()),
                                                           ddl_param,
                                                           sorted_metas,
                                                           ObArray<MacroBlockId>(),
                                                           ddl_dump_table_cnt > 0 ? ddl_sstables.at(0) : nullptr, /* if dump exist using meta from dump */
                                                           tablet_param->storage_schema_,
                                                           &merge_ctx->mutex_,
                                                           merge_ctx->arena_,
                                                           sstable_handle))) {
    LOG_WARN("failed to create sstable", K(ret), K(cg_idx), K(ddl_param));
  } else if (OB_FAIL(merge_param.set_cg_slice_sstable(start_slice_idx, cg_idx, sstable_handle))) {
    LOG_WARN("failed to set ddl sstable", K(ret), K(ddl_param), KPC(tablet_param->storage_schema_));
  }
  return ret;
}

int ObIDDLMergeHelper::prepare_ddl_param(const ObDDLTabletMergeDagParamV2 &merge_param,
                                            const int64_t cg_idx,
                                            ObTabletDDLParam &ddl_param)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  if (!merge_param.is_valid() || !is_supported_direct_load_type(merge_param.direct_load_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_param));
  } else if (OB_FAIL(merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get merge param", K(ret));
  } else {
    ddl_param.direct_load_type_    = merge_param.direct_load_type_;
    ddl_param.ls_id_               = ls_id;
    ddl_param.table_key_           = merge_param.table_key_;
    ddl_param.start_scn_           = merge_param.start_scn_;
    ddl_param.commit_scn_          = merge_param.rec_scn_;
    ddl_param.snapshot_version_    = merge_param.ddl_task_param_.snapshot_version_;
    ddl_param.data_format_version_ = merge_param.ddl_task_param_.tenant_data_version_;
    ddl_param.rec_scn_             = merge_param.rec_scn_;
    ddl_param.trans_id_            = merge_param.trans_id_;
    ddl_param.seq_no_              = merge_param.seq_no_;
    /* set table key*/
    ddl_param.table_key_.column_group_idx_ = cg_idx;
    LOG_INFO("prepare_ddl_param", K(ddl_param));
    if (OB_FAIL(merge_param.get_table_type(cg_idx, ddl_param.table_key_.table_type_, merge_param.direct_load_type_))) {
      LOG_WARN("failed to get table type", K(ret));
    }
  }
  return ret;
}

int ObIDDLMergeHelper::prepare_ddl_param(const ObDDLTabletMergeDagParamV2 &merge_param,
                                         const int64_t cg_idx,
                                         const int64_t start_slice_idx,
                                         const int64_t end_slice_idx,
                                         ObTabletDDLParam &ddl_param)
{
  int ret = OB_SUCCESS;
  if (!merge_param.is_valid() ||
      !is_supported_direct_load_type(merge_param.direct_load_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_param), K(merge_param.is_valid()));
  } else if (OB_FAIL(prepare_ddl_param(merge_param, cg_idx, ddl_param))) {
    LOG_WARN("failed to prepare ddl param", K(ret));
  } else if (merge_param.need_merge_all_slice() && !merge_param.for_major_) {
    //let is_merge_slice()=true, currently only used for inc-major direct load
    ddl_param.table_key_.slice_range_.start_slice_idx_ = 0;
    ddl_param.table_key_.slice_range_.end_slice_idx_   = INT32_MAX;
  } else {
    ddl_param.table_key_.slice_range_.start_slice_idx_ = start_slice_idx;
    ddl_param.table_key_.slice_range_.end_slice_idx_   = end_slice_idx;
  }
  return ret;
}

int ObIDDLMergeHelper::get_rec_scn_from_ddl_kvs(ObDDLTabletMergeDagParamV2 &merge_param)
{
  int ret = OB_SUCCESS;
  // for empty tablet, rec_scn should equal to start_scn
  share::SCN rec_scn = share::SCN::max(merge_param.rec_scn_, merge_param.start_scn_);
  ObTabletHandle tablet_handle;

  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;
  ObWriteTabletParam *tablet_param = nullptr;
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;

  if (!merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_param));
  } else if (OB_FAIL(merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_ctx should not be null", K(ret));
  } else {
    int64_t ddl_kv_count = merge_ctx->ddl_kv_handles_.count();
    int64_t ddl_dump_count = 0;
    /* get rec scn from ddl kv */
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_ctx->ddl_kv_handles_.count(); ++i) {
      if (!merge_ctx->ddl_kv_handles_.at(i).is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ddl kv handle", K(ret), K(i));
      } else {
        rec_scn = share::SCN::max(rec_scn, merge_ctx->ddl_kv_handles_.at(i).get_obj()->get_end_scn());
      }
    }
    /* get rec scn from ddl sstable, since ddl kv may be null */
    SMART_VAR(ObTableStoreIterator, ddl_sstable_iter) {
      //TODO: refine the code @zhuoran.zzr
      if (OB_FAIL(ret)) {
      } else if (is_incremental_major_direct_load(merge_param.direct_load_type_)
          && OB_FAIL(tablet_handle.get_obj()->get_inc_major_ddl_sstables(ddl_sstable_iter, merge_param.trans_id_, merge_param.seq_no_))) {
        LOG_WARN("failed to get inc major ddl sstable", KR(ret), K(merge_param));
      } else if (!is_incremental_major_direct_load(merge_param.direct_load_type_)
          && OB_FAIL(tablet_handle.get_obj()->get_ddl_sstables(ddl_sstable_iter))) {
        LOG_WARN("failed to get ddl sstable", K(ret));
      }
      while(OB_SUCC(ret)) {
        ObITable *table = nullptr;
        if (OB_FAIL(ddl_sstable_iter.get_next(table))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next table failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (nullptr == table || OB_UNLIKELY(!table->is_sstable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
        } else {
          rec_scn = share::SCN::max(rec_scn, table->get_rec_scn());
          ddl_dump_count++;
        }
      }
    }
    if (OB_SUCC(ret)) {
      merge_param.rec_scn_ = rec_scn;
      FLOG_INFO("[DDL_MERGE_TASK]get rec scn", K(ret), K(target_tablet_id), K(ddl_kv_count), K(ddl_dump_count), K(rec_scn));
    }
  }
  return ret;
}

int ObSNDDLMergeHelperV2::get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param)
{
  return ObIDDLMergeHelper::get_rec_scn_from_ddl_kvs(merge_param);
}

int ObSNDDLMergeHelperV2::assemble_sstable(ObDDLTabletMergeDagParamV2 &merge_param)
{
  int ret = OB_SUCCESS;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;

  ObTabletHandle tablet_handle;
  ObSSTable *major_sstable = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObTablesHandleArray co_sstable_array;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObSSTable *first_major_sstable = nullptr;

  if (!merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_param));
  } else if (OB_FAIL(merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  }

  /* check major sstable exist */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FALSE_IT(first_major_sstable = static_cast<ObSSTable *>(
                                                table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
  }

  /* update table store */
  if (OB_FAIL(ret)) {
  } else if (nullptr == first_major_sstable) { /* do nothing when major sstable exist */
    if (OB_FAIL(ObDDLMergeTaskUtils::build_sstable(merge_param, co_sstable_array, major_sstable)))  {
      LOG_WARN("failed to build sstable", K(ret));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::update_tablet_table_store(merge_param, co_sstable_array, major_sstable))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(merge_param));
    }
  } else {
    /* only update checkpoint when major already exist */
    if (OB_FAIL(ObDDLMergeTaskUtils::only_update_ddl_checkpoint(merge_param))) {
      LOG_WARN("failed to only update ddl checkpoint", K(ret), K(merge_param));
    }
  }

  /* release ddl kv when build major sstable */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(merge_param));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(merge_param));
    }
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(ObDDLKVType::DDL_KV_FULL, merge_param.for_major_ ? share::SCN::max_scn() : merge_param.rec_scn_))) {
    LOG_WARN("release all ddl kv failed", K(ret), K(merge_param));
  }

  /* report check sum */
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ERRSIM_DDL_RETRY_REPORT_CHECKSUM)) {
    ret = OB_EAGAIN;
    DEBUG_SYNC(AFTER_DDL_RETRY_REPORT_CHECKSUM);
  } else if (merge_param.for_major_ &&
             !merge_param.for_replay_ &&
             !merge_param.for_lob_ &&
             OB_FAIL(ObDDLUtil::report_ddl_checksum_from_major_sstable(target_ls_id,
                                                                       target_tablet_id,
                                                                       merge_param.ddl_task_param_.target_table_id_,
                                                                       merge_param.ddl_task_param_.execution_id_,
                                                                       merge_param.ddl_task_param_.ddl_task_id_,
                                                                       merge_param.ddl_task_param_.tenant_data_version_))) {
    LOG_WARN("failed to report ddl checksum", K(ret), K(merge_param));
  }
  return ret;
}

bool ObSNDDLMergeHelperV2::is_supported_direct_load_type(const ObDirectLoadType direct_load_type)
{
  return ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL  == direct_load_type ||
         ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DATA == direct_load_type;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSSDDLMergeHelper::process_prepare_task(ObIDag *dag,
                                             ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                             ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices)
{
  int ret = OB_SUCCESS;

  cg_slices.reset();

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObTabletHandle tablet_handle;
  ObStorageSchema                 *storage_schema = nullptr;
  ObWriteTabletParam              *tablet_param   = nullptr;
  ObDDLTabletContext::MergeCtx    *merge_ctx      = nullptr;

  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  hash::ObHashSet<int64_t> slice_idxes;
  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  const ObSSTable *first_major_sstable = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (nullptr == dag || !dag_merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(dag), K(dag_merge_param));
  } else if (!is_supported_direct_load_type(dag_merge_param.direct_load_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported direct load type", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get merge param", K(ret));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (nullptr == tablet_param || nullptr == merge_ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_ctx should not be null", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_ISNULL(storage_schema = tablet_param->storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema should not be null", K(ret), K(dag_merge_param));
  }

  /* prepare cg sstable array in context
   * for ss mode, slice count should always be 1
   */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(slice_idxes.create(1007, ObMemAttr(MTL_ID(), "slice_idx_set")))) {
    LOG_WARN("create slice index set failed", K(ret));
  } else if (OB_FAIL(slice_idxes.set_refactored(0))) {
      LOG_WARN("failed to set refactored", K(ret)); // should have at least one slice in slice idx
  } else if (OB_FAIL(dag_merge_param.init_cg_sstable_array(slice_idxes))) { /* for both major and dump only slice exist*/
    LOG_WARN("failed to set cg slice sstable", K(ret), K(dag_merge_param));
  }


  /* preppare for dump sstable */
  if (OB_FAIL(ret)) {
  } else if (ObITable::is_ddl_dump_sstable(dag_merge_param.table_key_.table_type_)) {
    /* freeze & get frozen ddl kvs */
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TASK_EXPIRED;
        LOG_INFO("ddl kv mgr not exist", K(ret), K(dag_merge_param));
      } else {
        LOG_WARN("get ddl kv mgr failed", K(ret), K(dag_merge_param));
      }
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(true/*frozen_only*/, frozen_ddl_kvs))) {
      LOG_WARN("get freezed ddl kv failed", K(ret), K(dag_merge_param));
    } else if (OB_FAIL(merge_ctx->ddl_kv_handles_.assign(frozen_ddl_kvs))) {
      LOG_WARN("failed to frozen ddl kv", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
      int max_retry_cnt = 10000;
      while(max_retry_cnt > 0) {
        max_retry_cnt--;
        if (OB_FAIL(frozen_ddl_kvs.at(i).get_obj()->close())) {
          if (OB_EAGAIN != ret) {
            break;
          } else {
            ob_usleep(500);
          }
        } else {
          break;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("falied to close frozen ddl kv", K(ret), K(max_retry_cnt));
      }
    }

    /* check if major sstable already exist */
    if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_FALSE_IT(first_major_sstable = static_cast<ObSSTable *>(
                                                table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
    } else if (nullptr != first_major_sstable) {
      if (ddl_kv_mgr_handle.is_valid() &&
          OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(ObDDLKVType::DDL_KV_FULL, share::SCN::max_scn()))) {
        LOG_WARN("release all ddl kv failed", K(ret), K(dag_merge_param));
      } else {
        ret = OB_TASK_EXPIRED;
        LOG_WARN("major sstable has already exist", K(ret), K(dag_merge_param));
      }
    }
    /* set in cg_slice_sstable_array*/
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cg_slices.push_back(ObTuple<int64_t, int64_t, int64_t>(0, 0, 0)))) {
      LOG_WARN("failed to push back values", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!ObITable::is_ddl_dump_sstable(dag_merge_param.table_key_.table_type_)) {
    /* for ss mode, major build from cg meta file, follower not need build major*/
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_param->storage_schema_->get_column_group_count(); i++) {
      /* when build major only one slice is needed */
      if (OB_FAIL(cg_slices.push_back(ObTuple<int64_t, int64_t, int64_t>(i /* cg_idx */, 0 /* start_slice */, 0 /* end_slice */)))) {
        LOG_WARN("failed to get slice", K(ret));
      }
    }
  }
  return ret;
}

int ObSSDDLMergeHelper::merge_cg_slice(ObIDag* dag,
                                       ObDDLTabletMergeDagParamV2 & dag_merge_param,
                                       const int64_t cg_idx,
                                       const int64_t start_slice,
                                       const int64_t end_slice)
{
  int ret = OB_SUCCESS;
  if (nullptr == dag || !dag_merge_param.is_valid() || cg_idx < 0 || start_slice > end_slice) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(dag), K(dag_merge_param), K(cg_idx), K(start_slice), K(end_slice));
  } else if (dag_merge_param.for_major_ &&
             OB_FAIL(merge_cg_sstable(dag, dag_merge_param, cg_idx))) {
    LOG_WARN("failed to merge cg sstable", K(ret));
  } else if (!dag_merge_param.for_major_ &&
             OB_FAIL(merge_dump_sstable(dag_merge_param))) {
    LOG_WARN("failed to dump sstable", K(ret), K(dag_merge_param));
  }
  return ret;
}

bool ObSSDDLMergeHelper::is_supported_direct_load_type(const ObDirectLoadType direct_load_type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL_V2 == direct_load_type ||
         ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2 == direct_load_type ||
         ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DDL  == direct_load_type ||
         ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DATA == direct_load_type;
}

int ObSSDDLMergeHelper::merge_dump_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param)
{
  int ret = OB_SUCCESS;

  ObLSID ls_id;
  ObTabletID tablet_id;
  ObArenaAllocator                *allocator      = nullptr;
  ObStorageSchema                 *storage_schema = nullptr;
  ObWriteTabletParam              *tablet_param   = nullptr;
  ObDDLTabletContext::MergeCtx    *merge_ctx      = nullptr;
  ObArray<ObSSTable*> ddl_sstables;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;

  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  ObTableHandleV2 ro_sstable_handle;
  ObArray<MacroBlockId> macro_id_array;
  hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode> macro_id_set;

  /* check arg invalid */
  if (OB_UNLIKELY(!dag_merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param));
  } else if (!is_supported_direct_load_type(dag_merge_param.direct_load_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported direct load type", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (nullptr == tablet_param || nullptr == merge_ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet param should not be null", K(ret), K(dag_merge_param));
  } else if (FALSE_IT(allocator = &merge_ctx->arena_)) {
  } else if (OB_ISNULL(storage_schema = tablet_param->storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_schema should not be null", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(macro_id_set.create(1023, ObMemAttr(MTL_ID(), "ss_ddl_id_arr")))) {
    LOG_WARN("create set of macro block id failed", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  }

  SMART_VAR(ObTableStoreIterator, ddl_sstable_iter) {
  if (OB_FAIL(ret)) {
  } else if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet handle is invalid", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_sstables(ddl_sstable_iter))) {
    LOG_WARN("failed to get ddl sstable", K(ret));
  } else if (OB_UNLIKELY(ddl_sstable_iter.count() == 0 && merge_ctx->ddl_kv_handles_.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstable_iter.count()), K(merge_ctx->ddl_kv_handles_.count()));
  } else {
    // 1. get macro id from ddl sstable
    ObSSTableMetaHandle meta_handle;
    ObMacroIdIterator macro_id_iter;
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      meta_handle.reset();
      macro_id_iter.reset();
      if (OB_FAIL(ddl_sstable_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next table failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable()) || OB_ISNULL(sstable = static_cast<ObSSTable *>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table), KPC(sstable));
      } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
        LOG_WARN("get sstable meta handle failed", K(ret));
      } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_other_block_iter(macro_id_iter))) {
        LOG_WARN("get other block iterator failed", K(ret));
      } else if (OB_FAIL(ddl_sstables.push_back(static_cast<ObSSTable *>(table)))) {
        LOG_WARN("failed to push back ddl sstable", K(ret));
      } else {
        MacroBlockId macro_id;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(macro_id_iter.get_next_macro_id(macro_id))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next macro id failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(macro_id_set.set_refactored(macro_id))) {
            LOG_WARN("set macro id failed", K(ret), K(macro_id));
          }
        }
      }
    }
    }

    // 2. get macro block id from ddl kv
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_ctx->ddl_kv_handles_.count(); ++i) {
      ObDDLKV *cur_kv = merge_ctx->ddl_kv_handles_.at(i).get_obj();
      ObDDLMemtable *ddl_memtable = nullptr;
      ObArray<MacroBlockId> macro_id_array;
      if (OB_ISNULL(cur_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv should not be null", K(ret));
      } else if (cur_kv->get_ddl_memtables().empty()) {
        // do nothing
      } else if (OB_ISNULL(ddl_memtable = cur_kv->get_ddl_memtables().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl_mem_table should not be null", K(ret), K(dag_merge_param));
      } else if (OB_FAIL(ddl_memtable->get_block_meta_tree()->get_macro_id_array(macro_id_array))) {
        LOG_WARN("get macro id array failed", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < macro_id_array.count(); ++j) {
          if (OB_FAIL(macro_id_set.set_refactored(macro_id_array.at(j)))) {
            LOG_WARN("set macro id failed", K(ret), K(macro_id_array.at(j)));
          }
        }
      }
    }

    // 3. persist macro id set into other_block_ids of compacted sstable
    if (OB_SUCC(ret)) {
      ObArray<ObDDLBlockMeta> empty_meta_array;
      hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode>::iterator iter = macro_id_set.begin();
      if (OB_FAIL(macro_id_array.reserve(macro_id_set.size()))) {
        LOG_WARN("reserve macro id array failed", K(ret));
      }
      for (; OB_SUCC(ret) && iter != macro_id_set.end(); ++iter) {
        const MacroBlockId &cur_id = iter->first;
        if (OB_FAIL(macro_id_array.push_back(cur_id))) {
          LOG_WARN("push back macro id failed", K(ret), K(cur_id));
        }
      }

      ObTabletDDLParam ddl_param;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(prepare_ddl_param(dag_merge_param, 0 /* cg_idx */, 0 /* start_slice_idx */, 0 /* end_slice_idx */, ddl_param))) {
        LOG_WARN("failed to prepare ddl param", K(ret));
      } else if (!dag_merge_param.for_major_ && OB_FAIL(calc_scn_range(merge_ctx->ddl_kv_handles_, ddl_sstables,
                                              ddl_param.table_key_.scn_range_.start_scn_,
                                              ddl_param.table_key_.scn_range_.end_scn_,
                                              ddl_param.snapshot_version_))) {
        LOG_WARN("failed to get ddl kv scn range", K(ret), K(ddl_param));
      } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(*(tablet_handle.get_obj()),
                                                             ddl_param,
                                                             empty_meta_array,
                                                             macro_id_array,
                                                             nullptr/*first ddl sstable*/,
                                                             storage_schema,
                                                             &merge_ctx->mutex_,
                                                             *allocator,
                                                             ro_sstable_handle))) {
        LOG_WARN("create sstable failed", K(ret), K(ddl_param));
      } else if (OB_FAIL(dag_merge_param.set_cg_slice_sstable(0, 0, ro_sstable_handle))) {
        LOG_WARN("failed to set cg slice sstable", K(ret));
      }
    }
  }
  return ret;
}

void ObSSDDLMergeHelper::update_max_meta_seq(const int64_t seq)
{
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  max_meta_seq_ = max(max_meta_seq_, seq);
}

int64_t ObSSDDLMergeHelper::get_next_max_meta_seq()
{
  int64_t res = 0;
  {
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    res = max_meta_seq_ + compaction::MACRO_STEP_SIZE;
  }
  return res;
}

int ObSSDDLMergeHelper::prepare_cg_table_key(const ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                             const int64_t cg_idx,
                                             const ObStorageSchema *storage_schema,
                                             ObITable::TableKey &cur_cg_table_key)
{
  int ret = OB_SUCCESS;
  if (!dag_merge_param.is_valid() || cg_idx < 0 || nullptr == storage_schema) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), K(cg_idx), KPC(storage_schema));
  } else if (FALSE_IT(cur_cg_table_key = dag_merge_param.table_key_)) {           /* init table key */
  } else if (!storage_schema->is_row_store()) {
    if (cg_idx == dag_merge_param.table_key_.column_group_idx_) {                 /* update for cg when target is column store */
      cur_cg_table_key.table_type_ =  ObITable::TableType::COLUMN_ORIENTED_SSTABLE;
    } else {
      cur_cg_table_key.table_type_ =  ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
      cur_cg_table_key.column_group_idx_ = cg_idx;
    }
  }
  return ret;
}

int ObSSDDLMergeHelper::get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param)
{
  return ObIDDLMergeHelper::get_rec_scn_from_ddl_kvs(merge_param);
}
int ObSSDDLMergeHelper::merge_cg_sstable(ObIDag *dag,
                                         ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                         int64_t cg_idx)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObArenaAllocator  *allocator = nullptr;
  ObStorageSchema   *storage_schema = nullptr;
  ObWriteTabletParam              *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx    *merge_ctx    = nullptr;

  ObITable::TableKey cur_cg_table_key;
  ObTableHandleV2 cg_sstable_handle;
  ObMacroMetaStoreManager *store_mgr = nullptr;
  ObArray<ObMacroMetaStoreManager::StoreItem> sorted_meta_stores;

  ObTabletHandle tablet_handle;
  int64_t create_schema_version_on_tablet = 0;

  if (nullptr == dag || !dag_merge_param.is_valid() || cg_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(dag), K(cg_idx));
  } else if (!dag_merge_param.for_major_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this func is only used for major merge", K(ret));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (nullptr == tablet_param || nullptr == merge_ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet param should not be null", K(ret));
  } else if (OB_ISNULL(storage_schema = tablet_param->storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema should not be null", K(ret), KPC(tablet_param));
  } else if (FALSE_IT(allocator = &merge_ctx->arena_)) {
  } else if (OB_FAIL(prepare_cg_table_key(dag_merge_param, cg_idx, storage_schema, cur_cg_table_key))) {
    LOG_WARN("failed to prepare cg table key", K(ret));
  } else if (OB_FAIL(get_meta_store_store(dag_merge_param, tablet_id, cg_idx, sorted_meta_stores))) {
    LOG_WARN("failed to get store mgr", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    HEAP_VARS_3((ObSSTableIndexBuilder, sstable_index_builder, true /*use buffer*/),
                (ObIndexBlockRebuilder, index_block_rebuilder),
                (storage::ObDDLRedoLogWriterCallback, clustered_index_flush_callback)) {
    SMART_VARS_3((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, param), (ObWholeDataStoreDesc, data_desc)) {
      ObMacroSeqParam macro_seq_param;
      macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      macro_seq_param.start_ = 0;

      ObDDLRedoLogWriterCallbackInitParam init_param;
      init_param.ls_id_ = ls_id;
      init_param.tablet_id_ = tablet_id;
      init_param.block_type_ = DDL_MB_INDEX_TYPE;
      init_param.table_key_ = cur_cg_table_key;
      init_param.start_scn_ = dag_merge_param.start_scn_;
      init_param.task_id_ = dag_merge_param.ddl_task_param_.ddl_task_id_;
      init_param.data_format_version_ = dag_merge_param.ddl_task_param_.tenant_data_version_;
      init_param.parallel_cnt_ = max(dag_merge_param.get_tablet_ctx()->slice_count_, 1); /* slice count may be 0, need greater than 0*/
      init_param.cg_cnt_ = storage_schema->get_column_group_count();
      init_param.row_id_offset_ = 0;

      const ObStorageColumnGroupSchema &cg_schema = storage_schema->get_column_groups().at(cg_idx);
      if (OB_FAIL(data_desc.init(true/*is_ddl*/, *storage_schema,
                                 ls_id, tablet_id,
                                 compaction::ObMergeType::MAJOR_MERGE,
                                 dag_merge_param.table_key_.get_snapshot_version(),
                                 dag_merge_param.ddl_task_param_.tenant_data_version_,
                                 tablet_param->is_micro_index_clustered_,
                                 tablet_param->tablet_transfer_seq_,
                                 0, /* concurrent_cnt */
                                 tablet_handle.get_obj()->get_reorganization_scn(),
                                 SCN::min_scn(),
                                 &cg_schema,
                                 cg_idx))) {
        LOG_WARN("init data store desc failed", K(ret), K(tablet_id));
      } else {
        data_desc.get_static_desc().exec_mode_ = compaction::EXEC_MODE_OUTPUT;
        data_desc.get_static_desc().schema_version_ = storage_schema->get_schema_version();
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sstable_index_builder.init(data_desc.get_desc(), ObSSTableIndexBuilder::DISABLE/*small SSTable op*/))) {
        LOG_WARN("init sstable index builder failed", K(ret), K(data_desc));
      } else if (tablet_param->is_micro_index_clustered_) {
        init_param.direct_load_type_ = dag_merge_param.direct_load_type_;
        if (OB_FAIL(clustered_index_flush_callback.init(init_param))) {
          LOG_WARN("failed to init call back", K(ret), K(init_param));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(index_block_rebuilder.init(sstable_index_builder, macro_seq_param, 0/*task_idx*/, dag_merge_param.table_key_,
                                                    tablet_param->is_micro_index_clustered_ ? &clustered_index_flush_callback : nullptr))) {
        LOG_WARN("fail to alloc index builder", K(ret));
      } else {
        SMART_VARS_3((ObMacroMetaTempStoreIter, macro_meta_iter), (ObDataMacroBlockMeta, macro_meta), (ObMicroBlockData, leaf_index_block)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < sorted_meta_stores.count(); ++i) {
          ObMacroMetaTempStore *cur_macro_meta_store = sorted_meta_stores.at(i).macro_meta_store_;
          FLOG_INFO("switch to next meta store", K(ret), K(i), K(sorted_meta_stores.at(i)));
          macro_meta_iter.reset();
          macro_meta.reset();
          leaf_index_block.reset();
          if (OB_ISNULL(cur_macro_meta_store)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("current macro meta store is null", K(ret));
          } else if (OB_FAIL(macro_meta_iter.init(*cur_macro_meta_store))) {
            LOG_WARN("init macro meta store iterator failed", K(ret));
          } else {
            while (OB_SUCC(ret)) {
              if (OB_FAIL(macro_meta_iter.get_next(macro_meta, leaf_index_block))) {
                if (OB_ITER_END != ret) {
                  LOG_WARN("get next macro meta failed", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  break;
                }
              } else if (OB_FAIL(index_block_rebuilder.append_macro_row(macro_meta, leaf_index_block.get_buf(), leaf_index_block.get_buf_size()))) {
                LOG_WARN("append macro row failed", K(ret), K(macro_meta), K(leaf_index_block));
              } else {
                LOG_TRACE("rebuilder append macro row", K(ret), K(macro_meta));
              }
            }
          }
        }
        }
      }

      // In single-threaded construction of the index tree, macro seq starts from `task_cnt * 2 ^ 25`.
      ObMacroDataSeq tmp_seq(dag_merge_param.get_tablet_ctx()->slice_count_ * compaction::MACRO_STEP_SIZE);
      tmp_seq.set_index_block();
      share::ObPreWarmerParam pre_warm_param;
      storage::ObDDLRedoLogWriterCallback flush_callback;
      init_param.direct_load_type_ = DIRECT_LOAD_LOAD_DATA_V2;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(index_block_rebuilder.close())) {
        LOG_WARN("close index block rebuilder failed", K(ret));
      } else if (OB_FAIL(pre_warm_param.init(ls_id, tablet_id))) {
        LOG_WARN("failed to init pre warm param", KR(ret));
      } else if (OB_FAIL(flush_callback.init(init_param))) {
        // The row_id_offset of cg index_macro_block has no effect, so set 0 to pass the defensive check.
        LOG_WARN("fail to init redo log writer callback", K(ret), K(cur_cg_table_key), K(init_param));
      } else if (OB_FAIL(sstable_index_builder.close_with_macro_seq(
                         res, tmp_seq.macro_data_seq_,
                         OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                         0 /*nested_offset*/, pre_warm_param, &flush_callback))) {
        LOG_WARN("fail to close", K(ret), K(sstable_index_builder), K(tmp_seq), K(dag_merge_param));
      } else {
        update_max_meta_seq(tmp_seq.macro_data_seq_);
        res.root_macro_seq_ = get_next_max_meta_seq();
      }


      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(param.init_for_ss_ddl(res,
                                               cur_cg_table_key,
                                               *storage_schema,
                                               create_schema_version_on_tablet,
                                               dag_merge_param.direct_load_type_,
                                               dag_merge_param.trans_id_,
                                               dag_merge_param.seq_no_))) {
        LOG_WARN("fail to init ddl param for ddl", K(ret), K(create_schema_version_on_tablet),
          K(sstable_index_builder), K(cur_cg_table_key), KPC(storage_schema));
      } else if (!param.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param", K(ret), K(param.is_valid()), K(param));
      } else {
        ObMutexGuard guard(merge_ctx->mutex_);
        if (cur_cg_table_key.is_co_sstable()) {
          if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, *allocator, cg_sstable_handle))) {
            LOG_WARN("create sstable failed", K(ret), K(param));
          }
        } else {
          if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(param, *allocator, cg_sstable_handle))) {
            LOG_WARN("create sstable failed", K(ret), K(param));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(update_max_meta_seq(tmp_seq.macro_data_seq_))) {
      } else if (OB_FAIL(dag_merge_param.set_cg_slice_sstable(0, cg_idx, cg_sstable_handle))) {
        LOG_WARN("failed to set cg slice sstable", K(ret), K(dag_merge_param));
      }
    }// SMART_VARS_2
    }// HEAP_VAR
  }
  return ret;
}

int ObSSDDLMergeHelper::build_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                      ObTablesHandleArray &co_sstable_array,
                                      ObSSTable *&major_sstable)
{
  int ret = OB_SUCCESS;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam  *tablet_param = nullptr;

  major_sstable = nullptr;
  bool is_column_store_table = false;
  bool for_major = dag_merge_param.for_major_;
  hash::ObHashMap<int64_t, ObArray<ObTableHandleV2>*> *slice_idx_cg_sstables = nullptr;
  ObDDLTabletContext::MergeCtx    *merge_ctx = nullptr;
  if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_ISNULL(tablet_param->storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet param should not be null", K(ret), K(lbt()));
  } else if (FALSE_IT(slice_idx_cg_sstables = dag_merge_param.for_lob_ ? &dag_merge_param.get_tablet_ctx()->lob_merge_ctx_.slice_cg_sstables_ :
                                                                     &dag_merge_param.get_tablet_ctx()->merge_ctx_.slice_cg_sstables_)) {
  } else if (FALSE_IT(is_column_store_table = ObITable::is_column_store_sstable(dag_merge_param.table_key_.table_type_))) {
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx should not be null", K(ret), K(dag_merge_param));
  } else if (!dag_merge_param.for_major_) {
    ObArray<ObTableHandleV2> *table_array = nullptr;
    if (OB_FAIL(merge_ctx->slice_cg_sstables_.get_refactored(0, table_array))) {
      LOG_WARN("failed to get slice cg sstables", K(ret));
    } else if (0 == table_array->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no sstable to upload", K(ret));
    } else {
      major_sstable = static_cast<ObSSTable*>(table_array->at(0).get_table());
    }
  } else {
    for (hash::ObHashMap<int64_t, ObArray<ObTableHandleV2>*>::const_iterator iter = slice_idx_cg_sstables->begin();
         OB_SUCC(ret) && iter != slice_idx_cg_sstables->end();
         iter++) {
      int64_t start_slice_idx_ = iter->first;
      ObArray<ObTableHandleV2> *sstable_handles = iter->second;

      /* get root sstable */
      ObTableHandleV2 &root_sstable  = sstable_handles->at(dag_merge_param.table_key_.get_column_group_id());

      ObArray<ObITable*> cg_sstables;
      for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < sstable_handles->count(); cg_idx++) {
        if (cg_idx != dag_merge_param.table_key_.get_column_group_id() &&
            OB_FAIL(cg_sstables.push_back(sstable_handles->at(cg_idx).get_table()))) {
          LOG_WARN("failed to push back val", K(ret));
        }
      }

      /* fill cg sstable when sstable is not empty*/
      if (OB_FAIL(ret)) {
      } else if (is_column_store_table &&
                 !static_cast<ObCOSSTableV2*>(root_sstable.get_table())->is_cgs_empty_co_table() &&
                 OB_FAIL(static_cast<ObCOSSTableV2*>(root_sstable.get_table())->fill_cg_sstables(cg_sstables))) {
        LOG_WARN("failed to fill cg sstable", K(ret));
      } else if (OB_FAIL(co_sstable_array.add_table(root_sstable))) {
        LOG_WARN("failed to add table");
      } else if (for_major && 0 == start_slice_idx_) {
        major_sstable = static_cast<ObSSTable*>(root_sstable.get_table());
      }
    }
  }
  return ret;
}

int ObSSDDLMergeHelper::write_ddl_finish_log(ObDDLTabletMergeDagParamV2 &dag_merge_param, ObSSTable *&major_sstable)
{
  int ret = OB_SUCCESS;

  int64_t tenant_data_version = 0;
  ObITable::TableKey table_key;
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam  *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  ObDDLFinishLog finish_log;
  bool is_remote_write = false;
  storage::ObDDLRedoLogWriter ddl_clog_writer;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObTabletHandle tablet_handle;
  ObIAllocator *allocator = nullptr;

  if (!dag_merge_param.is_valid() || OB_ISNULL(major_sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), KP(major_sstable));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is null", K(ret));
  } else if (OB_ISNULL(allocator = &merge_ctx->arena_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(dag_merge_param));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(dag_merge_param));
    }
    LOG_WARN("failed to get ddl kv mgr", K(ret));
  } else {
    tenant_data_version = dag_merge_param.ddl_task_param_.tenant_data_version_;
    table_key = dag_merge_param.table_key_;
  }

  if (OB_FAIL(ret)) {
  } else {
    char *buf = NULL;
    int64_t pos = 0;
    ObSSTablePersistWrapper sstable_persister(tenant_data_version, major_sstable);
    const int64_t buf_len = sstable_persister.get_serialize_size();
    if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc for sstable fail", K(ret), K(buf_len), K(major_sstable));
    } else if (OB_FAIL(sstable_persister.serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize sstable fail", K(ret));
    } else if (OB_FAIL(finish_log.init(MTL_ID(), target_ls_id, table_key, buf, pos, tenant_data_version))) {
      LOG_WARN("init finish log fail", K(ret));
    } else if (OB_FAIL(ddl_clog_writer.init(target_ls_id, target_tablet_id))) {
      LOG_WARN("fail to init ddl clog writer", K(ret), K(target_ls_id), K(target_tablet_id));
    } else if (OB_FAIL(ddl_clog_writer.write_finish_log_with_retry(true, finish_log, is_remote_write))) {
      LOG_WARN("fail write finish log", K(ret), K(finish_log));
    } else if (is_remote_write) {
      LOG_INFO("remote write ddl log succ");
    } else if (OB_FAIL(ddl_clog_writer.wait_finish_log(target_ls_id, table_key, tenant_data_version))) {
      LOG_WARN("fail wait finish log", K(ret));
    }
  }
  return ret;
}


int ObSSDDLMergeHelper::write_partial_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                              ObSSTable *&major_sstable,
                                              ObSSTable *&out_sstable)
{
  int ret = OB_SUCCESS;
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam  *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;
  ObArenaAllocator *allocator = nullptr;

  ObTabletHandle tablet_handle;
  storage::ObDDLRedoLogWriterCallback callback;
  out_sstable = nullptr;
  if (!dag_merge_param.is_valid() || OB_ISNULL(major_sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), KP(major_sstable));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is null", K(ret));
  } else if (FALSE_IT(allocator = &merge_ctx->arena_)) {
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else {
    ObDDLRedoLogWriterCallbackInitParam init_param;
    init_param.ls_id_ = target_ls_id;
    init_param.tablet_id_ = target_tablet_id;
    init_param.direct_load_type_ = dag_merge_param.direct_load_type_;
    init_param.block_type_ = dag_merge_param.ddl_task_param_.is_no_logging_ ? DDL_MB_SS_EMPTY_DATA_TYPE : DDL_MB_SSTABLE_META_TYPE;
    init_param.table_key_ = major_sstable->get_key();
    init_param.start_scn_ = dag_merge_param.start_scn_;
    init_param.task_id_ = dag_merge_param.ddl_task_param_.ddl_task_id_;
    init_param.data_format_version_ = dag_merge_param.ddl_task_param_.tenant_data_version_;
    init_param.parallel_cnt_ = max(dag_merge_param.get_tablet_ctx()->slice_count_, 1);
    init_param.cg_cnt_ = tablet_param->storage_schema_->get_column_group_count();
    init_param.row_id_offset_ = 0;
    if (OB_FAIL(callback.init(init_param))) {
      LOG_WARN("fail to init redo log writer callback", K(ret), K(init_param));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObTabletPersisterParam param(dag_merge_param.ddl_task_param_.tenant_data_version_,
                                       target_tablet_id,
                                       tablet_handle.get_obj()->get_transfer_seq(),
                                       major_sstable->get_key().get_snapshot_version(),
                                       get_next_max_meta_seq(),
                                       &callback,
                                       nullptr/*ddl finish callback*/);
    int64_t out_macro_seq = get_next_max_meta_seq();
    ObCOSSTableV2 *out_co_sstable = NULL;
    if (FAILEDx(ObTabletPersister::persist_major_sstable_linked_block_if_large(*allocator,
                                                                             param,
                                                                             *major_sstable,
                                                                             out_co_sstable,
                                                                             out_macro_seq))) {
      LOG_WARN("persist major to linked block fail", K(ret), K(param), K(major_sstable));
    } else {
      LOG_INFO("persist large major to linekd block succ", K(major_sstable), KPC(out_sstable), K(out_macro_seq));
      out_sstable = nullptr == out_co_sstable ? major_sstable : out_co_sstable;
      update_max_meta_seq(out_macro_seq);
    }
  }
  return ret;
}



int ObSSDDLMergeHelper::update_tablet_table_store(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                                  ObTablesHandleArray &table_array,
                                                  ObSSTable *&major_sstable)
{
  int ret = OB_SUCCESS;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam  *tablet_param = nullptr;

  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;

  int64_t rebuild_seq = -1;
  int64_t snapshot_version = 0;
  int64_t multi_version_start = 0;

  bool for_major = dag_merge_param.for_major_;
  ObLSService *ls_service = MTL(ObLSService*);

  if (!dag_merge_param.is_valid() || nullptr == major_sstable) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), KPC(major_sstable));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(target_ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (!ls_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls handle", K(ret), K(target_ls_id));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else {
    rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    snapshot_version    = dag_merge_param.for_major_ ? max(dag_merge_param.ddl_task_param_.snapshot_version_, tablet_handle.get_obj()->get_snapshot_version()) :
                                  tablet_handle.get_obj()->get_snapshot_version();
    multi_version_start =  dag_merge_param.for_major_ ? max(dag_merge_param.ddl_task_param_.snapshot_version_, tablet_handle.get_obj()->get_multi_version_start()) :
                                      0;
  }

  if (OB_FAIL(ret)) {
  } else {
    ObUpdateTableStoreParam table_store_param(snapshot_version, multi_version_start, tablet_param->storage_schema_, rebuild_seq, major_sstable);
    if (OB_FAIL(table_store_param.init_with_compaction_info(ObCompactionTableStoreParam(for_major ? compaction::MEDIUM_MERGE : compaction::MINI_MERGE,
                                                                                        share::SCN::min_scn(),
                                                                                        for_major /* need_report*/,
                                                                                          false /* has truncate info*/)))) {
      LOG_WARN("init with compaction info failed", K(ret));
    } else {
      table_store_param.ddl_info_.update_with_major_flag_ =  for_major;
      table_store_param.ddl_info_.keep_old_ddl_sstable_   =  !for_major;
      table_store_param.ddl_info_.data_format_version_    = dag_merge_param.ddl_task_param_.tenant_data_version_;
      table_store_param.ddl_info_.ddl_commit_scn_         = dag_merge_param.rec_scn_;
      table_store_param.ddl_info_.ddl_checkpoint_scn_      = dag_merge_param.rec_scn_;

      if (!dag_merge_param.for_major_) {
        // data is not complete, now update ddl table store only for reducing count of ddl dump sstable.
        table_store_param.ddl_info_.ddl_replay_status_ = tablet_handle.get_obj()->get_tablet_meta().ddl_replay_status_;
      } else {
        // data is complete, mark ddl replay status finished
        table_store_param.ddl_info_.ddl_replay_status_ = dag_merge_param.table_key_.is_co_sstable() ? CS_REPLICA_REPLAY_COLUMN_FINISH : CS_REPLICA_REPLAY_ROW_STORE_FINISH;
      }

      for (int64_t i = 0; !for_major && OB_SUCC(ret) && i < table_array.get_count(); i++ ) {
        const ObSSTable *cur_slice_sstable = static_cast<ObSSTable *>(table_array.get_table(i));
        if (OB_ISNULL(cur_slice_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("slice sstable is null", K(ret), K(i), KP(cur_slice_sstable));
        } else if (OB_FAIL(table_store_param.ddl_info_.slice_sstables_.push_back(cur_slice_sstable))) {
          LOG_WARN("push back slice ddl sstable failed", K(ret), K(i), KPC(cur_slice_sstable));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!dag_merge_param.for_major_) {
        /* update local merge param for dump sstable */
        ObTableStoreIterator iter;
        ObSSTableUploadRegHandle upload_register_handle;
        if (OB_FAIL(ls_handle.get_ls()->prepare_register_sstable_upload(upload_register_handle))) {
            LOG_WARN("prepare mini sstable upload register fail", K(ret));
        } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(target_tablet_id, table_store_param, new_tablet_handle))) {
          LOG_WARN("failed to update tablet table store", K(ret), K(target_tablet_id), K(table_store_param));
        } else if (OB_FAIL(new_tablet_handle.get_obj()->get_ddl_sstables(iter))) {
          LOG_ERROR("get ddl sstables fail", K(new_tablet_handle));
        } else if (!iter.is_valid()) {
          // TODO : @yanyuan.cxf Directly Update SS Tablet Meta (ddl_checkpoint_scn)
          FLOG_INFO("no ddl sstable to upload", K(new_tablet_handle), K(dag_merge_param.table_key_));
        } else {
          ASYNC_UPLOAD_INC_SSTABLE(SSIncSSTableType::DDL_SSTABLE,
                                   upload_register_handle,
                                   major_sstable->get_key(),
                                   SCN::min_scn() /* ddl sstable no need snapshot_version */);
        }
      } else {
        if (OB_FAIL(update_major_table_store(dag_merge_param, major_sstable))) {
          LOG_WARN("failed to update table store", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObSSDDLMergeHelper::update_major_table_store(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                                  ObSSTable *&major_sstable)
{
  int ret = OB_SUCCESS;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam  *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;

  int64_t rebuild_seq = -1;
  int64_t snapshot_version = 0;
  int64_t multi_version_start = 0;

  bool for_major = dag_merge_param.for_major_;
  ObLSService *ls_service = MTL(ObLSService*);
  share::SCN transfer_scn;
  ObITable::TableKey table_key;
  ObArenaAllocator *allocator = nullptr;
  ObSSTable *out_sstable = nullptr;
  bool is_exist = false;

  if (!dag_merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(target_ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (!ls_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls handle", K(ret), K(target_ls_id));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else {
    rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    snapshot_version    = dag_merge_param.for_major_ ? max(dag_merge_param.ddl_task_param_.snapshot_version_, tablet_handle.get_obj()->get_snapshot_version()) :
                                  tablet_handle.get_obj()->get_snapshot_version();
    multi_version_start =  dag_merge_param.for_major_ ? max(dag_merge_param.ddl_task_param_.snapshot_version_, tablet_handle.get_obj()->get_multi_version_start()) :
                                      0;
    transfer_scn = tablet_handle.get_obj()->get_reorganization_scn();
    table_key = dag_merge_param.table_key_;
    allocator = &merge_ctx->arena_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(write_partial_sstable(dag_merge_param, major_sstable, out_sstable))) {
    LOG_WARN("persist large sstable partially fail", K(ret), KPC(major_sstable));
  } else if (OB_FAIL(ObSSDDLUtil::check_ddl_shared_major_exist(target_ls_id, target_tablet_id, transfer_scn, table_key.get_snapshot_version(), *allocator, is_exist))) {
    LOG_WARN("check major exist fail", K(ret), K(table_key), K(target_ls_id));
  } else if (is_exist) {
    LOG_INFO("major already exist on shared tablet, skip update shared tablet", K(ret), K(table_key), K(target_ls_id));
  } else if (OB_FAIL(ObSSDDLUtil::update_shared_tablet_table_store(ls_handle, *out_sstable, *tablet_param->storage_schema_, dag_merge_param.ddl_task_param_.tenant_data_version_, transfer_scn))) {
    LOG_WARN("failed to update shared tablet", K(ret), K(target_ls_id), K(target_tablet_id));
  }

  /* cannot skip write finish log, even if major already exist */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(write_ddl_finish_log(dag_merge_param, out_sstable))) {
    LOG_WARN("write ddl finish log fail", K(ret));
  } else if (OB_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(target_ls_id, target_tablet_id))) {
    LOG_WARN("fail to submit tablet update task", K(ret), K(target_ls_id), K(target_tablet_id));
  } else {
    FLOG_INFO("[DDL_MERGE_TASK]update table store success", K(ret), K(target_ls_id), K(table_key));
  }

  return ret;
}

int ObSSDDLMergeHelper::assemble_sstable(ObDDLTabletMergeDagParamV2 &merge_param)
{
  int ret = OB_SUCCESS;
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam  *tablet_param = nullptr;
  ObTabletHandle tablet_handle;

  ObSSTable *major_sstable = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObTablesHandleArray co_sstable_array;

  if (!merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_param));
  } else if (OB_FAIL(merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_ISNULL(tablet_param->storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet param should not be null", K(ret), KPC(tablet_param));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_sstable(merge_param, co_sstable_array, major_sstable))) {
    LOG_WARN("failed to build major sstable", K(ret), KPC(major_sstable));
  } else if (OB_FAIL(update_tablet_table_store(merge_param, co_sstable_array, major_sstable))) {
    LOG_WARN("failed to update tablet table store", K(ret), KPC(major_sstable));
  }

  if (OB_FAIL(ret)) {
  } else if (merge_param.for_major_ &&
            !merge_param.for_replay_ &&
            !merge_param.for_lob_ &&
            OB_FAIL(ObDDLUtil::report_ddl_sstable_checksum(target_ls_id,
                                                           target_tablet_id,
                                                           merge_param.ddl_task_param_.target_table_id_,
                                                           merge_param.ddl_task_param_.execution_id_,
                                                           merge_param.ddl_task_param_.ddl_task_id_,
                                                           merge_param.ddl_task_param_.tenant_data_version_,
                                                           tablet_handle,
                                                           major_sstable))) {
    LOG_WARN("failed to report ddl checksum", K(ret), K(merge_param));
  }

  /* release ddl kv */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(merge_param));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(merge_param));
    }
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(DDL_KV_FULL, merge_param.for_major_ ? SCN::max_scn() : merge_param.rec_scn_))) {
    LOG_WARN("release all ddl kv failed", K(ret), K(merge_param));
  } else if (merge_param.for_major_ && OB_FAIL(ddl_kv_mgr_handle.get_obj()->remove_idempotence_checker())) {
    LOG_WARN("failed to remove idempotence checker", K(ret));
  }

  return ret;
}

int ObSSDDLMergeHelper::get_meta_store_store(ObDDLTabletMergeDagParamV2 &merge_param, const ObTabletID tablet_id, const int64_t cg_idx, ObArray<ObMacroMetaStoreManager::StoreItem> &sorted_meta_stores)
{
  int ret = OB_SUCCESS;
  ObMacroMetaStoreManager *store_mgr = nullptr;
  if (!merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_param));
  } else if (FALSE_IT(store_mgr = merge_param.get_tablet_ctx()->macro_meta_store_mgr_)) {
  } else if (nullptr == store_mgr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store mgr should not be null", K(ret), K(merge_param));
  } else if (OB_FAIL(store_mgr->get_sorted_macro_meta_stores(tablet_id, cg_idx, sorted_meta_stores))) {
    LOG_WARN("get sorted macro meta store array failed", K(ret), K(cg_idx));
  }
  return ret;
}
#endif

} // namespace  storage
} // namespace oceanbase
