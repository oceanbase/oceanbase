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

#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/direct_load/ob_direct_load_batch_datum_rows.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/column_store/ob_column_store_replica_util.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/ddl/ob_ddl_pipeline.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/ddl/ob_ddl_merge_helper.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;


ObDDLTabletContext::MergeCtx::~MergeCtx()
{
  fifo_.reset();
  for (hash::ObHashMap<int64_t, ObArray<ObTableHandleV2>*>::const_iterator iter = slice_cg_sstables_.begin();
      iter != slice_cg_sstables_.end();
      iter++) {
    if (nullptr != iter->second) {
      iter->second->~ObArray<ObTableHandleV2>();
    }
  }
  if (nullptr != merge_helper_) {
    merge_helper_->~ObIDDLMergeHelper();
    merge_helper_ = nullptr;
  }
  slice_cg_sstables_.destroy();
  arena_.reset();
  is_inited_ = false;
}

int ObDDLTabletContext::MergeCtx::init(const ObDirectLoadType direct_load_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(ObIDDLMergeHelper::get_merge_helper(arena_, direct_load_type, merge_helper_))) {
    LOG_WARN("failed to get merge helper", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObDDLSlice::ObDDLSlice()
  : is_inited_(false), has_end_chunk_(false), slice_idx_(-1)
{

}

ObDDLSlice::~ObDDLSlice()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    void *tmp = nullptr;
    if (OB_FAIL(chunk_queue_.pop(tmp))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("pop chunk failed", K(ret));
      }
    } else {
      ObChunk *tmp_chunk = (ObChunk *)tmp;
      if (OB_NOT_NULL(tmp_chunk)) {
        tmp_chunk->~ObChunk();
        ob_free(tmp_chunk);
      }
    }
  }
  for (int64_t i = 0; i < remain_cg_blocks_.count(); ++i) {
    ObCGBlockFile *cg_block_file = remain_cg_blocks_.at(i).block_file_;
    if (nullptr != cg_block_file) {
      cg_block_file->~ObCGBlockFile();
      ob_free(cg_block_file);
      cg_block_file = nullptr;
    }
  }
  chunk_queue_.destroy();
}

int ObDDLSlice::init(const ObTabletID &tablet_id, const int64_t slice_idx, const int64_t column_group_count)
{
  int ret = OB_SUCCESS;
  const int64_t queue_cap = 100;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0 || column_group_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(slice_idx), K(column_group_count));
  } else if (OB_FAIL(chunk_queue_.init(queue_cap, "DDL_ChunkQueue", MTL_ID()))) {
    LOG_WARN("init chunk queue failed", K(ret));
  } else if (OB_FAIL(remain_cg_blocks_.reserve(column_group_count))) {
    LOG_WARN("reserve remain cg blocks failed", K(ret), K(column_group_count));
  } else {
    // push back empty remain block
    ObRemainCgBlock remain_block;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_group_count; ++i) {
      if (OB_FAIL(remain_cg_blocks_.push_back(remain_block))) {
        LOG_WARN("push back remain block failed", K(ret), K(i), K(remain_block));
      }
    }
    if (OB_SUCC(ret)) {
      tablet_id_ = tablet_id;
      slice_idx_ = slice_idx;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDDLSlice::push_chunk(ObChunk *&chunk_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(chunk_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(chunk_data));
  } else {
    const bool is_end_chunk = chunk_data->is_end_chunk();
    const int64_t DEFAULT_TIMEOUT_US = 5LL * 1000 * 1000; // 5s
    if (OB_FAIL(chunk_queue_.push(chunk_data, DEFAULT_TIMEOUT_US))) {
      if (OB_UNLIKELY(OB_TIMEOUT != ret)) {
        LOG_WARN("push chunk data failed", K(ret), KPC(chunk_data));
      } else {
        ret = OB_EAGAIN;
      }
    } else {
      chunk_data = nullptr;
      has_end_chunk_ = is_end_chunk;
    }
  }
  return ret;
}

int ObDDLSlice::pop_chunk(ObChunk *&chunk_data)
{
  int ret = OB_SUCCESS;
  void *tmp = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(chunk_queue_.pop(tmp))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("pop chunk data failed", K(ret));
    }
  } else {
    chunk_data = (ObChunk *)tmp;
  }
  return ret;
}

int ObDDLSlice::set_remain_block(const int64_t cg_idx, ObCGBlockFile *block_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= remain_cg_blocks_.count() || nullptr == block_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_idx), K(remain_cg_blocks_.count()), KP(block_file));
  } else {
    remain_cg_blocks_.at(cg_idx).block_file_ = block_file;
  }
  return ret;
}

int ObDDLSlice::set_block_flushed(const int64_t cg_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= remain_cg_blocks_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_idx), K(remain_cg_blocks_.count()));
  } else {
    remain_cg_blocks_.at(cg_idx).has_flushed_macro_block_ = true;
  }
  return ret;
}

int ObDDLSlice::get_remain_block(const int64_t cg_idx, ObRemainCgBlock &remain_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= remain_cg_blocks_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_idx), K(remain_cg_blocks_.count()));
  } else {
    remain_block = remain_cg_blocks_.at(cg_idx);
    remain_cg_blocks_.at(cg_idx).block_file_ = nullptr;
  }
  return ret;
}

ObDDLTabletContext::ObDDLTabletContext()
  : is_inited_(false), arena_(ObMemAttr(MTL_ID(), "ddl_tblt_ctx")),
    slice_count_(0), table_slice_offset_(0), scan_task_(nullptr),
    last_lob_id_(0), last_autoinc_val_(0), bucket_count_(0),
    macro_meta_store_mgr_(nullptr), vector_index_ctx_(nullptr)
{

}

ObDDLTabletContext::~ObDDLTabletContext()
{
  reset();
}

int init_tablet_param(ObTablet *tablet, ObStorageSchema *storage_schema, const ObDirectLoadType direct_load_type, ObIAllocator &allocator, ObWriteTabletParam &tablet_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == tablet || nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(tablet), KP(storage_schema));
  } else {
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
    tablet_param.tablet_transfer_seq_ = tablet_meta.transfer_info_.transfer_seq_;
    tablet_param.is_micro_index_clustered_ = tablet_meta.micro_index_clustered_;
    tablet_param.reorganization_scn_ = tablet->get_reorganization_scn();
    tablet_param.storage_schema_ = storage_schema;
    if (is_incremental_minor_direct_load(direct_load_type)) {
      // do nothing
    } else if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*try_create]*/))) {
      LOG_WARN("failed to create ddl kv mgr", K(ret));
    }
  }
  return ret;
}

int ObDDLTabletContext::init(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t ddl_thread_count,
    const int64_t snapshot_version,
    const ObDirectLoadType direct_load_type,
    const ObDDLTableSchema &ddl_table_schema,
    const int64_t ddl_task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || ddl_thread_count <= 0 ||
                         !is_valid_direct_load(direct_load_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(ret), K(ls_id), K(tablet_id), K(ddl_thread_count),
             K(direct_load_type));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    bucket_count_ = ddl_thread_count * 2;
    if (GCTX.is_shared_storage_mode() && OB_ISNULL(macro_meta_store_mgr_ = OB_NEW(ObMacroMetaStoreManager, ObMemAttr(MTL_ID(), "mb_meta_mgr")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for macro meta manager failed", K(ret));
    } else if (OB_FAIL(slice_map_.create(bucket_count_, ObMemAttr(MTL_ID(), "tblt_slice_map")))) {
      LOG_WARN("create slice map failed", K(ret), K(bucket_count_));
    } else if (OB_FAIL(bucket_lock_.init(bucket_count_))) {
      LOG_WARN("init bucket lock failed", K(ret), K(bucket_count_));
    } else {
      ObLSHandle ls_handle;
      ObTabletHandle tablet_handle;
      ObTabletBindingMdsUserData mds_data;
      if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("ddl get tablet failed", K(ret), K(ls_handle), K(tablet_id));
      } else if (OB_FAIL(init_tablet_param(tablet_handle.get_obj(), ddl_table_schema.storage_schema_, direct_load_type, arena_, tablet_param_))) {
        LOG_WARN("init tablet param failed", K(ret));
      } else if (OB_FAIL(merge_ctx_.init(direct_load_type))) {
        LOG_WARN("failed to init merge ctx", K(ret));
      } else if (is_incremental_major_direct_load(direct_load_type)) {
        if (!tablet_param_.storage_schema_->is_row_store() || !tablet_param_.storage_schema_->is_user_data_table()) {
          // do nothing
        } else if (OB_FAIL(ls_handle.get_ls()->check_has_cs_replica(tablet_param_.with_cs_replica_))) {
          LOG_WARN("failed to check ls has cs replica", K(ret), KPC(this));
        }
      } else if (OB_FAIL(ObCSReplicaUtil::check_need_process_for_cs_replica_for_ddl(*tablet_handle.get_obj(), // only check for data tablet
              *tablet_param_.storage_schema_, tablet_param_.with_cs_replica_))) {
        LOG_WARN("check need process cs replica failed", K(ret), KPC(this));
      }

      if (OB_FAIL(ret)) {
      } else if (tablet_param_.with_cs_replica_
              && OB_FAIL(ObStorageSchemaUtil::alloc_cs_replica_storage_schema(arena_,
                                                                              tablet_param_.storage_schema_,
                                                                              tablet_param_.cs_replica_storage_schema_))) {
        LOG_WARN("fail to alloc cs replica storage schema", K(ret), KPC(this));
      } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), mds_data))) {
        LOG_WARN("failed to get ddl data from tablet", K(ret), K(tablet_id));
      } else if (mds_data.lob_meta_tablet_id_.is_valid()) {
        lob_meta_tablet_id_ = mds_data.lob_meta_tablet_id_;
        ObTabletHandle lob_meta_tablet_handle;
        if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, lob_meta_tablet_id_, lob_meta_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
          LOG_WARN("ddl get tablet failed", K(ret), K(ls_handle), KPC(this));
        } else if (OB_FAIL(init_tablet_param(lob_meta_tablet_handle.get_obj(), ddl_table_schema.lob_meta_storage_schema_, direct_load_type, arena_, lob_meta_tablet_param_))) {
          LOG_WARN("init lob meta tablet param failed", K(ret));
        } else if (OB_FAIL(lob_merge_ctx_.init(direct_load_type))) {
          LOG_WARN("failed to init merge ctx", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_vector_index_context(snapshot_version, ddl_task_id, ddl_table_schema))) {
        LOG_WARN("init vector index context failed", K(ret));
      } else {
        is_inited_ = true;
        LOG_INFO("[CS-Replica] init tablet context", K(tablet_id), K(direct_load_type), K(tablet_param_));
      }
    }
  }
  return ret;
}

int ObDDLTabletContext::init_vector_index_context(const int64_t snapshot_version, const int64_t ddl_task_id, const ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (ddl_table_schema.table_item_.vec_dim_ > 0) {
    if (OB_ISNULL(buf = arena_.alloc(sizeof(ObVectorIndexTabletContext)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      vector_index_ctx_ = new (buf) ObVectorIndexTabletContext();
      if (OB_FAIL(vector_index_ctx_->init(ls_id_, tablet_id_, tablet_param_.storage_schema_->get_index_type(), snapshot_version, ddl_task_id, ddl_table_schema))) {
        LOG_WARN("init vector index ctx failed", K(ret));
      }
    }
  }
  return ret;
}

void ObDDLTabletContext::reset()
{
  is_inited_ = false;
  ls_id_.reset();
  tablet_id_.reset();
  if (nullptr != tablet_param_.cs_replica_storage_schema_) {
    ObTabletObjLoadHelper::free(arena_, tablet_param_.cs_replica_storage_schema_);
  }
  tablet_param_.reset();
  lob_meta_tablet_id_.reset();
  lob_meta_tablet_param_.reset();
  slice_count_ = 0;
  table_slice_offset_ = 0;
  scan_task_ = nullptr;
  last_lob_id_ = 0;
  last_autoinc_val_ = 0;
  bucket_lock_.destroy();
  bucket_count_ = 0;
  SLICE_MAP::iterator slice_iter = slice_map_.begin();
  for (; slice_iter != slice_map_.end(); ++slice_iter) {
    ObDDLSlice *ddl_slice = slice_iter->second;
    if (OB_NOT_NULL(ddl_slice)) {
      ddl_slice->~ObDDLSlice();
      ob_free(ddl_slice);
    }
  }
  slice_map_.destroy();

  if (OB_NOT_NULL(macro_meta_store_mgr_)) {
    macro_meta_store_mgr_->~ObMacroMetaStoreManager();
    ob_free(macro_meta_store_mgr_);
    macro_meta_store_mgr_ = nullptr;
  }
  if (nullptr != vector_index_ctx_) {
    vector_index_ctx_->~ObVectorIndexTabletContext();
    arena_.free(vector_index_ctx_);
    vector_index_ctx_ = nullptr;
  }
  arena_.reset();
}

int ObDDLTabletContext::update_max_lob_id(const int64_t lob_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  last_lob_id_ = max(last_lob_id_, lob_id);
  return ret;
}

int ObDDLTabletContext::update_max_autoinc_val(const int64_t val)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  last_autoinc_val_ = max(last_autoinc_val_, val);
  return ret;
}

int ObDDLTabletContext::get_or_create_slice(const int64_t slice_idx, ObDDLSlice *&ddl_slice, bool &is_new_slice)
{
  int ret = OB_SUCCESS;
  ddl_slice = nullptr;
  is_new_slice = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_idx));
  } else {
    ObBucketRLockGuard guard(bucket_lock_, slice_idx % bucket_count_);
    if (OB_FAIL(slice_map_.get_refactored(slice_idx, ddl_slice))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get slice failed", K(ret));
      }
    } else {
      is_new_slice = false;
    }
  }
  if (OB_HASH_NOT_EXIST == ret) {
    ObBucketWLockGuard guard(bucket_lock_, slice_idx % bucket_count_);
    ret = slice_map_.get_refactored(slice_idx, ddl_slice);
    if (OB_SUCCESS == ret) {
      is_new_slice = false;
    } else if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get slice failed", K(ret));
    } else {
      ObStorageSchema *storage_schema = tablet_param_.with_cs_replica_ ?
                                        tablet_param_.cs_replica_storage_schema_ :
                                        tablet_param_.storage_schema_;
      ObDDLSlice *tmp_slice = OB_NEW(ObDDLSlice, ObMemAttr(MTL_ID(), "dag_ddl_slice"));
      if (OB_ISNULL(tmp_slice)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(tmp_slice->init(tablet_id_, slice_idx, storage_schema->get_column_group_count()))) {
        LOG_WARN("init slice failed", K(ret));
      } else if (OB_FAIL(slice_map_.set_refactored(slice_idx, tmp_slice))) {
        LOG_WARN("add slice to map failed", K(ret));
      } else {
        ddl_slice = tmp_slice;
        is_new_slice = true;
      }
      if (OB_FAIL(ret) && nullptr != tmp_slice) {
        tmp_slice->~ObDDLSlice();
        ob_free(tmp_slice);
        tmp_slice = nullptr;
      }
    }
  }
  return ret;
}

int ObDDLTabletContext::remove_slice(const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_idx));
  } else {
    ObDDLSlice *ddl_slice = nullptr;
    ObBucketWLockGuard guard(bucket_lock_, slice_idx % bucket_count_);
    if (OB_FAIL(slice_map_.erase_refactored(slice_idx, &ddl_slice))) {
      LOG_WARN("erase slice failed", K(ret));
    } else if (OB_ISNULL(ddl_slice)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl slice is null", K(ret), KP(ddl_slice));
    } else {
      ddl_slice->~ObDDLSlice();
      ob_free(ddl_slice);
    }
  }
  return ret;
}

int ObDDLTabletContext::get_all_slices(ObIArray<ObDDLSlice *> &ddl_slices)
{
  int ret = OB_SUCCESS;
  ddl_slices.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(ddl_slices.reserve(slice_map_.size()))) {
    LOG_WARN("reserve slice array failed", K(ret));
  } else {
    SLICE_MAP::iterator slice_iter = slice_map_.begin();
    for (; slice_iter != slice_map_.end(); ++slice_iter) {
      ObDDLSlice *ddl_slice = slice_iter->second;
      if (OB_ISNULL(ddl_slice)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl slice is null", K(ret), K(ddl_slice));
      } else if (OB_FAIL(ddl_slices.push_back(ddl_slice))) {
        LOG_WARN("push back ddl slice failed", K(ret));
      }
    }
  }
  return ret;
}
