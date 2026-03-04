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

#include "storage/tablet/ob_sstablet_persister.h"

#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"
#include "storage/incremental/atomic_protocol/ob_atomic_op_handle.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_op.h"
#endif

namespace oceanbase
{
namespace storage
{
// ======================================================
//         ObSSTabletPersister::InnerWriteSumary
// ======================================================
int ObSSTabletPersister::InnerWriteSummary::record(
  const ObSharedObjectWriteInfo &write_info,
  const ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  MacroBlockId tmp_block_id;
  int64_t tmp_offset = 0;
  int64_t tmp_size = 0;

  if (OB_FAIL(!addr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid addr", K(ret), K(addr));
  } else if (OB_FAIL(addr.get_block_addr(tmp_block_id, tmp_offset, tmp_size))) {
    STORAGE_LOG(WARN, "failed to get block addr", K(ret), K(addr));
  } else if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid write info", K(ret), K(write_info));
  } else if (OB_FAIL(written_objects_.push_back(tmp_block_id))) {
    STORAGE_LOG(WARN, "failed to push back block id", K(ret), K(addr));
  } else {
    if (tmp_block_id.is_shared_tablet_sub_meta_in_table()) {
      bytes_in_table_ += write_info.size_;
      ++write_ops_for_table_;
    } else {
      bytes_in_oss_ += upper_align(write_info.size_, DIO_READ_ALIGN_SIZE);
      ++write_ops_for_oss_;
    }
  }
  return ret;
}

// ======================================================
//         ObSSTabletPersister::InnerWriteBuffer
// ======================================================
ObSSTabletPersister::InnerWriteBuffer::~InnerWriteBuffer()
{
  int ret = OB_SUCCESS;
  if (pending_write_cnt() > 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "write buffer still has unsynced write infos", K(ret), K(pending_write_cnt()));
  }
  pending_write_infos_.reset();
  pending_object_opts_.reset();
  block_ids_.reset();
  buf_size_in_bytes_ = 0;
}

int ObSSTabletPersister::InnerWriteBuffer::init(const WriteStrategy write_strategy)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(write_strategy >= WriteStrategy::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid write strategy", K(ret), K(write_strategy));
  } else {
    write_strategy_ = write_strategy;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTabletPersister::InnerWriteBuffer::append_batch(
  const ObSharedObjectWriteInfo &write_info,
  const int64_t macro_seq,
  /* out */ ObMetaDiskAddr &addr)
{
#ifndef OB_BUILD_SHARED_STORAGE
  return OB_NOT_SUPPORTED;
#else
  int ret = OB_SUCCESS;
  addr.reset();
  ObStorageObjectOpt object_opt;
  MacroBlockId block_id;

  if (OB_UNLIKELY(!is_valid_())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid write buffer", K(ret), KPC(this));
  } else if (OB_UNLIKELY(WriteStrategy::BATCH != write_strategy_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected call", K(ret), K(write_strategy_), K(common::lbt()));
  } else if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid write info", K(ret), K(write_info));
  } else if (OB_UNLIKELY(macro_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro seq", K(ret), K(macro_seq));
  } else if (OB_FAIL(get_storage_opt_by_write_info_(param_,
                                                    macro_seq,
                                                    write_info,
                                                    /* out */ object_opt))) {
    STORAGE_LOG(WARN, "failed to get storage opt by write info", K(ret), K(param_),
      K(macro_seq), K(write_info));
  } else if (OB_FAIL(pending_write_infos_.push_back(write_info))) {
    STORAGE_LOG(WARN, "failed to push back write info", K(ret), K(write_info));
  } else if (OB_FAIL(pending_object_opts_.push_back(object_opt))) {
    STORAGE_LOG(WARN, "failed to push back object opt", K(ret), K(object_opt));
  } else if (OB_FAIL(ObObjectManager::ss_get_object_id(object_opt, block_id))) {
    STORAGE_LOG(WARN, "failed to get object id by object opt", K(ret), K(object_opt));
  } else if (OB_FAIL(block_ids_.push_back(block_id))) {
    STORAGE_LOG(WARN, "failed to push back block id", K(ret), K(block_id));
  }
  /// COMMENT: RAW_BLOCK
  else if (OB_FAIL(addr.set_block_addr(block_id,
                                       /*offset*/ 0,
                                       write_info.size_,
                                       ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
    STORAGE_LOG(WARN, "failed to set block addr", K(ret), K(block_id), K(write_info));
  } else if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid phy addr", K(ret), K(addr));
  }
  // record write op
  else if (OB_FAIL(write_summary_.record(write_info, addr))) {
    STORAGE_LOG(WARN, "failed to record write op", K(ret), K(write_info), K(addr),
      K_(write_summary));
  } else {
    buf_size_in_bytes_ += write_info.size_;
    if (buf_size_in_bytes_ >= buffer_size_limit_) {
      STORAGE_LOG(INFO, "write buffer size reach the limit, do sync", K(ret),
        K_(buf_size_in_bytes), K_(buffer_size_limit), "pending_row_cnts",
        pending_write_cnt());
      // reach the size limit, do sync
      if (OB_FAIL(sync_batch())) {
        STORAGE_LOG(WARN, "failed to do sync", K(ret));
      }
    }
  }
  return ret;
#endif
}

int ObSSTabletPersister::InnerWriteBuffer::append(
  const ObIArray<ObSharedObjectWriteInfo> &write_infos,
  /* out */ int64_t &macro_seq,
  /* out */ ObIArray<ObMetaDiskAddr> &addrs)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(write_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected empty write infos", K(ret), K(common::lbt()));
  } else if (OB_UNLIKELY(macro_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro seq", K(ret), K(macro_seq));
  } else {
    switch (write_strategy_) {
      case ObSSTabletPersister::WriteStrategy::BATCH: {
        if (OB_FAIL(append_batch_(write_infos,
                                  /* out */ macro_seq,
                                  /* out */ addrs))) {
          STORAGE_LOG(WARN, "failed to append for user tenant", K(ret));
        }
      } break;
      case ObSSTabletPersister::WriteStrategy::AGGREGATED: {
        if (OB_FAIL(append_and_write_aggregated_(write_infos,
                                                 /* out */ macro_seq,
                                                 /* out */ addrs))) {
          STORAGE_LOG(WARN, "failed to append for non-user tenant", K(ret));
        }
      } break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected write strategy", K(ret), K(write_strategy_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(addrs.count() != write_infos.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "addrs mismatch with write infos", K(ret), K(write_infos.count()),
      K(addrs.count()));
  }
  return ret;
}

int ObSSTabletPersister::InnerWriteBuffer::append_batch_(
  const ObIArray<ObSharedObjectWriteInfo> &write_infos,
  /* out */ int64_t &macro_seq,
  /* out */ ObIArray<ObMetaDiskAddr> &addrs)
{
  OB_ASSERT(!write_infos.empty());
  OB_ASSERT(WriteStrategy::BATCH == write_strategy_);

  int ret = OB_SUCCESS;
  addrs.reset();
  const int64_t total_cnt = write_infos.count();

  if (OB_FAIL(addrs.reserve(total_cnt))) {
    STORAGE_LOG(WARN, "failed to do reserve", K(ret), K(total_cnt));
  } else {
    MacroBlockId block_id;
    ObMetaDiskAddr phy_addr;
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
      block_id.reset();
      phy_addr.reset();
      ObStorageObjectOpt object_opt;
      const ObSharedObjectWriteInfo &write_info = write_infos.at(i);

      if (OB_FAIL(append_batch(write_info, macro_seq, /* out */ phy_addr))) {
        STORAGE_LOG(WARN, "failed to append write info", K(ret), K(write_info),
          K(macro_seq));
      } else if (OB_FAIL(addrs.push_back(phy_addr))) {
        STORAGE_LOG(WARN, "failed to push back addr", K(ret), K(i), K(phy_addr));
      } else {
        // inc macro seq if everything is ok.
        ++macro_seq;
      }
    }
  }
  return ret;
}

int ObSSTabletPersister::InnerWriteBuffer::append_and_write_aggregated_(
  const ObIArray<ObSharedObjectWriteInfo> &write_infos,
  /* out */ int64_t &macro_seq,
  /* out */ ObIArray<ObMetaDiskAddr> &addrs)
{
  OB_ASSERT(!write_infos.empty());
  OB_ASSERT(WriteStrategy::AGGREGATED == write_strategy_);

  int ret = OB_SUCCESS;
  addrs.reset();
  ObTenantStorageMetaService *meta_service = nullptr;
  const int64_t total_cnt = write_infos.count();
  common::ObSEArray<ObSharedObjectsWriteCtx, 16> write_ctxs;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "WriteCtxs", ctx_id));
  ObStorageObjectOpt storage_opt;
  ObSharedObjectBatchHandle handle;

  if (OB_UNLIKELY(pending_write_cnt() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pending write buffer must be empty for non-user tenant", K(ret),
      KPC(this));
  } else if (OB_FAIL(addrs.reserve(total_cnt))) {
    STORAGE_LOG(WARN, "failed to do reserve", K(ret), K(total_cnt));
  } else if (OB_ISNULL(meta_service = MTL(ObTenantStorageMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unepxected null meta_serivce", K(ret), KP(meta_service));
  } else if (OB_FAIL(ObTabletPersistCommon::build_async_write_start_opt(param_,
                                                                        macro_seq,
                                                                        /* out */ storage_opt))) {
    STORAGE_LOG(WARN, "failed to build async write start opt", K(ret), K_(param), K(macro_seq));
  } else if (OB_FAIL(meta_service->get_shared_object_reader_writer().async_batch_write(write_infos,
                                                                                       /* out */ handle,
                                                                                       /* out */ storage_opt))) {
    STORAGE_LOG(WARN, "failed to async batch write", K(ret));
  }
  // update macro seq
  else if (OB_FAIL(ObTabletPersistCommon::sync_cur_macro_seq_from_opt(param_, storage_opt, /* out */ macro_seq))) {
    STORAGE_LOG(WARN, "failed to sync macro seq", K(ret), K_(param), K(storage_opt), K(macro_seq));
  } else if (OB_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
    STORAGE_LOG(WARN, "failed to wait handle", K(ret), K(handle));
  } else if (OB_UNLIKELY(total_cnt != write_ctxs.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "write ctxs mismatch with write infos", K(write_infos.count()),
      K(write_ctxs.count()), K(write_ctxs), K(handle));
  } else if (OB_FAIL(ObTabletPersistCommon::wait_write_info_callback(write_infos))) {
    STORAGE_LOG(WARN, "failed to wait write info callback", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
      const ObSharedObjectWriteInfo &write_info = write_infos.at(i);
      const ObSharedObjectsWriteCtx &write_ctx = write_ctxs.at(i);
      if (OB_UNLIKELY(!write_ctx.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected invalid write ctx", K(ret), K(i), K(write_ctx), K(handle));
      } else if (OB_FAIL(addrs.push_back(write_ctx.addr_))) {
        STORAGE_LOG(WARN, "failed to push back write ctx addr", K(ret), K(write_ctx));
      } else if (OB_FAIL(write_summary_.record(write_info, write_ctx.addr_))) {
        STORAGE_LOG(WARN, "failed to record write op", K(ret), K(write_info), "addr",
          write_ctx.addr_, K_(write_summary));
      }
    }
  }
  return ret;
}

int ObSSTabletPersister::InnerWriteBuffer::sync_batch()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(WriteStrategy::BATCH != write_strategy_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected call", K(ret), K(write_strategy_), K(common::lbt()));
  } else if (0 == pending_write_cnt()) {
    // nothing to do
  } else if (OB_FAIL(inner_sync_batch_())) {
    STORAGE_LOG(WARN, "failed to inner do sync", K(ret));
  }
  return ret;
}

int ObSSTabletPersister::InnerWriteBuffer::inner_sync_batch_()
{
#ifndef OB_BUILD_SHARED_STORAGE
  return OB_NOT_SUPPORTED;
#else
  int ret = OB_SUCCESS;
  const int64_t ctx_id = share::is_reserve_mode()
                      ? ObCtxIds::MERGE_RESERVE_CTX_ID
                      : ObCtxIds::DEFAULT_CTX_ID;
  ObSEArray<ObStorageObjectWriteInfo, 16> storage_write_infos;
  storage_write_infos.set_attr(lib::ObMemAttr(MTL_ID(), "StorWriteInfos", ctx_id));
  const int64_t total_cnt = pending_write_cnt();

  if (OB_UNLIKELY(!is_valid_())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid write buffer", K(ret), KPC(this));
  } else if (OB_UNLIKELY(0 == total_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pending write infos is empty", K(ret), K(total_cnt));
  } else if (OB_FAIL(storage_write_infos.reserve(total_cnt))) {
    STORAGE_LOG(WARN, "failed to do reserve", K(ret), K(total_cnt));
  } else {
    ObTieredMetadataBatchHandle batch_write_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
      const ObSharedObjectWriteInfo &write_info = pending_write_infos_.at(i);
      ObStorageObjectWriteInfo storage_write_info;

      if (OB_FAIL(convert_write_info_(write_info, storage_write_info))) {
        STORAGE_LOG(WARN, "failed to convert write info", K(ret), K(write_info));
      } else if (OB_FAIL(storage_write_infos.push_back(storage_write_info))) {
        STORAGE_LOG(WARN, "failed to push back storage write info", K(ret), K(storage_write_info));
      }
    }
    if (FAILEDx(ObTieredMetadataObjectManager::async_batch_write_object(pending_object_opts_,
                                                                        storage_write_infos,
                                                                        /* out */ batch_write_handle))) {
      STORAGE_LOG(WARN, "failed to do async batch write", K(ret), K(pending_object_opts_.count()), K(storage_write_infos.count()));
    }
    /// NOTE: macro_id_array of @c batch_write_handle is inited after async write is called.
    else if (OB_UNLIKELY(batch_write_handle.macro_id_array().count() != total_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected batch write handle", K(ret), K(batch_write_handle));
    } else if (FAILEDx(trigger_callback_if_need_(batch_write_handle.macro_id_array()))) {
      STORAGE_LOG(WARN, "failed to do callback", K(ret));
    } else {
      OB_ASSERT(block_ids_.count() == total_cnt);
      // check block id
      const ObIArray<MacroBlockId> &written_blocks = batch_write_handle.macro_id_array();
      for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
        const MacroBlockId &block_id = block_ids_.at(i);
        const MacroBlockId &written_block_id = written_blocks.at(i);
        if (OB_UNLIKELY(!block_id.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected invalid block id", K(ret), K(block_id));
        } else if (OB_UNLIKELY(block_id != written_block_id)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "block id mismatch", K(ret), K(block_id), K(written_block_id));
        }
      }
      // wait handle and callback finished(if any callback was provided)
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(batch_write_handle.wait())) {
        STORAGE_LOG(WARN, "failed to wait batch write handle", K(tmp_ret), K(batch_write_handle));
        ret = COVER_SUCC(tmp_ret);
      }
      tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObTabletPersistCommon::wait_write_info_callback(pending_write_infos_))) {
        STORAGE_LOG(WARN, "failed to wait write info callback", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  // clean pending buffers
  if (FAILEDx(reuse_buffer_())) {
    STORAGE_LOG(WARN, "failed to reuse buffer", K(ret), K(pending_write_cnt()));
  }
  return ret;
#endif
}

/*static*/ int ObSSTabletPersister::InnerWriteBuffer::get_storage_opt_by_write_info_(
    const ObTabletPersisterParam &param,
    const int64_t macro_seq,
    const ObSharedObjectWriteInfo &write_info,
    /*out*/ ObStorageObjectOpt &object_opt)
{
#ifndef OB_BUILD_SHARED_STORAGE
  return  OB_NOT_SUPPORTED;
#else
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!param.is_valid()
                  || !param.for_ss_persist())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid persister param", K(ret), K(param));
  } else if (OB_UNLIKELY(macro_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro seq", K(ret), K(macro_seq));
  } else if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid write info",K(ret), K(write_info));
  } else {
    const bool write_to_oss = !ObSSTabletPersister::write_to_meta_table_(write_info.size_);
    int64_t op_id = -1;

    if (write_to_oss && (write_info.size_ % DIO_READ_ALIGN_SIZE) != 0) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "serialize size of meta member written to object storage"
        " must be aligned with 4K", K(ret), K(write_info));
    } else if (OB_FAIL(param.get_op_id(op_id))) {
      STORAGE_LOG(WARN, "failed to get op id from param", K(ret), K(param));
    } else {
      object_opt.set_ss_tablet_sub_meta_opt(param.ls_id_.id(),
                                            param.tablet_id_.id(),
                                            (uint32_t)op_id,
                                            (uint32_t)macro_seq,
                                            param.tablet_id_.is_inner_tablet(),
                                            param.reorganization_scn_,
                                            write_to_oss /* is_object_storage */);
    }
  }
  return ret;
#endif
}


/// NOTE: see @interface ObSharedObjectReaderWriter::inner_write_block
/*static*/ int ObSSTabletPersister::InnerWriteBuffer::convert_write_info_(
    const ObSharedObjectWriteInfo &write_info,
    /*out*/ ObStorageObjectWriteInfo &storage_write_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid write info", K(ret), K(write_info));
  } else {
    ObStorageObjectWriteInfo tmp_storage_write_info;
    tmp_storage_write_info.buffer_ = write_info.buffer_;
    tmp_storage_write_info.offset_ = 0;
    tmp_storage_write_info.size_ = write_info.size_;
    tmp_storage_write_info.mtl_tenant_id_ = MTL_ID();
    tmp_storage_write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    tmp_storage_write_info.io_desc_.set_unsealed();
    tmp_storage_write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    tmp_storage_write_info.set_ls_epoch_id(write_info.ls_epoch_);

    storage_write_info = tmp_storage_write_info;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!storage_write_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid storage write info", K(ret),
      K(write_info), K(storage_write_info));
  }
  return ret;
}

int ObSSTabletPersister::InnerWriteBuffer::reuse_buffer_()
{
  OB_ASSERT(WriteStrategy::BATCH == write_strategy_);

  int ret = OB_SUCCESS;
  // reclaim memory token by write buffer
  for (int64_t i = 0; OB_SUCC(ret) && i < pending_write_infos_.count(); ++i) {
    ObSharedObjectWriteInfo &write_info = pending_write_infos_.at(i);
    if (OB_ISNULL(write_info.buffer_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null write info", K(ret), K(write_info));
    } else {
      allocator_.free(const_cast<char *>(write_info.buffer_));
      write_info.reset();
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    pending_write_infos_.reuse();
    pending_object_opts_.reuse();
    block_ids_.reuse();
    buf_size_in_bytes_ = 0;
  }
  return ret;
}

int ObSSTabletPersister::InnerWriteBuffer::trigger_callback_if_need_(const ObIArray<MacroBlockId> &written_blocks)
{
  int ret = OB_SUCCESS;
  const int64_t total_cnt = pending_write_cnt();
  OB_ASSERT(total_cnt == written_blocks.count());

  if (OB_UNLIKELY(!is_valid_())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid write buffer", K(ret), KPC(this));
  } else {
    /// NOTE: see @interface ObSharedObjectReaderWriter::inner_write_block
    const ObLogicMacroBlockId unused_logic_id;

    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
      ObStorageObjectHandle handle_for_callback;
      const ObSharedObjectWriteInfo &write_info = pending_write_infos_.at(i);
      const MacroBlockId &block_id = written_blocks.at(i);

      if (OB_UNLIKELY(!write_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected invalid write info", K(ret), K(write_info));
      } else if (OB_FAIL(handle_for_callback.set_macro_block_id(block_id))) {
        STORAGE_LOG(WARN, "failed to set handle block id", K(ret), K(block_id));
      } else if (OB_ISNULL(write_info.write_callback_)) {
        // do nothing
      } else if (OB_FAIL(write_info.write_callback_->write(handle_for_callback,
                                                           unused_logic_id,
                                                           const_cast<char *>(write_info.buffer_),
                                                           write_info.size_,
                                                           /*row_count, unused*/ 0))) {
        STORAGE_LOG(WARN, "failed to write call back", K(ret), K(write_info), K(handle_for_callback));
      }
    }
  }
  return ret;
}

// ======================================================
//        ObSSTabletPersister::SSTableMetaWriteOp
// ======================================================
int ObSSTabletPersister::SSTableMetaWriteOp::do_write(const ObIArray<ObSharedObjectWriteInfo> &write_infos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObSSTableMetaPersistHelper::IWriteOperator::do_write(write_infos))) {
    STORAGE_LOG(WARN, "failed to do write", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid write op", K(ret), KPC(this));
  } else if (OB_FAIL(write_buffer_.append(write_infos,
                                          /* out */ macro_seq_,
                                          /* out */ written_addrs_))) {
    STORAGE_LOG(WARN, "failed to append write buffer", K(ret), K(write_infos));
  } else {
    MacroBlockId tmp_block_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < written_addrs_.count(); ++i) {
      tmp_block_id.reset();
      const ObMetaDiskAddr &tmp_meta_disk_addr = written_addrs_.at(i);

      if (OB_FAIL(tmp_meta_disk_addr.get_macro_block_id(tmp_block_id))) {
        STORAGE_LOG(WARN, "failed to get macro block id", K(ret), K(tmp_meta_disk_addr));
      } else if (OB_FAIL(block_info_set_.shared_meta_block_info_set_.set_refactored(tmp_block_id,
                                                                                    /*overwrite*/ 0))) {
        if (OB_HASH_EXIST != ret) {
          STORAGE_LOG(WARN, "failed to add meta addr into block info set", K(ret), K(tmp_meta_disk_addr),
            K(i), K(written_addrs_.count()));
        } else {
          STORAGE_LOG(DEBUG, "duplicate addr", K(ret), K(tmp_meta_disk_addr));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObSSTabletPersister::SSTableMetaWriteOp::fill_write_info(
    const ObTabletPersisterParam &param,
    const ObSSTable &sstable,
    common::ObIAllocator &allocator,
    /* out */ ObSharedObjectWriteInfo &write_info) const
{
  int ret = OB_SUCCESS;
  write_info.reset();

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid persister param", K(ret), K(param));
  } else {
    ObTabletPersistCommon::SSTablePersistWrapper wrapper(param.data_version_, &sstable);

    if (OB_FAIL(ObSSTabletPersister::fill_write_info_(write_buffer_.write_strategy(),
                                                      param,
                                                      &wrapper,
                                                      allocator,
                                                      write_info))) {
      STORAGE_LOG(WARN, "failed to fill sstable write info", K(ret), K(param), K(sstable),
        K(write_info));
    }
  }
  return ret;
}

bool ObSSTabletPersister::SSTableMetaWriteOp::is_valid() const
{
  return persist_param_.is_valid()
         && persist_param_.for_ss_persist()
         && macro_seq_ >= 0;
}

// ======================================================
//         ObSSTabletPersister::SubMetaWriteResult
// ======================================================
int ObSSTabletPersister::SubMetaWriteResult::get_table_store_meta_info(
    const bool is_ls_tx_data_tablet,
    /*out*/ ObSSTabletTableStoreMetaInfo &table_store_meta_info) const
{
#ifndef OB_BUILD_SHARED_STORAGE
  return OB_NOT_SUPPORTED;
#else
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_store_meta_info.is_inited())) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "table_store_meta_info init twice", K(ret), K(table_store_meta_info));
  } else if (is_empty_shell_) {
    table_store_meta_info.set_none();
  } else if (OB_UNLIKELY(!new_table_store_.is_valid()))  {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid table store", K(ret), K(new_table_store_));
  } else if (OB_FAIL(table_store_meta_info.init_by_table_store(new_table_store_, is_ls_tx_data_tablet))) {
    STORAGE_LOG(WARN, "failed to init table_store_meta_info", K(ret), K(new_table_store_), K(is_ls_tx_data_tablet));
  }
  return ret;
#endif
}

// ======================================================
//                  ObSSTabletPersister
// ======================================================
/*static*/ int ObSSTabletPersister::decide_write_strategy_(/* out */ WriteStrategy &strategy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  uint64_t data_version = 0;
  strategy = WriteStrategy::MAX;

  if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), data_version))) {
    STORAGE_LOG(WARN, "failed to get meta tenant data version, write to meta table is not allowed", K(ret));
  } else if (data_version >= DATA_VERSION_4_5_1_0) {
    strategy = WriteStrategy::BATCH;
  } else {
    strategy = WriteStrategy::AGGREGATED;
  }
  return ret;
}

/*static*/ int ObSSTabletPersister::persist_tablet(
    const ObTabletPersisterParam &param,
    const ObTablet &tablet)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid persist param", K(ret), K(param));
  } else if (OB_UNLIKELY(!param.is_inc_shared_object())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "only supports inc shared tablet persistence", K(ret), K(param));
  } else if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tablet", K(ret), K(tablet));
  } else {
    const int64_t ctx_id = share::is_reserve_mode()
                          ? ObCtxIds::MERGE_RESERVE_CTX_ID
                          : ObCtxIds::DEFAULT_CTX_ID;
    ObSSTabletPersister sstablet_persister(param, ctx_id);

    if (OB_FAIL(sstablet_persister.init_())) {
      STORAGE_LOG(WARN, "failed to init sstablet persister", K(ret), K(param));
    } else if (OB_FAIL(sstablet_persister.inner_persist_tablet_(tablet))) {
      STORAGE_LOG(WARN, "failed to persist sstablet", K(ret), K(param), K(tablet));
    }
  }
  return ret;
}

/// NOTE: Determine where to write
/// mocked by mittest@test_inc_shared_storage_macro_block_check.cpp
/*static*/ bool __attribute__((weak)) ObSSTabletPersister::write_to_meta_table_(const int64_t meta_serialize_size)
{
  return is_user_tenant(MTL_ID()) && meta_serialize_size <= OBJECT_LIMIT;
}

ObSSTabletPersister::ObSSTabletPersister(
    const ObTabletPersisterParam &param,
    const int64_t mem_ctx_id)
    : is_inited_(false),
      write_strategy_(WriteStrategy::MAX),
      param_(param),
      allocator_("SSTblPersist", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), mem_ctx_id),
      block_info_set_(),
      space_usage_(),
      cur_macro_seq_(param.start_macro_seq_),
      preallocated_seq_range_(param.start_macro_seq_, param.start_macro_seq_),
      write_buffer_(param,
                    lib::ObMemAttr(MTL_ID(), "WriteBuf", mem_ctx_id),
                    WRITE_BUFFER_SIZE_LIMIT,
                    allocator_),
      multi_stats_(&allocator_, 0/*always print when destructing*/)
{
  linked_block_write_ctxs_.set_attr(lib::ObMemAttr(MTL_ID(), "HistWriteCtx", mem_ctx_id));
}

int ObSSTabletPersister::init_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "inited twice", K(ret), KPC(this));
  } else if (OB_FAIL(block_info_set_.init())) {
    STORAGE_LOG(WARN, "failed to init block info set", K(ret));
  } else if (OB_FAIL(decide_write_strategy_(/* out */ write_strategy_))) {
    STORAGE_LOG(WARN, "failed to decide write strategy", K(ret));
  } else if (OB_FAIL(write_buffer_.init(write_strategy_))) {
    STORAGE_LOG(WARN, "failed to init write buffer", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

bool ObSSTabletPersister::is_ready_for_persist_() const
{
  return is_inited_
         && write_strategy_ < WriteStrategy::MAX
         && param_.is_valid()
         && param_.for_ss_persist();
}

int ObSSTabletPersister::inner_persist_tablet_(const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  const bool is_empty_shell = tablet.is_empty_shell();
  SubMetaWriteResult sub_meta_write_res(is_empty_shell);
  ObTabletMacroInfo tablet_macro_info;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;

  if (OB_UNLIKELY(!is_ready_for_persist_())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstablet persist is not ready", K(ret), KPC(this));
  } else if (OB_FAIL(multi_stats_.acquire_stats("persist_sstablet_meta", time_stats))) {
    STORAGE_LOG(WARN, "failed to acquire time stats", K(ret), KP(time_stats));
  } else if (is_empty_shell) {
    // empty shell has no sub meta to persist, skip
  } else {
    switch (write_strategy_) {
      case WriteStrategy::BATCH: {
        if (OB_FAIL(inner_persist_sub_tablet_meta_batch_(tablet,
                                                         /* ref */ *time_stats,
                                                         /* out */ sub_meta_write_res))) {
          STORAGE_LOG(WARN, "failed to persist sub tablet meta", K(ret), K(tablet), K(sub_meta_write_res));
        }
      } break;
      case WriteStrategy::AGGREGATED: {
        if (OB_FAIL(inner_persist_sub_tablet_meta_aggregated_(tablet,
                                                              /* ref */ *time_stats,
                                                              /* out */ sub_meta_write_res))) {
          STORAGE_LOG(WARN, "failed to persist sub tablet meta", K(ret), K(tablet), K(sub_meta_write_res));
        }
      } break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unepxected write strategy", K(ret), K(write_strategy_));
      }
    }

    // sanity check after sub tablet meta persistence finished
    if (FAILEDx(sanity_check_(sub_meta_write_res))) {
      STORAGE_LOG(WARN, "failed to do sanity check", K(ret));
    }
  }

  // generate and persist tablet macro info
  ObLinkedMacroBlockItemWriter linked_writer;
  if (FAILEDx(fetch_and_persist_tablet_macro_info_(linked_writer, tablet_macro_info))) {
    STORAGE_LOG(WARN, "failed to fetch and persist tablet macro info", K(ret),
      K(tablet_macro_info));
  } else if (OB_FAIL(check_tablet_macro_info_(tablet_macro_info))) {
    STORAGE_LOG(WARN, "unepected invalid tablet macro info", K(ret));
  }

  // calc new tablet's space usage
  int64_t op_id = -1;
  if (FAILEDx(ObTabletPersistCommon::calc_tablet_space_usage(is_empty_shell,
                                                             block_info_set_,
                                                             sub_meta_write_res.new_table_store_,
                                                             space_usage_))) {
    STORAGE_LOG(WARN, "failed to calc tablet space usage", K(ret), K(is_empty_shell),
      K(sub_meta_write_res.new_table_store_), K(space_usage_));
  }
  // persist aggregated (1st level) tablet meta
  else if (OB_FAIL(inner_persist_aggregated_meta_(tablet,
                                             tablet_macro_info,
                                             sub_meta_write_res,
                                             *time_stats))) {
    STORAGE_LOG(WARN, "failed to persist aggregated tablet meta", K(ret), K(tablet),
     K(tablet_macro_info), K(sub_meta_write_res));
  } else if (OB_FAIL(param_.get_op_id(op_id))) {
    STORAGE_LOG(WARN, "failed to get op id from param", K(ret), K(param_));
  } else if (OB_FAIL(time_stats->set_extra_info("ls_id:%ld,tablet_id:%ld,op_id:%ld,write_strategy:%d",
                                                param_.ls_id_.id(),
                                                param_.tablet_id_.id(),
                                                op_id,
                                                write_strategy_))) {
    STORAGE_LOG(WARN, "failed to set time stats extra info", K(ret));
  }
  return ret;
}

int ObSSTabletPersister::inner_persist_sub_tablet_meta_batch_(
    const ObTablet &tablet,
    ObMultiTimeStats::TimeStats &time_stats,
    /*out*/ SubMetaWriteResult &sub_meta_write_res)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  /// NOTE: preallocate macro seq for tablet table store
  preallocate_macro_seqs_(1);
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;

  common::ObSEArray<ObMetaDiskAddr, 2> phy_addrs;
  phy_addrs.set_attr(lib::ObMemAttr(MTL_ID(), "SubMetaAddrs", ctx_id));

  if (OB_UNLIKELY(tablet.is_empty_shell()))  {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not supported for empty shell tablet", K(ret), K((tablet)));
  }

  /*--step.1: persist new table store--*/
  if (OB_SUCC(ret)) {
    ObSharedObjectWriteInfo table_store_write_info;
    int64_t table_store_macro_seq = 0;
    if (OB_FAIL(get_preallocated_macro_seq_(/* out */ table_store_macro_seq))) {
      STORAGE_LOG(WARN, "failed to get preallocated macro seq", K(ret));
    } else if (OB_FAIL(fetch_table_store_and_write_info_(tablet,
                                                         /* out */ table_store_write_info,
                                                         /* out */ sub_meta_write_res.new_table_store_))) {
      STORAGE_LOG(WARN, "failed to fetch table store write info", K(ret), K(tablet));
    } else if (OB_FAIL(write_buffer_.append_batch(table_store_write_info,
                                                  table_store_macro_seq,
                                                  /* out */ sub_meta_write_res.table_store_addr_))) {
      STORAGE_LOG(WARN, "failed to append table store write info", K(ret), K(table_store_write_info),
        K(table_store_macro_seq));
    } else if (OB_FAIL(phy_addrs.push_back(sub_meta_write_res.table_store_addr_)))  {
      STORAGE_LOG(WARN, "failed to push back table store addr", K(ret), K(sub_meta_write_res.table_store_addr_));
    } else {
      time_stats.click("persist_table_store");
    }
  }
  /*--step.2: persist storage schema--*/
  if (OB_SUCC(ret)) {
    ObSharedObjectWriteInfo storage_schema_write_info;
    if (OB_FAIL(fetch_storage_schema_and_write_info_(tablet, /* out */ storage_schema_write_info))) {
      STORAGE_LOG(WARN, "failed to fetch storage schema write info", K(ret), K(tablet));
    } else if (OB_FAIL(write_buffer_.append_batch(storage_schema_write_info,
                                                  cur_macro_seq_,
                                                  /* out */ sub_meta_write_res.storage_schema_addr_))) {
      STORAGE_LOG(WARN, "failed to append storage schema write info", K(ret), K(storage_schema_write_info),
        K(cur_macro_seq_));
    } else if (OB_FAIL(phy_addrs.push_back(sub_meta_write_res.storage_schema_addr_))) {
      STORAGE_LOG(WARN, "failed to push back storage schema addr", K(ret), K(sub_meta_write_res.storage_schema_addr_));
    } else {
      // update macro seq
      ++cur_macro_seq_;
      time_stats.click("persist_storage_schema");
    }
  }

  /*--step.3: update block info set and space usage--*/
  if (OB_SUCC(ret)) {
    OB_ASSERT(2 == phy_addrs.count());
    MacroBlockId tmp_block_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_addrs.count(); ++i) {
      tmp_block_id.reset();
      const ObMetaDiskAddr &phy_addr = phy_addrs.at(i);

      // build block info set
      if (OB_FAIL(phy_addr.get_macro_block_id(tmp_block_id))) {
        STORAGE_LOG(WARN, "failed to get macro block id", K(ret), K(phy_addr));
      } else if (OB_FAIL(block_info_set_.shared_meta_block_info_set_.set_refactored(tmp_block_id,
                                                                                    /*overwrite*/ 0))) {
        if (OB_HASH_EXIST != ret) {
          STORAGE_LOG(WARN, "failed to update block info set", K(ret), K(sub_meta_write_res.storage_schema_addr_));
        } else {
          STORAGE_LOG(DEBUG, "duplicate addr", K(ret), K(sub_meta_write_res.storage_schema_addr_));
          ret = OB_SUCCESS;
        }
      }
      /// NOTE: update tablet space usage(aligned by 4KiB)
      int64_t size = 0;
      if (FAILEDx(phy_addr.get_size_for_tablet_space_usage(size))) {
        STORAGE_LOG(WARN, "failed to get size for tablet space usage", K(ret), K(phy_addr));
      } else {
        space_usage_.tablet_clustered_meta_size_ += upper_align(size, DIO_READ_ALIGN_SIZE);
      }
    }
  }

  // check sub meta write result
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!sub_meta_write_res.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid sub meta write result", K(ret), K(sub_meta_write_res));
  } else if (OB_FAIL(write_buffer_.sync_batch())) {
    STORAGE_LOG(WARN, "failed to sync write buffer", K(ret), K(write_buffer_));
  } else {
    time_stats.click("sync_write_buffer");
  }
  return ret;
}

int ObSSTabletPersister::inner_persist_sub_tablet_meta_aggregated_(
    const ObTablet &tablet,
    ObMultiTimeStats::TimeStats &time_stats,
    /*out*/ SubMetaWriteResult &sub_meta_write_res)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  common::ObSEArray<ObSharedObjectWriteInfo, 2> write_infos;
  common::ObSEArray<ObMetaDiskAddr, 2> phy_addrs;
  common::ObSEArray<ObMetaDiskAddr *, 2> addr_ptrs;

  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  write_infos.set_attr(lib::ObMemAttr(MTL_ID(), "WriteInfos", ctx_id));
  phy_addrs.set_attr(lib::ObMemAttr(MTL_ID(), "PhyAddrs", ctx_id));
  addr_ptrs.set_attr(lib::ObMemAttr(MTL_ID(), "AddrPtrs", ctx_id));

  if (OB_UNLIKELY(tablet.is_empty_shell()))  {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not supported for empty shell tablet", K(ret), K((tablet)));
  }

  /*--step.1: persist new table store--*/
  if (OB_SUCC(ret)) {
    ObSharedObjectWriteInfo table_store_write_info;
    if (OB_FAIL(fetch_table_store_and_write_info_(tablet,
                                                  table_store_write_info,
                                                  sub_meta_write_res.new_table_store_))) {
      STORAGE_LOG(WARN, "failed to fetch table store write info", K(ret), K(tablet));
    } else if (OB_FAIL(write_infos.push_back(table_store_write_info))) {
      STORAGE_LOG(WARN, "failed to push write info", K(ret), K(table_store_write_info));
    } else if (OB_FAIL(addr_ptrs.push_back(&sub_meta_write_res.table_store_addr_))) {
      STORAGE_LOG(WARN, "failed to push addr ptr", K(ret));
    } else {
      time_stats.click("persist_table_store");
    }
  }
  /*--step.2: persist storage schema--*/
  if (OB_SUCC(ret)) {
    ObSharedObjectWriteInfo storage_schema_write_info;
    if (OB_FAIL(fetch_storage_schema_and_write_info_(tablet, storage_schema_write_info))) {
      STORAGE_LOG(WARN, "failed to fetch storage schema write info", K(ret), K(tablet));
    } else if (OB_FAIL(write_infos.push_back(storage_schema_write_info))) {
      STORAGE_LOG(WARN, "failed to push write info", K(ret), K(storage_schema_write_info));
    } else if (OB_FAIL(addr_ptrs.push_back(&sub_meta_write_res.storage_schema_addr_))) {
      STORAGE_LOG(WARN, "failed to push addr ptr", K(ret));
    } else {
      time_stats.click("persist_storage_schema");
    }
  }

  /*--step.3: batch write buffer--*/
  if (FAILEDx(write_buffer_.append(write_infos, cur_macro_seq_, /*out*/ phy_addrs))) {
    STORAGE_LOG(WARN, "unexpected empty write buffer", K(ret), K(write_buffer_));
  } else if (OB_UNLIKELY(addr_ptrs.count() != write_infos.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the write result mismatch", K(ret), K(addr_ptrs),
      K(phy_addrs), K(write_infos));
  } else {
    MacroBlockId tmp_block_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < write_infos.count(); ++i) {
      tmp_block_id.reset();
      ObMetaDiskAddr *addr_ptr = addr_ptrs.at(i);
      const ObMetaDiskAddr &phy_addr = phy_addrs.at(i);

      if (OB_ISNULL(addr_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null addr ptr", K(ret), KP(addr_ptr));
      } else {
        *addr_ptr = phy_addr;
      }
      // build block info set
      if (FAILEDx(phy_addr.get_macro_block_id(tmp_block_id))) {
        STORAGE_LOG(WARN, "failed to get macro block id", K(ret), K(phy_addr));
      } else if (OB_FAIL(block_info_set_.shared_meta_block_info_set_.set_refactored(tmp_block_id,
                                                                                    /*overwrite*/ 0))) {
        if (OB_HASH_EXIST != ret) {
          STORAGE_LOG(WARN, "failed to update block info set", K(ret), K(sub_meta_write_res.storage_schema_addr_));
        } else {
          STORAGE_LOG(DEBUG, "duplicate addr", K(ret), K(sub_meta_write_res.storage_schema_addr_));
          ret = OB_SUCCESS;
        }
      }
      /// NOTE: update tablet space usage(aligned by 4KiB)
      int64_t size = 0;
      if (FAILEDx(phy_addr.get_size_for_tablet_space_usage(size))) {
        STORAGE_LOG(WARN, "failed to get size for tablet space usage", K(ret), K(phy_addr));
      } else {
        space_usage_.tablet_clustered_meta_size_ += upper_align(size, DIO_READ_ALIGN_SIZE);
      }
    }
  }

  // check sub meta write result
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!sub_meta_write_res.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid sub meta write result", K(ret), K(sub_meta_write_res));
  } else {
    time_stats.click("sync_write_buffer");
  }
  return ret;
}

int ObSSTabletPersister::inner_persist_aggregated_meta_(
    const ObTablet &tablet,
    const ObTabletMacroInfo &macro_info,
    const SubMetaWriteResult &sub_meta_write_res,
    ObMultiTimeStats::TimeStats &time_stats)
{
#ifndef OB_BUILD_SHARED_STORAGE
  return OB_NOT_SUPPORTED;
#else
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  ObAtomicTabletMetaOp *op = nullptr;
  int64_t op_id = 0;

  const bool is_ls_tx_data_tablet = tablet.is_ls_tx_data_tablet();
  ObSSTabletTableStoreMetaInfo table_store_meta_info;

  if (OB_UNLIKELY(param_.is_major_shared_object())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "major shared object should not call this method", K(ret), K(param_));
  } else if (OB_UNLIKELY(write_buffer_.pending_write_cnt() > 0))  {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected non-empty write buffer", K(ret), K(write_buffer_));
  }
  // check if sub meta write result is valid
  else if (OB_UNLIKELY(!sub_meta_write_res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid sub meta write result", K(ret), K(sub_meta_write_res));
  } else if (OB_FAIL(sub_meta_write_res.get_table_store_meta_info(is_ls_tx_data_tablet, table_store_meta_info))) {
    STORAGE_LOG(WARN, "failed to get table store meta info", K(ret), K(sub_meta_write_res),
      K(is_ls_tx_data_tablet));
  }
  // check passin macro info
  else if (OB_UNLIKELY(!macro_info.is_inited()
                       || !macro_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid macro info", K(ret), K(macro_info));
  } else if (OB_ISNULL(param_.op_handle_)
             || OB_ISNULL(param_.file_)
             || OB_ISNULL(op = param_.op_handle_->get_atomic_op())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null members", K(ret), K(param_), KP(op));
  } else if (OB_FAIL(param_.get_op_id(op_id))) {
    STORAGE_LOG(WARN, "failed to get op id", K(ret), K(param_));
  } else {
    SMART_VAR(ObTablet, new_tablet) {
      if (OB_FAIL(init_new_tablet_for_persist_(tablet,
                                              allocator_,
                                              sub_meta_write_res,
                                              space_usage_,
                                              /*out*/ new_tablet))) {
        STORAGE_LOG(WARN, "failed to create new tablet for persist", K(ret), K(tablet), K(sub_meta_write_res),
          K(new_tablet));
      } else {
        // persist first level meta by new tablet
        ObTabletTaskFileInfo task_info;
        ObSSMetaUpdateMetaInfo meta_info;
        task_info.type_ = ObAtomicOpType::TABLET_META_WRITE_OP;
        // write task info
        if (OB_FAIL(task_info.set_tablet_info(param_.data_version_, &macro_info, &new_tablet))) {
          LOG_WARN("failed to set tablet", K(ret), K(param_));
        } else if (OB_FAIL(meta_info.set(param_.update_reason_,
                                  new_tablet.get_tablet_meta().get_acquire_scn(),
                                  param_.sstable_op_id_,
                                  table_store_meta_info))) {
          STORAGE_LOG(WARN, "failed to set meta_info", K(ret), K(param_),
            K(table_store_meta_info), K(new_tablet));
        } else if (OB_FAIL(task_info.set_meta_info(meta_info))) {
          STORAGE_LOG(WARN, "failed to set task info", K(ret), K(task_info),
            K(meta_info));
        } else if (OB_FAIL(op->write_task_info(task_info))) {
          STORAGE_LOG(WARN, "persist shared tablet failed", K(ret), K(task_info));
        }

        // finish-op
        ObAtomicOpHandle<ObAtomicTabletMetaOp> &op_handle = *param_.op_handle_;
        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_NOT_NULL(op)
              && OB_TMP_FAIL(param_.file_->abort_op(op_handle))) {
            STORAGE_LOG(WARN, "failed to abort ob", K(ret), KPC(param_.file_), K(op_handle));
          }
        } else if (OB_FAIL(param_.file_->finish_op(op_handle))) {
          STORAGE_LOG(WARN, "failed to finish op", K(ret), KPC(param_.file_), K(op_handle));
        }

        // record addr and space usage
        MacroBlockId block_id;
        ObMetaDiskAddr tablet_addr;
        ObStorageObjectOpt opt;
        const int64_t offset = 0;
        int64_t tablet_persisted_size = -1;
        const int64_t secondary_meta_size = macro_info.get_serialize_size();
        if (OB_FAIL(ret)) {
        } else if(OB_FAIL(ObAtomicTabletMetaFile::generate_file_obj_opt(ObAtomicFileType::TABLET_META,
                                                                        param_.ls_id_.id(),
                                                                        param_.tablet_id_.id(),
                                                                        op_id,
                                                                        param_.tablet_id_.is_ls_inner_tablet(),
                                                                        opt))) {
          STORAGE_LOG(WARN, "failed to get atomic_tablet_meta opt", K(ret), K(op_id), K(param_));
        } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, block_id))) {
          STORAGE_LOG(WARN, "Failed to set macro_block_id", K(ret), K(opt), K(block_id));
        } else if (OB_FAIL(ObTabletPersistCommon::get_tablet_persist_size(param_.data_version_,
                                                                          &macro_info,
                                                                          &new_tablet,
                                                                          tablet_persisted_size))) {
          STORAGE_LOG(WARN, "failed to get tablet_persisted size", K(ret), K(new_tablet), K(tablet_persisted_size));
        } else if (OB_FAIL(tablet_addr.set_block_addr(block_id, offset, tablet_persisted_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) { // without share_block_header, thus raw_block
          STORAGE_LOG(WARN, "failed to get tablet_addr with size", K(ret), K(block_id), K(tablet_persisted_size));
        } else if (FALSE_IT(new_tablet.set_tablet_addr(tablet_addr))) {
        } else if (OB_FAIL(new_tablet.set_macro_info_addr(block_id, offset + (tablet_persisted_size - secondary_meta_size), secondary_meta_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
          STORAGE_LOG(WARN, "failed to set macro info addr", K(ret), K(block_id), K(offset), K(tablet_persisted_size), K(secondary_meta_size));
        } else {
          int64_t size = 0;

          if (OB_FAIL(tablet_addr.get_size_for_tablet_space_usage(size))) {
            STORAGE_LOG(WARN, "failed to get size for tablet space usage", K(ret), K(tablet_addr));
          } else {
            space_usage_.tablet_clustered_meta_size_ += upper_align(size, DIO_READ_ALIGN_SIZE);
            new_tablet.tablet_meta_.space_usage_ = space_usage_;
          }
        }
        time_stats.click("persist_tablet_aggregated_meta");
      }
    }
  }
  return ret;
#endif
}

int ObSSTabletPersister::fetch_table_store_and_write_info_(
    const ObTablet &tablet,
    /*out*/ ObSharedObjectWriteInfo &out_write_info,
    /*out*/ ObTabletTableStore &new_table_store)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
#ifdef ERRSIM
  const int64_t large_co_sstable_threshold_config = GCONF.errsim_large_co_sstable_threshold;
  const int64_t large_co_sstable_threshold = 0 == large_co_sstable_threshold_config ? ObSSTableMetaPersistHelper::SSTABLE_MAX_SERIALIZE_SIZE : large_co_sstable_threshold_config;
#else
  const int64_t large_co_sstable_threshold = ObSSTableMetaPersistHelper::SSTABLE_MAX_SERIALIZE_SIZE;
#endif
  out_write_info.reset();

  /// write_infos' memory will be allocated by allocator of @c write_buffer_
  ObSSTableMetaPersistCtx sstable_persist_ctx(block_info_set_, linked_block_write_ctxs_, write_buffer_.get_allocator());

  ObTabletMemberWrapper<ObTabletTableStore> old_table_store_wrapper;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  const ObTabletTableStore *old_table_store = nullptr;
  int64_t sstable_meta_size_aligned = 0; // aligned by 4K

  if (OB_UNLIKELY(param_.is_major_shared_object())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "major shared object should not call this method", K(ret), K(param_));
  }
  // exclude empty shell tablet
  else if (OB_UNLIKELY(tablet.is_empty_shell())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "empty shell tablet should not call this method", K(ret), K(tablet));
  } else if (OB_FAIL(multi_stats_.acquire_stats("fetch_table_store_and_write_info", time_stats))) {
    STORAGE_LOG(WARN, "failed to acquire time stats", K(ret));
  } else if (OB_FAIL(sstable_persist_ctx.init(ctx_id, ObTablet::SHARED_MACRO_BUCKET_CNT))) {
    STORAGE_LOG(WARN, "failed to init sstable_persist_ctx", K(ret), K(ctx_id), K(sstable_persist_ctx));
  } else if (OB_FAIL(tablet.fetch_table_store(old_table_store_wrapper))) {
    STORAGE_LOG(WARN, "failed to fetch table store", K(ret), K(old_table_store_wrapper));
  } else if (OB_FAIL(old_table_store_wrapper.get_member(old_table_store))) {
    STORAGE_LOG(WARN, "failed to get table store from wrapper", K(ret),
      K(old_table_store_wrapper), KP(old_table_store));
  } else if (OB_ISNULL(old_table_store)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null table store", K(ret), K(old_table_store_wrapper), KP(old_table_store));
  } else if (FALSE_IT(time_stats->click("fetch_table_store"))) {
  }

  SSTableMetaWriteOp sst_write_op(param_, cur_macro_seq_, block_info_set_, write_buffer_);
  ObSSTableMetaPersistHelper sst_persist_helper(param_, sstable_persist_ctx, large_co_sstable_threshold, cur_macro_seq_);

  if (FAILEDx(sst_persist_helper.register_write_op(&sst_write_op))) {
    STORAGE_LOG(WARN, "failed to register sstable write op", K(ret), K(sst_write_op), K(sst_persist_helper));
  } else if (OB_FAIL(sst_persist_helper.do_persist_all_sstables(*old_table_store,
                                                                allocator_,
                                                                multi_stats_,
                                                                new_table_store,
                                                                sstable_meta_size_aligned))) {
    STORAGE_LOG(WARN, "failed to persist all sstable from table store", K(ret), KPC(old_table_store),
      K(new_table_store), K(sstable_meta_size_aligned));
  } else if (FALSE_IT(time_stats->click("fetch_and_persist_sstable"))) {
  }

  // serialize new table store into write info if everything is ok
  ObTabletPersistCommon::PersistWithDataVersionWrapper<ObTabletTableStore> member_wrapper(param_.data_version_, &new_table_store);
  if (FAILEDx(fill_write_info_(write_strategy_,
                               param_,
                               &member_wrapper,
                               write_buffer_.get_allocator(),
                               out_write_info))) {
    STORAGE_LOG(WARN, "failed to fill table store write info", K(ret), K(param_), K(out_write_info));
  } else {
    // update tablet space usage
    space_usage_.tablet_clustered_meta_size_ += sstable_meta_size_aligned;
    space_usage_.tablet_clustered_meta_size_ += sstable_persist_ctx.total_tablet_meta_size_;
  }
  return ret;
}

int ObSSTabletPersister::fetch_storage_schema_and_write_info_(
    const ObTablet &tablet,
    /*out*/ ObSharedObjectWriteInfo &out_write_info)
{
  OB_ASSERT(is_ready_for_persist_());
  OB_ASSERT(!tablet.is_empty_shell());
  int ret = OB_SUCCESS;
  out_write_info.reset();
  ObStorageSchema *storage_schema = nullptr;

  if (OB_UNLIKELY(param_.is_major_shared_object())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "major shared object should not call this method", K(ret), K(param_));
  }
  // exclude empty shell tablet
  else if (OB_UNLIKELY(tablet.is_empty_shell())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "empty shell tablet should not call this method", K(ret), K(tablet));
  } else if (OB_FAIL(tablet.load_storage_schema(allocator_, storage_schema))) {
    STORAGE_LOG(WARN, "failed to load storage schema", K(ret), KP(storage_schema));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null storage schema", K(ret), KP(storage_schema));
  } else if (OB_FAIL(fill_write_info_(write_strategy_,
                                      param_,
                                      storage_schema,
                                      write_buffer_.get_allocator(),
                                      out_write_info))) {
    STORAGE_LOG(WARN, "failed to fill storage schema write info", K(ret), K(param_), K(out_write_info));
  }
  return ret;
}

int ObSSTabletPersister::fetch_and_persist_tablet_macro_info_(
    /*out*/ ObLinkedMacroBlockItemWriter &linked_writer,
    /*out*/ ObTabletMacroInfo &macro_info)
{
  OB_ASSERT(is_ready_for_persist_());
  int ret = OB_SUCCESS;
  ObLinkedMacroInfoWriteParam linked_macro_info_param;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;

  if (OB_UNLIKELY(param_.is_major_shared_object())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "major shared object should not call this method", K(ret), K(param_));
  } else if (OB_UNLIKELY(macro_info.is_inited())) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "macro info init twice", K(ret), K(macro_info));
  } else if (OB_FAIL(multi_stats_.acquire_stats("fetch_and_persist_tablet_macro_info", time_stats))) {
    STORAGE_LOG(WARN, "failed to acquire time stats", K(ret), KP(time_stats));
  } else if (OB_FAIL(ObTabletPersistCommon::make_tablet_macro_info(param_,
                                                                   cur_macro_seq_,
                                                                   allocator_,
                                                                   /* out */ block_info_set_,
                                                                   /* out */ linked_writer,
                                                                   /* out */ macro_info))) {
    STORAGE_LOG(WARN, "failed to make tablet macro info", K(ret), K(param_));
  } else if (FALSE_IT(time_stats->click("init_macro_info"))) {
  } else if (OB_FAIL(time_stats->set_extra_info("%s:%ld,%s:%ld,%s:%ld,%s:%ld",
              "meta_block_cnt", block_info_set_.meta_block_info_set_.size(),
              "data_block_cnt", block_info_set_.data_block_info_set_.size(),
              "shared_meta_block_cnt", block_info_set_.shared_meta_block_info_set_.size(),
              "shared_data_block_cnt", block_info_set_.clustered_data_block_info_map_.size()))) {
    STORAGE_LOG(WARN, "failed to set time stats extra info", K(ret));
  } else {
    // update cur_macro_seq_
    const int64_t link_last_seq =  linked_writer.get_last_macro_seq();
    OB_ASSERT(link_last_seq >= cur_macro_seq_);
    cur_macro_seq_ = link_last_seq;
  }

  if (OB_FAIL(ret)) {
    /// add written blocks into @c history_write_ctxs_ for rollback
    int tmp_ret = OB_SUCCESS;
    ObSharedObjectsWriteCtx tablet_linked_block_write_ctx;
    ObMetaDiskAddr addr;
    addr.set_block_addr(ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK,
                        0, /*offset*/
                        1, /*size*/
                        ObMetaDiskAddr::DiskType::BLOCK); // unused;
    tablet_linked_block_write_ctx.set_addr(addr);
    if (!linked_writer.is_closed() && OB_TMP_FAIL(linked_writer.close())) {
      STORAGE_LOG(WARN, "failed to close linked block writer", K(ret), K(tmp_ret));
    }
    for (int64_t i = 0; i < linked_writer.get_meta_block_list().count(); ++i) {
      const blocksstable::MacroBlockId &macro_id = linked_writer.get_meta_block_list().at(i);
      if (OB_TMP_FAIL(tablet_linked_block_write_ctx.add_object_id(macro_id))) {
        STORAGE_LOG(WARN, "failed to add object id", K(ret), K(tmp_ret), K(i), K(macro_id));
      }
    }
    if (OB_TMP_FAIL(linked_block_write_ctxs_.push_back(tablet_linked_block_write_ctx))) {
      STORAGE_LOG(WARN, "failed to push back tablet_linked_block_write_ctx", K(ret), K(tmp_ret),
        K(tablet_linked_block_write_ctx));
    }
  }
  return ret;
}

/*static*/ int ObSSTabletPersister::check_tablet_macro_info_(const ObTabletMacroInfo &macro_info)
{
  int ret = OB_SUCCESS;
  ObMacroInfoIterator macro_iter;

  if (OB_UNLIKELY(!macro_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tablet macro info", K(ret), K(macro_info));
  } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, macro_info))) {
    STORAGE_LOG(WARN, "failed to init macro info iterator", K(ret), K(macro_info));
  } else {
    while (OB_SUCC(ret)) {
      ObTabletBlockInfo block_info;
      if (OB_FAIL(macro_iter.get_next(block_info))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "fail to get next block info", K(ret), K(macro_info));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (!block_info.macro_id().is_shared_data_or_meta()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected not shared macro block detected", K(ret), K(block_info));
      }
    }
  }
  return ret;
}

/*static*/ int ObSSTabletPersister::check_tablet_space_usage_(
    const InnerWriteSummary &summary,
    const ObTabletSpaceUsage &tablet_space_usage)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!tablet_space_usage.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tablet space usage", K(ret), K(tablet_space_usage));
  } else if (tablet_space_usage.tablet_clustered_meta_size_ > summary.total_write_bytes()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet space usage statistics are incorrect", K(ret),
      K(tablet_space_usage), K(summary));
  }
  return ret;
}

static int check_block_macro_seq(
  const int64_t start_macro_seq,
  const int64_t end_macro_seq,
  const MacroBlockId &block_id,
  /* out */ hash::ObHashSet<int64_t> &allocated_macro_seqs,
  /* out */ int64_t &min_macro_seq)
{
  OB_ASSERT(start_macro_seq < end_macro_seq);
  int ret = OB_SUCCESS;
  const int64_t block_macro_seq = block_id.get_macro_seq();

  if (OB_UNLIKELY(!block_id.is_shared_data_or_meta())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected not shared macro block detected", K(ret), K(block_id));
  } else if (OB_UNLIKELY(block_macro_seq < start_macro_seq
                          || block_macro_seq >= end_macro_seq)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "block macro seq out of range", K(ret), K(block_macro_seq),
      K(start_macro_seq), K(end_macro_seq), K(block_id));
  } else if (OB_FAIL(allocated_macro_seqs.set_refactored(block_macro_seq, /* overwrite */ 0))) {
    if (OB_HASH_EXIST == ret) {
      STORAGE_LOG(WARN, "unexpected duplicate macro seq", K(ret), K(block_macro_seq),
        K(block_id), K(allocated_macro_seqs));
    } else {
      STORAGE_LOG(WARN, "failed to set macro seq", K(ret));
    }
  } else {
    min_macro_seq = MIN(min_macro_seq, block_macro_seq);
  }
  return ret;
}

/* static */ int ObSSTabletPersister::check_macro_seqs_for_write_batch_(
    const int64_t start_macro_seq,
    const int64_t end_macro_seq,
    const InnerWriteSummary &write_summary,
    const SubMetaWriteResult &sub_meta_write_res,
    const ObIArray<ObSharedObjectsWriteCtx> &linked_block_write_ctxs)
{
  int ret = OB_SUCCESS;
  const int64_t ctx_id = share::is_reserve_mode()
                      ? ObCtxIds::MERGE_RESERVE_CTX_ID
                      : ObCtxIds::DEFAULT_CTX_ID;
  hash::ObHashSet<int64_t> allocated_macro_seqs;
  int64_t min_macro_seq = INT64_MAX;

  if (OB_UNLIKELY(start_macro_seq < 0
                  || end_macro_seq < 0
                  || start_macro_seq >= end_macro_seq)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro seq range", K(ret), K(start_macro_seq), K(end_macro_seq));
  } else if (OB_UNLIKELY(!sub_meta_write_res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid sub meta write result", K(ret), K(sub_meta_write_res));
  } else if (OB_FAIL(allocated_macro_seqs.create(16,
                                                 lib::ObMemAttr(MTL_ID(), "MacroSeqSet", ctx_id)))) {
    STORAGE_LOG(WARN, "failed to create macro seq set", K(ret));
  } else {
    const ObIArray<MacroBlockId> &written_block_ids = write_summary.written_objects_;
    // blocks of common tablet meta
    for (int64_t i = 0; OB_SUCC(ret) && i < written_block_ids.count(); ++i) {
      if (OB_FAIL(check_block_macro_seq(start_macro_seq,
                                        end_macro_seq,
                                        written_block_ids.at(i),
                                        /* out */ allocated_macro_seqs,
                                        /* out */ min_macro_seq))) {
        STORAGE_LOG(WARN, "failed to check block macro seq", K(ret));
      }
    }
    // blocks of sstable macro info(if any)
    for (int64_t ctx_idx = 0; OB_SUCC(ret) && ctx_idx < linked_block_write_ctxs.count(); ++ctx_idx) {
      const ObIArray<MacroBlockId> &linked_block_ids = linked_block_write_ctxs.at(ctx_idx).block_ids_;
      for (int64_t block_idx = 0; OB_SUCC(ret) && block_idx < linked_block_ids.count(); ++block_idx) {
        if (OB_FAIL(check_block_macro_seq(start_macro_seq,
                                        end_macro_seq,
                                        linked_block_ids.at(block_idx),
                                        /* out */ allocated_macro_seqs,
                                        /* out */ min_macro_seq))) {
          STORAGE_LOG(WARN, "failed to check block macro seq", K(ret));
        }
      }
    }

    /// NOTE: Batch read optimization requires ensuring the macro seq of table sotre is
    /// minimized(exclude tablet macro info)
    /// For more detail:
    MacroBlockId table_store_block_id;
    if (FAILEDx(sub_meta_write_res.table_store_addr_.get_macro_block_id(table_store_block_id))) {
      STORAGE_LOG(WARN, "failed to get table store block id", K(ret), K(sub_meta_write_res.table_store_addr_));
    } else if (OB_UNLIKELY(!table_store_block_id.is_shared_data_or_meta())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected not shared macro block detected", K(ret), K(table_store_block_id));
    } else if (OB_UNLIKELY(min_macro_seq != table_store_block_id.get_macro_seq())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "table store block id's macro seq is not minimized", K(ret),
        K(min_macro_seq), K(table_store_block_id));
    }
  }
  return ret;
}

/// @brief: persistence checking
int ObSSTabletPersister::sanity_check_(const SubMetaWriteResult &sub_meta_write_res)
{
  OB_ASSERT(!sub_meta_write_res.is_empty_shell_);
  int ret = OB_SUCCESS;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  const InnerWriteSummary &write_summary = write_buffer_.write_summary();

  if (OB_FAIL(multi_stats_.acquire_stats("sanity_check", time_stats))) {
    STORAGE_LOG(WARN, "failed to acquire time stats", K(ret));
  } else if (OB_FAIL(check_tablet_space_usage_(write_summary, space_usage_))) {
    STORAGE_LOG(WARN, "check tablet space usage failed", K(ret));
  } else if (WriteStrategy::BATCH != write_strategy_) {
    // do nothing
  } else if (OB_FAIL(check_macro_seqs_for_write_batch_(param_.start_macro_seq_,
                                                       cur_macro_seq_,
                                                       write_buffer_.write_summary(),
                                                       sub_meta_write_res,
                                                       linked_block_write_ctxs_))) {
    STORAGE_LOG(WARN, "failed to check macro seqs", K(ret));
  } else {
    time_stats->click("check_macro_seq");
  }

  if (OB_SUCC(ret)) {
    /// NOTE: written blocks must be counted in @c block_info_set_
    const ObIArray<MacroBlockId> &written_blocks = write_summary.written_objects_;
    const ObBlockInfoSet::TabletMacroSet & shared_meta_block_info_set = block_info_set_.shared_meta_block_info_set_;

    for (int64_t i = 0; OB_SUCC(ret) && i < written_blocks.count(); ++i) {
      const MacroBlockId &block_id = written_blocks.at(i);

      if (OB_UNLIKELY(!block_id.is_shared_data_or_meta())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unnexpected not shared macro block detected", K(ret), K(block_id));
      } else if (OB_FAIL(shared_meta_block_info_set.exist_refactored(block_id))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else if (OB_HASH_NOT_EXIST == ret) {
          STORAGE_LOG(WARN, "written addr is not counted in block info set", K(ret),
            K(block_id));
        } else {
          STORAGE_LOG(WARN, "failed to do exist refactored", K(ret), K(block_id));
        }
      }
    }
    time_stats->click("check_shared_block_info_set");
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(time_stats->set_extra_info("write_summary:%s",
                                                ObCStringHelper().convert(write_summary)))) {
    STORAGE_LOG(WARN, "failed to set time stats extra info", K(ret));
  }
  return ret;
}

void ObSSTabletPersister::preallocate_macro_seqs_(const int64_t n)
{
  OB_ASSERT(n > 0);
  preallocated_seq_range_ = {cur_macro_seq_, cur_macro_seq_ + n};
  cur_macro_seq_ += n;
}

int ObSSTabletPersister::get_preallocated_macro_seq_(/* out */ int64_t &macro_seq)
{
  int ret = OB_SUCCESS;
  macro_seq = 0;
  int64_t &start = preallocated_seq_range_.first;
  const int64_t end = preallocated_seq_range_.second;

  if (start >= end) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "no preallocated seq left", K(ret), K(start), K(end));
  } else {
    macro_seq = start++;
  }
  return ret;
}

/*static*/ int ObSSTabletPersister::init_new_tablet_for_persist_(
    const ObTablet &old_tablet,
    common::ObArenaAllocator &allocator,
    const SubMetaWriteResult &sub_meta_write_res,
    const ObTabletSpaceUsage &new_space_usage,
    /*out*/ ObTablet &new_tablet)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = old_tablet.get_ls_id();
  const ObTabletID &tablet_id = old_tablet.get_tablet_id();
  const ObTabletMapKey key(ls_id, tablet_id);

  if (OB_UNLIKELY(!sub_meta_write_res.is_valid()
                  || !new_space_usage.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid sub meta write result", K(ret), K(sub_meta_write_res), K(new_space_usage));
  } else if  (OB_FAIL(new_tablet.init_for_ss_perist(allocator,
                                                    old_tablet,
                                                    sub_meta_write_res.storage_schema_addr_,
                                                    sub_meta_write_res.table_store_addr_,
                                                    new_space_usage))) {
    STORAGE_LOG(WARN, "failed to init tablet for ss persistence", K(ret), K(old_tablet),
      K(sub_meta_write_res), K(new_space_usage));
  } else if (OB_UNLIKELY(!new_tablet.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid new_tablet", K(ret), K(new_tablet));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
