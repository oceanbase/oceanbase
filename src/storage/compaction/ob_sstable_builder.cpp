/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_sstable_builder.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"

namespace oceanbase
{
using namespace blocksstable;
namespace compaction
{
/**
 * ---------------------------------------------------------ObMacroBlockMergeHelper--------------------------------------------------------------
 */
int ObSSTableRebuildMicroBlockIter::prefetch()
{
  int ret = OB_SUCCESS;

  while(OB_SUCC(ret)) {
    if (prefetch_idx_ - iter_idx_ < PREFETCH_DEPTH
       && prefetch_idx_ < macro_id_array_.count() - 1) {
      prefetch_idx_++;
      int64_t io_index = prefetch_idx_ % PREFETCH_DEPTH;
      ObStorageObjectHandle &macro_io_handle = macro_io_handle_[io_index];
      macro_io_handle.reset();

      ObStorageObjectReadInfo read_info;
      read_info.offset_ = 0;
      read_info.size_ = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      read_info.io_desc_.set_sys_module_id(ObIOModule::INDEX_BLOCK_MICRO_ITER_IO);
      read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
      read_info.macro_block_id_ = macro_id_array_.at(prefetch_idx_);
      read_info.buf_ = io_buf_[io_index];
      read_info.mtl_tenant_id_ = MTL_ID();

      if (OB_FAIL(ObObjectManager::async_read_object(read_info, macro_io_handle))) {
        LOG_WARN("Fail to read macro block", K(ret), K(read_info));
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObSSTableRebuildMicroBlockIter::init()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < PREFETCH_DEPTH; ++i) {
    if (OB_ISNULL(io_buf_[i] = reinterpret_cast<char*>(io_allocator_.alloc(common::OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc SSTableRebuildMicroBlockIter read info buffer", K(ret), K(i));
    }
  }
  return ret;
}

int ObSSTableRebuildMicroBlockIter::open_next_macro_block()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_iter_end())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(prefetch())) {
    STORAGE_LOG(WARN, "fail to prefetch", K(ret));
  } else {
    iter_idx_++;
    ObDatumRange range;
    ObStorageObjectHandle &macro_io_handle = get_curr_macro_handle();
    range.set_whole_range();
    mirco_block_iter_.reset();

    if (OB_FAIL(macro_io_handle.wait())) {
        LOG_WARN("failed to read macro block from io", K(ret));
    } else if (OB_FAIL(mirco_block_iter_.open(
                macro_io_handle.get_buffer(),
                macro_io_handle.get_data_size(),
                range,
                index_read_info_,
                false,
                false,
                false))) {
      STORAGE_LOG(WARN, "fail to open macro block", K(ret));
    }
  }

  return ret;
}

int ObSSTableRebuildMicroBlockIter::get_next_micro_block(
    ObMicroBlockDesc &micro_block_desc,
    ObMicroIndexInfo &micro_index_info)
{
  int ret = OB_SUCCESS;
  allocator_.reuse();
  if (OB_FAIL(mirco_block_iter_.get_next_micro_block_desc(micro_block_desc, micro_index_info, allocator_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next micro block desc", K(ret));
    }
  } else {
    micro_index_info.parent_macro_id_ = get_curr_macro_handle().get_macro_id();
    if (OB_UNLIKELY(!micro_index_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iterated invalid micro index info", K(ret));
    }
  }
  return ret;
}


/**
 * ---------------------------------------------------------ObSSTableBuilder--------------------------------------------------------------
 */
ObSSTableBuilder::ObSSTableBuilder()
  : allocator_(ObMemAttr(MTL_ID(), "sstBuilder")),
    index_builder_(false /* not use buffer*/),
    data_store_desc_(),
    index_read_info_(NULL),
    rebuilder_ptr_(nullptr)
{
}

ObSSTableBuilder::~ObSSTableBuilder()
{
  reset();
}

void ObSSTableBuilder::reset()
{
  index_builder_.reset();
  data_store_desc_.reset();
  index_read_info_ = NULL;
  if (OB_NOT_NULL(rebuilder_ptr_)) {
    rebuilder_ptr_->~ObSSTableRebuilder();
    allocator_.free(rebuilder_ptr_);
    rebuilder_ptr_ = nullptr;
  }
}

int ObSSTableBuilder::set_index_read_info(const ObITableReadInfo *read_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != index_read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected index read info", K(ret), KPC(this), KP(index_read_info_));
  } else {
    index_read_info_ = read_info;
  }
  return ret;
}

int ObSSTableBuilder::prepare_index_builder()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!data_store_desc_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data store desc", K(ret), K(data_store_desc_));
  } else if (OB_FAIL(index_builder_.init(data_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to init", K(ret), K(data_store_desc_));
  }

  return ret;
}

int ObSSTableBuilder::build_sstable_merge_res(
    const ObStaticMergeParam &merge_param,
    const share::ObPreWarmerParam &pre_warm_param,
    int64_t &macro_start_seq,
    ObMergeBlockInfo &block_info,
    ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  const int64_t input_macro_seq = macro_start_seq;
  if (GCTX.is_shared_storage_mode() && is_major_merge_type(data_store_desc_.get_desc().get_merge_type())) {
#ifdef OB_BUILD_SHARED_STORAGE
    // no need to rebuild sstable in shared storage mode
    if (OB_FAIL(index_builder_.close_with_macro_seq(
      res, macro_start_seq, OB_DEFAULT_MACRO_BLOCK_SIZE/*nested_size*/, 0/*nested_offset*/, pre_warm_param))) {
      STORAGE_LOG(WARN, "fail to close", K(ret), K(index_builder_));
    } else {
      const ObMergeBlockInfo &block_info_from_builder = index_builder_.get_merge_block_info();
      block_info.add_index_block_info(block_info_from_builder);
      STORAGE_LOG(INFO, "success to close index builder", KR(ret), K(macro_start_seq), K(input_macro_seq), K(block_info_from_builder));
    }
#else
    ret = OB_NOT_SUPPORTED;
#endif
  } else {
    // TODO temp solution, use different sstable builder in different mode
    void *buf = NULL;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableRebuilder)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc sstable rebuilder", KR(ret));
    } else if (FALSE_IT(rebuilder_ptr_ = new(buf) ObSSTableRebuilder(data_store_desc_, index_read_info_))) {
    } else if (OB_FAIL(rebuilder_ptr_->build_res_with_rewrite_macros(
              merge_param, pre_warm_param, input_macro_seq, index_builder_,
              block_info, res))) {
      LOG_WARN("failed to build res with rewrite macros", KR(ret));
    }
  }
  return ret;
}

ObSSTableRebuilder::ObSSTableRebuilder(
  ObWholeDataStoreDesc &desc,
  const ObITableReadInfo *index_read_info)
  : data_store_desc_(desc),
    rebuild_index_builder_(false /* not use buffer*/),
    macro_writer_(true/*is_need_macro_buffer*/),
    index_read_info_(index_read_info)
{
}

ObSSTableRebuilder::~ObSSTableRebuilder()
{
  reset();
}

void ObSSTableRebuilder::reset()
{
  rebuild_index_builder_.reset();
  macro_writer_.reset();
  index_read_info_ = NULL;
}

int ObSSTableRebuilder::build_res_with_rewrite_macros(
    const ObStaticMergeParam &merge_param,
    const share::ObPreWarmerParam &pre_warm_param,
    const int64_t input_macro_seq,
    ObSSTableIndexBuilder &index_builder,
    ObMergeBlockInfo &block_info,
    ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, DEFAULT_MACRO_ID_COUNT> macro_id_array;
  macro_id_array.set_attr(ObMemAttr(MTL_ID(), "sstBuilder", ObCtxIds::MERGE_NORMAL_CTX_ID));
  ObLocalArena local_arena("MetaIter");
  blocksstable::ObSSTableIndexBuilder::ObMacroMetaIter iter;
  int64_t multiplexed_macro_block_count = 0;
  int64_t macro_start_seq = input_macro_seq;
  bool build_res_with_rebuild = false;

  if (OB_FAIL(rebuild_index_builder_.init(data_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to init", K(ret), K(data_store_desc_));
  } else if (OB_FAIL(open_macro_writer(pre_warm_param))) {
    STORAGE_LOG(WARN, "fail to open macro writer", K(ret), K(pre_warm_param));
  } else if (OB_FAIL(index_builder.init_meta_iter(local_arena, iter))) {
    STORAGE_LOG(WARN, "fail to init meta iter", K(ret), K(index_builder));
  } else if (OB_FAIL(check_need_rebuild(merge_param, macro_id_array, iter, multiplexed_macro_block_count))) {
    STORAGE_LOG(WARN, "failed to check need rebuild", K(ret));
  } else if (macro_id_array.count() != 0) {
    build_res_with_rebuild = true;
    iter.reuse();
    STORAGE_LOG(INFO, "rebuild sstable merge", K(ret), K(data_store_desc_.get_desc().get_table_cg_idx()));
    if (OB_FAIL(rebuild_macro_block(macro_id_array, iter))) {
      STORAGE_LOG(WARN, "fail to rebuild macro block", K(ret), K(macro_id_array));
    } else if (OB_FAIL(rebuild_index_builder_.close_with_macro_seq(
        res, macro_start_seq, OB_DEFAULT_MACRO_BLOCK_SIZE/*nested_size*/, 0/*nested_offset*/, pre_warm_param))) {
      STORAGE_LOG(WARN, "fail to close", K(ret), K(rebuild_index_builder_));
    } else { //update merge info
      block_info.multiplexed_macro_block_count_ = multiplexed_macro_block_count;
      block_info.macro_block_count_ = res.data_blocks_cnt_;
      const ObMergeBlockInfo &block_info_from_builder = rebuild_index_builder_.get_merge_block_info();
      block_info.add_index_block_info(block_info_from_builder);
      STORAGE_LOG(INFO, "after rebuild sstable", K(ret), "cg_idx", data_store_desc_.get_desc().get_table_cg_idx(),
         "old_multiplexed_macro_block_count", block_info.multiplexed_macro_block_count_,
         "old_total_macro_count", block_info.macro_block_count_,
         "new_multiplexed_macro_block_count", multiplexed_macro_block_count,
         "new_total_macro_count", res.data_blocks_cnt_);
    }
  }
  if (OB_FAIL(ret) || build_res_with_rebuild) {
  } else if (OB_FAIL(index_builder.close_with_macro_seq(
    res, macro_start_seq, OB_DEFAULT_MACRO_BLOCK_SIZE/*nested_size*/, 0/*nested_offset*/, pre_warm_param))) {
    STORAGE_LOG(WARN, "fail to close", K(ret), K(index_builder));
  } else if (!is_local_exec_mode(merge_param.get_exec_mode())) {
    STORAGE_LOG(INFO, "success to close index builder", KR(ret), K(macro_start_seq), K(input_macro_seq));
  }
  return ret;
}

int ObSSTableRebuilder::open_macro_writer(const share::ObPreWarmerParam &pre_warm_param)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq macro_start_seq(0);
  data_store_desc_.get_desc().sstable_index_builder_ = &rebuild_index_builder_;
  macro_start_seq.set_rebuild_merge_type();

  ObMacroSeqParam macro_seq_param;
  macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  macro_seq_param.start_ = macro_start_seq.macro_data_seq_;

  if (OB_FAIL(macro_writer_.open(
          data_store_desc_.get_desc(), 0 /*parallel_idx*/, macro_seq_param,
          pre_warm_param, rebuild_index_builder_.get_private_object_cleaner()))) {
    STORAGE_LOG(WARN, "failed to open macro writer", K(ret), K(data_store_desc_));
  }

  return ret;
}

int ObSSTableRebuilder::pre_check_rebuild(const ObStaticMergeParam &merge_param, MetaIter &iter, bool &need_check_rebuild)
{
  int ret = OB_SUCCESS;
  need_check_rebuild = true;
  const int64_t data_version = data_store_desc_.get_desc().get_major_working_cluster_version();
  if (data_version < DATA_VERSION_4_3_0_0) {
    need_check_rebuild = false;
  } else if (data_version >= DATA_VERSION_4_3_2_0) {
    if (merge_param.concurrent_cnt_ <= 1 || iter.get_macro_block_count() <= 1) {
      need_check_rebuild = false;
    }
  }
  return ret;
}

int ObSSTableRebuilder::check_need_rebuild(const ObStaticMergeParam &merge_param,
                                         ObIArray<MacroBlockId> &macro_id_array,
                                         MetaIter &iter,
                                         int64_t &multiplexed_macro_block_count)
{
  int ret = OB_SUCCESS;
  macro_id_array.reset();
  multiplexed_macro_block_count = 0;
  const int64_t snapshot_version = merge_param.scn_range_.end_scn_.get_val_for_tx();
  blocksstable::ObDataMacroBlockMeta macro_meta;
  blocksstable::MacroBlockId last_macro_id;
  int64_t last_macro_block_sum = 0;
  int64_t reduce_macro_block_cnt = 0;
  bool last_macro_is_first = false;
  bool need_check_rebuild = true;

  if (OB_FAIL(pre_check_rebuild(merge_param, iter, need_check_rebuild))) {
    STORAGE_LOG(WARN, "Fail to pre check need rebuild", K(ret));
  } else if (need_check_rebuild) {
    // find continues macro to rewrite
    while (OB_SUCC(ret) && OB_SUCC(iter.get_next_macro_block(macro_meta))) {
      if (check_macro_block_could_merge(macro_meta)) {
        const int64_t macro_block_sum = macro_meta.val_.occupy_size_ + macro_meta.val_.block_size_;
        bool need_merge = false;
        // check last_macro_block_sum + cur_macro can be merged into one
        if (OB_FAIL(check_cur_macro_need_merge(last_macro_block_sum, macro_meta, need_merge))) {
          STORAGE_LOG(WARN, "fail to check_cur_macro_need_merge", K(ret), K(macro_meta));
        } else if (!need_merge) { // found first can't merge macro, reset collect info
          last_macro_id = macro_meta.get_macro_id();
          last_macro_is_first = true;
          last_macro_block_sum = macro_block_sum;
          multiplexed_macro_block_count = snapshot_version != macro_meta.val_.snapshot_version_ ?
            multiplexed_macro_block_count + 1 : multiplexed_macro_block_count;
        } else {
          if (last_macro_is_first && OB_FAIL(macro_id_array.push_back(last_macro_id))) {
            STORAGE_LOG(WARN, "failed to push back macro id", K(ret), K(last_macro_id));
          } else if (OB_FAIL(macro_id_array.push_back(macro_meta.get_macro_id()))) {
            STORAGE_LOG(WARN, "failed to push back macro id", K(ret), K(macro_meta));
          } else {
            reduce_macro_block_cnt++;
            last_macro_block_sum += macro_block_sum;
            last_macro_is_first = false;
          }
        }
      } else {
        last_macro_is_first = false;
        last_macro_block_sum = 0;
      }
    }

    if (OB_LIKELY(ret == OB_ITER_END)) {
      ret = OB_SUCCESS;
      if (iter.get_macro_block_count() * REBUILD_MACRO_BLOCK_THRESOLD / 100 >= reduce_macro_block_cnt) {
        macro_id_array.reset();
      }
    }
  }

  return ret;
}

int ObSSTableRebuilder::check_cur_macro_need_merge(
    const int64_t last_macro_blocks_sum,
    const ObDataMacroBlockMeta &curr_macro_meta,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_sum = curr_macro_meta.val_.occupy_size_ + curr_macro_meta.val_.block_size_;
  int64_t estimate_meta_size = 0;
  need_merge = true;

  if (last_macro_blocks_sum == 0 // is first macro block
      || last_macro_blocks_sum + macro_block_sum >= DEFAULT_MACRO_BLOCK_SIZE) {
    need_merge = false;
  } else if (OB_FAIL(macro_writer_.get_estimate_meta_block_size(curr_macro_meta, estimate_meta_size))) {
    STORAGE_LOG(WARN, "fail to get_estimate_meta_block_size", K(ret), K(curr_macro_meta));
  } else if (last_macro_blocks_sum + estimate_meta_size + macro_block_sum >= DEFAULT_MACRO_BLOCK_SIZE) {
    need_merge = false;
  }

  return ret;
}

int ObSSTableRebuilder::rebuild_macro_block(const ObIArray<MacroBlockId> &macro_id_array, MetaIter &iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == index_read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null read info", K(ret));
  } else {
    ObSSTableRebuildMicroBlockIter micro_iter(macro_id_array, *index_read_info_);
    if (OB_FAIL(micro_iter.init())) {
      STORAGE_LOG(WARN, "init SSTableRebuildMicroBlockIter failed", K(ret));
    } else {
      blocksstable::ObDataMacroBlockMeta macro_meta;
      int64_t macro_id_idx = 0;
      while (OB_SUCC(ret) && OB_SUCC(iter.get_next_macro_block(macro_meta))) {
        if (OB_UNLIKELY(!macro_meta.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected macro meta", K(ret), K(macro_meta));
        } else if (macro_id_idx < macro_id_array.count() && macro_meta.get_macro_id() == macro_id_array.at(macro_id_idx)) {
          if (OB_FAIL(micro_iter.open_next_macro_block())) {
            STORAGE_LOG(WARN, "fail to open next macro block", K(ret), K(micro_iter));
          } else if (OB_FAIL(rewrite_macro_block(micro_iter))) {
            STORAGE_LOG(WARN, "fail to rewrite macro block", K(ret), K(micro_iter));
          } else {
            macro_id_idx++;
            STORAGE_LOG(INFO, "reopen macro block", K(ret), K(macro_meta.get_macro_id()));
          }
        } else if (OB_FAIL(macro_writer_.append_macro_block(macro_meta))) {
          STORAGE_LOG(WARN, "fail to appen macro block", K(ret), K(macro_meta));
        }
      }

      if (OB_UNLIKELY(ret != OB_ITER_END)) {
        STORAGE_LOG(WARN, "unexpected ret", K(ret));
      } else if (OB_UNLIKELY(macro_id_idx != macro_id_array.count()
          || !micro_iter.is_iter_end())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ietr idx", K(ret), K(macro_id_idx), K(macro_id_array), K(micro_iter));
      } else if (OB_FAIL(macro_writer_.close())) {
        STORAGE_LOG(WARN, "failed to close", K(ret), K(macro_writer_));
      }
    }
  }
  return ret;
}

int ObSSTableRebuilder::rewrite_macro_block(ObSSTableRebuildMicroBlockIter &micro_iter)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDesc micro_block_desc;
  ObMicroIndexInfo micro_index_info;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(micro_iter.get_next_micro_block(micro_block_desc, micro_index_info))) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        break;
      } else {
        STORAGE_LOG(WARN, "fail to get next micro block", K(ret), K(micro_iter));
      }
    } else if (OB_FAIL(macro_writer_.append_micro_block(micro_block_desc, micro_index_info))) {
      STORAGE_LOG(WARN, "fail to append micro", K(ret), K(micro_block_desc), K(macro_writer_));
    }
  }

  return ret;
}

}
}
