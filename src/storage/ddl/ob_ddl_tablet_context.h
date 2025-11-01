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

#ifndef _OCEANBASE_STORAGE_DDL_OB_DDL_TABLET_CONTEXT_H_
#define _OCEANBASE_STORAGE_DDL_OB_DDL_TABLET_CONTEXT_H_

#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_tablet_slice_writer.h"

namespace oceanbase
{
namespace storage
{
class ObTabletSliceWriter;
class ObVectorIndexTabletContext;
class ObPipeline;
class ObMacroMetaStoreManager;
class ObDDLIndependentDag;
class ObDDLTabletScanTask;

struct ObDDLChunk final
{
public:
  ObDDLChunk() :
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(-1),
    is_slice_end_(false),
    chunk_data_(nullptr) { }
  ~ObDDLChunk() {}
  OB_INLINE bool is_valid() const {
    return tablet_id_.is_valid() && slice_idx_ >= 0
    && (is_slice_end_ || (nullptr != chunk_data_ && chunk_data_->is_valid()));
  }
  OB_INLINE bool has_chunk_data() const { return nullptr != chunk_data_; }
  void reset()
  {
    tablet_id_.reset();
    slice_idx_ = -1;
    is_slice_end_ = false;
    chunk_data_ = nullptr;
  }
  TO_STRING_KV(K_(tablet_id), K_(slice_idx), K_(is_slice_end), K_(chunk_data));
public:
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  bool is_slice_end_;
  ObChunk *chunk_data_;
};


struct ObRemainCgBlock
{
public:
  ObRemainCgBlock() : has_flushed_macro_block_(false), block_file_(nullptr) {}
  bool is_valid() const { return has_flushed_macro_block_ || nullptr != block_file_; }
  TO_STRING_KV(K(has_flushed_macro_block_), KP(block_file_));

public:
  bool has_flushed_macro_block_;
  ObCGBlockFile *block_file_;
};

class ObDDLSlice
{
public:
  ObDDLSlice();
  ~ObDDLSlice();
  int init(const ObTabletID &tablet_id, const int64_t slice_idx, const int64_t column_group_count);
  int push_chunk(ObChunk *&chunk_data);
  int pop_chunk(ObChunk *&chunk_data);
  bool is_inited() const { return is_inited_; }
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  int64_t get_slice_idx() const { return slice_idx_; }
  int64_t get_queue_size() const { return chunk_queue_.size(); }
  int set_remain_block(const int64_t cg_idx, ObCGBlockFile *block_file);
  int set_block_flushed(const int64_t cg_idx); // if the slice is empty, then the ddl slice should not be created
  int get_remain_block(const int64_t cg_idx, ObRemainCgBlock &remain_block);
  bool has_end_chunk() const { return has_end_chunk_; }
  TO_STRING_KV(K_(is_inited), K_(tablet_id), K_(slice_idx), K_(has_end_chunk), K(chunk_queue_.size()));

private:
  bool is_inited_;
  bool has_end_chunk_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  common::LightyQueue chunk_queue_;
  ObArray<ObRemainCgBlock> remain_cg_blocks_; // not support lob meta tablet and column replica for now
};

struct ObDDLTabletContext final
{
public:
  struct MergeCtx
  {
  public:
    MergeCtx() : ddl_kv_handles_(), mutex_(), fifo_(MTL_ID()), arena_(ObMemAttr(MTL_ID(), "ddl_tblt_prm")), slice_cg_sstables_()  {}
    ~MergeCtx();
  public:
    ObArray<ObDDLKVHandle> ddl_kv_handles_;
    lib::ObMutex mutex_;     // arena mutex, whic may be used in merge cg slice at the same time;
    ObFIFOAllocator fifo_;    // used for build index layer, thread safe
    ObArenaAllocator arena_;  // used for prepare task & create sstable;
    hash::ObHashMap<int64_t, ObArray<ObTableHandleV2>*> slice_cg_sstables_;

  };

public:

  ObDDLTabletContext();
  ~ObDDLTabletContext();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t ddl_thread_count,
      const int64_t snapshot_version,
      const ObDirectLoadType direct_load_type,
      const ObDDLTableSchema &ddl_table_schema,
      const int64_t ddl_task_id = 0);
  void reset();
  int update_max_lob_id(const int64_t lob_id);
  int64_t get_last_lob_id() const { return last_lob_id_; }
  int update_max_autoinc_val(const int64_t val);
  int64_t get_last_autoinc_val() const { return last_autoinc_val_; }
  int get_or_create_slice(const int64_t slice_idx, ObDDLSlice *&ddl_slice, bool &is_new_slice);
  int remove_slice(const int64_t slice_idx);
  int get_all_slices(ObIArray<ObDDLSlice *> &ddl_slices);
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(tablet_id), K_(tablet_param), K_(lob_meta_tablet_id), K_(lob_meta_tablet_param),
      K_(slice_count), K_(table_slice_offset), K_(last_lob_id), K_(last_autoinc_val), K(bucket_count_), K(slice_map_.size()), KP(macro_meta_store_mgr_));
private:
  int init_vector_index_context(
      const int64_t snapshot_version,
      const int64_t ddl_task_id,
      const ObDDLTableSchema &ddl_table_schema);
public:
  bool is_inited_;
  ObArenaAllocator arena_;

  // merge_ctx
  MergeCtx merge_ctx_;
  MergeCtx lob_merge_ctx_;

  // const param
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObWriteTabletParam tablet_param_;
  ObTabletID lob_meta_tablet_id_;
  ObWriteTabletParam lob_meta_tablet_param_;
  int64_t slice_count_;
  int64_t table_slice_offset_; // for table level autoinc column
  ObDDLWriteStat write_stat_;
  ObDDLWriteStat lob_write_stat_;

  ObDDLTabletScanTask *scan_task_;

private:
  // runtime context
  lib::ObMutex mutex_;
  int64_t last_lob_id_; // record max lob id for future dml
  int64_t last_autoinc_val_; // record last autoinc val for future dml
  ObBucketLock bucket_lock_;
  int64_t bucket_count_;
  typedef hash::ObHashMap<int64_t, ObDDLSlice *, hash::NoPthreadDefendMode> SLICE_MAP;
  SLICE_MAP slice_map_;

public:
  ObMacroMetaStoreManager *macro_meta_store_mgr_;
  ObVectorIndexTabletContext *vector_index_ctx_;
};
}  // end namespace storage
}  // end namespace oceanbase
#endif//_OCEANBASE_STORAGE_DDL_OB_DDL_TABLET_CONTEXT_H_
