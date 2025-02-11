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

#include "ob_index_block_builder.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "share/compaction/ob_shared_storage_compaction_util.h"
#endif

namespace oceanbase
{
ERRSIM_POINT_DEF(EN_COMPACTION_DISABLE_SHARED_MACRO);
ERRSIM_POINT_DEF(EN_SSTABLE_SINGLE_ROOT_TREE);
ERRSIM_POINT_DEF(EN_SSTABLE_META_IN_TAIL);
using namespace common;
using namespace storage;
using namespace compaction;
namespace blocksstable
{

ObIndexTreeRootCtx::ObIndexTreeRootCtx()
    : allocator_(nullptr),
      last_key_(),
      task_idx_(-1),
      clustered_micro_info_array_(nullptr),
      clustered_index_write_ctx_(nullptr),
      absolute_offsets_(nullptr),
      use_old_macro_block_count_(0),
      meta_block_offset_(0),
      meta_block_size_(0),
      last_macro_size_(0),
      index_tree_info_(),
      meta_block_info_(),
      data_write_ctx_(nullptr),
      task_type_(ObIndexBuildTaskType::IDX_BLK_BUILD_MAX_TYPE),
      data_blocks_info_(nullptr),
      is_inited_(false)
{
}

ObIndexTreeRootCtx::~ObIndexTreeRootCtx() { reset(); }

void ObIndexTreeRootCtx::reset()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    if (OB_NOT_NULL(data_write_ctx_)) {
      data_write_ctx_->~ObMacroBlocksWriteCtx();
      allocator_->free(data_write_ctx_);
      data_write_ctx_ = nullptr;
    }
    if (OB_NOT_NULL(data_blocks_info_)) {
      data_blocks_info_->~ObDataBlockInfo();
      allocator_->free(data_blocks_info_);
      data_blocks_info_ = nullptr;
    }
    // reset clustered micro info arrays
    clustered_micro_info_array_->~ObIArray<ObClusteredIndexBlockMicroInfos>();
    allocator_->free(static_cast<void*>(clustered_micro_info_array_));
    clustered_micro_info_array_ = nullptr;
    // reset clustered index write ctx
    if (OB_NOT_NULL(clustered_index_write_ctx_)) {
      clustered_index_write_ctx_->~ObMacroBlocksWriteCtx();
      allocator_->free(clustered_index_write_ctx_);
      clustered_index_write_ctx_ = nullptr;
    }
    // reset absolute offset arrays
    if (nullptr != absolute_offsets_) {
      absolute_offsets_->~ObIArray<int64_t>();
      allocator_->free(static_cast<void *>(absolute_offsets_));
      absolute_offsets_ = nullptr;
    }
    allocator_ = nullptr;
    is_inited_ = false;
  }
  last_key_.reset();
  task_idx_ = -1;
  meta_block_offset_ = 0;
  meta_block_size_ = 0;
  last_macro_size_ = 0;
  index_tree_info_.reset();
  meta_block_info_.reset();
  task_type_ = ObIndexBuildTaskType::IDX_BLK_BUILD_MAX_TYPE;
}

int ObIndexTreeRootCtx::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  void *clustered_index_array_buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "index tree root ctx inited twice", K(ret));
  } else if (OB_ISNULL(clustered_index_array_buf = allocator.alloc(
                           sizeof(ObClusteredMicroInfosArray)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory for clustered micro info array", K(ret));
  } else if (OB_ISNULL(clustered_micro_info_array_ = (new (clustered_index_array_buf)
                                ObClusteredMicroInfosArray(
                                    sizeof(ObClusteredIndexBlockMicroInfos),
                                    ModulePageAllocator(allocator))))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new clustered micro info array", K(ret));
  } else {
    allocator_ = &allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObIndexTreeRootCtx::add_absolute_row_offset(const int64_t absolute_row_offset)
{
  int ret = OB_SUCCESS;
  void *array_buf = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "index tree root ctx not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(absolute_row_offset < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected row offset", K(ret), K(absolute_row_offset));
  } else if (OB_ISNULL(absolute_offsets_)) {
    if (OB_ISNULL(array_buf =
                      allocator_->alloc(sizeof(ObAbsoluteOffsetArray)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
    } else if (OB_ISNULL(
                   absolute_offsets_ = new (array_buf) ObAbsoluteOffsetArray(
                       sizeof(int64_t), ModulePageAllocator(*allocator_)))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to new ObMacroMetasArray", K(ret));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(array_buf)) {
      allocator_->free(array_buf);
      array_buf = nullptr;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!absolute_offsets_->empty() &&
      OB_UNLIKELY(absolute_row_offset <= absolute_offsets_->at(absolute_offsets_->count() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected absolute offset", K(absolute_row_offset), KPC(absolute_offsets_));
  } else if (OB_FAIL(absolute_offsets_->push_back(absolute_row_offset))) {
    STORAGE_LOG(WARN, "fail to push back absolute row offset", K(ret),
                K(absolute_row_offset));
  }

  return ret;
}

int ObIndexTreeRootCtx::add_clustered_index_block_micro_infos(
    const MacroBlockId &macro_id,
    const int64_t block_offset,
    const int64_t block_size,
    const ObLogicMicroBlockId &logic_micro_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("index tree root ctx not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(clustered_micro_info_array_->push_back(
                 ObClusteredIndexBlockMicroInfos(macro_id, block_offset,
                                                 block_size, logic_micro_id)))) {
    LOG_WARN("fail to push back clustered micro info", K(ret));
  } else {
    LOG_DEBUG("add clustered index block micro infos", K(ret), K(macro_id),
              K(block_offset), K(block_size), K(logic_micro_id));
  }
  return ret;
}

bool ObIndexTreeRootBlockDesc::is_valid() const
{
  return is_empty() ||
         (addr_.is_valid() &&
          (is_mem_type() ? (buf_ != nullptr) : (buf_ == nullptr)) &&
          height_ > 0);
}

void ObIndexTreeRootBlockDesc::set_empty()
{
  addr_.set_none_addr();
  buf_ = nullptr;
  height_ = 0;
  is_meta_root_ = false;
}

void ObIndexTreeInfo::set_empty()
{
  root_desc_.set_empty();
  row_count_ = 0;
  max_merged_trans_version_ = 0;
  contain_uncommitted_row_ = false;
}

ObSSTableMergeRes::ObSSTableMergeRes()
  : root_desc_(),
    data_root_desc_(),
    data_block_ids_(),
    other_block_ids_(),
    index_blocks_cnt_(0),
    data_blocks_cnt_(0),
    micro_block_cnt_(0),
    data_column_cnt_(0),
    row_count_(0),
    max_merged_trans_version_(0),
    contain_uncommitted_row_(false),
    occupy_size_(0),
    original_size_(0),
    data_checksum_(0),
    use_old_macro_block_count_(0),
    data_column_checksums_(),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    encrypt_id_(0),
    master_key_id_(0),
    nested_offset_(0),
    nested_size_(0),
    table_backup_flag_(),
    root_row_store_type_(ObRowStoreType::MAX_ROW_STORE),
    root_macro_seq_(0)
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  table_backup_flag_.clear();
  if (share::is_reserve_mode()) {
    ObMemAttr attr(MTL_ID(), "SSTMrgeResArr", ObCtxIds::MERGE_RESERVE_CTX_ID);
    data_block_ids_.set_attr(attr);
    other_block_ids_.set_attr(attr);
    data_column_checksums_.set_attr(attr);
  }
}

ObSSTableMergeRes::~ObSSTableMergeRes() { reset(); }

void ObSSTableMergeRes::reset()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < data_block_ids_.count(); ++i) {
    const MacroBlockId &block_id = data_block_ids_.at(i);
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(block_id))) {
      // overwrite ret
      STORAGE_LOG(ERROR, "failed to dec macro block ref cnt", K(ret),
                  "macro id", block_id);
    }
  }
  data_block_ids_.destroy();
  for (int64_t i = 0; i < other_block_ids_.count(); ++i) {
    const MacroBlockId &block_id = other_block_ids_.at(i);
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(block_id))) {
      // overwrite ret
      STORAGE_LOG(ERROR, "failed to dec macro block ref cnt", K(ret),
                  "macro id", block_id);
    }
  }
  other_block_ids_.destroy();
  index_blocks_cnt_ = 0;
  data_blocks_cnt_ = 0;
  micro_block_cnt_ = 0;
  data_column_cnt_ = 0;
  row_count_ = 0;
  max_merged_trans_version_ = 0;
  contain_uncommitted_row_ = false;
  occupy_size_ = 0;
  original_size_ = 0;
  data_checksum_ = 0;
  use_old_macro_block_count_ = 0;
  data_column_checksums_.reset();
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  encrypt_id_ = 0;
  master_key_id_ = 0;
  nested_offset_ = 0;
  nested_size_ = 0;
  table_backup_flag_.clear();
  root_row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
  root_macro_seq_ = 0;
}

bool ObSSTableMergeRes::is_valid() const
{
  return root_desc_.is_valid()
      && data_root_desc_.is_valid()
      && index_blocks_cnt_ >= 0
      && data_blocks_cnt_ >= 0
      && micro_block_cnt_ >= 0
      && data_column_cnt_ > 0
      && nested_offset_ >= 0
      && nested_size_ >= 0
      && table_backup_flag_.is_valid()
      && root_row_store_type_ < ObRowStoreType::MAX_ROW_STORE
      && root_macro_seq_ >= 0;
}

int ObSSTableMergeRes::assign(const ObSSTableMergeRes &src)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(&src == this)) {
    // do nothing
  } else {
    reset(); // clear ref
    root_desc_ = src.root_desc_;
    data_root_desc_ = src.data_root_desc_;
    index_blocks_cnt_ = src.index_blocks_cnt_;
    data_blocks_cnt_ = src.data_blocks_cnt_;
    micro_block_cnt_ = src.micro_block_cnt_;
    data_column_cnt_ = src.data_column_cnt_;
    row_count_ = src.row_count_;
    max_merged_trans_version_ = src.max_merged_trans_version_;
    contain_uncommitted_row_ = src.contain_uncommitted_row_;
    table_backup_flag_ = src.table_backup_flag_;
    occupy_size_ = src.occupy_size_;
    original_size_ = src.original_size_;
    data_checksum_ = src.data_checksum_;
    use_old_macro_block_count_ = src.use_old_macro_block_count_;
    data_column_checksums_ = src.data_column_checksums_;
    compressor_type_ = src.compressor_type_;
    encrypt_id_ = src.encrypt_id_;
    master_key_id_ = src.master_key_id_;
    nested_size_ = src.nested_size_;
    nested_offset_ = src.nested_offset_;
    root_row_store_type_ = src.root_row_store_type_;
    root_macro_seq_ = src.root_macro_seq_;
    MEMCPY(encrypt_key_, src.encrypt_key_, sizeof(encrypt_key_));

    if (OB_FAIL(data_block_ids_.reserve(src.data_block_ids_.count()))) {
      STORAGE_LOG(WARN, "failed to reserve count", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.data_block_ids_.count();
           ++i) {
        const MacroBlockId &block_id = src.data_block_ids_.at(i);
        if (OB_FAIL(data_block_ids_.push_back(block_id))) {
          STORAGE_LOG(WARN, "failed to push back block id", K(ret));
        } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(block_id))) {
          data_block_ids_.pop_back(); // pop if inc_ref failed
          STORAGE_LOG(ERROR, "failed to inc macro block ref cnt", K(ret),
                      "macro id", block_id);
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                   other_block_ids_.reserve(src.other_block_ids_.count()))) {
      STORAGE_LOG(WARN, "failed to reserve count", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.other_block_ids_.count();
           ++i) {
        const MacroBlockId &block_id = src.other_block_ids_.at(i);
        if (OB_FAIL(other_block_ids_.push_back(block_id))) {
          STORAGE_LOG(WARN, "failed to push back block id", K(ret));
        } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(block_id))) {
          other_block_ids_.pop_back(); // pop if inc_ref failed
          STORAGE_LOG(ERROR, "failed to inc macro block ref cnt", K(ret),
                      "macro id", block_id);
        }
      }
    }
  }
  return ret;
}

int ObSSTableMergeRes::fill_column_checksum_for_empty_major(
    const int64_t column_count, common::ObIArray<int64_t> &column_checksums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(column_count));
  } else {
    const int64_t default_column_checksum = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
      if (OB_FAIL(column_checksums.push_back(default_column_checksum))) {
        STORAGE_LOG(WARN, "fail to push back column checksums", K(ret));
      }
    }
  }
  return ret;
}

void ObSSTableMergeRes::set_table_flag_with_macro_id_array()
{
  table_backup_flag_.set_no_backup();
  table_backup_flag_.set_no_local();
  for (int64_t i = 0; i < other_block_ids_.count(); ++i) {
    const MacroBlockId &block_id = other_block_ids_.at(i);
    if (block_id.is_local_id()) {
      table_backup_flag_.set_has_local();
    } else if (block_id.is_backup_id()) {
      table_backup_flag_.set_has_backup();
    }
    if (table_backup_flag_.has_backup() && table_backup_flag_.has_local()) {
      break;
    }
  }
  for (int64_t j = 0; j < data_block_ids_.count(); ++j) {
    const MacroBlockId &block_id = data_block_ids_.at(j);
    if (block_id.is_local_id()) {
      table_backup_flag_.set_has_local();
    } else if (block_id.is_backup_id()) {
      table_backup_flag_.set_has_backup();
    }
    if (table_backup_flag_.has_backup() && table_backup_flag_.has_local()) {
      break;
    }
  }
}

int ObSSTableMergeRes::prepare_column_checksum_array(const int64_t data_column_cnt)
{
  int ret = OB_SUCCESS;
  data_column_cnt_ = data_column_cnt;
  data_column_checksums_.reset();
  if (OB_UNLIKELY(0 == data_column_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected empty roots", K(ret));
  } else if (OB_FAIL(data_column_checksums_.reserve(data_column_cnt))) {
    STORAGE_LOG(WARN, "failed to reserve data_column_checksums_", K(ret),
                K(data_column_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_column_cnt; i++) {
      if (OB_FAIL(data_column_checksums_.push_back(0))) {
        STORAGE_LOG(WARN, "failed to push column checksum", K(ret));
      }
    }
  }
  return ret;
}

ObSSTableIndexBuilder::ObSSTableIndexBuilder(const bool use_double_write_buffer)
  : sstable_allocator_("SSTMidIdxData"),
    self_allocator_("SSTMidIdxSelf"),
    row_allocator_("SSTMidIdxRow"),
    mutex_(common::ObLatchIds::INDEX_BUILDER_LOCK),
    data_store_desc_(),
    index_store_desc_(),
    leaf_store_desc_(),
    container_store_desc_(),
    index_builder_(),
    meta_tree_builder_(),
    index_block_loader_(),
    macro_writer_(use_double_write_buffer),
    device_handle_(nullptr),
    roots_(sizeof(ObIndexTreeRootCtx *),
    ModulePageAllocator(sstable_allocator_)),
    res_(),
    object_cleaner_(),
    optimization_mode_(ENABLE),
    enable_dump_disk_(false),
    is_closed_(false),
    is_inited_(false)
{
}

ObSSTableIndexBuilder::~ObSSTableIndexBuilder() { reset(); }

void ObSSTableIndexBuilder::reset()
{
  data_store_desc_.reset();
  index_store_desc_.reset();
  leaf_store_desc_.reset();
  container_store_desc_.reset();
  for (int64_t i = 0; i < roots_.count(); ++i) {
    roots_[i]->~ObIndexTreeRootCtx();
    sstable_allocator_.free(static_cast<void *>(roots_[i]));
    roots_[i] = nullptr;
  }
  index_builder_.reset();
  meta_tree_builder_.reset();
  index_block_loader_.reset();
  macro_writer_.reset();
  device_handle_ = nullptr;
  roots_.reset();
  res_.reset();
  sstable_allocator_.reset();
  self_allocator_.reset();
  object_cleaner_.reset();
  optimization_mode_ = ENABLE;
  enable_dump_disk_ = false;
  is_closed_ = false;
  is_inited_ = false;
}

bool ObSSTableIndexBuilder::check_index_desc(const ObDataStoreDesc &index_desc) const
{
  bool ret = true;
  // these args influence write_micro_block and need to be evaluated
  if (!index_desc.is_valid() ||
      index_desc.get_row_column_count() !=
          index_desc.get_rowkey_column_count() + 1 ||
      (index_desc.is_major_merge_type() &&
       index_desc.get_major_working_cluster_version() < DATA_VERSION_4_0_0_0)) {
    ret = false;
  }
  return ret;
}

int ObSSTableIndexBuilder::init(const ObDataStoreDesc &data_desc,
                                ObSpaceOptimizationMode mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSSTableIndexBuilder has been inited", K(ret));
  } else if (OB_UNLIKELY(!data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid data store desc", K(ret), K(data_desc));
  } else if (OB_FAIL(data_store_desc_.assign(data_desc))) {
    STORAGE_LOG(WARN, "fail to assign data store desc", K(ret), K(data_desc));
  } else if (OB_FAIL(index_store_desc_.gen_index_store_desc(
                 data_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to generate index store desc", K(ret),
                K_(data_store_desc));
  } else if (OB_UNLIKELY(!check_index_desc(index_store_desc_.get_desc()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid index store desc", K(ret), K(index_store_desc_),
                K(index_store_desc_.is_valid()));
  } else if (OB_FAIL(set_row_store_type(index_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to set row store type", K(ret));
  } else if (OB_FAIL(container_store_desc_.shallow_copy(
                 index_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to assign container_store_desc", K(ret),
                K(index_store_desc_));
  } else {
    if (GCTX.is_shared_storage_mode()) {
      optimization_mode_ = DISABLE;
      enable_dump_disk_ = false;
    } else {
      optimization_mode_ = mode;
      enable_dump_disk_ = true;
    }
    index_store_desc_.get_desc().sstable_index_builder_ = this;
    index_store_desc_.get_desc().need_pre_warm_ = true;
    index_store_desc_.get_desc().need_build_hash_index_for_micro_block_ = false;
    container_store_desc_.need_build_hash_index_for_micro_block_ = false;
    if (OB_FAIL(leaf_store_desc_.shallow_copy(index_store_desc_.get_desc()))) {
      STORAGE_LOG(WARN, "fail to assign leaf store desc", K(ret));
    } else if (OB_UNLIKELY(!index_store_desc_.get_desc().is_for_index() ||
                           !container_store_desc_.is_for_index_or_meta() ||
                           !leaf_store_desc_.is_for_index())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to check all data store desc for index",
                  KR(ret), K_(index_store_desc), K_(container_store_desc),
                  K_(leaf_store_desc));
    } else {
      leaf_store_desc_.micro_block_size_ =
          leaf_store_desc_.get_micro_block_size_limit(); // nearly 2M
      is_inited_ = true;
    }
  }
  LOG_INFO("init sstable index builder", K(ret), K(data_desc),
           K(index_store_desc_), K(container_store_desc_), K(leaf_store_desc_),
           KP(&object_cleaner_));
  return ret;
}

int ObSSTableIndexBuilder::set_row_store_type(ObDataStoreDesc &index_desc)
{
  // Ban string related encoding methods.
  // TODO: support sting encodings
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!index_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid index descriptor", K(ret), K(index_desc));
  } else if (ENCODING_ROW_STORE == index_desc.get_row_store_type()) {
    index_desc.row_store_type_ = SELECTIVE_ENCODING_ROW_STORE;
    index_desc.encoder_opt_.set_store_type(SELECTIVE_ENCODING_ROW_STORE);
  }
  return ret;
}

int ObSSTableIndexBuilder::new_index_builder(ObDataIndexBlockBuilder *&builder,
                                             const ObDataStoreDesc &data_store_desc,
                                             ObIAllocator &data_allocator,
                                             const blocksstable::ObMacroSeqParam &macro_seq_param,
                                             const share::ObPreWarmerParam &pre_warm_param,
                                             ObIMacroBlockFlushCallback *ddl_callback)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr != builder)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid not null builder", K(ret), KP(builder));
  } else if (OB_ISNULL(
                 buf = data_allocator.alloc(sizeof(ObDataIndexBlockBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(builder = new (buf) ObDataIndexBlockBuilder())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObDataIndexBlockBuilder", K(ret));
  } else if (OB_FAIL(builder->init(data_store_desc, *this, macro_seq_param, pre_warm_param, ddl_callback))) {
    STORAGE_LOG(WARN, "fail to init index builder", K(ret));
  }
  return ret;
}

int ObSSTableIndexBuilder::init_builder_ptrs(
    ObSSTableIndexBuilder *&sstable_builder,
    ObDataStoreDesc *&data_store_desc,
    ObDataStoreDesc *&index_store_desc,
    ObDataStoreDesc *&leaf_store_desc,
    ObDataStoreDesc *&container_store_desc,
    ObIndexTreeRootCtx *&index_tree_root_ctx)
{
  int ret = OB_SUCCESS;
  void *desc_buf = nullptr;
  ObIndexTreeRootCtx *tmp_root_ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_ISNULL(desc_buf = sstable_allocator_.alloc(
                           sizeof(ObIndexTreeRootCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(tmp_root_ctx = new (desc_buf) ObIndexTreeRootCtx())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObIndexTreeRootCtx", K(ret));
  } else if (OB_FAIL(tmp_root_ctx->init(sstable_allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to init a ObIndexTreeRootCtx", K(ret));
  } else {
    sstable_builder = this;
    data_store_desc = &data_store_desc_.get_desc();
    index_store_desc = &index_store_desc_.get_desc();
    leaf_store_desc = &leaf_store_desc_;
    container_store_desc = &container_store_desc_;
    if (OB_FAIL(append_root(*tmp_root_ctx))) {
      STORAGE_LOG(WARN, "Fail to append index tree root ctx", K(ret), K(*tmp_root_ctx));
    } else {
      index_tree_root_ctx = tmp_root_ctx;
      tmp_root_ctx = nullptr;
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != tmp_root_ctx) {
      tmp_root_ctx->~ObIndexTreeRootCtx();
      sstable_allocator_.free(desc_buf);
      tmp_root_ctx = nullptr;
      desc_buf = nullptr;
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::append_root(ObIndexTreeRootCtx &index_tree_root_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(roots_.push_back(&index_tree_root_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to push into roots", K(ret));
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::trim_empty_roots()
{
  int ret = OB_SUCCESS;
  const int64_t root_count = roots_.count();
  IndexTreeRootCtxList tmp_roots;
  ObIndexBuildTaskType task_type = ObIndexBuildTaskType::IDX_BLK_BUILD_MAX_TYPE;
  if (0 == root_count) {
    // do nothing
  } else if (OB_FAIL(tmp_roots.reserve(root_count))) {
    STORAGE_LOG(WARN, "fail to reserve tmp roots", K(ret), K(root_count));
  } else {
    bool use_absolute_offset = false;
    for (int64_t i = 0; i < root_count && OB_SUCC(ret); ++i) {
      if (nullptr == roots_[i]) {
        // skip
      } else if (0 == roots_[i]->get_data_block_cnt()) {
        roots_[i]->~ObIndexTreeRootCtx();  // release
      } else if (!roots_[i]->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected index tree root ctx", K(ret), KPC(roots_[i]));
      } else if (!enable_dump_disk_ &&
                 !(roots_[i]->meta_block_info_.in_array() &&
                   roots_[i]->index_tree_info_.empty())) {
        // if disable, meta_block_info must be in_array and index_tree_info must be empty
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected dump state", K(ret), K(enable_dump_disk_), KPC(roots_[i]));
      } else {
        if (FAILEDx(tmp_roots.push_back(roots_[i]))) {
          STORAGE_LOG(WARN, "fail to push back root", K(ret), KPC(roots_[i]));
        } else if (tmp_roots.count() == 1) {
          task_type = tmp_roots.at(0)->task_type_;
        } else if (OB_UNLIKELY((roots_[i]->task_type_ != task_type))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected task type", K(ret), K(task_type), KPC(roots_[i]));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (root_count == tmp_roots.count()) {
      // root and tmp root is the same
    } else {
      roots_.reset();
      if (OB_FAIL(roots_.assign(tmp_roots))) {
        STORAGE_LOG(WARN, "fail to copy from tmp roots", K(ret));
        for (int64_t i = 0; i < tmp_roots.count(); ++i) {
          tmp_roots[i]->~ObIndexTreeRootCtx(); // release
        }
      }
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::ObMacroMetaIter::init(
    common::ObIAllocator &allocator,
    IndexTreeRootCtxList &roots,
    const int64_t meta_row_column_count,
    const bool is_cg,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "Init twice", K(ret));
  } else if (OB_FAIL(index_block_loader_.init(allocator, data_version))) {
    STORAGE_LOG(WARN, "fail to init index block loader", K(ret), K(data_version));
  } else if (OB_FAIL(meta_row_.init(allocator, meta_row_column_count))) {
    STORAGE_LOG(WARN, "fail to init meta row", K(ret), K(meta_row_column_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < roots.count(); ++i) {
      block_cnt_ += roots[i]->get_data_block_cnt();
    }
  }
  if (OB_SUCC(ret)) {
    roots_ = &roots;
    row_idx_ = -1;
    cur_roots_idx_ = -1;
    is_cg_ = is_cg;
    is_inited_ = true;
  }
  return ret;
}

void ObSSTableIndexBuilder::ObMacroMetaIter::reuse()
{
  row_idx_ = -1;
  cur_roots_idx_ = -1;
  index_block_loader_.reuse();
}

int ObSSTableIndexBuilder::ObMacroMetaIter::get_next_macro_block(ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret));
  } else if (-1 == cur_roots_idx_) {
    if (0 == roots_->count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(index_block_loader_.open(roots_->at(++cur_roots_idx_)->meta_block_info_))) {
      STORAGE_LOG(WARN, "fail to open macro meta loader", K(ret), K(roots_->at(cur_roots_idx_)->meta_block_info_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(index_block_loader_.get_next_row(meta_row_))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      STORAGE_LOG(WARN, "fail to get row", K(ret),
                  K(cur_roots_idx_), K(block_cnt_), K(index_block_loader_));
    } else if (cur_roots_idx_ == roots_->count() - 1) {
      // iter end
    } else if (FALSE_IT(index_block_loader_.reuse())) {
    } else if (FALSE_IT(cur_roots_idx_++)) {
    } else if (OB_FAIL(index_block_loader_.open(roots_->at(cur_roots_idx_)->meta_block_info_))) {
      STORAGE_LOG(WARN, "fail to open macro meta loader", K(ret),
          K(cur_roots_idx_), K(roots_->at(cur_roots_idx_)->meta_block_info_));
    } else if (OB_FAIL(index_block_loader_.get_next_row(meta_row_))) {
      STORAGE_LOG(WARN, "fail to get row", K(ret), K(block_cnt_), K(index_block_loader_));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(macro_meta.parse_row(meta_row_))) {
      STORAGE_LOG(WARN, "fail to parse row", K(ret), K(meta_row_));
    } else if (is_cg_) {
      macro_meta.end_key_.datums_[0].set_int(macro_meta.val_.row_count_ + row_idx_);
      row_idx_ += macro_meta.val_.row_count_;
    }
  }

  return ret;
}

int ObSSTableIndexBuilder::init_meta_iter(common::ObIAllocator &allocator, ObMacroMetaIter &iter)
{
  int ret = OB_SUCCESS;
  const uint64_t data_version = ObBaseIndexBlockBuilder::get_data_version(data_store_desc_.get_desc());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_FAIL(trim_empty_roots())) {
    STORAGE_LOG(WARN, "fail to trim empty roots", K(ret));
  } else if (OB_FAIL(sort_roots())) {
    STORAGE_LOG(WARN, "fail to sort roots", K(ret));
  } else if (OB_FAIL(iter.init(allocator,
                               roots_,
                               container_store_desc_.get_row_column_count(),
                               index_store_desc_.get_desc().is_cg(),
                               data_version))) {
    STORAGE_LOG(WARN, "fail to init iter", K(ret), K(data_version));
  }
  return ret;
}

int ObSSTableIndexBuilder::sort_roots()
{
  int ret = OB_SUCCESS;
  if (index_store_desc_.get_desc().is_cg()) {
    ObIndexTreeRootCtxCGCompare cmp(ret);
    lib::ob_sort(roots_.begin(), roots_.end(), cmp);
  } else {
    ObIndexTreeRootCtxCompare cmp(
        ret, index_store_desc_.get_desc().get_datum_utils());
    lib::ob_sort(roots_.begin(), roots_.end(), cmp);
  }
  return ret;
}

int ObSSTableIndexBuilder::get_clustered_micro_info(
    const int64_t roots_idx,
    const int64_t macro_meta_idx,
    ObClusteredIndexBlockMicroInfos *&clustered_micro_info) const
{
  int ret = OB_SUCCESS;
  if (!micro_index_clustered()) {
    clustered_micro_info = nullptr;
  } else {
    // Clustered index tree.
    ObClusteredMicroInfosArray *clustered_micro_info_array = roots_[roots_idx]->clustered_micro_info_array_;
    if (OB_ISNULL(clustered_micro_info_array)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected clustered micro info array", K(ret), K(roots_idx), KPC(roots_[roots_idx]));
    } else if (OB_UNLIKELY(clustered_micro_info_array->count() != roots_[roots_idx]->meta_block_info_.get_row_count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected clustered micro info array", K(ret),
                  K(clustered_micro_info_array->count()),
                  "macro_metas_count", roots_[roots_idx]->meta_block_info_.get_row_count());
    } else {
      clustered_micro_info = &clustered_micro_info_array->at(macro_meta_idx);
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::merge_index_tree(
    const share::ObPreWarmerParam &pre_warm_param,
    ObSSTableMergeRes &res,
    int64_t &macro_seq,
    ObIMacroBlockFlushCallback *callback)
{
  int ret = OB_SUCCESS;
  ObMacroSeqParam macro_seq_param;
  macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  macro_seq_param.start_ = macro_seq;
  container_store_desc_.data_store_type_ = ObMacroBlockCommonHeader::SSTableIndex;
  ObIndexTreeInfo tree_info;
  bool all_in_mem = true;
  const ObIndexBuildTaskType task_type = roots_[0]->task_type_;
  const bool in_array = roots_[0]->meta_block_info_.in_array();
  if (OB_FAIL(macro_writer_.open(container_store_desc_, 0 /*parallel_idx*/,
                                 macro_seq_param, pre_warm_param, object_cleaner_, callback,
                                 nullptr, device_handle_))) {
    STORAGE_LOG(WARN, "fail to open index macro writer", K(ret));
  } else {
    switch (task_type) {
      case MERGE_TASK:
      case MERGE_CG_TASK:
      case REBUILD_NORMAL_TASK:
      case REBUILD_CG_SELF_CAL_TASK:
      case REBUILD_DDL_TASK:
        if (OB_FAIL(index_builder_.init(
            data_store_desc_.get_desc(), index_store_desc_.get_desc(), self_allocator_, &macro_writer_, 1))) {
          STORAGE_LOG(WARN, "fail to new index builder", K(ret));
        } else if (OB_FAIL(merge_index_tree_from_meta_block(res))) {
          STORAGE_LOG(WARN, "fail to inner merge index tree", K(ret), K(res));
        }
        break;
      case REBUILD_BACKUP_TASK:
      case REBUILD_BACKUP_DDL_TASK:
        for (int64_t i = 0; i < roots_.count(); ++i) {
          all_in_mem &= !roots_[i]->index_tree_info_.in_disk();
        }
        if (in_array) {
          if (OB_FAIL(index_builder_.init(
              data_store_desc_.get_desc(), index_store_desc_.get_desc(), self_allocator_, &macro_writer_, 1))) {
            STORAGE_LOG(WARN, "fail to new index builder", K(ret));
          } else if (OB_FAIL(merge_index_tree_from_meta_block(res))) {
            STORAGE_LOG(WARN, "fail to inner merge index tree", K(ret), K(res));
          }
        } else if (all_in_mem) {
          if (OB_FAIL(index_builder_.init(
              data_store_desc_.get_desc(), index_store_desc_.get_desc(), self_allocator_, &macro_writer_, 1))) {
            STORAGE_LOG(WARN, "fail to new index builder", K(ret));
          } else if (OB_FAIL(merge_index_tree_from_all_mem_index_block(res))) {
            STORAGE_LOG(WARN, "fail to merge index tree from all mem index block", K(ret), K(res));
          }
        } else {
          if (OB_FAIL(index_builder_.init(
              data_store_desc_.get_desc(), index_store_desc_.get_desc(), self_allocator_, &macro_writer_, 2))) {
            STORAGE_LOG(WARN, "fail to new index builder", K(ret));
          } else if (OB_FAIL(merge_index_tree_from_index_row(res))) {
            STORAGE_LOG(WARN, "fail to inner merge index tree", K(ret), K(res));
          }
        }

        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "the task type is invalid", K(ret), K(task_type));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(index_builder_.close(self_allocator_, tree_info))) {
    STORAGE_LOG(WARN, "fail to close merged index tree", K(ret));
  } else if (OB_FAIL(macro_writer_.close())) {
    STORAGE_LOG(WARN, "fail to close container macro writer", K(ret));
  } else if (OB_FAIL(macro_writer_.get_macro_block_write_ctx().get_macro_id_array(res.other_block_ids_))) {
    STORAGE_LOG(WARN, "fail to get macro ids of index blocks", K(ret));
  } else {
    macro_seq = macro_writer_.get_last_macro_seq();
    res.index_blocks_cnt_ += macro_writer_.get_macro_block_write_ctx().macro_block_list_.count();
    res.root_desc_ = tree_info.root_desc_;
    res.row_count_ = tree_info.row_count_;
    res.max_merged_trans_version_ = tree_info.max_merged_trans_version_;
    res.contain_uncommitted_row_ = tree_info.contain_uncommitted_row_;
    index_builder_.reset();
  }
  return ret;
}

int ObSSTableIndexBuilder::merge_index_tree_from_meta_block(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObDatumRow meta_row;
  ObDataMacroBlockMeta macro_meta;
  const bool use_absolute_offset = roots_[0]->use_absolute_offset();
  if (OB_FAIL(meta_row.init(self_allocator_, container_store_desc_.get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init", K(ret), K(container_store_desc_.get_row_column_count()));
  } else if (OB_FAIL(data_desc.assign(index_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to assign data desc", K(ret), K_(index_store_desc));
  } else {
    const int64_t curr_logical_version =
        index_store_desc_.get_desc().get_logical_version();
    ObIndexBlockRowDesc row_desc(data_desc.get_desc());
    int64_t row_idx = -1;
    ObLogicMacroBlockId prev_logic_id;
    const bool need_rewrite = index_store_desc_.get_desc().is_cg();
    for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
      index_block_loader_.reuse();
      ObMacroBlocksWriteCtx *write_ctx = roots_[i]->data_write_ctx_;
      const ObIndexBlockInfo& meta_block_info = roots_[i]->meta_block_info_;
      int64_t meta_row_count = meta_block_info.get_row_count();
      if (OB_FAIL(index_block_loader_.open(meta_block_info))) {
        STORAGE_LOG(WARN, "fail to open macro meta loader", K(ret), K(meta_block_info));
      } else {
        int64_t iter_idx = 0;
        ObClusteredIndexBlockMicroInfos *clustered_micro_info = nullptr;
        while (OB_SUCC(ret)) {
          int64_t absolute_row_offset = -1;
          meta_row.reuse();
          macro_meta.reset();
          if (OB_FAIL(index_block_loader_.get_next_row(meta_row))) {
            if (OB_UNLIKELY(ret != OB_ITER_END)) {
              STORAGE_LOG(WARN, "fail to get row", K(ret),
                  K(iter_idx), K(meta_row_count), K(index_block_loader_));
            }
          } else if (OB_FAIL(macro_meta.parse_row(meta_row))) {
            STORAGE_LOG(WARN, "fail to parse row", K(ret), K(meta_row));
          } else if (OB_UNLIKELY(macro_meta.get_logic_id() == prev_logic_id)) {
            // Since we rely on upper stream of sstable writing process to ensure the uniqueness of logic id
            // and we don't want more additional memory/time consumption, we only check continuous ids here
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "unexpected duplicate logic macro id", K(ret), K(macro_meta), K(prev_logic_id));
          } else if (use_absolute_offset) {
            absolute_row_offset = roots_[i]->absolute_offsets_->at(iter_idx);
          } else {
            absolute_row_offset = macro_meta.val_.row_count_ + row_idx;
          }

          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(absolute_row_offset < 0)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected absolute row offset", K(ret), K(use_absolute_offset), K(row_idx), K(macro_meta));
          } else if (need_rewrite && FALSE_IT(macro_meta.end_key_.datums_[0].set_int(absolute_row_offset))) {
          } else if (FALSE_IT(row_desc.row_offset_ = absolute_row_offset)) {
          } else if (OB_FAIL(get_clustered_micro_info(i, iter_idx, clustered_micro_info))) {
            LOG_WARN("fail to get clustered micro info", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
          } else if (OB_FAIL(index_builder_.append_row(macro_meta, clustered_micro_info, row_desc))) {
            STORAGE_LOG(WARN, "fail to append row", K(ret), K(i), K(iter_idx),
                K(macro_meta), KPC(clustered_micro_info), KPC(roots_[i]));
          } else if (OB_FAIL(collect_data_blocks_info(macro_meta, res))) {
            STORAGE_LOG(WARN, "fail to collect data blocks info", K(ret), K(macro_meta));
          } else if (FALSE_IT(prev_logic_id = macro_meta.get_logic_id())) {
          } else if (FALSE_IT(iter_idx++)) {
          } else {
            row_idx += macro_meta.val_.row_count_;
          }
        }
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          STORAGE_LOG(WARN, "unexpected ret", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else if (OB_UNLIKELY(iter_idx != meta_row_count)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected iter idx", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else {
          ret = OB_SUCCESS;
        }

        // TODO(baichangmin): encapsulate here, transfer_macro_block_ref_cnt()?
        // Transfer macro block and refcnt from clustered index block writer.
        if (OB_FAIL(ret)) {
          // do nothing.
        } else if (micro_index_clustered()) {
          ObMacroBlocksWriteCtx *clustered_index_write_ctx = roots_[i]->clustered_index_write_ctx_;
          if (OB_UNLIKELY(OB_ISNULL(clustered_index_write_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected clustered index write ctx", K(ret), KP(clustered_index_write_ctx));
          } else if (FALSE_IT(res.index_blocks_cnt_ += clustered_index_write_ctx->get_macro_block_count())) {
          } else if (OB_FAIL(clustered_index_write_ctx->get_macro_id_array(res.other_block_ids_))) {
            STORAGE_LOG(WARN, "fail to get macro ids from clustered index block writer",
                K(ret), K(res.other_block_ids_), KPC(clustered_index_write_ctx));
          }
        }

      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(write_ctx->get_macro_id_array(res.data_block_ids_))) {
          STORAGE_LOG(WARN, "fail to get macro ids of data blocks", K(ret));
        } else {
          res.use_old_macro_block_count_ += write_ctx->use_old_macro_block_count_;
        }
      }
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::merge_index_tree_from_all_mem_index_block(ObSSTableMergeRes &res)
{
  // mem_index_block save macro meta to row_desc
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObDatumRow index_row;
  if (OB_FAIL(index_row.init(self_allocator_, container_store_desc_.get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init", K(ret), K(container_store_desc_.get_row_column_count()));
  } else if (OB_FAIL(data_desc.assign(index_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to assign data desc", K(ret), K_(index_store_desc));
  } else {
    const int64_t rowkey_column_count = index_store_desc_.get_desc().get_rowkey_column_count();
    ObIndexBlockRowParser row_parser;
    const ObIndexBlockRowMinorMetaInfo *index_row_meta = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
      index_block_loader_.reuse();
      ObMacroBlocksWriteCtx *write_ctx = roots_[i]->data_write_ctx_;
      const ObIndexBlockInfo &index_block_info = roots_[i]->index_tree_info_;
      int index_row_count = index_block_info.get_row_count();
      if (OB_UNLIKELY(!index_block_info.in_mem())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected index_block_info", K(ret), K(index_block_info), KPC(roots_[i]));
      } else if (OB_FAIL(index_block_loader_.open(index_block_info))) {
        STORAGE_LOG(WARN, "fail to open macro meta loader", K(ret), K(index_block_info));
      } else {
        int64_t iter_idx = 0;
        while (OB_SUCC(ret)) {
          row_parser.reset();
          index_row.reuse();
          ObIndexBlockRowDesc row_desc;
          if (OB_FAIL(index_block_loader_.get_next_row(index_row))) {
            if (OB_UNLIKELY(ret != OB_ITER_END)) {
              STORAGE_LOG(WARN, "fail to get row", K(ret),
                  K(iter_idx), K(index_row_count), K(index_block_loader_));
            }
          } else if (OB_FAIL(row_parser.init(rowkey_column_count, index_row))) {
            STORAGE_LOG(WARN, "fail to init row parser", K(ret), K(rowkey_column_count), K(index_row));
          } else if (OB_FAIL(row_desc.init(data_desc.get_desc(), row_parser, index_row))) {
            LOG_WARN("fail to init idx row desc", K(ret), K(data_desc.get_desc()), K(row_parser), K(index_row));
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(index_builder_.append_row(row_desc))) {
            STORAGE_LOG(WARN, "fail to append row", K(ret), K(iter_idx), K(row_desc), KPC(roots_.at(i)));
          } else {
            iter_idx++;
          }
        }

        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          STORAGE_LOG(WARN, "unexpected ret", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else if (OB_UNLIKELY(iter_idx != index_row_count)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected iter idx", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else if (OB_FAIL(write_ctx->get_macro_id_array(res.data_block_ids_))) {
          STORAGE_LOG(WARN, "fail to get macro ids of data blocks", K(ret),
              K(res.data_block_ids_), KPC(write_ctx));
        } else if (OB_FAIL(collect_data_blocks_info_from_root(*roots_[i], res))) {
          STORAGE_LOG(WARN, "fail to collect data info from root", K(ret), KPC(roots_[i]), K(res));
        } else {
          res.use_old_macro_block_count_ += write_ctx->use_old_macro_block_count_;
        }
      }
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::merge_index_tree_from_index_row(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
    ObMacroBlocksWriteCtx *write_ctx = roots_[i]->data_write_ctx_;
    const ObIndexBlockInfo &index_block_info = roots_[i]->index_tree_info_;
    if (OB_UNLIKELY(!index_block_info.not_need_load_block())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected index tree root ctx", K(ret), KPC(roots_[i]));
    } else if (index_block_info.in_mem()){
      index_block_loader_.reuse();
      // the last macro meta's absolute_offset is micro_block's index block row absolute offset
      const int64_t last_row_idx = index_block_info.get_row_count() - 1;
      const int64_t absolute_offset = roots_[i]->absolute_offsets_->at(last_row_idx);
      if (OB_FAIL(index_builder_.append_micro_block(*index_block_info.agg_info_,
                                                    absolute_offset,
                                                    *index_block_info.micro_block_desc_))) {
        STORAGE_LOG(WARN, "fail to append micro block", K(ret), K(last_row_idx), K(absolute_offset));
      }
    } else if (index_block_info.in_disk()) {
      int64_t index_row_count = index_block_info.next_level_rows_list_->count();
      for (int64_t j = 0; OB_SUCC(ret) && j < index_row_count; j++) {
        if (OB_FAIL(index_builder_.append_row(*index_block_info.next_level_rows_list_->at(j)))) {
          STORAGE_LOG(WARN, "fail to append row", K(ret),
              K(j), KPC(index_block_info.next_level_rows_list_->at(j)), KPC(roots_.at(i)));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(write_ctx->get_macro_id_array(res.data_block_ids_))) {
        STORAGE_LOG(WARN, "fail to get macro ids of data blocks", K(ret));
      } else if (OB_FAIL(collect_data_blocks_info_from_root(*roots_[i], res))) {
        STORAGE_LOG(WARN, "fail to collect data info from root", K(ret), KPC(roots_[i]), K(res));
      } else {
        res.use_old_macro_block_count_ += write_ctx->use_old_macro_block_count_;
      }
    }
  }
  return ret;
}


int ObSSTableIndexBuilder::build_cg_meta_tree()
{
  // cg need rewrite
  int ret = OB_SUCCESS;
  ObDatumRow meta_row;
  ObDatumRow cg_meta_row;
  ObDataMacroBlockMeta macro_meta;
  const uint64_t data_version = ObBaseIndexBlockBuilder::get_data_version(data_store_desc_.get_desc());
  if (OB_FAIL(meta_row.init(self_allocator_, container_store_desc_.get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init meta row", K(ret), K(container_store_desc_.get_row_column_count()));
  } else if (OB_FAIL(cg_meta_row.init(self_allocator_, container_store_desc_.get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init cg meta row", K(ret), K(container_store_desc_.get_row_column_count()));
  } else {
    int64_t row_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
      index_block_loader_.reuse();
      if (OB_FAIL(index_block_loader_.open(roots_[i]->meta_block_info_))) {
        STORAGE_LOG(WARN, "fail to open macro meta loader", K(ret), K(roots_[i]->meta_block_info_));
      } else {
        int64_t meta_row_count = roots_[i]->meta_block_info_.get_row_count();
        int64_t iter_idx = 0;
        while (OB_SUCC(ret)) {
          meta_row.reuse();
          cg_meta_row.reuse();
          row_allocator_.reuse();
          if (OB_FAIL(index_block_loader_.get_next_row(meta_row))) {
            if (OB_UNLIKELY(ret != OB_ITER_END)) {
              STORAGE_LOG(WARN, "fail to get row", K(ret),
                  K(iter_idx), K(meta_row_count), K(index_block_loader_));
            }
          } else if (OB_FAIL(macro_meta.parse_row(meta_row))) {
            STORAGE_LOG(WARN, "fail to parse row", K(ret), K(meta_row));
          } else if (FALSE_IT(macro_meta.end_key_.datums_[0].set_int(macro_meta.val_.row_count_ + row_idx))) {
          } else if (OB_FAIL(macro_meta.build_row(cg_meta_row, row_allocator_, data_version))) {
            STORAGE_LOG(WARN, "fail to build row", K(ret), K(macro_meta), K(data_version));
          } else if (OB_FAIL(meta_tree_builder_.append_leaf_row(cg_meta_row))) {
            STORAGE_LOG(WARN, "fail to append leaf row", K(ret), K(cg_meta_row));
          } else {
            row_idx += macro_meta.val_.row_count_;
            iter_idx++;
          }
        }
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          STORAGE_LOG(WARN, "unexpected ret", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else if (OB_UNLIKELY(iter_idx != meta_row_count)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected iter idx", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}


int ObSSTableIndexBuilder::build_meta_tree_from_all_mem_meta_block()
{
  int ret = OB_SUCCESS;
  ObDatumRow meta_row;
  if (OB_FAIL(meta_row.init(self_allocator_, container_store_desc_.get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init meta row", K(ret), K(container_store_desc_.get_row_column_count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
      index_block_loader_.reuse();
      if (OB_UNLIKELY(!(roots_[i]->meta_block_info_.in_mem() || roots_[i]->meta_block_info_.in_array()))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected index tree root ctx", K(ret), KPC(roots_[i]));
      } else if (OB_FAIL(index_block_loader_.open(roots_[i]->meta_block_info_))) {
        STORAGE_LOG(WARN, "fail to open macro meta loader", K(ret), K(roots_[i]->meta_block_info_));
      } else {
        int64_t meta_row_count = roots_[i]->meta_block_info_.get_row_count();
        int64_t iter_idx = 0;
        while (OB_SUCC(ret)) {
          meta_row.reuse();
          if (OB_FAIL(index_block_loader_.get_next_row(meta_row))) {
            if (OB_UNLIKELY(ret != OB_ITER_END)) {
              STORAGE_LOG(WARN, "fail to get row", K(ret),
                  K(iter_idx), K(meta_row_count), K(index_block_loader_));
            }
          } else if (OB_FAIL(meta_tree_builder_.append_leaf_row(meta_row))) {
            STORAGE_LOG(WARN, "fail to append leaf row", K(ret), K(meta_row));
          } else {
            iter_idx++;
          }
        }
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          STORAGE_LOG(WARN, "unexpected ret", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else if (OB_UNLIKELY(iter_idx != meta_row_count)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected iter idx", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::build_meta_tree_from_meta_block(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDesc load_micro_block_desc;
  MacroBlockId macro_block_id;
  for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
    if (roots_[i]->meta_block_info_.in_mem()) {
      if (OB_FAIL(meta_tree_builder_.append_micro_block(*roots_[i]->meta_block_info_.micro_block_desc_))) {
        STORAGE_LOG(WARN, "fail to append mem micro block", K(ret), K(i), K(roots_[i]->meta_block_info_));
      }
    } else if (roots_[i]->meta_block_info_.in_disk()) {
      index_block_loader_.reuse();
      if (OB_FAIL(index_block_loader_.open(roots_[i]->meta_block_info_))) {
        STORAGE_LOG(WARN, "fail to open macro meta loader", K(ret), K(roots_[i]->meta_block_info_));
      } else {
        int64_t meta_micro_block_cnt = roots_[i]->meta_block_info_.get_micro_block_count();
        int64_t iter_idx = 0;
        while (OB_SUCC(ret)) {
          load_micro_block_desc.reset();
          row_allocator_.reuse();
          if (OB_FAIL(index_block_loader_.get_next_micro_block_desc(load_micro_block_desc, container_store_desc_, row_allocator_))) {
            if (OB_UNLIKELY(ret != OB_ITER_END)) {
              STORAGE_LOG(WARN, "fail to get next micro block desc", K(ret),
                  K(iter_idx), K(meta_micro_block_cnt), K(index_block_loader_));
            }
          } else if (roots_[i]->meta_block_info_.need_rewrite_ && index_block_loader_.is_last()) {
            // rewrite
            if (OB_FAIL(meta_tree_builder_.append_micro_block(load_micro_block_desc))) {
              STORAGE_LOG(WARN, "fail to append disk rewrite micro block", K(ret));
            }
          } else {
            if (OB_FAIL(meta_tree_builder_.append_reuse_micro_block(load_micro_block_desc))) {
                STORAGE_LOG(WARN, "fail to append disk reuse micro block", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            iter_idx++;
          }
        }
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          STORAGE_LOG(WARN, "unexpected ret", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else if (OB_UNLIKELY(iter_idx != meta_micro_block_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected iter idx", K(ret), K(i), K(iter_idx), KPC(roots_[i]));
        } else {
          ret = OB_SUCCESS;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (roots_[i]->meta_block_info_.need_rewrite_ &&
          OB_FAIL(roots_[i]->meta_block_info_.block_write_ctx_->pop_macro_block_id(macro_block_id))) {
        STORAGE_LOG(WARN, "fail to pop macro id of rewrite block", K(ret), K(macro_block_id));
      } else if (OB_FAIL(roots_[i]->meta_block_info_.block_write_ctx_->get_macro_id_array(res.other_block_ids_))) {
        STORAGE_LOG(WARN, "fail to get macro ids of meta index blocks", K(ret));
      } else {
        res.index_blocks_cnt_ += roots_[i]->meta_block_info_.block_write_ctx_->macro_block_list_.count();
      }
    }
  }
  return ret;
}



int ObSSTableIndexBuilder::build_meta_tree_from_backup_meta_block(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDesc load_micro_block_desc;
  MacroBlockId macro_block_id;
  for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
    index_block_loader_.reuse();
    row_allocator_.reuse();
    const ObIndexBlockInfo &meta_block_info = roots_[i]->meta_block_info_;
    if (OB_UNLIKELY(!meta_block_info.not_need_load_block())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected index tree root ctx", K(ret), KPC(roots_[i]));
    } else if (meta_block_info.in_mem()) {
      if (OB_FAIL(meta_tree_builder_.append_micro_block(*meta_block_info.micro_block_desc_))) {
        STORAGE_LOG(WARN, "fail to append mem micro block", K(ret), K(i), K(meta_block_info));
      }
    } else if (meta_block_info.in_disk()) {
      int64_t next_level_row_count = meta_block_info.next_level_rows_list_->count();
      for (int64_t j = 0; OB_SUCC(ret) && j < next_level_row_count; j++) {
        if (OB_FAIL(meta_tree_builder_.append_row(*meta_block_info.next_level_rows_list_->at(j)))) {
          STORAGE_LOG(WARN, "fail to append row", K(ret),
              K(j), KPC(meta_block_info.next_level_rows_list_->at(j)), KPC(roots_.at(i)));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(roots_[i]->meta_block_info_.block_write_ctx_->get_macro_id_array(res.other_block_ids_))) {
        STORAGE_LOG(WARN, "fail to get macro ids of meta index blocks", K(ret));
      } else {
        res.index_blocks_cnt_ += roots_[i]->meta_block_info_.block_write_ctx_->macro_block_list_.count();
      }
    }
  }
  return ret;
}


int ObSSTableIndexBuilder::build_meta_tree(
    const share::ObPreWarmerParam &pre_warm_param,
    ObSSTableMergeRes &res,
    int64_t &macro_seq,
    ObIMacroBlockFlushCallback *callback)
{
  int ret = OB_SUCCESS;
  ObMacroBlocksWriteCtx *index_write_ctx = nullptr;
  ObMetaIndexBlockBuilder &builder = meta_tree_builder_;
  ObDataStoreDesc &desc = container_store_desc_;

  ObMacroSeqParam macro_seq_param;
  macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  macro_seq_param.start_ = macro_seq;
  container_store_desc_.data_store_type_ = ObMacroBlockCommonHeader::SSTableMacroMeta;
  const ObIndexBuildTaskType task_type = roots_[0]->task_type_;
  const bool in_array = roots_[0]->meta_block_info_.in_array();
  if (OB_FAIL(macro_writer_.open(desc, 0 /*parallel_idx*/, macro_seq_param,
                                 pre_warm_param, object_cleaner_, callback, nullptr, device_handle_))) {
    STORAGE_LOG(WARN, "fail to open index macro writer", K(ret));
  } else if (OB_FAIL(builder.init(desc, self_allocator_, macro_writer_))) {
    STORAGE_LOG(WARN, "fail to init index builder", K(ret));
  } else {
    bool all_in_mem = true;
    for (int64_t i = 0; i < roots_.count(); ++i) {
      all_in_mem &= !roots_[i]->meta_block_info_.in_disk();
    }
    switch (task_type) {
      case MERGE_TASK:
      case REBUILD_NORMAL_TASK:
      case REBUILD_DDL_TASK:
        if (in_array || all_in_mem) {
          if (OB_FAIL(build_meta_tree_from_all_mem_meta_block())) {
            STORAGE_LOG(WARN, "fail to build all mem meta", K(ret));
          }
        } else {
          if (OB_FAIL(build_meta_tree_from_meta_block(res))) {
            STORAGE_LOG(WARN, "fail to build not all mem meta", K(ret));
          }
        }
        break;
      case MERGE_CG_TASK:
      case REBUILD_CG_SELF_CAL_TASK:
        if (OB_FAIL(build_cg_meta_tree())) {
          STORAGE_LOG(WARN, "fail to build cg meta tree", K(ret));
        }
        break;
      case REBUILD_BACKUP_TASK:
      case REBUILD_BACKUP_DDL_TASK:
        if (in_array || all_in_mem) {
          if (OB_FAIL(build_meta_tree_from_all_mem_meta_block())) {
            STORAGE_LOG(WARN, "fail to build all mem meta", K(ret));
          }
        } else {
          if (OB_FAIL(build_meta_tree_from_backup_meta_block(res))) {
            STORAGE_LOG(WARN, "fail to build backup not all mem meta", K(ret));
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "the index build type is invalid", K(ret), K(task_type));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(builder.close(self_allocator_, roots_, index_block_loader_, res.nested_size_, res.nested_offset_, res.data_root_desc_))) {
    STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
  } else if (OB_FAIL(macro_writer_.close())) {
    STORAGE_LOG(WARN, "fail to close macro block writer", K(ret));
  } else if (OB_FAIL(macro_writer_.get_macro_block_write_ctx().get_macro_id_array(res.other_block_ids_))) {
    STORAGE_LOG(WARN, "fail to get macro ids of meta index blocks", K(ret));
  } else {
    macro_seq = macro_writer_.get_last_macro_seq();
    res.index_blocks_cnt_ += macro_writer_.get_macro_block_write_ctx().macro_block_list_.count();
    builder.reset();
    row_allocator_.reset();
  }
  return ret;
}

int ObSSTableIndexBuilder::collect_data_blocks_info(const ObDataMacroBlockMeta &macro_meta,
                                                    ObSSTableMergeRes &res) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(accumulate_macro_column_checksum(macro_meta, res))) {
    STORAGE_LOG(WARN, "fail to accumulate macro column checksum", K(ret), K(macro_meta));
  } else {
    res.occupy_size_ += macro_meta.val_.occupy_size_;
    res.original_size_ += macro_meta.val_.original_size_;
    res.micro_block_cnt_ += macro_meta.val_.micro_block_count_;
    res.data_checksum_ = ob_crc64_sse42(res.data_checksum_,
        &macro_meta.val_.data_checksum_, sizeof(res.data_checksum_));
    ++res.data_blocks_cnt_;
  }
  return ret;
}

int ObSSTableIndexBuilder::collect_data_blocks_info_from_root(const ObIndexTreeRootCtx &root,
                                                    ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  const ObDataBlockInfo &data_block_info = *root.data_blocks_info_;
  if (OB_UNLIKELY(data_block_info.data_column_cnt_ != res.data_column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_block_info.data_column_cnt_), K(res.data_column_cnt_));
  } else {
    for (int64_t i = 0; i < res.data_column_cnt_; ++i) {
      res.data_column_checksums_.at(i) += data_block_info.data_column_checksums_.at(i);
    }
    for (int64_t i = 0; i < root.get_data_block_cnt(); ++i) {
      res.data_checksum_ = ob_crc64_sse42(res.data_checksum_,
          &data_block_info.meta_data_checksums_.at(i), sizeof(res.data_checksum_));
    }
    res.occupy_size_ += data_block_info.occupy_size_;
    res.original_size_ += data_block_info.original_size_;
    res.micro_block_cnt_ += data_block_info.micro_block_cnt_;
    res.data_blocks_cnt_ += root.get_data_block_cnt();
  }
  return ret;
}

int ObSSTableIndexBuilder::accumulate_macro_column_checksum(
    const ObDataMacroBlockMeta &meta, ObSSTableMergeRes &res) const
{
  // accumulate column checksum for sstable data block
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta.is_valid() ||
                  meta.get_meta_val().column_count_ > res.data_column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(meta),
                K_(res.data_column_cnt));
  } else if (OB_UNLIKELY(
                 index_store_desc_.get_desc().is_major_or_meta_merge_type() &&
                 !index_store_desc_.get_desc()
                      .get_default_col_checksum_array_valid() &&
                 res.data_column_cnt_ > meta.get_meta_val().column_count_)) {
    // when default_col_checksum_array is invalid, need to make sure col_cnt in
    // macro equal to col_cnt of result sstable
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "index store desc is invalid", K(ret),
                K_(index_store_desc), K(meta.get_meta_val().column_count_),
                K(res.data_column_cnt_));
  } else {
    for (int64_t i = 0; i < meta.get_meta_val().column_count_; ++i) {
      res.data_column_checksums_.at(i) += meta.val_.column_checksums_[i];
    }
    for (int64_t i = meta.get_meta_val().column_count_;
         i < res.data_column_cnt_; ++i) {
      res.data_column_checksums_.at(i) +=
          meta.val_.row_count_ *
          index_store_desc_.get_desc().get_col_default_checksum_array().at(i);
    }
  }
  return ret;
}

void ObSSTableIndexBuilder::clean_status() {
  index_builder_.reset();
  meta_tree_builder_.reset();
  macro_writer_.reset();
  index_block_loader_.reset();
  // release memory to avoid occupying too much if retry frequently
  self_allocator_.reset();
}

bool ObSSTableIndexBuilder::micro_index_clustered() const
{
  return data_store_desc_.get_desc().micro_index_clustered();
}

int64_t ObSSTableIndexBuilder::get_tablet_transfer_seq() const
{
  return data_store_desc_.get_desc().get_tablet_transfer_seq();
}

int ObSSTableIndexBuilder::close(ObSSTableMergeRes &res,
                                 const int64_t nested_size,
                                 const int64_t nested_offset,
                                 ObIMacroBlockFlushCallback *callback,
                                 ObIODevice *device_handle)
{
  ObMacroDataSeq tmp_seq(0);
  tmp_seq.set_index_block();
  share::ObPreWarmerParam pre_warm_param(share::MEM_PRE_WARM);
  return close_with_macro_seq(res, tmp_seq.macro_data_seq_, nested_size,
                              nested_offset, pre_warm_param, callback, device_handle);
}

int ObSSTableIndexBuilder::close_with_macro_seq(
    ObSSTableMergeRes &res, int64_t &macro_seq, const int64_t nested_size,
    const int64_t nested_offset, const share::ObPreWarmerParam &pre_warm_param,
    ObIMacroBlockFlushCallback *callback, ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  res.reset();
  ObArenaAllocator load_allocator("IndexLoader", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const uint64_t data_version = ObBaseIndexBlockBuilder::get_data_version(data_store_desc_.get_desc());
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(((0 == nested_offset) ^
                          (OB_DEFAULT_MACRO_BLOCK_SIZE == nested_size)) ||
                         !pre_warm_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(nested_offset),
                K(nested_size), K(pre_warm_param));
  } else if (OB_UNLIKELY(is_closed_)) {
    if (OB_FAIL(res.assign(res_))) {
      STORAGE_LOG(WARN, "fail to assign res", K(ret), K_(res));
    }
  } else if (OB_FAIL(index_block_loader_.init(load_allocator, data_version))) {
    STORAGE_LOG(WARN, "fail to init index block loader", K(ret), K(data_version));
  } else if (FALSE_IT(device_handle_ = device_handle)) {
  } else if (OB_FAIL(res.prepare_column_checksum_array(index_store_desc_.get_desc().get_full_stored_col_cnt()))) {
    STORAGE_LOG(WARN, "fail to prepare column checksum array", K(ret));
  } else if (OB_FAIL(trim_empty_roots())) {
    STORAGE_LOG(WARN, "fail to trim empty roots", K(ret));
  } else if (OB_UNLIKELY(roots_.empty())) {
    res.root_desc_.set_empty();
    res.data_root_desc_.set_empty();
    STORAGE_LOG(DEBUG, "sstable has no data", K(ret));
  } else if (OB_FAIL(sort_roots())) {
    STORAGE_LOG(WARN, "fail to sort roots", K(ret));
  } else if (0 == nested_offset && device_handle_ == nullptr) {
    const bool is_single_block = check_single_block();
    if (is_single_block) {
      ObSpaceOptimizationMode tmp_mode = optimization_mode_;
      // tmp code, we should support reuse data for small sstable
      if (index_store_desc_.get_desc().is_cg() && res.row_count_ > 50000) {
        tmp_mode = DISABLE;
      }
#ifdef ERRSIM
      if (OB_SUCCESS != EN_COMPACTION_DISABLE_SHARED_MACRO) {
        tmp_mode = DISABLE;
        FLOG_INFO("ERRSIM EN_COMPACTION_DISABLE_SHARED_MACRO", KR(ret));
      }
#endif
      switch (tmp_mode) {
        case ENABLE:
          if (OB_FAIL(check_and_rewrite_sstable(res))) {
            STORAGE_LOG(WARN, "fail to check and rewrite small sstable", K(ret));
          }
          break;
        case DISABLE:
          res.nested_offset_ = 0;
          res.nested_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "the optimization mode is invalid", K(ret), K(optimization_mode_));
          break;
      }
    } else {
      res.nested_offset_ = nested_offset;
      res.nested_size_ = nested_size;
    }
  } else {
    // if nested_offset is not 0, this sstable is reused-small-sstable, we don't
    // need to rewrite it
    res.nested_offset_ = nested_offset;
    res.nested_size_ = nested_size;
  }
  if (OB_FAIL(ret) || roots_.empty() || is_closed_) {
    // do nothing
  } else if (OB_FAIL(merge_index_tree(pre_warm_param, res, macro_seq, callback))) {
    STORAGE_LOG(WARN, "fail to merge index tree", K(ret), KP(callback));
  } else if (OB_FAIL(build_meta_tree(pre_warm_param, res, ++macro_seq, callback))) {
    STORAGE_LOG(WARN, "fail to build meta tree", K(ret), KP(callback));
  }

  if (OB_SUCC(ret) && OB_LIKELY(!is_closed_)) {
    const ObDataStoreDesc &desc = index_store_desc_.get_desc();
    res.root_row_store_type_ = desc.get_row_store_type();
    res.compressor_type_ = desc.get_compressor_type();
    res.encrypt_id_ = desc.get_encrypt_id();
    MEMCPY(res.encrypt_key_, desc.get_encrypt_key(),
           share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    res.master_key_id_ = desc.get_master_key_id();
    res.set_table_flag_with_macro_id_array();
    if (OB_FAIL(res_.assign(res))) {
      STORAGE_LOG(WARN, "fail to save merge res", K(ret), K(res));
    } else if (OB_FAIL(object_cleaner_.mark_succeed())) {
      LOG_WARN("fail to mark succeed in private object cleaner", K(ret));
    } else {
      is_closed_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "succeed to close sstable index builder", K(res));
  } else {
    int64_t data_blocks_cnt = 0;
    for (int64_t i = 0; i < roots_.count(); ++i) {
      if (OB_NOT_NULL(roots_.at(i))) {
        data_blocks_cnt += roots_.at(i)->get_data_block_cnt();
      }
    }
    if (is_retriable_error(ret)) {
      STORAGE_LOG(WARN, "fail to close sstable index builder", K(ret),
                  K(data_blocks_cnt), K(sstable_allocator_.total()),
                  K(sstable_allocator_.used()), K(self_allocator_.total()),
                  K(self_allocator_.used()), K(row_allocator_.total()),
                  K(row_allocator_.used()));
    } else {
      STORAGE_LOG(ERROR, "fail to close sstable index builder", K(ret),
                  K(data_blocks_cnt), K(sstable_allocator_.total()),
                  K(sstable_allocator_.used()), K(self_allocator_.total()),
                  K(self_allocator_.used()), K(row_allocator_.total()),
                  K(row_allocator_.used()));
    }
    clean_status(); // clear since re-entrant
  }
  index_block_loader_.reset();
  return ret;
}

int ObSSTableIndexBuilder::check_and_rewrite_sstable(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  int64_t macro_size = 0;
  for (int64_t i = 0; i < roots_.count(); i++) {
    macro_size = roots_[i]->last_macro_size_;
  }
  const int64_t align_macro_size = upper_align(macro_size, DIO_READ_ALIGN_SIZE);

  if (align_macro_size >= SMALL_SSTABLE_THRESHOLD) { // skip rewrite
    res.nested_offset_ = 0;
    res.nested_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  } else if (0 == macro_size) {
    if (OB_FAIL(check_and_rewrite_sstable_without_size(res))) {
      STORAGE_LOG(WARN,
                  "fail to check and rewrite small sstable without macro size",
                  K(ret), K(macro_size));
    }
  } else { // align_macro_size < SMALL_SSTABLE_THRESHOLD && 0 != macro_size
    if (OB_FAIL(rewrite_small_sstable(res))) {
      STORAGE_LOG(WARN, "fail to rewrite small sstable with given macro size",
                  K(ret));
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::rewrite_small_sstable(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObBlockInfo block_info;
  ObStorageObjectHandle read_handle;
  ObDataMacroBlockMeta macro_meta;
  ObStorageObjectReadInfo read_info;
  read_info.offset_ = 0;
  read_info.size_ =
      upper_align(roots_[0]->last_macro_size_, DIO_READ_ALIGN_SIZE);
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.io_timeout_ms_ =
      std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_INDEX_BUILDER_IO);

  if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char *>(
                    self_allocator_.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret),
                K(read_info.size_));
  } else {
    if (OB_FAIL(get_single_macro_meta_for_small_sstable(row_allocator_, index_block_loader_,
          container_store_desc_, roots_, macro_meta))) {
        STORAGE_LOG(WARN, "fail to get single macro meta", K(ret));
    } else if (FALSE_IT(read_info.macro_block_id_ = macro_meta.val_.macro_id_)) {
    } else if (OB_FAIL(ObObjectManager::async_read_object(read_info, read_handle))) {
      STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info), K(macro_meta), K(roots_[0]->last_macro_size_));
    } else if (OB_FAIL(read_handle.wait())) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(read_info));
    } else {
      ObSharedMacroBlockMgr *shared_block_mgr = MTL(ObSharedMacroBlockMgr*);
      if (OB_FAIL(shared_block_mgr->write_block(
          read_info.buf_, read_handle.get_data_size(), block_info, *(roots_[0]->data_write_ctx_)))) {
        STORAGE_LOG(WARN, "fail to write small sstable through shared_block_mgr", K(ret));
      } else if (OB_UNLIKELY(!block_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "successfully rewrite small sstable, but block info is invali", K(ret), K(block_info));
      } else if (FALSE_IT(macro_meta.val_.macro_id_ = block_info.macro_id_)) {
      } else if (OB_FAIL(change_single_macro_meta_for_small_sstable(macro_meta))){
        STORAGE_LOG(WARN, "fail to change index tree root ctx macro id for small sst", K(ret),
            K(block_info.macro_id_), KPC(roots_[0]));
      } else {
        res.nested_offset_ = block_info.nested_offset_;
        res.nested_size_ = block_info.nested_size_;
      }
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::check_and_rewrite_sstable_without_size(
    ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObBlockInfo block_info;

  if (OB_FAIL(do_check_and_rewrite_sstable(block_info))) {
    STORAGE_LOG(WARN, "fail to check macro block size and rewrite", K(ret));
  } else if (block_info.is_small_sstable()) {
    res.nested_offset_ = block_info.nested_offset_;
    res.nested_size_ = block_info.nested_size_;
  }

  if (OB_SUCC(ret) && !block_info.is_small_sstable()) {
    res.nested_offset_ = 0;
    res.nested_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  }
  return ret;
}

int ObSSTableIndexBuilder::do_check_and_rewrite_sstable(
    ObBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta macro_meta;
  ObSSTableMacroBlockHeader macro_header;
  const char *data_buf = nullptr;

  if (OB_FAIL(get_single_macro_meta_for_small_sstable(row_allocator_, index_block_loader_,
      container_store_desc_, roots_, macro_meta))) {
    STORAGE_LOG(WARN, "fail to get single macro meta", K(ret));
  } else if (OB_FAIL(load_single_macro_block(macro_meta, OB_STORAGE_OBJECT_MGR.get_macro_block_size(), 0, self_allocator_, data_buf))) {
    STORAGE_LOG(WARN, "fail to load macro block", K(ret), K(macro_meta), KPC(roots_[0]));
  } else if (OB_FAIL(parse_macro_header(data_buf, OB_STORAGE_OBJECT_MGR.get_macro_block_size(), macro_header))) {
    STORAGE_LOG(WARN, "fail to parse macro header", K(ret), KP(data_buf), K(macro_meta), KPC(roots_[0]));
  } else {
    roots_[0]->meta_block_offset_ =
        macro_header.fixed_header_.meta_block_offset_;
    roots_[0]->meta_block_size_ = macro_header.fixed_header_.meta_block_size_;
    const int64_t align_size =
        upper_align(macro_header.fixed_header_.meta_block_offset_ +
                        macro_header.fixed_header_.meta_block_size_,
                    DIO_READ_ALIGN_SIZE);
    if (align_size < SMALL_SSTABLE_THRESHOLD) { // need to be rewritten
      ObSharedMacroBlockMgr *shared_block_mgr = MTL(ObSharedMacroBlockMgr*);
      if (OB_FAIL(shared_block_mgr->write_block(
          data_buf, align_size, block_info, *(roots_[0]->data_write_ctx_)))) {
        STORAGE_LOG(WARN, "fail to write small sstable through shared_block_mgr", K(ret));
      } else if (OB_UNLIKELY(!block_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "successfully rewrite small sstable, but block info is invalid", K(ret), K(block_info));
      } else if (FALSE_IT(macro_meta.val_.macro_id_ = block_info.macro_id_)) {
      } else if (OB_FAIL(change_single_macro_meta_for_small_sstable(macro_meta))){
        STORAGE_LOG(WARN, "fail to change index tree root ctx macro id for small sst", K(ret),
            K(block_info.macro_id_), KPC(roots_[0]));
      }
    }
  }

  return ret;
}

int ObSSTableIndexBuilder::load_single_macro_block(
    const ObDataMacroBlockMeta &macro_meta,
    const int64_t nested_size,
    const int64_t nested_offset,
    ObIAllocator &allocator,
    const char *&data_buf)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle read_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = macro_meta.val_.macro_id_;
  read_info.offset_ = nested_offset;
  read_info.size_ = nested_size;
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ =
      std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_INDEX_BUILDER_IO);

  if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char *>(
                    allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret),
                K(read_info.size_));
  } else if (OB_FAIL(ObObjectManager::async_read_object(read_info, read_handle))) {
    STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info), K(macro_meta));
  } else if (OB_FAIL(read_handle.wait())) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
  } else {
    data_buf = read_info.buf_;
  }

  return ret;
}

int ObSSTableIndexBuilder::get_single_macro_meta_for_small_sstable(
    ObIAllocator &allocator,
    ObIndexBlockLoader &index_block_loader,
    const ObDataStoreDesc &container_store_desc,
    const IndexTreeRootCtxList &roots,
    ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  ObDatumRow *meta_row;
  index_block_loader.reuse();
  const ObIndexBlockInfo& meta_block_info = roots[0]->meta_block_info_;
  if (roots.count() != 1 || roots[0]->meta_block_info_.get_row_count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpect macro meta count for small sstable",
        K(ret), K(roots[0]->meta_block_info_));
  } else if (OB_FAIL(index_block_loader.open(roots[0]->meta_block_info_))) {
    STORAGE_LOG(WARN, "fail to open macro meta loader", K(ret), K(roots[0]->meta_block_info_));
  } else if (OB_ISNULL(meta_row = OB_NEWx(ObDatumRow, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc meta row mem", K(ret));
  } else if (OB_FAIL(meta_row->init(allocator, container_store_desc.get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init", K(ret), K(container_store_desc.get_row_column_count()));
  } else if (OB_FAIL(index_block_loader.get_next_row(*meta_row))) {
    STORAGE_LOG(WARN, "fail to get row", K(ret));
  } else if (OB_FAIL(macro_meta.parse_row(*meta_row))) {
    STORAGE_LOG(WARN, "fail to parse row", K(ret));
  }
  return ret;
}

int ObSSTableIndexBuilder::change_single_macro_meta_for_small_sstable(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  ObBaseIndexBlockDumper macro_meta_dumper;
  ObDatumRow meta_row;
  const uint64_t data_version = ObBaseIndexBlockBuilder::get_data_version(data_store_desc_.get_desc());
  if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro meta", K(ret), K(macro_meta));
  } else if (OB_FAIL(macro_meta_dumper.init(index_store_desc_.get_desc(), container_store_desc_, this,
      sstable_allocator_, self_allocator_, true, enable_dump_disk()))) {
    STORAGE_LOG(WARN, "fail to init macro meta dumper", K(ret));
  } else if (OB_FAIL(meta_row.init(self_allocator_, container_store_desc_.get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init meta row", K(ret), K(container_store_desc_.get_row_column_count()));
  } else if (OB_FAIL(macro_meta.build_row(meta_row, row_allocator_, data_version))) {
    STORAGE_LOG(WARN, "fail to build row", K(ret), K(macro_meta), K(data_version));
  } else if (OB_FAIL(macro_meta_dumper.append_row(meta_row))) {
    STORAGE_LOG(WARN, "failed to append row to index block dumper", K(ret), K(macro_meta));
  } else if (FALSE_IT(roots_[0]->meta_block_info_.reset())) {
  } else if (OB_FAIL(macro_meta_dumper.close(roots_[0]->meta_block_info_))) {
    STORAGE_LOG(WARN, "Fail to close macro meta dumper", K(ret));
  }
  return ret;
}

int ObSSTableIndexBuilder::parse_macro_header(
    const char *buf, const int64_t buf_size,
    ObSSTableMacroBlockHeader &macro_header) {
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMacroBlockCommonHeader common_header;

  if (OB_UNLIKELY(buf_size <= 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "the argument is invalid", K(ret), K(buf_size), KP(buf));
  } else if (OB_FAIL(common_header.deserialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize common header", K(ret), K(buf_size),
                KP(buf), K(pos));
  } else if (OB_FAIL(macro_header.deserialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize macro header", K(ret), KP(buf),
                K(buf_size), K(pos));
  } else if (OB_UNLIKELY(!macro_header.is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid macro header", K(ret), K(macro_header));
  }
  return ret;
}

bool ObSSTableIndexBuilder::check_single_block() {
  int64_t cnt = 0;
  for (int64_t i = 0; i < roots_.count(); i++) {
    cnt += roots_[i]->meta_block_info_.get_row_count();
  }
  return 1 == cnt;
}

bool ObSSTableIndexBuilder::is_retriable_error(const int ret_code) const
{
  bool b_ret = false;
  switch (ret_code) {
    case OB_TIMEOUT:
    case OB_ALLOCATE_MEMORY_FAILED:
    case OB_EAGAIN:
    case OB_IO_ERROR:
    case OB_OBJECT_STORAGE_IO_ERROR:
      b_ret = true;
      break;
    default:
      break;
  }
  return b_ret;
}

//===================== ObBaseIndexBlockBuilder(public) ================
ObBaseIndexBlockBuilder::ObBaseIndexBlockBuilder()
    : is_inited_(false), is_closed_(false), index_store_desc_(nullptr),
      data_store_desc_(nullptr), row_builder_(), last_rowkey_(),
      row_allocator_("BaseMidIdx"), allocator_(nullptr), micro_writer_(nullptr),
      macro_writer_(nullptr), micro_block_adaptive_splitter_(), row_offset_(-1),
      index_block_aggregator_(), next_level_builder_(nullptr), level_(0) {}

ObBaseIndexBlockBuilder::~ObBaseIndexBlockBuilder() { reset(); }

void ObBaseIndexBlockBuilder::reset()
{
  is_closed_ = false;
  data_store_desc_ = nullptr;
  index_store_desc_ = nullptr;
  last_rowkey_.reset();
  row_allocator_.reset();
  if (OB_NOT_NULL(micro_writer_)) {
    micro_writer_->~ObIMicroBlockWriter();
    allocator_->free(micro_writer_);
    micro_writer_ = nullptr;
  }
  row_builder_.reset();
  if (OB_NOT_NULL(next_level_builder_)) {
    next_level_builder_->~ObBaseIndexBlockBuilder();
    allocator_->free(next_level_builder_);
    next_level_builder_ = nullptr;
  }
  macro_writer_ = nullptr;
  micro_block_adaptive_splitter_.reset();
  index_block_aggregator_.reset();
  allocator_ = nullptr;
  level_ = 0;
  row_offset_ = -1; // starts from -1
  is_inited_ = false;
}

int ObBaseIndexBlockBuilder::init(const ObDataStoreDesc &data_store_desc,
                                  ObDataStoreDesc &index_store_desc,
                                  ObIAllocator &allocator,
                                  ObMacroBlockWriter *macro_writer,
                                  const int64_t level)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBaseIndexBlockBuilder has been inited", K(ret));
  } else {
    data_store_desc_ = &data_store_desc;
    index_store_desc_ = &index_store_desc;
    allocator_ = &allocator;
    macro_writer_ = macro_writer;
    level_ = level;
    if (OB_FAIL(row_builder_.init(allocator, data_store_desc, index_store_desc))) {
      STORAGE_LOG(WARN, "fail to init ObBaseIndexBlockBuilder", K(ret));
    } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(
                   index_store_desc_, allocator, micro_writer_))) {
      STORAGE_LOG(WARN, "fail to build micro writer", K(ret));
    } else if (OB_FAIL(
                   index_block_aggregator_.init(data_store_desc, allocator))) {
      STORAGE_LOG(WARN, "fail to init index block aggregator", K(ret));
    } else if (OB_FAIL(micro_block_adaptive_splitter_.init(
                   index_store_desc_->get_macro_store_size(),
                   MIN_INDEX_MICRO_BLOCK_ROW_CNT /*min_micro_row_count*/,
                   true /*is_use_adaptive*/))) {
      STORAGE_LOG(WARN, "Failed to init micro block adaptive split", K(ret),
                  "macro_store_size",
                  index_store_desc_->get_macro_store_size());
    } else {
      if (need_pre_warm() &&
          index_store_desc.get_tablet_id().is_user_tablet()) {
        // TODO (lixia.yq): compile may not pass after refresh, may need refactor
      }
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBaseIndexBlockBuilder::check_order(const ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row desc", K(ret), K(row_desc));
  } else if (!last_rowkey_.is_valid()) {   // skip
  } else if (index_store_desc_->is_cg()) { // datum_utils is lacked for cg
    if (row_desc.row_key_.get_datum(0).get_int() <=
        last_rowkey_.get_datum(0).get_int()) {
      ret = OB_ROWKEY_ORDER_ERROR;
      STORAGE_LOG(ERROR, "input rowkey is less then last rowkey.",
                  K(row_desc.row_key_), K(last_rowkey_), K(ret));
    }
  } else {
    const ObDatumRowkey &cur_rowkey = row_desc.row_key_;
    int32_t compare_result = 0;
    const ObStorageDatumUtils &datum_utils =
        index_store_desc_->get_datum_utils();
    if (OB_FAIL(
            cur_rowkey.compare(last_rowkey_, datum_utils, compare_result))) {
      STORAGE_LOG(WARN, "Failed to compare last key", K(ret), K(cur_rowkey),
                  K(last_rowkey_));
    } else if (OB_UNLIKELY(compare_result < 0)) {
      ret = OB_ROWKEY_ORDER_ERROR;
      STORAGE_LOG(ERROR, "input rowkey is less then last rowkey.",
                  K(cur_rowkey), K(last_rowkey_), K(ret));
    }
  }
  return ret;
}

int ObBaseIndexBlockBuilder::append_row(const ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row_to_append = NULL;
  if (OB_FAIL(check_order(row_desc))) {
    STORAGE_LOG(WARN, "fail to check order", K(ret), K(row_desc));
  } else if (OB_FAIL(row_builder_.build_row(row_desc, row_to_append))) {
    STORAGE_LOG(WARN, "fail to build index row", K(ret), K(row_desc));
  } else if (OB_FAIL(insert_and_update_index_tree(row_to_append))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      STORAGE_LOG(WARN, "fail to insert index row", K(ret), KPC(row_to_append));
    }
  } else {
    last_rowkey_.reset();
    row_allocator_.reuse();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(
                 row_desc.row_key_.deep_copy(last_rowkey_, row_allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy last rowkey", K(ret), K(row_desc));
  } else if (OB_FAIL(index_block_aggregator_.eval(row_desc))) {
    STORAGE_LOG(WARN, "fail to aggregate index row", K(ret), K(row_desc));
  } else {
    row_offset_ = row_desc.row_offset_; // row_offset is increasing
  }
  return ret;
}

int ObBaseIndexBlockBuilder::append_row(
    const ObDataMacroBlockMeta &macro_meta,
    const ObClusteredIndexBlockMicroInfos *clustered_micro_info,
    ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row_to_append = NULL;
  if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_meta));
  } else if (OB_FAIL(meta_to_row_desc(macro_meta, *index_store_desc_, clustered_micro_info, row_desc))) {
    LOG_WARN("fail to build row desc from macro meta and clustered micro info",
             K(ret), K(macro_meta), K(clustered_micro_info), KPC(data_store_desc_));
  } else if (OB_FAIL(append_row(row_desc))) {
    LOG_WARN("fail to append row", K(ret), K(row_desc), K(macro_meta));
  }
  return ret;
}

int ObBaseIndexBlockBuilder::close(ObIAllocator &allocator, ObIndexTreeInfo &tree_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBaseIndexBlockBuilder *root_builder = nullptr;
  ObIndexTreeRootBlockDesc &desc = tree_info.root_desc_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid index builder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "base builder is closed", K(ret), K(is_closed_));
  } else if (OB_UNLIKELY(index_block_aggregator_.get_row_count() <= 0)) {
    tree_info.set_empty();
    is_closed_ = true;
    STORAGE_LOG(DEBUG, "macro block writer has no data", K(ret), K(desc));
  } else if (OB_FAIL(close_index_tree(root_builder))) {
    STORAGE_LOG(WARN, "fail to close index tree", K(ret));
  } else if (OB_ISNULL(root_builder)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null root builder", K(ret), K(root_builder));
  } else {
    ObMetaDiskAddr &root_addr = desc.addr_;
    desc.height_ = root_builder->level_ + 1;
    ObMicroBlockDesc micro_block_desc;
    ObIMicroBlockWriter *micro_writer = root_builder->micro_writer_;
    if (OB_UNLIKELY(micro_writer->get_row_count() == 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected empty root block", K(ret));
    } else if (OB_FAIL(
                   micro_writer->build_micro_block_desc(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to build root block", K(ret));
    } else if (FALSE_IT(micro_block_desc.last_rowkey_ =
                            root_builder->last_rowkey_)) {
    } else if (OB_UNLIKELY(micro_block_desc.get_block_size() >=
                           ROOT_BLOCK_SIZE_LIMIT)) {
      if (OB_FAIL(macro_writer_->append_index_micro_block(micro_block_desc))) {
        micro_writer->dump_diagnose_info(); // ignore dump error
        STORAGE_LOG(WARN, "fail to append root block", K(ret),
                    K(micro_block_desc));
      } else {
        ObIndexBlockRowDesc root_row_desc(*index_store_desc_);
        root_builder->block_to_row_desc(micro_block_desc, root_row_desc);
        if (OB_FAIL(root_addr.set_block_addr(
                root_row_desc.macro_id_, root_row_desc.block_offset_,
                root_row_desc.block_size_, ObMetaDiskAddr::DiskType::BLOCK))) {
          STORAGE_LOG(WARN, "fail to set block address", K(ret),
                      K(root_row_desc));
        }
      }
    } else {
      char *&root_buf = desc.buf_;
      const int64_t buf_size =
          micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
      int64_t pos = 0;
      if (OB_ISNULL(root_buf =
                        static_cast<char *>(allocator.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc root buf", K(ret), K(buf_size));
      } else if (OB_FAIL(micro_block_desc.header_->serialize(root_buf, buf_size,
                                                             pos))) {
        STORAGE_LOG(WARN, "fail to serialize header", K(ret),
                    K(micro_block_desc));
      } else {
        MEMCPY(root_buf + pos, micro_block_desc.buf_, buf_size - pos);
        if (OB_FAIL(root_addr.set_mem_addr(0, buf_size))) {
          STORAGE_LOG(WARN, "fail to set memory address", K(ret), K(buf_size));
        }
      }
    }
    if (OB_FAIL(ret) && nullptr != desc.buf_) {
      allocator.free(desc.buf_);
    } else if (OB_SUCC(ret)) {
      const ObIndexBlockAggregator &root_aggregator =
          root_builder->index_block_aggregator_;
      tree_info.row_count_ = root_aggregator.get_row_count();
      tree_info.max_merged_trans_version_ =
          root_aggregator.get_max_merged_trans_version();
      tree_info.contain_uncommitted_row_ =
          root_aggregator.contain_uncommitted_row();
      root_builder->is_closed_ = true;
    }
  }
  return ret;
}

//===================== ObBaseIndexBlockBuilder(protected) ================

int ObBaseIndexBlockBuilder::build_index_micro_block(
    ObMicroBlockDesc &micro_block_desc) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid base index builder", K(ret), K(is_inited_));
  } else if (micro_writer_->get_row_count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected empty index micro block", K(ret));
  } else if (OB_FAIL(micro_writer_->build_micro_block_desc(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to build micro block", K(ret));
  } else if (FALSE_IT(micro_block_desc.last_rowkey_ = last_rowkey_)) {
  }
  return ret;
}

void ObBaseIndexBlockBuilder::clean_status() {
  micro_writer_->reuse(); // only reuse when index row has been inserted
  index_block_aggregator_.reuse();
}

int ObBaseIndexBlockBuilder::append_index_micro_block() {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t block_size = 0;
  ObMicroBlockDesc micro_block_desc;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid base index builder", K(ret), K(is_inited_));
  } else if (OB_FAIL(build_index_micro_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to build index micro block", K(ret));
  } else {
    block_size = micro_block_desc.buf_size_;
    if (OB_FAIL(macro_writer_->append_index_micro_block(micro_block_desc))) {
      micro_writer_->dump_diagnose_info(); // ignore dump error
      STORAGE_LOG(WARN, "fail to append index micro block", K(ret),
                  K(micro_block_desc));
    } else if (OB_FAIL(micro_block_adaptive_splitter_.update_compression_info(
                   micro_block_desc.row_count_, block_size,
                   micro_block_desc.buf_size_))) {
      STORAGE_LOG(WARN, "Fail to update_compression_info", K(ret),
                  K(micro_block_desc));
    } else if (OB_FAIL(append_next_row(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to append next row", K(ret), K(micro_block_desc));
    }
    clean_status();
  }

  return ret;
}

int ObBaseIndexBlockBuilder::get_aggregated_row(
    ObIndexBlockRowDesc &next_row_desc) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_block_aggregator_.get_index_agg_result(next_row_desc))) {
    STORAGE_LOG(WARN, "fail to get aggregated row", K(ret),
                K_(index_block_aggregator));
  } else {
    next_row_desc.row_offset_ = row_offset_;
  }
  return ret;
}

int ObBaseIndexBlockBuilder::update_with_aggregate_info(const ObIndexRowAggInfo &index_row_agg_info,
                                                        ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!index_row_agg_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid agg info", K(ret), K(index_row_agg_info));
  } else {
    if (index_row_agg_info.need_data_aggregate_) {
      row_desc.aggregated_row_ = &index_row_agg_info.aggregated_row_;
    } else {
      row_desc.aggregated_row_ = nullptr;
    }
    index_row_agg_info.aggregate_info_.get_agg_result(row_desc);
  }
  return ret;
}

int ObBaseIndexBlockBuilder::close_index_tree(ObBaseIndexBlockBuilder *&root_builder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid base index builder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(next_level_builder_ == nullptr)) {
    root_builder = this;
  } else {
    if (OB_LIKELY(micro_writer_->get_row_count() > 0)) {
      if (OB_FAIL(append_index_micro_block())) {
        STORAGE_LOG(WARN, "Fail to append index micro block, ", K(ret));
      }
    }
    is_closed_ = true; // mark this level builder is closed
    if (OB_SUCC(ret) &&
        OB_FAIL(next_level_builder_->close_index_tree(root_builder))) {
      STORAGE_LOG(WARN, "fail to close index tree, ", K(ret));
    }
  }
  return ret;
}

uint64_t ObBaseIndexBlockBuilder::get_data_version(const ObDataStoreDesc &data_store_desc)
{
  uint64_t data_version = 0;
  if (data_store_desc.is_major_merge_type()) {
    data_version = data_store_desc.get_major_working_cluster_version();
  } else {
    data_version = CLUSTER_CURRENT_VERSION;
  }
  return data_version;
}

/* below three functions should keep pace with each other */
void ObBaseIndexBlockBuilder::block_to_row_desc(
    const ObMicroBlockDesc &micro_block_desc, ObIndexBlockRowDesc &row_desc) {
  row_desc.row_key_ = micro_block_desc.last_rowkey_;
  row_desc.macro_id_ = micro_block_desc.macro_id_;
  row_desc.logic_micro_id_ = micro_block_desc.logic_micro_id_;
  row_desc.data_checksum_ = micro_block_desc.header_->data_checksum_;
  row_desc.block_offset_ = micro_block_desc.block_offset_;
  row_desc.block_size_ =
      micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
  row_desc.row_count_ = micro_block_desc.row_count_;
  row_desc.row_count_delta_ = micro_block_desc.row_count_delta_;
  row_desc.is_deleted_ = micro_block_desc.can_mark_deletion_;
  row_desc.max_merged_trans_version_ =
      micro_block_desc.max_merged_trans_version_;
  row_desc.contain_uncommitted_row_ = micro_block_desc.contain_uncommitted_row_;
  row_desc.has_string_out_row_ = micro_block_desc.has_string_out_row_;
  row_desc.has_lob_out_row_ = micro_block_desc.has_lob_out_row_;
  row_desc.is_last_row_last_flag_ = micro_block_desc.is_last_row_last_flag_;
  row_desc.aggregated_row_ = micro_block_desc.aggregated_row_;
  row_desc.is_serialized_agg_row_ = false;
  // This flag only valid in macro level.
  row_desc.has_macro_block_bloom_filter_ = 0;
}

int ObBaseIndexBlockBuilder::meta_to_row_desc(
    const ObDataMacroBlockMeta &macro_meta,
    const ObDataStoreDesc &index_store_desc,
    const ObClusteredIndexBlockMicroInfos *clustered_micro_info,
    ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc *data_desc = nullptr;
  if (OB_UNLIKELY(nullptr == row_desc.get_data_store_desc())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null data store desc", K(ret));
  } else if (FALSE_IT(data_desc = const_cast<ObDataStoreDesc *>(
                          row_desc.get_data_store_desc()))) {
  } else if (OB_FAIL(data_desc->static_desc_->end_scn_.convert_for_tx(
                 macro_meta.val_.snapshot_version_))) {
    STORAGE_LOG(WARN, "fail to convert scn", K(ret),
                K(macro_meta.val_.snapshot_version_));
  } else {
    ObStaticDataStoreDesc *static_desc = data_desc->static_desc_;
    static_desc->snapshot_version_ = macro_meta.val_.snapshot_version_;

    row_desc.is_secondary_meta_ = false;
    row_desc.is_macro_node_ = true;

    // Set clustered micro info for macro row.
    if (OB_NOT_NULL(clustered_micro_info)) {
      row_desc.macro_id_ = clustered_micro_info->macro_id_;
      row_desc.block_offset_ = clustered_micro_info->block_offset_;
      row_desc.block_size_ = clustered_micro_info->block_size_;
      row_desc.logic_micro_id_ = clustered_micro_info->logic_micro_id_;
      row_desc.shared_data_macro_id_ = macro_meta.val_.macro_id_;
      // Row store type, compress type, encrypt info, and schema version.
      data_desc->row_store_type_ = index_store_desc.get_row_store_type();
      static_desc->compressor_type_ = index_store_desc.get_compressor_type();
      static_desc->master_key_id_ = index_store_desc.get_master_key_id();
      static_desc->encrypt_id_ = index_store_desc.get_encrypt_id();
      MEMCPY(static_desc->encrypt_key_, index_store_desc.get_encrypt_key(), sizeof(static_desc->encrypt_key_));
      static_desc->schema_version_ = index_store_desc.get_schema_version();
    } else {
      row_desc.macro_id_ = macro_meta.val_.macro_id_;
      row_desc.block_offset_ = macro_meta.val_.block_offset_;
      row_desc.block_size_ = macro_meta.val_.block_size_;
      // Row store type, compress type, encrypt info, and schema version.
      if (index_store_desc.get_major_working_cluster_version() >= CLUSTER_VERSION_4_3_5_1) {
        const ObRowStoreType data_row_store_type = macro_meta.val_.row_store_type_;
        if (ObRowStoreType::ENCODING_ROW_STORE == data_row_store_type) {
          data_desc->row_store_type_ = ObRowStoreType::SELECTIVE_ENCODING_ROW_STORE;
        } else {
          data_desc->row_store_type_ = macro_meta.val_.row_store_type_;
        }
      } else {
        data_desc->row_store_type_ = macro_meta.val_.row_store_type_;
      }
      static_desc->compressor_type_ = macro_meta.val_.compressor_type_;
      static_desc->master_key_id_ = macro_meta.val_.master_key_id_;
      static_desc->encrypt_id_ = macro_meta.val_.encrypt_id_;
      MEMCPY(static_desc->encrypt_key_, macro_meta.val_.encrypt_key_, sizeof(static_desc->encrypt_key_));
      static_desc->schema_version_ = macro_meta.val_.schema_version_;
    }

    // Set macro block bloom filter flag for macro row.
    if (macro_meta.val_.macro_block_bf_size_ > 0) {
      row_desc.has_macro_block_bloom_filter_ = 1;
    } else {
      row_desc.has_macro_block_bloom_filter_ = 0;
    }

    row_desc.row_key_ = macro_meta.end_key_;
    row_desc.row_count_ = macro_meta.val_.row_count_;
    row_desc.row_count_delta_ = macro_meta.val_.row_count_delta_;
    row_desc.is_deleted_ = macro_meta.val_.is_deleted_;
    row_desc.max_merged_trans_version_ =
        macro_meta.val_.max_merged_trans_version_;
    row_desc.contain_uncommitted_row_ =
        macro_meta.val_.contain_uncommitted_row_;
    row_desc.micro_block_count_ = macro_meta.val_.micro_block_count_;
    row_desc.macro_block_count_ = 1;
    row_desc.has_string_out_row_ = macro_meta.val_.has_string_out_row_;
    row_desc.has_lob_out_row_ = !macro_meta.val_.all_lob_in_row_;
    // We have validate macro meta in caller, so we do not validate agg_row_buf and agg_row_len here.
    if (nullptr != macro_meta.val_.agg_row_buf_) {
      row_desc.serialized_agg_row_buf_ = macro_meta.val_.agg_row_buf_;
      row_desc.is_serialized_agg_row_ = true;
    } else {
      row_desc.serialized_agg_row_buf_ = nullptr;
      row_desc.is_serialized_agg_row_ = false;
    }
    // is_last_row_last_flag_ only used in data macro block
  }
  return ret;
}

int ObBaseIndexBlockBuilder::row_desc_to_meta(
    const ObIndexBlockRowDesc &macro_row_desc, ObDataMacroBlockMeta &macro_meta,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  macro_meta.end_key_ = macro_row_desc.row_key_;
  macro_meta.val_.macro_id_ =
      macro_row_desc.macro_id_; // DEFAULT_IDX_ROW_MACRO_ID
  macro_meta.val_.block_offset_ = macro_row_desc.block_offset_;
  macro_meta.val_.block_size_ = macro_row_desc.block_size_;
  macro_meta.val_.row_count_ = macro_row_desc.row_count_;
  macro_meta.val_.row_count_delta_ = macro_row_desc.row_count_delta_;
  macro_meta.val_.is_deleted_ = macro_row_desc.is_deleted_;
  macro_meta.val_.max_merged_trans_version_ =
      macro_row_desc.max_merged_trans_version_;
  macro_meta.val_.contain_uncommitted_row_ =
      macro_row_desc.contain_uncommitted_row_;
  macro_meta.val_.has_string_out_row_ = macro_row_desc.has_string_out_row_;
  macro_meta.val_.all_lob_in_row_ = !macro_row_desc.has_lob_out_row_;
  macro_meta.val_.is_last_row_last_flag_ =
      macro_row_desc.is_last_row_last_flag_;
  if (nullptr != macro_row_desc.aggregated_row_) {
    agg_row_writer_.reset();
    char *agg_row_buf = nullptr;
    int64_t agg_row_upper_size = 0;
    int64_t pos = 0;
    if (OB_UNLIKELY(macro_row_desc.is_serialized_agg_row_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected serialzied agg row in descriptor", K(ret),
                  K(macro_row_desc));
    } else if (OB_FAIL(agg_row_writer_.init(
                   data_store_desc_->get_agg_meta_array(),
                   *macro_row_desc.aggregated_row_, allocator))) {
      STORAGE_LOG(WARN, "Fail to init aggregate row writer", K(ret));
    } else if (FALSE_IT(agg_row_upper_size = agg_row_writer_.get_serialize_data_size())) {
    } else if (OB_ISNULL(agg_row_buf = static_cast<char *>(
                             allocator.alloc(agg_row_upper_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory for agg row", K(ret),
                  K(agg_row_upper_size));
    } else if (OB_FAIL(agg_row_writer_.write_agg_data(
                   agg_row_buf, agg_row_upper_size, pos))) {
      STORAGE_LOG(WARN, "Fail to write aggregated data", K(ret));
    } else {
      macro_meta.val_.agg_row_buf_ = agg_row_buf;
      macro_meta.val_.agg_row_len_ = pos;
    }
  }
  return ret;
}

//===================== ObBaseIndexBlockBuilder(private) ================

int ObBaseIndexBlockBuilder::new_next_builder(ObBaseIndexBlockBuilder *&next_builder)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(level_ >= MAX_LEVEL_LIMIT)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected too high index block tree level", K(ret),
                K(level_));
  } else if (OB_ISNULL(
                 buf = allocator_->alloc(sizeof(ObBaseIndexBlockBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(next_builder = new (buf) ObBaseIndexBlockBuilder())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObBaseIndexBlockBuilder", K(ret));
  } else if (OB_FAIL(next_builder->init(*data_store_desc_, *index_store_desc_,
                                        *allocator_, macro_writer_,
                                        level_ + 1))) {
    STORAGE_LOG(WARN, "fail to init next builder", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator_->free(buf);
    buf = nullptr;
  }
  return ret;
}

int ObBaseIndexBlockBuilder::append_next_row(
    const ObMicroBlockDesc &micro_block_desc) {
  int ret = OB_SUCCESS;
  ObIndexBlockRowDesc next_row_desc(*index_store_desc_);
  if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block desc", K(ret), K(micro_block_desc));
  } else if (FALSE_IT(block_to_row_desc(micro_block_desc, next_row_desc))) {
  } else if (OB_FAIL(get_aggregated_row(next_row_desc))) {
    STORAGE_LOG(WARN, "fail to get aggregated row", K(ret));
  } else if (OB_ISNULL(next_level_builder_) &&
             OB_FAIL(new_next_builder(next_level_builder_))) {
    STORAGE_LOG(WARN, "new next builder error.", K(ret),
                K(next_level_builder_));
  } else if (OB_FAIL(next_level_builder_->append_row(next_row_desc))) {
    STORAGE_LOG(WARN, "fail to append index row", K(ret), K(next_row_desc));
  }
  return ret;
}

int ObBaseIndexBlockBuilder::append_micro_block(const ObIndexRowAggInfo &agg_info,
                                                const int64_t absolute_row_offset,
                                                ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;
  ObIndexBlockRowDesc row_desc(*index_store_desc_);
  if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block desc", K(ret), K(micro_block_desc));
  } else if (OB_FAIL(macro_writer_->append_index_micro_block(micro_block_desc))) {
    micro_writer_->dump_diagnose_info(); // ignore dump error
    STORAGE_LOG(WARN, "fail to append index micro block", K(ret), K(micro_block_desc));
  } else if (FALSE_IT(block_to_row_desc(micro_block_desc, row_desc))) {
  } else if (FALSE_IT(row_desc.row_offset_ = absolute_row_offset)) {
  } else if (OB_FAIL(update_with_aggregate_info(agg_info, row_desc))) {
    STORAGE_LOG(WARN, "fail to update agg info", K(ret), K(micro_block_desc));
  } else if (OB_FAIL(append_row(row_desc))) {
    STORAGE_LOG(WARN, "fail to append index row", K(ret), K(row_desc));
  }
  return ret;
}

int ObBaseIndexBlockBuilder::insert_and_update_index_tree(const ObDatumRow *index_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid index builder", K(ret), K(is_inited_));
  } else if (OB_ISNULL(index_row) || OB_UNLIKELY(!index_row->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(index_row));
  } else {
    bool is_split = false;
    // For the leaf level of the index tree, micro block splitting never occurs
    // here; instead, splitting and appending happens during macro block flush.
    if (0 < micro_writer_->get_row_count() &&
        OB_FAIL(micro_block_adaptive_splitter_.check_need_split(
            micro_writer_->get_block_size(), micro_writer_->get_row_count(),
            index_store_desc_->get_micro_block_size(),
            macro_writer_->get_macro_data_size(), false /*is_keep_freespace*/,
            is_split))) {
      STORAGE_LOG(WARN, "Failed to check need split", K(ret),
                  "micro_block_size", index_store_desc_->get_micro_block_size(),
                  "current_macro_size", macro_writer_->get_macro_data_size(),
                  KPC(micro_writer_));
    } else if (is_split || OB_FAIL(micro_writer_->append_row(*index_row))) {
      if (OB_FAIL(ret) && OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret),
                    K(*index_row));
      } else if (OB_UNLIKELY(0 == micro_writer_->get_row_count())) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "The single row is too large, ", K(ret),
                    K(*index_row), KPC_(index_store_desc));
      } else if (OB_UNLIKELY(1 == micro_writer_->get_row_count())) {
        micro_writer_->dump_diagnose_info(); // ignore dump error
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN,
                    "row is too large to put more than one into micro block",
                    K(ret), K(*index_row), KPC_(index_store_desc));
      } else if (OB_FAIL(append_index_micro_block())) {
        STORAGE_LOG(WARN, "Fail to append index micro block, ", K(ret));
      } else if (OB_FAIL(micro_writer_->append_row(*index_row))) {
        STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret),
                    K(*index_row));
      }
    }
  }
  return ret;
}

//===================== ObDataIndexBlockBuilder ================
ObDataIndexBlockBuilder::ObDataIndexBlockBuilder()
  : ObBaseIndexBlockBuilder(),
    sstable_builder_(nullptr),
    task_allocator_("DataMidIdxTask"),
    meta_row_allocator_("DataMidIdxRow"),
    macro_meta_dumper_(),
    micro_helper_(),
    macro_row_desc_(),
    index_tree_root_ctx_(nullptr),
    meta_block_writer_(nullptr),
    meta_row_(),
    macro_meta_(meta_row_allocator_),
    cg_rowkey_(),
    leaf_store_desc_(nullptr),
    local_leaf_store_desc_(nullptr),
    clustered_index_writer_(nullptr),
    data_blocks_cnt_(0),
    meta_block_offset_(0),
    meta_block_size_(0),
    estimate_leaf_block_size_(0),
    estimate_meta_block_size_(0),
    micro_index_clustered_(false)
{
}

ObDataIndexBlockBuilder::~ObDataIndexBlockBuilder() { reset(); }

void ObDataIndexBlockBuilder::reset() {
  sstable_builder_ = nullptr;
  macro_meta_dumper_.reset();
  micro_helper_.reset();
  index_tree_root_ctx_ = nullptr;
  if (OB_NOT_NULL(meta_block_writer_)) {
    meta_block_writer_->~ObIMicroBlockWriter();
    task_allocator_.free(meta_block_writer_);
    meta_block_writer_ = nullptr;
  }
  if (OB_NOT_NULL(clustered_index_writer_)) {
    clustered_index_writer_->~ObClusteredIndexBlockWriter();
    clustered_index_writer_ = nullptr;
  }
  meta_row_.reset();
  cg_rowkey_.reset();
  leaf_store_desc_ = nullptr;
  if (OB_NOT_NULL(local_leaf_store_desc_)) {
    local_leaf_store_desc_->~ObDataStoreDesc();
    local_leaf_store_desc_ = nullptr;
  }
  data_blocks_cnt_ = 0;
  meta_block_offset_ = 0;
  meta_block_size_ = 0;
  estimate_leaf_block_size_ = 0;
  estimate_meta_block_size_ = 0;
  ObBaseIndexBlockBuilder::reset();
  meta_row_allocator_.reset();
  task_allocator_.reset();
}

int ObDataIndexBlockBuilder::init(const ObDataStoreDesc &data_store_desc,
                                  ObSSTableIndexBuilder &sstable_builder,
                                  const blocksstable::ObMacroSeqParam &macro_seq_param,
                                  const share::ObPreWarmerParam &pre_warm_param,
                                  ObIMacroBlockFlushCallback *ddl_callback)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObDataStoreDesc *tmp_data_store_desc = nullptr;
  ObDataStoreDesc *index_store_desc = nullptr;
  ObDataStoreDesc *container_store_desc = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDataIndexBlockBuilder has been inited", K(ret));
  } else if (OB_UNLIKELY(data_store_desc.micro_index_clustered() != sstable_builder.micro_index_clustered())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init data index block builder, unexpected micro_index_clustered argument", K(ret),
             K(sstable_builder.micro_index_clustered()), K(data_store_desc.micro_index_clustered()));
  } else if (FALSE_IT(micro_index_clustered_ = data_store_desc.micro_index_clustered())) {
  } else if (OB_FAIL(sstable_builder.init_builder_ptrs(sstable_builder_,
                                                       tmp_data_store_desc,
                                                       index_store_desc,
                                                       leaf_store_desc_,
                                                       container_store_desc,
                                                       index_tree_root_ctx_))) {
    LOG_WARN("fail to init referemce pointer members", K(ret));
  } else if (OB_UNLIKELY(index_store_desc->get_row_store_type() != data_store_desc.get_row_store_type()
                         && (index_store_desc->get_row_store_type() == FLAT_ROW_STORE
                             || data_store_desc.get_row_store_type() == FLAT_ROW_STORE)
                         && !data_store_desc.is_force_flat_store_type_)) {
    // since n-1 micro block should keep format same with data_blocks
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expect row store type equal", K(ret), KPC(index_store_desc), K(data_store_desc));
  } else if (OB_FAIL(micro_helper_.open(*index_store_desc, task_allocator_))) {
    LOG_WARN("fail to open base writer", K(ret));
  } else if (OB_FAIL(meta_row_.init(task_allocator_, index_store_desc->get_row_column_count()))) {
    LOG_WARN("fail to init meta row", K(ret));
  } else if (FALSE_IT(index_tree_root_ctx_->task_type_ = index_store_desc->is_cg()
                                                             ? ObIndexBuildTaskType::MERGE_CG_TASK
                                                             : ObIndexBuildTaskType::MERGE_TASK)) {
  } else if (data_store_desc.is_force_flat_store_type_) {
    local_leaf_store_desc_ = nullptr;
    if (OB_ISNULL(local_leaf_store_desc_ = OB_NEWx(ObDataStoreDesc, &task_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc Data Store Desc", K(ret));
    } else if (OB_FAIL(local_leaf_store_desc_->shallow_copy(*index_store_desc))) {
      LOG_WARN("fail to assign leaf store desc", K(ret));
    } else if (FALSE_IT(local_leaf_store_desc_->force_flat_store_type())) {
    } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(local_leaf_store_desc_,
                                                              task_allocator_,
                                                              meta_block_writer_,
                                                              GCONF.micro_block_merge_verify_level))) {
      LOG_WARN("fail to init meta block writer", K(ret));
    } else if (FALSE_IT(local_leaf_store_desc_->micro_block_size_
                        = local_leaf_store_desc_->get_micro_block_size_limit())) {
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::init(data_store_desc,
                                                     *local_leaf_store_desc_,
                                                     task_allocator_,
                                                     nullptr,
                                                     0))) {
      LOG_WARN("fail to init base index builder", K(ret));
    } else if (OB_FAIL(macro_meta_dumper_.init(*index_store_desc,
                                               *container_store_desc,
                                               &sstable_builder,
                                               *index_tree_root_ctx_->allocator_,
                                               task_allocator_,
                                               !index_store_desc->is_cg(),
                                               sstable_builder_->enable_dump_disk()))) {
      LOG_WARN("fail to init macro meta dumper", K(ret));
    } else if (micro_index_clustered()) {
      if (OB_ISNULL(clustered_index_writer_ = OB_NEWx(ObClusteredIndexBlockWriter, &task_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc clustered index writer", K(ret));
      } else if (OB_FAIL(clustered_index_writer_->init(data_store_desc,
                                                       *local_leaf_store_desc_,
                                                       macro_seq_param,
                                                       pre_warm_param,
                                                       index_tree_root_ctx_,
                                                       task_allocator_,
                                                       ddl_callback))) {
        LOG_WARN("fail to init clustered index block writer", K(ret));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(local_leaf_store_desc_)) {
      local_leaf_store_desc_->~ObDataStoreDesc();
      task_allocator_.free(local_leaf_store_desc_);
      local_leaf_store_desc_ = nullptr;
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(clustered_index_writer_)) {
      clustered_index_writer_->~ObClusteredIndexBlockWriter();
      task_allocator_.free(clustered_index_writer_);
    }
  } else {
    if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(index_store_desc,
                                                       task_allocator_,
                                                       meta_block_writer_,
                                                       GCONF.micro_block_merge_verify_level))) {
      LOG_WARN("fail to init meta block writer", K(ret));
    } else if (OB_FAIL(
                   ObBaseIndexBlockBuilder::init(data_store_desc, *leaf_store_desc_, task_allocator_, nullptr, 0))) {
      LOG_WARN("fail to init base index builder", K(ret));
    } else if (OB_FAIL(macro_meta_dumper_.init(*index_store_desc,
                                               *container_store_desc,
                                               &sstable_builder,
                                               *index_tree_root_ctx_->allocator_,
                                               task_allocator_,
                                               !index_store_desc->is_cg(),
                                               sstable_builder_->enable_dump_disk()))) {
      LOG_WARN("fail to init macro meta dumper", K(ret));
    } else if (micro_index_clustered()) {
      if (OB_ISNULL(clustered_index_writer_ = OB_NEWx(ObClusteredIndexBlockWriter, &task_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc clustered index writer", K(ret));
      } else if (OB_FAIL(clustered_index_writer_->init(data_store_desc,
                                                       *leaf_store_desc_,
                                                       macro_seq_param,
                                                       pre_warm_param,
                                                       index_tree_root_ctx_,
                                                       task_allocator_,
                                                       ddl_callback))) {
        LOG_WARN("fail to init clustered index block writer", K(ret));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(clustered_index_writer_)) {
      clustered_index_writer_->~ObClusteredIndexBlockWriter();
      task_allocator_.free(clustered_index_writer_);
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::insert_and_update_index_tree(
    const ObDatumRow *index_row) {
  // insert n-1 level index row
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_row) || OB_UNLIKELY(!index_row->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(index_row));
  } else if (OB_FAIL(micro_writer_->append_row(*index_row))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret),
                  K(*index_row));
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::cal_macro_meta_block_size(
    const ObDatumRowkey &rowkey, int64_t &estimate_meta_block_size) {
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta macro_meta;
  const int64_t rowkey_cnt = rowkey.datum_cnt_;
  const int64_t column_cnt = data_store_desc_->get_row_column_count();
  if (OB_UNLIKELY(rowkey_cnt != meta_row_.count_ - 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected rowkey cnt equal", K(ret), K(rowkey_cnt),
                K(meta_row_), K(rowkey));
  } else if (OB_FAIL(macro_meta.end_key_.assign(rowkey.datums_,
                                                rowkey.datum_cnt_))) {
    STORAGE_LOG(WARN, "fail to assign end key for macro meta", K(ret),
                K(rowkey));
  } else {
    macro_meta.val_.rowkey_count_ = rowkey_cnt;
    macro_meta.val_.column_count_ = column_cnt;
    macro_meta.val_.compressor_type_ = data_store_desc_->get_compressor_type();
    macro_meta.val_.row_store_type_ = data_store_desc_->get_row_store_type();
    macro_meta.val_.logic_id_.logic_version_ =
        data_store_desc_->get_logical_version();
    macro_meta.val_.logic_id_.column_group_idx_ =
        data_store_desc_->get_table_cg_idx();
    macro_meta.val_.logic_id_.tablet_id_ =
        data_store_desc_->get_tablet_id().id();
    macro_meta.val_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID;
    meta_row_.reuse();
    meta_row_allocator_.reuse();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_meta.build_estimate_row(meta_row_,
                                                     meta_row_allocator_,
                                                     leaf_store_desc_->get_major_working_cluster_version()))) {
      STORAGE_LOG(WARN, "fail to build meta row",
                  K(ret), K(macro_meta), K(leaf_store_desc_->get_major_working_cluster_version()));
    } else {
      meta_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      meta_block_writer_->reuse();
      if (OB_FAIL(meta_block_writer_->append_row(meta_row_))) {
        STORAGE_LOG(WARN, "fail to append meta row", K(ret), K_(meta_row));
      } else {
        const int64_t max_agg_data_size = index_block_aggregator_.get_max_agg_size();
        const int64_t max_macro_block_bf_size
            = data_store_desc_->enable_macro_block_bloom_filter() ? 64 * 1024 /* max 64KB for bf */ : 0;
        // for cs_encoding, the estimate_block_size calculated by build_block
        // may less than the real block_size, because the the cs_encoding has
        // compression for string column and the real macro meta data may has
        // lower compression ratio than the fake estimated data, so here use
        // original block size for estimating.
        if (ObStoreFormat::is_row_store_type_with_cs_encoding(
                index_store_desc_->row_store_type_)) {
          estimate_meta_block_size =
              meta_block_writer_->get_original_size() + max_agg_data_size + max_macro_block_bf_size;
#ifdef OB_BUILD_TDE_SECURITY
          const int64_t encrypted_size =
              share::ObEncryptionUtil::encrypted_length(
                  static_cast<share::ObCipherOpMode>(
                      index_store_desc_->get_encrypt_id()),
                  estimate_meta_block_size);
          estimate_meta_block_size =
              max(estimate_meta_block_size, encrypted_size);
#endif
        } else {
          ObMicroBlockDesc meta_block_desc;
          if (OB_FAIL(meta_block_writer_->build_micro_block_desc(
                  meta_block_desc))) {
            STORAGE_LOG(WARN, "fail to build macro meta block", K(ret),
                        K_(meta_row));
          } else if (FALSE_IT(meta_block_desc.last_rowkey_ = rowkey)) {
          } else if (OB_UNLIKELY(!meta_block_desc.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected meta block desc", K(ret),
                        K(meta_block_desc));
          } else {
            estimate_meta_block_size = meta_block_desc.buf_size_ +
                                       meta_block_desc.header_->header_size_ +
                                       max_agg_data_size +
                                       max_macro_block_bf_size;
#ifdef OB_BUILD_TDE_SECURITY
            const int64_t encrypted_size =
                share::ObEncryptionUtil::encrypted_length(
                    static_cast<share::ObCipherOpMode>(
                        index_store_desc_->get_encrypt_id()),
                    estimate_meta_block_size);
            estimate_meta_block_size =
                max(estimate_meta_block_size, encrypted_size);
#endif
          }
        }
      }
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::add_row_offset(ObIndexBlockRowDesc &row_desc) {
  int ret = OB_SUCCESS;
  ObDatumRowkey &cg_rowkey = row_desc.row_key_;
  cg_rowkey.reset();
  cg_rowkey_.set_int(row_desc.row_count_ + row_offset_); // start from -1
  if (OB_FAIL(cg_rowkey.assign(&cg_rowkey_, 1))) {
    STORAGE_LOG(WARN, "fail to assign rowkey with row", K(ret), K_(cg_rowkey));
  }
  return ret;
}

bool ObDataIndexBlockBuilder::micro_index_clustered() const
{
  return data_store_desc_->micro_index_clustered();
}

int ObDataIndexBlockBuilder::append_row(const ObMicroBlockDesc &micro_block_desc,
                                         const ObMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  int64_t estimate_meta_block_size = 0;
  ObIndexBlockRowDesc row_desc(*data_store_desc_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid index builder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block desc", K(ret), K(micro_block_desc));
  } else if (FALSE_IT(block_to_row_desc(micro_block_desc, row_desc))) {
  } else if (index_store_desc_->is_cg() && OB_FAIL(add_row_offset(row_desc))) {
    STORAGE_LOG(WARN, "fail to add row offset column", K(ret));
  } else {
    row_desc.is_data_block_ = true; // mark data block
    row_desc.micro_block_count_ = 1;
    row_desc.row_offset_ = row_offset_ + micro_block_desc.row_count_;
    const int64_t cur_data_block_size =
        micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
    int64_t remain_size = macro_block.get_remain_size() - cur_data_block_size;
    if (remain_size <= 0) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(cal_macro_meta_block_size(row_desc.row_key_,
                                                 estimate_meta_block_size))) {
      STORAGE_LOG(WARN, "fail to cal macro meta block size", K(ret),
                  K(micro_block_desc));
    } else if ((remain_size = remain_size - estimate_meta_block_size) <= 0) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (FALSE_IT(
                   micro_writer_->set_block_size_upper_bound(remain_size))) {
      // remain_size is set to be block_size_upper_bound of n-1 level
      // micro_writer
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::append_row(row_desc))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(WARN, "fail to append row", K(ret), K(micro_block_desc),
                    K(row_desc));
      } else {
        STORAGE_LOG(DEBUG, "succeed to prevent append_row", K(ret),
                    K(macro_block.get_remain_size()), K(cur_data_block_size),
                    K(estimate_meta_block_size), K(remain_size));
      }
    } else {
      // only update these two variables when succeeded to append data micro
      // block
      estimate_leaf_block_size_ = micro_writer_->get_original_size();
      estimate_meta_block_size_ = estimate_meta_block_size;
    }

    // Append index row into clustered index block writer.
    if (OB_SUCC(ret) && micro_index_clustered()) {
      if (OB_FAIL(clustered_index_writer_->append_row(row_desc))) {
        STORAGE_LOG(WARN, "fail to append row to clustered index block", K(ret), K(row_desc));
      }
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::append_macro_block(
    const ObDataMacroBlockMeta &macro_meta,
    const ObMicroBlockData *micro_block_data)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDesc *clustered_micro_desc = nullptr;
  const ObMicroIndexInfo *clustered_micro_index_info = nullptr;
  const uint64_t data_version = ObBaseIndexBlockBuilder::get_data_version(*leaf_store_desc_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro block desc", K(ret), K(macro_meta));
  } else {
    meta_row_.reuse();
    meta_row_allocator_.reuse();
    if (OB_FAIL(macro_meta.build_row(meta_row_, meta_row_allocator_, data_version))) {
      STORAGE_LOG(WARN, "fail to build row", K(ret), K(macro_meta), K(data_version));
    } else if (OB_FAIL(macro_meta_dumper_.append_row(meta_row_))) {
      STORAGE_LOG(WARN, "failed to append row to index block dumper", K(ret), K(meta_row_));
    } else if (micro_index_clustered()) {
      // Reuse clustered index micro block when `micro_index_clustered` enabled.
      if (OB_UNLIKELY(nullptr == micro_block_data)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to append macro block, unexpected nullptr argument",
                 K(ret), KP(micro_block_data));
      } else if (OB_FAIL(clustered_index_writer_->reuse_clustered_micro_block(
                     macro_meta.get_macro_id(), *micro_block_data))) {
        STORAGE_LOG(WARN, "fail to reuse clustered index micro block", K(ret), KPC(micro_block_data));
      }
    }
    if (OB_SUCC(ret)) {
      ++data_blocks_cnt_;
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::write_meta_block(
    ObMacroBlock &macro_block, const MacroBlockId &block_id,
    const ObIndexBlockRowDesc &macro_row_desc,
    const int64_t ddl_start_row_offset)
{
  int ret = OB_SUCCESS;
  int64_t data_offset = 0;
  ObMicroBlockDesc meta_block_desc; // meta block
  macro_meta_.reset();
  meta_block_writer_->reuse();
  meta_row_.reuse();
  meta_row_allocator_.reuse();
  const uint64_t data_version = ObBaseIndexBlockBuilder::get_data_version(*leaf_store_desc_);

  if (OB_UNLIKELY(!macro_row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro row desc", K(ret), K(macro_row_desc));
  } else if (OB_FAIL(macro_block.get_macro_block_meta(macro_meta_))) {
    STORAGE_LOG(WARN, "fail to get macro block meta", K(ret), K_(macro_meta));
  } else if (OB_UNLIKELY(macro_meta_.val_.micro_block_count_ !=
                         macro_row_desc.micro_block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "check micro block count failed", K(ret), K_(macro_meta),
                K(macro_row_desc));
  } else if (FALSE_IT(macro_meta_.end_key_ =
                          last_rowkey_)) { // fill rowkey for cg
  } else if (FALSE_IT(update_macro_meta_with_offset(macro_block.get_row_count(),
                                                    ddl_start_row_offset))) {
  } else if (OB_FAIL(row_desc_to_meta(macro_row_desc, macro_meta_,
                                      meta_row_allocator_))) {
    STORAGE_LOG(WARN, "fail to convert macro row descriptor to meta", K(ret),
                K(macro_row_desc));
  } else if (OB_FAIL(macro_meta_.build_row(meta_row_, meta_row_allocator_, data_version))) {
    STORAGE_LOG(WARN, "fail to build row", K(ret), K_(macro_meta), K(data_version));
  } else if (OB_FAIL(meta_block_writer_->append_row(meta_row_))) {
    STORAGE_LOG(WARN, "fail to append macro row", K(ret), K(meta_row_));
  } else if (OB_FAIL(meta_block_writer_->build_micro_block_desc(meta_block_desc))) {
    STORAGE_LOG(WARN, "fail to build meta block", K(ret));
  } else if (FALSE_IT(meta_block_desc.last_rowkey_ = last_rowkey_)) {
  } else if (OB_FAIL(micro_helper_.compress_encrypt_micro_block(meta_block_desc,
                                                                macro_block.get_current_macro_seq(),
                                                                macro_block.get_data_size()))) {
    STORAGE_LOG(WARN, "fail to compress and encrypt micro block", K(ret));
  } else if (OB_FAIL(macro_block.write_index_micro_block(meta_block_desc, false, data_offset))) {
    STORAGE_LOG(WARN, "fail to write meta index block", K(ret),
                K(meta_block_desc));
  }
  if (OB_SUCC(ret)) {
    macro_meta_.val_.macro_id_ = block_id; // real macro id
    meta_block_offset_ = data_offset;
    meta_block_size_ = meta_block_desc.get_block_size();
    if (OB_FAIL(macro_meta_.build_row(meta_row_, meta_row_allocator_, data_version))) {
      STORAGE_LOG(WARN, "fail to build row", K(ret), K_(macro_meta), K(data_version));
    } else if (OB_FAIL(macro_meta_dumper_.append_row(meta_row_))) {
      STORAGE_LOG(WARN, "failed to append row to index block dumper", K(ret), K_(macro_meta));
    } else {
      // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
      share::ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "succeed to write macro meta in macro block", K(ret),
                  K(macro_meta_));
    }
  }
  return ret;
}

void ObDataIndexBlockBuilder::update_macro_meta_with_offset(
    const int64_t macro_block_row_count, const int64_t ddl_start_row_offset) {
  if (leaf_store_desc_->get_major_working_cluster_version() >=
      DATA_VERSION_4_3_1_0) {
    if (ddl_start_row_offset >= 0) {
      macro_meta_.val_.ddl_end_row_offset_ =
          ddl_start_row_offset + macro_block_row_count - 1;
    } else {
      macro_meta_.val_.ddl_end_row_offset_ = -1 /*default*/;
    }
  } else {
    macro_meta_.val_.version_ = ObDataBlockMetaVal::DATA_BLOCK_META_VAL_VERSION;
  }
}

int ObDataIndexBlockBuilder::append_index_micro_block_and_macro_meta(
    ObMacroBlock &macro_block,
    const MacroBlockId &block_id,
    const int64_t ddl_start_row_offset)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMicroBlockDesc leaf_block_desc; // n-1 level index block
  int64_t data_offset = 0;
  int64_t leaf_block_size = 0;
  // Build index micro block and append to data macro block.
  if (OB_FAIL(build_index_micro_block(leaf_block_desc))) {
    STORAGE_LOG(WARN, "fail to build n-1 level micro block", K(ret));
  } else {
    if (OB_FAIL(micro_helper_.compress_encrypt_micro_block(
            leaf_block_desc, macro_block.get_current_macro_seq(),
            macro_block.get_data_size()))) {
      STORAGE_LOG(WARN, "fail to compress and encrypt micro block", K(ret));
    } else if (OB_FAIL(macro_block.write_index_micro_block(
                   leaf_block_desc, true, data_offset))) {
      STORAGE_LOG(WARN, "fail to write n-1 level index block", K(ret),
                  K(leaf_block_desc));
    } else {
      leaf_block_desc.macro_id_ = block_id;
      leaf_block_desc.block_offset_ = data_offset;
      leaf_block_size = leaf_block_desc.get_block_size();
    }
  }

  // Build macro meta row and append to data macro block.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_macro_meta_row_desc(leaf_block_desc, macro_row_desc_))) {
    STORAGE_LOG(WARN, "fail to append next row", K(ret), K(leaf_block_desc));
  } else if (OB_UNLIKELY(block_id != macro_row_desc_.macro_id_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "expect macro id equal", K(ret), K(leaf_block_desc), K_(macro_row_desc));
  // Set macro id to special value (DEFAULT_IDX_ROW_MACRO_ID) for macro meta
  // at the end of the data macro block.
  } else if (FALSE_IT(macro_row_desc_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID)) {
  } else if (OB_FAIL(write_meta_block(macro_block, block_id, macro_row_desc_, ddl_start_row_offset))) {
    STORAGE_LOG(WARN, "fail to build meta block", K(ret));
  } else {
    index_tree_root_ctx_->last_macro_size_ =
        data_offset + leaf_block_size + meta_block_size_;
  }

  if (OB_SUCC(ret) && micro_index_clustered()) {
    // Build clustered index micro block and append to clustered index block writer.
    if (OB_FAIL(clustered_index_writer_->build_and_append_clustered_index_micro_block())) {
      STORAGE_LOG(WARN, "fail to write clustered index micro block", K(ret));
    }
  }

  if (OB_FAIL(ret) && OB_BUF_NOT_ENOUGH == ret) {
    STORAGE_LOG(
        WARN, "error!!!fail to write leaf/meta block into data macro block",
        K(ret), K_(estimate_leaf_block_size), K_(estimate_meta_block_size),
        K(leaf_block_desc), K_(macro_row_desc));
    STORAGE_LOG(INFO, "print error leaf block");
    micro_writer_->dump_diagnose_info();
    STORAGE_LOG(INFO, "print error meta block");
    meta_block_writer_->dump_diagnose_info();
    if (common::ObStoreFormat::is_row_store_type_with_encoding(
            macro_block.get_row_store_type())) {
      ret = OB_ENCODING_EST_SIZE_OVERFLOW;
      STORAGE_LOG(WARN,
                  "build macro block failed by probably estimated maximum "
                  "encoding data size overflow",
                  K(ret));
    }
  }
  clean_status();
  return ret;
}

int ObDataIndexBlockBuilder::set_parallel_task_idx(const int64_t task_idx) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_tree_root_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null index_tree_root_ctx_", K(ret));
  } else {
    index_tree_root_ctx_->task_idx_ = task_idx;
  }
  return ret;
}

int ObDataIndexBlockBuilder::generate_macro_row(
    ObMacroBlock &macro_block, const MacroBlockId &block_id,
    const int64_t ddl_start_row_offset) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid index builder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro block id", K(block_id),
                K(ddl_start_row_offset));
  } else if (OB_UNLIKELY(!macro_block.is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid empty macro block", K(ret));
  } else if (OB_FAIL(append_index_micro_block_and_macro_meta(macro_block, block_id, ddl_start_row_offset))) {
    STORAGE_LOG(WARN, "fail to append n-1 level micro block", K(ret));
  } else {
    ++data_blocks_cnt_;
    row_offset_ = -1; // row offset starts from -1 for each data macro block
    if (index_store_desc_->is_cg()) {
      last_rowkey_.reset(); // do not compare row offset column
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::close(const ObDatumRowkey &last_key,
                                   ObMacroBlocksWriteCtx *data_write_ctx) {
  int ret = OB_SUCCESS;
  ObBaseIndexBlockBuilder *root_builder = nullptr;
  int64_t row_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid index builder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data index builder is closed", K(ret), K(is_closed_));
  } else if (OB_ISNULL(data_write_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data write ctx must be not-null", K(ret), K(data_write_ctx));
  } else if (OB_UNLIKELY(index_block_aggregator_.get_row_count() < 0)) {
    LOG_DEBUG("this partial index tree is empty", K(ret));
  } else if (OB_FAIL(close_index_tree(root_builder))) {
    LOG_WARN("fail to close index tree", K(ret));
  } else if (OB_UNLIKELY(root_builder != this)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObDataIndexBlockBuilder should not grow", K(ret), K(root_builder), K(this));
  } else if (OB_UNLIKELY(row_count = get_row_count()) > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("generate_macro_row should flush all index rows", K(ret), K(row_count));
  }
  // Close clustered index block writer.
  if (OB_FAIL(ret)) {
  } else if (!micro_index_clustered()) {
    // do nothing.
  } else if (OB_ISNULL(clustered_index_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to close data index block builder, unexpected clustered index writer",
             K(ret), KP(clustered_index_writer_));
  } else if (OB_FAIL(clustered_index_writer_->close())) {
    LOG_WARN("fail to close clustered index block writer", K(ret));
  }
  // Append index tree root ctx to sstable builder.
  if (OB_FAIL(ret)) {
  } else if (0 == macro_meta_dumper_.get_row_count()) {
    // do not append root to sstable builder since it's empty
  } else if (OB_FAIL(data_write_ctx->deep_copy(
      index_tree_root_ctx_->data_write_ctx_, *index_tree_root_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "Fail to copy data write ctx", K(ret));
  } else if (OB_FAIL(macro_meta_dumper_.close(index_tree_root_ctx_->meta_block_info_))) {
    STORAGE_LOG(WARN, "Fail to close macro meta dumper", K(ret));
  } else {
    const int64_t blocks_cnt = index_tree_root_ctx_->meta_block_info_.get_row_count();
    if (blocks_cnt == 1) {
      index_tree_root_ctx_->meta_block_size_ = meta_block_size_;
      index_tree_root_ctx_->meta_block_offset_ = meta_block_offset_;
    }
    if (index_tree_root_ctx_->meta_block_info_.in_mem()) {
      index_tree_root_ctx_->last_key_ = index_tree_root_ctx_->meta_block_info_.micro_block_desc_->last_rowkey_; // no need deep_copy
    } else if (OB_FAIL(macro_meta_dumper_.get_last_rowkey().deep_copy(index_tree_root_ctx_->last_key_, *index_tree_root_ctx_->allocator_))) {
      STORAGE_LOG(WARN, "Fail to deep copy macro meta dumper", K(ret));
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "succeed to close data index builder", KPC_(index_tree_root_ctx));
    }
  }
  is_closed_ = true; // close() is not re-entrant
  return ret;
}

int ObDataIndexBlockBuilder::generate_macro_meta_row_desc(
    const ObMicroBlockDesc &micro_block_desc,
    ObIndexBlockRowDesc &macro_row_desc)
{
  int ret = OB_SUCCESS;
  macro_row_desc.set_data_store_desc(index_store_desc_);
  if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block desc", K(ret), K(micro_block_desc));
  } else if (FALSE_IT(block_to_row_desc(micro_block_desc, macro_row_desc))) {
  } else if (OB_FAIL(get_aggregated_row(macro_row_desc))) {
    STORAGE_LOG(WARN, "fail to get aggregated row", K(ret));
  } else {
    macro_row_desc.is_macro_node_ = true;
    macro_row_desc.row_key_ = micro_block_desc.last_rowkey_;
    macro_row_desc.macro_block_count_ = 1;
  }
  return ret;
}

//===================== ObMetaIndexBlockBuilder ================
ObMetaIndexBlockBuilder::ObMetaIndexBlockBuilder()
    : ObBaseIndexBlockBuilder(), micro_writer_(nullptr), macro_writer_(nullptr),
      last_leaf_rowkey_(), leaf_rowkey_allocator_("MetaMidIdx"),
      meta_row_offset_(-1) {}

ObMetaIndexBlockBuilder::~ObMetaIndexBlockBuilder() { reset(); }

void ObMetaIndexBlockBuilder::reset() {
  if (OB_NOT_NULL(micro_writer_)) {
    micro_writer_->~ObIMicroBlockWriter();
    micro_writer_ = nullptr;
  }
  macro_writer_ = nullptr;
  last_leaf_rowkey_.reset();
  leaf_rowkey_allocator_.reset();
  meta_row_offset_ = -1;
  ObBaseIndexBlockBuilder::reset();
}

int ObMetaIndexBlockBuilder::init(ObDataStoreDesc &data_store_desc,
                                  ObIAllocator &allocator,
                                  ObMacroBlockWriter &macro_writer) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMetaIndexBlockBuilder has been inited", K(ret));
  } else if (OB_FAIL(ObBaseIndexBlockBuilder::init(data_store_desc,
                                                   data_store_desc, allocator,
                                                   &macro_writer, 0))) {
    // For meta tree, data and index share the same descriptor
    STORAGE_LOG(WARN, "fail to init base index builder", K(ret));
  } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(
                 &data_store_desc, allocator, micro_writer_))) {
    STORAGE_LOG(WARN, "fail to build micro writer", K(ret));
  } else {
    macro_writer_ = &macro_writer;
  }
  return ret;
}

int ObMetaIndexBlockBuilder::build_micro_block(
    ObMicroBlockDesc &micro_block_desc) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == micro_writer_->get_row_count())) {
    STORAGE_LOG(DEBUG, "build empty micro block", K(ret));
  } else if (OB_FAIL(micro_writer_->build_micro_block_desc(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to build micro block", K(ret));
  } else {
    micro_block_desc.last_rowkey_ = last_leaf_rowkey_;
  }
  return ret;
}

int ObMetaIndexBlockBuilder::append_micro_block(
    ObMicroBlockDesc &micro_block_desc) {
  int ret = OB_SUCCESS;
  ObIndexBlockRowDesc row_desc(*index_store_desc_);
  if (OB_FAIL(macro_writer_->append_index_micro_block(micro_block_desc))) {
    micro_writer_->dump_diagnose_info(); // ignore dump error
    STORAGE_LOG(WARN, "fail to append micro block of meta", K(ret),
                K(micro_block_desc));
  } else if (FALSE_IT(block_to_row_desc(micro_block_desc, row_desc))) {
  } else if (FALSE_IT(row_desc.is_data_block_ = true)) {
  } else if (FALSE_IT(row_desc.is_secondary_meta_ = true)) {
  } else if (FALSE_IT(row_desc.micro_block_count_ = 1)) {
  } else if (FALSE_IT(meta_row_offset_ += row_desc.micro_block_count_)) { // update meta row offset
  } else if (FALSE_IT(row_desc.row_offset_ = meta_row_offset_)) {
  } else if (OB_FAIL(ObBaseIndexBlockBuilder::append_row(row_desc))) {
    STORAGE_LOG(WARN, "fail to append n-1 level index row of meta", K(ret),
                K(row_desc));
  } else {
    micro_writer_->reuse();
    leaf_rowkey_allocator_.reuse();
  }
  return ret;
}

int ObMetaIndexBlockBuilder::append_reuse_micro_block(ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;
  ObIndexBlockRowDesc row_desc(*index_store_desc_);
  block_to_row_desc(micro_block_desc, row_desc);
  row_desc.is_data_block_ = true;
  row_desc.is_secondary_meta_ = true;
  row_desc.micro_block_count_ = 1;
  meta_row_offset_ += row_desc.row_count_; // update meta row offset
  row_desc.row_offset_ = meta_row_offset_;
  if (OB_FAIL(ObBaseIndexBlockBuilder::append_row(row_desc))) {
    STORAGE_LOG(WARN, "fail to append n-1 level index row of meta", K(ret), K(row_desc));
  }
  return ret;
}

int ObMetaIndexBlockBuilder::append_leaf_row(const ObDatumRow &leaf_row)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDesc micro_block_desc;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid ObMetaIndexBlockBuilder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!leaf_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(leaf_row));
  } else if (OB_FAIL(micro_writer_->append_row(leaf_row))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      STORAGE_LOG(WARN, "fail to append leaf row of meta", K(ret), K(leaf_row));
    } else if (OB_UNLIKELY(0 == micro_writer_->get_row_count())) {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "The single row is too large, ", K(ret), K(leaf_row));
    } else if (OB_FAIL(build_micro_block(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to build micro block of meta", K(ret),
                  K(leaf_row));
    } else if (OB_FAIL(append_micro_block(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to append micro block of meta to macro block",
                  K(ret), K(leaf_row));
    } else if (OB_FAIL(micro_writer_->append_row(leaf_row))) {
      STORAGE_LOG(WARN, "fail to append leaf row of meta", K(ret), K(leaf_row));
    }
  }
  if (OB_SUCC(ret)) {
    leaf_rowkey_allocator_.reuse();
    const int64_t rowkey_cnt = leaf_row.get_column_count() - 1;
    ObDatumRowkey src_rowkey;
    if (OB_UNLIKELY(rowkey_cnt < 1 ||
                    rowkey_cnt !=
                        index_store_desc_->get_rowkey_column_count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected rowkey cnt ", K(ret), K(rowkey_cnt),
                  KPC_(index_store_desc));
    } else if (OB_FAIL(
                   src_rowkey.assign(leaf_row.storage_datums_, rowkey_cnt))) {
      STORAGE_LOG(WARN, "Failed to assign src rowkey", K(ret), K(rowkey_cnt),
                  K(leaf_row));
    } else if (OB_FAIL(src_rowkey.deep_copy(last_leaf_rowkey_,
                                            leaf_rowkey_allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy rowkey", K(src_rowkey));
    }
  }
  return ret;
}

int ObMetaIndexBlockBuilder::close(
    ObIAllocator &allocator,
    const IndexTreeRootCtxList &roots,
    ObIndexBlockLoader &index_block_loader,
    const int64_t nested_size,
    const int64_t nested_offset,
    ObIndexTreeRootBlockDesc &block_desc)
{
  int ret = OB_SUCCESS;
  ObIndexTreeInfo tree_info;
  ObMicroBlockDesc micro_block_desc;
  int64_t single_root_tree_block_size_limit = ROOT_BLOCK_SIZE_LIMIT;
  bool allow_meta_in_tail = !roots[0]->is_backup_task(); // backup can't send read io
#ifdef ERRSIM
  int tp_ret = EN_SSTABLE_SINGLE_ROOT_TREE;
  if (OB_SIZE_OVERFLOW == tp_ret) {
    single_root_tree_block_size_limit = 64;
    FLOG_INFO("ERRSIM EN_SSTABLE_SINGLE_ROOT_TREE", K(tp_ret), K(single_root_tree_block_size_limit));
  } else if (OB_BUF_NOT_ENOUGH == tp_ret) {
    single_root_tree_block_size_limit = ROOT_BLOCK_SIZE_LIMIT * 10; // 160K
    FLOG_INFO("ERRSIM EN_SSTABLE_SINGLE_ROOT_TREE", K(tp_ret), K(single_root_tree_block_size_limit));
  }

  if (OB_SUCCESS != EN_SSTABLE_META_IN_TAIL && OB_SUCCESS != EN_COMPACTION_DISABLE_SHARED_MACRO) {
    allow_meta_in_tail = false;
    FLOG_INFO("EN_SSTABLE_META_IN_TAIL succeed", K(allow_meta_in_tail));
  }

  FLOG_INFO("errsim meta tree builder",
            K(ret), K(EN_SSTABLE_SINGLE_ROOT_TREE), K(EN_SSTABLE_META_IN_TAIL), K(EN_COMPACTION_DISABLE_SHARED_MACRO),
            K(single_root_tree_block_size_limit), K(allow_meta_in_tail));
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid ObMetaIndexBlockBuilder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta index builder is closed", K(ret), K(is_closed_));
  } else if (OB_FAIL(build_micro_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to build micro block of meta", K(ret));
  } else if (index_block_aggregator_.get_row_count() <= 0 &&
             micro_block_desc.get_block_size() <= single_root_tree_block_size_limit) {
    // Meta block's size is smaller than ROOT_BLOCK_SIZE_LIMIT, all meta data
    // will be stored in root.
    if (OB_FAIL(ObBaseIndexBlockBuilder::close(allocator, tree_info))) {
      STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
    } else if (OB_FAIL(build_single_node_tree(allocator, micro_block_desc,
                                              block_desc))) {
      STORAGE_LOG(WARN, "fail to build single node tree of meta", K(ret));
    }
  } else if (index_block_aggregator_.get_row_count() <= 0 && 1 == micro_block_desc.row_count_
             && allow_meta_in_tail) {
    // This sstable only has one data block, but the size of meta data exceeds ROOT_BLOCK_SIZE_LIMIT,
    // so sstable's root points to the tail of its data block (macro meta row).
    if (OB_FAIL(build_single_macro_row_desc(roots, index_block_loader, nested_size, nested_offset, allocator))) {
      STORAGE_LOG(WARN, "fail to build single marcro row descn", K(ret), K(nested_size), K(nested_offset));
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::close(allocator, tree_info))) {
      STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
    } else {
      block_desc = tree_info.root_desc_;
    }
  } else {
    // Multi-level meta tree, cannot be small sstable.
    if (OB_UNLIKELY(!(nested_offset == 0 && nested_size == OB_DEFAULT_MACRO_BLOCK_SIZE))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to build meta tree, should not be small sstable",
               K(ret), K(nested_offset), K(nested_size), K(common::lbt()));
    } else if (micro_block_desc.row_count_ > 0 && OB_FAIL(append_micro_block(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to append micro block of meta to macro block", K(ret));
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::close(allocator, tree_info))) {
      STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
    } else {
      block_desc = tree_info.root_desc_;
    }
  }

  if (OB_SUCC(ret)) {
    is_closed_ = true;
    STORAGE_LOG(DEBUG, "succeed to close index tree of meta", K(ret),
                K(block_desc));
  }
  return ret;
}

int ObMetaIndexBlockBuilder::build_single_macro_row_desc(
    const IndexTreeRootCtxList &roots,
    ObIndexBlockLoader &index_block_loader,
    const int64_t nested_size,
    const int64_t nested_offset,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObDataMacroBlockMeta macro_meta;
  if (OB_UNLIKELY(1 != roots.count() || 1 != roots[0]->meta_block_info_.get_row_count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected roots", K(ret), K(roots.count()), K(roots[0]->meta_block_info_.get_row_count()), KPC(roots[0]));
  } else if (OB_FAIL(ObSSTableIndexBuilder::get_single_macro_meta_for_small_sstable(
      leaf_rowkey_allocator_, index_block_loader, *index_store_desc_, roots, macro_meta))) {
    STORAGE_LOG(WARN, "fail to get single macro meta", K(ret));
  } else if (0 >= roots[0]->meta_block_size_ || 0 >= roots[0]->meta_block_offset_) {
    ObSSTableMacroBlockHeader macro_header;
    const char *data_buf = nullptr;
    if (OB_FAIL(ObSSTableIndexBuilder::load_single_macro_block(macro_meta, nested_size, nested_offset, allocator, data_buf))) {
      STORAGE_LOG(WARN, "fail to load macro block", K(ret), K(nested_size), K(nested_offset), K(macro_meta), KPC(roots[0]));
    } else if (OB_FAIL(ObSSTableIndexBuilder::parse_macro_header(
                   data_buf, OB_STORAGE_OBJECT_MGR.get_macro_block_size(),
                   macro_header))) {
      STORAGE_LOG(WARN, "fail to parse macro header", K(ret), KP(data_buf));
    } else {
      roots[0]->meta_block_offset_ =
          macro_header.fixed_header_.meta_block_offset_;
      roots[0]->meta_block_size_ = macro_header.fixed_header_.meta_block_size_;
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(data_desc.assign(*index_store_desc_))) {
    STORAGE_LOG(WARN, "fail to assign data desc", K(ret),
                KPC(index_store_desc_));
  } else {
    const ObDataBlockMetaVal &macro_meta_val = macro_meta.val_;
    ObStaticDataStoreDesc &static_desc = data_desc.get_static_desc();
    data_desc.get_desc().row_store_type_ = macro_meta_val.row_store_type_;
    static_desc.compressor_type_ = macro_meta_val.compressor_type_;
    static_desc.master_key_id_ = macro_meta_val.master_key_id_;
    static_desc.encrypt_id_ = macro_meta_val.encrypt_id_;
    MEMCPY(static_desc.encrypt_key_, macro_meta_val.encrypt_key_,
           share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    ObIndexBlockRowDesc row_desc(data_desc.get_desc());
    row_desc.row_key_ = macro_meta.end_key_;
    row_desc.macro_id_ = macro_meta_val.macro_id_;
    row_desc.block_offset_ = roots[0]->meta_block_offset_;
    row_desc.block_size_ = roots[0]->meta_block_size_;
    row_desc.row_count_ = macro_meta_val.row_count_;
    row_desc.row_count_delta_ = macro_meta_val.row_count_delta_;
    row_desc.is_deleted_ = macro_meta_val.is_deleted_;
    row_desc.max_merged_trans_version_ =
        macro_meta_val.max_merged_trans_version_;
    row_desc.contain_uncommitted_row_ = macro_meta_val.contain_uncommitted_row_;
    last_leaf_rowkey_.reset();
    row_desc.is_data_block_ = true;
    row_desc.is_secondary_meta_ = true;
    row_desc.micro_block_count_ = 1;
    if (OB_FAIL(ObBaseIndexBlockBuilder::append_row(row_desc))) {
      STORAGE_LOG(WARN, "fail to append n-1 level index row of meta", K(ret),
                  K(roots));
    }
  }
  return ret;
}

int ObMetaIndexBlockBuilder::build_single_node_tree(
    ObIAllocator &allocator, const ObMicroBlockDesc &micro_block_desc,
    ObIndexTreeRootBlockDesc &block_desc) {
  int ret = OB_SUCCESS;
  ObMetaDiskAddr &root_addr = block_desc.addr_;
  block_desc.height_ = 1;
  char *&root_buf = block_desc.buf_;
  const int64_t buf_size =
      micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
  int64_t pos = 0;
  if (OB_ISNULL(root_buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc root buf", K(ret), K(buf_size));
  } else if (OB_FAIL(micro_block_desc.header_->serialize(root_buf, buf_size,
                                                         pos))) {
    STORAGE_LOG(WARN, "fail to serialize header", K(ret), K(micro_block_desc));
  } else {
    MEMCPY(root_buf + pos, micro_block_desc.buf_, buf_size - pos);
    if (OB_FAIL(root_addr.set_mem_addr(0, buf_size))) {
      STORAGE_LOG(WARN, "fail to set memory address", K(ret), K(buf_size));
    } else {
      block_desc.is_meta_root_ = true;
      STORAGE_LOG(
          INFO,
          "successfully build single node tree, whose root is a data root",
          K(ret), K(block_desc));
    }
  }
  if (OB_FAIL(ret) && nullptr != root_buf) {
    allocator.free(root_buf);
    STORAGE_LOG(INFO, "succeed to close meta index builder", K(block_desc));
  }
  return ret;
}

//============================= ObIndexBlockRebuilder ==============================

ObIndexBlockRebuilder::ObIndexBlockRebuilder()
  : mutex_(common::ObLatchIds::INDEX_BUILDER_LOCK),
    index_tree_dumper_(nullptr),
    meta_tree_dumper_(nullptr),
    meta_row_(),
    task_allocator_("RebuMidIdxTask"),
    row_allocator_("RebuMidIdxRow"),
    macro_block_io_allocator_("RebuMidTdxMIO"),
    data_write_ctx_(),
    meta_store_desc_(),
    index_store_desc_(nullptr),
    index_tree_root_ctx_(nullptr),
    sstable_builder_(nullptr),
    device_handle_(nullptr),
    clustered_index_writer_(nullptr),
    is_inited_(false)
{
}

ObIndexBlockRebuilder::~ObIndexBlockRebuilder() { reset(); }

void ObIndexBlockRebuilder::reset()
{
  if (index_tree_dumper_ != nullptr) {
    index_tree_dumper_->~ObIndexTreeBlockDumper();
    task_allocator_.free(index_tree_dumper_);
    index_tree_dumper_ = nullptr;
  }
  if (meta_tree_dumper_ != nullptr) {
    meta_tree_dumper_->~ObBaseIndexBlockDumper();
    task_allocator_.free(meta_tree_dumper_);
    meta_tree_dumper_ = nullptr;
  }
  if (OB_NOT_NULL(clustered_index_writer_)) {
    clustered_index_writer_->~ObClusteredIndexBlockWriter();
    clustered_index_writer_ = nullptr;
  }
  meta_row_.reset();
  data_write_ctx_.reset();
  meta_store_desc_.reset();
  index_store_desc_ = nullptr;
  index_tree_root_ctx_ = nullptr;
  is_inited_ = false;
  sstable_builder_ = nullptr;
  task_allocator_.reset();
  macro_block_io_allocator_.reset();
  row_allocator_.reset();
}

void ObIndexBlockRebuilder::set_task_type(
    const bool is_cg,
    const bool use_absolute_offset,
    const common::ObIArray<ObIODevice *> *device_handle_array)
{
  if (device_handle_array != nullptr) {
    if (use_absolute_offset) {
      index_tree_root_ctx_->task_type_ = ObIndexBuildTaskType::REBUILD_BACKUP_DDL_TASK;
    } else {
      index_tree_root_ctx_->task_type_ = ObIndexBuildTaskType::REBUILD_BACKUP_TASK;
    }
  } else if (use_absolute_offset) {
    index_tree_root_ctx_->task_type_ = ObIndexBuildTaskType::REBUILD_DDL_TASK;
  } else if (is_cg) {
    index_tree_root_ctx_->task_type_ = ObIndexBuildTaskType::REBUILD_CG_SELF_CAL_TASK;
  } else {
    index_tree_root_ctx_->task_type_ = ObIndexBuildTaskType::REBUILD_NORMAL_TASK;
  }
}

OB_INLINE bool ObIndexBlockRebuilder::need_index_tree_dumper() const
{
  return index_tree_root_ctx_->is_backup_task() && sstable_builder_->enable_dump_disk();
}

int ObIndexBlockRebuilder::init(ObSSTableIndexBuilder &sstable_builder,
                                const int64_t *task_idx,
                                const ObITable::TableKey &table_key,
                                common::ObIArray<ObIODevice *> *device_handle_array)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc *data_store_desc = nullptr;
  ObDataStoreDesc *container_store_desc = nullptr;
  ObDataStoreDesc *leaf_store_desc = nullptr; //unused

  blocksstable::ObMacroSeqParam macro_seq_param;
  macro_seq_param.seq_type_ = blocksstable::ObMacroSeqParam::SeqType::SEQ_TYPE_INC;
  macro_seq_param.start_ = 0;
  // No need pre_warmer in rebuilder.
  share::ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIndexBlockRebuilder has been inited", K(ret));
  } else if (OB_FAIL(sstable_builder.init_builder_ptrs(sstable_builder_,
                                                       data_store_desc,
                                                       index_store_desc_,
                                                       leaf_store_desc,
                                                       container_store_desc,
                                                       index_tree_root_ctx_))) {
    LOG_WARN("fail to init reference pointer members", K(ret));
  } else if (OB_FAIL(meta_store_desc_.shallow_copy(*index_store_desc_))) {
    LOG_WARN("fail to assign leaf store desc", K(ret), KPC(index_store_desc_));
  } else if (OB_FAIL(meta_row_.init(task_allocator_, container_store_desc->get_row_column_count()))) {
    LOG_WARN("fail to init meta row", K(ret), K(container_store_desc->get_row_column_count()));
  } else if (FALSE_IT(set_task_type(index_store_desc_->is_cg(), use_absolute_offset(table_key), device_handle_array))) {
  } else if (index_tree_root_ctx_->is_backup_task()) {
    // device_handle_array size must be 2, and the 1st one is index tree, the 2nd one is meta tree
    if (OB_UNLIKELY(micro_index_clustered())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("micro_cluster_index is not support for backup task",
               K(ret), K(index_tree_root_ctx_->task_type_), KP(device_handle_array));
    } else if (OB_UNLIKELY(device_handle_array->count() != 2)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid device handle array", K(ret), "device count", device_handle_array->count());
    } else if (OB_ISNULL(meta_tree_dumper_ = OB_NEWx(ObBaseIndexBlockDumper, &task_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc meta tree dumper for rebuilder", K(ret));
    } else if (OB_FAIL(meta_tree_dumper_->init(*index_store_desc_,
                                               *container_store_desc,
                                               &sstable_builder,
                                               *index_tree_root_ctx_->allocator_,
                                               task_allocator_,
                                               true,
                                               sstable_builder_->enable_dump_disk(),
                                               device_handle_array->at(1)))) {
      LOG_WARN("fail to init meta tree dumper", K(ret));
    }

    if (OB_SUCC(ret) && need_index_tree_dumper()) {
      if (OB_ISNULL(index_tree_dumper_ = OB_NEWx(ObIndexTreeBlockDumper, &task_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc index tree dumper for rebuilder", K(ret));
      } else if (OB_FAIL(index_tree_dumper_->init(*data_store_desc,
                                                  *index_store_desc_,
                                                  &sstable_builder,
                                                  *container_store_desc,
                                                  *index_tree_root_ctx_->allocator_,
                                                  task_allocator_,
                                                  true,
                                                  sstable_builder_->enable_dump_disk(),
                                                  device_handle_array->at(0)))) {
        LOG_WARN("fail to init index tree dumper", K(ret));
      } else if (OB_ISNULL(index_tree_root_ctx_->data_blocks_info_
                           = OB_NEWx(ObDataBlockInfo, index_tree_root_ctx_->allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc data blocks info for root ctx", K(ret));
      } else if (OB_FAIL(index_tree_root_ctx_->data_blocks_info_->data_column_checksums_.reserve(
                     index_store_desc_->get_full_stored_col_cnt()))) {
        LOG_WARN("fail to reserve data column checksums", K(ret),
                 "column count", index_store_desc_->get_full_stored_col_cnt());
      } else {
        index_tree_root_ctx_->data_blocks_info_->data_column_cnt_ = index_store_desc_->get_full_stored_col_cnt();
        for (int64_t i = 0; OB_SUCC(ret) && i < index_store_desc_->get_full_stored_col_cnt(); i++) {
          if (OB_FAIL(index_tree_root_ctx_->data_blocks_info_->data_column_checksums_.push_back(0))) {
            LOG_WARN("failed to push column checksum", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(index_tree_root_ctx_->data_blocks_info_)) {
        index_tree_root_ctx_->data_blocks_info_->~ObDataBlockInfo();
        index_tree_root_ctx_->allocator_->free(index_tree_root_ctx_->data_blocks_info_);
        index_tree_root_ctx_->data_blocks_info_ = nullptr;
      }
      if (OB_NOT_NULL(index_tree_dumper_)) {
        index_tree_dumper_->~ObIndexTreeBlockDumper();
        task_allocator_.free(index_tree_dumper_);
        index_tree_dumper_ = nullptr;
      }
      if (OB_NOT_NULL(meta_tree_dumper_)) {
        meta_tree_dumper_->~ObBaseIndexBlockDumper();
        task_allocator_.free(meta_tree_dumper_);
        meta_tree_dumper_ = nullptr;
      }
    }
  } else {
    const bool need_check_order = ObIndexBuildTaskType::REBUILD_CG_SELF_CAL_TASK != index_tree_root_ctx_->task_type_;
    if (OB_ISNULL(meta_tree_dumper_ = OB_NEWx(ObBaseIndexBlockDumper, &task_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc index tree dumper for rebuilder", K(ret));
    } else if (OB_FAIL(meta_tree_dumper_->init(*index_store_desc_,
                                               *container_store_desc,
                                               &sstable_builder,
                                               *index_tree_root_ctx_->allocator_,
                                               task_allocator_,
                                               need_check_order,
                                               sstable_builder_->enable_dump_disk(),
                                               nullptr /* device_handle */))) {
      LOG_WARN("fail to init index block dumper", K(ret));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(meta_tree_dumper_)) {
        meta_tree_dumper_->~ObBaseIndexBlockDumper();
        task_allocator_.free(meta_tree_dumper_);
        meta_tree_dumper_ = nullptr;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    if (task_idx != nullptr) {
      if (*task_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected task idx value", K(ret), K(task_idx));
      } else {
        index_tree_root_ctx_->task_idx_ = *task_idx;
#ifdef OB_BUILD_SHARED_STORAGE
        if (GCTX.is_shared_storage_mode()) {
          macro_seq_param.start_ = index_tree_root_ctx_->task_idx_ * oceanbase::compaction::MACRO_STEP_SIZE;
        }
#endif
      }
    }
    // Init clustered index writer.
    if (OB_FAIL(ret)) {
    } else if (micro_index_clustered()) {
      if (OB_ISNULL(clustered_index_writer_ = OB_NEWx(ObClusteredIndexBlockWriter, &task_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc clustered index writer", K(ret));
      } else if (OB_FAIL(clustered_index_writer_->init(*index_store_desc_,
                                                       *leaf_store_desc,
                                                       macro_seq_param,
                                                       pre_warm_param,
                                                       index_tree_root_ctx_,
                                                       task_allocator_,
                                                       nullptr /* ddl_callback */))) {
        LOG_WARN("fail to init clustered index block writer", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succeed to init ObIndexBlockRebuilder",
               "task_idx", index_tree_root_ctx_->task_idx_,
               "task_type", index_tree_root_ctx_->task_type_,
               KP(device_handle_), KPC(index_store_desc_), KPC(container_store_desc), KP(&sstable_builder));
      is_inited_ = true;
    }
  }
  return ret;
}

int ObIndexBlockRebuilder::get_macro_meta(const char *buf, const int64_t size,
                                          const MacroBlockId &macro_id,
                                          common::ObIAllocator &allocator,
                                          ObDataMacroBlockMeta *&macro_meta) {
  int ret = OB_SUCCESS;
  ObSSTableMacroBlockHeader macro_header;
  if (OB_FAIL(inner_get_macro_meta(buf, size, macro_id, allocator, macro_meta,
                                   macro_header))) {
    STORAGE_LOG(WARN, "fail to get macro meta", K(ret));
  }
  return ret;
}

bool ObIndexBlockRebuilder::use_absolute_offset(const ObITable::TableKey &table_key)
{
  return table_key.is_ddl_merge_sstable()
    && !table_key.slice_range_.is_merge_slice(); // not ddl merge slice
}

int ObIndexBlockRebuilder::get_tablet_transfer_seq (int64_t &tablet_transfer_seq) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuilder is not inited", K(ret));
  } else if (OB_ISNULL(sstable_builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable_builder_ shoulde not be nullptr", K(ret), KP(sstable_builder_));
  } else {
    tablet_transfer_seq = sstable_builder_->get_tablet_transfer_seq();
  }
  return ret;
}

int ObIndexBlockRebuilder::inner_get_macro_meta(
    const char *buf, const int64_t size, const MacroBlockId &macro_id,
    common::ObIAllocator &allocator, ObDataMacroBlockMeta *&macro_meta,
    ObSSTableMacroBlockHeader &macro_header) {
  int ret = OB_SUCCESS;
  ObMacroBlockReader reader;
  // FIXME: macro_reader, micro_reader, read_info, datum_row could be used as
  // member variables to avoid repeatedly allocate memory and construct objects
  // and array. Fix if needed read info could be inited once protected by lock.
  // readers and row need to deal with concurrency
  ObMicroBlockReaderHelper micro_reader_helper;
  ObIMicroBlockReader *micro_reader;
  ObMicroBlockData micro_data;
  ObMicroBlockData meta_block;
  ObDatumRow datum_row;
  ObDataMacroBlockMeta tmp_macro_meta;
  ObDataMacroBlockMeta *tmp_meta_ptr = nullptr;
  bool is_compressed = false;
  if (OB_UNLIKELY(size <= 0 || !macro_id.is_valid()) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size),
                K(macro_id));
  } else if (OB_FAIL(get_meta_block(buf, size, allocator, macro_header,
                                    micro_data))) {
    STORAGE_LOG(WARN, "fail to get meta block and read info", K(ret), KP(buf),
                K(size));
  } else if (OB_FAIL(reader.decrypt_and_decompress_data(
                 macro_header, micro_data.get_buf(), micro_data.get_buf_size(),
                 meta_block.get_buf(), meta_block.get_buf_size(),
                 is_compressed))) {
    STORAGE_LOG(WARN, "fail to get micro block data", K(ret), K(macro_header),
                K(micro_data));
  } else if (OB_FAIL(micro_reader_helper.init(allocator))) {
    STORAGE_LOG(WARN, "fail to init micro reader helper", K(ret));
  } else if (OB_UNLIKELY(!meta_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid micro block data", K(ret), K(meta_block));
  } else if (OB_FAIL(micro_reader_helper.get_reader(meta_block.get_store_type(),
                                                    micro_reader))) {
    STORAGE_LOG(WARN, "fail to get micro reader by store type", K(ret),
                K(meta_block.get_store_type()));
  } else if (OB_FAIL(micro_reader->init(meta_block, nullptr))) {
    STORAGE_LOG(WARN, "fail to init micro reader", K(ret));
  } else if (OB_FAIL(datum_row.init(
                 allocator,
                 macro_header.fixed_header_.rowkey_column_count_ + 1))) {
    STORAGE_LOG(WARN, "fail to init datum row", K(ret));
  } else if (OB_FAIL(micro_reader->get_row(0, datum_row))) {
    STORAGE_LOG(WARN, "fail to get meta row", K(ret));
  } else if (OB_FAIL(tmp_macro_meta.parse_row(datum_row))) {
    STORAGE_LOG(WARN, "fail to parse meta row", K(ret), K(datum_row),
                K(macro_header), K(macro_id));
  } else {
    tmp_macro_meta.val_.macro_id_ = macro_id;
    if (OB_FAIL(tmp_macro_meta.deep_copy(tmp_meta_ptr, allocator))) {
      STORAGE_LOG(WARN, "fail to deep copy", K(ret), K(datum_row),
                  K(macro_header), K(tmp_macro_meta));
    } else {
      macro_meta = tmp_meta_ptr;
    }
  }
  return ret;
}

int ObIndexBlockRebuilder::check_and_get_abs_offset(
    const ObDataMacroBlockMeta &macro_meta,
    const int64_t absolute_row_offset,
    int64_t &abs_offset)
{
  int ret = OB_SUCCESS;
  switch (index_tree_root_ctx_->task_type_) {
    case REBUILD_NORMAL_TASK:
    case REBUILD_CG_SELF_CAL_TASK:
      abs_offset = -1;
      break;
    case REBUILD_DDL_TASK:
    case REBUILD_BACKUP_DDL_TASK:
      if (macro_meta.val_.version_ < ObDataBlockMetaVal::DATA_BLOCK_META_VAL_VERSION_V2
          || macro_meta.val_.ddl_end_row_offset_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpectd ddl end row offset", K(ret), K(macro_meta));
      } else {
        abs_offset = macro_meta.val_.ddl_end_row_offset_;
      }
      break;
    case REBUILD_BACKUP_TASK:
      if (absolute_row_offset < 0) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid absolute row offset", K(ret), K(absolute_row_offset), KPC(index_tree_root_ctx_));
      } else {
        abs_offset = absolute_row_offset;
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "the task type is invalid", K(ret), K(index_tree_root_ctx_->task_type_));
  }
  return ret;
}

int ObIndexBlockRebuilder::append_macro_row(
    const char *buf,
    const int64_t size,
    const MacroBlockId &macro_id,
    const int64_t absolute_row_offset)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDataMacroBlockMeta *macro_meta = nullptr;  // macro meta with true macro id
  ObSSTableMacroBlockHeader macro_header;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuilder not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(inner_get_macro_meta(buf, size, macro_id, allocator, macro_meta, macro_header))) {
    STORAGE_LOG(WARN, "fail to get macro meta", K(ret), K(macro_id));
  } else if (OB_ISNULL(macro_meta)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null macro meta", K(ret),K(macro_id));
  } else if (OB_UNLIKELY(!macro_meta->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro meta", K(ret), KPC(macro_meta));
  } else if (OB_FAIL(inner_append_macro_row(*macro_meta, absolute_row_offset))) {
    STORAGE_LOG(WARN, "fail to append macro meta", K(ret),
        K(absolute_row_offset), K(macro_id), KPC(macro_meta));
  } else {
    index_tree_root_ctx_->meta_block_offset_ =
        macro_header.fixed_header_.meta_block_offset_;
    index_tree_root_ctx_->meta_block_size_ =
        macro_header.fixed_header_.meta_block_size_;
    index_tree_root_ctx_->last_macro_size_ =
        index_tree_root_ctx_->meta_block_offset_ +
        index_tree_root_ctx_->meta_block_size_;
  }

  // Write clustered index micro block.
  if (OB_FAIL(ret)) {
  } else if (!micro_index_clustered()) {
  } else if (OB_FAIL(clustered_index_writer_->rewrite_and_append_clustered_index_micro_block(
                         buf, size, *macro_meta /* with true macro id */))) {
    LOG_WARN("fail to rewrite and append clustered index micro block", K(ret), K(macro_meta));
  }

  if (OB_NOT_NULL(macro_meta)) {
    macro_meta->~ObDataMacroBlockMeta();
    macro_meta = nullptr;
  }
  return ret;
}

int ObIndexBlockRebuilder::get_meta_block(
    const char *buf, const int64_t buf_size, common::ObIAllocator &allocator,
    ObSSTableMacroBlockHeader &macro_header, ObMicroBlockData &meta_block) {
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMacroBlockCommonHeader common_header;
  if (OB_UNLIKELY(buf_size <= 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(common_header.deserialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize common header", K(ret), KP(buf),
                K(buf_size), K(pos));
  } else if (OB_FAIL(common_header.check_integrity())) {
    STORAGE_LOG(WARN, "invalid common header", K(ret), K(common_header));
  } else if (OB_FAIL(macro_header.deserialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize macro header", K(ret), KP(buf),
                K(buf_size), K(pos));
  } else if (OB_UNLIKELY(!macro_header.is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid macro header", K(ret), K(macro_header));
  } else {
    meta_block.get_buf() = buf + macro_header.fixed_header_.meta_block_offset_;
    meta_block.get_buf_size() = macro_header.fixed_header_.meta_block_size_;
    STORAGE_LOG(DEBUG, "meta block and read info", K(macro_header));
  }
  return ret;
}

int ObIndexBlockRebuilder::add_macro_block_meta(const ObDataMacroBlockMeta &macro_meta,
                                                const int64_t absolute_row_offset)
{
  int ret = OB_SUCCESS;
  ObIndexBlockRowDesc row_desc(meta_store_desc_);
  row_allocator_.reuse();
  meta_row_.reuse();
  const uint64_t data_version = ObBaseIndexBlockBuilder::get_data_version(*index_store_desc_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "index tree root ctx not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(macro_meta.build_row(meta_row_, row_allocator_, data_version))) {
    STORAGE_LOG(WARN, "fail to build row", K(ret), K(macro_meta), K(data_version));
  } else if (OB_FAIL(meta_tree_dumper_->append_row(meta_row_))) {
    STORAGE_LOG(WARN, "failed to append row to index block dumper", K(ret), K(macro_meta));
  } else if (need_index_tree_dumper()) {
    if (OB_FAIL(ObBaseIndexBlockBuilder::meta_to_row_desc(
            macro_meta, meta_store_desc_, nullptr, row_desc))) {
      STORAGE_LOG(WARN, "fail to build row desc from macro meta", K(ret), K(macro_meta));
    } else if (FALSE_IT(row_desc.row_offset_ = absolute_row_offset)) {
    } else if (OB_FAIL(index_tree_dumper_->append_row(row_desc))) {
      STORAGE_LOG(WARN, "failed to append row to index block dumper", K(ret), K(macro_meta));
    } else if (OB_FAIL(collect_data_blocks_info(macro_meta))) {
      STORAGE_LOG(WARN, "failed to collect data block info", K(ret), K(macro_meta));
    }
  }
  return ret;
}

int ObIndexBlockRebuilder::append_macro_row(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuilder not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro meta", K(ret), K(macro_meta));
  } else if (OB_FAIL(inner_append_macro_row(macro_meta, -1/*absolute_row_offset*/))) {
    STORAGE_LOG(WARN, "fail to append macro meta", K(ret), K(macro_meta), KPC(index_tree_root_ctx_));
  }
  // Write clustered index micro block.
  if (OB_FAIL(ret)) {
  } else if (!micro_index_clustered()) {
  } else if (OB_FAIL(clustered_index_writer_->rewrite_and_append_clustered_index_micro_block(
                             macro_meta /* with true macro id */))) {
    LOG_WARN("fail to rewrite and append clustered index micro block", K(ret), K(macro_meta));
  }
  return ret;
}

int ObIndexBlockRebuilder::inner_append_macro_row(
    const ObDataMacroBlockMeta &macro_meta,
    const int64_t absolute_row_offset)
{
  int ret = OB_SUCCESS;
  int64_t abs_offset = -1;
  if (OB_FAIL(check_and_get_abs_offset(macro_meta, absolute_row_offset, abs_offset))) {
    STORAGE_LOG(WARN, "fail to check_and_get_abs_offset", K(ret),
        K(macro_meta), K(absolute_row_offset), KPC(index_tree_root_ctx_));
  } else {
    if (index_tree_root_ctx_->use_absolute_offset() && index_store_desc_->is_cg()) {
      macro_meta.end_key_.datums_[0].set_int(abs_offset);
    }
    lib::ObMutexGuard guard(mutex_); // migration will append concurrently
    if (OB_FAIL(add_macro_block_meta(macro_meta, abs_offset))) {
      STORAGE_LOG(WARN, "failed to add macro block meta", K(ret), K(macro_meta));
    } else if (OB_FAIL(data_write_ctx_.add_macro_block_id(macro_meta.val_.macro_id_))) { // inc_ref
      STORAGE_LOG(WARN, "failed to add block id", K(ret), K(macro_meta));
    } else if (index_tree_root_ctx_->use_absolute_offset() && OB_FAIL(index_tree_root_ctx_->add_absolute_row_offset(abs_offset))) {
      STORAGE_LOG(WARN, "failed to add abs row offset", K(ret), K(abs_offset));
    } else {
      STORAGE_LOG(DEBUG, "append macro meta with absolute offset",
          K(abs_offset), K(absolute_row_offset), K(macro_meta));
    }
  }
  return ret;
}

int ObIndexBlockRebuilder::collect_data_blocks_info(const ObDataMacroBlockMeta &meta)
{
  int ret = OB_SUCCESS;
  ObDataBlockInfo &data_blocks_info = *index_tree_root_ctx_->data_blocks_info_;
  if (OB_UNLIKELY(!meta.is_valid()
      || meta.get_meta_val().column_count_ > data_blocks_info.data_column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(meta), K(data_blocks_info.data_column_cnt_));
  } else if (OB_UNLIKELY(index_store_desc_->is_major_or_meta_merge_type()
        && !index_store_desc_->get_default_col_checksum_array_valid()
        && data_blocks_info.data_column_cnt_ > meta.get_meta_val().column_count_)) {
    // when default_col_checksum_array is invalid, need to make sure col_cnt in macro equal to col_cnt of result sstable
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "index store desc is invalid", K(ret), K_(index_store_desc),
      K(meta.get_meta_val().column_count_), K(data_blocks_info.data_column_cnt_));
  } else {
    for (int64_t i = 0; i < meta.get_meta_val().column_count_; ++i) {
      data_blocks_info.data_column_checksums_.at(i) += meta.val_.column_checksums_[i];
    }
    for (int64_t i = meta.get_meta_val().column_count_; i < data_blocks_info.data_column_cnt_; ++i) {
      data_blocks_info.data_column_checksums_.at(i) += meta.val_.row_count_
       * index_store_desc_->get_col_default_checksum_array().at(i);
    }
    data_blocks_info.occupy_size_ += meta.val_.occupy_size_;
    data_blocks_info.original_size_ += meta.val_.original_size_;
    data_blocks_info.micro_block_cnt_ += meta.val_.micro_block_count_;
    data_blocks_info.meta_data_checksums_.push_back(meta.val_.data_checksum_);
  }
  return ret;
}

bool ObIndexBlockRebuilder::micro_index_clustered() const
{
  return index_store_desc_->micro_index_clustered();
}

int ObIndexBlockRebuilder::close() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuilder not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(index_tree_dumper_ != nullptr &&
                         meta_tree_dumper_->get_row_count() != index_tree_dumper_->get_row_count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpect row count is not same", KPC(meta_tree_dumper_), KPC(index_tree_dumper_));
  } else if (nullptr != index_tree_root_ctx_->absolute_offsets_ &&
             index_tree_root_ctx_->absolute_offsets_->count() !=
                 meta_tree_dumper_->get_row_count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected row offset count not same with macro metas",
        K(ret), KPC(meta_tree_dumper_), KPC(index_tree_root_ctx_->absolute_offsets_));
  } else if (meta_tree_dumper_->get_row_count() == 0) {
    // do not append root to sstable builder since it's empty
  } else if (OB_FAIL(data_write_ctx_.deep_copy(
                 index_tree_root_ctx_->data_write_ctx_,
                 *index_tree_root_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy data write ctx", K(ret));
  } else if (need_index_tree_dumper() &&
             OB_FAIL(index_tree_dumper_->close(
                 index_tree_root_ctx_->index_tree_info_))) {
    STORAGE_LOG(WARN, "Fail to close index tree dumper", K(ret));
  } else if (OB_FAIL(meta_tree_dumper_->close(
                 index_tree_root_ctx_->meta_block_info_))) {
    STORAGE_LOG(WARN, "Fail to close meta dumper dumper", K(ret));
  } else if (OB_FAIL(meta_tree_dumper_->get_last_rowkey().deep_copy(
                 index_tree_root_ctx_->last_key_,
                 *index_tree_root_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "Fail to deep copy last rowkey", K(ret));
  } else if (!micro_index_clustered()) {
    // do nothing.
  } else if (OB_ISNULL(clustered_index_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to close data index block builder, unexpected clustered index writer",
             K(ret), KP(clustered_index_writer_));
  } else if (OB_FAIL(clustered_index_writer_->close())) {
    LOG_WARN("fail to close clustered index block writer", K(ret));
  }

  STORAGE_LOG(DEBUG, "close index block rebuilder",
              K(ret), KPC_(index_tree_root_ctx), KPC_(index_tree_dumper), KPC_(meta_tree_dumper));

  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
