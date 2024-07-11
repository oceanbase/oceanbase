/**
 *
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

#include "ob_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_imacro_block_flush_callback.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "lib/utility/utility.h"
#include "lib/checksum/ob_crc64.h"
#include "share/schema/ob_column_schema.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_encryption_util.h"
#include "storage/ob_storage_struct.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{

ObIndexTreeRootCtx::~ObIndexTreeRootCtx()
{
  reset();
}

void ObIndexTreeRootCtx::reset()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    for (int64_t j = 0; j < macro_metas_->count(); ++j) {
      ObDataMacroBlockMeta *&meta = macro_metas_->at(j);
      if (OB_NOT_NULL(meta)) {
        if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(meta->get_macro_id()))) {
          STORAGE_LOG(ERROR, "failed to dec macro block ref cnt", K(ret),
              K(macro_metas_->count()), K(j), "macro id", meta->get_macro_id());
        }
      }
      meta->~ObDataMacroBlockMeta();
      allocator_->free(static_cast<void*>(static_cast<void*>(meta)));
      meta = nullptr;
    }
    macro_metas_->~ObIArray<ObDataMacroBlockMeta*>();
    allocator_->free(static_cast<void*>(macro_metas_));
    macro_metas_ = nullptr;
    if (nullptr != absolute_offsets_) {
      absolute_offsets_->~ObIArray<int64_t>();
      allocator_->free(static_cast<void*>(absolute_offsets_));
      absolute_offsets_ = nullptr;
    }
    allocator_ = nullptr;
    is_inited_ = false;
  }
  last_key_.reset();
  task_idx_ = -1;
  data_column_cnt_ = 0;
  data_blocks_cnt_ = 0;
  use_old_macro_block_count_ = 0;
  meta_block_offset_ = 0;
  meta_block_size_ = 0;
  last_macro_size_ = 0;
  use_absolute_offset_ = false;
}

int ObIndexTreeRootCtx::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  void *array_buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "index tree root ctx inited twice", K(ret));
  } else if (OB_ISNULL(array_buf = allocator.alloc(sizeof(ObMacroMetasArray)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(macro_metas_ = new (array_buf) ObMacroMetasArray(sizeof(ObDataMacroBlockMeta *), ModulePageAllocator(allocator)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new ObMacroMetasArray", K(ret));
  } else {
    allocator_ = &allocator;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(array_buf)) {
    allocator.free(array_buf);
    array_buf = nullptr;
  }
  return ret;
}

bool ObIndexTreeRootCtx::is_absolute_vaild() const
{
  return (!use_absolute_offset_&& absolute_offsets_ == nullptr) ||
         (use_absolute_offset_ && absolute_offsets_ != nullptr);
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
    if (OB_ISNULL(array_buf = allocator_->alloc(sizeof(ObAbsoluteOffsetArray)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
    } else if (OB_ISNULL(absolute_offsets_ = new (array_buf) ObAbsoluteOffsetArray(sizeof(int64_t), ModulePageAllocator(*allocator_)))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to new ObMacroMetasArray", K(ret));
    }

    if(OB_FAIL(ret) && OB_NOT_NULL(array_buf)) {
      allocator_->free(array_buf);
      array_buf = nullptr;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(absolute_offsets_->push_back(absolute_row_offset))) {
    STORAGE_LOG(WARN, "fail to push back absolute row offset", K(ret), K(absolute_row_offset));
  }

  return ret;
}

int ObIndexTreeRootCtx::add_macro_block_meta(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta *dst_macro_meta = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "index tree root ctx not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(macro_meta.deep_copy(dst_macro_meta, *allocator_))) {
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(macro_meta));
  } else if (OB_FAIL(macro_metas_->push_back(dst_macro_meta))) {
    STORAGE_LOG(WARN, "fail to push back macro block merge info", K(ret));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(dst_macro_meta->get_macro_id()))) {
    macro_metas_->pop_back();
    STORAGE_LOG(ERROR, "failed to inc macro block ref cnt", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dst_macro_meta)) {
    dst_macro_meta->~ObDataMacroBlockMeta();
    allocator_->free(static_cast<void*>(dst_macro_meta));
    dst_macro_meta = nullptr;
  }
  return ret;
}

int ObIndexTreeRootCtx::get_macro_id_array(common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "index tree root ctx not inited", K(ret), K_(is_inited));
  } else {
    int64_t start = block_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_metas_->count(); ++i) {
      const MacroBlockId block_id = macro_metas_->at(i)->get_macro_id();
      if (OB_FAIL(block_ids.push_back(block_id))) {
        STORAGE_LOG(WARN, "failed to push back block id", K(ret));
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(block_id))) {
        block_ids.pop_back();
        STORAGE_LOG(ERROR, "failed to inc macro block ref cnt", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = block_ids.count() - 1; i >= start; --i) {
        const MacroBlockId block_id = block_ids.at(i);
        block_ids.pop_back();
        if (OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.dec_ref(block_id))) {
          STORAGE_LOG(ERROR, "fail to dec macro block ref cnt", K(tmp_ret), K(block_id), K(i));
        }
      }
    }
  }
  return ret;
}

int ObIndexTreeRootCtx::change_macro_id(const MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "index tree root ctx not inited", K(ret), K_(is_inited));
  } else if (macro_metas_->count() != 1){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "index tree root ctx macro meta count unexpected",
        K(ret), K(macro_metas_->count()));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_metas_->at(0)->get_macro_id()))) {
    STORAGE_LOG(ERROR, "failed to dec macro block ref cnt", K(ret),
        K(macro_metas_->count()), "macro id", macro_metas_->at(0)->get_macro_id());
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_block_id))) {
    STORAGE_LOG(ERROR, "failed to inc macro block ref cnt", K(ret), "macro id", macro_block_id);
  } else {
    macro_metas_->at(0)->val_.macro_id_ = macro_block_id;
  }
  return ret;
}

bool ObIndexTreeRootBlockDesc::is_valid() const
{
  return is_empty()
      || (addr_.is_valid()
        && (is_mem_type() ? (buf_ != nullptr) : (buf_ == nullptr))
        && height_ > 0);
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
    root_row_store_type_(ObRowStoreType::MAX_ROW_STORE)
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  if (share::is_reserve_mode()) {
    ObMemAttr attr(MTL_ID(), "SSTMrgeResArr", ObCtxIds::MERGE_RESERVE_CTX_ID);
    data_block_ids_.set_attr(attr);
    other_block_ids_.set_attr(attr);
    data_column_checksums_.set_attr(attr);
  }
}
ObSSTableMergeRes::~ObSSTableMergeRes()
{
  reset();
}

void ObSSTableMergeRes::reset()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < data_block_ids_.count(); ++i) {
    const MacroBlockId &block_id = data_block_ids_.at(i);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(block_id))) {
      // overwrite ret
      STORAGE_LOG(ERROR, "failed to dec macro block ref cnt",
          K(ret), "macro id", block_id);
    }
  }
  data_block_ids_.destroy();
  for (int64_t i = 0; i < other_block_ids_.count(); ++i) {
    const MacroBlockId &block_id = other_block_ids_.at(i);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(block_id))) {
      // overwrite ret
      STORAGE_LOG(ERROR, "failed to dec macro block ref cnt",
          K(ret), "macro id", block_id);
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
  root_row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
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
      && root_row_store_type_ < ObRowStoreType::MAX_ROW_STORE;
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
    MEMCPY(encrypt_key_, src.encrypt_key_, sizeof(encrypt_key_));

    if (OB_FAIL(data_block_ids_.reserve(src.data_block_ids_.count()))) {
      STORAGE_LOG(WARN, "failed to reserve count", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.data_block_ids_.count(); ++i) {
        const MacroBlockId &block_id = src.data_block_ids_.at(i);
        if (OB_FAIL(data_block_ids_.push_back(block_id))) {
          STORAGE_LOG(WARN, "failed to push back block id", K(ret));
        } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(block_id))) {
          data_block_ids_.pop_back(); // pop if inc_ref failed
          STORAGE_LOG(ERROR, "failed to inc macro block ref cnt",
              K(ret), "macro id", block_id);
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(other_block_ids_.reserve(src.other_block_ids_.count()))) {
      STORAGE_LOG(WARN, "failed to reserve count", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.other_block_ids_.count(); ++i) {
        const MacroBlockId &block_id = src.other_block_ids_.at(i);
        if (OB_FAIL(other_block_ids_.push_back(block_id))) {
          STORAGE_LOG(WARN, "failed to push back block id", K(ret));
        } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(block_id))) {
          other_block_ids_.pop_back(); // pop if inc_ref failed
          STORAGE_LOG(ERROR, "failed to inc macro block ref cnt",
              K(ret), "macro id", block_id);
        }
      }
    }
  }
  return ret;
}

int ObSSTableMergeRes::fill_column_checksum_for_empty_major(
    const int64_t column_count,
    common::ObIArray<int64_t> &column_checksums)
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

int ObSSTableMergeRes::prepare_column_checksum_array(const int64_t data_column_cnt)
{
  int ret = OB_SUCCESS;
  data_column_cnt_ = data_column_cnt;
  data_column_checksums_.reset();
  if (OB_UNLIKELY(0 == data_column_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected empty roots", K(ret));
  } else if (OB_FAIL(data_column_checksums_.reserve(data_column_cnt))) {
    STORAGE_LOG(WARN, "failed to reserve data_column_checksums_", K(ret), K(data_column_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_column_cnt; i++) {
      if (OB_FAIL(data_column_checksums_.push_back(0))) {
        STORAGE_LOG(WARN, "failed to push column checksum", K(ret));
      }
    }
  }
  return ret;
}

ObSSTableIndexBuilder::ObSSTableIndexBuilder()
  : sstable_allocator_("SSTMidIdxData"),
    self_allocator_("SSTMidIdxSelf"),
    row_allocator_("SSTMidIdxRow"),
    mutex_(common::ObLatchIds::INDEX_BUILDER_LOCK),
    data_store_desc_(),
    index_store_desc_(),
    leaf_store_desc_(),
    container_store_desc_(),
    index_row_(),
    index_builder_(),
    data_builder_(),
    macro_writer_(),
    callback_(nullptr),
    roots_(sizeof(ObIndexTreeRootCtx *), ModulePageAllocator(sstable_allocator_)),
    res_(),
    optimization_mode_(ENABLE),
    is_closed_(false),
    is_inited_(false)
{
}

ObSSTableIndexBuilder::~ObSSTableIndexBuilder()
{
  reset();
}

void ObSSTableIndexBuilder::reset()
{
  data_store_desc_.reset();
  index_store_desc_.reset();
  leaf_store_desc_.reset();
  container_store_desc_.reset();
  for (int64_t i = 0; i < roots_.count(); ++i) {
    roots_[i]->~ObIndexTreeRootCtx();
    sstable_allocator_.free(static_cast<void*>(roots_[i]));
  }
  index_row_.reset();
  index_builder_.reset();
  data_builder_.reset();
  macro_writer_.reset();
  callback_ = nullptr;
  roots_.reset();
  index_row_.reset();
  res_.reset();
  sstable_allocator_.reset();
  self_allocator_.reset();
  optimization_mode_ = ENABLE;
  is_closed_ = false;
  is_inited_ = false;
}

bool ObSSTableIndexBuilder::check_index_desc(const ObDataStoreDesc &index_desc) const
{
  bool ret = true;
  // these args influence write_micro_block and need to be evaluated
  if (!index_desc.is_valid()
      || index_desc.merge_info_ != nullptr
      || index_desc.get_row_column_count() != index_desc.get_rowkey_column_count() + 1
      || (index_desc.is_major_merge_type() && index_desc.get_major_working_cluster_version() < DATA_VERSION_4_0_0_0)) {
    ret = false;
  }
  return ret;
}
int ObSSTableIndexBuilder::init(const ObDataStoreDesc &data_desc,
                                ObIMacroBlockFlushCallback *callback,
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
  } else if (OB_FAIL(index_store_desc_.gen_index_store_desc(data_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to generate index store desc", K(ret), K_(data_store_desc));
  } else if (OB_UNLIKELY(!check_index_desc(index_store_desc_.get_desc()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid index store desc", K(ret), K(index_store_desc_), K(index_store_desc_.is_valid()));
  } else if (OB_FAIL(set_row_store_type(index_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to set row store type", K(ret));
  } else if (OB_FAIL(container_store_desc_.shallow_copy(index_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to assign container_store_desc", K(ret), K(index_store_desc_));
  } else if (OB_FAIL(index_row_.init(index_store_desc_.get_desc().get_rowkey_column_count() + 1))) {
    STORAGE_LOG(WARN, "Failed to init index row", K(ret), K(index_store_desc_));
  } else {
    index_store_desc_.get_desc().sstable_index_builder_ = this;
    callback_ = callback;
    optimization_mode_ = data_desc.is_cg() && data_desc.get_major_working_cluster_version() >= DATA_VERSION_4_3_2_0 ? DISABLE : mode;
    if (OB_FAIL(leaf_store_desc_.shallow_copy(index_store_desc_.get_desc()))) {
      STORAGE_LOG(WARN, "fail to assign leaf store desc", K(ret));
    } else {
      leaf_store_desc_.micro_block_size_ = leaf_store_desc_.get_micro_block_size_limit(); // nearly 2M
      is_inited_ = true;
    }
  }
  STORAGE_LOG(TRACE, "init sstable index builder", K(ret),
      K(data_desc), K_(index_store_desc), K_(leaf_store_desc));
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
                                             ObIAllocator &data_allocator)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr != builder)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid not null builder", K(ret), KP(builder));
  } else if (OB_ISNULL(buf = data_allocator.alloc(sizeof(ObDataIndexBlockBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(builder = new (buf) ObDataIndexBlockBuilder())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObDataIndexBlockBuilder", K(ret));
  } else if (OB_FAIL(builder->init(data_store_desc, *this))) {
    STORAGE_LOG(WARN, "fail to init index builder", K(ret));
  }
  return ret;
}

int ObSSTableIndexBuilder::init_builder_ptrs(
    ObSSTableIndexBuilder *&sstable_builder,
    ObDataStoreDesc *&index_store_desc,
    ObDataStoreDesc *&leaf_store_desc,
    ObIndexTreeRootCtx *&index_tree_root_ctx,
    ObMacroMetasArray *&macro_meta_list)
{

  int ret = OB_SUCCESS;
  void *desc_buf = nullptr;
  ObIndexTreeRootCtx *tmp_root_ctx = nullptr;
  ObMacroMetasArray *tmp_macro_meta_array = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_ISNULL(desc_buf = sstable_allocator_.alloc(sizeof(ObIndexTreeRootCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(tmp_root_ctx = new (desc_buf) ObIndexTreeRootCtx())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObIndexTreeRootCtx", K(ret));
  } else if (tmp_root_ctx->init(sstable_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to init a ObIndexTreeRootCtx", K(ret));
  } else {
    sstable_builder = this;
    index_store_desc = &index_store_desc_.get_desc();
    leaf_store_desc = &leaf_store_desc_;
    if (OB_FAIL(append_root(*tmp_root_ctx))) {
      STORAGE_LOG(WARN, "Fail to append index tree root ctx, ", K(ret), K(*tmp_root_ctx));
    } else {
      macro_meta_list = tmp_root_ctx->macro_metas_;
      index_tree_root_ctx = tmp_root_ctx;
      tmp_macro_meta_array = nullptr;
      tmp_root_ctx = nullptr;
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != tmp_macro_meta_array) {
      tmp_macro_meta_array->~ObIArray<ObDataMacroBlockMeta*>();
      tmp_macro_meta_array = nullptr;
    }

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
  if (0 == root_count) {
    // do nothing
  } else if (OB_FAIL(tmp_roots.reserve(root_count))) {
    STORAGE_LOG(WARN, "fail to reserve tmp roots", K(ret), K(root_count));
  } else {
    bool use_absolute_offset = false;
    for (int64_t i = 0; i < root_count && OB_SUCC(ret); ++i) {
      if (nullptr == roots_[i]) {
        // skip
      } else if (0 == roots_[i]->data_blocks_cnt_) {
        roots_[i]->~ObIndexTreeRootCtx();  // release
      } else {
        if (OB_UNLIKELY(!roots_[i]->is_absolute_vaild())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "invalid absolute offset", K(ret), K(i), KPC(roots_[i]));
        } else if (OB_FAIL(tmp_roots.push_back(roots_[i]))) {
          STORAGE_LOG(WARN, "fail to push back root", K(ret), KPC(roots_[i]));
        } else if (tmp_roots.count() == 1) {
          use_absolute_offset = tmp_roots.at(0)->use_absolute_offset_;
        } else if (OB_UNLIKELY((roots_[i]->use_absolute_offset_ != use_absolute_offset))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected not same use absolute offset",
              K(ret), K(i), K(use_absolute_offset), KPC(roots_[i]));
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
          tmp_roots[i]->~ObIndexTreeRootCtx();  // release
        }
      }
    }
  }
  return ret;
}
int ObSSTableIndexBuilder::ObMacroMetaIter::init(IndexTreeRootCtxList &roots, const bool is_cg)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "Init twice", K(ret));
  } else {
    int64_t row_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < roots.count(); ++i) {
      ObMacroMetasArray *macro_metas = roots[i]->macro_metas_;
      for (int64_t j = 0; OB_SUCC(ret) && j < macro_metas->count(); ++j) {
        ObDataMacroBlockMeta *macro_meta = macro_metas->at(j);
        if (OB_ISNULL(macro_meta)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null macro meta", K(ret), K(j), KPC(roots.at(i)));
        } else if (is_cg && FALSE_IT(
            macro_meta->end_key_.datums_[0].set_int(macro_meta->val_.row_count_ + row_idx))) {
          // use row_idx to rewrite endkey
        } else if (OB_FAIL(macro_metas_.push_back(macro_meta))) {
          STORAGE_LOG(WARN, "fail to push back macro meta", K(ret), KPC(macro_meta));
        } else {
          ++block_cnt_;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    cur_block_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableIndexBuilder::ObMacroMetaIter::get_next_macro_block(const ObDataMacroBlockMeta *&macro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret));
  } else if (cur_block_idx_ == block_cnt_) {
    ret = OB_ITER_END;
  } else {
    macro_meta = macro_metas_.at(cur_block_idx_++);
  }
  return ret;
}

int ObSSTableIndexBuilder::init_meta_iter(ObMacroMetaIter &iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_FAIL(trim_empty_roots())) {
    STORAGE_LOG(WARN, "fail to trim empty roots", K(ret));
  } else if (OB_FAIL(sort_roots())) {
    STORAGE_LOG(WARN, "fail to sort roots", K(ret));
  } else if (OB_FAIL(iter.init(roots_, index_store_desc_.get_desc().is_cg()))) {
    STORAGE_LOG(WARN, "fail to init iter", K(ret));
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
    ObIndexTreeRootCtxCompare cmp(ret, index_store_desc_.get_desc().get_datum_utils());
    lib::ob_sort(roots_.begin(), roots_.end(), cmp);
  }
  return ret;
}

int ObSSTableIndexBuilder::merge_index_tree(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq start_seq;
  ObWholeDataStoreDesc data_desc;
  start_seq.set_index_block();
  const bool use_absolute_offset = roots_[0]->use_absolute_offset_;
  if (OB_FAIL(macro_writer_.open(container_store_desc_, start_seq, callback_))) {
    STORAGE_LOG(WARN, "fail to open index macro writer", K(ret));
  } else if (OB_FAIL(index_builder_.init(
      data_store_desc_.get_desc(), index_store_desc_.get_desc(), self_allocator_, &macro_writer_, 1))) {
    STORAGE_LOG(WARN, "fail to new index builder", K(ret));
  } else if (OB_FAIL(data_desc.assign(index_store_desc_.get_desc()))) {
    STORAGE_LOG(WARN, "fail to assign data desc", K(ret), K_(index_store_desc));
  } else {
    const int64_t curr_logical_version = index_store_desc_.get_desc().get_logical_version();
    ObIndexBlockRowDesc row_desc(data_desc.get_desc());
    int64_t row_idx = -1;
    ObLogicMacroBlockId prev_logic_id;
    const bool need_rewrite = index_store_desc_.get_desc().is_cg();
    for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
      ObMacroMetasArray *macro_metas = roots_[i]->macro_metas_;
      for (int64_t j = 0; OB_SUCC(ret) && j < macro_metas->count(); ++j) {
        ObDataMacroBlockMeta *macro_meta = macro_metas->at(j);
        int64_t absolute_row_offset = -1;
        if (OB_ISNULL(macro_meta)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null macro meta", K(ret), K(j), KPC(roots_.at(i)));
        } else if (OB_UNLIKELY(macro_meta->get_logic_id() == prev_logic_id)) {
          // Since we rely on upper stream of sstable writing process to ensure the uniqueness of logic id
          // and we don't want more additional memory/time consumption, we only check continuous ids here
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected duplicate logic macro id", K(ret), KPC(macro_meta), K(prev_logic_id));
        } else if (use_absolute_offset) {
          absolute_row_offset = roots_[i]->absolute_offsets_->at(j);
        } else {
          absolute_row_offset = macro_meta->val_.row_count_ + row_idx;
        }

        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(absolute_row_offset < 0)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected absolute row offset", K(ret), K(use_absolute_offset), K(row_idx), KPC(macro_meta));
        } else if (need_rewrite && FALSE_IT(macro_meta->end_key_.datums_[0].set_int(absolute_row_offset))) {
        } else if (FALSE_IT(row_desc.row_offset_ = absolute_row_offset)) {
        } else if (OB_FAIL(index_builder_.append_row(*macro_meta, row_desc))) {
          STORAGE_LOG(WARN, "fail to append row", K(ret), KPC(macro_meta), K(j), KPC(roots_.at(i)));
        } else if (FALSE_IT(prev_logic_id = macro_meta->get_logic_id())) {
        } else {
          row_idx += macro_meta->val_.row_count_;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObIndexTreeInfo tree_info;
    ObMacroBlocksWriteCtx *index_write_ctx = nullptr;
    if (OB_FAIL(index_builder_.close(self_allocator_, tree_info))) {
      STORAGE_LOG(WARN, "fail to close merged index tree", K(ret));
    } else if (OB_FAIL(macro_writer_.close())) {
      STORAGE_LOG(WARN, "fail to close container macro writer", K(ret));
    } else if (OB_FAIL(macro_writer_.get_macro_block_write_ctx().get_macro_id_array(res.other_block_ids_))) {
      STORAGE_LOG(WARN, "fail to get macro ids of index blocks", K(ret));
    } else {
      res.index_blocks_cnt_ += macro_writer_.get_macro_block_write_ctx().macro_block_list_.count();
      res.root_desc_ = tree_info.root_desc_;
      res.row_count_ = tree_info.row_count_;
      res.max_merged_trans_version_ = tree_info.max_merged_trans_version_;
      res.contain_uncommitted_row_ = tree_info.contain_uncommitted_row_;
      index_builder_.reset();
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::build_meta_tree(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq start_seq;
  ObMacroBlocksWriteCtx *index_write_ctx = nullptr;
  ObMetaIndexBlockBuilder &builder = data_builder_;
  ObDataStoreDesc &desc = container_store_desc_;
  start_seq.set_macro_meta_block();
  if (OB_FAIL(macro_writer_.open(desc, start_seq, callback_))) {
    STORAGE_LOG(WARN, "fail to open index macro writer", K(ret));
  } else if (OB_FAIL(builder.init(desc, self_allocator_, macro_writer_))) {
    STORAGE_LOG(WARN, "fail to init index builder", K(ret));
  } else if (OB_FAIL(index_row_.reserve(desc.get_row_column_count()))) {
    STORAGE_LOG(WARN, "Failed to reserve column cnt", K(ret), K(desc.get_row_column_count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
      ObMacroMetasArray *macro_metas = roots_[i]->macro_metas_;
      for (int64_t j = 0; OB_SUCC(ret) && j < macro_metas->count(); ++j) {
        row_allocator_.reuse();
        ObDataMacroBlockMeta *macro_meta = macro_metas->at(j);
        if (OB_ISNULL(macro_meta)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null macro meta", K(ret), K(j), K(roots_.at(i)));
        } else if (OB_FAIL(macro_meta->build_row(index_row_, row_allocator_))) {
          STORAGE_LOG(WARN, "fail to build row of macro meta", K(ret), KPC(macro_meta));
        } else if (OB_FAIL(builder.append_leaf_row(index_row_))) {
          STORAGE_LOG(WARN, "fail to append leaf row", K(ret), K(index_row_));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(builder.close(self_allocator_, roots_, res.nested_size_, res.nested_offset_, res.data_root_desc_))) {
    STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
  } else if (OB_FAIL(macro_writer_.close())) {
    STORAGE_LOG(WARN, "fail to close macro block writer", K(ret));
  } else if (OB_FAIL(macro_writer_.get_macro_block_write_ctx().get_macro_id_array(res.other_block_ids_))) {
    STORAGE_LOG(WARN, "fail to get macro ids of index blocks", K(ret));
  } else {
    res.index_blocks_cnt_ += macro_writer_.get_macro_block_write_ctx().macro_block_list_.count();
    builder.reset();
    row_allocator_.reset();
  }
  return ret;
}

int ObSSTableIndexBuilder::generate_macro_blocks_info(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;

  // collect info of data macro blocks
  int64_t cur_data_block_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
    ObMacroMetasArray *macro_metas = roots_[i]->macro_metas_;
    for (int64_t j = 0; OB_SUCC(ret) && j < macro_metas->count(); ++j) {
      ObDataMacroBlockMeta *macro_meta = macro_metas->at(j);
      if (OB_ISNULL(macro_meta)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null macro meta", K(ret), K(j), K(roots_.at(i)));
      } else if (OB_FAIL(accumulate_macro_column_checksum(*macro_meta, res))) {
        STORAGE_LOG(WARN, "fail to accumulate macro column checksum", K(ret), KPC(macro_meta));
      } else {
        res.occupy_size_ += macro_meta->val_.occupy_size_;
        res.original_size_ += macro_meta->val_.original_size_;
        res.micro_block_cnt_ += macro_meta->val_.micro_block_count_;
        res.data_checksum_ = ob_crc64_sse42(res.data_checksum_,
            &macro_meta->val_.data_checksum_, sizeof(res.data_checksum_));
        ++res.data_blocks_cnt_;
        ++cur_data_block_idx;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(roots_[i]->get_macro_id_array(res.data_block_ids_))) {
        STORAGE_LOG(WARN, "fail to get macro ids of data blocks", K(ret));
      } else {
        res.use_old_macro_block_count_ += roots_[i]->use_old_macro_block_count_;
      }
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::accumulate_macro_column_checksum(
    const ObDataMacroBlockMeta &meta, ObSSTableMergeRes &res)
{
  // accumulate column checksum for sstable data block
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta.is_valid()
      || meta.get_meta_val().column_count_ > res.data_column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(meta), K_(res.data_column_cnt));
  } else if (OB_UNLIKELY(index_store_desc_.get_desc().is_major_or_meta_merge_type()
        && !index_store_desc_.get_desc().get_default_col_checksum_array_valid()
        && res.data_column_cnt_ > meta.get_meta_val().column_count_)) {
    // when default_col_checksum_array is invalid, need to make sure col_cnt in macro equal to col_cnt of result sstable
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "index store desc is invalid", K(ret), K_(index_store_desc), K(meta.get_meta_val().column_count_),
      K(res.data_column_cnt_));
  } else {
    for (int64_t i = 0; i < meta.get_meta_val().column_count_; ++i) {
      res.data_column_checksums_.at(i) += meta.val_.column_checksums_[i];
    }
    for (int64_t i = meta.get_meta_val().column_count_; i < res.data_column_cnt_; ++i) {
      res.data_column_checksums_.at(i) += meta.val_.row_count_ * index_store_desc_.get_desc().get_col_default_checksum_array().at(i);
    }
  }
  return ret;
}

void ObSSTableIndexBuilder::clean_status()
{
  index_builder_.reset();
  data_builder_.reset();
  macro_writer_.reset();
  // release memory to avoid occupying too much if retry frequently
  self_allocator_.reset();
}

int ObSSTableIndexBuilder::close(
    ObSSTableMergeRes &res,
    const int64_t nested_size,
    const int64_t nested_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY((0 == nested_offset) ^ (OB_DEFAULT_MACRO_BLOCK_SIZE == nested_size))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(nested_offset), K(nested_size));
  } else if (OB_UNLIKELY(is_closed_)) {
    if (OB_FAIL(res.assign(res_))) {
      STORAGE_LOG(WARN, "fail to assign res", K(ret), K_(res));
    }
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
  } else if (check_version_for_small_sstable(index_store_desc_.get_desc()) && 0 == nested_offset) {
    const bool is_single_block = check_single_block();
    if (is_single_block) {
      switch (optimization_mode_) {
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
    }
  } else {
    // if nested_offset is not 0, this sstable is reused-small-sstable, we don't need to rewrite it
    res.nested_offset_ = nested_offset;
    res.nested_size_ = nested_size;
  }

  if (OB_FAIL(ret) || roots_.empty() || is_closed_) {
    // do nothing
  } else if (OB_FAIL(merge_index_tree(res))) {
    STORAGE_LOG(WARN, "fail to merge index tree", K(ret));
  } else if (OB_FAIL(build_meta_tree(res))) {
    STORAGE_LOG(WARN, "fail to build meta tree", K(ret));
  } else if (OB_FAIL(generate_macro_blocks_info(res))) {
    STORAGE_LOG(WARN, "fail to generate id list", K(ret));
  }

  if (OB_SUCC(ret) && OB_LIKELY(!is_closed_)) {
    const ObDataStoreDesc &desc = index_store_desc_.get_desc();
    res.root_row_store_type_ = desc.get_row_store_type();
    res.compressor_type_ = desc.get_compressor_type();
    res.encrypt_id_ = desc.get_encrypt_id();
    MEMCPY(res.encrypt_key_, desc.get_encrypt_key(),
        share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    res.master_key_id_ = desc.get_master_key_id();
    if (OB_FAIL(res_.assign(res))) {
      STORAGE_LOG(WARN, "fail to save merge res", K(ret), K(res));
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
        data_blocks_cnt += roots_.at(i)->data_blocks_cnt_;
      }
    }
    if (OB_TIMEOUT == ret || OB_ALLOCATE_MEMORY_FAILED == ret || OB_EAGAIN == ret) {
      STORAGE_LOG(WARN, "fail to close sstable index builder", K(ret), K(data_blocks_cnt),
          K(sstable_allocator_.total()), K(sstable_allocator_.used()), K(self_allocator_.total()),
          K(self_allocator_.used()), K(row_allocator_.total()), K(row_allocator_.used()));
    } else {
      STORAGE_LOG(ERROR, "fail to close sstable index builder", K(ret), K(data_blocks_cnt),
          K(sstable_allocator_.total()), K(sstable_allocator_.used()), K(self_allocator_.total()),
          K(self_allocator_.used()), K(row_allocator_.total()), K(row_allocator_.used()));
    }
    clean_status(); // clear since re-entrant
  }
  return ret;
}

bool ObSSTableIndexBuilder::check_version_for_small_sstable(const ObDataStoreDesc &index_desc)
{
  return !index_desc.is_major_merge_type()
      || index_desc.get_major_working_cluster_version() >= DATA_VERSION_4_1_0_0;
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
      STORAGE_LOG(WARN, "fail to check and rewrite small sstable without macro size", K(ret), K(macro_size));
    }
  } else { // align_macro_size < SMALL_SSTABLE_THRESHOLD && 0 != macro_size
    if (OB_FAIL(rewrite_small_sstable(res))) {
      STORAGE_LOG(WARN, "fail to rewrite small sstable with given macro size", K(ret));
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::rewrite_small_sstable(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObBlockInfo block_info;
  ObMacroBlockHandle read_handle;
  const ObDataMacroBlockMeta &macro_meta = *(roots_[0]->macro_metas_->at(0));
  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_ = macro_meta.val_.macro_id_;
  read_info.offset_ = 0;
  read_info.size_ = upper_align(roots_[0]->last_macro_size_, DIO_READ_ALIGN_SIZE);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
  read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_INDEX_BUILDER_IO);

  if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char*>(self_allocator_.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(read_info.size_));
  } else {
    if (OB_FAIL(ObBlockManager::async_read_block(read_info, read_handle))) {
      STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info), K(macro_meta), K(roots_[0]->last_macro_size_));
    } else if (OB_FAIL(read_handle.wait())) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(read_info));
    } else {
      ObSharedMacroBlockMgr *shared_block_mgr = MTL(ObSharedMacroBlockMgr*);
      ObMacroBlocksWriteCtx tmp_write_ctx;
      const MacroBlockId &macro_id = roots_[0]->macro_metas_->at(0)->val_.macro_id_;
      if (OB_FAIL(tmp_write_ctx.add_macro_block_id(macro_id))) {
        STORAGE_LOG(WARN, "fail to add macro block id to write ctx",
            K(ret), K(macro_id), K(tmp_write_ctx));
      } else if (OB_FAIL(shared_block_mgr->write_block(
          read_info.buf_, read_handle.get_data_size(), block_info, tmp_write_ctx))) {
        STORAGE_LOG(WARN, "fail to write small sstable through shared_block_mgr", K(ret));
      } else if (OB_UNLIKELY(!block_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "successfully rewrite small sstable, but block info is invali", K(ret), K(block_info));
      } else if (OB_FAIL(roots_[0]->change_macro_id(block_info.macro_id_))){
        STORAGE_LOG(WARN, "fail to change index tree root ctx macro id", K(ret),
            K(block_info.macro_id_), KPC(roots_[0]));
      } else {
        res.nested_offset_ = block_info.nested_offset_;
        res.nested_size_ = block_info.nested_size_;
      }
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::check_and_rewrite_sstable_without_size(ObSSTableMergeRes &res)
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

int ObSSTableIndexBuilder::do_check_and_rewrite_sstable(ObBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  const ObDataMacroBlockMeta &macro_meta = *(roots_[0]->macro_metas_->at(0));
  ObSSTableMacroBlockHeader macro_header;
  char *data_buf = nullptr;

  if (OB_FAIL(load_single_macro_block(macro_meta, OB_SERVER_BLOCK_MGR.get_macro_block_size(), 0, self_allocator_, data_buf))) {
    STORAGE_LOG(WARN, "fail to load macro block", K(ret), K(macro_meta), KPC(roots_[0]));
  } else if (OB_FAIL(parse_macro_header(data_buf, OB_SERVER_BLOCK_MGR.get_macro_block_size(), macro_header))) {
    STORAGE_LOG(WARN, "fail to parse macro header", K(ret), KP(data_buf));
  } else {
    roots_[0]->meta_block_offset_ = macro_header.fixed_header_.meta_block_offset_;
    roots_[0]->meta_block_size_ = macro_header.fixed_header_.meta_block_size_;
    const int64_t align_size = upper_align(
      macro_header.fixed_header_.meta_block_offset_ + macro_header.fixed_header_.meta_block_size_,
      DIO_READ_ALIGN_SIZE);
    if (align_size < SMALL_SSTABLE_THRESHOLD) { // need to be rewritten
      ObSharedMacroBlockMgr *shared_block_mgr = MTL(ObSharedMacroBlockMgr*);
      ObMacroBlocksWriteCtx tmp_write_ctx;
      const MacroBlockId &macro_id = roots_[0]->macro_metas_->at(0)->val_.macro_id_;
      if (OB_FAIL(tmp_write_ctx.add_macro_block_id(macro_id))) {
        STORAGE_LOG(WARN, "fail to add macro block id to write ctx",
            K(ret), K(macro_id), K(tmp_write_ctx));
      } else if (OB_FAIL(shared_block_mgr->write_block(data_buf, align_size, block_info, tmp_write_ctx))) {
        STORAGE_LOG(WARN, "fail to write small sstable through shared_block_mgr", K(ret));
      } else if (OB_UNLIKELY(!block_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "successfully rewrite small sstable, but block info is invalid", K(ret), K(block_info));
      } else if (OB_FAIL(roots_[0]->change_macro_id(block_info.macro_id_))){
        STORAGE_LOG(WARN, "fail to change index tree root ctx macro id", K(ret),
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
    char *&data_buf)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  ObMacroBlockHandle read_handle;
  read_info.macro_block_id_ = macro_meta.val_.macro_id_;
  read_info.offset_ = nested_offset;
  read_info.size_ = nested_size;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
  read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_INDEX_BUILDER_IO);

  if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char*>(allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(read_info.size_));
  } else if (OB_FAIL(ObBlockManager::async_read_block(read_info, read_handle))) {
    STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info), K(macro_meta));
  } else if (OB_FAIL(read_handle.wait())) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
  } else {
    data_buf = read_info.buf_;
  }

  return ret;
}

int ObSSTableIndexBuilder::parse_macro_header(
    const char *buf,
    const int64_t buf_size,
    ObSSTableMacroBlockHeader &macro_header)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMacroBlockCommonHeader common_header;

  if (OB_UNLIKELY(buf_size <= 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "the argument is invalid", K(ret), K(buf_size), KP(buf));
  } else if (OB_FAIL(common_header.deserialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize common header", K(ret), K(buf_size), KP(buf), K(pos));
  } else if (OB_FAIL(macro_header.deserialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize macro header", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(!macro_header.is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid macro header", K(ret), K(macro_header));
  }
  return ret;
}

bool ObSSTableIndexBuilder::check_single_block()
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < roots_.count(); i++) {
    cnt += roots_[i]->macro_metas_->count();
  }
  return 1 == cnt;
}


//===================== ObBaseIndexBlockBuilder(public) ================
ObBaseIndexBlockBuilder::ObBaseIndexBlockBuilder()
  : is_inited_(false),
    is_closed_(false),
    index_store_desc_(nullptr),
    data_store_desc_(nullptr),
    row_builder_(),
    last_rowkey_(),
    row_allocator_("BaseMidIdx"),
    allocator_(nullptr),
    micro_writer_(nullptr),
    macro_writer_(nullptr),
    index_block_pre_warmer_(),
    micro_block_adaptive_splitter_(),
    row_offset_(-1),
    index_block_aggregator_(),
    next_level_builder_(nullptr),
    level_(0)
{
}


ObBaseIndexBlockBuilder::~ObBaseIndexBlockBuilder()
{
  reset();
}

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
  index_block_pre_warmer_.reset();
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
    if (OB_FAIL(row_builder_.init(allocator, row_allocator_, data_store_desc, index_store_desc))) {
      STORAGE_LOG(WARN, "fail to init ObBaseIndexBlockBuilder", K(ret));
    } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(index_store_desc_, allocator, micro_writer_))) {
      STORAGE_LOG(WARN, "fail to build micro writer", K(ret));
    } else if (OB_FAIL(index_block_aggregator_.init(data_store_desc, allocator))) {
      STORAGE_LOG(WARN, "fail to init index block aggregator", K(ret));
    } else if (OB_FAIL(micro_block_adaptive_splitter_.init(index_store_desc_->get_macro_store_size(),
        MIN_INDEX_MICRO_BLOCK_ROW_CNT /*min_micro_row_count*/, true/*is_use_adaptive*/))) {
      STORAGE_LOG(WARN, "Failed to init micro block adaptive split", K(ret),
            "macro_store_size", index_store_desc_->get_macro_store_size());
    } else {
      if (need_pre_warm() && index_store_desc.get_tablet_id().is_user_tablet()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(index_block_pre_warmer_.init(ObRowkeyVectorHelper::can_use_non_datum_rowkey_vector(
                                                     index_store_desc_->is_cg(), index_store_desc_->get_tablet_id()) ?
                                                     &index_store_desc_->get_rowkey_col_descs()
                                                     : nullptr))) {
          STORAGE_LOG(WARN, "Failed to init index block prewarmer", K(tmp_ret));
        }
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
  } else if (!last_rowkey_.is_valid()) { // skip
  } else if (index_store_desc_->is_cg()) { // datum_utils is lacked for cg
    if (row_desc.row_key_.get_datum(0).get_int() <= last_rowkey_.get_datum(0).get_int()) {
      ret = OB_ROWKEY_ORDER_ERROR;
      STORAGE_LOG(ERROR, "input rowkey is less then last rowkey.", K(row_desc.row_key_), K(last_rowkey_), K(ret));
    }
  } else {
    const ObDatumRowkey &cur_rowkey = row_desc.row_key_;
    int32_t compare_result = 0;
    const ObStorageDatumUtils &datum_utils = index_store_desc_->get_datum_utils();
    if (OB_FAIL(cur_rowkey.compare(last_rowkey_, datum_utils, compare_result))) {
      STORAGE_LOG(WARN, "Failed to compare last key", K(ret), K(cur_rowkey), K(last_rowkey_));
    } else if (OB_UNLIKELY(compare_result < 0)) {
      ret = OB_ROWKEY_ORDER_ERROR;
      STORAGE_LOG(ERROR, "input rowkey is less then last rowkey.", K(cur_rowkey), K(last_rowkey_), K(ret));
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
  } else if (OB_FAIL(row_desc.row_key_.deep_copy(last_rowkey_, row_allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy last rowkey", K(ret), K(row_desc));
  } else if (OB_FAIL(index_block_aggregator_.eval(row_desc))) {
    STORAGE_LOG(WARN, "fail to aggregate index row", K(ret), K(row_desc));
  } else {
    row_offset_ = row_desc.row_offset_; // row_offset is increasing
  }
  return ret;
}

int ObBaseIndexBlockBuilder::append_row(
    const ObDataMacroBlockMeta &macro_meta, ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row_to_append = NULL;
  if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(macro_meta));
  } else if (OB_FAIL(meta_to_row_desc(macro_meta, row_desc))) {
    STORAGE_LOG(WARN, "fail to build row desc from macro meta", K(ret), K(macro_meta));
  } else if (OB_FAIL(append_row(row_desc))) {
    STORAGE_LOG(WARN, "fail to append row", K(ret), K(row_desc), K(macro_meta));
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
    } else if (OB_FAIL(micro_writer->build_micro_block_desc(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to build root block", K(ret));
    } else if (FALSE_IT(micro_block_desc.last_rowkey_ = root_builder->last_rowkey_)) {
    } else if (OB_UNLIKELY(micro_block_desc.get_block_size() >= ROOT_BLOCK_SIZE_LIMIT)) {
      if (index_block_pre_warmer_.is_valid()
          && OB_TMP_FAIL(index_block_pre_warmer_.reserve_kvpair(micro_block_desc, root_builder->level_+1))) {
        if (OB_BUF_NOT_ENOUGH != tmp_ret) {
          STORAGE_LOG(WARN, "Fail to reserve kvpair", K(tmp_ret));
        }
      }
      if (OB_FAIL(macro_writer_->append_index_micro_block(micro_block_desc))) {
        micro_writer->dump_diagnose_info(); // ignore dump error
        STORAGE_LOG(WARN, "fail to append root block", K(ret), K(micro_block_desc));
      } else {
        ObIndexBlockRowDesc root_row_desc(*index_store_desc_);
        root_builder->block_to_row_desc(micro_block_desc, root_row_desc);
        if (OB_FAIL(root_addr.set_block_addr(root_row_desc.macro_id_,
                                             root_row_desc.block_offset_,
                                             root_row_desc.block_size_,
                                             ObMetaDiskAddr::DiskType::BLOCK))) {
          STORAGE_LOG(WARN, "fail to set block address", K(ret), K(root_row_desc));
        }
      }
      if (OB_FAIL(ret) || OB_TMP_FAIL(tmp_ret) || !index_block_pre_warmer_.is_valid()) {
      } else if (OB_TMP_FAIL(index_block_pre_warmer_.update_and_put_kvpair(micro_block_desc))) {
        STORAGE_LOG(WARN, "Fail to update and put kvpair", K(tmp_ret));
      }
    } else {
      char *&root_buf = desc.buf_;
      const int64_t buf_size = micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
      int64_t pos = 0;
      if (OB_ISNULL(root_buf = static_cast<char *> (allocator.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc root buf", K(ret), K(buf_size));
      } else if (OB_FAIL(micro_block_desc.header_->serialize(root_buf, buf_size, pos))) {
        STORAGE_LOG(WARN, "fail to serialize header", K(ret), K(micro_block_desc));
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
      const ObIndexBlockAggregator &root_aggregator = root_builder->index_block_aggregator_;
      tree_info.row_count_ = root_aggregator.get_row_count();
      tree_info.max_merged_trans_version_ = root_aggregator.get_max_merged_trans_version();
      tree_info.contain_uncommitted_row_ = root_aggregator.contain_uncommitted_row();
      root_builder->is_closed_ = true;
    }
  }
  STORAGE_LOG(DEBUG, "tree info of index tree", K(ret), K(tree_info));
  return ret;
}

//===================== ObBaseIndexBlockBuilder(protected) ================

int ObBaseIndexBlockBuilder::build_index_micro_block(ObMicroBlockDesc &micro_block_desc)
{
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

void ObBaseIndexBlockBuilder::clean_status()
{
  micro_writer_->reuse(); // only reuse when index row has been inserted
  index_block_aggregator_.reuse();
}


int ObBaseIndexBlockBuilder::append_index_micro_block()
{
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
    if (index_block_pre_warmer_.is_valid()
        && OB_TMP_FAIL(index_block_pre_warmer_.reserve_kvpair(micro_block_desc, level_+1))) {
      if (OB_BUF_NOT_ENOUGH != tmp_ret) {
        STORAGE_LOG(WARN, "Fail to reserve kvpair", K(tmp_ret));
      }
    }
    if (OB_FAIL(macro_writer_->append_index_micro_block(micro_block_desc))) {
      micro_writer_->dump_diagnose_info(); // ignore dump error
      STORAGE_LOG(WARN, "fail to append index micro block", K(ret), K(micro_block_desc));
    } else if (OB_FAIL(micro_block_adaptive_splitter_.update_compression_info(micro_block_desc.row_count_,
        block_size, micro_block_desc.buf_size_))) {
      STORAGE_LOG(WARN, "Fail to update_compression_info", K(ret), K(micro_block_desc));
    } else if (OB_FAIL(append_next_row(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to append next row", K(ret), K(micro_block_desc));
    }
    if (OB_FAIL(ret) || OB_TMP_FAIL(tmp_ret) || !index_block_pre_warmer_.is_valid()) {
    } else if (OB_TMP_FAIL(index_block_pre_warmer_.update_and_put_kvpair(micro_block_desc))) {
      STORAGE_LOG(WARN, "Fail to build index block cache key and put into cache", K(tmp_ret));
    }
    index_block_pre_warmer_.reuse();
    clean_status();
  }

  return ret;
}

int ObBaseIndexBlockBuilder::get_aggregated_row(ObIndexBlockRowDesc &next_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_block_aggregator_.get_index_agg_result(next_row_desc))) {
    STORAGE_LOG(WARN, "fail to get aggregated row", K(ret), K_(index_block_aggregator));
  } else {
    next_row_desc.row_offset_ = row_offset_;
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
    if (OB_SUCC(ret) && OB_FAIL(next_level_builder_->close_index_tree(root_builder))) {
      STORAGE_LOG(WARN, "fail to close index tree, ", K(ret));
    }
  }
  return ret;
}

/* below three functions should keep pace with each other */
void ObBaseIndexBlockBuilder::block_to_row_desc(
    const ObMicroBlockDesc &micro_block_desc,
    ObIndexBlockRowDesc &row_desc)
{
  row_desc.row_key_ = micro_block_desc.last_rowkey_;
  row_desc.macro_id_ = micro_block_desc.macro_id_;
  row_desc.block_offset_ = micro_block_desc.block_offset_;
  row_desc.block_size_ = micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
  row_desc.row_count_ = micro_block_desc.row_count_;
  row_desc.row_count_delta_ = micro_block_desc.row_count_delta_;
  row_desc.is_deleted_ = micro_block_desc.can_mark_deletion_;
  row_desc.max_merged_trans_version_ = micro_block_desc.max_merged_trans_version_;
  row_desc.contain_uncommitted_row_ = micro_block_desc.contain_uncommitted_row_;
  row_desc.has_string_out_row_ = micro_block_desc.has_string_out_row_;
  row_desc.has_lob_out_row_ = micro_block_desc.has_lob_out_row_;
  row_desc.is_last_row_last_flag_ = micro_block_desc.is_last_row_last_flag_;
  row_desc.aggregated_row_ = micro_block_desc.aggregated_row_;
  row_desc.is_serialized_agg_row_ = false;
}

int ObBaseIndexBlockBuilder::meta_to_row_desc(
    const ObDataMacroBlockMeta &macro_meta,
    ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc *data_desc = nullptr;
  if (OB_UNLIKELY(nullptr == row_desc.data_store_desc_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null data store desc", K(ret));
  } else if (FALSE_IT(data_desc = const_cast<ObDataStoreDesc *>(row_desc.data_store_desc_))) {
  } else if (OB_FAIL(data_desc->static_desc_->end_scn_.convert_for_tx(macro_meta.val_.snapshot_version_))) {
    STORAGE_LOG(WARN, "fail to convert scn", K(ret), K(macro_meta.val_.snapshot_version_));
  } else {
    ObStaticDataStoreDesc *static_desc = data_desc->static_desc_;
    data_desc->row_store_type_ = macro_meta.val_.row_store_type_;
    static_desc->compressor_type_ = macro_meta.val_.compressor_type_;
    static_desc->master_key_id_ = macro_meta.val_.master_key_id_;
    static_desc->encrypt_id_ = macro_meta.val_.encrypt_id_;
    MEMCPY(static_desc->encrypt_key_, macro_meta.val_.encrypt_key_, sizeof(static_desc->encrypt_key_));
    static_desc->schema_version_ = macro_meta.val_.schema_version_;
    static_desc->snapshot_version_ = macro_meta.val_.snapshot_version_;

    row_desc.is_secondary_meta_ = false;
    row_desc.is_macro_node_ = true;

    row_desc.row_key_ = macro_meta.end_key_;
    row_desc.macro_id_ = macro_meta.val_.macro_id_;
    row_desc.block_offset_ = macro_meta.val_.block_offset_;
    row_desc.block_size_ = macro_meta.val_.block_size_;
    row_desc.row_count_ = macro_meta.val_.row_count_;
    row_desc.row_count_delta_ = macro_meta.val_.row_count_delta_;
    row_desc.is_deleted_ = macro_meta.val_.is_deleted_;
    row_desc.max_merged_trans_version_ = macro_meta.val_.max_merged_trans_version_;
    row_desc.contain_uncommitted_row_ = macro_meta.val_.contain_uncommitted_row_;
    row_desc.micro_block_count_ = macro_meta.val_.micro_block_count_;
    row_desc.macro_block_count_ = 1;
    row_desc.has_string_out_row_ = macro_meta.val_.has_string_out_row_;
    row_desc.has_lob_out_row_ = !macro_meta.val_.all_lob_in_row_;
    row_desc.serialized_agg_row_buf_ = macro_meta.val_.agg_row_buf_;
    row_desc.is_serialized_agg_row_ = true;
    // is_last_row_last_flag_ only used in data macro block
  }
  return ret;
}

int ObBaseIndexBlockBuilder::row_desc_to_meta(
    const ObIndexBlockRowDesc &macro_row_desc,
    ObDataMacroBlockMeta &macro_meta,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  macro_meta.end_key_ = macro_row_desc.row_key_;
  macro_meta.val_.macro_id_ = macro_row_desc.macro_id_; // DEFAULT_IDX_ROW_MACRO_ID
  macro_meta.val_.block_offset_ = macro_row_desc.block_offset_;
  macro_meta.val_.block_size_ = macro_row_desc.block_size_;
  macro_meta.val_.row_count_ = macro_row_desc.row_count_;
  macro_meta.val_.row_count_delta_ = macro_row_desc.row_count_delta_;
  macro_meta.val_.is_deleted_ = macro_row_desc.is_deleted_;
  macro_meta.val_.max_merged_trans_version_ = macro_row_desc.max_merged_trans_version_;
  macro_meta.val_.contain_uncommitted_row_ = macro_row_desc.contain_uncommitted_row_;
  macro_meta.val_.has_string_out_row_ = macro_row_desc.has_string_out_row_;
  macro_meta.val_.all_lob_in_row_ = !macro_row_desc.has_lob_out_row_;
  macro_meta.val_.is_last_row_last_flag_ = macro_row_desc.is_last_row_last_flag_;
  if (nullptr != macro_row_desc.aggregated_row_) {
    agg_row_writer_.reset();
    char *agg_row_buf = nullptr;
    int64_t agg_row_upper_size = 0;
    int64_t pos = 0;
    if (OB_UNLIKELY(macro_row_desc.is_serialized_agg_row_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected serialzied agg row in descriptor", K(ret), K(macro_row_desc));
    } else if (OB_FAIL(agg_row_writer_.init(
        data_store_desc_->get_agg_meta_array(), *macro_row_desc.aggregated_row_, allocator))) {
      STORAGE_LOG(WARN, "Fail to init aggregate row writer", K(ret));
    } else if (FALSE_IT(agg_row_upper_size = agg_row_writer_.get_data_size())) {
    } else if (OB_ISNULL(agg_row_buf = static_cast<char *>(allocator.alloc(agg_row_upper_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory for agg row", K(ret), K(agg_row_upper_size));
    } else if (OB_FAIL(agg_row_writer_.write_agg_data(agg_row_buf, agg_row_upper_size, pos))) {
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
    STORAGE_LOG(ERROR, "unexpected too high index block tree level", K(ret), K(level_));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObBaseIndexBlockBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(next_builder = new (buf) ObBaseIndexBlockBuilder())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObBaseIndexBlockBuilder", K(ret));
  } else if (OB_FAIL(next_builder->init(*data_store_desc_,
                                        *index_store_desc_,
                                        *allocator_,
                                        macro_writer_,
                                        level_ + 1))) {
    STORAGE_LOG(WARN, "fail to init next builder", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator_->free(buf);
    buf = nullptr;
  }
  return ret;
}

int ObBaseIndexBlockBuilder::append_next_row(const ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;
  ObIndexBlockRowDesc next_row_desc(*index_store_desc_);
  if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block desc", K(ret), K(micro_block_desc));
  } else if (FALSE_IT(block_to_row_desc(micro_block_desc, next_row_desc))) {
  } else if (OB_FAIL(get_aggregated_row(next_row_desc))) {
    STORAGE_LOG(WARN, "fail to get aggregated row", K(ret));
  } else if (OB_ISNULL(next_level_builder_)
      && OB_FAIL(new_next_builder(next_level_builder_))) {
    STORAGE_LOG(WARN, "new next builder error.", K(ret), K(next_level_builder_));
  } else if (OB_FAIL(next_level_builder_->append_row(next_row_desc))) {
    STORAGE_LOG(WARN, "fail to append index row", K(ret), K(next_row_desc));
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
    if (0 < micro_writer_->get_row_count() &&
        OB_FAIL(micro_block_adaptive_splitter_.check_need_split(micro_writer_->get_block_size(),
        micro_writer_->get_row_count(), index_store_desc_->get_micro_block_size(),
        macro_writer_->get_macro_data_size(), false /*is_keep_freespace*/, is_split))) {
      STORAGE_LOG(WARN, "Failed to check need split", K(ret),
          "micro_block_size", index_store_desc_->get_micro_block_size(),
          "current_macro_size", macro_writer_->get_macro_data_size(), KPC(micro_writer_));
    } else if (is_split || OB_FAIL(micro_writer_->append_row(*index_row))) {
      if (OB_FAIL(ret) && OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret), K(*index_row));
      } else if (OB_UNLIKELY(0 == micro_writer_->get_row_count())) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "The single row is too large, ", K(ret), K(*index_row), KPC_(index_store_desc));
      } else if (OB_UNLIKELY(1 == micro_writer_->get_row_count())) {
        micro_writer_->dump_diagnose_info(); // ignore dump error
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "row is too large to put more than one into micro block",
            K(ret), K(*index_row), KPC_(index_store_desc));
      } else if (OB_FAIL(append_index_micro_block())) {
        STORAGE_LOG(WARN, "Fail to append index micro block, ", K(ret));
      } else if (OB_FAIL(micro_writer_->append_row(*index_row))) {
        STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret), K(*index_row));
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
    micro_helper_(),
    macro_row_desc_(),
    index_tree_root_ctx_(nullptr),
    macro_meta_list_(nullptr),
    meta_block_writer_(nullptr),
    meta_row_(),
    macro_meta_(meta_row_allocator_),
    cg_rowkey_(),
    leaf_store_desc_(nullptr),
    local_leaf_store_desc_(nullptr),
    data_blocks_cnt_(0),
    meta_block_offset_(0),
    meta_block_size_(0),
    estimate_leaf_block_size_(0),
    estimate_meta_block_size_(0)
{
}

ObDataIndexBlockBuilder::~ObDataIndexBlockBuilder()
{
  reset();
}

void ObDataIndexBlockBuilder::reset()
{
  sstable_builder_ = nullptr;
  micro_helper_.reset();
  index_tree_root_ctx_ = nullptr;
  macro_meta_list_ = nullptr;
  if (OB_NOT_NULL(meta_block_writer_)) {
    meta_block_writer_->~ObIMicroBlockWriter();
    meta_block_writer_ = nullptr;
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

int ObDataIndexBlockBuilder::init(
    const ObDataStoreDesc &data_store_desc,
    ObSSTableIndexBuilder &sstable_builder)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObDataStoreDesc *index_store_desc = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDataIndexBlockBuilder has been inited", K(ret));
  } else if (OB_FAIL(sstable_builder.init_builder_ptrs(sstable_builder_, index_store_desc,
      leaf_store_desc_, index_tree_root_ctx_, macro_meta_list_))) {
    STORAGE_LOG(WARN, "fail to init referemce pointer members", K(ret));
  } else if (OB_UNLIKELY(index_store_desc->get_row_store_type() != data_store_desc.get_row_store_type()
      && (index_store_desc->get_row_store_type() == FLAT_ROW_STORE
          || data_store_desc.get_row_store_type() == FLAT_ROW_STORE)
      && !data_store_desc.is_force_flat_store_type_)) {
    // since n-1 micro block should keep format same with data_blocks
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "expect row store type equal", K(ret), KPC(index_store_desc), K(data_store_desc));
  } else if (OB_FAIL(micro_helper_.open(*index_store_desc, task_allocator_))) {
    STORAGE_LOG(WARN, "fail to open base writer", K(ret));
  } else if (OB_FAIL(meta_row_.init(task_allocator_, index_store_desc->get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init meta row", K(ret));
  } else if (data_store_desc.is_force_flat_store_type_) {
    local_leaf_store_desc_ = nullptr;
    if (OB_ISNULL(local_leaf_store_desc_ = OB_NEWx(ObDataStoreDesc, &task_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc Data Store Desc", K(ret));
    } else if (OB_FAIL(local_leaf_store_desc_->shallow_copy(*index_store_desc))) {
      STORAGE_LOG(WARN, "fail to assign leaf store desc", K(ret));
    } else if (FALSE_IT(local_leaf_store_desc_->force_flat_store_type())) {
    } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(
                  local_leaf_store_desc_, task_allocator_, meta_block_writer_,
                  GCONF.micro_block_merge_verify_level))) {
      STORAGE_LOG(WARN, "fail to init meta block writer", K(ret));
    } else if (FALSE_IT(local_leaf_store_desc_->micro_block_size_ = local_leaf_store_desc_->get_micro_block_size_limit())) {
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::init(
        data_store_desc, *local_leaf_store_desc_, task_allocator_, nullptr, 0))) {
      STORAGE_LOG(WARN, "fail to init base index builder", K(ret));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(local_leaf_store_desc_)) {
      local_leaf_store_desc_->~ObDataStoreDesc();
      task_allocator_.free(local_leaf_store_desc_);
      local_leaf_store_desc_ = nullptr;
    }
  } else {
    if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(
                  index_store_desc, task_allocator_, meta_block_writer_,
                  GCONF.micro_block_merge_verify_level))) {
      STORAGE_LOG(WARN, "fail to init meta block writer", K(ret));
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::init(
        data_store_desc, *leaf_store_desc_, task_allocator_, nullptr, 0))) {
      STORAGE_LOG(WARN, "fail to init base index builder", K(ret));
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::insert_and_update_index_tree(const ObDatumRow *index_row)
{
  // insert n-1 level index row
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_row) || OB_UNLIKELY(!index_row->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(index_row));
  } else if (OB_FAIL(micro_writer_->append_row(*index_row))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret), K(*index_row));
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::cal_macro_meta_block_size(
    const ObDatumRowkey &rowkey, int64_t &estimate_meta_block_size)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta macro_meta;
  const int64_t rowkey_cnt = rowkey.datum_cnt_;
  const int64_t column_cnt = data_store_desc_->get_row_column_count();
  if (OB_UNLIKELY(rowkey_cnt != meta_row_.count_ - 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected rowkey cnt equal", K(ret), K(rowkey_cnt), K(meta_row_), K(rowkey));
  } else if (OB_FAIL(macro_meta.end_key_.assign(rowkey.datums_, rowkey.datum_cnt_))) {
    STORAGE_LOG(WARN, "fail to assign end key for macro meta", K(ret), K(rowkey));
  } else {
    macro_meta.val_.rowkey_count_ = rowkey_cnt;
    macro_meta.val_.column_count_ = column_cnt;
    macro_meta.val_.compressor_type_ = data_store_desc_->get_compressor_type();
    macro_meta.val_.row_store_type_ = data_store_desc_->get_row_store_type();
    macro_meta.val_.logic_id_.logic_version_ = data_store_desc_->get_logical_version();
    macro_meta.val_.logic_id_.column_group_idx_ = data_store_desc_->get_table_cg_idx();
    macro_meta.val_.logic_id_.tablet_id_ = data_store_desc_->get_tablet_id().id();
    macro_meta.val_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID;
    meta_row_.reuse();
    meta_row_allocator_.reuse();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_meta.build_estimate_row(meta_row_, meta_row_allocator_))) {
      STORAGE_LOG(WARN, "fail to build meta row", K(ret), K(macro_meta));
    } else {
      meta_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      meta_block_writer_->reuse();
      if (OB_FAIL(meta_block_writer_->append_row(meta_row_))) {
        STORAGE_LOG(WARN, "fail to append meta row", K(ret), K_(meta_row));
      } else {
        const int64_t max_agg_data_size = index_block_aggregator_.get_max_agg_size();
        // for cs_encoding, the estimate_block_size calculated by build_block may less than the real block_size, because the
        // the cs_encoding has compression for string column and the real macro meta data may has lower compression
        // ratio than the fake estimated data, so here use original block size for estimating.
        if (ObStoreFormat::is_row_store_type_with_cs_encoding(index_store_desc_->row_store_type_)) {
          estimate_meta_block_size = meta_block_writer_->get_original_size() + max_agg_data_size;
#ifdef OB_BUILD_TDE_SECURITY
        const int64_t encrypted_size = share::ObEncryptionUtil::encrypted_length(
                                 static_cast<share::ObCipherOpMode>(index_store_desc_->get_encrypt_id()),
                                 estimate_meta_block_size);
        estimate_meta_block_size = max(estimate_meta_block_size, encrypted_size);
#endif
        } else {
          ObMicroBlockDesc meta_block_desc;
          if (OB_FAIL(meta_block_writer_->build_micro_block_desc(meta_block_desc))) {
            STORAGE_LOG(WARN, "fail to build macro meta block", K(ret), K_(meta_row));
          } else if (FALSE_IT(meta_block_desc.last_rowkey_ = rowkey)) {
          } else if (OB_UNLIKELY(!meta_block_desc.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected meta block desc", K(ret), K(meta_block_desc));
          } else {
            estimate_meta_block_size = meta_block_desc.buf_size_ + meta_block_desc.header_->header_size_ + max_agg_data_size;
#ifdef OB_BUILD_TDE_SECURITY
        const int64_t encrypted_size = share::ObEncryptionUtil::encrypted_length(
                                 static_cast<share::ObCipherOpMode>(index_store_desc_->get_encrypt_id()),
                                 estimate_meta_block_size);
        estimate_meta_block_size = max(estimate_meta_block_size, encrypted_size);
#endif
          }
        }
      }
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::add_row_offset( ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey &cg_rowkey = row_desc.row_key_;
  cg_rowkey.reset();
  cg_rowkey_.set_int(row_desc.row_count_ + row_offset_); // start from -1
  if (OB_FAIL(cg_rowkey.assign(&cg_rowkey_, 1))) {
    STORAGE_LOG(WARN, "fail to assign rowkey with row", K(ret), K_(cg_rowkey));
  }
  return ret;
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
    const int64_t cur_data_block_size = micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
    int64_t remain_size = macro_block.get_remain_size() - cur_data_block_size;
    if (remain_size <= 0) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(cal_macro_meta_block_size(
          row_desc.row_key_, estimate_meta_block_size))) {
      STORAGE_LOG(WARN, "fail to cal macro meta block size", K(ret), K(micro_block_desc));
    } else if ((remain_size = remain_size - estimate_meta_block_size) <= 0) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (FALSE_IT(micro_writer_->set_block_size_upper_bound(remain_size))) {
      // remain_size is set to be block_size_upper_bound of n-1 level micro_writer
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::append_row(row_desc))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(WARN, "fail to append row", K(ret), K(micro_block_desc), K(row_desc));
      } else {
        STORAGE_LOG(DEBUG, "succeed to prevent append_row", K(ret), K(macro_block.get_remain_size()),
            K(cur_data_block_size), K(estimate_meta_block_size), K(remain_size));
      }
    } else {
      // only update these two variables when succeeded to append data micro block
      estimate_leaf_block_size_ = micro_writer_->get_original_size();
      estimate_meta_block_size_ = estimate_meta_block_size;
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::append_macro_block(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro block desc", K(ret), K(macro_meta));
  } else {
    if (OB_FAIL(index_tree_root_ctx_->add_macro_block_meta(macro_meta))) {
      STORAGE_LOG(WARN, "failed to add macro block meta", K(ret), K(macro_meta));
    } else {
      ++data_blocks_cnt_;
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::write_meta_block(
    ObMacroBlock &macro_block,
    const MacroBlockId &block_id,
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

  if (OB_UNLIKELY(!macro_row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro row desc", K(ret), K(macro_row_desc));
  } else if (OB_FAIL(macro_block.get_macro_block_meta(macro_meta_))) {
    STORAGE_LOG(WARN, "fail to get macro block meta", K(ret), K_(macro_meta));
  } else if (OB_UNLIKELY(macro_meta_.val_.micro_block_count_
      != macro_row_desc.micro_block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "check micro block count failed", K(ret), K_(macro_meta), K(macro_row_desc));
  } else if (FALSE_IT(macro_meta_.end_key_ = last_rowkey_)) { // fill rowkey for cg
  } else if (FALSE_IT(update_macro_meta_with_offset(macro_block.get_row_count(), ddl_start_row_offset))) {
  } else if (OB_FAIL(row_desc_to_meta(macro_row_desc, macro_meta_, meta_row_allocator_))) {
    STORAGE_LOG(WARN, "fail to convert macro row descriptor to meta", K(ret), K(macro_row_desc));
  } else if (OB_FAIL(macro_meta_.build_row(meta_row_, meta_row_allocator_))) {
    STORAGE_LOG(WARN, "fail to build row", K(ret), K_(macro_meta));
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
    STORAGE_LOG(WARN, "fail to write meta index block", K(ret), K(meta_block_desc));
  }
  if (OB_SUCC(ret)) {
    macro_meta_.val_.macro_id_ = block_id; // real macro id
    meta_block_offset_ = data_offset;
    meta_block_size_ = meta_block_desc.get_block_size();
    if (OB_FAIL(index_tree_root_ctx_->add_macro_block_meta(macro_meta_))) {
      STORAGE_LOG(WARN, "failed to add macro block meta", K(ret), K_(macro_meta));
    } else {
      // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
      share::ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "succeed to write macro meta in macro block", K(ret), K(macro_meta_));
    }
  }
  return ret;
}

void ObDataIndexBlockBuilder::update_macro_meta_with_offset(const int64_t macro_block_row_count,
                                                           const int64_t ddl_start_row_offset)
{
  if (leaf_store_desc_->get_major_working_cluster_version() >= DATA_VERSION_4_3_1_0) {
    if (ddl_start_row_offset >= 0) {
      macro_meta_.val_.ddl_end_row_offset_ = ddl_start_row_offset + macro_block_row_count - 1;
    } else {
      macro_meta_.val_.ddl_end_row_offset_ = -1/*default*/;
    }
  } else {
    macro_meta_.val_.version_ = ObDataBlockMetaVal::DATA_BLOCK_META_VAL_VERSION;
  }
}

int ObDataIndexBlockBuilder::append_index_micro_block(ObMacroBlock &macro_block,
                                                      const MacroBlockId &block_id,
                                                      const int64_t ddl_start_row_offset)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMicroBlockDesc leaf_block_desc; // n-1 level index block
  int64_t data_offset = 0;
  int64_t leaf_block_size = 0;
  if (OB_FAIL(build_index_micro_block(leaf_block_desc))) {
    STORAGE_LOG(WARN, "fail to build n-1 level micro block", K(ret));
  } else {
    if (OB_TMP_FAIL(index_block_pre_warmer_.reserve_kvpair(leaf_block_desc, 1))) {
      if (OB_BUF_NOT_ENOUGH != tmp_ret) {
        STORAGE_LOG(WARN, "Fail to reserve index block value", K(tmp_ret));
      }
    }
    if (OB_FAIL(micro_helper_.compress_encrypt_micro_block(leaf_block_desc,
                                                           macro_block.get_current_macro_seq(),
                                                           macro_block.get_data_size()))) {
      STORAGE_LOG(WARN, "fail to compress and encrypt micro block", K(ret));
    } else if (OB_FAIL(macro_block.write_index_micro_block(leaf_block_desc, true, data_offset))) {
      STORAGE_LOG(WARN, "fail to write n-1 level index block", K(ret), K(leaf_block_desc));
    } else {
      leaf_block_desc.macro_id_ = block_id;
      leaf_block_desc.block_offset_ = data_offset;
      leaf_block_size = leaf_block_desc.get_block_size();
      if (OB_TMP_FAIL(tmp_ret)) {
      } else if (OB_TMP_FAIL(index_block_pre_warmer_.update_and_put_kvpair(leaf_block_desc))) {
        STORAGE_LOG(WARN, "Fail to build index block cache key and put into cache", K(tmp_ret));
      }
    }
    index_block_pre_warmer_.reuse();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_next_row(leaf_block_desc, macro_row_desc_))) {
    STORAGE_LOG(WARN, "fail to append next row", K(ret), K(leaf_block_desc));
  } else if (OB_UNLIKELY(block_id != macro_row_desc_.macro_id_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "expect macro id equal", K(ret), K(leaf_block_desc), K_(macro_row_desc));
  } else if (FALSE_IT(macro_row_desc_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID)) {
  } else if (OB_FAIL(write_meta_block(macro_block, block_id, macro_row_desc_, ddl_start_row_offset))) {
    STORAGE_LOG(WARN, "fail to build meta block", K(ret));
  } else {
    index_tree_root_ctx_->last_macro_size_ = data_offset + leaf_block_size + meta_block_size_;
  }

  if (OB_FAIL(ret) && OB_BUF_NOT_ENOUGH == ret) {
    STORAGE_LOG(WARN, "error!!!fail to write leaf/meta block into data macro block",
        K(ret), K_(estimate_leaf_block_size), K_(estimate_meta_block_size),
        K(leaf_block_desc), K_(macro_row_desc));
    STORAGE_LOG(INFO, "print error leaf block");
    micro_writer_->dump_diagnose_info();
    STORAGE_LOG(INFO, "print error meta block");
    meta_block_writer_->dump_diagnose_info();
    if (common::ObStoreFormat::is_row_store_type_with_encoding(macro_block.get_row_store_type())) {
      ret = OB_ENCODING_EST_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "build macro block failed by probably estimated maximum encoding data size overflow", K(ret));
    }
  }
  clean_status();
  return ret;
}

int ObDataIndexBlockBuilder::set_parallel_task_idx(const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_tree_root_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null index_tree_root_ctx_", K(ret));
  } else {
    index_tree_root_ctx_->task_idx_ = task_idx;
  }
  return ret;
}

int ObDataIndexBlockBuilder::generate_macro_row(ObMacroBlock &macro_block,
                                                const MacroBlockId &block_id,
                                                const int64_t ddl_start_row_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid index builder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro block id", K(block_id), K(ddl_start_row_offset));
  } else if (OB_UNLIKELY(!macro_block.is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid empty macro block", K(ret));
  } else if (OB_FAIL(append_index_micro_block(macro_block, block_id, ddl_start_row_offset))) {
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
                                   ObMacroBlocksWriteCtx *data_write_ctx)
{
  int ret = OB_SUCCESS;
  ObBaseIndexBlockBuilder *root_builder = nullptr;
  int64_t row_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid index builder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "data index builder is closed", K(ret), K(is_closed_));
  } else if (OB_ISNULL(data_write_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "data write ctx must be not-null", K(ret), K(data_write_ctx));
  } else if (OB_UNLIKELY(index_block_aggregator_.get_row_count() < 0)) {
    STORAGE_LOG(DEBUG, "this partial index tree is empty", K(ret));
  } else if (OB_FAIL(close_index_tree(root_builder))) {
    STORAGE_LOG(WARN, "fail to close index tree", K(ret));
  } else if (OB_UNLIKELY(root_builder != this)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ObDataIndexBlockBuilder should not grow",
        K(ret), K(root_builder), K(this));
  } else if (OB_UNLIKELY(row_count = get_row_count()) > 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "generate_macro_row should flush all index rows",
        K(ret), K(row_count));
  } else if (macro_meta_list_->count() == 0) {
    // do not append root to sstable builder since it's empty
  } else {
    const int64_t blocks_cnt = macro_meta_list_->count();
    ObDataMacroBlockMeta *last_meta = macro_meta_list_->at(blocks_cnt - 1);
    index_tree_root_ctx_->last_key_ = last_meta->end_key_; // no need deep_copy
    index_tree_root_ctx_->use_old_macro_block_count_ = data_write_ctx->use_old_macro_block_count_;
    index_tree_root_ctx_->data_column_cnt_ = data_store_desc_->get_row_column_count();
    // index_tree_root_ctx_->macro_metas_ = macro_meta_list_; // should be done in init method
    index_tree_root_ctx_->data_blocks_cnt_ = blocks_cnt;
    if (blocks_cnt == 1) {
      index_tree_root_ctx_->meta_block_size_ = meta_block_size_;
      index_tree_root_ctx_->meta_block_offset_ = meta_block_offset_;
    }
    data_write_ctx->clear();
    STORAGE_LOG(INFO, "succeed to close data index builder", KPC_(index_tree_root_ctx));
  }
  is_closed_ = true; // close() is not re-entrant
  return ret;
}

int ObDataIndexBlockBuilder::append_next_row(const ObMicroBlockDesc &micro_block_desc,
                                             ObIndexBlockRowDesc &macro_row_desc)
{
  int ret = OB_SUCCESS;
  macro_row_desc.data_store_desc_ = index_store_desc_;
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
  : ObBaseIndexBlockBuilder(),
    micro_writer_(nullptr),
    macro_writer_(nullptr),
    last_leaf_rowkey_(),
    leaf_rowkey_allocator_("MetaMidIdx"),
    meta_row_offset_(-1)
{
}

ObMetaIndexBlockBuilder::~ObMetaIndexBlockBuilder()
{
  reset();
}

void ObMetaIndexBlockBuilder::reset()
{
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
                                  ObMacroBlockWriter &macro_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMetaIndexBlockBuilder has been inited", K(ret));
  } else if (OB_FAIL(ObBaseIndexBlockBuilder::init(
      data_store_desc, data_store_desc, allocator, &macro_writer, 0))) {
    // For meta tree, data and index share the same descriptor
    STORAGE_LOG(WARN, "fail to init base index builder", K(ret));
  } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(&data_store_desc, allocator, micro_writer_))) {
    STORAGE_LOG(WARN, "fail to build micro writer", K(ret));
  } else {
    macro_writer_ = &macro_writer;
  }
  return ret;
}

int ObMetaIndexBlockBuilder::build_micro_block(ObMicroBlockDesc &micro_block_desc)
{
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

int ObMetaIndexBlockBuilder::append_micro_block(ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;
  ObIndexBlockRowDesc row_desc(*index_store_desc_);
  if (OB_FAIL(macro_writer_->append_index_micro_block(micro_block_desc))) {
    micro_writer_->dump_diagnose_info(); // ignore dump error
    STORAGE_LOG(WARN, "fail to append micro block of meta", K(ret), K(micro_block_desc));
  } else if (FALSE_IT(block_to_row_desc(micro_block_desc, row_desc))) {
  } else if (FALSE_IT(row_desc.is_data_block_ = true)) {
  } else if (FALSE_IT(row_desc.is_secondary_meta_ = true)) {
  } else if (FALSE_IT(row_desc.micro_block_count_ = 1)) {
  } else if (FALSE_IT(row_desc.row_offset_ = meta_row_offset_)) {
  } else if (OB_FAIL(ObBaseIndexBlockBuilder::append_row(row_desc))) {
    STORAGE_LOG(WARN, "fail to append n-1 level index row of meta", K(ret), K(row_desc));
  } else {
    micro_writer_->reuse();
    leaf_rowkey_allocator_.reuse();
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
      STORAGE_LOG(WARN, "fail to build micro block of meta", K(ret), K(leaf_row));
    } else if (OB_FAIL(append_micro_block(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to append micro block of meta to macro block", K(ret), K(leaf_row));
    } else if (OB_FAIL(micro_writer_->append_row(leaf_row))) {
      STORAGE_LOG(WARN, "fail to append leaf row of meta", K(ret), K(leaf_row));
    }
  }
  if (OB_SUCC(ret)) {
    leaf_rowkey_allocator_.reuse();
    const int64_t rowkey_cnt = leaf_row.get_column_count() - 1;
    ObDatumRowkey src_rowkey;
    if (OB_UNLIKELY(rowkey_cnt < 1
        || rowkey_cnt != index_store_desc_->get_rowkey_column_count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected rowkey cnt ", K(ret), K(rowkey_cnt), KPC_(index_store_desc));
    } else if (OB_FAIL(src_rowkey.assign(leaf_row.storage_datums_, rowkey_cnt))) {
      STORAGE_LOG(WARN, "Failed to assign src rowkey", K(ret), K(rowkey_cnt), K(leaf_row));
    } else if (OB_FAIL(src_rowkey.deep_copy(last_leaf_rowkey_, leaf_rowkey_allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy rowkey", K(src_rowkey));
    } else {
      meta_row_offset_++;
    }
  }
  return ret;
}

int ObMetaIndexBlockBuilder::close(
    ObIAllocator &allocator,
    const IndexTreeRootCtxList &roots,
    const int64_t nested_size,
    const int64_t nested_offset,
    ObIndexTreeRootBlockDesc &block_desc)
{
  int ret = OB_SUCCESS;
  ObIndexTreeInfo tree_info;
  ObMicroBlockDesc micro_block_desc;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid ObMetaIndexBlockBuilder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta index builder is closed", K(ret), K(is_closed_));
  } else if (OB_FAIL(build_micro_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to build micro block of meta", K(ret));
  } else if (index_block_aggregator_.get_row_count() <= 0 && micro_block_desc.get_block_size() <= ROOT_BLOCK_SIZE_LIMIT) {
    // meta block's size is smaller than ROOT_BLOCK_SIZE_LIMIT, all meta data will be stored in root
    if (OB_FAIL(ObBaseIndexBlockBuilder::close(allocator, tree_info))) {
      STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
    } else if (OB_FAIL(build_single_node_tree(allocator, micro_block_desc, block_desc))) {
      STORAGE_LOG(WARN, "fail to build single node tree of meta", K(ret));
    }
  } else if (index_block_aggregator_.get_row_count() <= 0 && 1 == micro_block_desc.row_count_
      && ObSSTableIndexBuilder::check_version_for_small_sstable(*index_store_desc_)) {
    // this sstable only has one data block, but the size of meta data exceeds ROOT_BLOCK_SIZE_LIMIT,
    // so sstable's root points to the tail of its data block (macro meta row)
    if (OB_FAIL(build_single_macro_row_desc(roots, nested_size, nested_offset, allocator))) {
      STORAGE_LOG(WARN, "fail to build single marcro row descn", K(ret), K(nested_size), K(nested_offset));
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::close(allocator, tree_info))) {
      STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
    } else {
      block_desc = tree_info.root_desc_;
    }
  } else {
    if (OB_FAIL(append_micro_block(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to append micro block of meta to macro block", K(ret));
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::close(allocator, tree_info))) {
      STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
    } else {
      block_desc = tree_info.root_desc_;
    }
  }

  if (OB_SUCC(ret)) {
    is_closed_ = true;
    STORAGE_LOG(DEBUG, "succeed to close index tree of meta", K(ret), K(block_desc));
  }
  return ret;
}

int ObMetaIndexBlockBuilder::build_single_macro_row_desc(
    const IndexTreeRootCtxList &roots,
    const int64_t nested_size,
    const int64_t nested_offset,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  if (OB_UNLIKELY(1 != roots.count() || 1 != roots[0]->macro_metas_->count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected roots", K(ret), K(roots.count()), K(roots[0]->macro_metas_->count()), KPC(roots[0]));
  } else if (0 >= roots[0]->meta_block_size_ || 0 >= roots[0]->meta_block_offset_) {
    const ObDataMacroBlockMeta &macro_meta = *(roots[0]->macro_metas_->at(0));
    ObSSTableMacroBlockHeader macro_header;
    char *data_buf = nullptr;
    if (OB_FAIL(ObSSTableIndexBuilder::load_single_macro_block(macro_meta, nested_size, nested_offset, allocator, data_buf))) {
      STORAGE_LOG(WARN, "fail to load macro block", K(ret), K(nested_size), K(nested_offset), K(macro_meta), KPC(roots[0]));
    } else if (OB_FAIL(ObSSTableIndexBuilder::parse_macro_header(
        data_buf, OB_SERVER_BLOCK_MGR.get_macro_block_size(), macro_header))) {
      STORAGE_LOG(WARN, "fail to parse macro header", K(ret), KP(data_buf));
    } else {
      roots[0]->meta_block_offset_ = macro_header.fixed_header_.meta_block_offset_;
      roots[0]->meta_block_size_ = macro_header.fixed_header_.meta_block_size_;
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(data_desc.assign(*index_store_desc_))) {
    STORAGE_LOG(WARN, "fail to assign data desc", K(ret), KPC(index_store_desc_));
  } else {
    const ObDataBlockMetaVal &macro_meta_val = roots[0]->macro_metas_->at(0)->val_;
    ObStaticDataStoreDesc &static_desc = data_desc.get_static_desc();
    data_desc.get_desc().row_store_type_ = macro_meta_val.row_store_type_;
    static_desc.compressor_type_ = macro_meta_val.compressor_type_;
    static_desc.master_key_id_ = macro_meta_val.master_key_id_;
    static_desc.encrypt_id_ = macro_meta_val.encrypt_id_;
    MEMCPY(static_desc.encrypt_key_, macro_meta_val.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    ObIndexBlockRowDesc row_desc(data_desc.get_desc());
    row_desc.row_key_ = roots[0]->macro_metas_->at(0)->end_key_;
    row_desc.macro_id_ = macro_meta_val.macro_id_;
    row_desc.block_offset_ = roots[0]->meta_block_offset_;
    row_desc.block_size_ = roots[0]->meta_block_size_;
    row_desc.row_count_ = macro_meta_val.row_count_;
    row_desc.row_count_delta_ = macro_meta_val.row_count_delta_;
    row_desc.is_deleted_ = macro_meta_val.is_deleted_;
    row_desc.max_merged_trans_version_ = macro_meta_val.max_merged_trans_version_;
    row_desc.contain_uncommitted_row_ = macro_meta_val.contain_uncommitted_row_;
    last_leaf_rowkey_.reset();
    row_desc.is_data_block_ = true;
    row_desc.is_secondary_meta_ = true;
    row_desc.micro_block_count_ = 1;
    if (OB_FAIL(ObBaseIndexBlockBuilder::append_row(row_desc))) {
      STORAGE_LOG(WARN, "fail to append n-1 level index row of meta", K(ret), K(roots));
    }
  }
  return ret;
}

int ObMetaIndexBlockBuilder::build_single_node_tree(
    ObIAllocator &allocator,
    const ObMicroBlockDesc &micro_block_desc,
    ObIndexTreeRootBlockDesc &block_desc)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr &root_addr = block_desc.addr_;
  block_desc.height_ = 1;
  char *&root_buf = block_desc.buf_;
  const int64_t buf_size = micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
  int64_t pos = 0;
  if (OB_ISNULL(root_buf = static_cast<char *> (allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc root buf", K(ret), K(buf_size));
  } else if (OB_FAIL(micro_block_desc.header_->serialize(root_buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to serialize header", K(ret), K(micro_block_desc));
  } else {
    MEMCPY(root_buf + pos, micro_block_desc.buf_, buf_size - pos);
    if (OB_FAIL(root_addr.set_mem_addr(0, buf_size))) {
      STORAGE_LOG(WARN, "fail to set memory address", K(ret), K(buf_size));
    } else {
      block_desc.is_meta_root_ = true;
      STORAGE_LOG(INFO, "successfully build single node tree, whose root is a data root", K(ret), K(block_desc));
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
  :is_inited_(false),
   need_sort_(true),
   mutex_(common::ObLatchIds::INDEX_BUILDER_LOCK),
   index_store_desc_(nullptr),
   index_tree_root_ctx_(nullptr),
   macro_meta_list_(nullptr),
   sstable_builder_(nullptr)
{
}

ObIndexBlockRebuilder::~ObIndexBlockRebuilder()
{
  reset();
}

void ObIndexBlockRebuilder::reset()
{
  index_store_desc_ = nullptr;
  index_tree_root_ctx_ = nullptr;
  macro_meta_list_ = nullptr;
  need_sort_ = true;
  is_inited_ = false;
  sstable_builder_ = nullptr;
}

int ObIndexBlockRebuilder::init(ObSSTableIndexBuilder &sstable_builder, bool need_sort, const int64_t *task_idx, const bool is_ddl_merge_sstable)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 109;
  ObDataStoreDesc *leaf_store_desc = nullptr; //unused
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIndexBlockRebuilder has been inited", K(ret));
  } else if (OB_FAIL(sstable_builder.init_builder_ptrs(sstable_builder_, index_store_desc_,
      leaf_store_desc, index_tree_root_ctx_, macro_meta_list_))) {
    STORAGE_LOG(WARN, "fail to init referemce pointer members", K(ret));
  } else {
    if (task_idx != nullptr) {
      if (*task_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected task idx value", K(ret), K(task_idx));
      } else {
        index_tree_root_ctx_->task_idx_ = *task_idx;
      }
    }
    if (OB_SUCC(ret)) {
      need_sort_ = need_sort;
      index_tree_root_ctx_->use_absolute_offset_ = is_ddl_merge_sstable;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObIndexBlockRebuilder::get_macro_meta(
    const char *buf,
    const int64_t size,
    const MacroBlockId &macro_id,
    common::ObIAllocator &allocator,
    ObDataMacroBlockMeta *&macro_meta)
{
  int ret = OB_SUCCESS;
  ObSSTableMacroBlockHeader macro_header;
  if (OB_FAIL(inner_get_macro_meta(buf, size, macro_id, allocator, macro_meta, macro_header))) {
    STORAGE_LOG(WARN, "fail to get macro meta", K(ret));
  }
  return ret;
}

int ObIndexBlockRebuilder::inner_get_macro_meta(
    const char *buf,
    const int64_t size,
    const MacroBlockId &macro_id,
    common::ObIAllocator &allocator,
    ObDataMacroBlockMeta *&macro_meta,
    ObSSTableMacroBlockHeader &macro_header)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReader reader;
  // FIXME: macro_reader, micro_reader, read_info, datum_row could be used as member variables
  // to avoid repeatedly allocate memory and construct objects and array. Fix if needed
  // read info could be inited once protected by lock. readers and row need to deal with concurrency
  ObMicroBlockReaderHelper micro_reader_helper;
  ObIMicroBlockReader *micro_reader;
  ObMicroBlockData micro_data;
  ObMicroBlockData meta_block;
  ObDatumRow datum_row;
  ObDataMacroBlockMeta tmp_macro_meta;
  ObDataMacroBlockMeta* tmp_meta_ptr = nullptr;
  bool is_compressed = false;
  if (OB_UNLIKELY(size <= 0 || !macro_id.is_valid()) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size), K(macro_id));
  } else if (OB_FAIL(get_meta_block(buf, size, allocator, macro_header, micro_data))) {
    STORAGE_LOG(WARN, "fail to get meta block and read info", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(reader.decrypt_and_decompress_data(
      macro_header,
      micro_data.get_buf(),
      micro_data.get_buf_size(),
      meta_block.get_buf(),
      meta_block.get_buf_size(),
      is_compressed))) {
    STORAGE_LOG(WARN, "fail to get micro block data", K(ret), K(macro_header), K(micro_data));
  } else if (OB_FAIL(micro_reader_helper.init(allocator))) {
      STORAGE_LOG(WARN, "fail to init micro reader helper", K(ret));
  } else if (OB_UNLIKELY(!meta_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid micro block data", K(ret), K(meta_block));
  } else if (OB_FAIL(micro_reader_helper.get_reader(meta_block.get_store_type(), micro_reader))) {
    STORAGE_LOG(WARN, "fail to get micro reader by store type",
        K(ret), K(meta_block.get_store_type()));
  } else if (OB_FAIL(micro_reader->init(meta_block, nullptr))) {
    STORAGE_LOG(WARN, "fail to init micro reader", K(ret));
  } else if (OB_FAIL(datum_row.init(allocator, macro_header.fixed_header_.rowkey_column_count_ + 1))) {
    STORAGE_LOG(WARN, "fail to init datum row", K(ret));
  } else if (OB_FAIL(micro_reader->get_row(0, datum_row))) {
    STORAGE_LOG(WARN, "fail to get meta row", K(ret));
  } else if (OB_FAIL(tmp_macro_meta.parse_row(datum_row))) {
    STORAGE_LOG(WARN, "fail to parse meta row", K(ret), K(datum_row), K(macro_header), K(macro_id));
  } else {
    tmp_macro_meta.val_.macro_id_ = macro_id;
    if (OB_FAIL(tmp_macro_meta.deep_copy(tmp_meta_ptr, allocator))) {
      STORAGE_LOG(WARN, "fail to deep copy", K(ret), K(datum_row), K(macro_header), K(tmp_macro_meta));
    } else {
      macro_meta = tmp_meta_ptr;
    }
  }
  return ret;
}

int ObIndexBlockRebuilder::append_macro_row(
    const char *buf,
    const int64_t size,
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDataMacroBlockMeta *macro_meta = nullptr;
  ObSSTableMacroBlockHeader macro_header;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuilder not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(inner_get_macro_meta(buf, size, macro_id, allocator, macro_meta, macro_header))) {
    STORAGE_LOG(WARN, "fail to get macro meta", K(ret),K(macro_id));
  } else if (OB_ISNULL(macro_meta)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null macro meta", K(ret),K(macro_id));
  } else if (OB_FAIL(append_macro_row(*macro_meta))) {
    STORAGE_LOG(WARN, "fail to append macro meta", K(ret),K(macro_id), KPC(macro_meta));
  } else {
    index_tree_root_ctx_->meta_block_offset_ = macro_header.fixed_header_.meta_block_offset_;
    index_tree_root_ctx_->meta_block_size_ = macro_header.fixed_header_.meta_block_size_;
    index_tree_root_ctx_->last_macro_size_ = index_tree_root_ctx_->meta_block_offset_
                                             + index_tree_root_ctx_->meta_block_size_;
  }
  if (OB_NOT_NULL(macro_meta)) {
    macro_meta->~ObDataMacroBlockMeta();
    macro_meta = nullptr;
  }
  return ret;
}

int ObIndexBlockRebuilder::get_meta_block(
    const char *buf,
    const int64_t buf_size,
    common::ObIAllocator &allocator,
    ObSSTableMacroBlockHeader &macro_header,
    ObMicroBlockData &meta_block)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMacroBlockCommonHeader common_header;
  if (OB_UNLIKELY(buf_size <= 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(common_header.deserialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize common header", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_FAIL(common_header.check_integrity())) {
    STORAGE_LOG(WARN, "invalid common header", K(ret), K(common_header));
  } else if (OB_FAIL(macro_header.deserialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize macro header", K(ret), KP(buf), K(buf_size), K(pos));
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

int ObIndexBlockRebuilder::append_macro_row(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
   if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuilder not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro meta", K(ret), K(macro_meta));
  } else {
    lib::ObMutexGuard guard(mutex_); // migration will append concurrently
    if (OB_FAIL(index_tree_root_ctx_->add_macro_block_meta(macro_meta))) {// inc_ref
      STORAGE_LOG(WARN, "failed to add macro block meta", K(ret), K(macro_meta));
    }
  }
  return ret;
}

int ObIndexBlockRebuilder::fill_abs_offset_for_ddl()
{
  int ret = OB_SUCCESS;
  if (index_tree_root_ctx_->use_absolute_offset_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_meta_list_->count(); ++i) {
      const ObDataMacroBlockMeta *macro_meta = macro_meta_list_->at(i);
      if (OB_ISNULL(macro_meta)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null macro meta", K(ret), K(i), KPC(index_tree_root_ctx_));
      } else if (macro_meta->val_.version_ < ObDataBlockMetaVal::DATA_BLOCK_META_VAL_VERSION_V2
          || macro_meta->val_.ddl_end_row_offset_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpectd ddl end row offset", K(ret), K(i), KPC(macro_meta));
      } else if (OB_FAIL(index_tree_root_ctx_->add_absolute_row_offset(macro_meta->val_.ddl_end_row_offset_))) {
        STORAGE_LOG(WARN, "failed to add abs row offset", K(ret), K(macro_meta->val_.ddl_end_row_offset_));
      } else {
        STORAGE_LOG(DEBUG, "append macro meta with absolute offset",
            K(macro_meta->val_.ddl_end_row_offset_), KPC(macro_meta));
      }
    }
  } else {
    // not ddl rebuild, do nothing.
  }
  return ret;
}

int ObIndexBlockRebuilder::close()
{
  int ret = OB_SUCCESS;
  ObDataMacroMetaCompare cmp(ret, index_store_desc_->get_datum_utils());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuilder not inited", K(ret), K_(is_inited));
  } else if (need_sort_ && FALSE_IT(lib::ob_sort(macro_meta_list_->begin(), macro_meta_list_->end(), cmp))) {
  } else if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "fail to sort meta list", K(ret), KPC(index_store_desc_));
  } else if (macro_meta_list_->count() == 0) {
    // do not append root to sstable builder since it's empty
  } else if (OB_FAIL(fill_abs_offset_for_ddl())) {
    STORAGE_LOG(WARN, "fail to fill abs offset for ddl", K(ret), KPC_(index_tree_root_ctx));
  } else {
    const int64_t blocks_cnt = macro_meta_list_->count();
    ObDataMacroBlockMeta *last_meta = macro_meta_list_->at(blocks_cnt - 1);
    index_tree_root_ctx_->last_key_ = last_meta->end_key_; // no need deep_copy
    index_tree_root_ctx_->data_column_cnt_ = last_meta->val_.column_count_;
    // index_tree_root_ctx_->macro_metas_ = macro_meta_list_; // should be done in init method
    index_tree_root_ctx_->data_blocks_cnt_ = blocks_cnt;
    STORAGE_LOG(INFO, "succeed to close rebuilder", KPC_(index_tree_root_ctx), K(need_sort_));
  }
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase
