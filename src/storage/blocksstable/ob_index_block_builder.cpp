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

#include "ob_index_block_builder.h"
#include "ob_index_block_macro_iterator.h"
#include "ob_macro_block_writer.h"
#include "ob_imacro_block_flush_callback.h"
#include "lib/utility/utility.h"
#include "lib/checksum/ob_crc64.h"
#include "share/schema/ob_column_schema.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_encryption_util.h"
#include "storage/ob_storage_struct.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{
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
      STORAGE_LOG(ERROR, "failed to dec macro block ref cnt",
          K(ret), "macro id", block_id);
    }
  }
  data_block_ids_.destroy();
  for (int64_t i = 0; i < other_block_ids_.count(); ++i) {
    const MacroBlockId &block_id = other_block_ids_.at(i);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(block_id))) {
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
  : orig_allocator_("SSTMidIdxData", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    allocator_(orig_allocator_),
    self_allocator_("SSTMidIdxSelf", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    mutex_(common::ObLatchIds::INDEX_BUILDER_LOCK),
    index_store_desc_(),
    container_store_desc_(),
    index_write_ctxs_(),
    index_row_(),
    micro_reader_(nullptr),
    reader_helper_(),
    index_builder_(),
    data_builder_(),
    macro_writer_(),
    callback_(nullptr),
    roots_(),
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
  index_store_desc_.reset();
  container_store_desc_.reset();
  for (int64_t i = 0; i < index_write_ctxs_.count(); ++i) {
    if (OB_NOT_NULL(index_write_ctxs_[i])) {
      index_write_ctxs_[i]->~ObMacroBlocksWriteCtx();
      index_write_ctxs_[i] = nullptr;
    }
  }
  for (int64_t i = 0; i < roots_.count(); ++i) {
    release_index_block_desc(roots_[i]);
  }
  index_row_.reset();
  micro_reader_ = nullptr;
  reader_helper_.reset();
  index_builder_.reset();
  data_builder_.reset();
  macro_writer_.reset();
  callback_ = nullptr;
  index_write_ctxs_.reset();
  roots_.reset();
  index_row_.reset();
  res_.reset();
  allocator_.reset();
  orig_allocator_.reset();
  self_allocator_.reset();
  optimization_mode_ = ENABLE;
  is_closed_ = false;
  is_inited_ = false;
}

bool ObSSTableIndexBuilder::check_index_desc(const ObDataStoreDesc &index_desc) const
{
  bool ret = true;
  // TODO(zhuixin.gsy): these args influence write_micro_block and need to be evaluated
  if (!index_desc.is_valid()
      || index_desc.need_prebuild_bloomfilter_
      || index_desc.merge_info_ != nullptr
      || index_desc.row_column_count_ != index_desc.rowkey_column_count_ + 1
      || (index_desc.is_major_merge() && index_desc.major_working_cluster_version_ < DATA_VERSION_4_0_0_0)) {
    ret = false;
  }
  return ret;
}
int ObSSTableIndexBuilder::init(const ObDataStoreDesc &index_desc,
                                ObIMacroBlockFlushCallback *callback,
                                ObSpaceOptimizationMode mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSSTableIndexBuilder has been inited", K(ret));
  } else if (OB_UNLIKELY(!check_index_desc(index_desc))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid index store desc", K(ret), K(index_desc));
  } else if (OB_FAIL(set_row_store_type(const_cast<ObDataStoreDesc&>(index_desc)))) {
    STORAGE_LOG(WARN, "fail to set row store type", K(ret));
  } else if (OB_FAIL(index_store_desc_.assign(index_desc))) {
    STORAGE_LOG(WARN, "fail to assign index_store_desc", K(ret), K(index_desc));
  } else if (OB_FAIL(container_store_desc_.assign(index_desc))) {
    STORAGE_LOG(WARN, "fail to assign container_store_desc", K(ret), K(index_desc));
  } else if (OB_FAIL(reader_helper_.init(allocator_))) {
    STORAGE_LOG(WARN, "fail to init reader helper", K(ret));
  } else if (OB_FAIL(reader_helper_.get_reader(index_desc.row_store_type_, micro_reader_))) {
    STORAGE_LOG(WARN, "fail to get micro reader", K(ret), K(index_desc));
  } else if (OB_FAIL(index_row_.init(index_desc.rowkey_column_count_ + 1))) {
    STORAGE_LOG(WARN, "Failed to init index row", K(ret), K(index_desc));
  } else {
    index_store_desc_.sstable_index_builder_ = this;
    index_store_desc_.need_pre_warm_ = true;
    callback_ = callback;
    optimization_mode_ = mode;
    index_store_desc_.need_build_hash_index_for_micro_block_ = false;
    container_store_desc_.need_build_hash_index_for_micro_block_ = false;
    is_inited_ = true;
  }
  STORAGE_LOG(DEBUG, "init sstable index builder", K(ret), K(index_desc), K_(index_store_desc));
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
  } else if (ENCODING_ROW_STORE == index_desc.row_store_type_) {
    index_desc.row_store_type_ = SELECTIVE_ENCODING_ROW_STORE;
    index_desc.encoder_opt_.set_store_type(SELECTIVE_ENCODING_ROW_STORE);
  }
  return ret;
}

int ObSSTableIndexBuilder::new_index_builder(ObDataIndexBlockBuilder *&builder,
                                             ObDataStoreDesc &data_store_desc,
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
    ObIAllocator *&sstable_allocator,
    ObIndexMicroBlockDesc *&root_micro_block_desc,
    ObMacroMetasArray *&macro_meta_list)
{

  int ret = OB_SUCCESS;
  void *array_buf = nullptr;
  void *desc_buf = nullptr;
  ObIndexMicroBlockDesc *tmp_root_desc = nullptr;
  ObMacroMetasArray *tmp_macro_meta_array = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_ISNULL(array_buf = allocator_.alloc(sizeof(ObMacroMetasArray)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(tmp_macro_meta_array = new (array_buf) ObMacroMetasArray())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new ObMacroMetasArray", K(ret));
  } else if (OB_ISNULL(desc_buf = allocator_.alloc(sizeof(ObIndexMicroBlockDesc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(tmp_root_desc = new (desc_buf) ObIndexMicroBlockDesc())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObIndexMicroBlockDesc", K(ret));
  } else {
    sstable_builder = this;
    index_store_desc = &index_store_desc_;
    sstable_allocator = &allocator_;
    tmp_root_desc->macro_metas_ = tmp_macro_meta_array;
    if (OB_FAIL(append_root(*tmp_root_desc))) {
      STORAGE_LOG(WARN, "Fail to append root micro block desc, ", K(ret), K(*tmp_root_desc));
    } else {
      macro_meta_list = tmp_macro_meta_array;
      root_micro_block_desc = tmp_root_desc;
      tmp_macro_meta_array = nullptr;
      tmp_root_desc = nullptr;
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != tmp_macro_meta_array) {
      tmp_macro_meta_array->~ObIArray<ObDataMacroBlockMeta*>();
      allocator_.free(array_buf);
      tmp_macro_meta_array = nullptr;
      array_buf = nullptr;
    }

    if (nullptr != tmp_root_desc) {
      tmp_root_desc->~ObIndexMicroBlockDesc();
      allocator_.free(desc_buf);
      tmp_root_desc = nullptr;
      desc_buf = nullptr;
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::append_root(ObIndexMicroBlockDesc &root_micro_block_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(roots_.push_back(&root_micro_block_desc))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to push into roots", K(ret));
    }
  }
  return ret;
}

void ObSSTableIndexBuilder::release_index_block_desc(ObIndexMicroBlockDesc *&root)
{
  if (OB_NOT_NULL(root)) {
    if (OB_NOT_NULL(root->data_write_ctx_)) {
      root->data_write_ctx_->~ObMacroBlocksWriteCtx();
      allocator_.free(static_cast<void*>(static_cast<void*>(root->data_write_ctx_)));
      root->data_write_ctx_ = nullptr;
    }
    if (OB_NOT_NULL(root->macro_metas_)) {
      for (int64_t j = 0; j < root->macro_metas_->count(); ++j) {
        ObDataMacroBlockMeta *&meta = root->macro_metas_->at(j);
        if (OB_NOT_NULL(meta)) {
          meta->~ObDataMacroBlockMeta();
          allocator_.free(static_cast<void*>(static_cast<void*>(meta)));
          meta = nullptr;
        }
      }
      root->macro_metas_->~ObIArray<ObDataMacroBlockMeta*>();
      allocator_.free(static_cast<void*>(root->macro_metas_));
      root->macro_metas_ = nullptr;
    }
    root->~ObIndexMicroBlockDesc();
    allocator_.free(static_cast<void*>(root));
    root = nullptr;
  }
}

int ObSSTableIndexBuilder::trim_empty_roots()
{
  int ret = OB_SUCCESS;
  const int64_t root_count = roots_.count();
  IndexMicroBlockDescList tmp_roots;
  if (0 == root_count) {
    // do nothing
  } else if (OB_FAIL(tmp_roots.reserve(root_count))) {
    STORAGE_LOG(WARN, "fail to reserve tmp roots", K(ret), K(root_count));
  } else {
    for (int64_t i = 0; i < root_count && OB_SUCC(ret); ++i) {
      if (nullptr == roots_[i]) {
        // skip
      } else if (0 == roots_[i]->data_blocks_cnt_) {
        release_index_block_desc(roots_[i]);  // release
      } else {
        if (OB_FAIL(tmp_roots.push_back(roots_[i]))) {
          STORAGE_LOG(WARN, "fail to push back root", K(ret), KPC(roots_[i]));
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
          release_index_block_desc(tmp_roots[i]);  // release
        }
      }
    }
  }
  return ret;
}

int ObSSTableIndexBuilder::sort_roots()
{
  int ret = OB_SUCCESS;
  ObIndexMicroBlockDescCompare cmp(ret, index_store_desc_.datum_utils_);
  std::sort(roots_.begin(), roots_.end(), cmp);
  return ret;
}

int ObSSTableIndexBuilder::merge_index_tree(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq start_seq;
  ObDataStoreDesc data_desc;
  start_seq.set_index_block();
  if (OB_FAIL(macro_writer_.open(container_store_desc_, start_seq, callback_))) {
    STORAGE_LOG(WARN, "fail to open index macro writer", K(ret));
  } else if (OB_FAIL(index_builder_.init(index_store_desc_, self_allocator_, &macro_writer_, 1))) {
    STORAGE_LOG(WARN, "fail to new index builder", K(ret));
  } else if (OB_FAIL(data_desc.assign(index_store_desc_))) {
    STORAGE_LOG(WARN, "fail to assign data desc", K(ret), K_(index_store_desc));
  } else {
    const int64_t curr_logical_version = index_store_desc_.get_logical_version();
    ObIndexBlockRowDesc row_desc(data_desc);
    ObLogicMacroBlockId prev_logic_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
      ObMacroMetasArray *macro_metas = roots_[i]->macro_metas_;
      for (int64_t j = 0; OB_SUCC(ret) && j < macro_metas->count(); ++j) {
        ObDataMacroBlockMeta *macro_meta = macro_metas->at(j);
        if (OB_ISNULL(macro_meta)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null macro meta", K(ret), K(j), KPC(roots_.at(i)));
        } else if (OB_UNLIKELY(macro_meta->get_logic_id() == prev_logic_id)) {
          // Since we rely on upper stream of sstable writing process to ensure the uniqueness of logic id
          // and we don't want more additional memory/time consumption, we only check continuous ids here
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected duplicate logic macro id", K(ret), KPC(macro_meta), K(prev_logic_id));
        } else if (OB_FAIL(index_builder_.append_row(*macro_meta, row_desc))) {
          STORAGE_LOG(WARN, "fail to append row", K(ret), KPC(macro_meta), K(j), KPC(roots_.at(i)));
        } else {
          prev_logic_id = macro_meta->get_logic_id();
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
    } else if (OB_FAIL(macro_writer_.get_macro_block_write_ctx().deep_copy(
        index_write_ctx, self_allocator_))) {
      STORAGE_LOG(WARN, "Fail to copy index write ctx", K(ret));
    } else if (OB_FAIL(index_write_ctxs_.push_back(index_write_ctx))) {
      STORAGE_LOG(WARN, "fail to push index write ctx", K(ret));
    } else {
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
  } else if (OB_FAIL(index_row_.reserve(desc.row_column_count_))) {
    STORAGE_LOG(WARN, "Failed to reserve column cnt", K(ret), K(desc.row_column_count_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < roots_.count(); ++i) {
      ObMacroMetasArray *macro_metas = roots_[i]->macro_metas_;
      for (int64_t j = 0; OB_SUCC(ret) && j < macro_metas->count(); ++j) {
        ObDataMacroBlockMeta *macro_meta = macro_metas->at(j);
        if (OB_ISNULL(macro_meta)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null macro meta", K(ret), K(j), K(roots_.at(i)));
        } else if (OB_FAIL(macro_meta->build_row(index_row_, self_allocator_))) {
          STORAGE_LOG(WARN, "fail to build row of macro meta", K(ret), KPC(macro_meta));
        } else if (OB_FAIL(builder.append_leaf_row(index_row_))) {
          STORAGE_LOG(WARN, "fail to append leaf row", K(ret), K(index_row_));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(builder.close(roots_, res.data_root_desc_))) {
    STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
  } else if (OB_FAIL(macro_writer_.close())) {
    STORAGE_LOG(WARN, "fail to close macro block writer", K(ret));
  } else if (OB_FAIL(macro_writer_.get_macro_block_write_ctx().deep_copy(
        index_write_ctx, self_allocator_))) {
      STORAGE_LOG(WARN, "Fail to copy index write ctx", K(ret));
  } else if (OB_FAIL(index_write_ctxs_.push_back(index_write_ctx))) {
    STORAGE_LOG(WARN, "fail to push index write ctx", K(ret));
  } else {
    builder.reset();
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
      ObMacroBlocksWriteCtx *write_ctx = roots_[i]->data_write_ctx_;
      if (OB_ISNULL(write_ctx)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "invalid null data write ctx", K(ret));
      } else if (OB_UNLIKELY(macro_metas->count() != write_ctx->macro_block_list_.count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "expect two equal", K(ret), KPC(macro_metas), KPC(write_ctx));
      } else if (OB_FAIL(write_ctx->get_macro_id_array(res.data_block_ids_))) {
        STORAGE_LOG(WARN, "fail to get macro ids of data blocks", K(ret));
      } else {
        res.use_old_macro_block_count_ += write_ctx->use_old_macro_block_count_;
      }
    }
  }

  // collect info of index macro blocks
  for (int64_t i = 0; OB_SUCC(ret) && i < index_write_ctxs_.count(); ++i) {
    ObMacroBlocksWriteCtx *write_ctx = index_write_ctxs_.at(i);
    if (OB_ISNULL(write_ctx)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "invalid null index write ctx", K(ret), K(index_write_ctxs_), K(i));
    } else if (OB_FAIL(write_ctx->get_macro_id_array(res.other_block_ids_))) {
      STORAGE_LOG(WARN, "fail to get macro ids of index blocks", K(ret));
    } else {
      res.index_blocks_cnt_ += write_ctx->macro_block_list_.count();
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
      || meta.get_meta_val().column_count_ > res.data_column_cnt_
      || res.data_column_cnt_ > index_store_desc_.col_default_checksum_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(meta), K_(res.data_column_cnt), K_(index_store_desc));
  } else if (OB_UNLIKELY((index_store_desc_.is_major_merge() || index_store_desc_.is_meta_major_merge())
        && !index_store_desc_.default_col_checksum_array_valid_
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
      res.data_column_checksums_.at(i) += meta.val_.row_count_
       * index_store_desc_.col_default_checksum_array_.at(i);
    }
  }
  return ret;
}

void ObSSTableIndexBuilder::clean_status()
{
  index_builder_.reset();
  data_builder_.reset();
  macro_writer_.reset();
  for (int64_t i = 0; i < index_write_ctxs_.count(); ++i) {
    if (OB_NOT_NULL(index_write_ctxs_[i])) {
      index_write_ctxs_[i]->~ObMacroBlocksWriteCtx();
      index_write_ctxs_[i] = nullptr;
    }
  }
  index_write_ctxs_.destroy();
  // release memory to avoid occupying too much if retry frequently
  self_allocator_.reset();
}

int ObSSTableIndexBuilder::close(ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid sstable builder", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_closed_)) {
    if (OB_FAIL(res.assign(res_))) {
      STORAGE_LOG(WARN, "fail to assign res", K(ret), K_(res));
    }
  } else if (OB_FAIL(res.prepare_column_checksum_array(index_store_desc_.full_stored_col_cnt_))) {
    STORAGE_LOG(WARN, "fail to prepare column checksum array", K(ret));
  } else if (OB_FAIL(trim_empty_roots())) {
    STORAGE_LOG(WARN, "fail to trim empty roots", K(ret));
  } else if (OB_UNLIKELY(roots_.empty())) {
    res.root_desc_.set_empty();
    res.data_root_desc_.set_empty();
    STORAGE_LOG(DEBUG, "sstable has no data", K(ret));
  } else if (OB_FAIL(sort_roots())) {
    STORAGE_LOG(WARN, "fail to sort roots", K(ret));
  } else if (check_version_for_small_sstable(index_store_desc_)) {
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
        case AUTO:
          if (OB_FAIL(check_and_rewrite_sstable_without_size(res))) {
            STORAGE_LOG(WARN, "fail to check and rewrite small sstable", K(ret));
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "the optimization mode is invalid", K(ret), K(optimization_mode_));
          break;
      }
    }
  } else {
    res.nested_offset_ = 0;
    res.nested_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
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
    res.root_row_store_type_ = index_store_desc_.row_store_type_;
    res.compressor_type_ = index_store_desc_.compressor_type_;
    res.encrypt_id_ = index_store_desc_.encrypt_id_;
    MEMCPY(res.encrypt_key_, index_store_desc_.encrypt_key_,
        share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    res.master_key_id_ = index_store_desc_.master_key_id_;
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
          K(allocator_.total()), K(allocator_.used()), K(self_allocator_.total()), K(self_allocator_.used()));
    } else {
      STORAGE_LOG(ERROR, "fail to close sstable index builder", K(ret), K(data_blocks_cnt),
          K(allocator_.total()), K(allocator_.used()), K(self_allocator_.total()), K(self_allocator_.used()));
    }
    clean_status(); // clear since re-entrant
  }
  return ret;
}

bool ObSSTableIndexBuilder::check_version_for_small_sstable(const ObDataStoreDesc &index_desc)
{
  return !index_desc.is_major_merge()
      || index_desc.major_working_cluster_version_ >= DATA_VERSION_4_1_0_0;
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
  read_info.io_desc_.set_group_id(ObIOModule::SSTABLE_INDEX_BUILDER_IO);
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);

  if (OB_FAIL(ObBlockManager::async_read_block(read_info, read_handle))) {
    STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info), K(macro_meta), K(roots_[0]->last_macro_size_));
  } else if (OB_FAIL(read_handle.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_timeout_ms));
  } else {
    ObSharedMacroBlockMgr *shared_block_mgr = MTL(ObSharedMacroBlockMgr*);
    if (OB_FAIL(shared_block_mgr->write_block(
        read_handle.get_buffer(), read_handle.get_data_size(), block_info, *(roots_[0]->data_write_ctx_)))) {
      STORAGE_LOG(WARN, "fail to write small sstable through shared_block_mgr", K(ret));
    } else if (OB_UNLIKELY(!block_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "successfully rewrite small sstable, but block info is invali", K(ret), K(block_info));
    } else {
      roots_[0]->macro_metas_->at(0)->val_.macro_id_ = block_info.macro_id_;
      res.nested_offset_ = block_info.nested_offset_;
      res.nested_size_ = block_info.nested_size_;
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
    roots_[0]->macro_metas_->at(0)->val_.macro_id_ = block_info.macro_id_;
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
  ObMacroBlockHandle read_handle;
  const ObDataMacroBlockMeta &macro_meta = *(roots_[0]->macro_metas_->at(0));
  ObSSTableMacroBlockHeader macro_header;

  if (OB_FAIL(load_single_macro_block(macro_meta, read_handle, macro_header))) {
    STORAGE_LOG(WARN, "fail to load macro block", K(ret), K(macro_meta), KPC(roots_[0]));
  } else {
    roots_[0]->meta_block_offset_ = macro_header.fixed_header_.meta_block_offset_;
    roots_[0]->meta_block_size_ = macro_header.fixed_header_.meta_block_size_;
    const int64_t align_size = upper_align(
      macro_header.fixed_header_.meta_block_offset_ + macro_header.fixed_header_.meta_block_size_,
      DIO_READ_ALIGN_SIZE);
    if (align_size < SMALL_SSTABLE_THRESHOLD) { // need to be rewritten
      ObSharedMacroBlockMgr *shared_block_mgr = MTL(ObSharedMacroBlockMgr*);
      if (OB_FAIL(shared_block_mgr->write_block(read_handle.get_buffer(), align_size, block_info, *(roots_[0]->data_write_ctx_)))) {
        STORAGE_LOG(WARN, "fail to write small sstable through shared_block_mgr", K(ret));
      } else if (OB_UNLIKELY(!block_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "successfully rewrite small sstable, but block info is invalid", K(ret), K(block_info));
      }
    }
  }

  return ret;
}

int ObSSTableIndexBuilder::load_single_macro_block(
    const ObDataMacroBlockMeta &macro_meta,
    ObMacroBlockHandle &read_handle,
    ObSSTableMacroBlockHeader &macro_header)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_ = macro_meta.val_.macro_id_;
  read_info.offset_ = 0;
  read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_desc_.set_group_id(ObIOModule::SSTABLE_INDEX_BUILDER_IO);
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);

  if (OB_FAIL(ObBlockManager::async_read_block(read_info, read_handle))) {
    STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info), K(macro_meta));
  } else if (OB_FAIL(read_handle.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_timeout_ms));
  } else if (OB_FAIL(parse_macro_header(read_handle.get_buffer(), read_handle.get_data_size(), macro_header))) {
    STORAGE_LOG(WARN, "fail to parse macro header", K(ret));
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
  :is_inited_(false),
   is_closed_(false),
   index_store_desc_(nullptr),
   idx_read_info_(),
   row_builder_(),
   last_rowkey_(),
   rowkey_allocator_("BaseBuilder", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
   allocator_(nullptr),
   micro_writer_(nullptr),
   macro_writer_(nullptr),
   index_block_pre_warmer_(),
   row_count_(0),
   row_count_delta_(0),
   max_merged_trans_version_(0),
   macro_block_count_(0),
   micro_block_count_(0),
   can_mark_deletion_(true),
   contain_uncommitted_row_(false),
   has_string_out_row_(false),
   has_lob_out_row_(false),
   is_last_row_last_flag_(false),
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
  index_store_desc_ = nullptr;
  idx_read_info_.reset();
  last_rowkey_.reset();
  rowkey_allocator_.reset();
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
  allocator_ = nullptr;
  level_ = 0;
  reset_accumulative_info();
  is_inited_ = false;
}

int ObBaseIndexBlockBuilder::init(ObDataStoreDesc &index_store_desc,
                                  ObIAllocator &allocator,
                                  ObMacroBlockWriter *macro_writer,
                                  const int64_t level)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBaseIndexBlockBuilder has been inited", K(ret));
  } else {
    index_store_desc_ = &index_store_desc;
    allocator_ = &allocator;
    macro_writer_ = macro_writer;
    level_ = level;
    if (!idx_read_info_.is_valid() && OB_FAIL(idx_read_info_.init(allocator,
        index_store_desc_->row_column_count_ - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
        index_store_desc_->schema_rowkey_col_cnt_,
        lib::is_oracle_mode(),
        index_store_desc_->get_rowkey_col_descs()))) {
      STORAGE_LOG(WARN, "Fail to init index read info", K(ret));
    } else if (OB_FAIL(row_builder_.init(*index_store_desc_))) {
      STORAGE_LOG(WARN, "fail to init ObBaseIndexBlockBuilder", K(ret));
    } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(index_store_desc_, allocator, micro_writer_))) {
      STORAGE_LOG(WARN, "fail to build micro writer", K(ret));
    } else {
      if (index_store_desc_->need_pre_warm_) {
        index_block_pre_warmer_.init();
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
  } else if (last_rowkey_.is_valid()) {
    const ObDatumRowkey &cur_rowkey = row_desc.row_key_;
    int32_t compare_result = 0;
    const ObStorageDatumUtils &datum_utils = index_store_desc_->datum_utils_;
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
    rowkey_allocator_.reuse();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_desc.row_key_.deep_copy(last_rowkey_, rowkey_allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy last rowkey", K(ret), K(row_desc));
  } else {
    row_count_ += row_desc.row_count_;
    row_count_delta_ += row_desc.row_count_delta_;
    can_mark_deletion_ = can_mark_deletion_ && row_desc.is_deleted_;
    max_merged_trans_version_ = max_merged_trans_version_ > row_desc.max_merged_trans_version_
                              ? max_merged_trans_version_
                              : row_desc.max_merged_trans_version_;
    contain_uncommitted_row_ = contain_uncommitted_row_
                            || row_desc.contain_uncommitted_row_;
    has_string_out_row_ = has_string_out_row_ || row_desc.has_string_out_row_;
    has_lob_out_row_ = has_lob_out_row_ || row_desc.has_lob_out_row_;
    micro_block_count_ += row_desc.micro_block_count_;
    macro_block_count_ += row_desc.macro_block_count_;
    // use the flag of the last row in last micro block
    is_last_row_last_flag_ = row_desc.is_last_row_last_flag_;
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
  } else if (OB_UNLIKELY(row_count_ <= 0)) {
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
        root_builder->update_accumulative_info(root_row_desc);
        if (OB_FAIL(root_addr.set_block_addr(root_row_desc.macro_id_,
                                             root_row_desc.block_offset_,
                                             root_row_desc.block_size_))) {
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
      tree_info.row_count_ = root_builder->row_count_;
      tree_info.max_merged_trans_version_ = root_builder->max_merged_trans_version_;
      tree_info.contain_uncommitted_row_ = root_builder->contain_uncommitted_row_;
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
  reset_accumulative_info(); // accumulate only for one index block
}


int ObBaseIndexBlockBuilder::append_index_micro_block()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMicroBlockDesc micro_block_desc;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid base index builder", K(ret), K(is_inited_));
  } else if (OB_FAIL(build_index_micro_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to build index micro block", K(ret));
  } else {
    if (index_block_pre_warmer_.is_valid()
        && OB_TMP_FAIL(index_block_pre_warmer_.reserve_kvpair(micro_block_desc, level_+1))) {
      if (OB_BUF_NOT_ENOUGH != tmp_ret) {
        STORAGE_LOG(WARN, "Fail to reserve kvpair", K(tmp_ret));
      }
    }
    if (OB_FAIL(macro_writer_->append_index_micro_block(micro_block_desc))) {
      micro_writer_->dump_diagnose_info(); // ignore dump error
      STORAGE_LOG(WARN, "fail to append index micro block", K(ret), K(micro_block_desc));
    } else if (OB_FAIL(append_next_row(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to append next row", K(ret), K(micro_block_desc));
    } else if (FALSE_IT(clean_status())) {
    }
    if (OB_FAIL(ret) || OB_TMP_FAIL(tmp_ret) || !index_block_pre_warmer_.is_valid()) {
    } else if (OB_TMP_FAIL(index_block_pre_warmer_.update_and_put_kvpair(micro_block_desc))) {
      STORAGE_LOG(WARN, "Fail to build index block cache key and put into cache", K(tmp_ret));
    }
    index_block_pre_warmer_.reuse();
  }

  return ret;
}

void ObBaseIndexBlockBuilder::update_accumulative_info(ObIndexBlockRowDesc &next_row_desc)
{
  next_row_desc.row_count_ = row_count_;
  next_row_desc.row_count_delta_ = row_count_delta_;
  next_row_desc.is_deleted_ = can_mark_deletion_;
  next_row_desc.max_merged_trans_version_ = max_merged_trans_version_;
  next_row_desc.contain_uncommitted_row_ = contain_uncommitted_row_;
  next_row_desc.has_string_out_row_ = has_string_out_row_;
  next_row_desc.has_lob_out_row_ = has_lob_out_row_;
  next_row_desc.macro_block_count_ = macro_block_count_;
  next_row_desc.micro_block_count_ = micro_block_count_;
  next_row_desc.is_last_row_last_flag_ = is_last_row_last_flag_;
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
    //TODO(zhuixin.gsy) check if row_count == 0 is error?
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
}

int ObBaseIndexBlockBuilder::meta_to_row_desc(
    const ObDataMacroBlockMeta &macro_meta,
    ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc *data_desc = nullptr;
  if (OB_ISNULL(row_desc.data_store_desc_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null data store desc", K(ret));
  } else if (FALSE_IT(data_desc = const_cast<ObDataStoreDesc *>(row_desc.data_store_desc_))) {
  } else if (OB_FAIL(data_desc->end_scn_.convert_for_tx(macro_meta.val_.snapshot_version_))) {
    STORAGE_LOG(WARN, "fail to convert scn", K(ret), K(macro_meta.val_.snapshot_version_));
  } else {
    data_desc->row_store_type_ = macro_meta.val_.row_store_type_;
    data_desc->compressor_type_ = macro_meta.val_.compressor_type_;
    data_desc->master_key_id_ = macro_meta.val_.master_key_id_;
    data_desc->encrypt_id_ = macro_meta.val_.encrypt_id_;
    MEMCPY(data_desc->encrypt_key_, macro_meta.val_.encrypt_key_, sizeof(data_desc->encrypt_key_));
    data_desc->schema_version_ = macro_meta.val_.schema_version_;
    data_desc->snapshot_version_ = macro_meta.val_.snapshot_version_;

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
  }
  return ret;
}

void ObBaseIndexBlockBuilder::row_desc_to_meta(
    const ObIndexBlockRowDesc &macro_row_desc,
    ObDataMacroBlockMeta &macro_meta)
{
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
}


//===================== ObBaseIndexBlockBuilder(private) ================
void ObBaseIndexBlockBuilder::reset_accumulative_info()
{
  row_count_ = 0;
  row_count_delta_ = 0;
  can_mark_deletion_ = true;
  max_merged_trans_version_ = 0;
  contain_uncommitted_row_ = false;
  has_string_out_row_ = false;
  has_lob_out_row_ = false;
  is_last_row_last_flag_ = false;
  macro_block_count_ = 0;
  micro_block_count_ = 0;
}

int ObBaseIndexBlockBuilder::new_next_builder(ObBaseIndexBlockBuilder *&next_builder)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObBaseIndexBlockBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(next_builder = new (buf) ObBaseIndexBlockBuilder())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObBaseIndexBlockBuilder", K(ret));
  } else if (OB_FAIL(next_builder->init(*index_store_desc_,
                                        *allocator_,
                                        macro_writer_,
                                        level_ + 1))) {
    STORAGE_LOG(WARN, "fail to init next builder", K(ret));
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
  } else if (FALSE_IT(update_accumulative_info(next_row_desc))) {
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
    if (micro_writer_->get_block_size() >= index_store_desc_->micro_block_size_ /*force split*/
      || OB_FAIL(micro_writer_->append_row(*index_row))) {
      if (OB_FAIL(ret) && OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret), K(*index_row));
      } else if (OB_UNLIKELY(0 == micro_writer_->get_row_count())) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "The single row is too large, ", K(ret), K(*index_row), KPC_(index_store_desc));
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
  : data_store_desc_(nullptr),
    sstable_builder_(nullptr),
    sstable_allocator_(nullptr),
    leaf_store_desc_(),
    micro_helper_(),
    macro_row_desc_(),
    root_micro_block_desc_(nullptr),
    macro_meta_list_(nullptr),
    meta_block_writer_(nullptr),
    meta_row_(),
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
  data_store_desc_ = nullptr;
  sstable_builder_ = nullptr;
  leaf_store_desc_.reset();
  micro_helper_.reset();
  root_micro_block_desc_ = nullptr;
  macro_meta_list_ = nullptr;
  if (OB_NOT_NULL(meta_block_writer_)) {
    meta_block_writer_->~ObIMicroBlockWriter();
    sstable_allocator_->free(meta_block_writer_);
    meta_block_writer_ = nullptr;
  }
  meta_row_.reset();
  data_blocks_cnt_ = 0;
  meta_block_offset_ = 0;
  meta_block_size_ = 0;
  estimate_leaf_block_size_ = 0;
  estimate_meta_block_size_ = 0;
  sstable_allocator_ = nullptr;
  ObBaseIndexBlockBuilder::reset();
}

int ObDataIndexBlockBuilder::init(
    ObDataStoreDesc &data_store_desc,
    ObSSTableIndexBuilder &sstable_builder)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObDataStoreDesc *index_store_desc = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDataIndexBlockBuilder has been inited", K(ret));
  } else if (OB_FAIL(sstable_builder.init_builder_ptrs(sstable_builder_, index_store_desc,
      sstable_allocator_, root_micro_block_desc_, macro_meta_list_))) {
    STORAGE_LOG(WARN, "fail to init referemce pointer members", K(ret));
  } else if (OB_UNLIKELY(index_store_desc->row_store_type_ != data_store_desc.row_store_type_
      && (index_store_desc->row_store_type_ == FLAT_ROW_STORE
          || data_store_desc.row_store_type_ == FLAT_ROW_STORE)
      && !data_store_desc.is_force_flat_store_type_)) {
    // since n-1 micro block should keep format same with data_blocks
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "expect row store type equal", K(ret), KPC(index_store_desc), K(data_store_desc));
  } else if (OB_FAIL(idx_read_info_.init(*sstable_allocator_, *index_store_desc))) {
    STORAGE_LOG(WARN, "failed to init idx read info", KPC(index_store_desc), K(ret));
  } else if (OB_FAIL(micro_helper_.open(*index_store_desc, idx_read_info_, *sstable_allocator_))) {
    STORAGE_LOG(WARN, "fail to open base writer", K(ret));
  } else if (OB_FAIL(meta_row_.init(index_store_desc->row_column_count_))) {
    STORAGE_LOG(WARN, "fail to init flat writer", K(ret));
  } else if (OB_FAIL(leaf_store_desc_.assign(*index_store_desc))) {
    STORAGE_LOG(WARN, "fail to assign leaf store desc", K(ret));
  } else if (data_store_desc.is_force_flat_store_type_) {
    leaf_store_desc_.force_flat_store_type();
  }
  if (FAILEDx(ObMacroBlockWriter::build_micro_writer(
                    &leaf_store_desc_,
                    *sstable_allocator_,
                    meta_block_writer_,
                    GCONF.micro_block_merge_verify_level))) {
    STORAGE_LOG(WARN, "fail to init meta block writer", K(ret));
  } else {
    // only increase the size limit of n-1 level micro block
    leaf_store_desc_.micro_block_size_ = leaf_store_desc_.micro_block_size_limit_; // nearly 2M
    if (OB_FAIL(ObBaseIndexBlockBuilder::init(leaf_store_desc_, *sstable_allocator_, nullptr, 0))) {
      STORAGE_LOG(WARN, "fail to init base index builder", K(ret));
    } else {
      data_store_desc_ = &data_store_desc;
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
  const int64_t column_cnt = data_store_desc_->row_column_count_;
  if (OB_UNLIKELY(rowkey_cnt != meta_row_.count_ - 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected rowkey cnt equal", K(ret), K(rowkey_cnt), K(meta_row_), K(rowkey));
  } else if (OB_FAIL(macro_meta.end_key_.assign(rowkey.datums_, rowkey.datum_cnt_))) {
    STORAGE_LOG(WARN, "fail to assign end key for macro meta", K(ret), K(rowkey));
  } else {
    macro_meta.val_.rowkey_count_ = rowkey_cnt;
    macro_meta.val_.column_count_ = column_cnt;
    macro_meta.val_.compressor_type_ = data_store_desc_->compressor_type_;
    macro_meta.val_.row_store_type_ = data_store_desc_->row_store_type_;
    macro_meta.val_.logic_id_.logic_version_ = data_store_desc_->get_logical_version();
    macro_meta.val_.logic_id_.tablet_id_ = data_store_desc_->tablet_id_.id();
    macro_meta.val_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID;
    meta_row_.reuse();
    row_allocator_.reuse();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_meta.build_estimate_row(meta_row_, row_allocator_))) {
      STORAGE_LOG(WARN, "fail to build meta row", K(ret), K(macro_meta));
    } else {
      meta_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      meta_block_writer_->reuse();
      ObMicroBlockDesc meta_block_desc;
      if (OB_FAIL(meta_block_writer_->append_row(meta_row_))) {
        STORAGE_LOG(WARN, "fail to append meta row", K(ret), K_(meta_row));
      } else if (OB_FAIL(meta_block_writer_->build_micro_block_desc(meta_block_desc))) {
        STORAGE_LOG(WARN, "fail to build macro meta block", K(ret), K_(meta_row));
      } else if (FALSE_IT(meta_block_desc.last_rowkey_ = rowkey)) {
      } else if (OB_UNLIKELY(!meta_block_desc.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected meta block desc", K(ret), K(meta_block_desc));
      } else {
        estimate_meta_block_size = meta_block_desc.buf_size_ + meta_block_desc.header_->header_size_;
#ifdef OB_BUILD_TDE_SECURITY
        const int64_t encrypted_size = share::ObEncryptionUtil::encrypted_length(
                                 static_cast<share::ObCipherOpMode>(index_store_desc_->encrypt_id_),
                                 estimate_meta_block_size);
        estimate_meta_block_size = max(estimate_meta_block_size, encrypted_size);
#endif
      }
    }
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
  } else if (FALSE_IT(row_desc.is_data_block_ = true)) { // mark data block
  } else {
    row_desc.micro_block_count_ = 1;
    const int64_t cur_data_block_size = micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_;
    int64_t remain_size = macro_block.get_remain_size() - cur_data_block_size;
    if (remain_size <= 0) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(cal_macro_meta_block_size(
        micro_block_desc.last_rowkey_, estimate_meta_block_size))) {
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
      estimate_leaf_block_size_ = remain_size;
      estimate_meta_block_size_ = estimate_meta_block_size;
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::append_macro_block(const ObMacroBlockDesc &macro_desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_desc.is_valid_with_macro_meta())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro block desc", K(ret), K(macro_desc));
  } else {
    ObDataMacroBlockMeta *macro_meta = macro_desc.macro_meta_;

    if (OB_FAIL(add_macro_block_meta( *macro_meta, *macro_meta_list_, *sstable_allocator_))) {
      STORAGE_LOG(WARN, "failed to add macro block meta", K(ret), KPC(macro_meta));
    } else {
      ++data_blocks_cnt_;
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::add_macro_block_meta(
    const ObDataMacroBlockMeta &macro_meta,
    ObIArray<ObDataMacroBlockMeta *> &macro_meta_list,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta *dst_macro_meta = nullptr;
  if (OB_FAIL(macro_meta.deep_copy(dst_macro_meta, allocator))) {
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(macro_meta));
  } else if (OB_FAIL(macro_meta_list.push_back(dst_macro_meta))) {
    STORAGE_LOG(WARN, "fail to push back macro block merge info", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dst_macro_meta)) {
    dst_macro_meta->~ObDataMacroBlockMeta();
    allocator.free(static_cast<void*>(dst_macro_meta));
    dst_macro_meta = nullptr;
  }
  return ret;
}

int ObDataIndexBlockBuilder::write_meta_block(
    ObMacroBlock &macro_block,
    const MacroBlockId &block_id,
    const ObIndexBlockRowDesc &macro_row_desc)
{
  int ret = OB_SUCCESS;
  int64_t data_offset = 0;
  ObMicroBlockDesc meta_block_desc; // meta block
  macro_meta_.reset();
  meta_block_writer_->reuse();
  meta_row_.reuse();
  row_allocator_.reuse();

  if (OB_UNLIKELY(!macro_row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro row desc", K(ret), K(macro_row_desc));
  } else if (OB_FAIL(macro_block.get_macro_block_meta(macro_meta_))) {
    STORAGE_LOG(WARN, "fail to get macro block meta", K(ret), K_(macro_meta));
  } else if (OB_UNLIKELY(macro_meta_.val_.micro_block_count_
      != macro_row_desc.micro_block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "check micro block count failed", K(ret), K_(macro_meta), K(macro_row_desc));
  } else if (FALSE_IT(row_desc_to_meta(macro_row_desc, macro_meta_))) {
  } else if (OB_FAIL(macro_meta_.build_row(meta_row_, row_allocator_))) {
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
    if (OB_FAIL(ObDataIndexBlockBuilder::add_macro_block_meta(
        macro_meta_, *macro_meta_list_, *sstable_allocator_))) {
      STORAGE_LOG(WARN, "failed to add macro block meta", K(ret), K_(macro_meta));
    } else {
      // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
      share::ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "succeed to write macro meta in macro block", K(ret), K(macro_meta_));
    }
  }
  return ret;
}

int ObDataIndexBlockBuilder::append_index_micro_block(ObMacroBlock &macro_block,
                                                      const MacroBlockId &block_id)
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
  } else if (OB_FAIL(write_meta_block(macro_block, block_id, macro_row_desc_))) {
    STORAGE_LOG(WARN, "fail to build meta block", K(ret));
  } else {
    root_micro_block_desc_->last_macro_size_ = data_offset + leaf_block_size + meta_block_size_;
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

int ObDataIndexBlockBuilder::generate_macro_row(ObMacroBlock &macro_block,
                                                const MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "invalid index builder", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro block id", K(block_id));
  } else if (OB_UNLIKELY(!macro_block.is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid empty macro block", K(ret));
  } else if (OB_FAIL(append_index_micro_block(macro_block, block_id))) {
    STORAGE_LOG(WARN, "fail to append n-1 level micro block", K(ret));
  } else {
    ++data_blocks_cnt_;
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
  } else if (OB_UNLIKELY(row_count_ < 0)) {
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
  } else if (OB_FAIL(data_write_ctx->deep_copy(
      root_micro_block_desc_->data_write_ctx_, *sstable_allocator_))) {
  } else {
    const int64_t blocks_cnt = macro_meta_list_->count();
    ObDataMacroBlockMeta *last_meta = macro_meta_list_->at(blocks_cnt - 1);
    root_micro_block_desc_->last_key_ = last_meta->end_key_; // no need deep_copy
    root_micro_block_desc_->data_column_cnt_ = data_store_desc_->row_column_count_;
    // root_micro_block_desc_->macro_metas_ = macro_meta_list_; // should be done in init method
    root_micro_block_desc_->data_blocks_cnt_ = blocks_cnt;
    if (blocks_cnt == 1) {
      root_micro_block_desc_->meta_block_size_ = meta_block_size_;
      root_micro_block_desc_->meta_block_offset_ = meta_block_offset_;
    }
    STORAGE_LOG(INFO, "succeed to close data index builder", KPC_(root_micro_block_desc));
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
  } else if (FALSE_IT(update_accumulative_info(macro_row_desc))) {
  } else {
    macro_row_desc.is_macro_node_ = true;
    macro_row_desc.row_key_ = micro_block_desc.last_rowkey_;
    macro_row_desc.macro_block_count_ = 1;
  }
  return ret;
}

//===================== ObMetaIndexBlockBuilder ================
ObMetaIndexBlockBuilder::ObMetaIndexBlockBuilder()
  :micro_writer_(nullptr),
   macro_writer_(nullptr),
   last_leaf_rowkey_(),
   leaf_rowkey_allocator_("MetaIdxBuilder", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
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
    allocator_->free(micro_writer_);
    micro_writer_ = nullptr;
  }
  macro_writer_ = nullptr;
  last_leaf_rowkey_.reset();
  leaf_rowkey_allocator_.reset();
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
  } else if (OB_FAIL(ObBaseIndexBlockBuilder::init(data_store_desc, allocator, &macro_writer))) {
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
    const int64_t rowkey_cnt = leaf_row.get_column_count() - 1;
    ObDatumRowkey src_rowkey;
    if (OB_UNLIKELY(rowkey_cnt < 1
        || rowkey_cnt != index_store_desc_->rowkey_column_count_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected rowkey cnt ", K(ret), K(rowkey_cnt), KPC_(index_store_desc));
    } else if (OB_FAIL(src_rowkey.assign(leaf_row.storage_datums_, rowkey_cnt))) {
      STORAGE_LOG(WARN, "Failed to assign src rowkey", K(ret), K(rowkey_cnt), K(leaf_row));
    } else if (OB_FAIL(src_rowkey.deep_copy(last_leaf_rowkey_, leaf_rowkey_allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy rowkey", K(src_rowkey));
    }
  }
  return ret;
}

int ObMetaIndexBlockBuilder::close(
    const IndexMicroBlockDescList &roots,
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
  } else if (row_count_ <= 0 && micro_block_desc.get_block_size() <= ROOT_BLOCK_SIZE_LIMIT) {
    // meta block's size is smaller than ROOT_BLOCK_SIZE_LIMIT, all meta data will be stored in root
    if (OB_FAIL(ObBaseIndexBlockBuilder::close(*allocator_, tree_info))) {
      STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
    } else if (OB_FAIL(build_single_node_tree(*allocator_, micro_block_desc, block_desc))) {
      STORAGE_LOG(WARN, "fail to build single node tree of meta", K(ret));
    }
  } else if (row_count_ <= 0 && 1 == micro_block_desc.row_count_
      && ObSSTableIndexBuilder::check_version_for_small_sstable(*index_store_desc_)) {
    // this sstable only has one data block, but the size of meta data exceeds ROOT_BLOCK_SIZE_LIMIT,
    // so sstable's root points to the tail of its data block (macro meta row)
    if (OB_FAIL(build_single_macro_row_desc(roots))) {
      STORAGE_LOG(WARN, "fail to build single marcro row descn", K(ret));
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::close(*allocator_, tree_info))) {
      STORAGE_LOG(WARN, "fail to close index tree of meta", K(ret));
    } else {
      block_desc = tree_info.root_desc_;
    }
  } else {
    if (OB_FAIL(append_micro_block(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to append micro block of meta to macro block", K(ret));
    } else if (OB_FAIL(ObBaseIndexBlockBuilder::close(*allocator_, tree_info))) {
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

int ObMetaIndexBlockBuilder::build_single_macro_row_desc(const IndexMicroBlockDescList &roots)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc data_desc;
  if (OB_UNLIKELY(1 != roots.count() || 1 != roots[0]->macro_metas_->count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected roots", K(ret), K(roots.count()), K(roots[0]->macro_metas_->count()), KPC(roots[0]));
  } else if (0 >= roots[0]->meta_block_size_ || 0 >= roots[0]->meta_block_offset_) {
    const ObDataMacroBlockMeta &macro_meta = *(roots[0]->macro_metas_->at(0));
    ObMacroBlockHandle read_handle;
    ObSSTableMacroBlockHeader macro_header;
    if (OB_FAIL(ObSSTableIndexBuilder::load_single_macro_block(macro_meta, read_handle, macro_header))) {
      STORAGE_LOG(WARN, "fail to load macro block", K(ret), K(macro_meta), KPC(roots[0]));
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
    data_desc.row_store_type_ = macro_meta_val.row_store_type_;
    data_desc.compressor_type_ = macro_meta_val.compressor_type_;
    data_desc.master_key_id_ = macro_meta_val.master_key_id_;
    data_desc.encrypt_id_ = macro_meta_val.encrypt_id_;
    MEMCPY(data_desc.encrypt_key_, macro_meta_val.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    ObIndexBlockRowDesc row_desc(data_desc);
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
   mutex_(common::ObLatchIds::INDEX_BUILDER_LOCK),
   index_store_desc_(nullptr),
   block_write_ctx_(),
   root_micro_block_desc_(nullptr),
   macro_meta_list_(nullptr),
   sstable_allocator_(nullptr),
   sstable_builder_(nullptr),
   allocator_("IdxRebuilder", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObIndexBlockRebuilder::~ObIndexBlockRebuilder()
{
  reset();
}

void ObIndexBlockRebuilder::reset()
{
  index_store_desc_ = nullptr;
  block_write_ctx_.reset();
  root_micro_block_desc_ = nullptr;
  macro_meta_list_ = nullptr;
  sstable_allocator_ = nullptr;
  sstable_builder_ = nullptr;
  allocator_.reset();
  is_inited_ = false;
}

int ObIndexBlockRebuilder::init(ObSSTableIndexBuilder &sstable_builder)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 109;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIndexBlockRebuilder has been inited", K(ret));
  } else if (OB_FAIL(sstable_builder.init_builder_ptrs(sstable_builder_, index_store_desc_,
      sstable_allocator_, root_micro_block_desc_, macro_meta_list_))) {
    STORAGE_LOG(WARN, "fail to init referemce pointer members", K(ret));
  } else {
    is_inited_ = true;
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
  int64_t meta_block_offset = 0;
  int64_t meta_block_size = 0;
  if (OB_FAIL(inner_get_macro_meta(buf, size, macro_id, allocator,
      macro_meta, meta_block_offset, meta_block_size))) {
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
    int64_t &meta_block_offset,
    int64_t &meta_block_size)
{
  int ret = OB_SUCCESS;
  ObSSTableMacroBlockHeader macro_header;
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
  if (OB_SUCC(ret)) {
    meta_block_offset = macro_header.fixed_header_.meta_block_offset_;
    meta_block_size = macro_header.fixed_header_.meta_block_size_;
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
  int64_t meta_block_offset = 0;
  int64_t meta_block_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuilder not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(inner_get_macro_meta(buf, size, macro_id, allocator, macro_meta,
      root_micro_block_desc_->meta_block_offset_, root_micro_block_desc_->meta_block_size_))) {
    STORAGE_LOG(WARN, "fail to get macro meta", K(ret),K(macro_id));
  } else if (OB_ISNULL(macro_meta)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null macro meta", K(ret),K(macro_id));
  } else if (OB_FAIL(append_macro_row(*macro_meta))) {
    STORAGE_LOG(WARN, "fail to append macro meta", K(ret),K(macro_id), KPC(macro_meta));
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
    if (OB_FAIL(ObDataIndexBlockBuilder::add_macro_block_meta(
        macro_meta, *macro_meta_list_, *sstable_allocator_))) {
      STORAGE_LOG(WARN, "failed to add macro block meta", K(ret), K(macro_meta));
    } else if (OB_FAIL(block_write_ctx_.add_macro_block_id(macro_meta.val_.macro_id_))) { // inc_ref
      STORAGE_LOG(WARN, "failed to add block id", K(ret), K(macro_meta));
    }
  }
  return ret;
}

int ObIndexBlockRebuilder::close()
{
  int ret = OB_SUCCESS;
  ObDataMacroMetaCompare cmp(ret, index_store_desc_->datum_utils_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "rebuilder not inited", K(ret), K_(is_inited));
  } else if (FALSE_IT(std::sort(macro_meta_list_->begin(), macro_meta_list_->end(), cmp))) {
  } else if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "fail to sort meta list", K(ret));
  } else if (macro_meta_list_->count() == 0) {
    // do not append root to sstable builder since it's empty
  } else if (OB_FAIL(block_write_ctx_.deep_copy(
      root_micro_block_desc_->data_write_ctx_, *sstable_allocator_))) {
    STORAGE_LOG(WARN, "Fail to copy data write ctx", K(ret));
  } else {
    const int64_t blocks_cnt = macro_meta_list_->count();
    ObDataMacroBlockMeta *last_meta = macro_meta_list_->at(blocks_cnt - 1);
    root_micro_block_desc_->last_key_ = last_meta->end_key_; // no need deep_copy
    root_micro_block_desc_->data_column_cnt_ = last_meta->val_.column_count_;
    // root_micro_block_desc_->macro_metas_ = macro_meta_list_; // should be done in init method
    root_micro_block_desc_->data_blocks_cnt_ = blocks_cnt;
    STORAGE_LOG(INFO, "succeed to close rebuilder", KPC_(root_micro_block_desc));
  }
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase
