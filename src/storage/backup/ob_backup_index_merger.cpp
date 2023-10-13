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

#include "storage/backup/ob_backup_index_merger.h"
#include "lib/oblog/ob_log_module.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_operator.h"

#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

/* ObIBackupMacroBlockIndexFuser */

ObIBackupMacroBlockIndexFuser::ObIBackupMacroBlockIndexFuser() : iter_array_(), result_()
{}

ObIBackupMacroBlockIndexFuser::~ObIBackupMacroBlockIndexFuser()
{}

int ObIBackupMacroBlockIndexFuser::get_result(ObBackupMacroRangeIndex &result)
{
  int ret = OB_SUCCESS;
  result = result_;
  return ret;
}

/* ObBackupMacroIndexMinorFuser */

ObBackupMacroIndexMinorFuser::ObBackupMacroIndexMinorFuser() : ObIBackupMacroBlockIndexFuser()
{}

ObBackupMacroIndexMinorFuser::~ObBackupMacroIndexMinorFuser()
{}

int ObBackupMacroIndexMinorFuser::fuse(MERGE_ITER_ARRAY &iter_array)
{
  int ret = OB_SUCCESS;
  ObBackupMacroRangeIndex output;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < iter_array.count(); ++i) {
    ObIMacroBlockIndexIterator *iter = iter_array.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter should not be null", K(ret));
    } else if (OB_UNLIKELY(iter->is_iter_end())) {
      continue;
    } else if (OB_FAIL(iter->get_cur_index(output))) {
      LOG_WARN("failed to get cur index", K(ret));
    } else {
      found = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (found) {
      result_ = output;
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

/* ObBackupMacroIndexMajorFuser */

ObBackupMacroIndexMajorFuser::ObBackupMacroIndexMajorFuser() : ObIBackupMacroBlockIndexFuser()
{}

ObBackupMacroIndexMajorFuser::~ObBackupMacroIndexMajorFuser()
{}

int ObBackupMacroIndexMajorFuser::fuse(MERGE_ITER_ARRAY &iter_array)
{
  int ret = OB_SUCCESS;
  if (iter_array.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter array count unexpected", K(ret), K(iter_array));
  } else if (OB_FAIL(iter_array.at(0)->get_cur_index(result_))) {
    LOG_WARN("failed to get cur index", K(ret), K(iter_array));
  }
  return ret;
}

/* ObBackupMacroIndexMajorFuser */

ObBackupMetaIndexFuser::ObBackupMetaIndexFuser()
  : iter_array_(), result_()
{}

ObBackupMetaIndexFuser::~ObBackupMetaIndexFuser()
{}

int ObBackupMetaIndexFuser::fuse(const MERGE_ITER_ARRAY &iter_array)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex output;
  int64_t largest_retry = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < iter_array.count(); ++i) {
    ObBackupMetaIndexIterator *iter = iter_array.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter should not be null", K(ret));
    } else if (OB_UNLIKELY(iter->is_iter_end())) {
      continue;
    } else if (OB_FAIL(iter->get_cur_index(output))) {
      LOG_WARN("failed to get cur index", K(ret));
    } else if (output.retry_id_ > largest_retry) {
      result_ = output;
      largest_retry = output.retry_id_;
    }
  }
  if (OB_SUCC(ret)) {
    if (-1 == largest_retry) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObBackupMetaIndexFuser::get_result(ObBackupMetaIndex &result)
{
  int ret = OB_SUCCESS;
  if (!result_.is_valid()) {
    ret = OB_ITER_END;
    LOG_WARN("result is not valid", K(ret), K_(result));
  } else {
    result = result_;
  }
  return ret;
}

/* ObIBackupMultiLevelIndexBuilder */

ObIBackupMultiLevelIndexBuilder::ObIBackupMultiLevelIndexBuilder()
    : is_inited_(false),
      cur_block_offset_(0),
      cur_block_length_(0),
      leaf_(NULL),
      dummy_(NULL),
      root_(NULL),
      write_ctx_(NULL),
      allocator_(),
      buffer_writer_("BackupIndMerger")
{}

ObIBackupMultiLevelIndexBuilder::~ObIBackupMultiLevelIndexBuilder()
{
  reset();
}

void ObIBackupMultiLevelIndexBuilder::reset()
{
  // iteration should start at dummy
  ObBackupIndexBufferNode *cur_node = dummy_;
  ObBackupIndexBufferNode *next_node = NULL;
  while (OB_NOT_NULL(cur_node)) {
    next_node = cur_node->get_next();
    cur_node->~ObBackupIndexBufferNode();
    cur_node = next_node;
  }
}

int ObIBackupMultiLevelIndexBuilder::init(
    const int64_t start_offset, ObBackupIndexBufferNode &node, ObBackupFileWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index builder init twice", K(ret));
  } else if (start_offset <= 0 || !node.is_inited() || !write_ctx.is_opened()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node or file writer is not valid", K(ret), K(start_offset), K(node), "opened", write_ctx.is_opened());
  } else {
    cur_block_offset_ = start_offset;
    cur_block_length_ = 0;
    leaf_ = &node;
    dummy_ = NULL;
    root_ = NULL;
    write_ctx_ = &write_ctx;
    is_inited_ = true;
  }
  return ret;
}

template <class IndexIndexType>
int ObIBackupMultiLevelIndexBuilder::build_index()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("index builder do not init", K(ret));
  } else if (OB_FAIL(build_and_flush_index_tree_<IndexIndexType>())) {
    LOG_WARN("failed to build and flush index tree", K(ret));
  }
  return ret;
}

template <class IndexIndex>
int ObIBackupMultiLevelIndexBuilder::build_and_flush_index_tree_()
{
  int ret = OB_SUCCESS;
  ObBackupIndexBufferNode *cur_node = leaf_;
  ObBackupIndexBufferNode *next_node = NULL;
  while (OB_SUCC(ret) && OB_NOT_NULL(cur_node)) {
    next_node = NULL;
    bool need_build_next_level = false;
    if (OB_FAIL(alloc_new_buffer_node_(
                   cur_node->get_tenant_id(), cur_node->get_block_type(), cur_node->get_node_level() + 1, next_node))) {
      LOG_WARN("failed to alloc new buffer node", K(ret), K(*cur_node));
    } else if (OB_FAIL(build_next_level_index_(*cur_node, *next_node))) {
      LOG_WARN("failed to build next level index", K(ret), K(*cur_node));
    } else if (OB_FAIL(check_need_build_next_level_(next_node, need_build_next_level))) {
      LOG_WARN("failed to check need build next level", K(ret));
    } else if (!need_build_next_level) {
      next_node->~ObBackupIndexBufferNode();
      next_node = NULL;
      break;
    } else {
      cur_block_offset_ += cur_block_length_;
      cur_node->set_next(next_node);
      cur_node = next_node;
      if (OB_ISNULL(dummy_)) {
        dummy_ = next_node;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(write_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write ctx should not be null", K(ret));
    } else if (OB_FAIL(flush_trailer_())) {
      LOG_WARN("failed to flush tail", K(ret));
    } else if (OB_FAIL(write_ctx_->close())) {
      LOG_WARN("failed to close write ctx", K(ret));
    }
  }
  return ret;
}

int ObIBackupMultiLevelIndexBuilder::alloc_new_buffer_node_(const uint64_t tenant_id,
    const ObBackupBlockType &block_type, const int64_t node_level, ObBackupIndexBufferNode *&new_node)
{
  int ret = OB_SUCCESS;
  new_node = NULL;
  void *buf = NULL;
  ObBackupIndexBufferNode *tmp_node = NULL;
  if (OB_INVALID_ID == tenant_id || node_level <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(node_level));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBackupIndexBufferNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_ISNULL(tmp_node = new (buf) ObBackupIndexBufferNode)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc iterator", K(ret));
  } else if (OB_FAIL(tmp_node->init(tenant_id, block_type, node_level))) {
    LOG_WARN("failed to init index buffer node", K(ret), K(tenant_id), K(block_type), K(node_level));
  } else {
    new_node = tmp_node;
  }
  return ret;
}

int ObIBackupMultiLevelIndexBuilder::check_need_build_next_level_(ObBackupIndexBufferNode *node, bool &need_build) const
{
  int ret = OB_SUCCESS;
  need_build = true;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid args", K(ret), K(node));
  } else if (OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL + 1 == node->get_node_level()) {
    need_build = true;
  } else {
    const int64_t write_count = node->get_write_count();
    if (write_count <= 1) {
      need_build = false;
    } else {
      need_build = true;
    }
    LOG_INFO("check need build next level", K(need_build), KPC(node), K(write_count));
  }
  return ret;
}

template <class IndexIndexType>
int ObIBackupMultiLevelIndexBuilder::build_next_level_index_impl_(
    ObBackupIndexBufferNode &cur_node, ObBackupIndexBufferNode &next_node)
{
  int ret = OB_SUCCESS;
  buffer_writer_.reuse();
  ObSelfBufferWriter &buffer_writer = buffer_writer_;
  ObArray<IndexIndexType> tmp_index_list;
  IndexIndexType index;
  IndexIndexType index_index;
  const int64_t common_header_len = sizeof(ObBackupCommonHeader);
  if (OB_FAIL(buffer_writer.ensure_space(common_header_len))) {
    LOG_WARN("failed to ensure space", K(ret), K(common_header_len));
  }
  while (OB_SUCC(ret) && cur_node.get_read_count() < cur_node.get_write_count()) {
    if (OB_FAIL(cur_node.get_backup_index(index))) {
      LOG_WARN("failed to get backup index", K(ret), K(cur_node));
    } else if (OB_FAIL(tmp_index_list.push_back(index))) {
      LOG_WARN("failed to push back", K(ret), K(cur_node), K(index));
    } else if (FALSE_IT(index_index.end_key_ = index.end_key_)) {
      // assign end key to index index
    } else if (OB_FAIL(write_single_index_(cur_node.get_block_type(), cur_node.get_node_level(), index))) {
      LOG_WARN("failed to write single index", K(ret), K(cur_node), K(index));
    } else if (tmp_index_list.count() < OB_BACKUP_INDEX_BLOCK_NODE_CAPACITY) {
      // no need close index block if not reach capacity
    } else if (OB_FAIL(close_index_block_(cur_node.get_block_type()))) {
      LOG_WARN("failed to close index block", K(ret), K(cur_node));
    } else {
      index_index.offset_ = cur_block_offset_;
      index_index.length_ = cur_block_length_;
      if (OB_FAIL(next_node.put_backup_index(index_index))) {
        LOG_WARN("failed to add backup index", K(ret), K(index_index));
      } else {
        LOG_INFO("put backup index", K(cur_node), K(next_node), K(index_index));
      }
      if (OB_SUCC(ret)) {
        if (cur_node.get_read_count() != cur_node.get_write_count()) {
          cur_block_offset_ += cur_block_length_;
        }
        tmp_index_list.reset();
        buffer_writer.reuse();
      }
    }
  }
  if (OB_SUCC(ret) && tmp_index_list.count() > 0) {
    if (OB_FAIL(write_index_list_(cur_node.get_block_type(), cur_node.get_node_level(), tmp_index_list))) {
      LOG_WARN("failed to write index list", K(ret), K(cur_node));
    } else {
      index_index.offset_ = cur_block_offset_;
      index_index.length_ = cur_block_length_;
      if (OB_FAIL(next_node.put_backup_index(index_index))) {
        LOG_WARN("failed to put backup index", K(ret), K(index_index), K(tmp_index_list), K(next_node), K(cur_node));
      } else {
        LOG_INFO("put backup index", K(cur_node), K(next_node), K(index_index));
      }
    } 
  }
  return ret;
}

template <class IndexType>
int ObIBackupMultiLevelIndexBuilder::write_index_list_(
    const ObBackupBlockType &block_type, const int64_t node_level, const common::ObIArray<IndexType> &index_list)
{
  int ret = OB_SUCCESS;
  buffer_writer_.reuse();
  ObSelfBufferWriter &buffer_writer = buffer_writer_;
  ObBackupMultiLevelIndexHeader multi_level_header;
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  ObBackupCommonHeader *common_header = NULL;
  if (index_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index list should not be empty", K(ret));
  } else if (OB_FAIL(buffer_writer.ensure_space(header_len))) {
    LOG_WARN("failed to ensure space", K(ret), K(header_len));
  } else if (OB_FAIL(buffer_writer.advance_zero(header_len))) {
    LOG_WARN("advance failed", K(ret), K(header_len));
  } else if (OB_FAIL(build_multi_level_index_header(block_type, node_level, multi_level_header))) {
    LOG_WARN("failed to build multi level index header", K(ret), K(block_type), K(node_level));
  } else if (OB_FAIL(buffer_writer.write_serialize(multi_level_header))) {
    LOG_WARN("failed to write serialize multi level header", K(ret), K(multi_level_header));
  } else if (OB_FAIL(encode_index_to_buffer_<IndexType>(index_list, buffer_writer))) {
    LOG_WARN("failed to encode index to buffer", K(ret), K(index_list));
  } else if (FALSE_IT(common_header = reinterpret_cast<ObBackupCommonHeader *>(buffer_writer.data()))) {
  } else if (OB_FAIL(build_common_header(block_type,
                  buffer_writer.pos() - sizeof(ObBackupCommonHeader),
                  0 /*align_length*/,
                  common_header))) {
    LOG_WARN("failed to build common header", K(ret), K(buffer_writer));
  } else if (OB_FAIL(common_header->set_checksum(buffer_writer.data() + common_header->header_length_,
                  buffer_writer.length() - common_header->header_length_))) {
    LOG_WARN("failed to set checksum", K(ret), K(buffer_writer), K(*common_header));
  } else if (OB_FAIL(write_ctx_->append_buffer(
                  ObBufferReader(buffer_writer.data(), buffer_writer.pos(), buffer_writer.pos())))) {
    LOG_WARN("failed to write buffer", K(ret), K(buffer_writer));
  } else {
    cur_block_length_ = buffer_writer.pos();
  }
  return ret;
}

template <class IndexType>
int ObIBackupMultiLevelIndexBuilder::write_single_index_(const ObBackupBlockType &block_type, const int64_t node_level, const IndexType &index)
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter &buffer_writer = buffer_writer_;
  ObBackupMultiLevelIndexHeader multi_level_header;
  const bool need_advance_zero = 0 == buffer_writer.pos();
  const int64_t common_header_len = sizeof(ObBackupCommonHeader);
  if (OB_FAIL(build_multi_level_index_header(block_type, node_level, multi_level_header))) {
    LOG_WARN("failed to build multi level index header", K(ret), K(block_type), K(node_level));
  } else if (need_advance_zero && OB_FAIL(buffer_writer.advance_zero(common_header_len))) {
    LOG_WARN("failed to advance zero", K(ret), K(common_header_len));
  } else if (need_advance_zero && OB_FAIL(buffer_writer.write_serialize(multi_level_header))) {
    LOG_WARN("failed to write serialize multi level header", K(ret), K(multi_level_header));
  } else if (OB_FAIL(buffer_writer.write_serialize(index))) {
    LOG_WARN("failed to write serialize", K(ret), K(index));
  } else {
    LOG_DEBUG("write index", K(need_advance_zero), K(index), K(multi_level_header));
  }
  return ret;
}

int ObIBackupMultiLevelIndexBuilder::close_index_block_(const ObBackupBlockType &block_type)
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter &buffer_writer = buffer_writer_;
  ObBackupCommonHeader *common_header = NULL;
  char *header_buf = buffer_writer.data();
  common_header = reinterpret_cast<ObBackupCommonHeader *>(header_buf);
  if (OB_FAIL(build_common_header(block_type, buffer_writer.pos() - sizeof(ObBackupCommonHeader),
        0 /*align_length*/, common_header))) {
    LOG_WARN("failed to build common header", K(ret), K(block_type));
  } else if (OB_FAIL(common_header->set_checksum(buffer_writer.data() + common_header->header_length_,
        buffer_writer.length() - common_header->header_length_))) {
    LOG_WARN("failed to set checksum", K(ret));
  } else if (OB_FAIL(write_ctx_->append_buffer(ObBufferReader(buffer_writer.data(), buffer_writer.pos(), buffer_writer.pos())))) {
    LOG_WARN("failed to write buffer", K(ret), K(buffer_writer));
  } else {
    cur_block_length_ = buffer_writer.pos();
  }
  return ret;
}

template <class IndexType>
int ObIBackupMultiLevelIndexBuilder::encode_index_to_buffer_(
    const common::ObIArray<IndexType> &index_list, ObBufferWriter &buffer_writer)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const IndexType &index = index_list.at(i);
    if (OB_FAIL(buffer_writer.write_serialize(index))) {
      LOG_WARN("failed to write serialize data", K(ret), K(index));
    }
  }
  return ret;
}

int ObIBackupMultiLevelIndexBuilder::get_index_tree_height_(int64_t &height) const
{
  int ret = OB_SUCCESS;
  height = 0;
  if (OB_ISNULL(leaf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leaf node should not be null", K(ret), K_(leaf));
  } else {
    ObBackupIndexBufferNode *cur_node = leaf_;
    while (OB_NOT_NULL(cur_node)) {
      ++height;
      cur_node = cur_node->get_next();
    }
  }
  return ret;
}

int ObIBackupMultiLevelIndexBuilder::flush_trailer_()
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter buffer_writer("BackupInd");
  ObBackupMultiLevelIndexTrailer *trailer = NULL;
  const int64_t trailer_len = sizeof(ObBackupMultiLevelIndexTrailer);
  int64_t tree_height = 0;
  if (OB_ISNULL(write_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write ctx should not be null", K(ret));
  } else if (OB_FAIL(buffer_writer.ensure_space(trailer_len))) {
    LOG_WARN("failed to ensure space", K(ret), K(trailer_len));
  } else if (OB_FAIL(buffer_writer.advance_zero(trailer_len))) {
    LOG_WARN("failed to advance zero", K(ret), K(trailer_len));
  } else if (OB_FAIL(get_index_tree_height_(tree_height))) {
    LOG_WARN("failed to get index tree height", K(ret));
  } else {
    char *header_buf = buffer_writer.data();
    trailer = reinterpret_cast<ObBackupMultiLevelIndexTrailer *>(header_buf);
    trailer->file_type_ = 0;
    trailer->tree_height_ = tree_height;
    trailer->last_block_offset_ = cur_block_offset_;
    trailer->last_block_length_ = cur_block_length_;
    trailer->set_trailer_checksum();
    ObBufferReader buffer_reader(header_buf, trailer_len, trailer_len);
    if (OB_FAIL(write_ctx_->append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
    } else {
      LOG_INFO("multi level index builder flush trailer", KPC(trailer));
    }
  }
  return ret;
}

/* ObBackupMultiLevelMacroIndexBuilder */

int ObBackupMultiLevelMacroIndexBuilder::build_next_level_index_(
    ObBackupIndexBufferNode &cur_node, ObBackupIndexBufferNode &next_node)
{
  return build_next_level_index_impl_<ObBackupMacroRangeIndexIndex>(cur_node, next_node);
}

/* ObBackupMultiLevelMetaIndexBuilder */

int ObBackupMultiLevelMetaIndexBuilder::build_next_level_index_(
    ObBackupIndexBufferNode &cur_node, ObBackupIndexBufferNode &next_node)
{
  return build_next_level_index_impl_<ObBackupMetaIndexIndex>(cur_node, next_node);
}

/* ObIBackupIndexMerger */

ObIBackupIndexMerger::ObIBackupIndexMerger()
    : is_inited_(false),
      merge_param_(),
      offset_(),
      buffer_writer_("BackupIndMerger"),
      dev_handle_(NULL),
      io_fd_(),
      write_ctx_(),
      buffer_node_(),
      sql_proxy_(NULL)
{}

ObIBackupIndexMerger::~ObIBackupIndexMerger()
{}

int ObIBackupIndexMerger::get_all_retries_(const int64_t task_id, const uint64_t tenant_id,
    const share::ObBackupDataType &backup_data_type, const share::ObLSID &ls_id, common::ObISQLClient &sql_client,
    common::ObIArray<ObBackupRetryDesc> &retry_list)
{
  int ret = OB_SUCCESS;
  retry_list.reset();
  if (OB_FAIL(
          ObLSBackupOperator::get_all_retries(task_id, tenant_id, backup_data_type, ls_id, retry_list, sql_client))) {
    LOG_WARN("failed to get all retries", K(ret), K(task_id), K(tenant_id), K(backup_data_type), K(ls_id));
  } else {
    LOG_INFO("get all retries", K(tenant_id), K(backup_data_type), K(ls_id), K(retry_list));
  }
  return ret;
}

int ObIBackupIndexMerger::open_file_writer_(const share::ObBackupPath &path, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  common::ObBackupIoAdapter util;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_RANDOMWRITER;
  if (OB_FAIL(util.mk_parent_dir(path.get_obstr(), storage_info))) {
    LOG_WARN("failed to make parent dir", K(path), K(path), KP(storage_info));
  } else if (OB_FAIL(util.open_with_access_type(dev_handle_, io_fd_, storage_info, path.get_obstr(), access_type))) {
    LOG_WARN("failed to open with access type", K(ret), K(path), KP(storage_info));
  } else {
    LOG_INFO("backup index merger open file writer", K(path), KP(storage_info));
  }
  return ret;
}

int ObIBackupIndexMerger::prepare_file_write_ctx_(ObBackupFileWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_ctx.open(OB_MAX_BACKUP_FILE_SIZE, io_fd_, *dev_handle_))) {
    LOG_WARN("failed to open backup file write ctx", K(ret));
  }
  return ret;
}

template <class IndexType>
int ObIBackupIndexMerger::encode_index_to_buffer_(
    const common::ObIArray<IndexType> &index_list, ObBufferWriter &buffer_writer)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const IndexType &index = index_list.at(i);
    if (OB_FAIL(buffer_writer.write_serialize(index))) {
      LOG_WARN("failed to write serialize data", K(ret), K(index));
    }
  }
  return ret;
}

template <class IndexType, class IndexIndexType>
int ObIBackupIndexMerger::write_index_list_(
    const ObBackupBlockType &block_type, const common::ObIArray<IndexType> &index_list)
{
  int ret = OB_SUCCESS;
  buffer_writer_.reuse();
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  ObBackupCommonHeader *common_header = NULL;
  ObBackupMultiLevelIndexHeader multi_level_header;
  const int64_t index_level = 0;
  if (index_list.empty()) {
    // do nothing
  } else if (OB_FAIL(buffer_writer_.ensure_space(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_writer_.advance_zero(header_len))) {
    LOG_WARN("advance failed", K(ret), K(header_len));
  } else if (OB_FAIL(build_multi_level_index_header(block_type, index_level, multi_level_header))) {
    LOG_WARN("failed to build multi level index header", K(ret), K(block_type), K(index_level));
  } else if (OB_FAIL(buffer_writer_.write_serialize(multi_level_header))) {
    LOG_WARN("failed to write serialize multi level header", K(ret), K(multi_level_header));
  } else if (OB_FAIL(encode_index_to_buffer_<IndexType>(index_list, buffer_writer_))) {
    LOG_WARN("failed to encode index to buffer", K(ret), K(index_list));
  } else if (FALSE_IT(common_header = reinterpret_cast<ObBackupCommonHeader *>(buffer_writer_.data()))) {
  } else if (OB_FAIL(build_common_header(
                 block_type, buffer_writer_.pos() - sizeof(ObBackupCommonHeader), 0 /*align_length*/, common_header))) {
    LOG_WARN("failed to build common header", K(ret), K(buffer_writer_));
  } else if (OB_FAIL(common_header->set_checksum(buffer_writer_.data() + common_header->header_length_,
                 buffer_writer_.length() - common_header->header_length_))) {
    LOG_WARN("failed to set checksum", K(ret), K(buffer_writer_), K(*common_header));
  } else if (OB_FAIL(write_ctx_.append_buffer(
                 ObBufferReader(buffer_writer_.data(), buffer_writer_.pos(), buffer_writer_.pos())))) {
    LOG_WARN("failed to write buffer", K(ret), K(buffer_writer_));
  } else {
    IndexIndexType index_index;
    index_index.end_key_ = index_list.at(index_list.count() - 1);
    index_index.offset_ = offset_;
    index_index.length_ = buffer_writer_.length();
    if (OB_FAIL(buffer_node_.put_backup_index(index_index))) {
      LOG_WARN("failed to add backup index", K(ret), K(index_index));
    } else {
      offset_ += buffer_writer_.length();
      buffer_writer_.reuse();
    }
  }
  return ret;
}

int ObIBackupIndexMerger::write_backup_file_header_(const ObBackupFileType &file_type)
{
  int ret = OB_SUCCESS;
  ObBackupFileHeader file_header;
  if (OB_FAIL(build_backup_file_header_(file_type, file_header))) {
    LOG_WARN("failed to build backup file header", K(ret), K(file_type));
  } else if (OB_FAIL(write_backup_file_header_(file_header))) {
    LOG_WARN("failed to write backup file header", K(ret), K(file_header));
  }
  return ret;
}

int ObIBackupIndexMerger::build_backup_file_header_(const ObBackupFileType &file_type, ObBackupFileHeader &file_header)
{
  int ret = OB_SUCCESS;
  ObBackupFileMagic magic;
  if (OB_FAIL(convert_backup_file_type_to_magic(file_type, magic))) {
    LOG_WARN("failed to convert type to magic", K(ret), K(file_type));
  } else {
    file_header.magic_ = magic;
    file_header.version_ = BACKUP_DATA_VERSION_V1;
    file_header.file_type_ = file_type;
    file_header.reserved_ = 0;
  }
  return ret;
}

int ObIBackupIndexMerger::write_backup_file_header_(const ObBackupFileHeader &file_header)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = DIO_READ_ALIGN_SIZE;
  char header_buf[buf_len] = "";
  ObBufferReader buffer_reader;
  if (OB_FAIL(file_header.check_valid())) {
    LOG_WARN("failed to check file header", K(ret));
  } else if (OB_FAIL(build_backup_file_header_buffer(file_header, buf_len, header_buf, buffer_reader))) {
    LOG_WARN("failed to build backup file header buffer", K(ret), K(file_header));
  } else if (OB_FAIL(write_ctx_.append_buffer(buffer_reader))) {
    LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
  } else {
    offset_ += buf_len;
    LOG_INFO("write backup file header", K(file_header));
  }
  return ret;
}

/* ObBackupMacroBlockIndexMerger */

ObBackupMacroBlockIndexMerger::ObBackupMacroBlockIndexMerger()
    : ObIBackupIndexMerger(), comparator_(), merge_iter_array_(), tmp_index_list_()
{}

ObBackupMacroBlockIndexMerger::~ObBackupMacroBlockIndexMerger()
{
  reset();
}

int ObBackupMacroBlockIndexMerger::init(const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  const ObBackupBlockType block_type = BACKUP_BLOCK_MACRO_DATA;
  const int64_t node_level = OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL + 1;
  const ObBackupFileType file_type = BACKUP_MACRO_RANGE_INDEX_FILE;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index merger init twice", K(ret));
  } else if (!merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(merge_param));
  } else if (OB_FAIL(buffer_writer_.ensure_space(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_node_.init(merge_param.tenant_id_, block_type, node_level))) {
    LOG_WARN("failed to init buffer node", K(ret), K(merge_param), K(block_type), K(node_level));
  } else if (OB_FAIL(prepare_merge_ctx_(merge_param, sql_proxy))) {
    LOG_WARN("failed to prepare merge ctx", K(ret), K(merge_param));
  } else if (OB_FAIL(write_backup_file_header_(file_type))) {
    LOG_WARN("failed to write backup file header", K(ret));
  } else if (OB_FAIL(merge_param_.assign(merge_param))) {
    LOG_WARN("failed to assign param", K(ret), K(merge_param));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupMacroBlockIndexMerger::reset()
{
  for (int64_t i = 0; i < merge_iter_array_.count(); ++i) {
    ObIMacroBlockIndexIterator *&iter = merge_iter_array_.at(i);
    if (OB_NOT_NULL(iter)) {
      ObLSBackupFactory::free(iter);
    }
  }
  merge_iter_array_.reset();
}

int ObBackupMacroBlockIndexMerger::merge_index()
{
  int ret = OB_SUCCESS;
  ObIBackupMacroBlockIndexFuser *fuser = NULL;
  MERGE_ITER_ARRAY unfinished_iters;
  MERGE_ITER_ARRAY min_iters;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("index merger do not init", K(ret));
  } else if (OB_FAIL(prepare_merge_fuser_(fuser))) {
    LOG_WARN("failed to prepare merge fuser", K(ret));
  } else {
    ObBackupMacroRangeIndex range_index;
    int64_t count = 0;
    while (OB_SUCC(ret)) {
      unfinished_iters.reset();
      min_iters.reset();
      range_index.reset();
      if (OB_FAIL(get_unfinished_iters_(merge_iter_array_, unfinished_iters))) {
        LOG_WARN("failed to get unfinished iters", K(ret), K(unfinished_iters));
      } else if (unfinished_iters.empty()) {
        LOG_INFO("merge index finish", K(count), K(merge_iter_array_));
        break;
      } else if (OB_FAIL(find_minimum_iters_(unfinished_iters, min_iters))) {
        LOG_WARN("failed to find minumum iters", K(ret), K(unfinished_iters));
      } else if (min_iters.empty()) {
        LOG_INFO("merge index finish");
        break;
      } else if (OB_FAIL(fuse_iters_(min_iters, fuser))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("iterator end", K(min_iters));
          break;
        } else {
          LOG_WARN("failed to fuse iters", K(ret), K(min_iters));
        }
      } else if (OB_FAIL(fuser->get_result(range_index))) {
        LOG_WARN("failed to get fuse result", K(ret), K(min_iters));
      } else if (OB_FAIL(process_result_(range_index))) {
        LOG_WARN("failed to process result", K(ret), K(min_iters));
      } else if (OB_FAIL(move_iters_next_(min_iters))) {
        LOG_WARN("failed to move iters next", K(ret), K(min_iters));
      } else {
        LOG_INFO("macro index merge round", K(count), K(range_index));
        count++;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(write_macro_index_list_())) {
        LOG_WARN("failed to write index list", K(ret));
      } else if (OB_FAIL(flush_index_tree_())) {
        LOG_WARN("failed to flush index tree", K(ret));
      }
    }
  }
  if (OB_NOT_NULL(fuser)) {
    ObLSBackupFactory::free(fuser);
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::prepare_merge_ctx_(
    const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupRetryDesc> retry_list;
  MERGE_ITER_ARRAY merge_iters;
  ObBackupPath backup_path;
  if (OB_FAIL(get_all_retries_(merge_param.task_id_,
          merge_param.tenant_id_,
          merge_param.backup_data_type_,
          merge_param.ls_id_,
          sql_proxy,
          retry_list))) {
    LOG_WARN("failed to get all retries", K(ret), K(merge_param));
  } else if (OB_FAIL(prepare_merge_iters_(merge_param, retry_list, sql_proxy, merge_iters))) {
    LOG_WARN("failed to prepare merge iters", K(ret), K(retry_list));
  } else if (OB_FAIL(merge_iter_array_.assign(merge_iters))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(get_output_file_path_(merge_param, backup_path))) {
    LOG_WARN("failed to get output file path", K(ret), K(merge_param));
  } else if (OB_FAIL(open_file_writer_(backup_path, merge_param.backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to prepare file writer", K(ret), K(backup_path), K(merge_param));
  } else if (OB_FAIL(prepare_file_write_ctx_(write_ctx_))) {
    LOG_WARN("failed to prepare file write ctx", K(ret));
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::prepare_merge_iters_(const ObBackupIndexMergeParam &merge_param,
    const common::ObIArray<ObBackupRetryDesc> &retry_list, common::ObISQLClient &sql_proxy,
    MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  merge_iters.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < retry_list.count(); ++i) {
    ObIMacroBlockIndexIterator *iter = NULL;
    const ObBackupRetryDesc &retry_desc = retry_list.at(i);
    const bool tenant_level = 0 == merge_param_.ls_id_.id();
    if (OB_FAIL(alloc_merge_iter_(tenant_level, merge_param, retry_desc, iter))) {
      LOG_WARN("failed to alloc merge iter", K(ret), K(merge_param), K(retry_desc));
    } else if (OB_FAIL(merge_iters.push_back(iter))) {
      LOG_WARN("failed to push back", K(ret), K(iter));
      ObLSBackupFactory::free(iter);
    } else {
      FLOG_INFO("prepare macro block merge iter", K(retry_desc));
    }
  }
  if (merge_param.backup_set_desc_.backup_type_.is_inc_backup() && merge_param.backup_data_type_.is_major_backup() &&
      0 == merge_param.ls_id_.id()) {
    ObIMacroBlockIndexIterator *iter = NULL;
    if (OB_FAIL(prepare_prev_backup_set_index_iter_(merge_param, sql_proxy, iter))) {
      LOG_WARN("failed to prepare prev backup set index iter", K(ret), K(merge_param));
    } else if (OB_FAIL(merge_iters.push_back(iter))) {
      LOG_WARN("failed to push back", K(ret), K(iter));
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::prepare_prev_backup_set_index_iter_(
    const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy, ObIMacroBlockIndexIterator *&iter)
{
  int ret = OB_SUCCESS;
  share::ObBackupSetFileDesc prev_backup_set_info;
  share::ObBackupSetDesc prev_backup_set_desc;
  ObBackupMacroRangeIndexIterator *tmp_iter = NULL;
  const ObBackupIndexIteratorType type = BACKUP_MACRO_RANGE_INDEX_ITERATOR;
  int64_t prev_tenant_index_retry_id = 0;
  int64_t prev_tenant_index_turn_id = 0;
  if (!merge_param.backup_set_desc_.backup_type_.is_inc_backup()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to prepare if not incremental", K(ret));
  } else if (OB_ISNULL(tmp_iter = static_cast<ObBackupMacroRangeIndexIterator *>(
                           ObLSBackupFactory::get_backup_index_iterator(type, merge_param.tenant_id_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get backup index iterator", K(ret), K(type));
  } else if (OB_FAIL(ObLSBackupOperator::get_prev_backup_set_desc(merge_param.tenant_id_,
                 merge_param.backup_set_desc_.backup_set_id_, merge_param.dest_id_,
                 prev_backup_set_info,
                 sql_proxy))) {
    LOG_WARN("failed to get prev backup set desc", K(ret), K(merge_param));
  } else if (OB_FALSE_IT(prev_backup_set_desc.backup_set_id_ = prev_backup_set_info.backup_set_id_)) {
  } else if (OB_FALSE_IT(prev_backup_set_desc.backup_type_ = prev_backup_set_info.backup_type_)) {
  } else if (OB_FALSE_IT(prev_tenant_index_turn_id = merge_param.backup_data_type_.is_major_backup()
                                                   ? prev_backup_set_info.major_turn_id_
                                                   : prev_backup_set_info.minor_turn_id_)) {
  } else if (OB_FAIL(get_prev_tenant_index_retry_id_(merge_param,
                                                     prev_backup_set_desc,
                                                     prev_tenant_index_turn_id,
                                                     prev_tenant_index_retry_id))) {
    LOG_WARN("failed to get prev tenant index retry id", K(ret), K(merge_param), K(prev_backup_set_desc));
  } else if (OB_FAIL(tmp_iter->init(merge_param.task_id_,
                 merge_param.backup_dest_,
                 merge_param.tenant_id_,
                 prev_backup_set_desc,
                 merge_param.ls_id_,
                 merge_param.backup_data_type_,
                 prev_tenant_index_turn_id,
                 prev_tenant_index_retry_id))) {
    LOG_WARN("failed to init backup macro range index iterator", K(ret), K(merge_param), K(prev_backup_set_desc), K(prev_tenant_index_retry_id));
  } else {
    iter = tmp_iter;
    tmp_iter = NULL;
    LOG_INFO("prepare prev backup set index iter", K(prev_backup_set_desc), K(merge_param));
  }
  if (OB_NOT_NULL(tmp_iter)) {
    ObLSBackupFactory::free(tmp_iter);
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::get_prev_tenant_index_retry_id_(const ObBackupIndexMergeParam &param,
    const share::ObBackupSetDesc &prev_backup_set_desc, const int64_t prev_turn_id, int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  const bool is_restore = false;
  const bool is_macro_index = true;
  const bool is_sec_meta = false;
  ObBackupTenantIndexRetryIDGetter retry_id_getter;
  if (OB_FAIL(retry_id_getter.init(param.backup_dest_, prev_backup_set_desc,
      param.backup_data_type_, prev_turn_id, is_restore, is_macro_index, is_sec_meta))) {
    LOG_WARN("failed to init retry id getter", K(ret), K(param), K(prev_turn_id));
  } else if (OB_FAIL(retry_id_getter.get_max_retry_id(retry_id))) {
    LOG_WARN("failed to get max retry id", K(ret));
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::alloc_merge_iter_(const bool tenant_level,
    const ObBackupIndexMergeParam &merge_param, const ObBackupRetryDesc &retry_desc, ObIMacroBlockIndexIterator *&iter)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = retry_desc.ls_id_;
  const int64_t turn_id = retry_desc.turn_id_;
  const int64_t retry_id = retry_desc.retry_id_;
  if (!retry_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(retry_desc));
  } else {
    if (!tenant_level) {
      const ObBackupIndexIteratorType type = BACKUP_MACRO_BLOCK_INDEX_ITERATOR;
      ObBackupMacroBlockIndexIterator *tmp_iter = NULL;
      if (OB_ISNULL(tmp_iter = static_cast<ObBackupMacroBlockIndexIterator *>(
                        ObLSBackupFactory::get_backup_index_iterator(type, merge_param.tenant_id_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc iterator", K(ret));
      } else if (OB_FAIL(tmp_iter->init(merge_param.task_id_,
                     merge_param.backup_dest_,
                     merge_param.tenant_id_,
                     merge_param.backup_set_desc_,
                     ls_id,
                     merge_param.backup_data_type_,
                     turn_id,
                     retry_id,
                     true/*need_read_inner_table*/))) {
        LOG_WARN("failed to init macro block index iterator", K(ret), K(merge_param), K(ls_id), K(turn_id));
      } else {
        iter = tmp_iter;
        tmp_iter = NULL;
      }
      if (OB_NOT_NULL(tmp_iter)) {
        ObLSBackupFactory::free(tmp_iter);
      }
    } else {
      const ObBackupIndexIteratorType type = BACKUP_MACRO_RANGE_INDEX_ITERATOR;
      ObBackupMacroRangeIndexIterator *tmp_iter = NULL;
      if (OB_ISNULL(tmp_iter = static_cast<ObBackupMacroRangeIndexIterator *>(
                        ObLSBackupFactory::get_backup_index_iterator(type, merge_param.tenant_id_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc iterator", K(ret));
      } else if (OB_FAIL(tmp_iter->init(merge_param.task_id_,
                     merge_param.backup_dest_,
                     merge_param.tenant_id_,
                     merge_param.backup_set_desc_,
                     ls_id,
                     merge_param.backup_data_type_,
                     turn_id,
                     retry_id))) {
        LOG_WARN(
            "failed to init macro block index iterator", K(ret), K(merge_param), K(ls_id), K(turn_id), K(retry_id));
      } else {
        iter = tmp_iter;
        tmp_iter = NULL;
      }
      if (OB_NOT_NULL(tmp_iter)) {
        ObLSBackupFactory::free(tmp_iter);
      }
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::get_unfinished_iters_(
    const MERGE_ITER_ARRAY &merge_iters, MERGE_ITER_ARRAY &unfinished_iters)
{
  int ret = OB_SUCCESS;
  unfinished_iters.reset();
  for (int i = 0; OB_SUCC(ret) && i < merge_iters.count(); ++i) {
    ObIMacroBlockIndexIterator *iter = merge_iters.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter should not be null", K(ret));
    } else if (iter->is_iter_end()) {
      continue;
    } else if (OB_FAIL(unfinished_iters.push_back(iter))) {
      LOG_WARN("failed to push back", K(ret), K(iter));
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::find_minimum_iters_(const MERGE_ITER_ARRAY &merge_iters, MERGE_ITER_ARRAY &min_iters)
{
  int ret = OB_SUCCESS;
  min_iters.reset();
  int64_t cmp_ret = 0;
  ObIMacroBlockIndexIterator *iter = NULL;
  for (int64_t i = merge_iters.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(iter = merge_iters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null index iter", K(ret));
    } else if (iter->is_iter_end()) {
      continue;  // skip
    } else if (min_iters.empty()) {
      if (OB_FAIL(min_iters.push_back(iter))) {
        LOG_WARN("failed to push back", K(ret), K(iter));
      }
    } else if (OB_FAIL(compare_index_iters_(min_iters.at(0), iter, cmp_ret))) {
      LOG_WARN("failed to compare index iters", K(ret), K(min_iters), K(*iter));
    } else {
      if (cmp_ret < 0) {
        min_iters.reset();
      }
      if (cmp_ret <= 0) {
        if (OB_FAIL(min_iters.push_back(iter))) {
          LOG_WARN("failed to push iter to min_iters", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::prepare_merge_fuser_(ObIBackupMacroBlockIndexFuser *&fuser)
{
  int ret = OB_SUCCESS;
  ObBackupMacroIndexFuserType type;
  ObIBackupMacroBlockIndexFuser *tmp_fuser = NULL;
  if (merge_param_.backup_data_type_.is_major_backup()) {
    type = BACKUP_MACRO_INDEX_MAJOR_FUSER;
  } else {
    type = BACKUP_MACRO_INDEX_MINOR_FUSER;
  }
  if (OB_ISNULL(tmp_fuser = ObLSBackupFactory::get_backup_macro_index_fuser(type, merge_param_.tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate provider", K(ret), K(type));
  } else {
    fuser = tmp_fuser;
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::fuse_iters_(MERGE_ITER_ARRAY &merge_iters, ObIBackupMacroBlockIndexFuser *fuser)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fuser)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fuser should not be null", K(ret));
  } else if (OB_FAIL(fuser->fuse(merge_iters))) {
    LOG_WARN("failed to fuse iters", K(ret), KP(fuser));
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::process_result_(const ObBackupMacroRangeIndex &index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!index.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(index));
  } else if (OB_FAIL(tmp_index_list_.push_back(index))) {
    LOG_WARN("failed to push back", K(ret), K(index));
  } else if (tmp_index_list_.count() >= OB_BACKUP_INDEX_BLOCK_NODE_CAPACITY) {
    if (OB_FAIL(write_macro_index_list_())) {
      LOG_WARN("failed to write macro block index list", K(ret));
    } else {
      LOG_INFO("process result", K_(tmp_index_list));
      tmp_index_list_.reset();
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::move_iters_next_(MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < merge_iters.count(); ++i) {
    ObIMacroBlockIndexIterator *iter = merge_iters.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter should not be null", K(ret));
    } else if (iter->is_iter_end()) {
      continue;
    } else if (OB_FAIL(iter->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("iter has reach end", K(ret), K(iter));
      } else {
        LOG_WARN("failed to do next", K(ret), K(iter));
      }
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::compare_index_iters_(
    ObIMacroBlockIndexIterator *lhs, ObIMacroBlockIndexIterator *rhs, int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObBackupMacroRangeIndex lvalue, rvalue;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(lhs), K(rhs));
  } else if (OB_UNLIKELY(lhs->is_iter_end() || rhs->is_iter_end())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected end row iters", K(ret));
  } else if (OB_FAIL(lhs->get_cur_index(lvalue))) {
    LOG_WARN("failed to get cur index", K(ret), K(*lhs));
  } else if (OB_FAIL(rhs->get_cur_index(rvalue))) {
    LOG_WARN("failed to get cur index", K(ret), K(*rhs));
  } else {
    cmp_ret = comparator_.operator()(lvalue, rvalue);
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::write_macro_index_list_()
{
  int ret = OB_SUCCESS;
  const ObBackupBlockType block_type = BACKUP_BLOCK_MARCO_RANGE_INDEX;
  if (OB_SUCCESS != (ret = 
          (write_index_list_<ObBackupMacroRangeIndex, ObBackupMacroRangeIndexIndex>(block_type, tmp_index_list_)))) {
    LOG_WARN("failed to write index list", K(ret), K(block_type), K_(tmp_index_list));
  } else {
    LOG_INFO("write macro index list", K_(tmp_index_list));
  }
  return ret;
}

int ObBackupMacroBlockIndexMerger::get_output_file_path_(
    const ObBackupIndexMergeParam &merge_param, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (merge_param.index_level_ == BACKUP_INDEX_LEVEL_LOG_STREAM) {
    if (OB_FAIL(share::ObBackupPathUtil::get_ls_macro_range_index_backup_path(merge_param.backup_dest_,
            merge_param.backup_set_desc_,
            merge_param.ls_id_,
            merge_param.backup_data_type_,
            merge_param.turn_id_,
            merge_param.retry_id_,
            backup_path))) {
      LOG_WARN("failed to get log stream macro range index file path", K(ret), K(merge_param));
    } else {
      LOG_INFO("get ls macro range index backup path", K(backup_path), K(merge_param));
    }
  } else if (merge_param.index_level_ == BACKUP_INDEX_LEVEL_TENANT) {
    if (OB_FAIL(share::ObBackupPathUtil::get_tenant_macro_range_index_backup_path(merge_param.backup_dest_,
            merge_param.backup_set_desc_,
            merge_param.backup_data_type_,
            merge_param.turn_id_,
            merge_param.retry_id_,
            backup_path))) {
      LOG_WARN("failed to get tenant macro range index file path", K(ret), K(merge_param));
    } else {
      LOG_INFO("get tenant macro range index backup path", K(backup_path), K(merge_param));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index level type", K(ret), K(merge_param));
  }

  return ret;
}

int ObBackupMacroBlockIndexMerger::flush_index_tree_()
{
  int ret = OB_SUCCESS;
  ObBackupMultiLevelMacroIndexBuilder builder;
  if (OB_FAIL(builder.init(offset_, buffer_node_, write_ctx_))) {
    LOG_WARN("failed to init multi level index builder", K(ret), K_(offset));
  } else if (OB_FAIL(builder.build_index<ObBackupMacroRangeIndexIndex>())) {
    LOG_WARN("failed to build index tree", K(ret));
  } else {
    LOG_INFO("flush macro block index tree", K_(offset), K_(buffer_node));
  }
  return ret;
}

/* ObBackupMetaIndexMerger */

ObBackupMetaIndexMerger::ObBackupMetaIndexMerger()
    : ObIBackupIndexMerger(), comparator_(), merge_iter_array_(), tmp_index_list_()
{}

ObBackupMetaIndexMerger::~ObBackupMetaIndexMerger()
{
  reset();
}

int ObBackupMetaIndexMerger::init(const ObBackupIndexMergeParam &merge_param, const bool is_sec_meta,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  const ObBackupBlockType block_type = BACKUP_BLOCK_META_DATA;
  const int64_t node_level = OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL + 1;
  const ObBackupFileType file_type = BACKUP_META_INDEX_FILE;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index merger init twice", K(ret));
  } else if (!merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(merge_param));
  } else if (OB_FAIL(buffer_writer_.ensure_space(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_node_.init(merge_param.tenant_id_, block_type, node_level))) {
    LOG_WARN("failed to init buffer node", K(ret), K(merge_param), K(block_type), K(node_level));
  } else if (OB_FAIL(prepare_merge_ctx_(merge_param, is_sec_meta, sql_proxy))) {
    LOG_WARN("failed to prepare merge ctx", K(ret), K(merge_param), K(is_sec_meta));
  } else if (OB_FAIL(write_backup_file_header_(file_type))) {
    LOG_WARN("failed to write backup file header", K(ret));
  } else if (OB_FAIL(merge_param_.assign(merge_param))) {
    LOG_WARN("failed to assign param", K(ret), K(merge_param));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupMetaIndexMerger::reset()
{
  for (int64_t i = 0; i < merge_iter_array_.count(); ++i) {
    ObBackupMetaIndexIterator *&iter = merge_iter_array_.at(i);
    if (OB_NOT_NULL(iter)) {
      ObLSBackupFactory::free(iter);
    }
  }
  merge_iter_array_.reset();
}

int ObBackupMetaIndexMerger::merge_index()
{
  int ret = OB_SUCCESS;
  MERGE_ITER_ARRAY unfinished_iters;
  MERGE_ITER_ARRAY min_iters;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("index merger do not init", K(ret));
  } else {
    int64_t count = 0;
    while (OB_SUCC(ret)) {
      unfinished_iters.reset();
      min_iters.reset();
      ObBackupMetaIndex meta_index;
      if (OB_FAIL(get_unfinished_iters_(merge_iter_array_, unfinished_iters))) {
        LOG_WARN("failed to get unfinished iters", K(ret), K(merge_iter_array_));
      } else if (unfinished_iters.empty()) {
        LOG_INFO("merge index finish", K(count), K(merge_iter_array_));
        break;
      } else if (OB_FAIL(find_minimum_iters_(unfinished_iters, min_iters))) {
        LOG_WARN("failed to find minumum iters", K(ret), K(unfinished_iters));
      } else if (min_iters.empty()) {
        LOG_INFO("merge index finish");
        break;
      } else if (OB_FAIL(get_fuse_result_(min_iters, meta_index))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("iterator end", K(min_iters));
          break;
        } else {
          LOG_WARN("failed to fuse iters", K(ret), K(min_iters));
        }
      } else if (OB_FAIL(process_result_(meta_index))) {
        LOG_WARN("failed to process result", K(ret), K(min_iters));
      } else if (OB_FAIL(move_iters_next_(min_iters))) {
        LOG_WARN("failed to move iters next", K(ret), K(min_iters));
      } else {
        LOG_INFO("meta index merge round", K(count), K(min_iters), K(meta_index));
        count++;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(write_meta_index_list_())) {
        LOG_WARN("failed to write index list", K(ret));
      } else if (OB_FAIL(flush_index_tree_())) {
        LOG_WARN("failed to flush index tree", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupMetaIndexMerger::prepare_merge_ctx_(
    const ObBackupIndexMergeParam &merge_param, const bool is_sec_meta, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupRetryDesc> retry_list;
  MERGE_ITER_ARRAY merge_iters;
  ObBackupPath backup_path;
  if (OB_FAIL(get_all_retries_(merge_param.task_id_,
          merge_param.tenant_id_,
          merge_param.backup_data_type_,
          merge_param.ls_id_,
          sql_proxy,
          retry_list))) {
    LOG_WARN("failed to get all retries", K(ret), K(merge_param));
  } else if (OB_FAIL(prepare_merge_iters_(merge_param, retry_list, is_sec_meta, merge_iters))) {
    LOG_WARN("failed to prepare merge iters", K(ret), K(merge_param), K(retry_list));
  } else if (OB_FAIL(merge_iter_array_.assign(merge_iters))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(get_output_file_path_(merge_param, is_sec_meta, backup_path))) {
    LOG_WARN("failed to get output file path", K(ret), K(merge_param));
  } else if (OB_FAIL(open_file_writer_(backup_path, merge_param.backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to prepare file writer", K(ret), K(backup_path), K(merge_param));
  } else if (OB_FAIL(prepare_file_write_ctx_(write_ctx_))) {
    LOG_WARN("failed to prepare file write ctx", K(ret));
  }
  return ret;
}

int ObBackupMetaIndexMerger::prepare_merge_iters_(const ObBackupIndexMergeParam &merge_param,
    const common::ObIArray<ObBackupRetryDesc> &retry_list, const bool is_sec_meta, MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  merge_iters.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < retry_list.count(); ++i) {
    const ObBackupRetryDesc &retry_desc = retry_list.at(i);
    ObBackupMetaIndexIterator *iter = NULL;
    if (OB_FAIL(alloc_merge_iter_(merge_param, retry_desc, is_sec_meta, iter))) {
      LOG_WARN("failed to alloc merge iter", K(ret), K(merge_param), K(retry_desc));
    } else if (OB_FAIL(merge_iters.push_back(iter))) {
      LOG_WARN("failed to push back", K(ret), K(iter));
      ObLSBackupFactory::free(iter);
    } else {
      FLOG_INFO("prepare meta index iter", K(retry_desc));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("prepare meta index iters", K(merge_param), K(retry_list), K(is_sec_meta), K(merge_iters));
  }
  return ret;
}

int ObBackupMetaIndexMerger::alloc_merge_iter_(const ObBackupIndexMergeParam &merge_param,
    const ObBackupRetryDesc &retry_desc, const bool is_sec_meta, ObBackupMetaIndexIterator *&iter)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndexIterator *tmp_iter = NULL;
  const share::ObLSID &ls_id = retry_desc.ls_id_;
  const int64_t retry_id = retry_desc.retry_id_;
  const int64_t turn_id = retry_desc.turn_id_;
  const ObBackupIndexIteratorType type = BACKUP_META_INDEX_ITERATOR;
  if (!retry_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret));
  } else if (OB_ISNULL(tmp_iter = static_cast<ObBackupMetaIndexIterator *>(
                           ObLSBackupFactory::get_backup_index_iterator(type, merge_param.tenant_id_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc iterator", K(ret));
  } else if (OB_FAIL(tmp_iter->init(merge_param.task_id_,
                 merge_param.backup_dest_,
                 merge_param.tenant_id_,
                 merge_param.backup_set_desc_,
                 ls_id,
                 merge_param.backup_data_type_,
                 turn_id,
                 retry_id,
                 is_sec_meta))) {
    LOG_WARN("failed to init meta index iterator", K(ret), K(merge_param));
  } else {
    iter = tmp_iter;
    tmp_iter = NULL;
  }
  if (OB_NOT_NULL(tmp_iter)) {
    ObLSBackupFactory::free(tmp_iter);
  }
  return ret;
}

int ObBackupMetaIndexMerger::get_unfinished_iters_(
    const MERGE_ITER_ARRAY &merge_iters, MERGE_ITER_ARRAY &unfinished_iters)
{
  int ret = OB_SUCCESS;
  unfinished_iters.reset();
  for (int i = 0; OB_SUCC(ret) && i < merge_iters.count(); ++i) {
    ObBackupMetaIndexIterator *iter = merge_iters.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter should not be null", K(ret));
    } else if (iter->is_iter_end()) {
      continue;
    } else if (OB_FAIL(unfinished_iters.push_back(iter))) {
      LOG_WARN("failed to push back", K(ret), K(iter));
    }
  }
  LOG_DEBUG("find unfinished iters", K(ret), K(merge_param_), K(merge_iters), K(unfinished_iters));
  return ret;
}

int ObBackupMetaIndexMerger::find_minimum_iters_(const MERGE_ITER_ARRAY &merge_iters, MERGE_ITER_ARRAY &min_iters)
{
  int ret = OB_SUCCESS;
  min_iters.reset();
  int64_t cmp_ret = 0;
  ObBackupMetaIndexIterator *iter = NULL;
  for (int64_t i = merge_iters.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(iter = merge_iters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null index iter", K(ret));
    } else if (iter->is_iter_end()) {
      continue;  // skip
    } else if (min_iters.empty()) {
      if (OB_FAIL(min_iters.push_back(iter))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else if (OB_FAIL(compare_index_iters_(min_iters.at(0), iter, cmp_ret))) {
      LOG_WARN("failed to compare index iters", K(ret), K(min_iters), K(iter));
    } else {
      if (cmp_ret < 0) {
        min_iters.reset();
      }
      if (cmp_ret <= 0) {
        if (OB_FAIL(min_iters.push_back(iter))) {
          LOG_WARN("failed to push iter to min_iters", K(ret));
        }
      }
    }
  }
  LOG_DEBUG("find minimum iters", K(ret), K(merge_param_), K(merge_iters), K(min_iters));
  return ret;
}

int ObBackupMetaIndexMerger::get_fuse_result_(const MERGE_ITER_ARRAY &iters, ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  meta_index.reset();
  ObBackupMetaIndexFuser fuser;
  if (OB_FAIL(fuser.fuse(iters))) {
    LOG_WARN("failed to fuse", K(ret));
  } else if (OB_FAIL(fuser.get_result(meta_index))) {
    LOG_WARN("failed to get fuse result", K(ret));
  } else {
    LOG_DEBUG("get fuse result", K(iters), K(meta_index));
  }
  return ret;
}

int ObBackupMetaIndexMerger::process_result_(const ObBackupMetaIndex &index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!index.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(index));
  } else if (OB_FAIL(tmp_index_list_.push_back(index))) {
    LOG_WARN("failed to push back", K(ret), K(index));
  } else if (tmp_index_list_.count() >= OB_BACKUP_INDEX_BLOCK_NODE_CAPACITY) {
    if (OB_FAIL(write_meta_index_list_())) {
      LOG_WARN("failed to write meta index list", K(ret));
    } else {
      tmp_index_list_.reset();
    }
  }
  return ret;
}

int ObBackupMetaIndexMerger::compare_index_iters_(
    ObBackupMetaIndexIterator *lhs, ObBackupMetaIndexIterator *rhs, int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex lvalue, rvalue;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(lhs), K(rhs));
  } else if (OB_UNLIKELY(lhs->is_iter_end() || rhs->is_iter_end())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected end row iters", K(ret));
  } else if (OB_FAIL(lhs->get_cur_index(lvalue))) {
    LOG_WARN("failed to get cur index", K(ret));
  } else if (OB_FAIL(rhs->get_cur_index(rvalue))) {
    LOG_WARN("failed to get cur index", K(ret));
  } else {
    cmp_ret = comparator_.operator()(lvalue, rvalue);
    LOG_DEBUG("compare meta index", K(lvalue), K(rvalue), K(cmp_ret));
  }
  return ret;
}

int ObBackupMetaIndexMerger::move_iters_next_(MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < merge_iters.count(); ++i) {
    ObBackupMetaIndexIterator *iter = merge_iters.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter should not be null", K(ret));
    } else if (iter->is_iter_end()) {
      continue;
    } else if (OB_FAIL(iter->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("meta index iter has reach end", K(*iter));
      } else {
        LOG_WARN("failed to do next", K(ret), K(iter));
      }
    }
  }
  return ret;
}

int ObBackupMetaIndexMerger::write_meta_index_list_()
{
  int ret = OB_SUCCESS;
  const ObBackupBlockType block_type = BACKUP_BLOCK_META_INDEX;
  if (OB_SUCCESS != (ret = (write_index_list_<ObBackupMetaIndex, ObBackupMetaIndexIndex>(block_type, tmp_index_list_)))) {
    LOG_WARN("failed to write index list", K(ret), K(block_type), K_(tmp_index_list));
  } else {
    LOG_INFO("write macro index list", K_(tmp_index_list));
  }
  return ret;
}

int ObBackupMetaIndexMerger::get_output_file_path_(
    const ObBackupIndexMergeParam &merge_param, const bool is_sec_meta, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (merge_param.index_level_ == BACKUP_INDEX_LEVEL_LOG_STREAM) {
    if (OB_FAIL(share::ObBackupPathUtil::get_ls_meta_index_backup_path(merge_param.backup_dest_,
            merge_param.backup_set_desc_,
            merge_param.ls_id_,
            merge_param.backup_data_type_,
            merge_param.turn_id_,
            merge_param.retry_id_,
            is_sec_meta,
            backup_path))) {
      LOG_WARN("failed to get tenant meta index backup path", K(ret), K(merge_param));
    } else {
      LOG_INFO("get ls meta index backup path", K(backup_path), K(merge_param), K(is_sec_meta));
    }
  } else if (merge_param.index_level_ == BACKUP_INDEX_LEVEL_TENANT) {
    if (OB_FAIL(share::ObBackupPathUtil::get_tenant_meta_index_backup_path(merge_param.backup_dest_,
            merge_param.backup_set_desc_,
            merge_param.backup_data_type_,
            merge_param.turn_id_,
            merge_param.retry_id_,
            is_sec_meta,
            backup_path))) {
      LOG_WARN("failed to get tenant meta index backup path", K(ret), K(merge_param));
    } else {
      LOG_INFO("get tenant meta index backup path", K(backup_path), K(merge_param), K(is_sec_meta));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index level type", K(ret), K(merge_param));
  }

  return ret;
}

int ObBackupMetaIndexMerger::flush_index_tree_()
{
  int ret = OB_SUCCESS;
  ObBackupMultiLevelMetaIndexBuilder builder;
  if (OB_FAIL(builder.init(offset_, buffer_node_, write_ctx_))) {
    LOG_WARN("failed to init multi level index builder", K(ret), K_(offset));
  } else if (OB_FAIL(builder.build_index<ObBackupMetaIndexIndex>())) {
    LOG_WARN("failed to build index tree", K(ret));
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
