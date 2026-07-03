/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_backup_index_rebuild_task.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_operator.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_debug_sync.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace backup {

#ifndef REPORT_TASK_RESULT
#define REPORT_TASK_RESULT(dag_id, result)\
    if (OB_SUCCESS != (tmp_ret = ObBackupUtils::report_task_result(param_.job_desc_.job_id_, \
                              param_.job_desc_.task_id_, \
                              param_.tenant_id_, \
                              param_.ls_id_, \
                              param_.turn_id_, \
                              param_.retry_id_, \
                              param_.job_desc_.trace_id_, \
                              (dag_id), \
                              (result), \
                              report_ctx_))) { \
      LOG_WARN("failed to report task result", K(tmp_ret)); \
    }
#endif

/* shared serialization helpers used by both macro/meta finish tasks */

namespace {

template <class IndexType>
int encode_index_to_buffer(const common::ObIArray<IndexType> &index_list, ObBufferWriter &buffer_writer)
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

template <class IndexType>
int write_compressed_data(const common::ObIArray<IndexType> &index_list, const ObCompressorType &compressor_type,
    ObSelfBufferWriter &buffer_writer, ObSelfBufferWriter &compress_buf_writer,
    int64_t &data_length, int64_t &data_zlength)
{
  int ret = OB_SUCCESS;
  compress_buf_writer.reuse();
  ObBackupIndexBlockCompressor compressor;
  const int64_t block_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
  const char *out = NULL;
  int64_t out_size = 0;
  if (OB_FAIL(compress_buf_writer.ensure_space(block_size))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(encode_index_to_buffer<IndexType>(index_list, compress_buf_writer))) {
    LOG_WARN("failed to encode index to buffer", K(ret), K(index_list));
  } else if (OB_FAIL(compressor.init(block_size, compressor_type))) {
    LOG_WARN("failed to init compressor", K(ret), K(block_size));
  } else if (OB_FAIL(compressor.compress(compress_buf_writer.data(), compress_buf_writer.length(), out, out_size))) {
    LOG_WARN("failed to compress writer", K(ret), K(compress_buf_writer));
  } else if (OB_FAIL(buffer_writer.write(out, out_size))) {
    LOG_WARN("failed to write buffer", K(ret), K(out), K(out_size));
  } else {
    data_length = compress_buf_writer.length();
    data_zlength = out_size;
  }
  return ret;
}

// write one leaf index block to the output file, append the corresponding
// index_index to buffer_node so that the multi level tree can be built later.
template <class IndexType, class IndexIndexType>
int write_index_list_to_ctx(const ObBackupBlockType &block_type, const ObCompressorType &compressor_type,
    const common::ObIArray<IndexType> &index_list, ObSelfBufferWriter &buffer_writer,
    ObSelfBufferWriter &compress_buf_writer,
    ObBackupFileWriteCtx &write_ctx, int64_t &offset, ObBackupIndexBufferNode &buffer_node)
{
  int ret = OB_SUCCESS;
  buffer_writer.reuse();
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  ObBackupCommonHeader *common_header = NULL;
  ObBackupMultiLevelIndexHeader multi_level_header;
  const int64_t index_level = 0;
  int64_t data_length = 0;
  int64_t data_zlength = 0;
  if (index_list.empty()) {
    // do nothing
  } else if (OB_FAIL(buffer_writer.ensure_space(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_writer.advance_zero(header_len))) {
    LOG_WARN("advance failed", K(ret), K(header_len));
  } else if (OB_FAIL(build_multi_level_index_header(block_type, index_level, multi_level_header))) {
    LOG_WARN("failed to build multi level index header", K(ret), K(block_type), K(index_level));
  } else if (OB_FAIL(buffer_writer.write_serialize(multi_level_header))) {
    LOG_WARN("failed to write serialize multi level header", K(ret), K(multi_level_header));
  } else if (OB_FAIL(write_compressed_data<IndexType>(index_list, compressor_type, buffer_writer, compress_buf_writer, data_length, data_zlength))) {
    LOG_WARN("failed to write compressed data", K(ret), K(index_list));
  } else if (FALSE_IT(common_header = reinterpret_cast<ObBackupCommonHeader *>(buffer_writer.data()))) {
  } else if (OB_FAIL(build_common_header(block_type,
                                         data_length + multi_level_header.get_serialize_size(),
                                         data_zlength + multi_level_header.get_serialize_size(),
                                         0 /*align_length*/,
                                         compressor_type,
                                         common_header))) {
    LOG_WARN("failed to build common header", K(ret), K(buffer_writer));
  } else if (OB_FAIL(common_header->set_checksum(buffer_writer.data() + common_header->header_length_,
                 buffer_writer.length() - common_header->header_length_))) {
    LOG_WARN("failed to set checksum", K(ret), K(buffer_writer), K(*common_header));
  } else if (OB_FAIL(write_ctx.append_buffer(
                 ObBufferReader(buffer_writer.data(), buffer_writer.pos(), buffer_writer.pos())))) {
    LOG_WARN("failed to write buffer", K(ret), K(buffer_writer));
  } else {
    IndexIndexType index_index;
    index_index.end_key_ = index_list.at(index_list.count() - 1);
    index_index.offset_ = offset;
    index_index.length_ = buffer_writer.length();
    if (OB_FAIL(buffer_node.put_backup_index(index_index))) {
      LOG_WARN("failed to add backup index", K(ret), K(index_index));
    } else {
      offset += buffer_writer.length();
    }
  }
  return ret;
}

int write_backup_file_header_to_ctx(const ObBackupFileType &file_type, ObBackupFileWriteCtx &write_ctx, int64_t &offset)
{
  int ret = OB_SUCCESS;
  ObBackupFileHeader file_header;
  ObBackupFileMagic magic;
  const int64_t buf_len = DIO_READ_ALIGN_SIZE;
  char header_buf[buf_len] = "";
  ObBufferReader buffer_reader;
  if (OB_FAIL(convert_backup_file_type_to_magic(file_type, magic))) {
    LOG_WARN("failed to convert type to magic", K(ret), K(file_type));
  } else {
    file_header.magic_ = magic;
    file_header.version_ = BACKUP_DATA_VERSION_V1;
    file_header.file_type_ = file_type;
    file_header.reserved_ = 0;
    if (OB_FAIL(file_header.check_valid())) {
      LOG_WARN("failed to check file header", K(ret));
    } else if (OB_FAIL(build_backup_file_header_buffer(file_header, buf_len, header_buf, buffer_reader))) {
      LOG_WARN("failed to build backup file header buffer", K(ret), K(file_header));
    } else if (OB_FAIL(write_ctx.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
    } else {
      offset += buf_len;
    }
  }
  return ret;
}

int open_backup_index_file_writer(const ObBackupPath &path, const ObBackupStorageInfo *storage_info,
    const int64_t dest_id, ObIODevice *&dev_handle, ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  ObStorageIdMod mod;
  mod.storage_id_ = dest_id;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  if (OB_FAIL(util.mk_parent_dir(path.get_obstr(), storage_info))) {
    LOG_WARN("failed to make parent dir", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(util.open_with_access_type(dev_handle,
                                                io_fd,
                                                storage_info,
                                                path.get_obstr(),
                                                access_type,
                                                mod,
                                                false /*is_batch_write*/))) {
    LOG_WARN("failed to open with access type", K(ret), K(path), KP(storage_info));
  }
  return ret;
}

// read the macro/meta index list of a single backup data file
template <class IndexType>
int read_single_file_index(const ObBackupIndexMergeParam &merge_param, const ObBackupRetryDesc &retry_desc,
    const int64_t file_id, common::ObIArray<IndexType> &index_list)
{
  int ret = OB_SUCCESS;
  constexpr bool is_macro_index = std::is_same<IndexType, ObBackupMacroBlockIndex>::value;
  index_list.reset();
  ObBackupPath backup_path;
  ObArenaAllocator allocator;
  ObBufferReader buffer_reader;
  ObBackupDataFileTrailer trailer;
  ObArray<ObBackupIndexBlockDesc> block_desc_list;
  ObStorageIdMod mod;
  mod.storage_id_ = merge_param.dest_id_;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  const ObBackupStorageInfo *storage_info = merge_param.backup_dest_.get_storage_info();
  if (OB_UNLIKELY(file_id < 0 || !retry_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(file_id), K(retry_desc));
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(merge_param.backup_dest_,
                 merge_param.backup_set_desc_,
                 retry_desc.ls_id_,
                 merge_param.backup_data_type_,
                 retry_desc.turn_id_,
                 retry_desc.retry_id_,
                 file_id,
                 backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(merge_param), K(retry_desc), K(file_id));
  } else if (OB_FAIL(ObIBackupIndexIterator::read_data_file_trailer_(backup_path, storage_info, mod, trailer))) {
    LOG_WARN("failed to read data file trailer", K(ret), K(backup_path));
  } else if (OB_FAIL(trailer.check_valid())) {
    LOG_WARN("failed to check trailer", K(ret), K(trailer));
  } else {
    const int64_t index_offset = is_macro_index ? trailer.macro_index_offset_ : trailer.meta_index_offset_;
    const int64_t index_length = is_macro_index ? trailer.macro_index_length_ : trailer.meta_index_length_;
    if (0 == index_length) {
      LOG_INFO("current file has no index data", K(file_id), K(is_macro_index), K(backup_path));
    } else if (OB_FAIL(ObIBackupIndexIterator::read_backup_index_block_(backup_path, storage_info, mod,
                   index_offset, index_length, allocator, buffer_reader))) {
      LOG_WARN("failed to read backup index block", K(ret), K(backup_path), K(trailer));
    } else if (OB_FAIL(ObIBackupIndexIterator::parse_from_index_blocks_impl_<IndexType>(
                   index_offset, buffer_reader, index_list, block_desc_list))) {
      LOG_WARN("failed to parse from index blocks", K(ret), K(backup_path), K(trailer));
    }
  }
  return ret;
}

}  // namespace

/* ObBackupIndexRebuildWorkItem */

ObBackupIndexRebuildWorkItem::ObBackupIndexRebuildWorkItem() : retry_desc_(), file_id_(-1)
{}

void ObBackupIndexRebuildWorkItem::reset()
{
  retry_desc_.reset();
  file_id_ = -1;
}

bool ObBackupIndexRebuildWorkItem::is_valid() const
{
  return retry_desc_.is_valid() && file_id_ >= 0;
}

/* ObBackupMacroIndexMergeFinishTask::BackupMacroBlockIndexComparator */

ObBackupMacroIndexMergeFinishTask::BackupMacroBlockIndexComparator::BackupMacroBlockIndexComparator(int &sort_ret)
    : result_code_(sort_ret)
{}

bool ObBackupMacroIndexMergeFinishTask::BackupMacroBlockIndexComparator::operator()(
    const ObBackupMacroBlockIndex *left, const ObBackupMacroBlockIndex *right)
{
  bool bret = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    result_code_ = OB_INVALID_DATA;
    LOG_WARN_RET(result_code_, "should not be null", K_(result_code), KP(left), KP(right));
  } else if (left->logic_id_ < right->logic_id_) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

/* ObBackupMacroIndexMergeFinishTask */

ObBackupMacroIndexMergeFinishTask::ObBackupMacroIndexMergeFinishTask()
    : ObITask(ObITaskType::TASK_TYPE_BACKUP_MACRO_INDEX_MERGE_FINISH),
      is_inited_(false),
      mutex_(common::ObLatchIds::BACKUP_UNORDERED_MACRO_BLOCK_INDEX_MERGER_MUTEX),
      work_idx_(0),
      work_items_(),
      merge_param_(),
      param_(),
      index_level_(),
      compressor_type_(),
      ls_backup_ctx_(NULL),
      report_ctx_(),
      offset_(0),
      buffer_writer_("BackupRebuildMacro"),
      compress_buf_writer_("BackupRebuildMacroCompress"),
      dev_handle_(NULL),
      io_fd_(),
      write_ctx_(),
      buffer_node_(),
      consume_count_(0),
      external_sort_(),
      sort_result_(OB_SUCCESS),
      comparator_(sort_result_)
{}

ObBackupMacroIndexMergeFinishTask::~ObBackupMacroIndexMergeFinishTask()
{
  external_sort_.clean_up();
  if (OB_NOT_NULL(dev_handle_) && io_fd_.is_valid()) {
    ObBackupIoAdapter util;
    util.close_device_and_fd(dev_handle_, io_fd_);
  }
}

int ObBackupMacroIndexMergeFinishTask::init(const ObLSBackupDataParam &param,
    const ObBackupIndexLevel &index_level, const ObCompressorType &compressor_type,
    ObLSBackupCtx *ls_backup_ctx, const ObBackupReportCtx &report_ctx,
    const common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items)
{
  int ret = OB_SUCCESS;
  const ObBackupBlockType block_type = BACKUP_BLOCK_MACRO_DATA;
  const int64_t node_level = OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL + 1;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro index merge finish task init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(param_.convert_to(index_level, compressor_type, merge_param_))) {
    LOG_WARN("failed to convert param", K(ret), K(param), K(index_level));
  } else if (OB_FAIL(work_items_.assign(work_items))) {
    LOG_WARN("failed to assign work items", K(ret));
  } else if (OB_FAIL(buffer_writer_.ensure_space(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_node_.init(merge_param_.tenant_id_, block_type, node_level))) {
    LOG_WARN("failed to init buffer node", K(ret), K_(merge_param), K(block_type), K(node_level));
  } else if (OB_FAIL(external_sort_.init(BUF_MEM_LIMIT, FILE_BUF_SIZE, EXPIRE_TIMESTAMP, merge_param_.tenant_id_, &comparator_))) {
    LOG_WARN("failed to init external sort", K(ret), K_(merge_param));
  } else {
    index_level_ = index_level;
    compressor_type_ = compressor_type;
    ls_backup_ctx_ = ls_backup_ctx;
    report_ctx_ = report_ctx;
    work_idx_ = 0;
    is_inited_ = true;
    LOG_INFO("init macro index merge finish task", K_(merge_param), "work_count", work_items_.count());
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::check_is_iter_end(bool &is_end)
{
  ObMutexGuard guard(mutex_);
  is_end = work_idx_ >= work_items_.count();
  return OB_SUCCESS;
}

int ObBackupMacroIndexMergeFinishTask::get_next_batch(
    common::ObIArray<ObBackupIndexRebuildWorkItem> &batch)
{
  int ret = OB_SUCCESS;
  batch.reset();
  ObMutexGuard guard(mutex_);
  for (int64_t i = 0; OB_SUCC(ret) && i < READ_BATCH_SIZE && work_idx_ < work_items_.count(); ++i) {
    if (OB_FAIL(batch.push_back(work_items_.at(work_idx_)))) {
      LOG_WARN("failed to push back", K(ret), K_(work_idx));
    } else {
      ++work_idx_;
    }
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::add_items_to_sort(const common::ObIArray<ObBackupMacroBlockIndex> &index_list)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    if (OB_FAIL(external_sort_.add_item(index_list.at(i)))) {
      LOG_WARN("failed to add item", K(ret), K(index_list.at(i)));
    }
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::prepare_merge_ctx_()
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  const ObBackupFileType file_type = BACKUP_MACRO_BLOCK_INDEX_FILE;
  if (OB_FAIL(get_output_file_path_(backup_path))) {
    LOG_WARN("failed to get output file path", K(ret), K_(merge_param));
  } else if (OB_FAIL(open_backup_index_file_writer(backup_path, merge_param_.backup_dest_.get_storage_info(),
                 merge_param_.dest_id_, dev_handle_, io_fd_))) {
    LOG_WARN("failed to open file writer", K(ret), K(backup_path), K_(merge_param));
  } else if (OB_FAIL(write_ctx_.open(OB_MAX_BACKUP_FILE_SIZE, io_fd_, *dev_handle_, *GCTX.bandwidth_throttle_))) {
    LOG_WARN("failed to open backup file write ctx", K(ret));
  } else if (OB_FAIL(write_backup_file_header_to_ctx(file_type, write_ctx_, offset_))) {
    LOG_WARN("failed to write backup file header", K(ret));
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro index merge finish task do not init", K(ret));
  } else if (OB_SUCCESS != sort_result_) {
    ret = sort_result_;
    LOG_WARN("read task failed", K(ret));
  } else if (OB_NOT_NULL(ls_backup_ctx_) && OB_SUCCESS != ls_backup_ctx_->result_code_) {
    ret = ls_backup_ctx_->result_code_;
    LOG_INFO("ls backup ctx already failed, skip", K(ret));
  } else if (OB_FAIL(prepare_merge_ctx_())) {
    LOG_WARN("failed to prepare merge ctx", K(ret), K_(merge_param));
  } else if (OB_FAIL(feed_prev_backup_set_to_sort_())) {
    LOG_WARN("failed to feed prev backup set to sort", K(ret), K_(merge_param));
  } else if (OB_FAIL(do_external_sort_())) {
    LOG_WARN("failed to do external sort", K(ret));
  } else if (OB_FAIL(consume_sort_output_())) {
    LOG_WARN("failed to consume sort output", K(ret));
  } else if (OB_FAIL(flush_index_tree_())) {
    LOG_WARN("failed to flush index tree", K(ret));
  } else {
    LOG_INFO("merge macro index success", K_(merge_param), "total_count", external_sort_.get_add_count(), K_(consume_count));
  }

  if (OB_NOT_NULL(dev_handle_)) {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dev_handle_->complete(io_fd_))) {
        LOG_WARN("fail to complete multipart upload", K(ret), K_(dev_handle), K_(io_fd));
      }
    } else {
      if (OB_TMP_FAIL(dev_handle_->abort(io_fd_))) {
        LOG_WARN("fail to abort multipart upload", K(ret), K(tmp_ret), K_(dev_handle), K_(io_fd));
      }
    }
    if (OB_TMP_FAIL(util.close_device_and_fd(dev_handle_, io_fd_))) {
      ret = COVER_SUCC(tmp_ret);
      LOG_WARN("fail to close device or fd", K(ret), K(tmp_ret), K_(dev_handle), K_(io_fd));
    } else {
      dev_handle_ = NULL;
      io_fd_.reset();
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(ls_backup_ctx_)) {
    bool is_set = false;
    ls_backup_ctx_->set_result_code(ret, is_set);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_meta_index_phase_())) {
      LOG_WARN("failed to create meta index phase", K(ret), K_(merge_param));
      if (OB_NOT_NULL(ls_backup_ctx_)) {
        bool is_set = false;
        ls_backup_ctx_->set_result_code(ret, is_set);
      }
    }
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::feed_prev_backup_set_to_sort_()
{
  int ret = OB_SUCCESS;
  if (!(merge_param_.backup_set_desc_.backup_type_.is_inc_backup()
        && merge_param_.backup_data_type_.is_user_backup()
        && 0 == merge_param_.ls_id_.id())) {
    // only tenant level user data incremental backup needs to merge prev backup set index
  } else {
    const uint64_t tenant_id = merge_param_.tenant_id_;
    share::ObBackupSetFileDesc prev_backup_set_info;
    share::ObBackupSetDesc prev_backup_set_desc;
    ObBackupOrderedMacroBlockIndexIterator *iter = NULL;
    const ObBackupIndexIteratorType type = BACKUP_ORDERED_MACRO_BLOCK_INDEX_ITERATOR;
    int64_t prev_retry_id = 0;
    int64_t prev_turn_id = 0;
    if (OB_ISNULL(iter = static_cast<ObBackupOrderedMacroBlockIndexIterator *>(
                      ObLSBackupFactory::get_backup_index_iterator(type, tenant_id)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to get backup index iterator", K(ret), K(type));
    } else if (OB_FAIL(ObLSBackupOperator::get_prev_backup_set_desc(tenant_id,
                   merge_param_.backup_set_desc_.backup_set_id_, merge_param_.dest_id_, prev_backup_set_info,
                   *report_ctx_.sql_proxy_))) {
      LOG_WARN("failed to get prev backup set desc", K(ret), K_(merge_param));
    } else if (OB_FALSE_IT(prev_backup_set_desc.backup_set_id_ = prev_backup_set_info.backup_set_id_)) {
    } else if (OB_FALSE_IT(prev_backup_set_desc.backup_type_ = prev_backup_set_info.backup_type_)) {
    } else if (OB_FALSE_IT(prev_turn_id = prev_backup_set_info.major_turn_id_)) {
    } else {
      ObBackupTenantIndexRetryIDGetter retry_id_getter;
      if (OB_FAIL(retry_id_getter.init(merge_param_.backup_dest_, prev_backup_set_desc,
              merge_param_.backup_data_type_, prev_turn_id, false /*is_restore*/, true /*is_macro_index*/, false /*is_sec_meta*/))) {
        LOG_WARN("failed to init retry id getter", K(ret), K_(merge_param), K(prev_turn_id));
      } else if (OB_FAIL(retry_id_getter.get_max_retry_id(prev_retry_id))) {
        LOG_WARN("failed to get max retry id", K(ret));
      } else if (OB_FAIL(iter->init(merge_param_.task_id_,
                     merge_param_.backup_dest_,
                     tenant_id,
                     prev_backup_set_desc,
                     merge_param_.ls_id_,
                     merge_param_.backup_data_type_,
                     prev_turn_id,
                     prev_retry_id))) {
        LOG_WARN("failed to init prev backup set ordered iterator", K(ret), K_(merge_param), K(prev_backup_set_desc));
      } else {
        ObBackupMacroBlockIndex macro_index;
        while (OB_SUCC(ret) && !iter->is_iter_end()) {
          macro_index.reset();
          if (OB_FAIL(share::dag_yield())) {
            LOG_WARN("fail to yield dag", K(ret));
          } else if (OB_FAIL(iter->get_cur_index(macro_index))) {
            LOG_WARN("failed to get cur index", K(ret), KPC(iter));
          } else if (!macro_index.reusable_ && !macro_index.is_ls_inner_tablet_macro_index()) {
            // do nothing
          } else if (OB_FAIL(external_sort_.add_item(macro_index))) {
            LOG_WARN("failed to add item", K(ret), K(macro_index));
          }
          if (OB_SUCC(ret) && OB_FAIL(iter->next())) {
            LOG_WARN("failed to get next", K(ret));
          }
        }
      }
    }
    if (OB_NOT_NULL(iter)) {
      ObLSBackupFactory::free(iter);
    }
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::do_external_sort_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(external_sort_.do_sort(true /*final_merge*/))) {
    LOG_WARN("failed to do external sort", K(ret));
  } else if (OB_SUCCESS != sort_result_) {
    ret = sort_result_;
    LOG_WARN("external sort comparator failed", K(ret));
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::consume_sort_output_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMacroBlockIndex> index_list;
  while (OB_SUCC(ret)) {
    index_list.reset();
    if (OB_FAIL(share::dag_yield())) {
      LOG_WARN("fail to yield dag", K(ret));
    } else if (OB_FAIL(get_next_batch_macro_index_list_(BATCH_SIZE, index_list))) {
      LOG_WARN("failed to get next batch macro index list", K(ret));
    } else if (index_list.empty()) {
      break;
    } else if (OB_FAIL(write_macro_index_list_(index_list))) {
      LOG_WARN("failed to write macro index list", K(ret), K(index_list));
    } else {
      consume_count_ += index_list.count();
    }
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::get_next_batch_macro_index_list_(
    const int64_t batch_size, common::ObIArray<ObBackupMacroBlockIndex> &index_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  int64_t cnt = batch_size;
  ObBackupMacroBlockIndex macro_index;
  while (OB_SUCC(ret) && cnt > 0) {
    macro_index.reset();
    if (OB_FAIL(get_next_macro_index_(macro_index))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next macro index", K(ret));
      }
    } else if (OB_FAIL(index_list.push_back(macro_index))) {
      LOG_WARN("failed to push back", K(ret), K(macro_index));
    } else {
      --cnt;
    }
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::get_next_macro_index_(ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  macro_index.reset();
  const ObBackupMacroBlockIndex *index = NULL;
  if (OB_FAIL(external_sort_.get_next_item(index))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next item", K(ret));
    }
  } else if (OB_ISNULL(index)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index should not be null", K(ret));
  } else {
    macro_index = *index;
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::write_macro_index_list_(const common::ObArray<ObBackupMacroBlockIndex> &index_list)
{
  int ret = OB_SUCCESS;
  const ObBackupBlockType block_type = BACKUP_BLOCK_MACRO_BLOCK_INDEX;
  if (OB_FAIL((write_index_list_to_ctx<ObBackupMacroBlockIndex, ObBackupMacroBlockIndexIndex>(
          block_type, merge_param_.compressor_type_, index_list, buffer_writer_, compress_buf_writer_,
          write_ctx_, offset_, buffer_node_)))) {
    LOG_WARN("failed to write index list", K(ret), K(block_type), K(index_list));
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::flush_index_tree_()
{
  int ret = OB_SUCCESS;
  ObBackupMultiLevelMacroBlockIndexBuilder builder;
  if (OB_FAIL(builder.init(offset_, merge_param_.compressor_type_, buffer_node_, write_ctx_))) {
    LOG_WARN("failed to init multi level index builder", K(ret), K_(offset));
  } else if (OB_FAIL(builder.build_index<ObBackupMacroBlockIndexIndex>())) {
    LOG_WARN("failed to build index tree", K(ret));
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::get_output_file_path_(share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  if (BACKUP_INDEX_LEVEL_LOG_STREAM == merge_param_.index_level_) {
    if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_ls_macro_block_index_backup_path(merge_param_.backup_dest_,
            merge_param_.backup_set_desc_, merge_param_.ls_id_, merge_param_.backup_data_type_,
            merge_param_.turn_id_, merge_param_.retry_id_, backup_path))) {
      LOG_WARN("failed to get ls macro block index backup path", K(ret), K_(merge_param));
    }
  } else if (BACKUP_INDEX_LEVEL_TENANT == merge_param_.index_level_) {
    ObBackupDataType backup_data_type;
    backup_data_type.set_major_data_backup();
    if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_tenant_macro_block_index_backup_path(merge_param_.backup_dest_,
            merge_param_.backup_set_desc_, backup_data_type, merge_param_.turn_id_, merge_param_.retry_id_, backup_path))) {
      LOG_WARN("failed to get tenant macro block index backup path", K(ret), K_(merge_param));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index level", K(ret), K_(merge_param));
  }
  return ret;
}

int ObBackupMacroIndexMergeFinishTask::create_meta_index_phase_()
{
  int ret = OB_SUCCESS;
  share::ObIDag *dag = get_dag();
  ObBackupMetaIndexMergeFinishTask *finish_task = NULL;
  ObBackupMetaIndexReadTask *read_task = NULL;
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be null", K(ret));
  } else if (OB_FAIL(dag->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc meta finish task", K(ret));
  } else if (OB_FAIL(finish_task->init(param_, index_level_, compressor_type_, ls_backup_ctx_, report_ctx_, work_items_))) {
    LOG_WARN("failed to init meta finish task", K(ret));
  } else if (OB_FAIL(dag->alloc_task(read_task))) {
    LOG_WARN("failed to alloc meta read task", K(ret));
  } else if (OB_FAIL(read_task->init(finish_task->get_merge_param(), finish_task))) {
    LOG_WARN("failed to init meta read task", K(ret));
  } else if (OB_FAIL(this->add_child(*read_task))) {
    LOG_WARN("failed to add child read task", K(ret));
  } else if (OB_FAIL(read_task->add_child(*finish_task))) {
    LOG_WARN("failed to add child finish task", K(ret));
  } else if (OB_FAIL(dag->add_task(*read_task))) {
    LOG_WARN("failed to add read task to dag", K(ret));
  } else if (OB_FAIL(dag->add_task(*finish_task))) {
    LOG_WARN("failed to add finish task to dag", K(ret));
  } else {
    LOG_INFO("create meta index phase", K_(merge_param), "work_count", work_items_.count());
  }
  return ret;
}

void ObBackupMacroIndexMergeFinishTask::report_read_task_failed(const int result_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_report_error = false;
  ATOMIC_CAS(&sort_result_, OB_SUCCESS, result_code);
  if (OB_NOT_NULL(ls_backup_ctx_)) {
    bool is_set = false;
    ls_backup_ctx_->set_result_code(result_code, is_set);
    need_report_error = is_set;
  }
  if (0 == param_.ls_id_.id() || need_report_error) {
    REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), result_code);
  }
}

/* ObBackupMacroIndexReadTask */

ObBackupMacroIndexReadTask::ObBackupMacroIndexReadTask()
    : ObITask(ObITaskType::TASK_TYPE_BACKUP_MACRO_INDEX_READ),
      is_inited_(false),
      merge_param_(),
      finish_task_(NULL)
{}

ObBackupMacroIndexReadTask::~ObBackupMacroIndexReadTask()
{}

int ObBackupMacroIndexReadTask::init(
    const ObBackupIndexMergeParam &merge_param, ObBackupMacroIndexMergeFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro index read task init twice", K(ret));
  } else if (!merge_param.is_valid() || OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(merge_param), KP(finish_task));
  } else if (OB_FAIL(merge_param_.assign(merge_param))) {
    LOG_WARN("failed to assign merge param", K(ret), K(merge_param));
  } else {
    finish_task_ = finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupMacroIndexReadTask::process()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupIndexRebuildWorkItem> batch;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro index read task do not init", K(ret));
  } else if (OB_NOT_NULL(finish_task_->get_ls_backup_ctx())
             && OB_SUCCESS != finish_task_->get_ls_backup_ctx()->result_code_) {
    LOG_INFO("ls backup ctx already failed, skip macro read task");
  } else if (OB_FAIL(finish_task_->get_next_batch(batch))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else if (batch.empty()) {
    LOG_INFO("no more work item for macro read task");
  } else {
    ObArray<ObBackupMacroBlockIndex> index_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch.count(); ++i) {
      index_list.reset();
      const ObBackupIndexRebuildWorkItem &item = batch.at(i);
      if (OB_FAIL(read_file_index_(item, index_list))) {
        LOG_WARN("failed to read file index", K(ret), K(item));
      } else if (index_list.empty()) {
        // do nothing
      } else if (OB_FAIL(finish_task_->add_items_to_sort(index_list))) {
        LOG_WARN("failed to add items to sort", K(ret), K(item));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(finish_task_)) {
    finish_task_->report_read_task_failed(ret);
  }
  return ret;
}

int ObBackupMacroIndexReadTask::read_file_index_(
    const ObBackupIndexRebuildWorkItem &item, common::ObIArray<ObBackupMacroBlockIndex> &index_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMacroBlockIndex> tmp_index_list;
  index_list.reset();
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(item));
  } else if (OB_FAIL(read_single_file_index<ObBackupMacroBlockIndex>(
                 merge_param_, item.retry_desc_, item.file_id_, tmp_index_list))) {
    LOG_WARN("failed to read single file index", K(ret), K(item));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_index_list.count(); ++i) {
      const ObBackupMacroBlockIndex &macro_index = tmp_index_list.at(i);
      if (!macro_index.reusable_ && !macro_index.is_ls_inner_tablet_macro_index()) {
        // skip: only reusable or ls inner tablet macro index need to be kept
      } else if (OB_FAIL(index_list.push_back(macro_index))) {
        LOG_WARN("failed to push back", K(ret), K(macro_index));
      }
    }
  }
  return ret;
}

int ObBackupMacroIndexReadTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObBackupMacroIndexReadTask *tmp_next_task = NULL;
  bool is_iter_end = false;
  share::ObIDag *dag = get_dag();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro index read task do not init", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be null", K(ret));
  } else if (OB_FAIL(finish_task_->check_is_iter_end(is_iter_end))) {
    LOG_WARN("failed to check is iter end", K(ret));
  } else if (is_iter_end) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(merge_param_, finish_task_))) {
    LOG_WARN("failed to init next task", K(ret));
  } else {
    next_task = tmp_next_task;
  }
  return ret;
}

/* ObBackupMetaIndexMergeFinishTask::BackupMetaIndexComparator */

ObBackupMetaIndexMergeFinishTask::BackupMetaIndexComparator::BackupMetaIndexComparator(int &sort_ret)
    : result_code_(sort_ret)
{}

bool ObBackupMetaIndexMergeFinishTask::BackupMetaIndexComparator::operator()(
    const ObBackupMetaIndex *left, const ObBackupMetaIndex *right)
{
  bool bret = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    result_code_ = OB_INVALID_DATA;
    LOG_WARN_RET(result_code_, "should not be null", K_(result_code), KP(left), KP(right));
  } else if (left->meta_key_ < right->meta_key_) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

/* ObBackupMetaIndexMergeFinishTask */

ObBackupMetaIndexMergeFinishTask::ObBackupMetaIndexMergeFinishTask()
    : ObITask(ObITaskType::TASK_TYPE_BACKUP_META_INDEX_MERGE_FINISH),
      is_inited_(false),
      mutex_(common::ObLatchIds::BACKUP_UNORDERED_MACRO_BLOCK_INDEX_MERGER_MUTEX),
      work_idx_(0),
      work_items_(),
      merge_param_(),
      param_(),
      index_level_(),
      compressor_type_(),
      ls_backup_ctx_(NULL),
      report_ctx_(),
      offset_(0),
      buffer_writer_("BackupRebuildMeta"),
      compress_buf_writer_("BackupRebuildMetaCompress"),
      dev_handle_(NULL),
      io_fd_(),
      write_ctx_(),
      buffer_node_(),
      consume_count_(0),
      external_sort_(),
      sort_result_(OB_SUCCESS),
      comparator_(sort_result_),
      tmp_index_list_(),
      has_pending_meta_index_(false),
      sort_iter_end_(false),
      pending_meta_index_()
{}

ObBackupMetaIndexMergeFinishTask::~ObBackupMetaIndexMergeFinishTask()
{
  external_sort_.clean_up();
  if (OB_NOT_NULL(dev_handle_) && io_fd_.is_valid()) {
    ObBackupIoAdapter util;
    util.close_device_and_fd(dev_handle_, io_fd_);
  }
}

int ObBackupMetaIndexMergeFinishTask::init(const ObLSBackupDataParam &param,
    const ObBackupIndexLevel &index_level, const ObCompressorType &compressor_type,
    ObLSBackupCtx *ls_backup_ctx, const ObBackupReportCtx &report_ctx,
    const common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items)
{
  int ret = OB_SUCCESS;
  const ObBackupBlockType block_type = BACKUP_BLOCK_META_DATA;
  const int64_t node_level = OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL + 1;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta index merge finish task init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(param_.convert_to(index_level, compressor_type, merge_param_))) {
    LOG_WARN("failed to convert param", K(ret), K(param), K(index_level));
  } else if (OB_FAIL(work_items_.assign(work_items))) {
    LOG_WARN("failed to assign work items", K(ret));
  } else if (OB_FAIL(buffer_writer_.ensure_space(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_node_.init(merge_param_.tenant_id_, block_type, node_level))) {
    LOG_WARN("failed to init buffer node", K(ret), K_(merge_param), K(block_type), K(node_level));
  } else if (OB_FAIL(external_sort_.init(BUF_MEM_LIMIT, FILE_BUF_SIZE, EXPIRE_TIMESTAMP, merge_param_.tenant_id_, &comparator_))) {
    LOG_WARN("failed to init external sort", K(ret), K_(merge_param));
  } else {
    index_level_ = index_level;
    compressor_type_ = compressor_type;
    ls_backup_ctx_ = ls_backup_ctx;
    report_ctx_ = report_ctx;
    work_idx_ = 0;
    is_inited_ = true;
    LOG_INFO("init meta index merge finish task", K_(merge_param), "work_count", work_items_.count());
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::check_is_iter_end(bool &is_end)
{
  ObMutexGuard guard(mutex_);
  is_end = work_idx_ >= work_items_.count();
  return OB_SUCCESS;
}

int ObBackupMetaIndexMergeFinishTask::get_next_batch(
    common::ObIArray<ObBackupIndexRebuildWorkItem> &batch)
{
  int ret = OB_SUCCESS;
  batch.reset();
  ObMutexGuard guard(mutex_);
  for (int64_t i = 0; OB_SUCC(ret) && i < READ_BATCH_SIZE && work_idx_ < work_items_.count(); ++i) {
    if (OB_FAIL(batch.push_back(work_items_.at(work_idx_)))) {
      LOG_WARN("failed to push back", K(ret), K_(work_idx));
    } else {
      ++work_idx_;
    }
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::add_items_to_sort(const common::ObIArray<ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    if (OB_FAIL(external_sort_.add_item(index_list.at(i)))) {
      LOG_WARN("failed to add item", K(ret), K(index_list.at(i)));
    }
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::prepare_merge_ctx_()
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  const ObBackupFileType file_type = BACKUP_META_INDEX_FILE;
  if (OB_FAIL(get_output_file_path_(backup_path))) {
    LOG_WARN("failed to get output file path", K(ret), K_(merge_param));
  } else if (OB_FAIL(open_backup_index_file_writer(backup_path, merge_param_.backup_dest_.get_storage_info(),
                 merge_param_.dest_id_, dev_handle_, io_fd_))) {
    LOG_WARN("failed to open file writer", K(ret), K(backup_path), K_(merge_param));
  } else if (OB_FAIL(write_ctx_.open(OB_MAX_BACKUP_FILE_SIZE, io_fd_, *dev_handle_, *GCTX.bandwidth_throttle_))) {
    LOG_WARN("failed to open backup file write ctx", K(ret));
  } else if (OB_FAIL(write_backup_file_header_to_ctx(file_type, write_ctx_, offset_))) {
    LOG_WARN("failed to write backup file header", K(ret));
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_report_error = false;
  ObBackupIoAdapter util;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta index merge finish task do not init", K(ret));
  } else if (OB_SUCCESS != sort_result_) {
    ret = sort_result_;
    LOG_WARN("read task failed", K(ret));
  } else if (OB_NOT_NULL(ls_backup_ctx_) && OB_SUCCESS != ls_backup_ctx_->result_code_) {
    ret = ls_backup_ctx_->result_code_;
    LOG_INFO("ls backup ctx already failed, skip", K(ret));
  } else if (OB_FAIL(prepare_merge_ctx_())) {
    LOG_WARN("failed to prepare merge ctx", K(ret), K_(merge_param));
  } else if (OB_FAIL(do_external_sort_())) {
    LOG_WARN("failed to do external sort", K(ret));
  } else if (OB_FAIL(consume_sort_output_())) {
    LOG_WARN("failed to consume sort output", K(ret));
  } else if (OB_FAIL(flush_index_tree_())) {
    LOG_WARN("failed to flush index tree", K(ret));
  } else {
    LOG_INFO("merge meta index success", K_(merge_param), "total_count", external_sort_.get_add_count(), K_(consume_count));
  }

  if (OB_NOT_NULL(dev_handle_)) {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dev_handle_->complete(io_fd_))) {
        LOG_WARN("fail to complete multipart upload", K(ret), K_(dev_handle), K_(io_fd));
      }
    } else {
      if (OB_TMP_FAIL(dev_handle_->abort(io_fd_))) {
        LOG_WARN("fail to abort multipart upload", K(ret), K(tmp_ret), K_(dev_handle), K_(io_fd));
      }
    }
    if (OB_TMP_FAIL(util.close_device_and_fd(dev_handle_, io_fd_))) {
      ret = COVER_SUCC(tmp_ret);
      LOG_WARN("fail to close device or fd", K(ret), K(tmp_ret), K_(dev_handle), K_(io_fd));
    } else {
      dev_handle_ = NULL;
      io_fd_.reset();
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (0 == param_.ls_id_.id() && !param_.backup_data_type_.is_sys_backup()) {
      ret = OB_E(EventTable::EN_BACKUP_BUILD_TENANT_LEVEL_INDEX_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_build_tenant_level_index");
        LOG_WARN("errsim backup build tenant level index task failed", K(ret));
      }
    } else {
      ret = OB_E(EventTable::EN_BACKUP_BUILD_LS_LEVEL_INDEX_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup_errsim", "backup_build_ls_level_index");
        LOG_WARN("errsim backup build ls level index task failed", K(ret));
      }
    }
  }
#endif
  if (OB_TMP_FAIL(report_check_tablet_info_event_())) {
    LOG_WARN("failed to report check tablet info event", K(tmp_ret));
  }
  if (OB_NOT_NULL(ls_backup_ctx_)) {
    ls_backup_ctx_->set_finished();
    bool is_set = false;
    ls_backup_ctx_->set_result_code(ret, is_set);
    need_report_error = is_set;
  }
  if (0 == param_.ls_id_.id() || need_report_error) {
    REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), ret);
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_ts;
  record_server_event_(cost_us);
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::do_external_sort_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(external_sort_.do_sort(true /*final_merge*/))) {
    LOG_WARN("failed to do external sort", K(ret));
  } else if (OB_SUCCESS != sort_result_) {
    ret = sort_result_;
    LOG_WARN("external sort comparator failed", K(ret));
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::consume_sort_output_()
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  tmp_index_list_.reset();
  while (OB_SUCC(ret)) {
    meta_index.reset();
    if (OB_FAIL(share::dag_yield())) {
      LOG_WARN("fail to yield dag", K(ret));
    } else if (OB_FAIL(get_next_fused_meta_index_(meta_index))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next fused meta index", K(ret));
      }
    } else if (OB_FAIL(tmp_index_list_.push_back(meta_index))) {
      LOG_WARN("failed to push back", K(ret), K(meta_index));
    } else {
      ++consume_count_;
      if (tmp_index_list_.count() >= OB_BACKUP_INDEX_BLOCK_NODE_CAPACITY) {
        if (OB_FAIL(write_meta_index_list_(tmp_index_list_))) {
          LOG_WARN("failed to write meta index list", K(ret));
        } else {
          tmp_index_list_.reset();
        }
      }
    }
  }
  if (OB_SUCC(ret) && !tmp_index_list_.empty()) {
    if (OB_FAIL(write_meta_index_list_(tmp_index_list_))) {
      LOG_WARN("failed to write meta index list", K(ret));
    } else {
      tmp_index_list_.reset();
    }
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::get_next_meta_index_(ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  meta_index.reset();
  const ObBackupMetaIndex *index = NULL;
  if (OB_FAIL(external_sort_.get_next_item(index))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next item", K(ret));
    }
  } else if (OB_ISNULL(index)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index should not be null", K(ret));
  } else {
    meta_index = *index;
  }
  return ret;
}

// Fuse adjacent records with the same meta_key by selecting the version with the
// largest (turn_id, retry_id). The sorted output guarantees the records that share
// a meta_key are adjacent.
int ObBackupMetaIndexMergeFinishTask::get_next_fused_meta_index_(ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  meta_index.reset();
  ObBackupMetaIndex fused;
  bool fused_valid = false;
  if (sort_iter_end_ && !has_pending_meta_index_) {
    ret = OB_ITER_END;
  } else {
    if (has_pending_meta_index_) {
      fused = pending_meta_index_;
      fused_valid = true;
      has_pending_meta_index_ = false;
    }
    while (OB_SUCC(ret)) {
      ObBackupMetaIndex cur;
      int tmp_ret = get_next_meta_index_(cur);
      if (OB_ITER_END == tmp_ret) {
        sort_iter_end_ = true;
        break;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to get next meta index", K(ret));
      } else if (!fused_valid) {
        fused = cur;
        fused_valid = true;
      } else if (cur.meta_key_ == fused.meta_key_) {
        if (cur.turn_id_ > fused.turn_id_
            || (cur.turn_id_ == fused.turn_id_ && cur.retry_id_ > fused.retry_id_)) {
          fused = cur;
        }
      } else {
        pending_meta_index_ = cur;
        has_pending_meta_index_ = true;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (fused_valid) {
        meta_index = fused;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::write_meta_index_list_(const common::ObArray<ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  const ObBackupBlockType block_type = BACKUP_BLOCK_META_INDEX;
  if (OB_FAIL((write_index_list_to_ctx<ObBackupMetaIndex, ObBackupMetaIndexIndex>(
          block_type, merge_param_.compressor_type_, index_list, buffer_writer_, compress_buf_writer_,
          write_ctx_, offset_, buffer_node_)))) {
    LOG_WARN("failed to write index list", K(ret), K(block_type), K(index_list));
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::flush_index_tree_()
{
  int ret = OB_SUCCESS;
  ObBackupMultiLevelMetaIndexBuilder builder;
  if (OB_FAIL(builder.init(offset_, merge_param_.compressor_type_, buffer_node_, write_ctx_))) {
    LOG_WARN("failed to init multi level index builder", K(ret), K_(offset));
  } else if (OB_FAIL(builder.build_index<ObBackupMetaIndexIndex>())) {
    LOG_WARN("failed to build index tree", K(ret));
  }
  return ret;
}

int ObBackupMetaIndexMergeFinishTask::get_output_file_path_(share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  if (BACKUP_INDEX_LEVEL_LOG_STREAM == merge_param_.index_level_) {
    if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_ls_meta_index_backup_path(merge_param_.backup_dest_,
            merge_param_.backup_set_desc_, merge_param_.ls_id_, merge_param_.backup_data_type_,
            merge_param_.turn_id_, merge_param_.retry_id_, backup_path))) {
      LOG_WARN("failed to get ls meta index backup path", K(ret), K_(merge_param));
    }
  } else if (BACKUP_INDEX_LEVEL_TENANT == merge_param_.index_level_) {
    ObBackupDataType backup_data_type;
    backup_data_type.set_major_data_backup();
    if (OB_FAIL(ObBackupPathUtil::get_tenant_meta_index_backup_path(merge_param_.backup_dest_,
            merge_param_.backup_set_desc_, backup_data_type, merge_param_.turn_id_, merge_param_.retry_id_,
            false /*is_sec_meta*/, backup_path))) {
      LOG_WARN("failed to get tenant meta index backup path", K(ret), K_(merge_param));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index level", K(ret), K_(merge_param));
  }
  return ret;
}

void ObBackupMetaIndexMergeFinishTask::record_server_event_(const int64_t cost_us) const
{
  SERVER_EVENT_ADD("backup",
      "build_index",
      "tenant_id",
      param_.tenant_id_,
      "backup_set_id",
      param_.backup_set_desc_.backup_set_id_,
      "ls_id",
      param_.ls_id_.id(),
      "cost_us",
      cost_us,
      "total_count",
      external_sort_.get_add_count(),
      "consume_count",
      consume_count_);
  FLOG_INFO("build backup index finish", K_(param), K(cost_us),
      "total_count", external_sort_.get_add_count(), K_(consume_count), "file_count", work_items_.count(),
      K_(index_level), "backup_data_type", param_.backup_data_type_);
}

int ObBackupMetaIndexMergeFinishTask::report_check_tablet_info_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_backup_ctx_)) {
    // tenant level rebuild has no ls backup ctx
  } else if (BACKUP_INDEX_LEVEL_TENANT == index_level_) {
    // do nothing
  } else if (!param_.backup_data_type_.is_user_backup()) {
    // do nothing
  } else {
    const int64_t total_cost_time = ls_backup_ctx_->check_tablet_info_cost_time_;
    const int64_t total_tablet_count = ls_backup_ctx_->data_tablet_id_list_.count();
    int64_t avg_cost_time = 0;
    if (0 != total_tablet_count) {
      avg_cost_time = total_cost_time / total_tablet_count;
    }
    SERVER_EVENT_ADD("backup_data", "check_tablet_info",
                     "tenant_id", ls_backup_ctx_->param_.tenant_id_,
                     "ls_id", ls_backup_ctx_->param_.ls_id_.id(),
                     "total_cost_time_us", total_cost_time,
                     "total_tablet_count", total_tablet_count,
                     "avg_cost_time_us", avg_cost_time);
  }
  return ret;
}

void ObBackupMetaIndexMergeFinishTask::report_read_task_failed(const int result_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_report_error = false;
  ATOMIC_CAS(&sort_result_, OB_SUCCESS, result_code);
  if (OB_NOT_NULL(ls_backup_ctx_)) {
    bool is_set = false;
    ls_backup_ctx_->set_result_code(result_code, is_set);
    need_report_error = is_set;
  }
  if (0 == param_.ls_id_.id() || need_report_error) {
    REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), result_code);
  }
}

/* ObBackupMetaIndexReadTask */

ObBackupMetaIndexReadTask::ObBackupMetaIndexReadTask()
    : ObITask(ObITaskType::TASK_TYPE_BACKUP_META_INDEX_READ),
      is_inited_(false),
      merge_param_(),
      finish_task_(NULL)
{}

ObBackupMetaIndexReadTask::~ObBackupMetaIndexReadTask()
{}

int ObBackupMetaIndexReadTask::init(
    const ObBackupIndexMergeParam &merge_param, ObBackupMetaIndexMergeFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta index read task init twice", K(ret));
  } else if (!merge_param.is_valid() || OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(merge_param), KP(finish_task));
  } else if (OB_FAIL(merge_param_.assign(merge_param))) {
    LOG_WARN("failed to assign merge param", K(ret), K(merge_param));
  } else {
    finish_task_ = finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupMetaIndexReadTask::process()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupIndexRebuildWorkItem> batch;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta index read task do not init", K(ret));
  } else if (OB_NOT_NULL(finish_task_->get_ls_backup_ctx())
             && OB_SUCCESS != finish_task_->get_ls_backup_ctx()->result_code_) {
    LOG_INFO("ls backup ctx already failed, skip meta read task");
  } else if (OB_FAIL(finish_task_->get_next_batch(batch))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else if (batch.empty()) {
    LOG_INFO("no more work item for meta read task");
  } else {
    ObArray<ObBackupMetaIndex> index_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch.count(); ++i) {
      index_list.reset();
      const ObBackupIndexRebuildWorkItem &item = batch.at(i);
      if (OB_FAIL(read_file_index_(item, index_list))) {
        LOG_WARN("failed to read file index", K(ret), K(item));
      } else if (index_list.empty()) {
        // do nothing
      } else if (OB_FAIL(finish_task_->add_items_to_sort(index_list))) {
        LOG_WARN("failed to add items to sort", K(ret), K(item));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(finish_task_)) {
    finish_task_->report_read_task_failed(ret);
  }
  return ret;
}

int ObBackupMetaIndexReadTask::read_file_index_(
    const ObBackupIndexRebuildWorkItem &item, common::ObIArray<ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(item));
  } else if (OB_FAIL(read_single_file_index<ObBackupMetaIndex>(
                 merge_param_, item.retry_desc_, item.file_id_, index_list))) {
    LOG_WARN("failed to read single file index", K(ret), K(item));
  } else if (OB_FAIL(filter_meta_index_list_(index_list))) {
    LOG_WARN("failed to filter meta index list", K(ret), K(item));
  }
  return ret;
}

int ObBackupMetaIndexReadTask::filter_meta_index_list_(common::ObIArray<ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMetaIndex> tmp_meta_index_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const ObBackupMetaIndex &tmp_meta = index_list.at(i);
    if (BACKUP_SSTABLE_META == tmp_meta.meta_key_.meta_type_ || BACKUP_TABLET_META == tmp_meta.meta_key_.meta_type_) {
      if (OB_FAIL(tmp_meta_index_list.push_back(tmp_meta))) {
        LOG_WARN("failed to push back", K(ret), K(tmp_meta));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(index_list.assign(tmp_meta_index_list))) {
      LOG_WARN("failed to assign index list", K(ret), K(tmp_meta_index_list));
    }
  }
  return ret;
}

int ObBackupMetaIndexReadTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndexReadTask *tmp_next_task = NULL;
  bool is_iter_end = false;
  share::ObIDag *dag = get_dag();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta index read task do not init", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be null", K(ret));
  } else if (OB_FAIL(finish_task_->check_is_iter_end(is_iter_end))) {
    LOG_WARN("failed to check is iter end", K(ret));
  } else if (is_iter_end) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(merge_param_, finish_task_))) {
    LOG_WARN("failed to init next task", K(ret));
  } else {
    next_task = tmp_next_task;
  }
  return ret;
}

/* ObBackupIndexRebuildPrepareTask */

ObBackupIndexRebuildPrepareTask::ObBackupIndexRebuildPrepareTask()
    : ObITask(ObITaskType::TASK_TYPE_BACKUP_INDEX_REBUILD_PREPARE),
      is_inited_(false),
      param_(),
      index_level_(),
      ls_backup_ctx_(NULL),
      provider_(NULL),
      task_mgr_(NULL),
      index_kv_cache_(NULL),
      report_ctx_(),
      compressor_type_()
{}

ObBackupIndexRebuildPrepareTask::~ObBackupIndexRebuildPrepareTask()
{}

int ObBackupIndexRebuildPrepareTask::init(const ObLSBackupDataParam &param, const ObBackupIndexLevel &index_level,
    ObLSBackupCtx *ls_backup_ctx, ObIBackupTabletProvider *provider, ObBackupMacroBlockTaskMgr *task_mgr,
    ObBackupIndexKVCache *kv_cache, const ObBackupReportCtx &report_ctx, const ObCompressorType &compressor_type)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("prepare task init twice", K(ret));
  } else if (!param.is_valid() || index_level < BACKUP_INDEX_LEVEL_LOG_STREAM || index_level >= MAX_BACKUP_INDEX_LEVEL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(index_level));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    report_ctx_ = report_ctx;
    index_level_ = index_level;
    ls_backup_ctx_ = ls_backup_ctx;
    provider_ = provider;
    task_mgr_ = task_mgr;
    index_kv_cache_ = kv_cache;
    compressor_type_ = compressor_type;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupIndexRebuildPrepareTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_report_error = false;
  DEBUG_SYNC(BEFORE_BACKUP_BUILD_INDEX);
  const int64_t start_ts = ObTimeUtility::current_time();
  ObArray<ObBackupIndexRebuildWorkItem> work_items;
  ObBackupIndexMergeParam merge_param;
  LOG_INFO("start backup index rebuild prepare", K_(index_level), K_(param));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("prepare task do not init", K(ret));
  } else if (OB_NOT_NULL(ls_backup_ctx_) && OB_SUCCESS != ls_backup_ctx_->result_code_) {
    ret = ls_backup_ctx_->result_code_;
    LOG_INFO("ls backup ctx already failed, skip", K(ret));
  } else if (OB_FAIL(check_all_tablet_released_())) {
    LOG_WARN("failed to check all tablet released", K(ret));
  } else if (OB_FAIL(mark_ls_task_final_())) {
    LOG_WARN("failed to mark ls task final", K(ret));
  } else if (OB_FAIL(param_.convert_to(index_level_, compressor_type_, merge_param))) {
    LOG_WARN("failed to convert param", K(ret), K_(param), K_(index_level));
  } else if (OB_FAIL(collect_work_items_(merge_param, work_items))) {
    LOG_WARN("failed to collect work items", K(ret), K(merge_param));
  }

  // Build the index pipeline: when macro index is needed, create the macro
  // phase first — its finish task (ObBackupMacroIndexMergeFinishTask::process)
  // will chain-create the meta phase automatically. Otherwise create the meta
  // phase directly.
  if (OB_SUCC(ret)) {
    if (need_build_index_(true /*is_build_macro_index*/)) {
      if (OB_FAIL(create_macro_index_phase_(work_items))) {
        LOG_WARN("failed to create macro index phase", K(ret), K(merge_param));
      }
    } else if (need_build_index_(false /*is_build_macro_index*/)) {
      if (OB_FAIL(create_meta_index_phase_(work_items))) {
        LOG_WARN("failed to create meta index phase", K(ret), K(merge_param));
      }
    }
  }

  // If the prepare task itself fails, no follow-up finish task will report the
  // result, so report here. On success the meta finish task does the reporting.
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(ls_backup_ctx_)) {
      ls_backup_ctx_->set_finished();
      bool is_set = false;
      ls_backup_ctx_->set_result_code(ret, is_set);
      need_report_error = is_set;
    }
    if (0 == param_.ls_id_.id() || need_report_error) {
      REPORT_TASK_RESULT(this->get_dag()->get_dag_id(), ret);
    }
    const int64_t cost_us = ObTimeUtility::current_time() - start_ts;
    UNUSED(cost_us);
  }
  return ret;
}

int ObBackupIndexRebuildPrepareTask::check_all_tablet_released_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (0 == param_.ls_id_.id()) {
    LOG_INFO("do nothing if tenant level build");
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else {
    bool all_released = ls_backup_ctx_->tablet_holder_.is_empty();
    if (!all_released) {
      LOG_ERROR("tablet handle not released", K(ret));
      if (OB_SUCCESS != (tmp_ret = ls_backup_ctx_->tablet_stat_.print_tablet_stat())) {
        LOG_WARN("failed to print tablet stat", K(ret), K(tmp_ret));
      }
      ls_backup_ctx_->tablet_stat_.reuse();
    }
  }
  return ret;
}

int ObBackupIndexRebuildPrepareTask::mark_ls_task_final_()
{
  int ret = OB_SUCCESS;
  if (0 == param_.ls_id_.id()) {
    // do nothing
  } else if (OB_FAIL(ObLSBackupOperator::mark_ls_task_info_final(param_.job_desc_.task_id_,
                 param_.tenant_id_,
                 param_.ls_id_,
                 param_.turn_id_,
                 param_.retry_id_,
                 param_.backup_data_type_,
                 *report_ctx_.sql_proxy_))) {
    LOG_WARN("failed to mark ls task info final", K(ret), K_(param));
  }
  return ret;
}

bool ObBackupIndexRebuildPrepareTask::need_build_index_(const bool is_build_macro_index) const
{
  bool bret = true;
  if (0 == param_.ls_id_.id()) {
    if (is_build_macro_index) {
      bret = param_.backup_data_type_.is_user_backup();
    } else {
      bret = !param_.backup_data_type_.is_sys_backup();
    }
  }
  return bret;
}

int ObBackupIndexRebuildPrepareTask::collect_work_items_(
    const ObBackupIndexMergeParam &merge_param, common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items)
{
  int ret = OB_SUCCESS;
  work_items.reset();
  ObArray<ObBackupRetryDesc> retry_list;
  if (OB_ISNULL(report_ctx_.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy should not be null", K(ret));
  } else if (OB_FAIL(ObLSBackupOperator::get_all_retries(merge_param.task_id_,
                 merge_param.tenant_id_,
                 merge_param.backup_data_type_,
                 merge_param.ls_id_,
                 retry_list,
                 *report_ctx_.sql_proxy_))) {
    LOG_WARN("failed to get all retries", K(ret), K(merge_param));
  } else {
    ObBackupIndexRebuildWorkItem work_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < retry_list.count(); ++i) {
      const ObBackupRetryDesc &retry_desc = retry_list.at(i);
      ObArray<int64_t> file_id_list;
      ObBackupPath dir_path;
      ObBackupIoAdapter util;
      ObBackupDataFileRangeOp file_range_op(OB_STR_BACKUP_MACRO_BLOCK_DATA);
      if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_ls_backup_data_dir_path(merge_param.backup_dest_,
              merge_param.backup_set_desc_, retry_desc.ls_id_, merge_param.backup_data_type_,
              retry_desc.turn_id_, retry_desc.retry_id_, dir_path))) {
        LOG_WARN("failed to get ls backup data dir path", K(ret), K(merge_param), K(retry_desc));
      } else if (OB_FAIL(util.list_files(dir_path.get_obstr(), merge_param.backup_dest_.get_storage_info(), file_range_op))) {
        LOG_WARN("failed to list files", K(ret), K(dir_path));
      } else if (OB_FAIL(file_range_op.get_file_list(file_id_list))) {
        LOG_WARN("failed to get file list", K(ret), K(dir_path));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < file_id_list.count(); ++j) {
          const int64_t file_id = file_id_list.at(j);
          if (file_id > retry_desc.last_file_id_) {
            // skip files beyond the max valid file id recorded in inner table
          } else {
            work_item.reset();
            work_item.retry_desc_ = retry_desc;
            work_item.file_id_ = file_id;
            if (OB_FAIL(work_items.push_back(work_item))) {
              LOG_WARN("failed to push back", K(ret), K(work_item));
            }
          }
        }
      }
    }
    LOG_INFO("collect work items for index rebuild", K(merge_param), K(retry_list), "work_count", work_items.count());
  }
  return ret;
}

int ObBackupIndexRebuildPrepareTask::create_macro_index_phase_(
    const common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items)
{
  int ret = OB_SUCCESS;
  share::ObIDag *dag = get_dag();
  ObBackupMacroIndexMergeFinishTask *finish_task = NULL;
  ObBackupMacroIndexReadTask *read_task = NULL;
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be null", K(ret));
  } else if (OB_FAIL(dag->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc macro finish task", K(ret));
  } else if (OB_FAIL(finish_task->init(param_, index_level_, compressor_type_, ls_backup_ctx_, report_ctx_, work_items))) {
    LOG_WARN("failed to init macro finish task", K(ret));
  } else if (OB_FAIL(dag->alloc_task(read_task))) {
    LOG_WARN("failed to alloc macro read task", K(ret));
  } else if (OB_FAIL(read_task->init(finish_task->get_merge_param(), finish_task))) {
    LOG_WARN("failed to init macro read task", K(ret));
  } else if (OB_FAIL(this->add_child(*read_task))) {
    LOG_WARN("failed to add child read task", K(ret));
  } else if (OB_FAIL(read_task->add_child(*finish_task))) {
    LOG_WARN("failed to add child finish task", K(ret));
  } else if (OB_FAIL(dag->add_task(*read_task))) {
    LOG_WARN("failed to add read task to dag", K(ret));
  } else if (OB_FAIL(dag->add_task(*finish_task))) {
    LOG_WARN("failed to add finish task to dag", K(ret));
  } else {
    LOG_INFO("create macro index phase", K_(param), "work_count", work_items.count());
  }
  return ret;
}

int ObBackupIndexRebuildPrepareTask::create_meta_index_phase_(
    const common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items)
{
  int ret = OB_SUCCESS;
  share::ObIDag *dag = get_dag();
  ObBackupMetaIndexMergeFinishTask *finish_task = NULL;
  ObBackupMetaIndexReadTask *read_task = NULL;
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be null", K(ret));
  } else if (OB_FAIL(dag->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc meta finish task", K(ret));
  } else if (OB_FAIL(finish_task->init(param_, index_level_, compressor_type_, ls_backup_ctx_, report_ctx_, work_items))) {
    LOG_WARN("failed to init meta finish task", K(ret));
  } else if (OB_FAIL(dag->alloc_task(read_task))) {
    LOG_WARN("failed to alloc meta read task", K(ret));
  } else if (OB_FAIL(read_task->init(finish_task->get_merge_param(), finish_task))) {
    LOG_WARN("failed to init meta read task", K(ret));
  } else if (OB_FAIL(this->add_child(*read_task))) {
    LOG_WARN("failed to add child read task", K(ret));
  } else if (OB_FAIL(read_task->add_child(*finish_task))) {
    LOG_WARN("failed to add child finish task", K(ret));
  } else if (OB_FAIL(dag->add_task(*read_task))) {
    LOG_WARN("failed to add read task to dag", K(ret));
  } else if (OB_FAIL(dag->add_task(*finish_task))) {
    LOG_WARN("failed to add finish task to dag", K(ret));
  } else {
    LOG_INFO("create meta index phase", K_(param), "work_count", work_items.count());
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
