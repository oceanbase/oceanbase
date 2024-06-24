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

#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_operator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace backup {

/* ObBackupDataFileRangeOp */

ObBackupDataFileRangeOp::ObBackupDataFileRangeOp(const common::ObString &file_prefix)
    : file_prefix_(file_prefix), file_id_list_()
{}

ObBackupDataFileRangeOp::~ObBackupDataFileRangeOp()
{}

int ObBackupDataFileRangeOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, entry is null", K(ret));
  } else if (OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, d_name is null", K(ret));
  } else {
    bool file_match = false;
    int64_t tmp_file_id = 0;
    ObString file_name(entry->d_name);
    if (OB_FAIL(ObIBackupIndexIterator::extract_backup_file_id_(file_name, file_prefix_, tmp_file_id, file_match))) {
      LOG_WARN("failed to extract file id", K(ret), K(file_name), K_(file_prefix));
    } else if (OB_UNLIKELY(!file_match)) {
      // file_name not match, skip
    } else if (OB_FAIL(file_id_list_.push_back(tmp_file_id))) {
      LOG_WARN("failed to push back", K(ret), K(tmp_file_id));
    }
  }
  return ret;
}

int ObBackupDataFileRangeOp::get_file_list(common::ObIArray<int64_t> &file_list)
{
  return file_list.assign(file_id_list_);
}

/* ObIBackupIndexIterator */

ObIBackupIndexIterator::ObIBackupIndexIterator()
    : is_inited_(false),
      task_id_(0),
      backup_dest_(),
      tenant_id_(OB_INVALID_ID),
      backup_set_desc_(),
      ls_id_(),
      backup_data_type_(),
      turn_id_(),
      retry_id_(),
      cur_file_id_(),
      file_id_list_(),
      block_desc_list_(),
      allocator_()
{}

ObIBackupIndexIterator::~ObIBackupIndexIterator()
{}

ObIBackupIndexIterator::ObIBackupIndexIterator(const int64_t task_id, const ObBackupDest &backup_dest,
    const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const ObLSID &ls_id,
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id)
    : is_inited_(false),
      task_id_(task_id),
      backup_dest_(),
      tenant_id_(tenant_id),
      backup_set_desc_(backup_set_desc),
      ls_id_(ls_id),
      backup_data_type_(backup_data_type),
      turn_id_(turn_id),
      retry_id_(retry_id),
      cur_file_id_(-1),
      file_id_list_(),
      block_desc_list_(),
      allocator_()
{}

int ObIBackupIndexIterator::get_file_id_list_(const bool need_read_inner_table, common::ObIArray<int64_t> &file_id_list)
{
  int ret = OB_SUCCESS;
  file_id_list.reset();
  ObBackupIoAdapter util;
  ObBackupPath backup_path;
  ObBackupDataFileRangeOp file_range_op(OB_STR_BACKUP_MACRO_BLOCK_DATA);
  ObArray<int64_t> tmp_file_id_list;
  int64_t last_file_id = 0;
  if (OB_FAIL(ObBackupPathUtil::get_ls_backup_data_dir_path(
          backup_dest_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, backup_path))) {
    LOG_WARN("failed to get log stream backup data dir path",
        K(ret),
        K_(backup_dest),
        K_(tenant_id),
        K_(backup_set_desc),
        K_(ls_id),
        K_(backup_data_type),
        K_(turn_id),
        K_(retry_id));
  } else if (OB_FAIL(util.list_files(backup_path.get_obstr(), backup_dest_.get_storage_info(), file_range_op))) {
    LOG_WARN("failed to list files", K(ret), K(backup_path), K_(backup_dest));
  } else if (OB_FAIL(file_range_op.get_file_list(tmp_file_id_list))) {
    LOG_WARN("failed to get file list", K(ret));
  } else if (!need_read_inner_table) {
    if (OB_FAIL(file_id_list.assign(tmp_file_id_list))) {
      LOG_WARN("failed to assign", K(ret), K(tmp_file_id_list));
    }
  } else if (OB_FAIL(get_last_file_id_from_inner_table_(last_file_id))) {
    LOG_WARN("failed to get last file id from inner table", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_file_id_list.count(); ++i) {
      const int64_t tmp_file_id = tmp_file_id_list.at(i);
      if (tmp_file_id <= last_file_id) {
        if (OB_FAIL(file_id_list.push_back(tmp_file_id))) {
          LOG_WARN("failed to push back", K(ret), K(tmp_file_id));
        }
      }
    }
    LOG_INFO("get file id list", K(backup_path), K(need_read_inner_table), K(file_id_list));
  }
  return ret;
}

int ObIBackupIndexIterator::get_last_file_id_from_inner_table_(int64_t &last_file_id)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  ObBackupLSTaskInfo ls_task_info;
  if (OB_FAIL(ObLSBackupOperator::get_backup_ls_task_info(tenant_id_,
          task_id_,
          ls_id_,
          turn_id_,
          retry_id_,
          backup_data_type_,
          for_update,
          ls_task_info,
          *GCTX.sql_proxy_))) {
    LOG_WARN("failed to get backup ls task info", K(ret));
  } else {
    last_file_id = ls_task_info.max_file_id_;
    LOG_INFO("get last file id from inner table", K(ls_task_info), K(last_file_id));
  }
  return ret;
}

int ObIBackupIndexIterator::get_data_backup_file_path_(const int64_t file_id, share::ObBackupPath &backup_path) const
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  if (OB_UNLIKELY(file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(file_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(backup_dest_,
                 backup_set_desc_,
                 ls_id_,
                 backup_data_type_,
                 turn_id_,
                 retry_id_,
                 file_id,
                 backup_path))) {
    LOG_WARN("failed to get major macro block file path", K(ret), K(file_id));
  }
  return ret;
}

int ObIBackupIndexIterator::get_backup_file_length_(
    const ObBackupPath &backup_path, const share::ObBackupStorageInfo *storage_info, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  file_length = 0;
  bool exist = false;
  ObBackupIoAdapter util;
  if (OB_FAIL(util.is_exist(backup_path.get_obstr(), storage_info, exist))) {
    LOG_WARN("failed to check file exist", K(ret), K(backup_path), KP(storage_info));
  } else if (OB_UNLIKELY(!exist)) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    LOG_WARN("index file do not exist", K(ret), K(backup_path));
  } else if (OB_FAIL(util.get_file_length(backup_path.get_obstr(), storage_info, file_length))) {
    LOG_WARN("failed to get file length", K(ret), K(backup_path), KP(storage_info));
  }
  return ret;
}

int ObIBackupIndexIterator::pread_file_(const common::ObString &backup_path,
    const share::ObBackupStorageInfo *storage_info, const int64_t offset, const int64_t read_size, char *buf)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  int64_t real_read_size = 0;
  if (OB_UNLIKELY(0 == backup_path.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is invalid", K(ret), K(backup_path));
  } else if (OB_UNLIKELY(read_size <= 0)) {
    LOG_INFO("read data len is zero", K(backup_path));
  } else if (OB_FAIL(util.read_part_file(backup_path, storage_info, buf, read_size, offset, real_read_size))) {
    LOG_WARN("failed to pread file", K(ret), K(backup_path), K(offset), K(read_size));
  } else if (OB_UNLIKELY(real_read_size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not read enough file", K(ret), K(read_size), K(real_read_size), K(backup_path));
  }
  return ret;
}

int ObIBackupIndexIterator::read_data_file_trailer_(const ObBackupPath &backup_path,
    const share::ObBackupStorageInfo *storage_info, ObBackupDataFileTrailer &file_trailer)
{
  int ret = OB_SUCCESS;
  file_trailer.reset();
  char *buf = NULL;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  const int64_t trailer_len = sizeof(ObBackupDataFileTrailer);
  if (OB_FAIL(get_backup_file_length_(backup_path, storage_info, file_length))) {
    LOG_WARN("failed to get file length", K(ret), K(backup_path), KP(storage_info));
  } else if (OB_UNLIKELY(file_length <= trailer_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup index file too small", K(ret), K(file_length), K(trailer_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(trailer_len));
  } else if (OB_FAIL(pread_file_(backup_path.get_obstr(), storage_info, file_length - trailer_len, trailer_len, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(backup_path), KP(storage_info), K(file_length), K(trailer_len));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, trailer_len);
    const ObBackupDataFileTrailer *trailer = NULL;
    if (OB_FAIL(buffer_reader.get(trailer))) {
      LOG_WARN("failed to get file trailer", K(ret));
    } else if (OB_ISNULL(trailer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup data file trailer is null", K(ret));
    } else if (OB_FAIL(trailer->check_valid())) {
      LOG_WARN("failed to check is valid", K(ret), K(*trailer));
    } else {
      file_trailer = *trailer;
    }
  }
  return ret;
}

int ObIBackupIndexIterator::read_index_file_trailer_(const ObBackupPath &backup_path,
    const share::ObBackupStorageInfo *storage_info, ObBackupMultiLevelIndexTrailer &file_trailer)
{
  int ret = OB_SUCCESS;
  file_trailer.reset();
  char *buf = NULL;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  const int64_t trailer_len = sizeof(ObBackupMultiLevelIndexTrailer);
  if (OB_FAIL(get_backup_file_length_(backup_path, storage_info, file_length))) {
    LOG_WARN("failed to get file length", K(ret), K(backup_path), KP(storage_info));
  } else if (OB_UNLIKELY(file_length <= trailer_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup index file too small", K(ret), K(file_length), K(trailer_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(trailer_len));
  } else if (OB_FAIL(pread_file_(backup_path.get_obstr(), storage_info, file_length - trailer_len, trailer_len, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(backup_path), KP(storage_info), K(file_length), K(trailer_len));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, trailer_len);
    const ObBackupMultiLevelIndexTrailer *trailer = NULL;
    if (OB_FAIL(buffer_reader.get(trailer))) {
      LOG_WARN("failed to get file trailer", K(ret));
    } else if (OB_ISNULL(trailer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup data file trailer is null", K(ret));
    } else if (OB_FAIL(trailer->check_valid())) {
      LOG_WARN("failed to check is valid", K(ret), K(*trailer));
    } else {
      file_trailer = *trailer;
    }
  }
  return ret;
}

int ObIBackupIndexIterator::read_backup_index_block_(const ObBackupPath &backup_path,
    const share::ObBackupStorageInfo *storage_info, const int64_t offset, const int64_t length,
    common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_path), K(offset), K(length));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(length));
  } else if (OB_FAIL(pread_file_(backup_path.get_obstr(), storage_info, offset, length, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(backup_path), KP(storage_info), K(offset), K(length));
  } else {
    buffer = blocksstable::ObBufferReader(buf, length);
  }
  return ret;
}

template <class IndexType>
int ObIBackupIndexIterator::parse_from_index_blocks_impl_(const int64_t offset,
    blocksstable::ObBufferReader &buffer_reader, common::ObIArray<IndexType> &index_list,
    common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  block_desc_list.reset();
  IndexType index;
  const ObBackupCommonHeader *common_header = NULL;
  int64_t cur_block_end_index = 0;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    int64_t start_pos = buffer_reader.pos();
    int64_t first_index = cur_block_end_index;
    if (OB_FAIL(buffer_reader.get(common_header))) {
      LOG_WARN("failed to read common header", K(ret));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common header is null", K(ret));
    } else if (OB_FAIL(common_header->check_valid())) {
      LOG_WARN("common header is not valid", K(ret));
    } else if (common_header->data_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer reader not enough", K(ret));
    } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
      LOG_WARN("failed to check data checksum", K(ret), K(buffer_reader));
    } else {
      int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
      for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
        if (OB_FAIL(buffer_reader.read_serialize(index))) {
          LOG_WARN("failed to read serialize", K(ret));
        } else if (OB_FAIL(index_list.push_back(index))) {
          LOG_WARN("failed to push back", K(ret), K(index));
        } else {
          cur_block_end_index++;
        }
      }
    }
    if (OB_SUCC(ret) && common_header->align_length_ > 0) {
      if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
        LOG_WARN("buffer reader buf not enough", K(ret), K(*common_header));
      }
    }
    if (OB_SUCC(ret)) {
      ObBackupIndexBlockDesc block_desc;
      block_desc.offset_ = offset + start_pos;
      block_desc.length_ = buffer_reader.pos() - start_pos;
      block_desc.first_index_ = first_index;
      block_desc.last_index_ = cur_block_end_index - 1;
      if (OB_FAIL(block_desc_list.push_back(block_desc))) {
        LOG_WARN("failed to push back", K(ret), K(block_desc));
      }
    }
  }
  return ret;
}

int ObIBackupIndexIterator::extract_backup_file_id_(
    const common::ObString &file_name, const common::ObString &prefix, int64_t &file_id, bool &match)
{
  int ret = OB_SUCCESS;
  const char *str = file_name.ptr();
  int64_t tmp_file_id = 0;
  file_id = 0;
  match = false;
  if (OB_UNLIKELY(file_name.empty()) || OB_ISNULL(str) || OB_UNLIKELY('\0' == (*str))) {
    match = false;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(file_name));
  } else if (!file_name.prefix_match(prefix)) {
    match = false;
  } else if (file_name.length() <= prefix.length() + 1) {
    match = false;
  } else {
    const int64_t suffix_length = file_name.length() - (prefix.length() + 1);
    const char *suffix_ptr = file_name.ptr() + prefix.length() + 1;
    ObString suffix_str = ObString(suffix_length, suffix_ptr);
    char *end_ptr = NULL;
    if (OB_FAIL(ob_strtoll(suffix_ptr, end_ptr, tmp_file_id))) {
      LOG_WARN("failed to convert str to ll", K(suffix_ptr), K(tmp_file_id));
    } else {
      file_id = tmp_file_id;
      match = true;
      LOG_INFO("extract backup fie id", K(file_name), K(prefix), K(tmp_file_id), K(suffix_ptr), K(suffix_length));
    }
  }
  return ret;
}

/* ObBackupMacroBlockIndexIterator */

ObBackupMacroBlockIndexIterator::ObBackupMacroBlockIndexIterator()
    : ObIMacroBlockIndexIterator(), cur_idx_(), cur_index_list_()
{}

ObBackupMacroBlockIndexIterator::~ObBackupMacroBlockIndexIterator()
{}

int ObBackupMacroBlockIndexIterator::init(const int64_t task_id, const ObBackupDest &backup_dest,
    const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
    const bool need_read_inner_table)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> file_id_list;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro block index iterator init twice", K(ret));
  } else if (task_id <= 0 || !backup_dest.is_valid() || OB_INVALID_ID == tenant_id || !backup_set_desc.is_valid() ||
             !backup_data_type.is_valid() || turn_id <= 0 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(task_id),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(backup_data_type),
        K(turn_id),
        K(retry_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    task_id_ = task_id;
    tenant_id_ = tenant_id;
    backup_set_desc_ = backup_set_desc;
    ls_id_ = ls_id;
    backup_data_type_ = backup_data_type;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    cur_idx_ = -1;
    cur_file_id_ = -1;
    if (OB_FAIL(get_file_id_list_(need_read_inner_table, file_id_list))) {
      LOG_WARN("failed to get file id list", K(ret));
    } else if (file_id_list.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no file exists", K(ret));
    } else {
      lib::ob_sort(file_id_list.begin(), file_id_list.end());
      if (OB_FAIL(file_id_list_.assign(file_id_list))) {
        LOG_WARN("failed to assign", K(ret), K(file_id_list));
      } else if (OB_FAIL(do_fetch_new_())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("iterator has reach end");
        } else {
          LOG_WARN("failed to do fetch new", K(ret), K(file_id_list));
        }
      } else {
        cur_idx_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexIterator::get_cur_index(ObBackupMacroRangeIndex &range_index)
{
  int ret = OB_SUCCESS;
  range_index.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (cur_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current index is not valid",
        K(ret),
        K_(tenant_id),
        K_(ls_id),
        K_(backup_data_type),
        K(cur_idx_),
        K_(cur_file_id),
        K(file_id_list_));
  } else if (cur_idx_ >= cur_index_list_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("iterator is end", K(ret), KPC(this));
  } else if (OB_FAIL(inner_get_next_macro_range_index_(range_index))) {
    LOG_WARN("failed to inner get next macro range index", K(ret));
  } else {
    LOG_INFO("inner get next macro range index", K_(backup_data_type), K(range_index));
  }
  return ret;
}

int ObBackupMacroBlockIndexIterator::next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (file_id_list_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file id list should not be empty", K(ret));
  } else if (!need_fetch_new_()) {
    cur_idx_++;
    LOG_INFO("no need fetch new", K_(cur_idx), K_(cur_file_id), K_(file_id_list));
  } else {
    cur_index_list_.reset();
    if (OB_FAIL(do_fetch_new_())) {
      LOG_WARN("failed to fetch new", K(ret));
    } else {
      cur_idx_ = 0;
    }
  }
  return ret;
}

bool ObBackupMacroBlockIndexIterator::is_iter_end() const
{
  bool bret = false;
  if (file_id_list_.empty()) {
    bret = true;
  } else {
    bret = cur_file_id_ > file_id_list_.at(file_id_list_.count() - 1) || -1 == cur_idx_;
  }
  return bret;
}

int ObBackupMacroBlockIndexIterator::do_fetch_new_()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && cur_index_list_.empty() && cur_file_id_ <= file_id_list_.at(file_id_list_.count() - 1)) {
    ++cur_file_id_;
    if (cur_file_id_ > file_id_list_.at(file_id_list_.count() - 1)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", K(ret), K_(cur_file_id), K_(file_id_list));
    } else if (OB_FAIL(inner_do_fetch_new_(cur_file_id_))) {
      LOG_WARN("failed to do fetch new", K(ret), K(cur_file_id_));
    } else {
      LOG_INFO("inner do fetch new", K_(cur_file_id), K_(cur_index_list), K_(file_id_list));
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexIterator::inner_do_fetch_new_(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMacroBlockIndex> index_list;
  ObArray<ObBackupIndexBlockDesc> block_desc_list;
  if (file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file id invalid", K(ret), K(file_id));
  } else if (!cur_index_list_.empty() && cur_idx_ < cur_index_list_.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support prepare when prev iterator not end", K(ret), K(cur_idx_), K(cur_index_list_.count()));
  } else if (FALSE_IT(cur_index_list_.reset())) {
  } else if (FALSE_IT(block_desc_list_.reset())) {
  } else if (OB_FAIL(fetch_macro_index_list_(file_id, index_list, block_desc_list))) {
    LOG_WARN("failed to fetch macro index list", K(ret), K(file_id));
  } else if (OB_FAIL(cur_index_list_.assign(index_list))) {
    LOG_WARN("failed to assign array", K(ret), K(index_list));
  } else if (OB_FAIL(block_desc_list_.assign(block_desc_list))) {
    LOG_WARN("failed to assign array", K(ret), K(block_desc_list_));
  } else {
    LOG_INFO("inner do fetch new", K(file_id));
  }
  return ret;
}

bool ObBackupMacroBlockIndexIterator::need_fetch_new_() const
{
  return -1 == cur_idx_ || cur_idx_ >= cur_index_list_.count() - 1;
}

int ObBackupMacroBlockIndexIterator::parse_from_index_blocks_(const int64_t offset,
    blocksstable::ObBufferReader &buffer_reader, common::ObIArray<ObBackupMacroBlockIndex> &index_list,
    common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list)
{
  return parse_from_index_blocks_impl_<ObBackupMacroBlockIndex>(offset, buffer_reader, index_list, block_desc_list);
}

int ObBackupMacroBlockIndexIterator::fetch_macro_index_list_(const int64_t file_id,
    common::ObIArray<ObBackupMacroBlockIndex> &cur_list, common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list)
{
  int ret = OB_SUCCESS;
  cur_list.reset();
  ObBackupPath backup_path;
  ObArenaAllocator allocator;
  blocksstable::ObBufferReader buffer_reader;
  ObBackupDataFileTrailer current_trailer;
  if (OB_UNLIKELY(file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(file_id));
  } else if (OB_FAIL(get_data_backup_file_path_(file_id, backup_path))) {
    LOG_WARN("failed to get backup file path", K(ret), K(file_id));
  } else if (OB_FAIL(read_data_file_trailer_(backup_path, backup_dest_.get_storage_info(), current_trailer))) {
    LOG_WARN("failed to read data file trailer", K(ret), K(backup_path), K_(backup_dest));
  } else if (OB_FAIL(current_trailer.check_valid())) {
    LOG_WARN("failed to check trailer", K(ret), K(current_trailer));
  } else if (0 == current_trailer.macro_index_length_) {
    LOG_INFO("current file has no macro block data", K(ret), K(file_id));
  } else if (OB_FAIL(read_backup_index_block_(backup_path,
                 backup_dest_.get_storage_info(),
                 current_trailer.macro_index_offset_,
                 current_trailer.macro_index_length_,
                 allocator,
                 buffer_reader))) {
    LOG_WARN("failed to read index block", K(ret), K(backup_path), K_(backup_dest), K(current_trailer));
  } else if (OB_FAIL(parse_from_index_blocks_(
                 current_trailer.macro_index_offset_, buffer_reader, cur_list, block_desc_list))) {
    LOG_WARN("failed to parse from block", K(ret), K(current_trailer), K(buffer_reader));
  } else {
    LOG_INFO("parse from macro index block", K(file_id), K(block_desc_list), K(cur_list.count()));
  }
  return ret;
}

int ObBackupMacroBlockIndexIterator::inner_get_next_macro_range_index_(ObBackupMacroRangeIndex &range_index)
{
  int ret = OB_SUCCESS;
  range_index.reset();
  ObBackupIndexBlockDesc block_desc;
  if (OB_FAIL(get_next_block_last_index_(block_desc))) {
    LOG_WARN("failed to get next block last index", K(ret));
  } else {
    int64_t i = cur_idx_;
    const ObBackupMacroBlockIndex &start_index = cur_index_list_.at(cur_idx_);
    ObBackupMacroBlockIndex end_index;
    for (; OB_SUCC(ret) && i < cur_index_list_.count() && i <= block_desc.last_index_; ++i) {
      const blocksstable::ObLogicMacroBlockId &cur_key = cur_index_list_.at(i).logic_id_;
      if (cur_key.tablet_id_ != start_index.logic_id_.tablet_id_) {
        end_index = cur_index_list_.at(i - 1);
        cur_idx_ = i - 1;
        break;
      } else if (i == block_desc.last_index_) {
        end_index = cur_index_list_.at(i);
        cur_idx_ = i;
        break;
      } else if (i == cur_index_list_.count() - 1) {
        end_index = cur_index_list_.at(i);
        cur_idx_ = i;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      range_index.start_key_ = start_index.logic_id_;
      range_index.end_key_ = end_index.logic_id_;
      range_index.backup_set_id_ = end_index.backup_set_id_;
      range_index.ls_id_ = end_index.ls_id_;
      range_index.turn_id_ = end_index.turn_id_;
      range_index.retry_id_ = end_index.retry_id_;
      range_index.file_id_ = end_index.file_id_;
      range_index.offset_ = block_desc.offset_;
      range_index.length_ = block_desc.length_;
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexIterator::get_next_block_last_index_(ObBackupIndexBlockDesc &last_index)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_desc_list_.count(); ++i) {
    if (block_desc_list_.at(i).last_index_ >= cur_idx_) {
      last_index = block_desc_list_.at(i);
      break;
    }
  }
  return ret;
}

/* ObBackupMacroRangeIndexIterator */

ObBackupMacroRangeIndexIterator::ObBackupMacroRangeIndexIterator()
    : ObIMacroBlockIndexIterator(),
      meet_end_(false),
      read_offset_(0),
      file_length_(0),
      backup_path_(),
      buffer_reader_(),
      cur_idx_(0),
      cur_index_list_()
{}

ObBackupMacroRangeIndexIterator::~ObBackupMacroRangeIndexIterator()
{}

int ObBackupMacroRangeIndexIterator::init(const int64_t task_id, const ObBackupDest &backup_dest,
    const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  int64_t file_length = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("range index iterator init twice", K(ret));
  } else if (!backup_dest.is_valid() || OB_INVALID_ID == tenant_id || !backup_set_desc.is_valid() ||
             !backup_data_type.is_valid() || turn_id < 0 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(ls_id),
        K(backup_data_type),
        K(turn_id),
        K(retry_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    task_id_ = task_id;
    tenant_id_ = tenant_id;
    backup_set_desc_ = backup_set_desc;
    ls_id_ = ls_id;
    backup_data_type_ = backup_data_type;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    cur_idx_ = 0;
    cur_index_list_.reset();
    ObBackupMultiLevelIndexTrailer index_trailer;
    if (OB_FAIL(get_range_index_backup_path_(backup_path))) {
      LOG_WARN("failed to get range index backup path", K(ret));
    } else if (OB_FAIL(get_backup_file_length_(backup_path, backup_dest_.get_storage_info(), file_length))) {
      LOG_WARN("failed to get file length", K(ret), K(backup_path), K_(backup_dest));
    } else if (OB_FAIL(read_index_file_trailer_(backup_path, backup_dest_.get_storage_info(), index_trailer))) {
      LOG_WARN("failed to read index file trailer", K(ret), K(backup_path), K(backup_dest));
    } else {
      backup_path_ = backup_path;
      file_length_ = file_length;
      read_offset_ = DIO_READ_ALIGN_SIZE;  // file header skipped // TODO(yangyi.yyy): align by length record in header
      // last_block_length is 0 means no index exist
      if (0 != index_trailer.last_block_length_) {
        if (OB_FAIL(do_fetch_new_())) {
          LOG_WARN("failed to do fetch new", K(ret));
        }
      } else {
        // if last_block_length is 0, then means no index exists
        meet_end_ = true;
        cur_idx_ = 0;
        cur_index_list_.reset();
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObBackupMacroRangeIndexIterator::get_cur_index(ObBackupMacroRangeIndex &range_index)
{
  int ret = OB_SUCCESS;
  range_index.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (cur_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current index is not valid", K(ret), K(cur_idx_));
  } else if (cur_idx_ >= cur_index_list_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("iterator is end", K(ret), K_(file_length), K_(meet_end), KPC(this));
  } else {
    range_index = cur_index_list_.at(cur_idx_);
    LOG_INFO("get currrent macro range index", K(range_index));
  }
  return ret;
}

bool ObBackupMacroRangeIndexIterator::is_iter_end() const
{
  return meet_end_ && cur_idx_ == cur_index_list_.count();
}

int ObBackupMacroRangeIndexIterator::next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (!need_fetch_new_()) {
    cur_idx_++;
    LOG_WARN("no need fetch new", K(ret), K_(cur_idx), K(cur_index_list_.count()), K_(meet_end));
  } else {
    cur_index_list_.reset();
    if (OB_FAIL(do_fetch_new_())) {
      LOG_WARN("failed to do fetch new", K(ret));
    } else {
      cur_idx_ = 0;
    }
  }
  return ret;
}

int ObBackupMacroRangeIndexIterator::get_range_index_backup_path_(ObBackupPath &backup_path) const
{
  int ret = OB_SUCCESS;
  if (0 == ls_id_.id()) {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_macro_range_index_backup_path(
            backup_dest_, backup_set_desc_, backup_data_type_, turn_id_, retry_id_, backup_path))) {
      LOG_WARN("failed to get tenant macro range index backup path",
          K(ret),
          K_(tenant_id),
          K_(backup_set_desc),
          K_(backup_data_type),
          K_(turn_id));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_ls_macro_range_index_backup_path(
            backup_dest_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, backup_path))) {
      LOG_WARN("failed to get tenant macro range index backup path",
          K(ret),
          K_(tenant_id),
          K_(backup_set_desc),
          K_(ls_id),
          K_(backup_data_type),
          K_(turn_id),
          K_(retry_id));
    }
  }
  return ret;
}

bool ObBackupMacroRangeIndexIterator::need_fetch_new_() const
{
  return cur_idx_ >= cur_index_list_.count() - 1 && !meet_end_;
}

int ObBackupMacroRangeIndexIterator::do_fetch_new_()
{
  int ret = OB_SUCCESS;
  if (!cur_index_list_.empty() && cur_idx_ < cur_index_list_.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support fetch new if index list is not empty", K(ret), K(cur_index_list_.count()), K(cur_idx_));
  } else {
    while (OB_SUCC(ret) && !meet_end_) {
      if (OB_FAIL(fetch_new_block_())) {
        LOG_WARN("failed to fetch new block", K(ret));
      } else if (OB_FAIL(decode_block_(buffer_reader_))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          if (!cur_index_list_.empty()) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("buffer not enough", K(ret));
            ret = OB_SUCCESS;
          }
        } else {
          LOG_WARN("failed to decode block", K(ret), K(buffer_reader_));
        }
      }
    }
  }
  return ret;
}

int ObBackupMacroRangeIndexIterator::fetch_new_block_()
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  allocator_.reuse();
  buffer_reader_.assign(NULL, 0);
  if (OB_FAIL(get_current_read_size_(read_size))) {
    LOG_WARN("failed to get current read size", K(ret));
  } else if (OB_FAIL(read_block_(backup_path_.get_obstr(),
                 backup_dest_.get_storage_info(),
                 read_offset_,
                 read_size,
                 allocator_,
                 buffer_reader_))) {
    LOG_WARN("failed to read block", K(ret), K(backup_path_), K_(backup_dest), K_(read_offset), K(read_size));
  }
  return ret;
}

int ObBackupMacroRangeIndexIterator::read_block_(const common::ObString &path,
    const share::ObBackupStorageInfo *storage_info, const int64_t offset, const int64_t length,
    common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(length));
  } else if (OB_FAIL(pread_file_(path, storage_info, offset, length, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(path), KP(storage_info), K(offset), K(length));
  } else {
    buffer_reader.assign(buf, length);
  }
  return ret;
}

int ObBackupMacroRangeIndexIterator::decode_block_(blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_macro_range_index_list_(buffer_reader, cur_index_list_))) {
    LOG_WARN("failed to get macro range index list", K(ret), K(buffer_reader));
  }
  return ret;
}

int ObBackupMacroRangeIndexIterator::get_macro_range_index_list_(
    blocksstable::ObBufferReader &buffer_reader, common::ObIArray<ObBackupMacroRangeIndex> &index_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  const ObBackupCommonHeader *common_header = NULL;
  ObBackupMultiLevelIndexHeader multi_level_header;
  while (OB_SUCC(ret) && !meet_end_) {
    if (OB_UNLIKELY(!buffer_reader.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid args", K(ret), K(buffer_reader));
    } else if (buffer_reader.remain() <= 0 || buffer_reader.remain() < sizeof(ObBackupCommonHeader)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer reader is not enough", K(ret), K(buffer_reader));
    } else if (OB_FAIL(buffer_reader.get(common_header))) {
      LOG_WARN("failed to get common header", K(ret), K(buffer_reader));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common header is null", K(ret));
    } else if (OB_FAIL(common_header->check_valid())) {
      LOG_WARN("common header is not valid", K(ret), K_(read_offset), K(common_header));
    } else if (BACKUP_BLOCK_MARCO_RANGE_INDEX_INDEX == common_header->data_type_) {
      meet_end_ = true;
      LOG_INFO("macro range index meet end", K_(file_length), K_(read_offset), KPC(common_header));
    } else if (common_header->data_length_ + common_header->align_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("backup data has incomplete data, skip it", K(ret), K(*common_header), K(buffer_reader.remain()));
    } else if (common_header->data_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer reader not enough", K(ret));
    } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
      LOG_WARN("failed to check common header data checksum", K(ret), K(*common_header));
    } else {
      const int64_t pos = buffer_reader.pos();
      int64_t data_size = 0;
      if (OB_FAIL(buffer_reader.read_serialize(multi_level_header))) {
        LOG_WARN("failed to read serialize", K(ret), K(buffer_reader));
      } else if (multi_level_header.index_level_ > 0) {
        meet_end_ = true;
        LOG_INFO("macro range index meet end", K_(file_length), K_(read_offset), K(multi_level_header));
      } else {
        data_size = common_header->data_length_ - (buffer_reader.pos() - pos);
        int64_t end_pos = buffer_reader.pos() + data_size;
        ObBackupMacroRangeIndex range_index;
        for (int i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
          range_index.reset();
          if (OB_FAIL(buffer_reader.read_serialize(range_index))) {
            LOG_WARN("failed to read serialize", K(ret), K(i), K(buffer_reader));
          } else if (OB_FAIL(index_list.push_back(range_index))) {
            LOG_WARN("failed to push back", K(ret), K(i), K(range_index));
          }
        }
        if (OB_SUCC(ret)) {
          read_offset_ += sizeof(ObBackupCommonHeader) + common_header->data_length_ + common_header->align_length_;
        }
      }
    }
  }
  return ret;
}

int ObBackupMacroRangeIndexIterator::get_current_read_size_(int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = std::min(OB_BACKUP_READ_BLOCK_SIZE, file_length_ - read_offset_);
  if (0 == read_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("not enough read size", K(file_length_), K(read_offset_));
  } else {
    LOG_INFO("get current read size", K_(file_length), K_(read_offset));
  }
  return ret;
}

/* ObBackupMetaIndexIterator */

ObBackupMetaIndexIterator::ObBackupMetaIndexIterator()
    : ObIBackupIndexIterator(), is_sec_meta_(false), cur_idx_(), cur_index_list_()
{}

ObBackupMetaIndexIterator::~ObBackupMetaIndexIterator()
{}

int ObBackupMetaIndexIterator::init(const int64_t task_id, const share::ObBackupDest &backup_dest,
    const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
    const bool is_sec_meta, const bool need_read_inner_table)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> file_id_list;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta index iterator", K(ret));
  } else if (task_id <= 0 || !backup_dest.is_valid() || OB_INVALID_ID == tenant_id || !backup_set_desc.is_valid() ||
             !backup_data_type.is_valid() || turn_id <= 0 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(task_id),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(backup_data_type),
        K(turn_id),
        K(retry_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    task_id_ = task_id;
    tenant_id_ = tenant_id;
    backup_set_desc_ = backup_set_desc;
    backup_data_type_ = backup_data_type;
    ls_id_ = ls_id;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    is_sec_meta_ = is_sec_meta;
    cur_idx_ = -1;
    cur_file_id_ = -1;
    is_inited_ = true;
    if (OB_FAIL(get_file_id_list_(need_read_inner_table, file_id_list))) {
      LOG_WARN("failed to get file id list", K(ret));
    } else if (file_id_list.empty()) {
      ret = OB_ITER_END;
      LOG_WARN("get end iter", K(ret));
    } else {
      lib::ob_sort(file_id_list.begin(), file_id_list.end());
      if (OB_FAIL(file_id_list_.assign(file_id_list))) {
        LOG_WARN("failed to assign", K(ret), K(file_id_list));
      } else if (OB_FAIL(do_fetch_new_())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("no indx", K(file_id_list));
        } else {
          LOG_WARN("failed to do fetch new", K(ret), K(file_id_list));
        }
      } else {
        cur_idx_ = 0;
      }
    }
  }
  return ret;
}

int ObBackupMetaIndexIterator::next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (file_id_list_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file id list should not be empty", K(ret));
  } else if (!need_fetch_new_()) {
    cur_idx_++;
    LOG_INFO("no need fetch new", K(cur_idx_), K_(cur_file_id), K_(file_id_list));
  } else {
    cur_index_list_.reset();
    if (OB_FAIL(do_fetch_new_())) {
      LOG_WARN("failed to do fetch new", K(ret));
    } else {
      cur_idx_ = 0;
    }
  }
  return ret;
}

int ObBackupMetaIndexIterator::get_cur_index(ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  meta_index.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (cur_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current index is not valid", K(ret), K(cur_idx_));
  } else if (cur_idx_ >= cur_index_list_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("iterator is end", K(ret), KPC(this));
  } else {
    meta_index = cur_index_list_.at(cur_idx_);
    LOG_DEBUG("get cur meta index",
        K(meta_index),
        K_(ls_id),
        K_(cur_idx),
        K_(cur_file_id),
        K_(backup_data_type),
        K(cur_index_list_.count()));
  }
  return ret;
}

bool ObBackupMetaIndexIterator::is_iter_end() const
{
  bool bret = false;
  if (file_id_list_.empty()) {
    bret = true;
  } else {
    bret = cur_file_id_ > file_id_list_.at(file_id_list_.count() - 1) || -1 == cur_idx_;
  }
  return bret;
}

int ObBackupMetaIndexIterator::do_fetch_new_()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && cur_index_list_.empty() && cur_file_id_ <= file_id_list_.at(file_id_list_.count() - 1)) {
    ++cur_file_id_;
    if (cur_file_id_ > file_id_list_.at(file_id_list_.count() - 1)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", K(ret), K_(cur_file_id), K_(file_id_list));
    } else if (OB_FAIL(inner_do_fetch_new_(cur_file_id_))) {
      LOG_WARN("failed to do fetch new", K(ret), K(cur_file_id_));
    } else {
      LOG_INFO("inner do fetch new",
          K_(tenant_id),
          K_(ls_id),
          K_(backup_data_type),
          K_(cur_file_id),
          K_(cur_index_list),
          K_(file_id_list));
    }
  }
  return ret;
}

int ObBackupMetaIndexIterator::inner_do_fetch_new_(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMetaIndex> index_list;
  ObArray<ObBackupIndexBlockDesc> block_desc_list;
  if (file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file id invalid", K(ret), K(file_id));
  } else if (!cur_index_list_.empty() && cur_idx_ < cur_index_list_.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support prepare should prev not end", K(ret), K(cur_idx_), K(cur_index_list_.count()));
  } else if (FALSE_IT(cur_index_list_.reset())) {
  } else if (FALSE_IT(block_desc_list_.reset())) {
  } else if (OB_FAIL(fetch_meta_index_list_(file_id, index_list, block_desc_list))) {
    LOG_WARN("failed to fetch macro index list", K(ret), K(file_id));
  } else if (OB_FAIL(filter_meta_index_list_(index_list))) {
    LOG_WARN("failed to filter meta index list", K(ret));
  } else if (OB_FAIL(cur_index_list_.assign(index_list))) {
    LOG_WARN("failed to assign array", K(ret), K(index_list));
  } else if (OB_FAIL(block_desc_list_.assign(block_desc_list))) {
    LOG_WARN("failed to assign array", K(ret), K(block_desc_list));
  } else {
    LOG_INFO("inner do fetch new", K(file_id), K(block_desc_list));
  }
  return ret;
}

bool ObBackupMetaIndexIterator::need_fetch_new_() const
{
  return -1 == cur_idx_ || cur_idx_ >= cur_index_list_.count() - 1;
}

int ObBackupMetaIndexIterator::parse_from_index_blocks_(const int64_t offset, blocksstable::ObBufferReader &buffer,
    common::ObIArray<ObBackupMetaIndex> &index_list, common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list)
{
  return parse_from_index_blocks_impl_<ObBackupMetaIndex>(offset, buffer, index_list, block_desc_list);
}

int ObBackupMetaIndexIterator::fetch_meta_index_list_(const int64_t file_id,
    common::ObIArray<ObBackupMetaIndex> &cur_list, common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list)
{
  int ret = OB_SUCCESS;
  cur_list.reset();
  ObBackupPath backup_path;
  ObArenaAllocator allocator;
  blocksstable::ObBufferReader buffer_reader;
  ObBackupDataFileTrailer current_trailer;
  if (OB_UNLIKELY(file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file id invalid", K(ret), K(file_id));
  } else if (OB_FAIL(get_data_backup_file_path_(file_id, backup_path))) {
    LOG_WARN("failed to get backup file path", K(ret), K(file_id));
  } else if (OB_FAIL(read_data_file_trailer_(backup_path, backup_dest_.get_storage_info(), current_trailer))) {
    LOG_WARN("failed to read data file trailer", K(ret), K(backup_path), K_(backup_dest));
  } else if (OB_FAIL(current_trailer.check_valid())) {
    LOG_WARN("failed to check trailer", K(ret), K(current_trailer));
  } else if (0 == current_trailer.meta_index_length_) {
    LOG_INFO("current file has no meta data", K(ret), K(file_id));
  } else if (OB_FAIL(read_backup_index_block_(backup_path,
                 backup_dest_.get_storage_info(),
                 current_trailer.meta_index_offset_,
                 current_trailer.meta_index_length_,
                 allocator,
                 buffer_reader))) {
    LOG_WARN("failed to read index block", K(ret), K(backup_path), K_(backup_dest), K(current_trailer));
  } else if (OB_FAIL(parse_from_index_blocks_(
                 current_trailer.meta_index_offset_, buffer_reader, cur_list, block_desc_list))) {
    LOG_WARN("failed to parse from block", K(ret), K(buffer_reader));
  } else {
    LOG_INFO("parse from meta index blocks", K(file_id), K(block_desc_list), K(cur_list.count()));
  }
  return ret;
}

int ObBackupMetaIndexIterator::filter_meta_index_list_(common::ObIArray<ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMetaIndex> tmp_meta_index_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const ObBackupMetaIndex &tmp_meta = index_list.at(i);
    if (is_sec_meta_) {
      if (BACKUP_MACRO_BLOCK_ID_MAPPING_META == tmp_meta.meta_key_.meta_type_) {
        if (OB_FAIL(tmp_meta_index_list.push_back(tmp_meta))) {
          LOG_WARN("failed to push back", K(ret), K(tmp_meta));
        }
      }
    } else {
      if (BACKUP_SSTABLE_META == tmp_meta.meta_key_.meta_type_ || BACKUP_TABLET_META == tmp_meta.meta_key_.meta_type_) {
        if (OB_FAIL(tmp_meta_index_list.push_back(tmp_meta))) {
          LOG_WARN("failed to push back", K(ret), K(tmp_meta));
        }
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

}  // namespace backup
}  // namespace oceanbase
