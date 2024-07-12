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

#include "storage/backup/ob_backup_ctx.h"
#include "lib/lock/ob_mutex.h"
#include "lib/oblog/ob_log_module.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_operator.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_reader.h"
#include "share/backup/ob_backup_struct.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/backup/ob_backup_data_table_operator.h"

#include <algorithm>

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::lib;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

/* ObSimpleBackupStat */

ObSimpleBackupStat::ObSimpleBackupStat()
    : start_ts_(0),
      end_ts_(0),
      total_bytes_(0),
      tablet_meta_count_(0),
      sstable_meta_count_(0),
      macro_block_count_(0),
      reused_macro_block_count_(0)
{}

ObSimpleBackupStat::~ObSimpleBackupStat()
{}

void ObSimpleBackupStat::reset()
{
  start_ts_ = 0;
  end_ts_ = 0;
  total_bytes_ = 0;
  tablet_meta_count_ = 0;
  sstable_meta_count_ = 0;
  macro_block_count_ = 0;
  reused_macro_block_count_ = 0;
}

/* ObSimpleBackupStatMgr */

ObSimpleBackupStatMgr::ObSimpleBackupStatMgr()
    : mutex_(common::ObLatchIds::BACKUP_LOCK), is_inited_(false), backup_dest_(), backup_set_desc_(), tenant_id_(OB_INVALID_ID), ls_id_(), stat_list_()
{}

ObSimpleBackupStatMgr::~ObSimpleBackupStatMgr()
{}

int ObSimpleBackupStatMgr::init(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
    const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("stat mgr init twice", K(ret));
  } else if (!backup_dest.is_valid() || !backup_set_desc.is_valid() || OB_INVALID_ID == tenant_id ||
             !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    reset_stat_list_();
    backup_set_desc_ = backup_set_desc;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSimpleBackupStatMgr::mark_begin(const share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObSimpleBackupStat *stat = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat mgr do not init", K(ret));
  } else if (OB_FAIL(get_stat_(backup_data_type, stat))) {
    LOG_WARN("failed to get stat", K(ret), K(backup_data_type));
  } else {
    stat->start_ts_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObSimpleBackupStatMgr::mark_end(const share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObSimpleBackupStat *stat = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat mgr do not init", K(ret));
  } else if (OB_FAIL(get_stat_(backup_data_type, stat))) {
    LOG_WARN("failed to get stat", K(ret), K(backup_data_type));
  } else {
    stat->end_ts_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObSimpleBackupStatMgr::add_macro_block(
    const share::ObBackupDataType &backup_data_type, const blocksstable::ObLogicMacroBlockId &logic_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObSimpleBackupStat *stat = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat mgr do not init", K(ret));
  } else if (OB_FAIL(get_stat_(backup_data_type, stat))) {
    LOG_WARN("failed to get stat", K(ret), K(backup_data_type));
  } else {
    stat->macro_block_count_++;
  }
  return ret;
}

int ObSimpleBackupStatMgr::add_sstable_meta(
    const share::ObBackupDataType &backup_data_type, const storage::ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObSimpleBackupStat *stat = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat mgr do not init", K(ret));
  } else if (OB_FAIL(get_stat_(backup_data_type, stat))) {
    LOG_WARN("failed to get stat", K(ret), K(backup_data_type));
  } else {
    stat->sstable_meta_count_++;
  }
  return ret;
}

int ObSimpleBackupStatMgr::add_tablet_meta(
    const share::ObBackupDataType &backup_data_type, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObSimpleBackupStat *stat = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat mgr do not init", K(ret));
  } else if (OB_FAIL(get_stat_(backup_data_type, stat))) {
    LOG_WARN("failed to get stat", K(ret), K(backup_data_type));
  } else {
    stat->tablet_meta_count_++;
  }
  return ret;
}

int ObSimpleBackupStatMgr::add_bytes(const share::ObBackupDataType &backup_data_type, const int64_t bytes)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObSimpleBackupStat *stat = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat mgr do not init", K(ret));
  } else if (OB_FAIL(get_stat_(backup_data_type, stat))) {
    LOG_WARN("failed to get stat", K(ret), K(backup_data_type));
  } else {
    stat->total_bytes_ += bytes;
  }
  return ret;
}

int ObSimpleBackupStatMgr::get_stat_(const share::ObBackupDataType &backup_data_type, ObSimpleBackupStat *&stat)
{
  int ret = OB_SUCCESS;
  stat = NULL;
  int64_t idx = 0;
  if (OB_FAIL(get_idx_(backup_data_type, idx))) {
    LOG_WARN("failed to get idx", K(ret), K(backup_data_type));
  } else if (idx < 0 || idx >= STAT_ARRAY_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx is not valid", K(ret), K(idx));
  } else {
    stat = &stat_list_[idx];
  }
  return ret;
}

void ObSimpleBackupStatMgr::print_stat()
{
  for (int64_t i = 0; i < STAT_ARRAY_SIZE; ++i) {
    const ObSimpleBackupStat &stat = stat_list_[i];
    const char *backup_data_event = NULL;
    if (0 == i) {
      backup_data_event = "backup_sys_data";
    } else if (1 == i) {
      backup_data_event = "backup_minor_data";
    } else if (2 == i) {
      backup_data_event = "backup_major_data";
    }
    SERVER_EVENT_ADD("backup",
        backup_data_event,
        "tenant_id",
        tenant_id_,
        "backup_set_id",
        backup_set_desc_.backup_set_id_,
        "ls_id",
        ls_id_.id(),
        "macro_block_count",
        stat.macro_block_count_,
        "meta_count",
        stat.tablet_meta_count_ + stat.sstable_meta_count_,
        "cost_us",
        stat.end_ts_ - stat.start_ts_,
        stat.total_bytes_);
    LOG_INFO("BACKUP STAT", K_(tenant_id), K_(ls_id), K(stat));
  }
}

int ObSimpleBackupStatMgr::get_idx_(const share::ObBackupDataType &backup_data_type, int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (backup_data_type.is_sys_backup()) {
    idx = 0;
  } else if (backup_data_type.is_minor_backup()) {
    idx = 1;
  } else if (backup_data_type.is_major_backup()) {
    idx = 2;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup data type not valid", K(ret), K(backup_data_type));
  }
  return ret;
}

void ObSimpleBackupStatMgr::reset_stat_list_()
{
  for (int64_t i = 0; i < STAT_ARRAY_SIZE; ++i) {
    stat_list_[i].reset();
  }
}

/* ObBackupDataCtx */

ObBackupDataCtx::ObBackupDataCtx()
    : is_inited_(false),
      file_id_(0),
      file_offset_(0),
      param_(),
      backup_data_type_(),
      dev_handle_(NULL),
      io_fd_(),
      file_write_ctx_(),
      macro_index_buffer_node_(),
      meta_index_buffer_node_(),
      file_trailer_(),
      tmp_buffer_("BackupDataCtx")
{}

ObBackupDataCtx::~ObBackupDataCtx()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(dev_handle_) && io_fd_.is_valid()) {
    ObBackupIoAdapter util;
    if (OB_FAIL(util.close_device_and_fd(dev_handle_, io_fd_))) {
      LOG_WARN("fail to close device and fd", K(ret), K_(dev_handle), K_(io_fd));
    }
  }
}

int ObBackupDataCtx::open(const ObLSBackupDataParam &param, const share::ObBackupDataType &backup_data_type,
    const int64_t file_id, common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  static const int64_t BUF_SIZE = 4 * 1024 * 1024;
  static const int64_t index_level = OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL;
  const uint64_t tenant_id = param.tenant_id_;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup data ctx init twice", K(ret));
  } else if (!param.is_valid() || !backup_data_type.is_valid() || file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_data_type), K(file_id));
  } else if (OB_FAIL(macro_index_buffer_node_.init(tenant_id, BACKUP_BLOCK_MACRO_INDEX, index_level))) {
    LOG_WARN("failed to init macro index buffer node", K(ret), K(tenant_id));
  } else if (OB_FAIL(meta_index_buffer_node_.init(tenant_id, BACKUP_BLOCK_META_INDEX, index_level))) {
    LOG_WARN("failed to init meta index buffer node", K(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_buffer_.ensure_space(BUF_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    file_id_ = file_id;
    file_offset_ = 0;
    backup_data_type_ = backup_data_type;
    if (OB_FAIL(prepare_file_write_ctx_(param, backup_data_type, file_id, bandwidth_throttle))) {
      LOG_WARN("failed to prepare file write ctx", K(ret), K(param), K(backup_data_type), K(file_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupDataCtx::write_backup_file_header(const ObBackupFileHeader &file_header)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = DIO_READ_ALIGN_SIZE;
  char header_buf[buf_len] = "";
  ObBufferReader buffer_reader;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data ctx do not init", K(ret));
  } else if (OB_FAIL(file_header.check_valid())) {
    LOG_WARN("file header is not valid", K(ret), K(file_header));
  } else if (OB_FAIL(build_backup_file_header_buffer(file_header, buf_len, header_buf, buffer_reader))) {
    LOG_WARN("failed to build backup file header buffer", K(ret), K(file_header));
  } else if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
    LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
  } else {
    file_offset_ += buf_len;
    LOG_INFO("write backup file header", K(file_header));
  }
  return ret;
}

int ObBackupDataCtx::write_macro_block_data(const blocksstable::ObBufferReader &buffer,
    const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  macro_index.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data ctx do not init", K(ret));
  } else if (!buffer.is_valid() || !logic_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(buffer), K(logic_id));
  } else if (OB_FAIL(write_macro_block_data_(buffer, logic_id, macro_index))) {
    LOG_WARN("failed to write macro block data", K(ret), K(buffer), K(logic_id));
  } else if (OB_FAIL(append_macro_block_index_(macro_index))) {
    LOG_WARN("failed to append macro block index", K(ret), K(macro_index));
  } else {
    LOG_DEBUG("write macro block data", K(macro_index));
  }
  return ret;
}

int ObBackupDataCtx::write_meta_data(const blocksstable::ObBufferReader &meta, const common::ObTabletID &tablet_id,
    const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  meta_index.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data ctx do not init", K(ret));
  } else if (!meta.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta), K(tablet_id));
  } else if (OB_FAIL(write_meta_data_(meta, tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to write macro block data", K(ret), K(meta), K(tablet_id), K(meta_type));
  } else if (OB_FAIL(append_meta_index_(meta_index))) {
    LOG_WARN("failed to append meta index", K(ret), K(meta_index));
  } else {
    LOG_DEBUG("write meta data", K(meta), K(tablet_id));
  }
  return ret;
}

int ObBackupDataCtx::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data ctx do not init", K(ret));
  } else if (OB_FAIL(flush_index_list_())) {
    LOG_WARN("failed to flush index list", K(ret));
  } else if (OB_FAIL(flush_trailer_())) {
    LOG_WARN("failed to flush trailer", K(ret));
  } else if (OB_FAIL(file_write_ctx_.close())) {
    LOG_WARN("failed to close file writer", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dev_handle_->complete(io_fd_))) {
      LOG_WARN("fail to complete multipart upload", K(ret), K_(dev_handle), K_(io_fd));
    }
  } else {
    if (OB_NOT_NULL(dev_handle_) && OB_TMP_FAIL(dev_handle_->abort(io_fd_))) {
      ret = COVER_SUCC(tmp_ret);
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
  return ret;
}

int ObBackupDataCtx::open_file_writer_(const share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  common::ObBackupIoAdapter util;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  if (OB_FAIL(util.mk_parent_dir(backup_path.get_obstr(), param_.backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(backup_path));
  } else if (OB_FAIL(util.open_with_access_type(
                 dev_handle_, io_fd_, param_.backup_dest_.get_storage_info(), backup_path.get_obstr(), access_type))) {
    LOG_WARN("failed to open with access type", K(ret), K(param_), K(backup_path));
  } else {
    LOG_INFO("open file writer", K(ret), K(backup_path));
  }
  return ret;
}

int ObBackupDataCtx::prepare_file_write_ctx_(
    const ObLSBackupDataParam &param, const share::ObBackupDataType &type, const int64_t file_id,
    common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  const int64_t data_file_size = get_data_file_size();
  if (OB_FAIL(get_macro_block_backup_path_(file_id, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(file_id));
  } else if (OB_FAIL(open_file_writer_(backup_path))) {
    LOG_WARN("failed to open file writer", K(ret), K(backup_path));
  } else if (OB_FAIL(file_write_ctx_.open(data_file_size, io_fd_, *dev_handle_, bandwidth_throttle))) {
    LOG_WARN("failed to open file write ctx", K(ret), K(param), K(type), K(backup_path), K(data_file_size), K(file_id));
  }
  return ret;
}

int ObBackupDataCtx::get_macro_block_backup_path_(const int64_t file_id, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  if (OB_UNLIKELY(file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(file_id));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(param_.backup_dest_,
                 param_.backup_set_desc_,
                 param_.ls_id_,
                 backup_data_type_,
                 param_.turn_id_,
                 param_.retry_id_,
                 file_id,
                 backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(param_), K(file_id), K(backup_data_type_));
  }
  return ret;
}

int ObBackupDataCtx::write_macro_block_data_(const blocksstable::ObBufferReader &reader,
    const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  const int64_t alignment = DIO_READ_ALIGN_SIZE;
  const ObBackupBlockType block_type = BACKUP_BLOCK_MACRO_DATA;
  if (!reader.is_valid() || !logic_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(reader), K(logic_id));
  } else if (OB_FAIL(write_data_align_(reader, block_type, alignment))) {
    LOG_WARN("failed to write data align", K(ret), K(reader));
  } else {
    int64_t length = 0;
    ObBufferReader buffer_reader(tmp_buffer_.data(), tmp_buffer_.length(), tmp_buffer_.length());
    if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(tmp_buffer_), K(buffer_reader));
    } else if (FALSE_IT(length = tmp_buffer_.pos())) {
    } else if (OB_FAIL(build_macro_block_index_(logic_id, file_offset_, length, macro_index))) {
      LOG_WARN("failed to build macro block index", K(ret), K(logic_id), K(file_offset_), K(length));
    } else {
      ++file_trailer_.macro_block_count_;
      file_offset_ += length;
    }
  }
  return ret;
}

int ObBackupDataCtx::write_meta_data_(const blocksstable::ObBufferReader &reader, const common::ObTabletID &tablet_id,
    const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  const int64_t alignment = DIO_READ_ALIGN_SIZE;
  const ObBackupBlockType block_type = BACKUP_BLOCK_META_DATA;
  if (!reader.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(reader), K(tablet_id));
  } else if (OB_FAIL(write_data_align_(reader, block_type, alignment))) {
    LOG_WARN("failed to write data align", K(ret), K(reader));
  } else {
    int64_t length = 0;
    ObBufferReader buffer_reader(tmp_buffer_.data(), tmp_buffer_.length(), tmp_buffer_.length());
    if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(tmp_buffer_), K(buffer_reader));
    } else if (FALSE_IT(length = tmp_buffer_.pos())) {
    } else if (OB_FAIL(build_meta_index_(tablet_id, meta_type, file_offset_, length, meta_index))) {
      LOG_WARN("failed to build meta index", K(ret), K(tablet_id), K(meta_type), K(file_offset_), K(length));
    } else {
      ++file_trailer_.meta_count_;
      file_offset_ += length;
    }
  }
  return ret;
}

int ObBackupDataCtx::build_macro_block_index_(const blocksstable::ObLogicMacroBlockId &logic_id, const int64_t offset,
    const int64_t length, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  if (!logic_id.is_valid() || offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id), K(offset), K(length));
  } else {
    macro_index.logic_id_ = logic_id;
    macro_index.backup_set_id_ = param_.backup_set_desc_.backup_set_id_;
    macro_index.ls_id_ = param_.ls_id_;
    macro_index.turn_id_ = param_.turn_id_;
    macro_index.retry_id_ = param_.retry_id_;
    macro_index.file_id_ = file_id_;
    macro_index.offset_ = offset;
    macro_index.length_ = length;
  }
  return ret;
}

int ObBackupDataCtx::build_meta_index_(const common::ObTabletID &tablet_id, const ObBackupMetaType &meta_type,
    const int64_t offset, const int64_t length, ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid() || meta_type < BACKUP_SSTABLE_META || meta_type >= BACKUP_META_MAX || offset < 0 ||
      length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id), K(meta_type), K(offset), K(length));
  } else {
    meta_index.meta_key_.tablet_id_ = tablet_id;
    meta_index.meta_key_.meta_type_ = meta_type;
    meta_index.backup_set_id_ = param_.backup_set_desc_.backup_set_id_;
    meta_index.ls_id_ = param_.ls_id_;
    meta_index.turn_id_ = param_.turn_id_;
    meta_index.retry_id_ = param_.retry_id_;
    meta_index.file_id_ = file_id_;
    meta_index.offset_ = offset;
    meta_index.length_ = length;
  }
  return ret;
}

int ObBackupDataCtx::append_macro_block_index_(const ObBackupMacroBlockIndex &macro_index)
{
  return append_index_<ObBackupMacroBlockIndex>(macro_index, macro_index_buffer_node_);
}

int ObBackupDataCtx::append_meta_index_(const ObBackupMetaIndex &meta_index)
{
  return append_index_<ObBackupMetaIndex>(meta_index, meta_index_buffer_node_);
}

template <typename IndexType>
int ObBackupDataCtx::append_index_(const IndexType &index, ObBackupIndexBufferNode &buffer_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!index.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(index));
  } else if (OB_FAIL(buffer_node.put_backup_index(index))) {
    LOG_WARN("failed to put backup index", K(ret), K(index));
  } else {
    LOG_DEBUG("append index", K(index));
  }
  return ret;
}

template <typename IndexType>
int ObBackupDataCtx::encode_index_to_buffer_(
    const common::ObIArray<IndexType> &index_list, ObBufferWriter &buffer_writer)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const IndexType &index = index_list.at(i);
    if (OB_FAIL(buffer_writer.write_serialize(index))) {
      LOG_WARN("failed to write data", K(ret), K(index));
    }
  }
  return ret;
}

int ObBackupDataCtx::flush_index_list_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_macro_block_index_list_())) {
    LOG_WARN("failed to flush macro block index list", K(ret));
  } else if (OB_FAIL(flush_meta_index_list_())) {
    LOG_WARN("failed to flush meta index list", K(ret));
  } else {
    LOG_INFO("flush index list", K_(param), K_(file_id));
  }
  return ret;
}

int ObBackupDataCtx::flush_macro_block_index_list_()
{
  int ret = OB_SUCCESS;
  file_trailer_.macro_index_offset_ = file_offset_;
  ObBackupIndexBufferNode &node = macro_index_buffer_node_;
  ObArray<ObBackupMacroBlockIndex> tmp_index_list;
  ObBackupMacroBlockIndex index;
  while (OB_SUCC(ret) && node.get_read_count() < node.get_write_count()) {
    if (OB_FAIL(node.get_backup_index(index))) {
      LOG_WARN("failed to get backup index", K(ret));
    } else if (OB_FAIL(tmp_index_list.push_back(index))) {
      LOG_WARN("failed to push back", K(ret), K(index));
    } else if (tmp_index_list.count() >= OB_BACKUP_INDEX_BLOCK_NODE_CAPACITY) {
      if (OB_FAIL(write_macro_block_index_list_(tmp_index_list))) {
        LOG_WARN("failed to write macro block index list", K(ret), K(tmp_index_list));
      } else {
        tmp_index_list.reset();
      }
    }
  }
  if (OB_SUCC(ret) && !tmp_index_list.empty()) {
    if (OB_FAIL(write_macro_block_index_list_(tmp_index_list))) {
      LOG_WARN("failed to write macro block index list", K(ret), K(tmp_index_list));
    }
  }
  if (OB_SUCC(ret)) {
    file_trailer_.macro_index_length_ = file_offset_ - file_trailer_.macro_index_offset_;
  }
  return ret;
}

int ObBackupDataCtx::flush_meta_index_list_()
{
  int ret = OB_SUCCESS;
  file_trailer_.meta_index_offset_ = file_offset_;
  ObBackupIndexBufferNode &node = meta_index_buffer_node_;
  ObArray<ObBackupMetaIndex> tmp_index_list;
  ObBackupMetaIndex index;
  while (OB_SUCC(ret) && node.get_read_count() < node.get_write_count()) {
    if (OB_FAIL(node.get_backup_index(index))) {
      LOG_WARN("failed to get backup index", K(ret));
    } else if (OB_FAIL(tmp_index_list.push_back(index))) {
      LOG_WARN("failed to push back", K(ret), K(index));
    } else if (tmp_index_list.count() >= OB_BACKUP_INDEX_BLOCK_NODE_CAPACITY) {
      if (OB_FAIL(write_meta_index_list_(tmp_index_list))) {
        LOG_WARN("failed to write meta index list", K(ret), K(tmp_index_list));
      } else {
        tmp_index_list.reset();
      }
    }
  }
  if (OB_SUCC(ret) && !tmp_index_list.empty()) {
    if (OB_FAIL(write_meta_index_list_(tmp_index_list))) {
      LOG_WARN("failed to write meta index list", K(ret), K(tmp_index_list));
    }
  }
  if (OB_SUCC(ret)) {
    file_trailer_.meta_index_length_ = file_offset_ - file_trailer_.meta_index_offset_;
  }
  return ret;
}

int ObBackupDataCtx::write_macro_block_index_list_(const common::ObIArray<ObBackupMacroBlockIndex> &index_list)
{
  return write_index_list_<ObBackupMacroBlockIndex>(BACKUP_BLOCK_MACRO_INDEX, index_list);
}

int ObBackupDataCtx::write_meta_index_list_(const common::ObIArray<ObBackupMetaIndex> &index_list)
{
  return write_index_list_<ObBackupMetaIndex>(BACKUP_BLOCK_META_INDEX, index_list);
}

template <class IndexType>
int ObBackupDataCtx::write_index_list_(
    const ObBackupBlockType &index_type, const common::ObIArray<IndexType> &index_list)
{
  int ret = OB_SUCCESS;
  tmp_buffer_.reuse();
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  ObBackupCommonHeader *common_header = NULL;
  if (index_list.empty()) {
    // do nothing
  } else if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
    LOG_WARN("advance failed", K(ret), K(header_len));
  } else if (OB_FAIL(encode_index_to_buffer_<IndexType>(index_list, tmp_buffer_))) {
    LOG_WARN("failed to encode index to buffer", K(ret), K(index_list));
  } else if (FALSE_IT(common_header = reinterpret_cast<ObBackupCommonHeader *>(tmp_buffer_.data()))) {
  } else if (OB_FAIL(build_common_header_(index_type, tmp_buffer_.length() - header_len, 0, common_header))) {
    LOG_WARN("failed to build common header", K(ret), K(tmp_buffer_));
  } else if (OB_FAIL(common_header->set_checksum(
                 tmp_buffer_.data() + common_header->header_length_, tmp_buffer_.length() - header_len))) {
    LOG_WARN("failed to set common header checksum", K(ret), K(*common_header));
  } else {
    ObBufferReader buffer_reader(tmp_buffer_.data(), tmp_buffer_.length(), tmp_buffer_.length());
    if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
    } else {
      file_offset_ += buffer_reader.length();
    }
  }
  return ret;
}

int ObBackupDataCtx::build_common_header_(const ObBackupBlockType &block_type, const int64_t data_length,
    const int64_t align_length, ObBackupCommonHeader *&common_header)
{
  return build_common_header(block_type, data_length, align_length, common_header);
}

int ObBackupDataCtx::write_data_align_(
    const blocksstable::ObBufferReader &reader, const ObBackupBlockType &block_type, const int64_t alignment)
{
  int ret = OB_SUCCESS;
  tmp_buffer_.reuse();
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  const int64_t align_size = common::upper_align(header_len + reader.length(), alignment);
  const int64_t align_length = align_size - reader.length() - header_len;
  ObBackupCommonHeader *common_header = NULL;
  if (!reader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(reader));
  } else if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
    LOG_WARN("advance failed", K(ret), K(header_len));
  } else if (OB_FAIL(tmp_buffer_.write(reader.data(), reader.length()))) {
    LOG_WARN("failed to write data", K(ret), K(reader));
  } else if (OB_FAIL(tmp_buffer_.advance_zero(align_length))) {
    LOG_WARN("failed to advance zero", K(ret), K(align_length));
  } else if (FALSE_IT(common_header = reinterpret_cast<ObBackupCommonHeader *>(tmp_buffer_.data()))) {
  } else if (OB_FAIL(build_common_header_(block_type, reader.length(), align_length, common_header))) {
    LOG_WARN("failed to build common header", K(ret), K(block_type), K(reader), K(align_length));
  } else if (OB_FAIL(common_header->set_checksum(tmp_buffer_.data() + common_header->header_length_, reader.length()))) {
    LOG_WARN("failed to set common header checksum", K(ret), K(tmp_buffer_), K(reader), K(*common_header));
  }
  return ret;
}

int ObBackupDataCtx::check_trailer_(const ObBackupDataFileTrailer &trailer)
{
  int ret = OB_SUCCESS;
  if (0 != trailer.macro_block_count_ && 0 == trailer.macro_index_length_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trailer is not valid", K(ret), K(trailer));
  } else if (0 != trailer.meta_count_ && 0 == trailer.meta_index_length_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trailer is not valid", K(ret), K(trailer));
  }
  return ret;
}

int ObBackupDataCtx::flush_trailer_()
{
  int ret = OB_SUCCESS;
  tmp_buffer_.reuse();
  char *trailer_buf = tmp_buffer_.data();
  ObBackupDataFileTrailer *file_trailer = NULL;
  const int64_t trailer_len = sizeof(ObBackupDataFileTrailer);
  file_trailer = reinterpret_cast<ObBackupDataFileTrailer *>(trailer_buf);
  if (OB_FAIL(tmp_buffer_.advance_zero(trailer_len))) {
    LOG_WARN("advance failed", K(ret), K(trailer_len));
  } else if (OB_FAIL(check_trailer_(file_trailer_))) {
    LOG_WARN("failed to check trailer", K(ret), K_(file_trailer));
  } else if (FALSE_IT(*file_trailer = file_trailer_)) {
  } else {
    file_trailer->data_type_ = 0;
    file_trailer->data_version_ = 0;
    file_trailer->data_accumulate_checksum_ = 0;
    file_trailer->offset_ = file_offset_;
    file_trailer->length_ = trailer_len;
    file_trailer->set_trailer_checksum();
    ObBufferReader buffer_reader(tmp_buffer_.data(), tmp_buffer_.length(), tmp_buffer_.length());
    if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
    } else {
      FLOG_INFO("flush data file trailer", K(*file_trailer));
    }
  }
  return ret;
}

/* ObBackupRecoveryRetryCtx */

ObBackupRecoverRetryCtx::ObBackupRecoverRetryCtx()
    : has_need_skip_tablet_id_(false),
      has_need_skip_logic_id_(false),
      need_skip_tablet_id_(),
      need_skip_logic_id_(),
      reused_pair_list_()
{}

ObBackupRecoverRetryCtx::~ObBackupRecoverRetryCtx()
{}

void ObBackupRecoverRetryCtx::reuse()
{
  reset();
}

void ObBackupRecoverRetryCtx::reset()
{
  has_need_skip_tablet_id_ = false;
  has_need_skip_logic_id_ = false;
  need_skip_tablet_id_.reset();
  need_skip_logic_id_.reset();
  reused_pair_list_.reset();
}

/* ObLSBackupCtx */

ObLSBackupCtx::ObLSBackupCtx()
    : is_inited_(),
      mutex_(common::ObLatchIds::BACKUP_LOCK),
      cond_(),
      is_finished_(false),
      result_code_(OB_SUCCESS),
      max_file_id_(),
      prefetch_task_id_(),
      finished_file_id_(),
      param_(),
      backup_stat_(),
      task_idx_(),
      tablet_stat_(),
      tablet_holder_(),
      stat_mgr_(),
      sys_tablet_id_list_(),
      data_tablet_id_list_(),
      backup_retry_ctx_(),
      sql_proxy_(NULL),
      rebuild_seq_(),
      check_tablet_info_cost_time_(),
      backup_tx_table_filled_tx_scn_(share::SCN::min_scn()),
      tablet_checker_(),
      bandwidth_throttle_(NULL)
{}

ObLSBackupCtx::~ObLSBackupCtx()
{
  cond_.destroy();
}

int ObLSBackupCtx::open(
    const ObLSBackupParam &param, const share::ObBackupDataType &backup_data_type,
    common::ObMySQLProxy &sql_proxy, ObBackupIndexKVCache &index_kv_cache, common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObTabletID> tablet_list;
  ObILSTabletIdReader *reader = NULL;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!param.is_valid() || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_data_type));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
    LOG_WARN("failed to init condition", K(ret));
  } else if (OB_FAIL(tablet_stat_.init(
                 param.tenant_id_, param.backup_set_desc_.backup_set_id_, param.ls_id_, backup_data_type))) {
    LOG_WARN("failed to init tablet stat", K(ret), K(param), K(backup_data_type));
  } else if (OB_FAIL(tablet_holder_.init(param.tenant_id_, param.ls_id_))) {
    LOG_WARN("failed to init tablet holder", K(ret), K(param));
  } else if (OB_FAIL(stat_mgr_.init(param.backup_dest_, param.backup_set_desc_, param.tenant_id_, param.ls_id_))) {
    LOG_WARN("failed to init stat", K(ret));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(tablet_checker_.init(param, backup_data_type, sql_proxy, index_kv_cache))) {
    LOG_WARN("failed to init tablet checker", K(ret), K(param), K(backup_data_type));
  } else {
    max_file_id_ = 0;
    prefetch_task_id_ = 0;
    finished_file_id_ = 0;
    backup_data_type_ = backup_data_type;
    task_idx_ = 0;
    sql_proxy_ = &sql_proxy;
    rebuild_seq_ = 0;
    check_tablet_info_cost_time_ = 0;
    bandwidth_throttle_ = &bandwidth_throttle;
    is_inited_ = true;
    if (OB_FAIL(prepare_tablet_id_reader_(reader))) {
      LOG_WARN("failed to prepare tablet id reader", K(ret), K(param));
    } else if (OB_FAIL(get_all_tablet_id_list_(reader, tablet_list))) {
      LOG_WARN("failed to get all tablet id list", K(ret));
    } else if (tablet_list.empty()) {
      ret = OB_NO_TABLET_NEED_BACKUP;
      LOG_WARN("no tablet in log stream need to backup", K(ret), K(param));
    } else if (OB_FAIL(seperate_tablet_id_list_(tablet_list, sys_tablet_id_list_, data_tablet_id_list_))) {
      LOG_WARN("failed to seperate tablet id list", K(ret), K(tablet_list));
    } else if (OB_FAIL(recover_last_retry_ctx_())) {
      LOG_WARN("failed to recover last retry ctx", K(ret));
    }
  }
  if (OB_NOT_NULL(reader)) {
    ObLSBackupFactory::free(reader);
  }
  return ret;
}

int ObLSBackupCtx::next(common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx do not init", K(ret));
  } else if (OB_FAIL(inner_do_next_(tablet_id))) {
    LOG_WARN("failed to inner do next", K(ret));
  } else {
    LOG_INFO("get next tablet", K(tablet_id));
  }
  return ret;
}

void ObLSBackupCtx::set_backup_data_type(const share::ObBackupDataType &backup_data_type)
{
  ObMutexGuard guard(mutex_);
  backup_data_type_ = backup_data_type;
  tablet_stat_.set_backup_data_type(backup_data_type);
};

void ObLSBackupCtx::reset()
{
  ObMutexGuard guard(mutex_);
  task_idx_ = 0;
  max_file_id_ = 0;
  prefetch_task_id_ = 0;
  finished_file_id_ = 0;
  tablet_stat_.reset();
  sys_tablet_id_list_.reset();
  data_tablet_id_list_.reset();
  backup_retry_ctx_.reset();
  rebuild_seq_ = 0;
  check_tablet_info_cost_time_ = 0;
}

void ObLSBackupCtx::reuse()
{
  ObMutexGuard guard(mutex_);
  task_idx_ = 0;
  max_file_id_ = 0;
  prefetch_task_id_ = 0;
  finished_file_id_ = 0;
  tablet_stat_.reuse();
  backup_retry_ctx_.reuse();
  rebuild_seq_ = 0;
  check_tablet_info_cost_time_ = 0;
}

int ObLSBackupCtx::set_tablet(const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *tablet_handle)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (!tablet_id.is_valid() || OB_ISNULL(tablet_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id), KP(tablet_handle));
  } else if (OB_FAIL(tablet_holder_.set_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to hold tablet", K(ret), K(tablet_id), KPC(tablet_handle));
  } else {
    LOG_DEBUG("backup set tablet", K(tablet_id), KP(tablet_handle));
  }
  return ret;
}

int ObLSBackupCtx::get_tablet(const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *&tablet_handle)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_holder_.get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else {
    LOG_DEBUG("backup get tablet", K(tablet_id), KP(tablet_handle));
  }
  return ret;
}

int ObLSBackupCtx::release_tablet(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_holder_.release_tablet(tablet_id))) {
    LOG_WARN("failed to release tablet", K(ret), K(tablet_id));
  } else {
    LOG_DEBUG("release tablet", K(tablet_id));
  }
  return ret;
}

void ObLSBackupCtx::set_result_code(const int64_t result, bool &is_set)
{
  ObMutexGuard guard(mutex_);
  // TODO(yangyi.yyy): change the report result logic to one place in 4.1
  if (OB_SUCCESS == result_code_ && OB_SUCCESS != result) {
    result_code_ = result;
    is_finished_ = true;
    is_set = true;
  } else {
    is_set = false;
  }
  LOG_INFO("set result code", K(result), K(result_code_), K(is_set));
}

int64_t ObLSBackupCtx::get_result_code() const
{
  ObMutexGuard guard(mutex_);
  return result_code_;
}

void ObLSBackupCtx::set_finished()
{
  ObMutexGuard guard(mutex_);
  is_finished_ = true;
}

bool ObLSBackupCtx::is_finished() const
{
  ObMutexGuard guard(mutex_);
  return is_finished_;
}

int ObLSBackupCtx::close()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  return ret;
}

int ObLSBackupCtx::get_max_file_id(int64_t &max_file_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  max_file_id = max_file_id_;
  return ret;
}

int ObLSBackupCtx::set_max_file_id(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  max_file_id_ = std::max(file_id, max_file_id_);
  return ret;
}

int ObLSBackupCtx::wait_task(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  static const int64_t DEFAULT_WAIT_TIME = 10 * 1000 * 1000;  // 10s
  while (OB_SUCC(ret) && file_id != ATOMIC_LOAD(&finished_file_id_)) {
    ObThreadCondGuard guard(cond_);
    if (OB_SUCCESS != result_code_) {
      LOG_INFO("ls backup ctx already failed", K(result_code_), K(file_id), K(finished_file_id_));
      break;
    } else if (OB_FAIL(cond_.wait_us(DEFAULT_WAIT_TIME))) {
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("waiting for task too long", K(file_id), K(finished_file_id_));
      }
    }
  }
  return ret;
}

int ObLSBackupCtx::finish_task(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != result_code_) {
    LOG_INFO("already failed, do nothing for finish task", K(ret));
  } else if (OB_UNLIKELY(file_id != ATOMIC_LOAD(&finished_file_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish task order unexpected", K(ret), K(file_id), K_(max_file_id));
  } else {
    ObThreadCondGuard guard(cond_);
    ATOMIC_INC(&finished_file_id_);
    if (OB_FAIL(cond_.broadcast())) {
      LOG_WARN("failed to broadcast condition", K(ret));
    } else {
      LOG_INFO("finish task", K(file_id), K_(finished_file_id));
    }
  }
  return ret;
}

int ObLSBackupCtx::recover_last_retry_ctx_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupRetryDesc> retry_list;
  const int64_t cur_turn_id = param_.turn_id_;
  const int64_t cur_retry_id = param_.retry_id_;
  if (OB_FAIL(get_all_retries_of_turn_(param_.turn_id_, retry_list))) {
    LOG_WARN("failed to get all retries of turn", K_(param));
  } else if (OB_FAIL(check_and_sort_retry_list_(cur_turn_id, cur_retry_id, retry_list))) {
    LOG_WARN("failed to check and sort retry list", K(ret), K_(param));
  } else if (retry_list.empty()) {
    LOG_INFO("no need recover ctx", K_(param));
  } else {
    if (backup_data_type_.is_minor_backup()) {
      if (OB_FAIL(recover_last_minor_retry_ctx_(retry_list))) {
        LOG_WARN("failed to recover last minor retry ctx", K(ret), K(retry_list));
      } else {
        LOG_INFO("recover last minor retry ctx", K(retry_list));
      }
    } else if (backup_data_type_.is_major_backup()) {
      if (OB_FAIL(recover_last_major_retry_ctx_(retry_list))) {
        LOG_WARN("failed to recover last major retry ctx", K(ret), K(retry_list));
      } else {
        LOG_INFO("recover last major retry ctx", K(retry_list));
      }
    }
  }
  return ret;
}

int ObLSBackupCtx::check_and_sort_retry_list_(
    const int64_t cur_turn_id, const int64_t cur_retry_id, common::ObArray<ObBackupRetryDesc> &retry_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < retry_list.count(); ++i) {
    const ObBackupRetryDesc &tmp_retry = retry_list.at(i);
    if (tmp_retry.turn_id_ != cur_turn_id || tmp_retry.retry_id_ >= cur_retry_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("retry not valid", K(ret), K(retry_list), K(tmp_retry), K(cur_turn_id), K(cur_retry_id));
    }
  }
  if (OB_SUCC(ret)) {
    BackupRetryCmp cmp;
    lib::ob_sort(retry_list.begin(), retry_list.end(), cmp);
  }
  return ret;
}

int ObLSBackupCtx::recover_last_minor_retry_ctx_(common::ObArray<ObBackupRetryDesc> &retry_list)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = retry_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObBackupRetryDesc &retry_desc = retry_list.at(i);
    if (OB_FAIL(inner_recover_last_minor_retry_ctx_(retry_desc, found))) {
      LOG_WARN("failed to inner recover last minor retry ctx", K(ret), K(retry_desc));
    } else if (found) {
      break;
    }
  }
  return ret;
}

int ObLSBackupCtx::recover_last_major_retry_ctx_(common::ObArray<ObBackupRetryDesc> &retry_list)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = retry_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObBackupRetryDesc &retry_desc = retry_list.at(i);
    if (OB_FAIL(inner_recover_last_major_retry_ctx_(retry_desc, found))) {
      LOG_WARN("failed to inner recover last major retry ctx", K(ret), K(retry_desc));
    } else if (found) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(recover_need_reuse_macro_block_(retry_list))) {
      LOG_WARN("failed to recover need reuse macro block", K(ret), K(retry_list));
    } else {
      LOG_INFO("recover need reuse macro block", K(retry_list), K(backup_retry_ctx_));
    }
  }
  return ret;
}

int ObLSBackupCtx::inner_recover_last_minor_retry_ctx_(const ObBackupRetryDesc &retry_desc, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (OB_FAIL(get_last_persist_tablet_meta_(retry_desc, found))) {
    LOG_WARN("failed to get last persist tablet meta", K(ret), K(retry_desc));
  } else if (!found) {
    // do nothing
  } else {
    add_recover_retry_ctx_event_(retry_desc);
  }
  return ret;
}

int ObLSBackupCtx::inner_recover_last_major_retry_ctx_(const ObBackupRetryDesc &retry_desc, bool &found)
{
  int ret = OB_SUCCESS;
  bool found_tablet_meta = false;
  bool found_macro_block = false;
  if (OB_FAIL(get_last_persist_tablet_meta_(retry_desc, found_tablet_meta))) {
    LOG_WARN("failed to get last persist tablet meta", K(ret), K(retry_desc));
  } else if (OB_FAIL(get_last_persist_macro_block_(retry_desc, found_macro_block))) {
    LOG_WARN("failed to get last persist macro block", K(ret), K(retry_desc));
  } else {
    found = found_tablet_meta || found_macro_block;
    if (found) {
      add_recover_retry_ctx_event_(retry_desc);
    }
  }
  return ret;
}

int ObLSBackupCtx::get_last_persist_macro_block_(const ObBackupRetryDesc &retry_desc, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  ObBackupMacroBlockIndexIterator iter;
  if (OB_FAIL(iter.init(param_.task_id_,
          param_.backup_dest_,
          param_.tenant_id_,
          param_.backup_set_desc_,
          param_.ls_id_,
          backup_data_type_,
          retry_desc.turn_id_,
          retry_desc.retry_id_,
          true/*need_read_inner_table*/))) {
    LOG_WARN("failed to init iterator", K(ret), K_(param));
  } else {
    ObArray<ObBackupMacroBlockIndex> index_list;
    ObArray<ObBackupIndexBlockDesc> block_desc_list;
    const ObArray<int64_t> &file_id_list = iter.file_id_list_;
    const int64_t file_count = file_id_list.count();
    for (int64_t i = file_count - 1; OB_SUCC(ret) && i >= 0; --i) {
      index_list.reset();
      block_desc_list.reset();
      const int64_t file_id = file_id_list.at(i);
      if (OB_FAIL(iter.fetch_macro_index_list_(file_id, index_list, block_desc_list))) {
        LOG_WARN("failed to fetch macro index list", K(ret), K(file_id));
      } else if (index_list.empty()) {
        continue;
      } else {
        backup_retry_ctx_.need_skip_logic_id_ = index_list.at(index_list.count() - 1).logic_id_;
        backup_retry_ctx_.has_need_skip_logic_id_ = true;
        found = true;
        break;
      }
    }
  }
  return ret;
}

int ObLSBackupCtx::get_last_persist_tablet_meta_(const ObBackupRetryDesc &retry_desc, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  ObBackupMetaIndexIterator iter;
  if (!retry_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(retry_desc));
  } else if (OB_FAIL(iter.init(param_.task_id_,
                 param_.backup_dest_,
                 param_.tenant_id_,
                 param_.backup_set_desc_,
                 param_.ls_id_,
                 backup_data_type_,
                 retry_desc.turn_id_,
                 retry_desc.retry_id_,
                 false /*is_sec_meta*/))) {
    LOG_WARN("failed to init meta index iterator", K(ret), K_(param));
  } else {
    ObArray<ObBackupMetaIndex> index_list;
    ObArray<ObBackupIndexBlockDesc> block_desc_list;
    const ObArray<int64_t> &file_id_list = iter.file_id_list_;
    const int64_t file_count = file_id_list.count();
    for (int64_t i = file_count - 1; OB_SUCC(ret) && i >= 0; --i) {
      index_list.reset();
      block_desc_list.reset();
      const int64_t file_id = file_id_list.at(i);
      common::ObTabletID need_skip_tablet_id;
      if (OB_FAIL(iter.fetch_meta_index_list_(file_id, index_list, block_desc_list))) {
        LOG_WARN("failed to fetch meta index list", K(ret), K(file_id));
      } else if (OB_FAIL(inner_get_last_persist_tablet_meta_(index_list, need_skip_tablet_id, found))) {
        LOG_WARN("failed to inner get last persist tablet meta", K(ret), K(index_list));
      } else if (!found) {
        continue;
      } else {
        backup_retry_ctx_.need_skip_tablet_id_ = need_skip_tablet_id;
        backup_retry_ctx_.has_need_skip_tablet_id_ = true;
        break;
      }
    }
  }
  return ret;
}

int ObLSBackupCtx::inner_get_last_persist_tablet_meta_(
    const common::ObIArray<ObBackupMetaIndex> &meta_index_list, common::ObTabletID &tablet_id, bool &found)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  found = false;
  for (int64_t i = meta_index_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObBackupMetaIndex &meta_index = meta_index_list.at(i);
    if (meta_index.meta_key_.meta_type_ == BACKUP_TABLET_META) {
      tablet_id = meta_index.meta_key_.tablet_id_;
      found = true;
      break;
    }
  }
  return ret;
}

int ObLSBackupCtx::get_all_retries_of_turn_(const int64_t turn_id, common::ObIArray<ObBackupRetryDesc> &retry_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupRetryDesc> tmp_retry_list;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy should not be null", K(ret));
  } else if (turn_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(turn_id));
  } else if (OB_FAIL(ObLSBackupOperator::get_all_retries(
                 param_.task_id_, param_.tenant_id_, backup_data_type_, param_.ls_id_, tmp_retry_list, *sql_proxy_))) {
    LOG_WARN("failed to get all retries", K(ret), K_(param), K_(backup_data_type));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_retry_list.count(); ++i) {
      const ObBackupRetryDesc &retry_desc = tmp_retry_list.at(i);
      if (turn_id == retry_desc.turn_id_) {
        if (OB_FAIL(retry_list.push_back(retry_desc))) {
          LOG_WARN("failed to push back", K(ret), K(retry_desc));
        }
      }
    }
  }
  return ret;
}

int ObLSBackupCtx::recover_need_reuse_macro_block_(const common::ObIArray<ObBackupRetryDesc> &retry_list)
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  for (int64_t i = retry_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObBackupRetryDesc &retry_desc = retry_list.at(i);
    if (OB_FAIL(inner_recover_need_reuse_macro_block_(retry_desc, is_end))) {
      LOG_WARN("failed to inner recover last major retry ctx", K(ret), K(retry_desc));
    } else if (is_end) {
      break;
    }
  }
  return ret;
}

int ObLSBackupCtx::inner_recover_need_reuse_macro_block_(const ObBackupRetryDesc &retry_desc, bool &is_end)
{
  int ret = OB_SUCCESS;
  is_end = false;
  ObBackupMacroBlockIndexIterator iter;
  const blocksstable::ObLogicMacroBlockId &need_reuse_logic_id = backup_retry_ctx_.need_skip_logic_id_;
  if (OB_FAIL(iter.init(param_.task_id_,
          param_.backup_dest_,
          param_.tenant_id_,
          param_.backup_set_desc_,
          param_.ls_id_,
          backup_data_type_,
          retry_desc.turn_id_,
          retry_desc.retry_id_,
          true/*need_read_inner_table*/))) {
    LOG_WARN("failed to init iterator", K(ret), K_(param));
  } else {
    ObArray<ObBackupMacroBlockIndex> index_list;
    ObArray<ObBackupIndexBlockDesc> block_desc_list;
    const ObArray<int64_t> &file_id_list = iter.file_id_list_;
    const int64_t file_count = file_id_list.count();
    for (int64_t i = file_count - 1; OB_SUCC(ret) && i >= 0; --i) {
      index_list.reset();
      block_desc_list.reset();
      const int64_t file_id = file_id_list.at(i);
      if (OB_FAIL(iter.fetch_macro_index_list_(file_id, index_list, block_desc_list))) {
        LOG_WARN("failed to fetch macro index list", K(ret), K(file_id));
      } else if (index_list.empty()) {
        continue;
      } else {
        const int64_t index_count = index_list.count();
        for (int64_t j = index_count - 1; OB_SUCC(ret) && j >= 0; --j) {
          const ObBackupMacroBlockIndex &tmp_index = index_list.at(j);
          ObBackupMacroBlockIDPair id_pair;
          id_pair.logic_id_ = tmp_index.logic_id_;
          if (tmp_index.logic_id_.tablet_id_ != need_reuse_logic_id.tablet_id_) {
            is_end = true;
            break;
          } else if (OB_FAIL(tmp_index.get_backup_physical_id(id_pair.physical_id_))) {
            LOG_WARN("failed to get backup physical id", K(ret), K(tmp_index));
          } else if (OB_FAIL(backup_retry_ctx_.reused_pair_list_.push_back(id_pair))) {
            LOG_WARN("failed to push back", K(ret), K(id_pair));
          }
        }
      }
      if (is_end) {
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(backup_retry_ctx_.reused_pair_list_.begin(), backup_retry_ctx_.reused_pair_list_.end());
  }
  return ret;
}

int ObLSBackupCtx::prepare_tablet_id_reader_(ObILSTabletIdReader *&reader)
{
  int ret = OB_SUCCESS;
  ObILSTabletIdReader *tmp_reader = NULL;
  const share::ObBackupDest &backup_dest = param_.backup_dest_;
  const uint64_t tenant_id = param_.tenant_id_;
  const share::ObBackupSetDesc &backup_set_desc = param_.backup_set_desc_;
  const share::ObLSID &ls_id = param_.ls_id_;
  const ObLSTabletIdReaderType type = LS_TABLET_ID_READER;
  if (OB_ISNULL(tmp_reader = ObLSBackupFactory::get_ls_tablet_id_reader(type, tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get ls tablet id reader", K(ret), K(type));
  } else if (OB_FAIL(tmp_reader->init(backup_dest, tenant_id, backup_set_desc, ls_id))) {
    LOG_WARN("failed to init tablet id reader", K(ret), K(param_));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }
  if (OB_NOT_NULL(tmp_reader)) {
    ObLSBackupFactory::free(tmp_reader);
  }
  return ret;
}

int ObLSBackupCtx::get_all_tablet_id_list_(
    ObILSTabletIdReader *reader, common::ObIArray<common::ObTabletID> &tablet_list)
{
  int ret = OB_SUCCESS;
  const int64_t turn_id = param_.turn_id_;
  const share::ObLSID &ls_id = param_.ls_id_;
  ObArray<common::ObTabletID> tablet_id_list;
  if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream tablet id reader should not be null", K(ret));
  } else if (OB_FAIL(reader->get_tablet_id_list(backup_data_type_, turn_id, ls_id, tablet_list))) {
    LOG_WARN("failed to get all tablet id list", K(ret), K_(param));
  } else {
    LOG_INFO("get all tablet id list", K_(param), K(tablet_list));
  }
  return ret;
}

int ObLSBackupCtx::seperate_tablet_id_list_(const common::ObIArray<common::ObTabletID> &tablet_id_list,
    common::ObIArray<common::ObTabletID> &sys_tablet_id_list,
    common::ObIArray<common::ObTabletID> &data_tablet_id_list)
{
  int ret = OB_SUCCESS;
  sys_tablet_id_list.reset();
  data_tablet_id_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_list.count(); ++i) {
    const common::ObTabletID &tablet_id = tablet_id_list.at(i);
    if (tablet_id.is_ls_inner_tablet()) {
      if (OB_FAIL(sys_tablet_id_list.push_back(tablet_id))) {
        LOG_WARN("failed to push back", K(ret), K(tablet_id));
      }
    } else {
      if (OB_FAIL(data_tablet_id_list.push_back(tablet_id))) {
        LOG_WARN("failed to push back", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObLSBackupCtx::inner_do_next_(common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  bool need_skip = false;
  while (OB_SUCC(ret)) {
    tablet_id.reset();
    if (OB_FAIL(get_next_tablet_(tablet_id))) {
      LOG_WARN("failed to get next tablet id", K(ret));
    } else if (OB_FAIL(check_need_skip_(tablet_id, need_skip))) {
      LOG_WARN("failed to check need skip", K(ret), K(tablet_id));
    } else if (!need_skip) {
      break;
    }
  }
  return ret;
}

int ObLSBackupCtx::check_need_skip_(const common::ObTabletID &tablet_id, bool &need_skip)
{
  int ret = OB_SUCCESS;
  if (backup_data_type_.is_minor_backup()) {
    if (OB_FAIL(check_need_skip_minor_(tablet_id, need_skip))) {
      LOG_WARN("failed to check need skip minor", K(ret), K(tablet_id));
    }
  } else if (backup_data_type_.is_major_backup()) {
    if (OB_FAIL(check_need_skip_major_(tablet_id, need_skip))) {
      LOG_WARN("failed to check need skip major", K(ret), K(tablet_id));
    }
  }
  LOG_INFO("tablet need skip", K_(backup_data_type), K(need_skip), K(tablet_id), K_(backup_retry_ctx));
  return ret;
}

int ObLSBackupCtx::check_need_skip_minor_(const common::ObTabletID &tablet_id, bool &need_skip)
{
  int ret = OB_SUCCESS;
  need_skip = false;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (backup_retry_ctx_.has_need_skip_tablet_id_) {
    need_skip = tablet_id.id() <= backup_retry_ctx_.need_skip_tablet_id_.id();
  }
  return ret;
}

int ObLSBackupCtx::check_need_skip_major_(const common::ObTabletID &tablet_id, bool &need_skip)
{
  int ret = OB_SUCCESS;
  need_skip = false;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (backup_retry_ctx_.has_need_skip_logic_id_) {
    need_skip = tablet_id.id() < backup_retry_ctx_.need_skip_logic_id_.tablet_id_;
  } else if (backup_retry_ctx_.has_need_skip_tablet_id_) {
    need_skip = tablet_id.id() <= backup_retry_ctx_.need_skip_tablet_id_.id();
  }
  return ret;
}

int ObLSBackupCtx::get_next_tablet_(common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObTabletID> *list_ptr = backup_data_type_.is_sys_backup() ? &sys_tablet_id_list_ : &data_tablet_id_list_;
  if (OB_ISNULL(list_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("list ptr should not be null", K(ret));
  } else if (task_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task idx should not be null", K(ret), K(task_idx_));
  } else if (list_ptr->empty()) {
    if (backup_data_type_.is_sys_backup()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys tablet list should not be empty", K(ret));
    } else {
      ret = OB_ITER_END;
      LOG_WARN("tablet is empty");
    }
  } else if (task_idx_ >= list_ptr->count()) {
    ret = OB_ITER_END;
    LOG_WARN("meet end", K(ret), K(task_idx_));
  } else {
    tablet_id = list_ptr->at(task_idx_);
    LOG_INFO("get next tablet", K(tablet_id), K(task_idx_));
    task_idx_++;
  }
  return ret;
}

void ObLSBackupCtx::add_recover_retry_ctx_event_(const ObBackupRetryDesc &retry_desc)
{
  LOG_INFO("recover last retry ctx", K_(param), K(retry_desc));
  SERVER_EVENT_ADD("backup",
      "recover_retry_ctx",
      "tenant_id",
      param_.tenant_id_,
      "backup_set_id",
      param_.backup_set_desc_.backup_set_id_,
      "ls_id",
      param_.ls_id_.id(),
      "turn_id",
      retry_desc.turn_id_,
      "retry_id",
      retry_desc.retry_id_,
      "backup_data_type",
      backup_data_type_,
      to_cstring(backup_retry_ctx_));
}

}  // namespace backup
}  // namespace oceanbase
