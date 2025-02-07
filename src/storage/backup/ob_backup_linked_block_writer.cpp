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

#include "storage/backup/ob_backup_linked_block_writer.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace backup
{

// ObBackupLinkedBlockWriter

ObBackupLinkedBlockWriter::ObBackupLinkedBlockWriter()
  : is_inited_(),
    file_offset_(NULL),
    write_ctx_(NULL),
    current_block_addr_()
{
}

ObBackupLinkedBlockWriter::~ObBackupLinkedBlockWriter()
{
}

int ObBackupLinkedBlockWriter::init(const ObBackupLinkedBlockAddr &entry_block_addr,
    ObBackupFileWriteCtx &write_ctx, int64_t &file_offset)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupLinkedBlockWriter init twice", K(ret));
  } else if (!entry_block_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(entry_block_addr));
  } else if (!write_ctx.is_opened()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file write ctx not opened", K(ret));
  } else {
    file_offset_ = &file_offset;
    write_ctx_ = &write_ctx;
    current_block_addr_ = entry_block_addr;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLinkedBlockWriter::write_block(
    const blocksstable::ObBufferReader &buffer_reader,
    ObBackupLinkedBlockAddr &block_addr)
{
  int ret = OB_SUCCESS;
  block_addr.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupLinkedBlockWriter is not inited", K(ret));
  } else if (!buffer_reader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(buffer_reader));
  } else if (OB_FAIL(flush_block_to_dest_(buffer_reader))) {
    LOG_WARN("failed to flush block to dest", K(ret), K(buffer_reader));
  } else {
    block_addr = current_block_addr_;
    if (OB_FAIL(calc_next_block_addr_())) {
      LOG_WARN("failed to calc next block id", K(ret));
    }
  }
  return ret;
}

int ObBackupLinkedBlockWriter::flush_block_to_dest_(const blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_ctx_) || OB_ISNULL(file_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file write ctx should not be null", K(ret), KP_(write_ctx), KP_(file_offset));
  } else if (OB_FAIL(write_ctx_->append_buffer(buffer_reader))) {
    LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
  } else {
    *file_offset_ += buffer_reader.length();
  }
  return ret;
}

int ObBackupLinkedBlockWriter::calc_next_block_addr_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file offset should not be null", K(ret), KP_(file_offset));
  } else {
    current_block_addr_.offset_ = *file_offset_ / DIO_READ_ALIGN_SIZE;
  }
  return ret;
}

// ObBackupLinkedBlockItemWriter

ObBackupLinkedBlockItemWriter::ObBackupLinkedBlockItemWriter()
  : is_inited_(false),
    is_closed_(false),
    tablet_id_(),
    table_key_(),
    buf_size_(0),
    prev_block_addr_(),
    block_writer_(),
    common_header_(),
    linked_header_(),
    buffer_writer_("BackupLinkWriter"),
    cur_block_write_item_cnt_(0),
    total_block_write_item_cnt_(0),
    total_block_cnt_(0),
    write_ctx_(NULL),
    has_prev_(false)
{
}

ObBackupLinkedBlockItemWriter::~ObBackupLinkedBlockItemWriter()
{
}

int ObBackupLinkedBlockItemWriter::init(const ObLSBackupDataParam &param,
    const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
    const int64_t file_id, ObBackupFileWriteCtx &write_ctx, int64_t &file_offset)
{
  int ret = OB_SUCCESS;
  ObBackupLinkedBlockAddr entry_block_addr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup linked block item writer do not init", K(ret));
  } else if (!param.is_valid() || !tablet_id.is_valid() || !table_key.is_valid() || file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(tablet_id), K(table_key), K(file_id));
  } else if (OB_FAIL(buffer_writer_.ensure_space(BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(get_entry_block_addr_(param, file_id, file_offset, entry_block_addr))) {
    LOG_WARN("failed to get initial block id", K(ret), K(param), K(entry_block_addr));
  } else if (OB_FAIL(block_writer_.init(entry_block_addr, write_ctx, file_offset))) {
    LOG_WARN("failed to init block writer", K(ret), K(entry_block_addr));
  } else {
    tablet_id_ = tablet_id;
    table_key_ = table_key;
    buf_size_ = BLOCK_SIZE;
    cur_block_write_item_cnt_ = 0;
    total_block_write_item_cnt_ = 0;
    prev_block_addr_.reset();
    has_prev_ = false;
    write_ctx_ = &write_ctx;
    is_closed_ = false;
    is_inited_ = true;
    LOG_INFO("init backup linked item writer", K(tablet_id), K(table_key), K(file_id));
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::write(const ObBackupLinkedItem &link_item)
{
  int ret = OB_SUCCESS;
  bool remain_enough = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupLinkedBlockItemWriter is not inited", K(ret));
  } else if (!link_item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(link_item));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObBackupLinkedBlockItemWriter has been closed", K(ret));
  } else if (OB_FAIL(check_remain_size_enough_(link_item, remain_enough))) {
    LOG_WARN("failed to check remain size enough", K(ret), K(link_item));
  } else if (!remain_enough && OB_FAIL(close_and_write_block_())) {
    LOG_WARN("failed to close and write block", K(ret), K(link_item));
  } else if (OB_FAIL(write_single_item_(link_item))) {
    LOG_WARN("failed to append item", K(ret), K(link_item));
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupLinkedBlockItemWriter is not inited", K(ret));
  } else if (OB_ISNULL(write_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write ctx should not be null", K(ret));
  } else if (OB_FAIL(close_and_write_block_())) {
    LOG_WARN("failed to close and write block", K(ret));
  } else {
    is_closed_ = true;
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::get_root_block_id(ObBackupLinkedBlockAddr &block_addr, int64_t &total_block_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupLinkedBlockItemWriter is not inited", K(ret));
  } else if (!is_closed_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not closed before access", K(ret));
  } else {
    block_addr = prev_block_addr_;
    total_block_count = total_block_cnt_;
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::get_entry_block_addr_(
    const ObLSBackupDataParam &param, const int64_t file_id,
    const int64_t file_offset, ObBackupLinkedBlockAddr &block_addr)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid() || file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(file_id));
  } else if (!is_aligned(file_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file offset is not aligned", K(ret), K(file_offset));
  } else {
    ObBackupDataType backup_data_type;
    backup_data_type.set_user_data_backup();
    block_addr.ls_id_ = param.ls_id_.id();
    block_addr.data_type_ = backup_data_type.type_;
    block_addr.turn_id_ = param.turn_id_;
    block_addr.retry_id_ = param.retry_id_;
    block_addr.file_id_ = file_id;
    block_addr.backup_set_id_ = param.backup_set_desc_.backup_set_id_;
    block_addr.offset_ = file_offset / DIO_READ_ALIGN_SIZE; // aligned offset is real file offset divided by 4096
    block_addr.length_ = BLOCK_SIZE / DIO_READ_ALIGN_SIZE;
    block_addr.block_type_ = ObBackupDeviceMacroBlockId::DATA_BLOCK;
    block_addr.id_mode_ = static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP);
    block_addr.version_ = ObBackupDeviceMacroBlockId::BACKUP_MACRO_BLOCK_ID_VERSION;
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::construct_linked_header_(char *header_buf)
{
  int ret = OB_SUCCESS;
  ObBackupLinkedBlockHeader *linked_header = reinterpret_cast<ObBackupLinkedBlockHeader *>(header_buf);
  if (OB_ISNULL(linked_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common header should not be null", K(ret), KP(linked_header));
  } else {
    linked_header->version_ = ObBackupLinkedBlockHeader::VERSION;
    linked_header->magic_ = ObBackupLinkedBlockHeader::MAGIC;
    linked_header->item_count_ = cur_block_write_item_cnt_;
    linked_header->tablet_id_ = tablet_id_;
    linked_header->table_key_ = table_key_;
    linked_header->prev_block_addr_ = prev_block_addr_;
    linked_header->has_prev_ = has_prev_;
    LOG_INFO("construct linked header", KPC(linked_header));
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::construct_common_header_(
    char *header_buf, const int64_t data_length, const int64_t align_length, ObBackupCommonHeader *&common_header)
{
  int ret = OB_SUCCESS;
  common_header = reinterpret_cast<ObBackupCommonHeader *>(header_buf);
  if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common header should not be null", K(ret), KP(common_header));
  } else {
    common_header->reset();
    const int64_t header_len = sizeof(ObBackupCommonHeader);
    common_header->data_type_ = BACKUP_BLOCK_OTHER_BLOCK_IDS;
    common_header->header_version_ = share::ObBackupCommonHeader::COMMON_HEADER_VERSION;
    common_header->data_version_ = 0;
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->header_length_ = header_len;
    common_header->data_length_ = data_length;
    common_header->data_zlength_ = common_header->data_length_;
    common_header->align_length_ = align_length;
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::write_single_item_(const ObBackupLinkedItem &link_item)
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter &buffer_writer = buffer_writer_;
  const bool need_advance_zero = 0 == buffer_writer.pos();
  if (!need_advance_zero) {
    // do nothing
  } else if (OB_FAIL(buffer_writer.advance_zero(sizeof(ObBackupCommonHeader)))) {
    LOG_WARN("failed to advance zero", K(ret), "header_len", sizeof(ObBackupCommonHeader));
  } else if (OB_FAIL(buffer_writer.advance_zero(sizeof(ObBackupLinkedBlockHeader)))) {
    LOG_WARN("failed to advance zero", K(ret), "header_len", linked_header_.get_serialize_size());
  }
  if (FAILEDx(buffer_writer.write_serialize(link_item))) {
    LOG_WARN("failed to write serialize", K(ret), K(link_item));
  } else {
    ++linked_header_.item_count_;
    ++cur_block_write_item_cnt_;
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::check_remain_size_enough_(
    const ObBackupLinkedItem &item, bool &is_enough)
{
  int ret = OB_SUCCESS;
  is_enough = (buf_size_ - buffer_writer_.pos()) >= item.get_serialize_size();
  LOG_INFO("check remain size enough", K(is_enough), K(buf_size_), K(buffer_writer_.pos()), K(item.get_serialize_size()), K(item));
  return ret;
}

int ObBackupLinkedBlockItemWriter::close_and_write_block_()
{
  int ret = OB_SUCCESS;
  if (is_closed_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is closed", K(ret));
  } else if (0 == cur_block_write_item_cnt_) {
    // do nothing
  } else if (OB_FAIL(close_block_())) {
    LOG_WARN("failed to close block", K(ret));
  } else if (OB_FAIL(write_block_())) {
    LOG_WARN("failed to write block", K(ret));
  } else {
    cur_block_write_item_cnt_ = 0;
    buffer_writer_.reuse();
    LOG_INFO("close and write block", K(ret));
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::close_block_()
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter &buffer_writer = buffer_writer_;
  char *header_buf = buffer_writer.data();
  ObBackupCommonHeader *common_header = NULL;
  const int64_t data_length = buffer_writer.length() - sizeof(ObBackupCommonHeader);
  const int64_t align_length = BLOCK_SIZE - buffer_writer.length();
  if (0 == linked_header_.item_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("linked header item count is 0", K(ret), K_(linked_header));
  } else if (OB_FAIL(construct_linked_header_(header_buf + sizeof(ObBackupCommonHeader)))) {
    LOG_WARN("failed to construct linked header", K(ret));
  } else if (OB_FAIL(construct_common_header_(
      header_buf, data_length, align_length, common_header))) {
    LOG_WARN("failed to construct common header", K(ret), KP(header_buf));
  } else if (OB_FAIL(common_header->set_checksum(
      buffer_writer.data() + common_header->header_length_,
      buffer_writer.length() - common_header->header_length_))) {
    LOG_WARN("failed to set checksum", K(ret));
  } else {
    ++total_block_cnt_;
  }
  return ret;
}

int ObBackupLinkedBlockItemWriter::write_block_()
{
  int ret = OB_SUCCESS;
  ObBackupLinkedBlockAddr block_addr;
  ObBufferReader buffer_reader(buffer_writer_.data(), BLOCK_SIZE, BLOCK_SIZE);
  if (!buffer_reader.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer reader is not valid", K(ret), K(buffer_reader));
  } else if (OB_FAIL(block_writer_.write_block(buffer_reader, block_addr))) {
    LOG_WARN("failed to write block", K(ret), K(buffer_reader), K(block_addr));
  } else if (OB_FAIL(written_block_list_.push_back(block_addr))) {
    LOG_WARN("failed to push back block id", K(ret), K(block_addr));
  } else {
    prev_block_addr_ = block_addr;
    has_prev_ = true;
    linked_header_.item_count_ = 0;
  }
  return ret;
}

}
}
