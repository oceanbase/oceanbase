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

#include "ob_backup_linked_block_reader.h"
#include "storage/backup/ob_backup_restore_util.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace backup
{

// ObBackupLinkedBlockReader

ObBackupLinkedBlockReader::ObBackupLinkedBlockReader()
  : is_inited_(false),
    buf_(NULL),
    buf_len_(0),
    total_block_cnt_(0),
    read_block_cnt_(0),
    next_block_addr_(),
    common_header_(),
    linked_header_(),
    backup_set_dest_(),
    mod_(),
    tablet_id_(),
    table_key_()
{
}

ObBackupLinkedBlockReader::~ObBackupLinkedBlockReader()
{
}

int ObBackupLinkedBlockReader::init(const share::ObBackupDest &backup_set_dest,
    const ObBackupLinkedBlockAddr &entry_block_id, const int64_t total_block_cnt,
    const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
    const ObMemAttr &mem_attr, const ObStorageIdMod &mod)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupLinkedBlockReader has already been inited", K(ret));
  } else if (!backup_set_dest.is_valid() || !entry_block_id.is_valid() || total_block_cnt <= 0 || !mod.is_valid()
      || !tablet_id.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_set_dest), K(entry_block_id), K(total_block_cnt), K(mod),
      K(tablet_id), K(table_key));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("failed to deep copy", K(ret), K(backup_set_dest));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    allocator_.set_attr(mem_attr);
    mod_ = mod;
    buf_len_ = BLOCK_SIZE;
    total_block_cnt_ = total_block_cnt;
    read_block_cnt_ = 0;
    next_block_addr_ = entry_block_id;
    tablet_id_ = tablet_id;
    table_key_ = table_key;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLinkedBlockReader::get_next_block(ObBufferReader &buffer_reader,
    ObBackupLinkedBlockAddr &block_id, bool &has_prev, int64_t &item_count)
{
  int ret = OB_SUCCESS;
  item_count = 0;
  has_prev = false;
  block_id = next_block_addr_;
  if (read_block_cnt_ >= total_block_cnt_) {
    ret = OB_ITER_END;
    LOG_WARN("linked block reader reach end", K(ret), K_(read_block_cnt), K_(total_block_cnt));
  } else if (!next_block_addr_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("next block id is not valid", K(ret), K_(next_block_addr));
  } else if (OB_FAIL(pread_block_(next_block_addr_, buffer_reader))) {
    LOG_WARN("failed to pread block", K(ret), K_(next_block_addr));
  } else if (OB_FAIL(parse_block_header_(buffer_reader))) {
    LOG_WARN("failed to parse block header", K(ret));
  } else {
    next_block_addr_ = linked_header_.prev_block_addr_;
    has_prev = linked_header_.has_prev_;
    item_count = linked_header_.item_count_;
    ++read_block_cnt_;
  }
  return ret;
}

int ObBackupLinkedBlockReader::pread_block_(const ObBackupLinkedBlockAddr &block_addr,
    blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  const int64_t offset = static_cast<int64_t>(block_addr.offset_) * DIO_READ_ALIGN_SIZE;
  const int64_t length = static_cast<int64_t>(block_addr.length_) * DIO_READ_ALIGN_SIZE;
  const ObBackupStorageInfo *storage_info = backup_set_dest_.get_storage_info();
  if (!block_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(block_addr));
  } else if (OB_FAIL(get_backup_file_path_(block_addr, backup_path))) {
    LOG_WARN("failed to get backup file path", K(ret), K(block_addr));
  } else if (length > buf_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length is larger than buf len", K(ret), K(length), K_(buf_len));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::pread_file(backup_path.get_obstr(),
                                                       storage_info,
                                                       mod_,
                                                       offset,
                                                       length,
                                                       buf_))) {
    LOG_WARN("failed to pread file", K(ret), K(block_addr), K(offset), K(length));
  } else {
    buffer_reader.assign(buf_, length);
  }
  return ret;
}

int ObBackupLinkedBlockReader::get_backup_file_path_(const ObBackupLinkedBlockAddr &block_addr, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  if (!block_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(block_addr));
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_set_dest_,
                                                                          ObLSID(block_addr.ls_id_),
                                                                          backup_data_type,
                                                                          block_addr.turn_id_,
                                                                          block_addr.retry_id_,
                                                                          block_addr.file_id_,
                                                                          backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K_(backup_set_dest), K(block_addr));
  }
  return ret;
}


int ObBackupLinkedBlockReader::parse_block_header_(blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(decode_common_header_(buffer_reader))) {
    LOG_WARN("failed to decode common header", K(ret));
  } else if (OB_FALSE_IT(pos = buffer_reader.pos())) {
  } else if (OB_FAIL(decode_linked_header_(buffer_reader))) {
    LOG_WARN("failed to decode linked header", K(ret), K(buffer_reader));
  } else {
    int64_t data_size = common_header_.data_length_ - (buffer_reader.pos() - pos);
    buffer_reader.assign(buffer_reader.current(), data_size);
  }
  return ret;
}

int ObBackupLinkedBlockReader::decode_common_header_(blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  const ObBackupCommonHeader *common_header = NULL;
  if (!buffer_reader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer reader is not valid", K(ret), K(buffer_reader));
  } else if (OB_FAIL(buffer_reader.get(common_header))) {
    LOG_WARN("failed to get common header from buffer", K(ret), K(buffer_reader));
  } else if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common header is null", K(ret));
  } else if (OB_FAIL(common_header->check_valid())) {
    LOG_WARN("common header is not valid", K(ret), KPC(common_header));
  } else if (common_header->data_length_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer reader is not enough", K(ret), K(buffer_reader), KPC(common_header));
  } else if (BACKUP_BLOCK_OTHER_BLOCK_IDS != common_header->data_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data type is not other block ids", K(ret), KPC(common_header));
  } else if (OB_FAIL(common_header->check_data_checksum(
      buffer_reader.current(), common_header->data_length_))) {
    LOG_WARN("failed to check data checksum", K(ret), KPC(common_header));
  } else {
    common_header_ = *common_header;
  }
  return ret;
}

int ObBackupLinkedBlockReader::decode_linked_header_(blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  const ObBackupLinkedBlockHeader *linked_header = NULL;
  if (!buffer_reader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer reader is not valid", K(ret), K(buffer_reader));
  } else if (OB_FAIL(buffer_reader.get(linked_header))) {
    LOG_WARN("failed to read serialize", K(ret), K(buffer_reader));
  } else if (linked_header->tablet_id_ != tablet_id_ || linked_header->table_key_ != table_key_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("linked header is not valid", K(ret), KPC(linked_header));
  } else {
    linked_header_ = *linked_header;
    LOG_INFO("decode linked header", K(linked_header_));
  }
  return ret;
}

// ObBackupLinkedBlockItemReader

ObBackupLinkedBlockItemReader::ObBackupLinkedBlockItemReader()
  : is_inited_(false),
    has_prev_(false),
    item_idx_(0),
    read_item_count_(0),
    read_block_count_(0),
    block_item_list_(),
    block_reader_(),
    read_block_list_()
{
}

ObBackupLinkedBlockItemReader::~ObBackupLinkedBlockItemReader()
{
}

int ObBackupLinkedBlockItemReader::init(const share::ObBackupDest &backup_set_dest,
    const ObBackupLinkedBlockAddr &entry_block_id, const int64_t total_block_count,
    const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
    const ObMemAttr &mem_attr, const ObStorageIdMod &mod)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupLinkedBlockItemReader has already been inited", K(ret));
  } else if (!backup_set_dest.is_valid() || !entry_block_id.is_valid() || total_block_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry block", K(ret), K(backup_set_dest), K(entry_block_id), K(total_block_count));
  } else if (OB_FAIL(block_reader_.init(backup_set_dest, entry_block_id,
      total_block_count, tablet_id, table_key, mem_attr, mod))) {
    LOG_WARN("failed to init block reader", K(ret), K(entry_block_id), K(total_block_count));
  } else if (OB_FAIL(read_item_block_())) {
    LOG_WARN("failed to read item block", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLinkedBlockItemReader::get_next_item(ObBackupLinkedItem &link_item)
{
  int ret = OB_SUCCESS;
  link_item.reset();
  bool need_fetch_new = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupLinkedBlockItemReader has not been inited", K(ret));
  } else if (!has_prev_ && item_idx_ >= block_item_list_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("ObBackupLinkedBlockItemReader has reach end", K_(has_prev), K_(item_idx), "count", block_item_list_.count());
  } else if (OB_FAIL(check_need_fetch_new_block_(need_fetch_new))) {
    LOG_WARN("failed to check need fetch new block", K(ret));
  } else if (!need_fetch_new) {
    // do nothing
  } else if (OB_FAIL(read_item_block_())) {
    LOG_WARN("failed to read item block", K(ret));
  }
  if (FAILEDx(inner_get_next_item_(link_item))) {
    LOG_WARN("failed to parse item", K(ret));
  }
  return ret;
}

int ObBackupLinkedBlockItemReader::check_need_fetch_new_block_(bool &need_fetch_new)
{
  int ret = OB_SUCCESS;
  need_fetch_new = false;
  if (!has_prev_) {
    need_fetch_new = false;
  } else if (item_idx_ >= block_item_list_.count()) {
    need_fetch_new = true;
  } else {
    need_fetch_new = false;
  }
  LOG_INFO("need fetch new", K(need_fetch_new), K_(item_idx), K_(has_prev));
  return ret;
}

int ObBackupLinkedBlockItemReader::read_item_block_()
{
  int ret = OB_SUCCESS;
  ObBufferReader buffer_reader;
  int64_t item_count = 0;
  ObBackupLinkedBlockAddr block_id;
  if (OB_FAIL(block_reader_.get_next_block(buffer_reader, block_id, has_prev_, item_count))) {
    LOG_WARN("failed to read block", K(ret));
  } else if (OB_FAIL(read_block_list_.push_back(block_id))) {
    LOG_WARN("failed to push back", K(ret), K(block_id));
  } else if (OB_FAIL(parse_item_list_(item_count, buffer_reader))) {
    LOG_WARN("failed to parse item", K(ret), K(item_count));
  } else {
    item_idx_ = 0;
    read_block_count_++;
  }
  return ret;
}

int ObBackupLinkedBlockItemReader::parse_item_list_(
    const int64_t item_count,
    blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  block_item_list_.reset();
  ObBackupLinkedItem link_item;
  int64_t cur_item_count = 0;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0 && cur_item_count < item_count) {
    link_item.reset();
    if (OB_FAIL(buffer_reader.read_serialize(link_item))) {
      LOG_WARN("failed to read serilize", K(ret));
    } else if (!link_item.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid item", K(ret), K(link_item));
    } else if (OB_FAIL(block_item_list_.push_back(link_item))) {
      LOG_WARN("failed to push back", K(ret), K(link_item));
    } else {
      cur_item_count++;
    }
  }
  LOG_INFO("parse item list", K(item_count), K(cur_item_count), K_(read_block_count), K(buffer_reader));
  return ret;
}

int ObBackupLinkedBlockItemReader::inner_get_next_item_(ObBackupLinkedItem &link_item)
{
  int ret = OB_SUCCESS;
  link_item.reset();
  if (item_idx_ >= block_item_list_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("linked block item reader reach end", K(ret), K_(read_item_count), K_(read_block_count), K_(item_idx), K(block_item_list_.count()));
  } else {
    link_item = block_item_list_.at(item_idx_);
    item_idx_++;
    read_item_count_++;
  }
  return ret;
}

}
}
