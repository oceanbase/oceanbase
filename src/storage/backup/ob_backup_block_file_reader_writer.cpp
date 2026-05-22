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

#include "storage/backup/ob_backup_block_file_reader_writer.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_util.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/checksum/ob_crc64.h"

namespace oceanbase
{
namespace backup
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

OB_SERIALIZE_MEMBER(ObBackupBlockFileMacroIdItem, macro_id_);

int ObBackupBlockFileMacroIdItem::assign(const ObBackupBlockFileMacroIdItem &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(other));
  } else {
    macro_id_ = other.macro_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupFileInfo, type_, file_size_, path_);

ObBackupFileInfo::ObBackupFileInfo()
  : type_(BACKUP_FILE_LIST_MAX_TYPE),
    file_size_(0),
    path_()
{
}

bool ObBackupFileInfo::is_valid() const
{
  bool bret = false;
  if (BACKUP_FILE_LIST_DIR_INFO == type_ && !path_.is_empty()) {
    bret = true;
  } else if (BACKUP_FILE_LIST_FILE_INFO == type_ && file_size_ >= 0 && !path_.is_empty()) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

void ObBackupFileInfo::reset()
{
  type_ = BACKUP_FILE_LIST_MAX_TYPE;
  file_size_ = 0;
  path_.reset();
}

int ObBackupFileInfo::assign(const ObBackupFileInfo &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(other));
  } else {
    type_ = other.type_;
    file_size_ = other.file_size_;
    path_ = other.path_;
  }
  return ret;
}

int ObBackupFileInfo::set_file_info(const char *file_name, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_name) || '\0' == file_name[0] || file_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file name", KR(ret), KP(file_name), K(file_size));
  } else if (OB_FAIL(path_.assign(file_name))) {
    LOG_WARN("failed to assign file name", KR(ret), K(file_name));
  } else {
    file_size_ = file_size;
    type_ = BACKUP_FILE_LIST_FILE_INFO;
  }
  return ret;
}

int ObBackupFileInfo::set_file_info(const share::ObBackupPath &file_path, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  ObBackupPathString file_name;
  if (file_path.is_empty() || file_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path or file size", KR(ret), K(file_path), K(file_size));
  } else if (OB_FAIL(file_path.get_basename(file_name))) {
    LOG_WARN("failed to get file name", KR(ret), K(file_path));
  } else if (OB_FAIL(set_file_info(file_name.ptr(), file_size))) {
    LOG_WARN("failed to set file info", KR(ret), K(file_name), K(file_size));
  }
  return ret;
}

int ObBackupFileInfo::set_dir_info(const char *dir_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir_name) || '\0' == dir_name[0]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dir name", KR(ret), KP(dir_name));
  } else if (OB_FAIL(path_.assign(dir_name))) {
    LOG_WARN("failed to assign dir name", KR(ret), K(dir_name));
  } else {
    type_ = BACKUP_FILE_LIST_DIR_INFO;
  }
  return ret;
}

bool ObBackupFileInfo::operator <(const ObBackupFileInfo &other) const
{
  bool bret = false;
  if (type_ != other.type_) {
    bret = type_ < other.type_;
  } else {
    bret = path_ < other.path_;
  }
  return bret;
}

//ObBackupFileListInfo
ObBackupFileListInfo::ObBackupFileListInfo()
  : file_list_()
{
}

void ObBackupFileListInfo::reset()
{
  file_list_.reset();
}

bool ObBackupFileListInfo::is_valid() const
{
  return !file_list_.empty();
}

int ObBackupFileListInfo::assign(const ObBackupFileListInfo &other)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file list info already valid", KR(ret));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file list info", KR(ret), K(other));
  } else if (OB_FAIL(file_list_.assign(other.file_list_))) {
    LOG_WARN("failed to assign file list", KR(ret));
  }
  return ret;
}

void ObBackupFileListInfo::sort_file_list()
{
  lib::ob_sort(file_list_.begin(), file_list_.end());
}

int ObBackupFileListInfo::push_file_info(const ObBackupFileInfo &file_info)
{
  int ret = OB_SUCCESS;
  if (!file_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file info", KR(ret), K(file_info));
  } else if (OB_FAIL(file_list_.push_back(file_info))) {
    LOG_WARN("failed to push file info", KR(ret), K(file_info));
  }
  return ret;
}

int ObBackupFileListInfo::push_file_info(const ObBackupPath &file_path, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  if (file_path.is_empty() || file_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path or file size", KR(ret), K(file_path), K(file_size));
  } else {
    ObBackupPathString file_name;
    ObBackupFileInfo file_info;
    if (OB_FAIL(file_path.get_basename(file_name))) {
      LOG_WARN("failed to get file name", KR(ret), K(file_path));
    } else if (OB_FAIL(file_info.set_file_info(file_name.ptr(), file_size))) {
      LOG_WARN("failed to set file info", KR(ret), K(file_name), K(file_size));
    } else if (OB_FAIL(push_file_info(file_info))) {
      LOG_WARN("failed to push file info", KR(ret), K(file_name), K(file_size));
    }
  }
  return ret;
}

int ObBackupFileListInfo::push_dir_info(const ObBackupPath &dir_path)
{
  int ret = OB_SUCCESS;
  if (dir_path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dir path", KR(ret), K(dir_path));
  } else {
    ObBackupPathString dir_name;
    ObBackupFileInfo file_info;
    if (OB_FAIL(dir_path.get_basename(dir_name))) {
      LOG_WARN("failed to get dir name", KR(ret), K(dir_path));
    } else if (OB_FAIL(file_info.set_dir_info(dir_name.ptr()))) {
      LOG_WARN("failed to set dir info", KR(ret), K(dir_name));
    } else if (OB_FAIL(push_file_info(file_info))) {
      LOG_WARN("failed to push dir info", KR(ret), K(dir_name));
    }
  }
  return ret;
}

/**
 * ------------------------------ObBackupDirListOp---------------------
 */
ObBackupDirListOp::ObBackupDirListOp(ObIArray<ObBackupFileInfo> &filelist)
  : filelist_(filelist)
{
}

int ObBackupDirListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObBackupFileInfo file_info;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", K(ret));
  } else {
    const char *filename = entry->d_name;
    const int64_t file_size = get_size();
    ObString filename_str(filename);
    if (0 == STRNCMP(filename, OB_STR_FILE_LIST, STRLEN(OB_STR_FILE_LIST))) {
      // do nothing
    } else if (NULL != strchr(filename, '/')) {
      // do nothing
    //Multipart upload failed in NFS mode, leaving behind some temporary files whose suffixes are not .obbak.
    } else if (!filename_str.suffix_match(OB_ARCHIVE_SUFFIX) && !filename_str.suffix_match(OB_BACKUP_SUFFIX)) {
    } else if (OB_FAIL(file_info.set_file_info(filename, file_size))) {
      LOG_WARN("failed to set file info", K(ret), KCSTRING(filename), K(file_size));
    } else if (OB_FAIL(filelist_.push_back(file_info))) {
      LOG_WARN("push back failed", K(ret), K(file_info));
    }
  }
  return ret;
}

// ObBackupBlockFileAddr

OB_SERIALIZE_MEMBER(ObBackupBlockFileAddr, file_id_, offset_, length_);

ObBackupBlockFileAddr::ObBackupBlockFileAddr()
  : file_id_(),
    offset_(),
    length_()
{
}

ObBackupBlockFileAddr::~ObBackupBlockFileAddr()
{
}

int ObBackupBlockFileAddr::set(const int64_t file_id, const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (file_id < 0 || offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(file_id), K(offset), K(length));
  } else {
    file_id_ = file_id;
    offset_ = offset;
    length_ = length;
  }
  return ret;
}

void ObBackupBlockFileAddr::reset()
{
  file_id_ = -1;
  offset_ = -1;
  length_ = -1;
}

bool ObBackupBlockFileAddr::is_valid() const
{
  return file_id_ >= 0
      && offset_ >= 0
      && length_ > 0;
}

uint64_t ObBackupBlockFileAddr::calc_checksum(const uint64_t checksum) const
{
  uint64_t new_checksum = checksum;
  new_checksum = ob_crc64(new_checksum, &file_id_, sizeof(file_id_));
  new_checksum = ob_crc64(new_checksum, &offset_, sizeof(offset_));
  new_checksum = ob_crc64(new_checksum, &length_, sizeof(length_));
  return new_checksum;
}

void ObBackupBlockFileAddr::operator=(const ObBackupBlockFileAddr &other)
{
  file_id_ = other.file_id_;
  offset_ = other.offset_;
  length_ = other.length_;
}

// ObBackupBlockFileHeader

ObBackupBlockFileHeader::ObBackupBlockFileHeader()
  : version_(),
    magic_(),
    data_type_(0),
    item_count_(0)
{
}

ObBackupBlockFileHeader::~ObBackupBlockFileHeader()
{
}

bool ObBackupBlockFileHeader::is_valid() const
{
  return item_count_ >= 0;
}

OB_SERIALIZE_MEMBER(ObBackupBlockFileHeader,
                    version_,
                    magic_,
                    data_type_,
                    item_count_);

// ObBackupBlockFileTailer

ObBackupBlockFileTailer::ObBackupBlockFileTailer()
  : version_(VERSION),
    magic_(MAGIC),
    entry_block_addr_(),
    total_block_count_(0),
    last_file_id_(0),
    file_length_(0),
    checksum_(0)
{
}

ObBackupBlockFileTailer::~ObBackupBlockFileTailer()
{
}

void ObBackupBlockFileTailer::reset()
{
  version_ = VERSION;
  magic_ = MAGIC;
  entry_block_addr_.reset();
  total_block_count_ = 0;
  last_file_id_ = 0;
  file_length_ = 0;
  checksum_ = 0;
}

bool ObBackupBlockFileTailer::is_valid() const
{
  return VERSION == version_
      && MAGIC == magic_
      && entry_block_addr_.is_valid()
      && total_block_count_ >= 0
      && last_file_id_ >= 0
      && file_length_ > 0
      && calc_checksum_() == checksum_;
}

int ObBackupBlockFileTailer::check_integrity() const
{
  int ret = OB_SUCCESS;
  if (VERSION != version_ || MAGIC != magic_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tailer version or magic mismatch", K(ret), K_(version), K_(magic));
  } else if (!entry_block_addr_.is_valid()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tailer entry block address invalid", K(ret), K_(entry_block_addr));
  } else if (total_block_count_ < 0 || last_file_id_ < 0 || file_length_ <= 0) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tailer block count or file info invalid", K(ret), K_(total_block_count), K_(last_file_id), K_(file_length));
  } else {
    const uint64_t expected_checksum = calc_checksum_();
    if (expected_checksum != checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("tailer checksum mismatch", K(ret), K(expected_checksum), K_(checksum));
    }
  }
  return ret;
}

uint64_t ObBackupBlockFileTailer::calc_checksum_() const
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, &version_, sizeof(version_));
  checksum = ob_crc64(checksum, &magic_, sizeof(magic_));
  checksum = entry_block_addr_.calc_checksum(checksum);
  checksum = ob_crc64(checksum, &total_block_count_, sizeof(total_block_count_));
  checksum = ob_crc64(checksum, &last_file_id_, sizeof(last_file_id_));
  checksum = ob_crc64(checksum, &file_length_, sizeof(file_length_));
  return checksum;
}

int ObBackupBlockFileTailer::set_entry_block_info(
    const ObBackupBlockFileAddr &entry_block_addr,
    const int64_t total_block_count,
    const int64_t last_file_id,
    const int64_t file_length)
{
  int ret = OB_SUCCESS;
  if (!entry_block_addr.is_valid() || total_block_count < 0 || last_file_id < 0 || file_length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(entry_block_addr), K(total_block_count), K(last_file_id), K(file_length));
  } else {
    entry_block_addr_ = entry_block_addr;
    total_block_count_ = total_block_count;
    last_file_id_ = last_file_id;
    file_length_ = file_length;
    checksum_ = calc_checksum_();
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupBlockFileTailer,
                    version_,
                    magic_,
                    entry_block_addr_,
                    total_block_count_,
                    last_file_id_,
                    file_length_,
                    checksum_);

// ObBackupBlockFileWriter

ObBackupBlockFileWriter::ObBackupBlockFileWriter()
  : is_inited_(false),
    is_dir_created_(false),
    storage_info_(nullptr),
    data_type_(ObBackupBlockFileDataType::MAX_TYPE),
    suffix_(ObBackupFileSuffix::NONE),
    file_list_dir_(),
    current_file_id_(0),
    file_offset_(0),
    total_block_count_(0),
    total_file_count_(1),
    mod_(),
    allocator_("BkupLinkWr"),
    max_file_size_()
{
}

ObBackupBlockFileWriter::~ObBackupBlockFileWriter()
{
}

int ObBackupBlockFileWriter::init(
    const common::ObObjectStorageInfo *storage_info,
    const ObBackupBlockFileDataType data_type,
    const share::ObBackupFileSuffix &suffix,
    const share::ObBackupPath &file_list_dir,
    const int64_t dest_id,
    const int64_t max_file_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupBlockFileWriter init twice", K(ret));
  } else if (max_file_size < BLOCK_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("max file size should not be less than 2MB", K(ret), K(max_file_size));
  } else {
    storage_info_ = storage_info;
    data_type_ = data_type;
    suffix_ = suffix;
    file_list_dir_ = file_list_dir;
    is_dir_created_ = false;
    current_file_id_ = 0;
    file_offset_ = 0;
    total_block_count_ = 0;
    total_file_count_ = 1;
    mod_.storage_id_ = dest_id;
    mod_.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
    max_file_size_ = max_file_size;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupBlockFileWriter::write_block(
    const ObBufferReader &buffer_reader,
    const int64_t item_count,
    const int64_t data_type,
    ObBackupBlockFileAddr &block_addr)
{
  int ret = OB_SUCCESS;
  block_addr.reset();
  ObSelfBufferWriter block_writer("BkupBlkWr", BLOCK_SIZE);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupBlockFileWriter not init", K(ret));
  } else if (!buffer_reader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer reader", K(ret), K(buffer_reader));
  } else if (OB_FAIL(check_and_switch_file_())) {
    LOG_WARN("failed to check and switch file", K(ret), K_(current_file_id));
  } else if (OB_FAIL(build_block_data_(buffer_reader, item_count, data_type, block_writer))) {
    LOG_WARN("failed to build block data", K(ret));
  } else if (OB_FAIL(write_block_to_storage_(block_writer, block_addr))) {
    LOG_WARN("failed to write block to storage", K(ret));
  } else {
    total_block_count_++;
    LOG_DEBUG("success write block file block", K(block_addr), K(total_block_count_),
             K(file_offset_), K(current_file_id_));
  }
  return ret;
}

int ObBackupBlockFileWriter::close(const ObBackupBlockFileAddr &entry_block_addr)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupBlockFileWriter not init", K(ret));
  } else if (!entry_block_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry block addr", K(ret), K(entry_block_addr));
  } else if (OB_FAIL(write_file_tailer_(entry_block_addr))) {
    LOG_WARN("failed to write file tailer", K(ret));
  } else if (OB_FAIL(close_current_file_())) {
    LOG_WARN("failed to close current file", K(ret));
  } else {
    LOG_INFO("success close block file block writer", K(entry_block_addr),
             K(total_block_count_), K(file_offset_), K(total_file_count_));
  }
  return ret;
}

int ObBackupBlockFileWriter::check_and_switch_file_()
{
  int ret = OB_SUCCESS;

  // Check if the current file size is close to the configured max size
  const int64_t file_size_limit = max_file_size_ > 0 ? max_file_size_ : OB_BACKUP_DEFAULT_MAX_FILE_SIZE;
  if (file_offset_ + BLOCK_SIZE > file_size_limit) {
    if (OB_FAIL(switch_to_new_file_())) {
      LOG_WARN("failed to switch to new file", K(ret));
    }
  }
  return ret;
}

int ObBackupBlockFileWriter::switch_to_new_file_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(close_current_file_())) {
    LOG_WARN("failed to close current file", K(ret), K_(current_file_id));
  } else {
    current_file_id_++;
    file_offset_ = 0;
    total_file_count_++;
    LOG_INFO("switch to new file", K(current_file_id_), K(total_file_count_));
  }
  return ret;
}

int ObBackupBlockFileWriter::build_block_data_(
    const ObBufferReader &buffer_reader,
    const int64_t item_count,
    const int64_t data_type,
    ObSelfBufferWriter &block_writer)
{
  int ret = OB_SUCCESS;
  const int64_t data_size = buffer_reader.length();

  // 1. Allocate space for ObBackupCommonHeader
  ObBackupCommonHeader *common_header = nullptr;
  char *header_buf = nullptr;
  const int64_t common_header_size = sizeof(ObBackupCommonHeader);
  if (OB_FAIL(block_writer.ensure_space(common_header_size))) {
    LOG_WARN("failed to ensure space for common header", K(ret));
  } else if (OB_FAIL(block_writer.advance_zero(common_header_size))) {
    LOG_WARN("failed to advance for common header", K(ret));
  } else {
    header_buf = block_writer.data();
    common_header = new (header_buf) ObBackupCommonHeader();
    common_header->reset();
  }

  // 2. Write to ObBackupBlockFileHeader
  if (OB_SUCC(ret)) {
    ObBackupBlockFileHeader block_file_header;
    block_file_header.version_ = ObBackupBlockFileHeader::VERSION;
    block_file_header.magic_ = ObBackupBlockFileHeader::MAGIC;
    block_file_header.data_type_ = data_type;
    block_file_header.item_count_ = item_count;

    int64_t pos = 0;
    const int64_t block_file_header_size = block_file_header.get_serialize_size();
    if (OB_FAIL(block_writer.write_serialize(block_file_header))) {
      LOG_WARN("failed to write block file header", K(ret));
    }
  }

  // 3. Write in actual data
  if (OB_SUCC(ret)) {
    if (OB_FAIL(block_writer.write(buffer_reader.data(), buffer_reader.length()))) {
      LOG_WARN("failed to write data", K(ret), "buffer_length", buffer_reader.length());
    }
  }

  // 4. Packed into block size (2MB alignment)
  if (OB_SUCC(ret)) {
    const int64_t current_size = block_writer.pos();
    const int64_t padding_size = BLOCK_SIZE - current_size;
    if (padding_size > 0) {
      if (OB_FAIL(block_writer.advance_zero(padding_size))) {
        LOG_WARN("failed to add padding", K(ret), K(padding_size));
      }
    }
  }

  // 5. Update ObBackupCommonHeader
  if (OB_SUCC(ret) && nullptr != common_header) {
    const int64_t data_length = block_writer.pos() - common_header_size;
    common_header->data_type_ = static_cast<uint16_t>(share::ObBackupFileType::SS_BACKUP_BLOCK_FILE);
    common_header->data_version_ = DATA_VERSION;
    common_header->compressor_type_ = static_cast<uint16_t>(ObCompressorType::NONE_COMPRESSOR);
    common_header->data_length_ = data_length;
    common_header->data_zlength_ = data_length;
    common_header->align_length_ = BLOCK_SIZE;

    // Calculate the checksum
    if (OB_FAIL(common_header->set_checksum(
        block_writer.data() + common_header_size, data_length))) {
      LOG_WARN("failed to set checksum", K(ret));
    }
  }

  return ret;
}

int ObBackupBlockFileWriter::write_block_to_storage_(
    const ObBufferWriter &buffer_writer,
    ObBackupBlockFileAddr &block_addr)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  ObBackupPath file_path;
  ObBackupIoAdapter util;
  blocksstable::ObBufferReader buffer_reader;
  buffer_reader.assign(buffer_writer.data(), buffer_writer.length(), buffer_writer.length());

  if (OB_FAIL(ensure_dir_created_())) {
    LOG_WARN("failed to ensure file opened", K(ret), K(current_file_id_));
  } else if (OB_FAIL(get_file_path_(current_file_id_, file_path))) {
    LOG_WARN("failed to get file path", K(ret), K(current_file_id_));
  } else if (OB_FAIL(util.pwrite(file_path.get_obstr(), storage_info_, buffer_reader.data(), file_offset_,
                                    buffer_reader.length(), ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER,
                                    write_size, false/*is_can_seal*/, mod_))) {
    LOG_WARN("failed to pwrite by uri", K(ret), K(file_path), K_(file_offset), K(buffer_reader.length()));
  } else if (write_size != buffer_reader.length()) {
    ret = OB_IO_ERROR;
    LOG_WARN("write size mismatch", K(ret), K(write_size), K(buffer_reader.length()));
  } else if (OB_FAIL(block_addr.set(
      current_file_id_,
      file_offset_,
      BLOCK_SIZE))) {
    LOG_WARN("failed to set block addr", K(ret));
  } else {
    file_offset_ += BLOCK_SIZE;
  }

  return ret;
}

int ObBackupBlockFileWriter::write_file_tailer_(const ObBackupBlockFileAddr &entry_block_addr)
{
  int ret = OB_SUCCESS;
  ObBackupBlockFileTailer tailer;
  ObBackupPath path;
  int64_t write_size = 0;
  char tailer_buf[TAILER_SIZE] = { 0 };
  int64_t pos = 0;
  ObBackupIoAdapter util;

  // Write the tailer only in the last file
  // Set the tail metadata (record the total number of blocks for the entire file group)
  if (OB_FAIL(tailer.set_entry_block_info(entry_block_addr, total_block_count_, current_file_id_, file_offset_))) {
    LOG_WARN("failed to set entry block info", K(ret));
  } else if (OB_FAIL(tailer.serialize(tailer_buf, TAILER_SIZE, pos))) {
    LOG_WARN("failed to serialize tailer", K(ret));
  } else if (OB_FAIL(get_file_path_(current_file_id_, path))) {
    LOG_WARN("failed to get file path", K(ret), K(current_file_id_));
  } else if (OB_FAIL(util.pwrite(path.get_obstr(), storage_info_, tailer_buf, file_offset_, pos,
                                    ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER,
                                    write_size, false/*is_can_seal*/, mod_))) {
    LOG_WARN("failed to pwrite tailer by uri", K(ret), K(path), K_(file_offset), K(pos));
  } else if (write_size != pos) {
    ret = OB_IO_ERROR;
    LOG_WARN("write size mismatch", K(ret), K(write_size), K(pos));
  } else {
    file_offset_ += pos;
    LOG_INFO("success write file tailer", K(tailer), K(pos), K(file_offset_), K(current_file_id_));
  }
  return ret;
}

int ObBackupBlockFileWriter::get_file_path_(const int64_t file_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  path = file_list_dir_;
  if (ObBackupBlockFileDataType::FILE_PATH_INFO == data_type_) {
    if (OB_FAIL(path.join_file_list(file_id, suffix_))) {
      LOG_WARN("failed to join ss backup file list", K(ret), K(path));
    }
  } else if (ObBackupBlockFileDataType::MACRO_BLOCK_ID == data_type_) {
    if (OB_FAIL(path.join_backup_file_list(file_id))) {
      LOG_WARN("failed to join ss backup file list", K(ret), K(file_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data type", K(ret), K(data_type_));
  }
  return ret;
}

 int ObBackupBlockFileWriter::ensure_dir_created_()
{
  int ret = OB_SUCCESS;
  if (!is_dir_created_) {
    ObBackupPath dir_path = file_list_dir_;
    ObBackupIoAdapter util;
    if (OB_FAIL(util.mkdir(dir_path.get_obstr(), storage_info_))) {
      LOG_WARN("failed to make parent dir", K(ret), K(dir_path));
    } else {
      is_dir_created_ = true;
    }
  }
  return ret;
}

int ObBackupBlockFileWriter::close_current_file_()
{
  int ret = OB_SUCCESS;
  if (file_offset_ > 0) {
    ObBackupPath file_path;
    ObBackupIoAdapter util;
    if (OB_FAIL(get_file_path_(current_file_id_, file_path))) {
      LOG_WARN("failed to get file path", K(ret), K(current_file_id_));
    } else if (OB_FAIL(util.seal_file(file_path.get_obstr(), storage_info_, mod_))) {
      LOG_WARN("failed to seal file", K(ret), K(file_path), K(current_file_id_));
    }
  }
  return ret;
}

// ObBackupBlockFileReader

const int64_t ObBackupBlockFileReader::BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;
const int64_t ObBackupBlockFileReader::TAILER_SIZE = sizeof(ObBackupBlockFileTailer);

ObBackupBlockFileReader::ObBackupBlockFileReader()
  : is_inited_(false),
    file_list_dir_(),
    suffix_(ObBackupFileSuffix::NONE),
    data_type_(ObBackupBlockFileDataType::MAX_TYPE),
    storage_info_(nullptr),
    file_tailer_(),
    common_header_(),
    block_file_header_(),
    total_block_count_(0),
    read_block_count_(0),
    file_count_(0),
    current_file_id_(0),
    current_offset_(0),
    current_file_size_(0),
    buf_(nullptr),
    buf_len_(0),
    allocator_("BkupLinkRd"),
    mod_()
{
}

ObBackupBlockFileReader::~ObBackupBlockFileReader()
{
}

int ObBackupBlockFileReader::init(
    const ObObjectStorageInfo *storage_info,
    const ObBackupBlockFileDataType data_type,
    const share::ObBackupPath &file_list_dir,
    const share::ObBackupFileSuffix &suffix,
    const ObStorageIdMod &mod)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupBlockFileReader init twice", K(ret));
  } else if (OB_ISNULL(storage_info) || file_list_dir.is_empty() || !mod.is_valid()
                || ObBackupFileSuffix::NONE == suffix || ObBackupBlockFileDataType::MAX_TYPE == data_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(storage_info), K(file_list_dir), K(mod), K(suffix), K(data_type));
  } else {
    storage_info_ = storage_info;
    file_list_dir_ = file_list_dir;
    suffix_ = suffix;
    data_type_ = data_type;
    mod_ = mod;

    if (OB_FAIL(detect_file_count_())) {
      LOG_WARN("failed to detect file count", K(ret));
    } else if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buffer", K(ret));
    } else {
      buf_len_ = BLOCK_SIZE;
      if (OB_FAIL(read_last_file_tailer_())) {
        LOG_WARN("failed to read file tailer", K(ret));
      } else {
        total_block_count_ = file_tailer_.total_block_count_;
        read_block_count_ = 0;
        current_file_id_ = 0;  // Start from the first file
        current_offset_ = 0;    // Start from offset 0
        current_file_size_ = 0; // Lazily populated on demand
        is_inited_ = true;
        LOG_INFO("success init from file", K(file_tailer_), K(file_count_));
      }
    }
  }

  return ret;
}

int ObBackupBlockFileReader::get_next_block(
    ObBufferReader &buffer_reader,
    ObBackupBlockFileAddr &block_addr,
    int64_t &item_count,
    int64_t &data_type)
{
  int ret = OB_SUCCESS;
  buffer_reader.assign(NULL, 0);
  block_addr.reset();
  item_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupBlockFileReader not init", K(ret));
  } else if (read_block_count_ >= total_block_count_) {
    ret = OB_ITER_END;
    LOG_DEBUG("already read all blocks", K(read_block_count_), K(total_block_count_));
  } else {
    bool switched_file = false;
    if (OB_FAIL(ensure_current_file_size_())) {
      LOG_WARN("failed to ensure current file size", K(ret), K_(current_file_id), K_(file_count));
    } else if (current_offset_ >= current_file_size_) {
      current_file_id_++;
      current_offset_ = 0;
      current_file_size_ = 0;
      switched_file = true;
      if (OB_FAIL(ensure_current_file_size_())) {
        LOG_WARN("failed to ensure file size after switch", K(ret), K_(current_file_id), K_(file_count));
      }
    }

    if (OB_SUCC(ret)) {
      if (switched_file) {
        LOG_INFO("switching to next file", K(current_file_id_), K(current_file_size_));
      }
      if (OB_FAIL(block_addr.set(current_file_id_, current_offset_, BLOCK_SIZE))) {
        LOG_WARN("failed to set block addr", K(ret), K(current_file_id_), K(current_offset_));
      } else if (OB_FAIL(read_block_(block_addr, buffer_reader))) {
        LOG_WARN("failed to read block", K(ret), K(block_addr));
      } else if (OB_FAIL(parse_block_headers_(buffer_reader, data_type))) {
        LOG_WARN("failed to parse block headers", K(ret));
      } else {
        item_count = block_file_header_.item_count_;
        read_block_count_++;
        current_offset_ += BLOCK_SIZE;

        const int64_t header_size =
            static_cast<int64_t>(sizeof(ObBackupCommonHeader)) + block_file_header_.get_serialize_size();
        const char *data = buffer_reader.data() + header_size;
        const int64_t data_len = common_header_.data_length_ - block_file_header_.get_serialize_size();
        if (OB_FALSE_IT(buffer_reader.assign(data, data_len))) {
          LOG_WARN("failed to assign buffer reader", K(ret), K(data_len));
        } else {
          LOG_DEBUG("success read block", K(block_addr), K(read_block_count_),
                   K(current_file_id_), K(current_offset_), K(current_file_size_));
        }
      }
    }
  }

  return ret;
}

// A 64MB macroblock list file is equivalent to storing the macroblock list of 4TB of tablet.
// Therefore, in most cases, the use of the is_exist method is sufficient here.
int ObBackupBlockFileReader::detect_file_count_()
{
  int ret = OB_SUCCESS;
  int64_t detected_count = 0;
  ObBackupPath path;

  // Count the number of files, and check one by one whether each file exists starting from 0.
  for (; OB_SUCC(ret); ++detected_count) {
    bool exist = false;
    path.reset();
    if (ObBackupBlockFileDataType::FILE_PATH_INFO == data_type_ && 1 == detected_count) {
      break;
    } else if (OB_FAIL(get_file_path_(detected_count, path))) {
      LOG_WARN("failed to get file path", K(ret), K(detected_count));
    } else if (OB_FAIL(ObBackupIoAdapter::adaptively_is_exist(path.get_obstr(), storage_info_, exist))) {
      LOG_WARN("failed to check file exist", K(ret), K(path));
    } else if (!exist) {
      LOG_DEBUG("can not find file", K(detected_count), K(path));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == detected_count) {
      ret = OB_BACKUP_FILE_NOT_EXIST;
      LOG_WARN("no backup files found", K(ret));
    } else {
      file_count_ = detected_count;
      current_file_size_ = 0;
      LOG_INFO("detected file count", K(detected_count));
    }
  }

  return ret;
}

int ObBackupBlockFileReader::ensure_current_file_size_()
{
  int ret = OB_SUCCESS;
  if (current_file_size_ > 0) {
    // already cached
  } else {
    ObBackupPath data_path;
    int64_t file_length = 0;
    if (current_file_id_ < 0 || current_file_id_ >= file_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current file id out of range", K(ret), K_(current_file_id), K_(file_count));
    } else if (OB_FAIL(get_file_path_(current_file_id_, data_path))) {
      LOG_WARN("failed to get file path", K(ret), K(current_file_id_));
    } else if (OB_FAIL(ObBackupIoAdapter::adaptively_get_file_length(
        data_path.get_obstr(), storage_info_, file_length))) {
      LOG_WARN("failed to get file length", K(ret), K(data_path));
    } else if (file_length <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid file length", K(ret), K(file_length), K_(current_file_id));
    } else {
      current_file_size_ = file_length;
    }
  }
  return ret;
}

int ObBackupBlockFileReader::read_last_file_tailer_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  int64_t file_length = 0;

  if (file_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file count", K(ret), K(file_count_));
  } else if (OB_FAIL(get_file_path_(file_count_ - 1, path))) {
    LOG_WARN("failed to get file path", K(ret), K(file_count_));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_get_file_length(path.get_obstr(), storage_info_, file_length))) {
    LOG_WARN("failed to get file length", K(ret), K(path));
  } else if (file_length < TAILER_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file too small to contain tailer", K(ret), K(file_length));
  } else {
    const int64_t tailer_size = file_length % BLOCK_SIZE;
    char *tailer_buf = nullptr;
    int64_t read_size = 0;
    const int64_t tailer_offset = file_length - tailer_size;

    if (tailer_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tailer size", K(ret), K(file_length), K(tailer_size));
    } else if (tailer_offset <= 0 || 0 != tailer_offset % BLOCK_SIZE) {
      ret = OB_INVALID_DATA;
      LOG_WARN("tailer offset mismatch with block alignment",
          K(ret), K(tailer_offset), K(BLOCK_SIZE), K(file_length));
    } else if (OB_ISNULL(tailer_buf = static_cast<char*>(allocator_.alloc(tailer_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buffer", K(ret));
    } else if (OB_FAIL(ObBackupIoAdapter::adaptively_read_part_file(
        path.get_obstr(),
        storage_info_,
        tailer_buf,
        tailer_size,
        tailer_offset,
        read_size,
        mod_))) {
      LOG_WARN("failed to read tailer", K(ret), K(path), K(tailer_offset));
    } else if (read_size != tailer_size) {
      ret = OB_IO_ERROR;
      LOG_WARN("read size mismatch", K(ret), K(read_size), K(tailer_size));
    } else {
      int64_t pos = 0;
      if (OB_FAIL(file_tailer_.deserialize(tailer_buf, tailer_size, pos))) {
        LOG_WARN("failed to deserialize tailer", K(ret));
      } else if (pos != tailer_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tailer deserialize size mismatch", K(ret), K(pos), K(tailer_size));
      } else if (OB_FAIL(file_tailer_.check_integrity())) {
        if (OB_CHECKSUM_ERROR == ret) {
          const uint64_t expected_checksum = file_tailer_.calc_checksum_for_log();
          LOG_WARN("tailer checksum mismatch", K(ret), K(expected_checksum),
              "file_tailer_checksum", file_tailer_.checksum_, K(file_tailer_));
        } else {
          LOG_WARN("invalid file tailer", K(ret), K(file_tailer_));
        }
      } else if (file_tailer_.last_file_id_ != file_count_ - 1) {
        ret = OB_INVALID_DATA;
        LOG_WARN("tailer last file id mismatch", K(ret), K_(file_count), K_(file_tailer));
      } else if (file_tailer_.file_length_ != tailer_offset) {
        ret = OB_INVALID_DATA;
        LOG_WARN("file tailer length mismatch",
            K(ret), K(file_tailer_.file_length_), K(tailer_offset), K(file_length));
      } else {
        LOG_INFO("success read file tailer from last file", K(file_tailer_), K(file_count_));
      }
    }
  }

  return ret;
}

int ObBackupBlockFileReader::read_block_(
    const ObBackupBlockFileAddr &block_addr,
    ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  int64_t read_size = 0;

  const int64_t file_id = block_addr.file_id_;

  if (OB_FAIL(get_file_path_(file_id, path))) {
    LOG_WARN("failed to get file path", K(ret), K(file_id));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_read_part_file(
      path.get_obstr(),
      storage_info_,
      buf_,
      block_addr.length_,
      block_addr.offset_,
      read_size,
      mod_))) {
    LOG_WARN("failed to read block", K(ret), K(path), K(block_addr));
  } else if (read_size != BLOCK_SIZE) {
    ret = OB_IO_ERROR;
    LOG_WARN("read size mismatch", K(ret), K(read_size), K(BLOCK_SIZE));
  } else if (OB_FALSE_IT(buffer_reader.assign(buf_, BLOCK_SIZE))) {
  } else {
    LOG_DEBUG("success read block from file", K(block_addr), K(file_id), K(read_size));
  }

  return ret;
}

int ObBackupBlockFileReader::parse_block_headers_(ObBufferReader &buffer_reader, int64_t &data_type)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *buf = buffer_reader.data();
  const int64_t buf_len = buffer_reader.capacity();

  if (buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer too small for common header", K(ret), K(buf_len));
  } else {
    MEMCPY(&common_header_, buf, sizeof(ObBackupCommonHeader));
    pos += sizeof(ObBackupCommonHeader);

    if (OB_FAIL(common_header_.check_valid())) {
      LOG_WARN("invalid common header", K(ret), K(common_header_));
    } else if (OB_FAIL(common_header_.check_data_checksum(
        buf + sizeof(ObBackupCommonHeader), common_header_.data_zlength_))) {
      LOG_WARN("data checksum mismatch", K(ret));
    } else if (OB_FAIL(block_file_header_.deserialize(buf, buf_len, pos))) {
      LOG_WARN("failed to deserialize block file header", K(ret));
    } else if (block_file_header_.version_ != ObBackupBlockFileHeader::VERSION
            || block_file_header_.magic_ != ObBackupBlockFileHeader::MAGIC) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid block file header", K(ret), K(block_file_header_));
    } else {
      data_type = block_file_header_.data_type_;
      LOG_DEBUG("success parse block headers", K(common_header_), K(block_file_header_));
    }
  }

  return ret;
}

int ObBackupBlockFileReader::get_file_path_(const int64_t file_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  path = file_list_dir_;
  if (ObBackupBlockFileDataType::FILE_PATH_INFO == data_type_) {
    if (OB_FAIL(path.join_file_list(file_id, suffix_))) {
      LOG_WARN("failed to join ss backup file list", K(ret), K(path));
    }
  } else if (ObBackupBlockFileDataType::MACRO_BLOCK_ID == data_type_) {
    if (OB_FAIL(path.join_backup_file_list(file_id))) {
      LOG_WARN("failed to join ss backup file list", K(ret), K(file_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data type", K(ret), K(data_type_));
  }

  return ret;
}

ObBackupBlockFileItemWriter::ObBackupBlockFileItemWriter()
  : is_inited_(false),
    is_closed_(false),
    block_writer_(),
    buffer_writer_("BkupItemWr", BLOCK_SIZE),
    block_addr_list_(),
    current_item_count_(0),
    total_item_count_(0),
    total_block_count_(0),
    current_block_data_type_(ObBackupBlockFileDataType::MAX_TYPE),
    allocator_("BkupItemWr"),
    mutex_(ObLatchIds::BACKUP_LOCK),
    max_file_size_()
{
}

ObBackupBlockFileItemWriter::~ObBackupBlockFileItemWriter()
{
}

int ObBackupBlockFileItemWriter::init(
    const common::ObObjectStorageInfo *storage_info,
    const ObBackupBlockFileDataType data_type,
    const share::ObBackupFileSuffix &suffix,
    const share::ObBackupPath &file_list_dir,
    const int64_t dest_id,
    const int64_t max_file_size)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupBlockFileItemWriter init twice", K(ret));
  } else if (OB_ISNULL(storage_info) || file_list_dir.is_empty()
                || ObBackupFileSuffix::NONE == suffix || ObBackupBlockFileDataType::MAX_TYPE == data_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(storage_info), K(file_list_dir),
                K(suffix), K(data_type), K(dest_id), K(max_file_size));
  } else if (OB_FAIL(block_writer_.init(storage_info, data_type, suffix, file_list_dir, dest_id, max_file_size))) {
    LOG_WARN("failed to init block writer", K(ret), KP(storage_info),
                K(data_type), K(suffix), K(file_list_dir), K(dest_id), K(max_file_size));
  } else {
    current_item_count_ = 0;
    total_item_count_ = 0;
    total_block_count_ = 0;
    current_block_data_type_ = MAX_TYPE;
    is_closed_ = false;
    is_inited_ = true;
  }

  return ret;
}

int ObBackupBlockFileItemWriter::write_item(const ObIBackupBlockFileItem &item)
{
  int ret = OB_SUCCESS;
  bool need_flush = false;
  lib::ObMutexGuard guard(mutex_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupBlockFileItemWriter not init", K(ret));
  } else if (is_closed_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("writer already closed", K(ret));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid item", K(ret));
  } else if (OB_FAIL(check_buffer_capacity_(item.get_serialize_size(), need_flush))) {
    LOG_WARN("failed to check buffer capacity", K(ret));
  } else if (need_flush && OB_FAIL(flush_buffer_())) {
    LOG_WARN("failed to flush buffer", K(ret));
  } else {
    if (current_item_count_ == 0) {
      current_block_data_type_ = item.data_type();
    } else if (current_block_data_type_ != item.data_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("mixed data types in same block", K(ret), K(current_block_data_type_), "new_type", item.data_type());
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t need_size = item.get_serialize_size();
    int64_t pos = 0;
    char *tmp = static_cast<char*>(allocator_.alloc(need_size));
    if (OB_ISNULL(tmp)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc serialize buf failed", K(ret), K(need_size));
    } else if (OB_FAIL(item.serialize(tmp, need_size, pos))) {
      LOG_WARN("serialize item failed", K(ret), K(need_size), K(pos));
    } else if (OB_FAIL(buffer_writer_.write(tmp, pos))) {
      LOG_WARN("append serialized data failed", K(ret), K(pos));
    } else {
      current_item_count_++;
      total_item_count_++;
      LOG_DEBUG("success write item", K(current_item_count_), K(total_item_count_));
    }
    if (nullptr != tmp) {
      allocator_.free(tmp);
    }
  }

  return ret;
}

int ObBackupBlockFileItemWriter::check_buffer_capacity_(const int64_t need_size, bool &need_flush)
{
  int ret = OB_SUCCESS;
  need_flush = false;

  const int64_t current_size = buffer_writer_.pos();
  const int64_t header_size = sizeof(ObBackupBlockFileHeader);

  if (current_size + need_size + header_size > BLOCK_SIZE - RESERVED_SIZE) {
    need_flush = true;
    LOG_DEBUG("buffer needs flush", K(current_size), K(need_size), K(current_item_count_));
  }

  return ret;
}

int ObBackupBlockFileItemWriter::flush_buffer_()
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter block_writer("BkupBlkItem", BLOCK_SIZE);

  if (current_item_count_ <= 0) {
    // do nothing
  } else if (OB_FAIL(build_block_data_(block_writer))) {
    LOG_WARN("failed to build block data", K(ret));
  } else {
    blocksstable::ObBufferReader buffer_reader;
    ObBackupBlockFileAddr block_addr;
    buffer_reader.assign(buffer_writer_.data(), buffer_writer_.length(), buffer_writer_.length());
    if (OB_FAIL(block_writer_.write_block(
        buffer_reader, current_item_count_, current_block_data_type_, block_addr))) {
      LOG_WARN("failed to write block", K(ret));
    } else if (OB_FAIL(block_addr_list_.push_back(block_addr))) {
      LOG_WARN("failed to add block addr", K(ret));
    } else {
      total_block_count_++;
      LOG_DEBUG("success flush block", K(block_addr), K(current_item_count_),
               K(total_block_count_));

      buffer_writer_.reuse();
      current_item_count_ = 0;
      current_block_data_type_ = MAX_TYPE;
    }
  }

  return ret;
}

int ObBackupBlockFileItemWriter::build_block_data_(
    blocksstable::ObSelfBufferWriter &block_writer)
{
  int ret = OB_SUCCESS;

  ObBackupBlockFileHeader block_file_header;
  block_file_header.version_ = ObBackupBlockFileHeader::VERSION;
  block_file_header.magic_ = ObBackupBlockFileHeader::MAGIC;
  block_file_header.data_type_ = current_block_data_type_;
  block_file_header.item_count_ = current_item_count_;

  if (!block_file_header.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block file header is not valid", K(ret), K(block_file_header));
  } else if (OB_FAIL(block_writer.write_serialize(block_file_header))) {
    LOG_WARN("failed to write block file header", K(ret), K(block_file_header));
  } else if (OB_FAIL(block_writer.write(buffer_writer_.data(), buffer_writer_.pos()))) {
    LOG_WARN("failed to write items data", K(ret), "pos", buffer_writer_.pos());
  } else {
    LOG_DEBUG("success build block data", K(block_file_header), "pos", buffer_writer_.pos());
  }

  return ret;
}

int ObBackupBlockFileItemWriter::close(
    ObBackupBlockFileAddr &entry_block_addr,
    int64_t &total_item_count,
    int64_t &total_block_count)
{
  int ret = OB_SUCCESS;
  entry_block_addr.reset();
  total_item_count = 0;
  total_block_count = 0;
  lib::ObMutexGuard guard(mutex_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupBlockFileItemWriter not init", K(ret));
  } else if (is_closed_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("writer already closed", K(ret));
  } else {
    if (current_item_count_ > 0) {
      if (OB_FAIL(flush_buffer_())) {
        LOG_WARN("failed to flush final buffer", K(ret));
      }
    }

    if (OB_SUCC(ret) && block_addr_list_.count() > 0) {
      entry_block_addr = block_addr_list_.at(0);
      if (OB_FAIL(block_writer_.close(entry_block_addr))) {
        LOG_WARN("failed to close block writer", K(ret));
      } else {
        total_item_count = total_item_count_;
        total_block_count = total_block_count_;
        is_closed_ = true;
        LOG_INFO("success close item writer", K(entry_block_addr),
                 K(total_item_count), K(total_block_count), K(current_block_data_type_));
      }
    }
  }

  return ret;
}

// Template Reader Implementation

ObBackupBlockFileItemReader::ObBackupBlockFileItemReader()
  : is_inited_(false),
    block_reader_(),
    current_buffer_(),
    current_pos_(0),
    current_item_count_(0),
    item_index_(0),
    current_data_type_(MACRO_BLOCK_ID),
    has_more_blocks_(true),
    entry_block_addr_(),
    total_block_count_(0),
    allocator_("BkupItemRd"),
    mutex_(ObLatchIds::BACKUP_LOCK)
{
}

ObBackupBlockFileItemReader::~ObBackupBlockFileItemReader()
{
}

int ObBackupBlockFileItemReader::init(
    const common::ObObjectStorageInfo *storage_info,
    const ObBackupBlockFileDataType data_type,
    const share::ObBackupPath &file_list_dir,
    const share::ObBackupFileSuffix &suffix,
    const ObStorageIdMod &mod)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupBlockFileItemReader init twice", K(ret));
  } else if (OB_ISNULL(storage_info) || file_list_dir.is_empty() || !mod.is_valid()
                || ObBackupFileSuffix::NONE == suffix || ObBackupBlockFileDataType::MAX_TYPE == data_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(storage_info), K(file_list_dir), K(mod), K(suffix), K(data_type));
  } else if (OB_FAIL(block_reader_.init(storage_info, data_type, file_list_dir, suffix, mod))) {
    LOG_WARN("failed to init block reader from file", K(ret), KP(storage_info), K(data_type),
                K(file_list_dir), K(suffix), K(mod));
  } else {
    current_pos_ = 0;
    current_item_count_ = 0;
    item_index_ = 0;
    has_more_blocks_ = true;

    if (OB_FAIL(load_next_block_())) {
      LOG_WARN("failed to load first block", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObBackupBlockFileItemReader::get_next_item(ObIBackupBlockFileItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  lib::ObMutexGuard guard(mutex_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupBlockFileItemReader not init", K(ret));
  } else {
    // Check if we need to load next block
    if (item_index_ >= current_item_count_) {
      if (!has_more_blocks_) {
        ret = OB_ITER_END;
        LOG_DEBUG("no more items", K(ret));
      } else if (OB_FAIL(load_next_block_())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to load next block", K(ret));
        }
      }
    }

    // Validate type matching
    if (OB_SUCC(ret)) {
      const int64_t expected_type = item.data_type();
      if (OB_FAIL(validate_block_type_(expected_type))) {
        LOG_WARN("block type mismatch", K(ret), K(expected_type), K(current_data_type_));
      }
    }

    // Unified deserialization
    if (OB_SUCC(ret) && item_index_ < current_item_count_) {
      const char *buf = current_buffer_.data();
      const int64_t buf_len = current_buffer_.capacity();

      if (OB_FAIL(item.deserialize(buf, buf_len, current_pos_))) {
        LOG_WARN("failed to deserialize item", K(ret), K(current_pos_), K(buf_len));
      } else {
        item_index_++;
        LOG_DEBUG("success get item", K(item_index_), K(current_item_count_));
      }
    }
  }

  return ret;
}

int ObBackupBlockFileItemReader::load_next_block_()
{
  int ret = OB_SUCCESS;
  ObBackupBlockFileAddr block_addr;
  int64_t item_count = 0;
  int64_t data_type = 0;

  current_pos_ = 0;
  current_item_count_ = 0;
  item_index_ = 0;

  if (OB_FAIL(block_reader_.get_next_block(current_buffer_, block_addr, item_count, data_type))) {
    if (OB_ITER_END == ret) {
      has_more_blocks_ = false;
      LOG_DEBUG("no more blocks to read");
    } else {
      LOG_WARN("failed to get next block", K(ret));
    }
  } else {
    current_data_type_ = data_type;
    current_item_count_ = item_count;
    has_more_blocks_ = true;  // Continue reading until OB_ITER_END
    LOG_DEBUG("success load block", K(block_addr), K(current_data_type_),
             K(current_item_count_));
  }

  return ret;
}

int ObBackupBlockFileItemReader::validate_block_type_(int64_t file_data_type)
{
  int ret = OB_SUCCESS;
  if (file_data_type != current_data_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data type mismatch", K(ret), K(file_data_type), K(current_data_type_));
  }
  return ret;
}

int ObBackupFileListWriterUtil::add_file_to_file_list_info(
    const ObBackupPath &file_path,
    const ObBackupStorageInfo storage_info,
    ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_util;
  int64_t file_size = 0;
  if (file_path.is_empty() || !storage_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path or storage info", KR(ret), K(file_path), K(storage_info));
  } else if (OB_FAIL(io_util.get_file_length(file_path.get_obstr(), &storage_info, file_size))) {
    LOG_WARN("failed to get file size", KR(ret), K(file_path), K(storage_info));
  } else if (OB_FAIL(file_list_info.push_file_info(file_path, file_size))) {
    LOG_WARN("failed to add file to list", KR(ret), K(file_list_info), K(file_path), K(file_size));
  }
  return ret;
}

int ObBackupFileListWriterUtil::add_file_to_file_list_info(
    const ObBackupPath &file_path,
    const share::ObIBackupSerializeProvider &file_serializer,
    ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  int64_t file_size = 0;
  share::ObBackupSerializeHeaderWrapper serializer_wrapper(&(const_cast<share::ObIBackupSerializeProvider &>(file_serializer)));
  file_size = serializer_wrapper.get_serialize_size();
  if (file_path.is_empty() || file_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path or file size", KR(ret), K(file_path), K(file_size));
  } else if (OB_FAIL(file_list_info.push_file_info(file_path, file_size))) {
    LOG_WARN("failed to add file to list", KR(ret), K(file_list_info), K(file_path), K(file_size));
  }
  return ret;
}

int ObBackupFileListWriterUtil::write_file_list_to_path(
    const common::ObObjectStorageInfo *storage_info,
    const share::ObBackupFileSuffix &suffix,
    const share::ObBackupPath &file_list_dir,
    const int64_t dest_id,
    ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info) || file_list_dir.is_empty() || ObBackupFileSuffix::NONE == suffix) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_info), K(file_list_dir), K(suffix));
  } else if (OB_INVALID_DEST_ID == dest_id || OB_INVALID_ID == dest_id || OB_START_DEST_ID > dest_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dest id", K(ret), K(dest_id));
  } else {
    ObBackupBlockFileItemWriter writer;
    const ObBackupBlockFileDataType data_type = ObBackupBlockFileDataType::FILE_PATH_INFO;
    file_list_info.sort_file_list();
    const int64_t max_file_size = OB_BACKUP_DEFAULT_MAX_FILE_SIZE;
    if (OB_FAIL(writer.init(storage_info, data_type, suffix, file_list_dir, dest_id, max_file_size))) {
      LOG_WARN("failed to init writer", KR(ret), K(storage_info), K(data_type),
                  K(suffix), K(file_list_dir), K(dest_id), K(max_file_size));
    } else {
      const int64_t file_count = file_list_info.file_list_.count();
      int tmp_ret = OB_SUCCESS;
      ObBackupBlockFileAddr entry_block_addr;
      int64_t total_item_count = -1;
      int64_t total_block_count = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < file_count; ++i) {
        const ObBackupFileInfo &file_info = file_list_info.file_list_.at(i);
        if (OB_FAIL(writer.write_item(file_info))) {
          LOG_WARN("failed to write file info", KR(ret), K(file_info));
        }
      }
      if (OB_TMP_FAIL(writer.close(entry_block_addr, total_item_count, total_block_count))) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("failed to close writer", KR(ret));
      }
    }
  }
  return ret;
}

// Get all directories from this directory (Level Order Traversal), return absolute paths
int ObBackupFileListReaderUtil::get_all_dir_list(
    const common::ObObjectStorageInfo *storage_info,
    const share::ObBackupPath &dir_path,
    const ObBackupFileSuffix &suffix,
    const common::ObStorageIdMod &storage_id_mod,
    common::ObIArray<ObBackupPathString> &dir_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info) || dir_path.is_empty() || !storage_id_mod.is_valid() || ObBackupFileSuffix::NONE == suffix) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_info), K(dir_path), K(storage_id_mod), K(suffix));
  } else {
    ObArray<ObBackupPathString> tmp_dir_list;;
    ObBackupPath cur_path;
    ObBackupPathString root_path;
    int64_t cur_index = 0;
    if (OB_FAIL(root_path.assign(dir_path.get_obstr()))) {
      LOG_WARN("failed to assign root path", KR(ret));
    } else if (OB_FAIL(dir_list.push_back(root_path))) {
      LOG_WARN("failed to push back root path", KR(ret));
    }
    while (OB_SUCC(ret) && cur_index < dir_list.count()) {
      cur_path.reset();
      tmp_dir_list.reset();
      if (OB_FAIL(cur_path.init(dir_list.at(cur_index).str()))) {
        LOG_WARN("failed to assign cur path", KR(ret));
      } else if (OB_FAIL(get_dir_list_(storage_info, cur_path, suffix, storage_id_mod, tmp_dir_list))) {
        LOG_WARN("failed to get dir list", KR(ret), K(cur_path), K(suffix), K(storage_id_mod));
      } else if (OB_FAIL(append(dir_list, tmp_dir_list))) {
        LOG_WARN("failed to append dir list", KR(ret));
      } else if (FALSE_IT(cur_index++)) {
      }
    }
  }
  return ret;
}

int ObBackupFileListReaderUtil::read_file_list_from_path(
    const common::ObObjectStorageInfo *storage_info,
    const share::ObBackupPath &file_list_dir,
    const share::ObBackupFileSuffix &suffix,
    const common::ObStorageIdMod &storage_id_mod,
    ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info) || file_list_dir.is_empty() || !storage_id_mod.is_valid() || ObBackupFileSuffix::NONE == suffix) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_info), K(file_list_dir), K(storage_id_mod), K(suffix));
  } else {
    file_list_info.reset();
    ObBackupBlockFileItemReader reader;
    ObBackupFileInfo item;
    const ObBackupBlockFileDataType data_type = ObBackupBlockFileDataType::FILE_PATH_INFO;
    if (OB_FAIL(reader.init(storage_info, data_type, file_list_dir, suffix, storage_id_mod))) {
      LOG_WARN("failed to init reader", KR(ret), K(file_list_dir), K(suffix), K(storage_id_mod));
    }
    while (OB_SUCC(ret)) {
      item.reset();
      if (OB_FAIL(reader.get_next_item(item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next item", KR(ret));
        }
      } else if (OB_FAIL(file_list_info.push_file_info(item))) {
        LOG_WARN("failed to push back item", KR(ret), K(item));
      }
    }
  }
  return ret;
}

int ObBackupFileListReaderUtil::get_dir_list_(
    const common::ObObjectStorageInfo *storage_info,
    const share::ObBackupPath &dir_path,
    const share::ObBackupFileSuffix &suffix,
    const common::ObStorageIdMod &storage_id_mod,
    common::ObIArray<share::ObBackupPathString> &dir_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info) || dir_path.is_empty() || !storage_id_mod.is_valid() || ObBackupFileSuffix::NONE == suffix) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_info), K(dir_path), K(storage_id_mod), K(suffix));
  } else {
    dir_list.reset();
    ObBackupBlockFileItemReader reader;
    ObBackupFileInfo item;
    ObBackupPathString child_dir_path;
    if (OB_FAIL(reader.init(storage_info, ObBackupBlockFileDataType::FILE_PATH_INFO, dir_path, suffix, storage_id_mod))) {
      LOG_WARN("failed to init reader", KR(ret), K(dir_path), K(suffix), K(storage_id_mod));
    }
    while (OB_SUCC(ret)) {
      item.reset();
      if (OB_FAIL(reader.get_next_item(item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next item", KR(ret));
        }
      } else if (item.is_dir()) {
        child_dir_path.reset();
        if (OB_FAIL(compose_absolute_path(dir_path, item.path_, child_dir_path))) {
          LOG_WARN("failed to compose child dir path", KR(ret), K(dir_path), K(item.path_));
        } else if (OB_FAIL(dir_list.push_back(child_dir_path))) {
          LOG_WARN("failed to push back item", KR(ret), K(item));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObBackupFileListReaderUtil::compose_absolute_path(
    const share::ObBackupPath &dir_path,
    const share::ObBackupPathString &relative_path,
    share::ObBackupPathString &child_dir_path)
{
  int ret = OB_SUCCESS;
  if (dir_path.is_empty() || relative_path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(dir_path), K(relative_path));
  } else if (OB_FAIL(child_dir_path.assign(dir_path.get_obstr()))) {
    LOG_WARN("failed to assign dir path", KR(ret), K(dir_path));
  } else {
    int64_t pos = child_dir_path.size();
    if (OB_FAIL(databuff_printf(child_dir_path.ptr(), child_dir_path.capacity(), pos, "/%s", relative_path.ptr()))) {
      LOG_WARN("failed to concat relative path", KR(ret), K(dir_path), K(relative_path));
    }
  }
  return ret;
}

int ObBackupFileListReaderUtil::get_ls_id_list(
    const common::ObObjectStorageInfo *storage_info,
    const share::ObBackupPath &file_list_dir,
    const share::ObBackupFileSuffix &suffix,
    const common::ObStorageIdMod &storage_id_mod,
    common::ObIArray<share::ObLSID> &ls_id_list)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(storage_info) || file_list_dir.is_empty()
          || !storage_id_mod.is_valid() || ObBackupFileSuffix::NONE == suffix) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_info), K(file_list_dir), K(storage_id_mod), K(suffix));
  } else {
    ObArray<ObBackupPathString> dir_list;
    ls_id_list.reset();
    ObBackupBlockFileItemReader reader;
    ObBackupFileInfo item;
    int64_t ls_id = 0;
    ObLSID ls_id_obj;
    if (OB_FAIL(reader.init(storage_info, ObBackupBlockFileDataType::FILE_PATH_INFO,
                                file_list_dir, suffix, storage_id_mod))) {
      LOG_WARN("failed to init reader", KR(ret), K(file_list_dir), K(suffix), K(storage_id_mod));
    }
    while (OB_SUCC(ret)) {
      item.reset();
      ls_id_obj.reset();
      if (OB_FAIL(reader.get_next_item(item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next item", KR(ret));
        }
      } else if (item.is_dir()) {
        if (OB_FAIL(item.path_.is_empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("item path is empty", KR(ret), K(item));
        } else if (0 != STRNCMP(OB_STR_LS, item.path_.ptr(), strlen(OB_STR_LS))) {
        } else if (OB_FAIL(share::ObBackupUtil::parse_ls_id(item.path_.ptr(), ls_id))) {
          LOG_WARN("failed to parse ls id", KR(ret), K(item.path_));
        } else if (FALSE_IT(ls_id_obj = ls_id)) {
        } else if (!ls_id_obj.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ls id", KR(ret), K(ls_id_obj));
        } else if (OB_FAIL(ls_id_list.push_back(ls_id_obj))) {
          LOG_WARN("failed to push back ls id", KR(ret), K(ls_id_obj));
        }
      } else {
        break;
      }
    }
  }

  return ret;
}

} // namespace backup
} // namespace oceanbase
