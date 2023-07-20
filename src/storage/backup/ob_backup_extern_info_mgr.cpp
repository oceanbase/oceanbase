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

#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "common/ob_smart_var.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_backup_restore_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

/* ObBackupLSMetaInfo */

OB_SERIALIZE_MEMBER(ObBackupLSMetaInfo, ls_meta_package_);

ObBackupLSMetaInfo::ObBackupLSMetaInfo() : ls_meta_package_()
{}

ObBackupLSMetaInfo::~ObBackupLSMetaInfo()
{}

bool ObBackupLSMetaInfo::is_valid() const
{
  return ls_meta_package_.is_valid();
}

int64_t ObBackupLSMetaInfo::get_total_serialize_buf_size() const
{
  return sizeof(ObBackupCommonHeader) + get_serialize_size();
}

int ObBackupLSMetaInfo::serialize_to(char *buf, int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t required_size = get_total_serialize_buf_size();
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret), KP(buf));
  } else if (buf_size - pos < required_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not enough", K(ret), K(buf_size), K(pos), K(required_size));
  } else {
    // serialize common header.
    ObBackupCommonHeader *common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->reset();
    common_header->compressor_type_ = 0;
    common_header->data_type_ = 0;
    common_header->data_version_ = 0;
    common_header->header_length_ = sizeof(ObBackupCommonHeader);
    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    // serialize self.
    if (OB_FAIL(serialize(buf, buf_size, pos))) {
      LOG_WARN("failed to serialize", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupLSMetaInfo::deserialize_from(char *buf, int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf size is too small", K(ret), K(buf_size), K(sizeof(ObBackupCommonHeader)));
  } else {
    ObBackupCommonHeader *common_header = reinterpret_cast<ObBackupCommonHeader *>(buf);
    int64_t pos = common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_size - pos) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf size is too small", K(ret), K(buf_size), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_WARN("failed to check round start desc checksum", K(ret), K(*common_header));
    } else if (OB_FAIL(deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize round start desc", K(ret), K(*common_header));
    }
  }
  return ret;
}

/* ObExternLSMetaMgr */

ObExternLSMetaMgr::ObExternLSMetaMgr()
    : is_inited_(false),
      backup_dest_(),
      backup_set_desc_(),
      ls_id_(),
      turn_id_(),
      retry_id_()
{}

ObExternLSMetaMgr::~ObExternLSMetaMgr()
{}

int ObExternLSMetaMgr::init(const ObBackupDest &backup_dest, const ObBackupSetDesc &backup_set_desc,
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern backup data mgr init twice.", K(ret));
  } else if (!backup_dest.is_valid() || !backup_set_desc.is_valid() ||
             !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_set_desc), K(ls_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    backup_set_desc_ = backup_set_desc;
    ls_id_ = ls_id;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    is_inited_ = true;
  }
  return ret;
}

int ObExternLSMetaMgr::write_ls_meta_info(const ObBackupLSMetaInfo &ls_meta)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  int64_t buf_size = ls_meta.get_total_serialize_buf_size();
  ObArenaAllocator allocator;
  ObBackupPath path;
  ObBackupIoAdapter util;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size), K(ls_meta));
  } else if (OB_FAIL(ls_meta.serialize_to(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize ls meta info.", K(ret), K(buf_size), K(ls_meta));
  } else if (pos != buf_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialized size not match.", K(ret), K(pos), K(buf_size), K(ls_meta));
  } else if (OB_FAIL(get_ls_meta_backup_path_(path))) {
    LOG_WARN("failed to get ls meta backup path", K(ret), K(ls_meta));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_obstr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(ret), K(path));
  } else if (OB_FAIL(util.write_single_file(path.get_obstr(), backup_dest_.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write tenant tablet logstream info.", K(ret), K(path), K(ls_meta));
  } else {
    LOG_INFO("succeed to write ls meta info", K(path), K(ls_meta));
  }
  return ret;
}

int ObExternLSMetaMgr::read_ls_meta_info(ObBackupLSMetaInfo &ls_meta)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char *buf = nullptr;
  int64_t read_size = 0;
  ObBackupPath path;
  ObArenaAllocator allocator;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup extern mgr not init.", K(ret));
  } else if (OB_FAIL(get_ls_meta_backup_path_(path))) {
    LOG_WARN("failed to get backup ls meta path", K(ret));
  } else if (OB_FAIL(util.get_file_length(path.get_obstr(), backup_dest_.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get ls meta backup path file length.", K(ret), K(path));
    } else {
      LOG_INFO("tablet to ls info file not exist.", K(ret), K(path));
    }
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("tablet to ls info file is empty.", K(ret), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(path), K(file_length));
  } else if (OB_FAIL(util.read_single_file(path.get_obstr(), backup_dest_.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read tablet to ls info file.", K(ret), K(path), K(file_length));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match.", K(ret), K(path), K(file_length), K(read_size));
  } else if (OB_FAIL(ls_meta.deserialize_from(buf, file_length))) {
    LOG_WARN("failed to deserialize tablet to ls info.", K(ret), K(path), K(file_length));
  } else {
    LOG_INFO("succeed to read ls meta info", K(path), K(ls_meta));
  }
  return ret;
}

int ObExternLSMetaMgr::get_ls_meta_backup_path_(ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupPathUtil::get_ls_meta_info_backup_path(backup_dest_, backup_set_desc_, ls_id_, turn_id_, 
      retry_id_, path))) {
    LOG_WARN("failed to get ls meta info backup path", K(ret), K_(backup_dest), K_(backup_set_desc), K_(ls_id), 
        K_(turn_id), K_(retry_id));
  }
  return ret;
}

void ObTabletInfoTrailer::reset()
{
  file_id_ = 0;
  tablet_cnt_ = 0;
  offset_ = 0;
  length_ = 0;
}

int ObTabletInfoTrailer::assign(const ObTabletInfoTrailer &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(that));
  } else {
    file_id_ = that.file_id_;
    tablet_cnt_ = that.tablet_cnt_;
    offset_ = that.offset_;
    length_ = that.length_;
  }
  return ret;
}

bool ObTabletInfoTrailer::is_valid() const
{
  return file_id_ >= 0 && tablet_cnt_ >= 0 && offset_ >= 0 && length_ >= 0;
}

int ObTabletInfoTrailer::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  // custom serialization, the format is as following:
  // | VER | LEN | Member1 | Member2 | ... |
  int ret = OB_SUCCESS;
  int64_t len = get_serialize_size();
  if (buf_len - pos < len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf length not enough", K(ret), K(buf_len), K(pos), K(len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, UNIS_VERSION))) {
    LOG_WARN("failed to encode version", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, len))) {
    LOG_WARN("failed to encode len", K(ret));
  } else if (OB_FAIL(serialize_(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize_", K(ret));
  }

  return ret;
}

int ObTabletInfoTrailer::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t ver = 0;
  int64_t len = 0;
  int64_t tmp_pos = pos;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &ver))) {
    LOG_WARN("failed to decode version", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &len))) {
    LOG_WARN("failed to decode len", K(ret));
  } else if (ver != UNIS_VERSION) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("object version mismatch", K(ret), K(ver));
  } else if (data_len < len + pos) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("buf length not enough", K(ret), K(len), K(pos), K(data_len));
  } else if (OB_FALSE_IT(pos = tmp_pos)) {
  } else if (OB_FAIL(deserialize_(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize_", K(ret));
  }

  return ret;
}

int64_t ObTabletInfoTrailer::get_serialize_size() const
{
  return sizeof(int64_t)/* VER */ + sizeof(int64_t)/* LEN */ + get_serialize_size_();
}

int ObTabletInfoTrailer::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, file_id_))) {
    LOG_WARN("failed to encode file_id", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, tablet_cnt_))) {
    LOG_WARN("failed to encode table_cnt", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, offset_))) {
    LOG_WARN("failed to encode offset", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, length_))) {
    LOG_WARN("failed to encode length", K(ret));
  }
  return ret;
}

int ObTabletInfoTrailer::deserialize_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &file_id_))) {
    LOG_WARN("failed to decode file_id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tablet_cnt_))) {
    LOG_WARN("failed to decode tablet_cnt", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &offset_))) {
    LOG_WARN("failed to decode offset", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &length_))) {
    LOG_WARN("failed to decode length", K(ret));
  }
  return ret;
}

int64_t ObTabletInfoTrailer::get_serialize_size_() const
{
  return sizeof(file_id_) + sizeof(tablet_cnt_) + sizeof(offset_) + sizeof(length_);
}

int ObExternTabletMetaWriter::init(
    const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id,
    const int64_t turn_id, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_file_id = 1;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet meta writer init twice", K(ret));
  } else if (!backup_set_dest.is_valid() || !ls_id.is_valid() || turn_id < 1 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_dest), K(ls_id), K(turn_id), K(retry_id));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("failed to deep copy backup set dest", K(ret));
  } else if (OB_FAIL(tmp_buffer_.ensure_space(BUF_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else {
    ls_id_ = ls_id;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    if (OB_FAIL(prepare_backup_file_(start_file_id))) {
      LOG_WARN("failed to prepare backup file", K(ret), K(start_file_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObExternTabletMetaWriter::prepare_backup_file_(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  common::ObBackupIoAdapter util;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_RANDOMWRITER;
  const int64_t data_file_size = get_data_file_size();
  if (OB_FAIL(ObBackupPathUtil::get_ls_data_tablet_info_path(
      backup_set_dest_, ls_id_, turn_id_, retry_id_, file_id, backup_path))) {
    LOG_WARN("failed to get ls data tablet info path", K(ret), K(backup_set_dest_), K(ls_id_), K(turn_id_), K(retry_id_));
  } else if (OB_FAIL(util.mk_parent_dir(backup_path.get_obstr(), backup_set_dest_.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(backup_path));
  } else if (OB_FAIL(util.open_with_access_type(
      dev_handle_, io_fd_, backup_set_dest_.get_storage_info(), backup_path.get_obstr(), access_type))) {
    LOG_WARN("failed to open with access type", K(ret), K(backup_set_dest_), K(backup_path));
  } else if (OB_FAIL(file_write_ctx_.open(data_file_size, io_fd_, *dev_handle_))) {
    LOG_WARN("failed to open file write ctx", K(ret), K(backup_path), K(data_file_size), K(file_id));
  } else {
    file_trailer_.reset();
    file_trailer_.file_id_ = file_id;
    LOG_INFO("open file writer", K(ret), K(backup_path));
  }
  return ret;
}

int ObExternTabletMetaWriter::write_meta_data(
    const blocksstable::ObBufferReader &meta_data, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet meta writer not inited", K(ret));
  } else if (!meta_data.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta_data), K(tablet_id));
  } else if (need_switch_file_(meta_data) && OB_FAIL(switch_file_())) {
    LOG_WARN("failed to switch file", K(ret));
  } else if (OB_FAIL(write_meta_data_(meta_data, tablet_id))) {
    LOG_WARN("failed to write meta data", K(ret), K(tablet_id));
  } else {
    LOG_INFO("write meta data", K(meta_data), K(tablet_id));
  }
  return ret;
}

bool ObExternTabletMetaWriter::need_switch_file_(const blocksstable::ObBufferReader &buffer)
{
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  const int64_t align_size = common::upper_align(header_len + buffer.length(), DIO_READ_ALIGN_SIZE);
  return file_trailer_.length_ + align_size + TRAILER_BUF > get_data_file_size();
}

int ObExternTabletMetaWriter::switch_file_()
{
  int ret = OB_SUCCESS;
  int64_t next_file_id = file_trailer_.file_id_ + 1;
  if (OB_FAIL(close())) {
    LOG_WARN("failed to close", K(ret));
  } else if (OB_FAIL(prepare_backup_file_(next_file_id))) {
    LOG_WARN("failed to prepare backup file", K(ret));
  }
  return ret;
}

int ObExternTabletMetaWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet meta writer not inited", K(ret));
  } else if (OB_FAIL(flush_trailer_())) {
    LOG_WARN("failed to flush trailer", K(ret));
  } else if (OB_FAIL(file_write_ctx_.close())) {
    LOG_WARN("failed to close file writer", K(ret));
  }
  return ret;
}

int ObExternTabletMetaWriter::flush_trailer_()
{
  int ret = OB_SUCCESS;
  tmp_buffer_.reuse();
  ObBackupSerializeHeaderWrapper serializer_wrapper(&file_trailer_);
  int64_t pos = 0;
  int64_t buf_len = serializer_wrapper.get_serialize_size();
  if (OB_FAIL(tmp_buffer_.advance_zero(buf_len))) {
    LOG_WARN("failed to advance zero", K(ret));
  } else if (OB_FAIL(serializer_wrapper.serialize(tmp_buffer_.data(), tmp_buffer_.pos(), pos))) {
    LOG_WARN("failed to serialize", K(ret), K(ret), K(tmp_buffer_));
  } else {
    blocksstable::ObBufferReader buffer_reader(tmp_buffer_.data(), tmp_buffer_.length(), tmp_buffer_.length());
    if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(buffer_reader));
    } else {
      LOG_INFO("flush data file trailer", K(file_trailer_));
    }
  }
  return ret;
}

int ObExternTabletMetaWriter::write_meta_data_(
    const blocksstable::ObBufferReader &meta_data, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const int64_t alignment = DIO_READ_ALIGN_SIZE;
  const share::ObBackupFileType type = share::ObBackupFileType::BACKUP_TABLET_METAS_INFO;
  if (OB_FAIL(write_data_align_(meta_data, type, alignment))) {
    LOG_WARN("failed to write data align", K(ret), K(meta_data));
  } else {
    int64_t length = 0;
    blocksstable::ObBufferReader buffer_reader(tmp_buffer_.data(), tmp_buffer_.pos(), tmp_buffer_.pos());
    if (OB_FAIL(file_write_ctx_.append_buffer(buffer_reader))) {
      LOG_WARN("failed to append buffer", K(ret), K(tmp_buffer_), K(buffer_reader));
    } else if (FALSE_IT(length = tmp_buffer_.pos())) {
    } else {
      file_trailer_.tablet_cnt_++;
      file_trailer_.length_ += length;
    }
  }
  return ret;
}

int ObExternTabletMetaWriter::write_data_align_(
    const blocksstable::ObBufferReader &buffer, const share::ObBackupFileType &type, const int64_t alignment)
{
  int ret = OB_SUCCESS;
  tmp_buffer_.reuse();
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  const int64_t align_size = common::upper_align(header_len + buffer.length(), alignment);
  const int64_t align_length = align_size - buffer.length() - header_len;
  ObBackupCommonHeader *common_header = NULL;
  if (!buffer.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(buffer));
  } else if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
    LOG_WARN("advance failed", K(ret), K(header_len));
  } else if (OB_FAIL(tmp_buffer_.write(buffer.data(), buffer.length()))) {
    LOG_WARN("failed to write data", K(ret), K(buffer));
  } else if (OB_FAIL(tmp_buffer_.advance_zero(align_length))) {
    LOG_WARN("failed to advance zero", K(ret), K(align_length));
  } else if (FALSE_IT(common_header = reinterpret_cast<ObBackupCommonHeader *>(tmp_buffer_.data()))) {
  } else if (OB_FAIL(build_common_header(type, buffer.length(), align_length, common_header))) {
    LOG_WARN("failed to build common header", K(ret), K(type), K(buffer), K(align_length));
  } else if (OB_FAIL(common_header->set_checksum(tmp_buffer_.data() + common_header->header_length_, buffer.length()))) {
    LOG_WARN("failed to set common header checksum", K(ret), K(tmp_buffer_), K(buffer), K(*common_header));
  }
  return ret;
}

int ObExternTabletMetaReader::init(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet meta reader init twice", K(ret), K(backup_set_dest), K(ls_id));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("failed to assign backup dest", K(ret));
  } else if (OB_FAIL(fill_tablet_info_trailer_(backup_set_dest, ls_id))) {
    LOG_WARN("failed to fill tablet info trailer", K(ret), K(backup_set_dest), K(ls_id));
  } else {
    ls_id_ = ls_id;
    is_inited_ = true;
  }
  return ret;
}

int ObExternTabletMetaReader::fill_tablet_info_trailer_(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoIdGetter id_getter;
  ObArray<int64_t> file_ids;
  if (OB_FAIL(id_getter.init(backup_set_dest))) {
    LOG_WARN("failed to init id getter", K(ret), K(backup_set_dest), K(ls_id));
  } else if (OB_FAIL(id_getter.get_max_turn_id_and_retry_id(ls_id, turn_id_, retry_id_))) {
    LOG_WARN("failed to get max turn_id and retry_id", K(ret));
  } else if (OB_FAIL(id_getter.get_tablet_info_file_ids(ls_id, turn_id_, retry_id_, file_ids))) {
    LOG_WARN("failed to get tablet info file ids", K(ret), K(turn_id_), K(retry_id_));
  } else {
    ARRAY_FOREACH(file_ids, i) {
      share::ObBackupPath path;
      ObTabletInfoTrailer trailer;
      if (OB_FAIL(ObBackupPathUtil::get_ls_data_tablet_info_path(backup_set_dest, ls_id, turn_id_, retry_id_, file_ids.at(i), path))) {
        LOG_WARN("failed to get ls data tablet info path", K(ret), K(backup_set_dest_), K(ls_id_), K(turn_id_), K(retry_id_));
      } else if (OB_FAIL(read_file_trailer_(path.get_obstr(), backup_set_dest.get_storage_info(), trailer))) {
        LOG_WARN("failed to read file trailer", K(ret), K(path));
      } else if (OB_FAIL(tablet_info_trailer_array_.push_back(trailer))) {
        LOG_WARN("failed to push back trailer", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      cur_trailer_idx_ = 0;
      cur_buf_offset_ = tablet_info_trailer_array_.at(cur_trailer_idx_).offset_;
      cur_tablet_idx_ = 0;
      LOG_INFO("fill tablet info trailer", K(tablet_info_trailer_array_));
    }
  }
  return ret;
}

int ObExternTabletMetaReader::read_file_trailer_(
    const common::ObString &path, const share::ObBackupStorageInfo *storage_info, ObTabletInfoTrailer &trailer)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_util;
  bool exist = false;
  int64_t file_length = 0;
  char *buf = NULL;
  ObArenaAllocator allocator;
  ObTabletInfoTrailer tmp_trailer;
  ObBackupSerializeHeaderWrapper serializer_wrapper(&tmp_trailer);
  const int64_t trailer_len = serializer_wrapper.get_serialize_size();
  int64_t pos = 0;
  if (OB_FAIL(io_util.is_exist(path, storage_info, exist))) {
    LOG_WARN("failed to check file exist", K(ret), K(path), KP(storage_info));
  } else if (OB_UNLIKELY(!exist)) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    LOG_WARN("index file do not exist", K(ret), K(path));
  } else if (OB_FAIL(io_util.get_file_length(path, storage_info, file_length))) {
    LOG_WARN("failed to get file length", K(ret), K(path), KP(storage_info));
  } else if (OB_UNLIKELY(file_length <= trailer_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup index file too small", K(ret), K(file_length), K(trailer_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(trailer_len));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::pread_file(path, storage_info, file_length - trailer_len, trailer_len, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(path), KP(storage_info), K(file_length), K(trailer_len));
  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, trailer_len, pos))) {
    LOG_WARN("failed to deserialize.", K(ret));
  } else if (OB_FAIL(trailer.assign(tmp_trailer))) {
    LOG_WARN("failed to assign trailer", K(tmp_trailer));
  } else {
    LOG_INFO("read trailer succeed", K(trailer));
  }
  return ret;
}

int ObExternTabletMetaReader::get_next(storage::ObMigrationTabletParam &tablet_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet meta reader not init", K(ret));
  } else if (end_() && OB_FAIL(read_next_batch_())) {
    LOG_WARN("failed to update inner array", K(ret));
  } else if (OB_FAIL(tablet_meta.assign(tablet_meta_array_.at(cur_tablet_idx_)))) {
    LOG_WARN("failed to assign tablet meta", K(ret), K(cur_tablet_idx_), K(tablet_meta_array_));
  } else if (OB_FALSE_IT(cur_tablet_idx_++)) {
  }
  return ret;
}

bool ObExternTabletMetaReader::end_()
{
  return tablet_meta_array_.count() == cur_tablet_idx_;
}

int ObExternTabletMetaReader::read_next_batch_()
{
  int ret = OB_SUCCESS;
  if (cur_buf_offset_ < tablet_info_trailer_array_.at(cur_trailer_idx_).length_) {
    if (OB_FAIL(read_next_range_tablet_metas_())) {
      LOG_WARN("failed to get next range tablet metas", K(ret), K(cur_buf_offset_), K(cur_trailer_idx_), K(tablet_info_trailer_array_));
    }
  } else if (cur_trailer_idx_ < tablet_info_trailer_array_.count() - 1) {
    cur_buf_offset_ = tablet_info_trailer_array_.at(++cur_trailer_idx_).offset_;
    if (OB_FAIL(read_next_range_tablet_metas_())) {
      LOG_WARN("failed to get next range tablet metas", K(ret), K(cur_buf_offset_), K(cur_trailer_idx_), K(tablet_info_trailer_array_));
    }
  } else {
    ret = OB_ITER_END;
    LOG_INFO("iterate to the end", K(ret),
        K(ls_id_), K(retry_id_), K(turn_id_), K(cur_buf_offset_), K(cur_trailer_idx_), K(tablet_info_trailer_array_),
        K(cur_tablet_idx_), K(tablet_meta_array_));
  }
  return ret;
}

int ObExternTabletMetaReader::read_next_range_tablet_metas_()
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  char *buf = nullptr;
  const int64_t DEFAULT_BUF_LEN = 2 * 1024 * 1024; // 2M
  const int64_t buf_len = tablet_info_trailer_array_.at(cur_trailer_idx_).length_ - cur_buf_offset_ < DEFAULT_BUF_LEN ?
                          (tablet_info_trailer_array_.at(cur_trailer_idx_).length_ - cur_buf_offset_) : DEFAULT_BUF_LEN;
  int64_t cur_total_len = 0;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  const int64_t file_id = tablet_info_trailer_array_.at(cur_trailer_idx_).file_id_;
  if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(buf_len));
  } else if (OB_FAIL(ObBackupPathUtil::get_ls_data_tablet_info_path(backup_set_dest_, ls_id_, turn_id_, retry_id_, file_id, path))) {
    LOG_WARN("failed to get ls data tablet info path", K(ret), K(backup_set_dest_), K(ls_id_), K(turn_id_), K(retry_id_));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::pread_file(path.get_obstr(), backup_set_dest_.get_storage_info(), cur_buf_offset_, buf_len, buf))) {
    LOG_WARN("failed to pread buffer", K(ret), K(path), K(buf_len));
  } else {
    storage::ObMigrationTabletParam tablet_meta;
    ObArray<storage::ObMigrationTabletParam> cur_tablet_meta_array;
    blocksstable::ObBufferReader buffer_reader(buf, buf_len);
    // deserialize tablet meta one by one.
    while(OB_SUCC(ret)) {
      tablet_meta.reset();
      int64_t pos = 0;
      const ObBackupCommonHeader *common_header = NULL;
      if (buffer_reader.remain() == 0) {
        cur_total_len = buffer_reader.capacity();
        LOG_INFO("read buf finish", K(cur_total_len), K(buffer_reader));
        break;
      } else if (OB_FAIL(buffer_reader.get(common_header))) {
        LOG_WARN("failed to get common_header", K(ret), K(path), K(buffer_reader));
      } else if (OB_ISNULL(common_header)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("common header is null", K(ret), K(path), K(buffer_reader));
      } else if (OB_FAIL(common_header->check_valid())) {
        LOG_WARN("common_header is not valid", K(ret), K(path), K(buffer_reader));
      } else if (common_header->data_zlength_ > buffer_reader.remain()) {
        cur_total_len = buffer_reader.pos() - sizeof(ObBackupCommonHeader);
        LOG_INFO("buf not enough, wait later", K(cur_total_len), K(buffer_reader));
        break;
      } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
        LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(path), K(buffer_reader));
      } else if (OB_FAIL(tablet_meta.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {
        LOG_WARN("failed to read data_header", K(ret), K(*common_header), K(path), K(buffer_reader));
      } else if (OB_FAIL(cur_tablet_meta_array.push_back(tablet_meta))) {
        LOG_WARN("failed to push back tablet meta", K(ret));
      } else if (OB_FAIL(buffer_reader.advance(common_header->data_length_ + common_header->align_length_))) {
        LOG_WARN("failed to advance buffer", K(ret));
      } else {
        LOG_INFO("read tablet meta", K(path), K(tablet_meta));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      tablet_meta_array_.reset();
      if (OB_FAIL(tablet_meta_array_.assign(cur_tablet_meta_array))) {
        LOG_WARN("failed to assign tablet meta array", K(ret));
      } else {
        cur_buf_offset_ += cur_total_len;
        cur_tablet_idx_ = 0;
        LOG_INFO("read range tablet metas", K(cur_tablet_idx_), K(tablet_meta_array_));
      }
    }
  }
  return ret;
}

int ObExternBackupInfoIdGetter::init(const share::ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls meta info id getter init twice", K(ret));
  } else if (!backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("failed to deep copy backup set dest", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObExternBackupInfoIdGetter::get_max_turn_id_and_retry_id(const share::ObLSID &ls_id, int64_t &turn_id, int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObLSMetaInfoDirFilter filter;
  share::ObBackupPath backup_path;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("id getter not inited", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest_, ls_id, backup_path))) {
    LOG_WARN("failed to get ls backup dier path", K(ret), K(backup_set_dest_), K(ls_id));
  } else if (OB_FAIL(util.list_directories(backup_path.get_obstr(), backup_set_dest_.get_storage_info(), filter))) {
    LOG_WARN("failed to list directories", K(ret), K(backup_path), K(backup_set_dest_));
  } else if (filter.turn_id() <= 0 || filter.retry_id() < 0) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    LOG_WARN("invalid turn id and retry id, may not have meta info dir", K(ret), K(filter), K(backup_path));
  } else {
    turn_id = filter.turn_id();
    retry_id = filter.retry_id();
    LOG_INFO("get max turn_id and retry_id", K(turn_id), K(retry_id), K(backup_path));
  }
  return ret;
}

int ObExternBackupInfoIdGetter::get_tablet_info_file_ids(
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, ObIArray<int64_t> &file_id_array)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObLSTabletInfoIdFilter filter;
  share::ObBackupPath backup_path;
  file_id_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("id getter not inited", K(ret));
  } else if (turn_id <= 0 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(turn_id), K(retry_id));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest_, ls_id, backup_path))) {
    LOG_WARN("failed to get ls backup dier path", K(ret), K(backup_set_dest_), K(ls_id));
  } else if (OB_FAIL(backup_path.join_meta_info_turn_and_retry(turn_id, retry_id))) {
    LOG_WARN("failed to join meta info turn and retry", K(ret));
  } else if (OB_FAIL(filter.init())) {
    LOG_WARN("failed to inited", K(ret));
  } else if (OB_FAIL(util.list_files(backup_path.get_obstr(), backup_set_dest_.get_storage_info(), filter))) {
    LOG_WARN("failed to list directories", K(ret), K(backup_path), K(backup_set_dest_));
  } else if (OB_FAIL(filter.get_file_id_array(file_id_array))) {
    LOG_WARN("failed to get file id array", K(ret));
  } else {
    LOG_INFO("succeed get tablet info file ids", K(file_id_array));
  }
  return ret;
}

int ObExternBackupInfoIdGetter::ObLSMetaInfoDirFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObString dir_name(entry->d_name);
  int64_t cur_turn_id = 0;
  int64_t cur_retry_id = 0;
  if (2 != sscanf(dir_name.ptr(), "meta_info_turn_%ld_retry_%ld", &cur_turn_id, &cur_retry_id)) {
  } else if (cur_turn_id > turn_id_) {
    turn_id_ = cur_turn_id;
    retry_id_ = cur_retry_id;
  } else if (cur_turn_id == turn_id_ && cur_retry_id > retry_id_) {
    retry_id_ = cur_retry_id;
  }
  return ret;
}

int ObExternBackupInfoIdGetter::ObLSTabletInfoIdFilter::init()
{
  int ret = OB_SUCCESS;
  const int64_t default_bucket_num  = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSTabletInfoIdFilter init twice", K(ret));
  } else if (OB_FAIL(file_id_set_.create(default_bucket_num))) {
    LOG_WARN("failed to create id set", K(ret), K(default_bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObExternBackupInfoIdGetter::ObLSTabletInfoIdFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObString file_name(entry->d_name);
  int64_t file_id = 0;
  if (1 != sscanf(file_name.ptr(), "tablet_info.%ld", &file_id)) {
  } else if (file_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file id", K(ret), K(file_id));
  } else if (OB_FAIL(file_id_set_.set_refactored(file_id, 0/*not cover exist object*/))) {
    LOG_WARN("failed to set refactored", K(ret), K(file_id));
  }
  return ret;
}

int ObExternBackupInfoIdGetter::ObLSTabletInfoIdFilter::get_file_id_array(ObIArray<int64_t> &file_id_array)
{
  int ret = OB_SUCCESS;
  if (file_id_set_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file id must not be empty", K(ret));
  } else {
    for (FileIdSet::const_iterator iter = file_id_set_.begin(); OB_SUCC(ret) && iter != file_id_set_.end(); iter++) {
      if (OB_FAIL(file_id_array.push_back(iter->first))) {
        LOG_WARN("failed to push backup", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
