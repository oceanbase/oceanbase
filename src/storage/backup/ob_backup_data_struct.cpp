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

#include "storage/backup/ob_backup_data_struct.h"
#include "common/ob_record_header.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/backup/ob_backup_task.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/location_cache/ob_location_service.h"
#include "share/backup/ob_backup_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace backup {

/* ObBackupJobDesc */

ObBackupJobDesc::ObBackupJobDesc() : job_id_(-1), task_id_(-1), trace_id_()
{}

bool ObBackupJobDesc::is_valid() const
{
  return job_id_ > 0 && task_id_ > 0;
}

bool ObBackupJobDesc::operator==(const ObBackupJobDesc &other) const
{
  return job_id_ == other.job_id_ && task_id_ == other.task_id_;
}

/* ObLSBackupParam */

ObLSBackupParam::ObLSBackupParam()
    : job_id_(0), task_id_(0), backup_dest_(), tenant_id_(0), dest_id_(0), backup_set_desc_(), ls_id_(), turn_id_(0), retry_id_(0)
{}

ObLSBackupParam::~ObLSBackupParam()
{}

bool ObLSBackupParam::is_valid() const
{
  return job_id_ > 0 && task_id_ > 0 && backup_dest_.is_valid() && OB_INVALID_ID != tenant_id_ && dest_id_ > 0
      && backup_set_desc_.is_valid() && ls_id_.is_valid() && turn_id_ >= 0 && retry_id_ >= 0;
}

int ObLSBackupParam::assign(const ObLSBackupParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param));
  } else if (OB_FAIL(backup_dest_.deep_copy(param.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret));
  } else {
    job_id_ = param.job_id_;
    task_id_ = param.task_id_;
    tenant_id_ = param.tenant_id_;
    dest_id_ = param.dest_id_;
    backup_set_desc_ = param.backup_set_desc_;
    ls_id_ = param.ls_id_;
    turn_id_ = param.turn_id_;
    retry_id_ = param.retry_id_;
  }
  return ret;
}

int ObLSBackupParam::convert_to(const ObBackupIndexLevel &index_level, const share::ObBackupDataType &backup_data_type,
    ObBackupIndexMergeParam &merge_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup param is not valid", K(ret), K(*this));
  } else if (OB_FAIL(merge_param.backup_dest_.deep_copy(backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K_(backup_dest));
  } else {
    merge_param.tenant_id_ = tenant_id_;
    merge_param.dest_id_ = dest_id_;
    merge_param.backup_set_desc_ = backup_set_desc_;
    merge_param.backup_data_type_ = backup_data_type;
    merge_param.index_level_ = index_level;
    merge_param.ls_id_ = ls_id_;
    merge_param.turn_id_ = turn_id_;
    merge_param.retry_id_ = retry_id_;
  }
  return ret;
}

/* ObBackupIndexMergeParam */

ObBackupIndexMergeParam::ObBackupIndexMergeParam()
    : task_id_(0),
      backup_dest_(),
      tenant_id_(OB_INVALID_ID),
      dest_id_(0),
      backup_set_desc_(),
      backup_data_type_(),
      index_level_(),
      ls_id_(),
      turn_id_(),
      retry_id_()
{}

ObBackupIndexMergeParam::~ObBackupIndexMergeParam()
{}

bool ObBackupIndexMergeParam::is_valid() const
{
  return task_id_ > 0 && backup_dest_.is_valid() && OB_INVALID_ID != tenant_id_ && dest_id_ > 0 && backup_set_desc_.is_valid() &&
         backup_data_type_.is_valid() && turn_id_ > 0 && retry_id_ >= 0;
}

int ObBackupIndexMergeParam::assign(const ObBackupIndexMergeParam &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_dest_.deep_copy(other.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    dest_id_ = other.dest_id_;
    backup_set_desc_ = other.backup_set_desc_;
    backup_data_type_ = other.backup_data_type_;
    index_level_ = other.index_level_;
    ls_id_ = other.ls_id_;
    turn_id_ = other.turn_id_;
    retry_id_ = other.retry_id_;
  }
  return ret;
}

int convert_backup_file_type_to_magic(const ObBackupFileType &file_type, ObBackupFileMagic &magic)
{
  int ret = OB_SUCCESS;
  switch (file_type) {
    case BACKUP_DATA_FILE: {
      magic = BACKUP_DATA_FILE_MAGIC;
      break;
    }
    case BACKUP_MACRO_RANGE_INDEX_FILE: {
      magic = BACKUP_MACRO_RANGE_INDEX_FILE_MAGIC;
      break;
    }
    case BACKUP_META_INDEX_FILE: {
      magic = BACKUP_META_INDEX_FILE_MAGIC;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file type not expected", K(ret), K(file_type));
      break;
    }
  }
  return ret;
}

/* ObBackupFileHeader */

ObBackupFileHeader::ObBackupFileHeader()
{
  reset();
}

void ObBackupFileHeader::reset()
{
  memset(this, 0, sizeof(ObBackupFileHeader));
  magic_ = 0;
  version_ = 0;
  file_type_ = 0;
  reserved_ = 0;
}

int ObBackupFileHeader::check_valid() const
{
  int ret = OB_SUCCESS;
  if (magic_ < BACKUP_DATA_FILE_MAGIC || magic_ >= BACKUP_MAGIC_MAX) {
    ret = OB_INVALID_DATA;
    LOG_WARN("magic is not valid", K(ret), K_(magic));
  } else if (version_ < BACKUP_DATA_VERSION_V1 || version_ >= BACKUP_DATA_VERSION_MAX) {
    ret = OB_INVALID_DATA;
    LOG_WARN("version is not valid", K(ret), K_(version));
  } else if (file_type_ < BACKUP_DATA_FILE || file_type_ >= BACKUP_FILE_TYPE_MAX) {
    ret = OB_INVALID_DATA;
    LOG_WARN("file type is not valid", K(ret), K_(file_type));
  } else if ((file_type_ == BACKUP_DATA_FILE && magic_ != BACKUP_DATA_FILE_MAGIC) ||
             (file_type_ == BACKUP_MACRO_RANGE_INDEX_FILE && magic_ != BACKUP_MACRO_RANGE_INDEX_FILE_MAGIC) ||
             (file_type_ == BACKUP_META_INDEX_FILE && magic_ != BACKUP_META_INDEX_FILE_MAGIC)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("magic and file type not match", K(ret), K_(file_type), K_(magic));
  }
  return ret;
}

/* ObBackupDataFileTrailer */

ObBackupDataFileTrailer::ObBackupDataFileTrailer()
{
  reset();
}

void ObBackupDataFileTrailer::reset()
{
  memset(this, 0, sizeof(ObBackupDataFileTrailer));
}

void ObBackupDataFileTrailer::set_trailer_checksum()
{
  trailer_checksum_ = calc_trailer_checksum();
}

int16_t ObBackupDataFileTrailer::calc_trailer_checksum() const
{
  int16_t checksum = 0;
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(data_type_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(data_version_));
  format_i64(macro_block_count_, checksum);
  format_i64(meta_count_, checksum);
  format_i64(macro_index_offset_, checksum);
  format_i64(macro_index_length_, checksum);
  format_i64(meta_index_offset_, checksum);
  format_i64(meta_index_length_, checksum);
  format_i64(offset_, checksum);
  format_i64(length_, checksum);
  format_i64(data_accumulate_checksum_, checksum);
  return checksum;
}

int ObBackupDataFileTrailer::check_trailer_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_trailer_checksum();
  if (OB_UNLIKELY(trailer_checksum_ != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("check trailer checksum failed", K(ret), K(*this), K(checksum));
  }
  return ret;
}

int ObBackupDataFileTrailer::check_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_trailer_checksum())) {
    LOG_WARN("failed to check trailer checksum", K(ret));
  }
  return ret;
}

/* ObBackupMacroBlockId */

ObBackupMacroBlockId::ObBackupMacroBlockId()
 : logic_id_(), macro_block_id_(),
   nested_offset_(0), nested_size_(0)
{}

bool ObBackupMacroBlockId::is_valid() const
{
  return logic_id_.is_valid() &&
         macro_block_id_.is_valid() &&
         nested_offset_ >= 0 &&
         nested_size_ >= 0;
}

void ObBackupMacroBlockId::reset()
{
  logic_id_.reset();
  macro_block_id_.reset();
  nested_offset_ = 0;
  nested_size_ = 0;
}

/* ObBackupPhysicalID */

ObBackupPhysicalID::ObBackupPhysicalID() : first_id_(0), second_id_(0), third_id_(0)
{}

ObBackupPhysicalID::ObBackupPhysicalID(const ObBackupPhysicalID &id)
{
  first_id_ = id.first_id_;
  second_id_ = id.second_id_;
  third_id_ = id.third_id_;
}

void ObBackupPhysicalID::reset()
{
  first_id_ = 0;
  second_id_ = 0;
  third_id_ = 0;
}

bool ObBackupPhysicalID::is_valid() const
{
  return backup_set_id_ > 0 && ls_id_ > 0 && turn_id_ > 0 && file_id_ >= 0 && offset_ >= 0 && length_ > 0;
}

int ObBackupPhysicalID::get_backup_macro_block_index(
    const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroBlockIndex &macro_index) const
{
  int ret = OB_SUCCESS;
  macro_index.reset();
  if (!is_valid() || !logic_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KPC(this));
  } else {
    macro_index.logic_id_ = logic_id;
    macro_index.backup_set_id_ = backup_set_id_;
    macro_index.ls_id_ = ObLSID(ls_id_);
    macro_index.turn_id_ = turn_id_;
    macro_index.retry_id_ = retry_id_;
    macro_index.file_id_ = file_id_;
    macro_index.offset_ = static_cast<int64_t>(offset_) * DIO_READ_ALIGN_SIZE;
    macro_index.length_ = static_cast<int64_t>(length_) * DIO_READ_ALIGN_SIZE;
  }
  return ret;
}

ObBackupPhysicalID &ObBackupPhysicalID::operator=(const ObBackupPhysicalID &other)
{
  first_id_ = other.first_id_;
  second_id_ = other.second_id_;
  third_id_ = other.third_id_;
  return *this;
}

bool ObBackupPhysicalID::operator==(const ObBackupPhysicalID &other) const
{
  return first_id_ == other.first_id_ && second_id_ == other.second_id_ && third_id_ == other.third_id_;
}

bool ObBackupPhysicalID::operator!=(const ObBackupPhysicalID &other) const
{
  return !(other == *this);
}

const ObBackupPhysicalID ObBackupPhysicalID::get_default()
{
  static ObBackupPhysicalID default_value;
  default_value.ls_id_ = INT64_MAX;
  default_value.backup_set_id_ = MAX_BACKUP_SET_ID;
  default_value.turn_id_ = MAX_BACKUP_TURN_ID;
  default_value.retry_id_ = MAX_BACKUP_RETRY_ID;
  default_value.file_id_ = MAX_BACKUP_FILE_ID;
  default_value.offset_ = MAX_BACKUP_FILE_SIZE;
  default_value.length_ = MAX_BACKUP_BLOCK_SIZE;
  return default_value;
}

DEFINE_SERIALIZE(ObBackupPhysicalID)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  if (OB_ISNULL(buf) || buf_len <= 0 || (buf_len - pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(ser_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, first_id_))) {
    LOG_WARN("failed to encode first id", K(ret), KP(buf), K(buf_len), K(pos), K(ser_len), KPC(this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, second_id_))) {
    LOG_WARN("failed to encode second id", K(ret), KP(buf), K(buf_len), K(pos), K(ser_len), KPC(this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, third_id_))) {
    LOG_WARN("failed to encode third id", K(ret), KP(buf), K(buf_len), K(pos), K(ser_len), KPC(this));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObBackupPhysicalID)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &first_id_))) {
    LOG_WARN("failed to decode first id", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &second_id_))) {
    LOG_WARN("failed to decode second id", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &third_id_))) {
    LOG_WARN("failed to decode third id", K(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObBackupPhysicalID)
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(first_id_);
  len += serialization::encoded_length_i64(second_id_);
  len += serialization::encoded_length_i64(third_id_);
  return len;
}

/* ObBackupMacroBlockIndex */

OB_SERIALIZE_MEMBER(
    ObBackupMacroBlockIndex, logic_id_, backup_set_id_, ls_id_, turn_id_, retry_id_, file_id_, offset_, length_);

ObBackupMacroBlockIndex::ObBackupMacroBlockIndex()
    : logic_id_(), backup_set_id_(0), ls_id_(0), turn_id_(0), retry_id_(0), file_id_(0), offset_(0), length_(0)
{}

void ObBackupMacroBlockIndex::reset()
{
  logic_id_.reset();
  backup_set_id_ = 0;
  ls_id_.reset();
  turn_id_ = 0;
  retry_id_ = 0;
  file_id_ = 0;
  offset_ = 0;
  length_ = 0;
}

bool ObBackupMacroBlockIndex::is_valid() const
{
  return logic_id_.is_valid() && ls_id_.is_valid() && turn_id_ > 0 && retry_id_ >= 0 && file_id_ >= 0 && offset_ >= 0 &&
         length_ > 0 && is_aligned(offset_) && is_aligned(length_);
}

int ObBackupMacroBlockIndex::get_backup_physical_id(ObBackupPhysicalID &physical_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self not valid", KPC(this));
  } else {
    physical_id.type_ = 0;
    physical_id.ls_id_ = ls_id_.id();
    physical_id.turn_id_ = turn_id_;
    physical_id.retry_id_ = retry_id_;
    physical_id.file_id_ = file_id_;
    physical_id.backup_set_id_ = backup_set_id_;
    physical_id.offset_ = offset_ / DIO_READ_ALIGN_SIZE;
    physical_id.length_ = length_ / DIO_READ_ALIGN_SIZE;
  }
  return ret;
}

bool ObBackupMacroBlockIndex::operator==(const ObBackupMacroBlockIndex &other) const
{
  return logic_id_ == other.logic_id_ && backup_set_id_ == other.backup_set_id_ && ls_id_ == other.ls_id_ &&
         turn_id_ == other.turn_id_ && retry_id_ == other.retry_id_ && file_id_ == other.file_id_ &&
         offset_ == other.offset_ && length_ == other.length_;
}

/* ObBackupMacroRangeIndex */

OB_SERIALIZE_MEMBER(ObBackupMacroRangeIndex, start_key_, end_key_, backup_set_id_, ls_id_, turn_id_, retry_id_,
    file_id_, offset_, length_);

ObBackupMacroRangeIndex::ObBackupMacroRangeIndex()
    : start_key_(),
      end_key_(),
      backup_set_id_(0),
      ls_id_(),
      turn_id_(0),
      retry_id_(0),
      file_id_(0),
      offset_(0),
      length_(0)
{}

void ObBackupMacroRangeIndex::reset()
{
  start_key_.reset();
  end_key_.reset();
  backup_set_id_ = 0;
  ls_id_.reset();
  turn_id_ = 0;
  retry_id_ = 0;
  file_id_ = 0;
  offset_ = 0;
  length_ = 0;
}

bool ObBackupMacroRangeIndex::is_valid() const
{
  return start_key_.is_valid() && end_key_.is_valid() && backup_set_id_ > 0 && ls_id_.is_valid() && turn_id_ > 0 &&
         retry_id_ >= 0 && file_id_ >= 0 && offset_ >= 0 && length_ > 0;
}

bool ObBackupMacroRangeIndex::operator==(const ObBackupMacroRangeIndex &other) const
{
  return start_key_ == other.start_key_ && end_key_ == other.end_key_ && backup_set_id_ == other.backup_set_id_ &&
         ls_id_ == other.ls_id_ && turn_id_ == other.turn_id_ && retry_id_ == other.retry_id_ &&
         file_id_ == other.file_id_ && offset_ == other.offset_ && length_ == other.length_;
}

/* ObBackupMacroRangeIndexIndex */

OB_SERIALIZE_MEMBER(ObBackupMacroRangeIndexIndex, end_key_, offset_, length_);

ObBackupMacroRangeIndexIndex::ObBackupMacroRangeIndexIndex() : end_key_(), offset_(-1), length_(-1)
{}

void ObBackupMacroRangeIndexIndex::reset()
{
  end_key_.reset();
  offset_ = -1;
  length_ = -1;
}

bool ObBackupMacroRangeIndexIndex::is_valid() const
{
  return end_key_.is_valid() && offset_ > 0 && length_ > 0;
}

/* ObBackupMacroBlockIndexComparator */

int ObBackupMacroBlockIndexComparator::operator()(
    const ObBackupMacroRangeIndex &lhs, const ObBackupMacroRangeIndex &rhs) const
{
  int ret = 0;
  const ObLogicMacroBlockId &lvalue = lhs.end_key_;
  const ObLogicMacroBlockId &rvalue = rhs.end_key_;
  if (lvalue > rvalue) {
    ret = -1;
  } else if (lvalue < rvalue) {
    ret = 1;
  } else {
    ret = 0;
  }
  return ret;
}

/* ObBackupMetaIndexComparator */

int ObBackupMetaIndexComparator::operator()(const ObBackupMetaIndex &lhs, const ObBackupMetaIndex &rhs) const
{
  int ret = 0;
  const ObBackupMetaKey &lvalue = lhs.meta_key_;
  const ObBackupMetaKey &rvalue = rhs.meta_key_;
  if (lvalue > rvalue) {
    ret = -1;
  } else if (lvalue < rvalue) {
    ret = 1;
  } else {
    ret = 0;
  }
  return ret;
}

/* ObBackupMultiLevelIndexHeader */

OB_SERIALIZE_MEMBER(ObBackupMultiLevelIndexHeader, magic_, backup_type_, index_level_);

ObBackupMultiLevelIndexHeader::ObBackupMultiLevelIndexHeader() : magic_(0), backup_type_(0), index_level_(0)
{}

void ObBackupMultiLevelIndexHeader::reset()
{
  magic_ = 0;
  backup_type_ = 0;
  index_level_ = 0;
}

bool ObBackupMultiLevelIndexHeader::is_valid() const
{
  return true;
}

/* ObBackupMetaKey */

OB_SERIALIZE_MEMBER(ObBackupMetaKey, tablet_id_, meta_type_);

ObBackupMetaKey::ObBackupMetaKey() : tablet_id_(), meta_type_(BACKUP_META_MAX)
{}

ObBackupMetaKey::~ObBackupMetaKey()
{}

bool ObBackupMetaKey::operator<(const ObBackupMetaKey &other) const
{
  bool bret = false;
  if (tablet_id_ < other.tablet_id_) {
    bret = true;
  } else if (tablet_id_ > other.tablet_id_) {
    bret = false;
  } else if (meta_type_ < other.meta_type_) {
    bret = true;
  } else if (meta_type_ > other.meta_type_) {
    bret = false;
  }
  return bret;
}

bool ObBackupMetaKey::operator>(const ObBackupMetaKey &other) const
{
  bool bret = false;
  if (tablet_id_ < other.tablet_id_) {
    bret = false;
  } else if (tablet_id_ > other.tablet_id_) {
    bret = true;
  } else if (meta_type_ < other.meta_type_) {
    bret = false;
  } else if (meta_type_ > other.meta_type_) {
    bret = true;
  }
  return bret;
}

bool ObBackupMetaKey::operator==(const ObBackupMetaKey &other) const
{
  return tablet_id_ == other.tablet_id_ && meta_type_ == other.meta_type_;
}

bool ObBackupMetaKey::operator!=(const ObBackupMetaKey &other) const
{
  return !(*this == other);
}

bool ObBackupMetaKey::is_valid() const
{
  return tablet_id_.is_valid() && meta_type_ >= BACKUP_SSTABLE_META && meta_type_ < BACKUP_META_MAX;
}

void ObBackupMetaKey::reset()
{
  tablet_id_.reset();
  meta_type_ = BACKUP_META_MAX;
}

int ObBackupMetaKey::get_backup_index_file_type(ObBackupFileType &backup_file_type) const
{
  int ret = OB_SUCCESS;
  if (BACKUP_TABLET_META == meta_type_ || BACKUP_SSTABLE_META == meta_type_) {
    backup_file_type = BACKUP_META_INDEX_FILE;
  } else if (BACKUP_MACRO_BLOCK_ID_MAPPING_META == meta_type_) {
    backup_file_type = BACKUP_SEC_META_INDEX_FILE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup file type not found", K(ret), KPC(this));
  }
  return ret;
}

/* ObBackupTabletMeta */

OB_SERIALIZE_MEMBER(ObBackupTabletMeta, tablet_id_, tablet_meta_);

ObBackupTabletMeta::ObBackupTabletMeta() : tablet_id_(), tablet_meta_()
{}

bool ObBackupTabletMeta::is_valid() const
{
  return tablet_id_.is_valid() && tablet_meta_.is_valid();
}

void ObBackupTabletMeta::reset()
{
  tablet_id_.reset();
  tablet_meta_.reset();
}

/* ObBackupSSTableMeta */

OB_SERIALIZE_MEMBER(ObBackupSSTableMeta, tablet_id_, sstable_meta_, logic_id_list_);

ObBackupSSTableMeta::ObBackupSSTableMeta() : tablet_id_(), sstable_meta_(), logic_id_list_()
{}

bool ObBackupSSTableMeta::is_valid() const
{
  return tablet_id_.is_valid() && sstable_meta_.is_valid();
}

void ObBackupSSTableMeta::reset()
{
  tablet_id_.reset();
  sstable_meta_.reset();
  logic_id_list_.reset();
}

int ObBackupSSTableMeta::assign(const ObBackupSSTableMeta &backup_sstable_meta)
{
  int ret = OB_SUCCESS;
  if (!backup_sstable_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup sstable meta is invalid", K(ret), K(backup_sstable_meta));
  } else if (OB_FAIL(sstable_meta_.assign(backup_sstable_meta.sstable_meta_))) {
    LOG_WARN("failed to assign sstable meta", K(ret), K(backup_sstable_meta));
  } else if (OB_FAIL(logic_id_list_.assign(backup_sstable_meta.logic_id_list_))) {
    LOG_WARN("failed to assign logic id list", K(ret), K(backup_sstable_meta));
  } else {
    tablet_id_ = backup_sstable_meta.tablet_id_;
  }
  return ret;
}

/* ObBackupMacroBlockIDPair */

OB_SERIALIZE_MEMBER(ObBackupMacroBlockIDPair, logic_id_, physical_id_);

ObBackupMacroBlockIDPair::ObBackupMacroBlockIDPair() : logic_id_(), physical_id_()
{}

ObBackupMacroBlockIDPair::~ObBackupMacroBlockIDPair()
{}

bool ObBackupMacroBlockIDPair::operator<(const ObBackupMacroBlockIDPair &other) const
{
  return logic_id_ < other.logic_id_;
}

void ObBackupMacroBlockIDPair::reset()
{
  logic_id_.reset();
  physical_id_.reset();
}

bool ObBackupMacroBlockIDPair::is_valid() const
{
  return logic_id_.is_valid() && physical_id_.is_valid();
}

/* ObBackupMacroBlockIDMapping */

ObBackupMacroBlockIDMapping::ObBackupMacroBlockIDMapping() : table_key_(), id_pair_list_(), map_()
{}

ObBackupMacroBlockIDMapping::~ObBackupMacroBlockIDMapping()
{}

void ObBackupMacroBlockIDMapping::reuse()
{
  table_key_.reset();
  id_pair_list_.reset();
  map_.reuse();
}

int ObBackupMacroBlockIDMapping::prepare_tablet_sstable(const uint64_t tenant_id,
    const storage::ObITable::TableKey &table_key, const common::ObIArray<blocksstable::ObLogicMacroBlockId> &list)
{
  int ret = OB_SUCCESS;
  static const int64_t bucket_count = 1000;
  lib::ObMemAttr mem_attr(tenant_id, ObModIds::BACKUP);
  if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(table_key));
  } else if (map_.created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("map is created before", K(ret));
  } else if (OB_FAIL(map_.create(bucket_count, mem_attr))) {
    LOG_WARN("failed to create map", K(ret));
  } else {
    table_key_ = table_key;
    for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      const blocksstable::ObLogicMacroBlockId &logic_id = list.at(i);
      ObBackupMacroBlockIDPair id_pair;
      id_pair.logic_id_ = logic_id;
      id_pair.physical_id_ = ObBackupPhysicalID::get_default();
      if (OB_FAIL(id_pair_list_.push_back(id_pair))) {
        LOG_WARN("failed to push back", K(ret), K(id_pair));
      } else if (OB_FAIL(map_.set_refactored(logic_id, i))) {
        LOG_WARN("failed to set refactored", K(ret), K(logic_id), K(i));
      }
    }
  }
  return ret;
}

/* ObBackupMacroBlockIDMappingsMeta */

ObBackupMacroBlockIDMappingsMeta::ObBackupMacroBlockIDMappingsMeta()
    : version_(MAPPING_META_VERSION_MAX), sstable_count_(0), id_map_list_()
{}

ObBackupMacroBlockIDMappingsMeta::~ObBackupMacroBlockIDMappingsMeta()
{}

void ObBackupMacroBlockIDMappingsMeta::reuse()
{
  version_ = MAPPING_META_VERSION_MAX;
  sstable_count_ = 0;
  for (int64_t i = 0; i < MAX_SSTABLE_CNT_IN_STORAGE; ++i) {
    id_map_list_[i].reuse();
  }
}

DEFINE_SERIALIZE(ObBackupMacroBlockIDMappingsMeta)
{
  int ret = OB_SUCCESS;
  int64_t version = version_;
  int64_t num_of_sstable = sstable_count_;
  if (version < MAPPING_META_VERSION_V1 || version > MAPPING_META_VERSION_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(version));
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(version);
    OB_UNIS_ENCODE(num_of_sstable);
  }
  if (OB_SUCC(ret) && num_of_sstable > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_count_; ++i) {
      const ObBackupMacroBlockIDMapping &item = id_map_list_[i];
      const ObITable::TableKey &table_key = item.table_key_;
      int64_t num_of_entries = item.id_pair_list_.count();
      if (!table_key.is_valid()) {
        ret = OB_ERR_SYS;
        LOG_WARN("get invalid data", K(ret), K(table_key));
      } else {
        OB_UNIS_ENCODE(table_key);
        OB_UNIS_ENCODE(num_of_entries);
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < item.id_pair_list_.count(); ++j) {
        const ObBackupMacroBlockIDPair &pair = item.id_pair_list_.at(j);
        if (!pair.is_valid()) {
          ret = OB_ERR_SYS;
          LOG_WARN("get invalid data", K(ret), K(pair));
        } else {
          OB_UNIS_ENCODE(pair);
        }
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObBackupMacroBlockIDMappingsMeta)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t num_of_sstable = 0;
  reuse();
  OB_UNIS_DECODE(version);
  if (version < MAPPING_META_VERSION_V1 || version > MAPPING_META_VERSION_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(version));
  } else {
    version_ = version;
  }
  OB_UNIS_DECODE(num_of_sstable);
  sstable_count_ = num_of_sstable;
  ObITable::TableKey table_key;
  ObLogicMacroBlockId logic_id;
  ObBackupMacroBlockIDPair pair;
  int64_t num_of_entries = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < num_of_sstable; ++i) {
    table_key.reset();
    OB_UNIS_DECODE(table_key);
    OB_UNIS_DECODE(num_of_entries);
    if (!table_key.is_valid() || num_of_entries < 0) {
      ret = OB_ERR_SYS;
      LOG_WARN("table key is not valid", K(ret), K(table_key), K(num_of_entries));
    } else {
      id_map_list_[i].table_key_ = table_key;
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < num_of_entries; ++j) {
      pair.reset();
      OB_UNIS_DECODE(pair);
      if (!pair.is_valid()) {
        ret = OB_ERR_SYS;
        LOG_WARN("get invalid data", K(ret), K(pair));
      } else if (OB_FAIL(id_map_list_[i].id_pair_list_.push_back(pair))) {
        LOG_WARN("failed to push back", K(ret), K(pair));
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObBackupMacroBlockIDMappingsMeta)
{
  int64_t len = 0;
  int64_t version = version_;
  int64_t num_of_sstable = sstable_count_;
  OB_UNIS_ADD_LEN(version);
  OB_UNIS_ADD_LEN(num_of_sstable);
  if (num_of_sstable > 0) {
    for (int64_t i = 0; i < sstable_count_; ++i) {
      const ObBackupMacroBlockIDMapping &item = id_map_list_[i];
      const ObITable::TableKey &table_key = item.table_key_;
      const ObArray<ObBackupMacroBlockIDPair> &id_pair_list = item.id_pair_list_;
      int64_t num_of_entries = id_pair_list.count();
      OB_UNIS_ADD_LEN(table_key);
      OB_UNIS_ADD_LEN(num_of_entries);
      for (int64_t j = 0; j < num_of_entries; ++j) {
        const ObBackupMacroBlockIDPair &pair = id_pair_list.at(j);
        OB_UNIS_ADD_LEN(pair);
      }
    }
  }
  return len;
}

/* ObBackupMetaIndex */

OB_SERIALIZE_MEMBER(
    ObBackupMetaIndex, meta_key_, backup_set_id_, ls_id_, turn_id_, retry_id_, file_id_, offset_, length_);

ObBackupMetaIndex::ObBackupMetaIndex()
    : meta_key_(), backup_set_id_(), ls_id_(), turn_id_(), retry_id_(), file_id_(), offset_(), length_()
{}

void ObBackupMetaIndex::reset()
{
  meta_key_.reset();
  ls_id_.reset();
  backup_set_id_ = 0;
  turn_id_ = 0;
  retry_id_ = 0;
  file_id_ = 0;
  offset_ = 0;
  length_ = 0;
}

bool ObBackupMetaIndex::is_valid() const
{
  return meta_key_.is_valid() && ls_id_.is_valid() && file_id_ >= 0 && offset_ >= 0 && length_ > 0 &&
         is_aligned(offset_) && is_aligned(length_);
}

bool ObBackupMetaIndex::operator==(const ObBackupMetaIndex &other) const
{
  return meta_key_ == other.meta_key_ && backup_set_id_ == other.backup_set_id_ && ls_id_ == other.ls_id_ &&
         turn_id_ == other.turn_id_ && retry_id_ == other.retry_id_ && file_id_ == other.file_id_ &&
         offset_ == other.offset_ && length_ == other.length_;
}

/* ObBackupMetaIndexIndex */

OB_SERIALIZE_MEMBER(ObBackupMetaIndexIndex, end_key_, offset_, length_);

ObBackupMetaIndexIndex::ObBackupMetaIndexIndex() : end_key_(), offset_(), length_()
{}

void ObBackupMetaIndexIndex::reset()
{
  end_key_.reset();
  offset_ = 0;
  length_ = 0;
}

bool ObBackupMetaIndexIndex::is_valid() const
{
  return end_key_.is_valid() && offset_ > 0 && length_ > 0;
}

/* ObBackupMultiLevelIndexTrailer */

ObBackupMultiLevelIndexTrailer::ObBackupMultiLevelIndexTrailer()
    : file_type_(), tree_height_(), last_block_offset_(), last_block_length_(), checksum_()
{}

void ObBackupMultiLevelIndexTrailer::reset()
{
  file_type_ = 0;
  tree_height_ = 0;
  last_block_offset_ = 0;
  last_block_length_ = 0;
  checksum_ = 0;
}

void ObBackupMultiLevelIndexTrailer::set_trailer_checksum()
{
  checksum_ = calc_trailer_checksum();
}

int16_t ObBackupMultiLevelIndexTrailer::calc_trailer_checksum() const
{
  int16_t checksum = 0;
  checksum = checksum ^ file_type_;
  checksum = checksum ^ tree_height_;
  format_i64(last_block_offset_, checksum);
  format_i64(last_block_length_, checksum);
  return checksum;
}

int ObBackupMultiLevelIndexTrailer::check_trailer_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_trailer_checksum();
  if (OB_UNLIKELY(checksum_ != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("check trailer checksum failed", K(*this), K(checksum), K(ret));
  }
  return ret;
}

int ObBackupMultiLevelIndexTrailer::check_valid() const
{
  int ret = OB_SUCCESS;
  if (tree_height_ <= 0 || last_block_offset_ < 0 || last_block_length_ < 0) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("invalid trailer", K(ret), K_(tree_height), K_(last_block_offset), K_(last_block_length));
  } else if (OB_FAIL(check_trailer_checksum())) {
    LOG_WARN("failed to check trailer checksum", K(ret));
  }
  return ret;
}

/* ObLSBackupStat */

ObLSBackupStat::ObLSBackupStat()
    : ls_id_(),
      backup_set_id_(-1),
      file_id_(-1),
      input_bytes_(0),
      output_bytes_(0),
      finish_macro_block_count_(0),
      finish_sstable_count_(0),
      finish_tablet_count_(0)
{}

/* ObBackupRetryDesc */

ObBackupRetryDesc::ObBackupRetryDesc() : ls_id_(), turn_id_(-1), retry_id_(-1), last_file_id_(-1)
{}

void ObBackupRetryDesc::reset()
{
  ls_id_.reset();
  turn_id_ = -1;
  retry_id_ = -1;
  last_file_id_ = -1;
}

bool ObBackupRetryDesc::is_valid() const
{
  return ls_id_.is_valid() && turn_id_ > 0 && retry_id_ >= 0 && last_file_id_ >= 0;
}

/* ObLSBackupDataParam */

ObLSBackupDataParam::ObLSBackupDataParam()
    : job_desc_(),
      backup_stage_(),
      tenant_id_(),
      dest_id_(0),
      backup_set_desc_(),
      ls_id_(),
      backup_data_type_(),
      turn_id_(),
      retry_id_()
{}

ObLSBackupDataParam::~ObLSBackupDataParam()
{}

bool ObLSBackupDataParam::is_valid() const
{
  return job_desc_.is_valid() && OB_INVALID_ID != tenant_id_ && dest_id_ > 0 && backup_set_desc_.is_valid() && turn_id_ > 0;
}

int ObLSBackupDataParam::convert_to(const ObLSBackupStage &stage, ObLSBackupDagInitParam &dag_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(this));
  } else if (OB_FAIL(dag_param.backup_dest_.deep_copy(backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K_(backup_dest));
  } else {
    dag_param.job_desc_ = job_desc_;
    dag_param.tenant_id_ = tenant_id_;
    dag_param.dest_id_ = dest_id_;
    dag_param.backup_set_desc_ = backup_set_desc_;
    dag_param.ls_id_ = ls_id_;
    dag_param.turn_id_ = turn_id_;
    dag_param.retry_id_ = retry_id_;
    dag_param.backup_stage_ = stage;
  }
  return ret;
}

int ObLSBackupDataParam::convert_to(const ObBackupIndexLevel &index_level, ObBackupIndexMergeParam &merge_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(this));
  } else if (OB_FAIL(merge_param.backup_dest_.deep_copy(backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K_(backup_dest));
  } else {
    merge_param.task_id_ = job_desc_.task_id_;
    merge_param.tenant_id_ = tenant_id_;
    merge_param.dest_id_ = dest_id_;
    merge_param.backup_set_desc_ = backup_set_desc_;
    merge_param.backup_data_type_ = backup_data_type_;
    merge_param.index_level_ = index_level;
    merge_param.ls_id_ = ls_id_;
    merge_param.turn_id_ = turn_id_;
    merge_param.retry_id_ = retry_id_;
  }
  return ret;
}

int build_backup_file_header_buffer(const ObBackupFileHeader &file_header, const int64_t buf_len, char *buf,
    blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  ObBackupFileHeader *header = NULL;
  const int64_t header_len = sizeof(ObBackupFileHeader);
  if (header_len > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf do not enough", K(ret), K(header_len), K(buf_len));
  } else if (OB_ISNULL(header = reinterpret_cast<ObBackupFileHeader *>(buf))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    *header = file_header;
    buffer_reader = blocksstable::ObBufferReader(buf, buf_len, buf_len);
  }
  return ret;
}

int ObLSBackupDataParam::assign(const ObLSBackupDataParam &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_dest_.deep_copy(other.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret));
  } else {
    job_desc_ = other.job_desc_;
    backup_stage_ = other.backup_stage_;
    tenant_id_ = other.tenant_id_;
    dest_id_ = other.dest_id_;
    backup_set_desc_ = other.backup_set_desc_;
    ls_id_ = other.ls_id_;
    backup_data_type_ = other.backup_data_type_;
    turn_id_ = other.turn_id_;
    retry_id_ = other.retry_id_;
  }
  return ret;
}

int build_common_header(const int64_t data_type, const int64_t data_length, const int64_t align_length,
    share::ObBackupCommonHeader *&common_header)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common header should not be null", K(ret), KP(common_header));
  } else {
    common_header->reset();
    const int64_t header_len = sizeof(ObBackupCommonHeader);
    common_header->data_type_ = data_type;
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

int build_multi_level_index_header(
    const int64_t index_type, const int64_t index_level, ObBackupMultiLevelIndexHeader &header)
{
  int ret = OB_SUCCESS;
  if (index_type < BACKUP_BLOCK_MACRO_DATA || index_type >= BACKUP_BLOCK_MAX || index_level < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(index_type), K(index_level));
  } else {
    header.backup_type_ = index_type;
    header.index_level_ = index_level;
    if (index_type == BACKUP_BLOCK_MARCO_RANGE_INDEX_INDEX) {
      header.magic_ = ObBackupMultiLevelIndexHeader::MACRO_MULTI_INDEX_MAGIC;
    } else {
      header.magic_ = ObBackupMultiLevelIndexHeader::META_MULTI_INDEX_MAGIC;
    }
  }
  return ret;
}

/* ObBackupLSTaskInfo */

ObBackupLSTaskInfo::ObBackupLSTaskInfo()
    : task_id_(),
      tenant_id_(),
      ls_id_(),
      turn_id_(),
      retry_id_(),
      backup_data_type_(),
      backup_set_id_(),
      input_bytes_(0),
      output_bytes_(0),
      tablet_count_(0),
      finish_tablet_count_(0),
      macro_block_count_(0),
      finish_macro_block_count_(0),
      max_file_id_(-1),
      is_final_(false)
{}

ObBackupLSTaskInfo::~ObBackupLSTaskInfo()
{}

bool ObBackupLSTaskInfo::is_valid() const
{
  return task_id_ > 0 && OB_INVALID_ID == tenant_id_ && ls_id_.is_valid() && turn_id_ > 0 && retry_id_ > 0;
}

void ObBackupLSTaskInfo::reset()
{}

/* ObBackupSkippedTablet */

ObBackupSkippedTablet::ObBackupSkippedTablet()
    : task_id_(), tenant_id_(), turn_id_(), retry_id_(), tablet_id_(), ls_id_(), backup_set_id_(), skipped_type_(), data_type_()
{}

ObBackupSkippedTablet::~ObBackupSkippedTablet()
{}

bool ObBackupSkippedTablet::is_valid() const
{
  return task_id_ > 0 && OB_INVALID_ID != tenant_id_ && turn_id_ > 0 && retry_id_ >= 0 && tablet_id_.is_valid()
         && ls_id_.is_valid() && backup_set_id_ > 0 && skipped_type_.is_valid()
         && data_type_.is_valid();
}

/* ObBackupReportCtx */

ObBackupReportCtx::ObBackupReportCtx() : location_service_(NULL), sql_proxy_(NULL), rpc_proxy_(NULL)
{}

ObBackupReportCtx::~ObBackupReportCtx()
{}

bool ObBackupReportCtx::is_valid() const
{
  return OB_NOT_NULL(location_service_) && OB_NOT_NULL(sql_proxy_) && OB_NOT_NULL(rpc_proxy_);
}

}  // namespace backup
}  // namespace oceanbase
