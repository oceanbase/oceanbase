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

#include "ob_backup_data_struct.h"
#include "storage/backup/ob_backup_task.h"

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
      retry_id_(),
      compressor_type_(INVALID_COMPRESSOR)
{}

ObBackupIndexMergeParam::~ObBackupIndexMergeParam()
{}

bool ObBackupIndexMergeParam::is_valid() const
{
  return task_id_ > 0 && backup_dest_.is_valid() && OB_INVALID_ID != tenant_id_ && dest_id_ > 0 && backup_set_desc_.is_valid()
         && backup_data_type_.is_valid() && turn_id_ > 0 && retry_id_ >= 0
         && compressor_type_ > INVALID_COMPRESSOR && compressor_type_ < MAX_COMPRESSOR;
}

int ObBackupIndexMergeParam::assign(const ObBackupIndexMergeParam &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_dest_.deep_copy(other.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret));
  } else {
    task_id_ = other.task_id_;
    tenant_id_ = other.tenant_id_;
    dest_id_ = other.dest_id_;
    backup_set_desc_ = other.backup_set_desc_;
    backup_data_type_ = other.backup_data_type_;
    index_level_ = other.index_level_;
    ls_id_ = other.ls_id_;
    turn_id_ = other.turn_id_;
    retry_id_ = other.retry_id_;
    compressor_type_ = other.compressor_type_;
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
    case BACKUP_MACRO_BLOCK_INDEX_FILE: {
      magic = BACKUP_MACRO_BLOCK_INDEX_FILE_MAGIC;
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
 : table_key_(), logic_id_(), macro_block_id_(),
   nested_offset_(0), nested_size_(0),
   is_ss_ddl_other_block_(false), absolute_row_offset_(0)
{}

bool ObBackupMacroBlockId::is_valid() const
{
  bool bret = false;
  if (!is_ss_ddl_other_block_) {
    bret = table_key_.is_valid()
        && logic_id_.is_valid()
        && macro_block_id_.is_valid()
        && nested_offset_ >= 0
        && nested_size_ >= 0
        && absolute_row_offset_ >= 0;
  } else {
    // other block has no logic id
    bret = macro_block_id_.is_valid()
        && nested_offset_ >= 0
        && nested_size_ >= 0;
  }
  return bret;
}

int ObBackupMacroBlockId::assign(const ObBackupMacroBlockId &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(other));
  } else {
    table_key_ = other.table_key_;
    logic_id_ = other.logic_id_;
    macro_block_id_ = other.macro_block_id_;
    nested_offset_ = other.nested_offset_;
    nested_size_ = other.nested_size_;
    absolute_row_offset_ = other.absolute_row_offset_;
    is_ss_ddl_other_block_ = other.is_ss_ddl_other_block_;
  }
  return ret;
}

void ObBackupMacroBlockId::reset()
{
  table_key_.reset();
  logic_id_.reset();
  macro_block_id_.reset();
  nested_offset_ = 0;
  nested_size_ = 0;
  absolute_row_offset_ = 0;
  is_ss_ddl_other_block_ = false;
}

/* ObBackupPhysicalID */

ObBackupPhysicalID::ObBackupPhysicalID() : first_id_(0), second_id_(0), third_id_(0)
{}

void ObBackupPhysicalID::reset()
{
  first_id_ = 0;
  second_id_ = 0;
  third_id_ = 0;
}

bool ObBackupPhysicalID::is_valid() const
{
  return backup_set_id_ > 0 && ls_id_ > 0 && turn_id_ > 0 && file_id_ >= 0 && aligned_offset_ >= 0 && aligned_length_ > 0;
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
    macro_index.offset_ = static_cast<int64_t>(aligned_offset_) * DIO_READ_ALIGN_SIZE;
    macro_index.length_ = static_cast<int64_t>(aligned_length_) * DIO_READ_ALIGN_SIZE;
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
  default_value.aligned_offset_ = MAX_BACKUP_FILE_SIZE;
  default_value.aligned_length_ = MAX_BACKUP_BLOCK_SIZE;
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
    ObBackupMacroBlockIndex, logic_id_, backup_set_id_, ls_id_, turn_id_, retry_id_, file_id_, offset_, length_,
    reusable_ // FARM COMPAT WHITELIST
    );

ObBackupMacroBlockIndex::ObBackupMacroBlockIndex()
    : logic_id_(), backup_set_id_(0), ls_id_(0), turn_id_(0), retry_id_(0), file_id_(0), offset_(0), length_(0), reusable_(false)
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
  reusable_ = false;
}

bool ObBackupMacroBlockIndex::is_valid() const
{
  bool bret = false;
  bret = logic_id_.is_valid() && ls_id_.is_valid() && turn_id_ > 0 && retry_id_ >= 0 && file_id_ >= 0 && offset_ >= 0 &&
      length_ > 0 && is_aligned(offset_) && is_aligned(length_);
  return bret;
}

// deep copy size may be 0, this is used for parallel external sort to deep copy items like string
int64_t ObBackupMacroBlockIndex::get_deep_copy_size() const
{
  return 0;
}

int ObBackupMacroBlockIndex::deep_copy(const ObBackupMacroBlockIndex &src, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  UNUSEDx(buf, len, pos);
  if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(src));
  } else {
    logic_id_ = src.logic_id_;
    backup_set_id_ = src.backup_set_id_;
    ls_id_ = src.ls_id_;
    turn_id_ = src.turn_id_;
    retry_id_ = src.retry_id_;
    file_id_ = src.file_id_;
    offset_ = src.offset_;
    length_ = src.length_;
    reusable_ = src.reusable_;
  }
  return ret;
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
    physical_id.aligned_offset_ = offset_ / DIO_READ_ALIGN_SIZE;
    physical_id.aligned_length_ = length_ / DIO_READ_ALIGN_SIZE;
  }
  return ret;
}

int ObBackupMacroBlockIndex::get_backup_physical_id(
    const ObBackupDataType &backup_data_type,
    ObBackupDeviceMacroBlockId &physical_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self not valid", KPC(this));
  } else {
    physical_id.ls_id_ = ls_id_.id();
    physical_id.data_type_ = static_cast<int64_t>(backup_data_type.type_);
    physical_id.turn_id_ = turn_id_;
    physical_id.retry_id_ = retry_id_;
    physical_id.file_id_ = file_id_;
    physical_id.backup_set_id_ = backup_set_id_;
    physical_id.block_type_ = ObBackupDeviceMacroBlockId::DATA_BLOCK;
    physical_id.offset_ = offset_ / DIO_READ_ALIGN_SIZE;
    physical_id.length_ = length_ / DIO_READ_ALIGN_SIZE;
    physical_id.reserved_ = 0;
    physical_id.id_mode_ = static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP);
    physical_id.version_ = blocksstable::MacroBlockId::MACRO_BLOCK_ID_VERSION_V2;
  }
  return ret;
}

bool ObBackupMacroBlockIndex::operator==(const ObBackupMacroBlockIndex &other) const
{
  return logic_id_ == other.logic_id_ && backup_set_id_ == other.backup_set_id_ && ls_id_ == other.ls_id_
      && turn_id_ == other.turn_id_ && retry_id_ == other.retry_id_ && file_id_ == other.file_id_
      && offset_ == other.offset_ && length_ == other.length_ && reusable_ == other.reusable_;
}

/* ObBackupMacroBlockIndexIndex */

OB_SERIALIZE_MEMBER(
    ObBackupMacroBlockIndexIndex, end_key_, offset_, length_);

void ObBackupMacroBlockIndexIndex::reset()
{
  end_key_.reset();
  offset_ = -1;
  length_ = -1;
}

bool ObBackupMacroBlockIndexIndex::is_valid() const
{
  return end_key_.is_valid() && offset_ >= 0 && length_ > 0;
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

uint64_t ObBackupMetaKey::calc_hash(uint64_t seed) const {
  uint64_t hash_code = 0;
  hash_code = murmurhash(&tablet_id_, sizeof(tablet_id_), seed);
  hash_code = murmurhash(&meta_type_, sizeof(meta_type_), hash_code);
  return hash_code;
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
ObBackupSSTableMeta::ObBackupSSTableMeta() : tablet_id_(), sstable_meta_(), logic_id_list_(),
    entry_block_addr_for_other_block_(), total_other_block_count_(0), is_major_compaction_mview_dep_(false)
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
  entry_block_addr_for_other_block_.reset();
  total_other_block_count_ = 0;
  is_major_compaction_mview_dep_ = false;
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
    entry_block_addr_for_other_block_ = backup_sstable_meta.entry_block_addr_for_other_block_;
    total_other_block_count_ = backup_sstable_meta.total_other_block_count_;
    tablet_id_ = backup_sstable_meta.tablet_id_;
    is_major_compaction_mview_dep_ = backup_sstable_meta.is_major_compaction_mview_dep_;
  }
  return ret;
}

int ObBackupSSTableMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize_", KR(ret));
      }
    }
    int64_t serial_size = pos - pos_bak;
    int64_t tmp_pos = 0;
    if (OB_SUCC(ret)) {
      CHECK_SERIALIZE_SIZE(ObBackupSSTableMeta, serial_size);
      ret = serialization::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes, size_nbytes, tmp_pos, serial_size);
    }
  }
  return ret;
}

int ObBackupSSTableMeta::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(sstable_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(logic_id_list_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(entry_block_addr_for_other_block_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, total_other_block_count_))) {
    LOG_WARN("fail to encode", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, is_major_compaction_mview_dep_))) {
    LOG_WARN("fail to encode", KR(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObBackupSSTableMeta::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    if (OB_BACKUP_SSTABLE_META_V1 != version && OB_BACKUP_SSTABLE_META_V2 != version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("version not match", K(ret), K(version));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_BACKUP_SSTABLE_META_V1 == version) {
      if (OB_FAIL(deserialize_v1(buf + pos_orig, len, pos))) {
        LOG_WARN("fail to deserialize_v1", KR(ret), K(len), K(pos));
      }
    } else if (OB_BACKUP_SSTABLE_META_V2 == version) {
      if (OB_FAIL(deserialize_v2(buf + pos_orig, len, pos))) {
        LOG_WARN("fail to deserialize_v2", KR(ret), K(len), K(pos));
      }
    }

    pos = pos_orig + len;
  }
  return ret;
}

int ObBackupSSTableMeta::deserialize_v2(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (pos < data_len && OB_FAIL(tablet_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(sstable_meta_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(logic_id_list_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(entry_block_addr_for_other_block_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(serialization::decode(buf, data_len, pos, total_other_block_count_))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(serialization::decode(buf, data_len, pos, is_major_compaction_mview_dep_))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int ObBackupSSTableMeta::deserialize_v1(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObBackupPhysicalID physical_id;
  entry_block_addr_for_other_block_.reset();
  if (pos < data_len && OB_FAIL(tablet_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(sstable_meta_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(logic_id_list_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(physical_id.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(serialization::decode(buf, data_len, pos, total_other_block_count_))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(serialization::decode(buf, data_len, pos, is_major_compaction_mview_dep_))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObBackupSSTableMeta::get_serialize_size(void) const
{
  int64_t len = get_serialize_size_();
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  len += NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
  return len;
}

int64_t ObBackupSSTableMeta::get_serialize_size_(void) const
{
  int64_t len = serialization::encoded_length(tablet_id_);
  len += serialization::encoded_length(sstable_meta_);
  len += serialization::encoded_length(logic_id_list_);
  len += serialization::encoded_length(entry_block_addr_for_other_block_);
  len += serialization::encoded_length(total_other_block_count_);
  len += serialization::encoded_length(is_major_compaction_mview_dep_);
  return len;
}

/* ObBackupMacroBlockIDPair */

OB_SERIALIZE_MEMBER(ObBackupMacroBlockIDPair,
    logic_id_,
    physical_id_ // FARM COMPAT WHITELIST: Type not match
    );

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

/* ObBackupMacroBlockIDPair */

OB_SERIALIZE_MEMBER(ObCompatBackupMacroBlockIDPair, logic_id_, physical_id_);

ObCompatBackupMacroBlockIDPair::ObCompatBackupMacroBlockIDPair() : logic_id_(), physical_id_()
{}

ObCompatBackupMacroBlockIDPair::~ObCompatBackupMacroBlockIDPair()
{}

bool ObCompatBackupMacroBlockIDPair::operator<(const ObCompatBackupMacroBlockIDPair &other) const
{
  return logic_id_ < other.logic_id_;
}

void ObCompatBackupMacroBlockIDPair::reset()
{
  logic_id_.reset();
  physical_id_.reset();
}

bool ObCompatBackupMacroBlockIDPair::is_valid() const
{
  return logic_id_.is_valid() && physical_id_.is_valid();
}

/* ObBackupMacroBlockIDMapping */

ObBackupMacroBlockIDMapping::ObBackupMacroBlockIDMapping() : table_key_(), id_pair_list_(), map_()
{
  id_pair_list_.set_attr(lib::ObMemAttr(MTL_ID(), ObModIds::BACKUP));
}

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
      ObCompatBackupMacroBlockIDPair id_pair;
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
  : allocator_("BkpMBIDMapMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    id_map_list_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    version_(MAPPING_META_VERSION_MAX),
    sstable_count_(0)
{}

ObBackupMacroBlockIDMappingsMeta::~ObBackupMacroBlockIDMappingsMeta()
{
  for (int64_t i = 0; i < id_map_list_.count(); ++i) {
    if (OB_NOT_NULL(id_map_list_[i])) {
      ObBackupMacroBlockIDMapping *tmp = id_map_list_[i];
      tmp->~ObBackupMacroBlockIDMapping();
      allocator_.free(tmp);
      id_map_list_[i] = nullptr;
    }
  }
  id_map_list_.reset();
  allocator_.reset();
}

void ObBackupMacroBlockIDMappingsMeta::reuse()
{
  version_ = MAPPING_META_VERSION_MAX;
  sstable_count_ = 0;
  for (int64_t i = 0; i < id_map_list_.count(); ++i) {
    if (OB_NOT_NULL(id_map_list_[i])) {
      id_map_list_[i]->reuse();
    }
  }
}

int ObBackupMacroBlockIDMappingsMeta::prepare_id_mappings(const int64_t sstable_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(sstable_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable count is unexpected zero", K(ret), K(sstable_count), K(version_));
  } else if (OB_UNLIKELY(sstable_count_ > 0)) {
    ret = OB_ERR_SYS;
    LOG_WARN("sstable_count_ is unexpected not zero", K(ret), K(sstable_count_));
  } else if (FALSE_IT(sstable_count_ = sstable_count)) {
  } else if (0 == sstable_count_) {
    // do nothing
  } else {
    if (OB_FAIL(id_map_list_.prepare_allocate(sstable_count_))) {
      LOG_WARN("failed to reserve ip map list", K(ret), K(sstable_count_));
    }

    for (int64_t idx = 0; OB_SUCC(ret) && idx < sstable_count; ++idx) {
      void *buf = nullptr;
      ObBackupMacroBlockIDMapping *new_id_mapping = nullptr;

      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBackupMacroBlockIDMapping)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc mem for id mapping", K(ret), K(idx));
      } else {
        ObBackupMacroBlockIDMapping *new_id_mapping = new (buf) ObBackupMacroBlockIDMapping();
        id_map_list_[idx] = new_id_mapping;
      }
    }

    if (OB_FAIL(ret)) {
      id_map_list_.reset();
      allocator_.reset();
    }
  }
  return ret;
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
      if (OB_ISNULL(id_map_list_.at(i))) {
        ret = OB_ERR_SYS;
        LOG_WARN("get invalid null id mapping", K(ret), K(i));
      } else {
        const ObBackupMacroBlockIDMapping &item = *id_map_list_[i];
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
          const ObCompatBackupMacroBlockIDPair &pair = item.id_pair_list_.at(j);
          if (!pair.is_valid()) {
            ret = OB_ERR_SYS;
            LOG_WARN("get invalid data", K(ret), K(pair));
          } else {
            OB_UNIS_ENCODE(pair);
          }
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
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (version < MAPPING_META_VERSION_V1 || version > MAPPING_META_VERSION_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(version));
  } else {
    version_ = version;
    OB_UNIS_DECODE(num_of_sstable);
    if (FAILEDx(prepare_id_mappings(num_of_sstable))) {
      LOG_WARN("failed to prepare id mappings", K(ret), K(num_of_sstable));
    } else {
      ObITable::TableKey table_key;
      ObLogicMacroBlockId logic_id;
      ObCompatBackupMacroBlockIDPair pair;
      int64_t num_of_entries = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < num_of_sstable; ++i) {
        table_key.reset();
        OB_UNIS_DECODE(table_key);
        OB_UNIS_DECODE(num_of_entries);
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (!table_key.is_valid() || num_of_entries < 0) {
          ret = OB_ERR_SYS;
          LOG_WARN("table key is not valid", K(ret), K(table_key), K(num_of_entries));
        } else {
          id_map_list_[i]->table_key_ = table_key;
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < num_of_entries; ++j) {
          pair.reset();
          OB_UNIS_DECODE(pair);
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (!pair.is_valid()) {
            ret = OB_ERR_SYS;
            LOG_WARN("get invalid data", K(ret), K(pair));
          } else if (OB_FAIL(id_map_list_[i]->id_pair_list_.push_back(pair))) {
            LOG_WARN("failed to push back", K(ret), K(pair));
          }
        }
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
  int ret = OB_SUCCESS;
  if (sstable_count_ != id_map_list_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sstable count must be equal with the count of id map list",
        K(ret), K(sstable_count_), K(id_map_list_.count()));
  } else {
    OB_UNIS_ADD_LEN(version);
    OB_UNIS_ADD_LEN(num_of_sstable);
    if (num_of_sstable > 0) {
      for (int64_t i = 0; i < id_map_list_.count(); ++i) {
        const ObBackupMacroBlockIDMapping *item = id_map_list_[i];
        if (OB_ISNULL(item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("sstable count must be equal with the count of id map list",
              K(ret), K(sstable_count_), K(i), K(id_map_list_));
        } else {
          const ObITable::TableKey &table_key = item->table_key_;
          const ObArray<ObCompatBackupMacroBlockIDPair> &id_pair_list = item->id_pair_list_;
          int64_t num_of_entries = id_pair_list.count();
          OB_UNIS_ADD_LEN(table_key);
          OB_UNIS_ADD_LEN(num_of_entries);
          for (int64_t j = 0; j < num_of_entries; ++j) {
            const ObCompatBackupMacroBlockIDPair &pair = id_pair_list.at(j);
            OB_UNIS_ADD_LEN(pair);
          }
        }
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

uint64_t ObBackupMetaIndex::calc_hash(uint64_t seed) const
{
  uint64_t hash_code = 0;
  hash_code = meta_key_.calc_hash(seed);
  hash_code = murmurhash(&backup_set_id_, sizeof(backup_set_id_), hash_code);
  hash_code = murmurhash(&ls_id_, sizeof(ls_id_), hash_code);
  hash_code = murmurhash(&turn_id_, sizeof(turn_id_), hash_code);
  hash_code = murmurhash(&retry_id_, sizeof(retry_id_), hash_code);
  hash_code = murmurhash(&file_id_, sizeof(file_id_), hash_code);
  hash_code = murmurhash(&offset_, sizeof(offset_), hash_code);
  hash_code = murmurhash(&length_, sizeof(length_), hash_code);
  return hash_code;
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

int ObBackupRetryDesc::set(const share::ObLSID &ls_id, const int64_t turn_id,
    const int64_t retry_id, const int64_t last_file_id)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || turn_id < 1 || retry_id < 0 || last_file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(ls_id), K(turn_id), K(retry_id), K(last_file_id));
  } else {
    ls_id_ = ls_id;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    last_file_id_ = last_file_id;
  }
  return ret;
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

int ObLSBackupDataParam::convert_to(const ObBackupIndexLevel &index_level,
    const ObCompressorType &compressor_type, ObBackupIndexMergeParam &merge_param)
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
    merge_param.compressor_type_ = compressor_type;
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

int build_common_header(const int64_t data_type, const int64_t data_length,
    const int64_t data_zlength, const int64_t align_length, const ObCompressorType &compressor_type,
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
    common_header->compressor_type_ = compressor_type;
    common_header->header_length_ = header_len;
    common_header->data_length_ = data_length;
    common_header->data_zlength_ = data_zlength;
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

/* ObBackupDeviceMacroBlockId */

ObBackupDeviceMacroBlockId::ObBackupDeviceMacroBlockId()
  : first_id_(0),
    second_id_(0),
    third_id_(0)
{
}

ObBackupDeviceMacroBlockId::~ObBackupDeviceMacroBlockId()
{
}

// TODO(yanfeng): extract parameters later
int ObBackupDeviceMacroBlockId::set(const int64_t backup_set_id, const int64_t ls_id,
    const int64_t data_type, const int64_t turn_id, const int64_t retry_id,
    const int64_t file_id, const int64_t offset, const int64_t length,
    const BlockType &block_type)
{
  int ret = OB_SUCCESS;
  if (backup_set_id <= 0 || ls_id <= 0 || data_type < 0 || turn_id <= 0 || retry_id < 0 || file_id < 0
      || block_type >= BLOCK_TYPE_MAX || block_type < DATA_BLOCK) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_set_id), K(ls_id), K(data_type),
        K(turn_id), K(retry_id), K(file_id));
  } else {
    backup_set_id_ = backup_set_id;
    ls_id_ = ls_id;
    data_type_ = data_type;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    file_id_ = file_id;
    offset_ = offset / DIO_READ_ALIGN_SIZE;
    length_ = length / DIO_READ_ALIGN_SIZE;
    block_type_ = block_type;
    id_mode_ = static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP);
    version_ = BACKUP_MACRO_BLOCK_ID_VERSION;
  }
  return ret;
}

blocksstable::MacroBlockId ObBackupDeviceMacroBlockId::get_macro_id() const
{
  return blocksstable::MacroBlockId(first_id_, second_id_, third_id_, 0/*forth_id*/);
}

int ObBackupDeviceMacroBlockId::set(const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro id is not valid", K(ret), K(macro_id));
  } else {
    first_id_ = macro_id.first_id();
    second_id_ = macro_id.second_id();
    third_id_ = macro_id.third_id();
  }
  return ret;
}

bool ObBackupDeviceMacroBlockId::check_valid(const int64_t first_id,
     const int64_t second_id, const int64_t third_id)
{
  ObBackupDeviceMacroBlockId tmp_id;
  tmp_id.first_id_ = first_id;
  tmp_id.second_id_ = second_id;
  tmp_id.third_id_ = third_id;
  return tmp_id.is_valid();
}

bool ObBackupDeviceMacroBlockId::is_backup_block_file(const int64_t first_id)
{
  ObBackupDeviceMacroBlockId tmp_id;
  tmp_id.first_id_ = first_id;
  return static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP) == tmp_id.id_mode_;
}

void ObBackupDeviceMacroBlockId::reset()
{
  first_id_ = 0;
  second_id_ = 0;
  third_id_ = 0;
}

bool ObBackupDeviceMacroBlockId::is_valid() const
{
  return backup_set_id_ > 0 && ls_id_ > 0 && data_type_ >= 0 && turn_id_ > 0 && retry_id_ >= 0 && file_id_ >= 0
      && offset_ >= 0 && length_ > 0 && block_type_ >= DATA_BLOCK && block_type_ < BLOCK_TYPE_MAX
      && static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP) == id_mode_;
}

int ObBackupDeviceMacroBlockId::get_backup_macro_block_index(
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

const ObBackupDeviceMacroBlockId ObBackupDeviceMacroBlockId::get_default()
{
  static ObBackupDeviceMacroBlockId default_value;
  default_value.ls_id_ = INT64_MAX;
  default_value.data_type_ = MAX_BACKUP_DATA_TYPE;
  default_value.backup_set_id_ = MAX_BACKUP_SET_ID;
  default_value.turn_id_ = MAX_BACKUP_TURN_ID;
  default_value.retry_id_ = MAX_BACKUP_RETRY_ID;
  default_value.file_id_ = BACKUP_FILE_ID_BIT;
  default_value.offset_ = 0;
  default_value.length_ = MAX_BACKUP_BLOCK_SIZE;
  default_value.id_mode_ = static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP);
  default_value.version_ = BACKUP_MACRO_BLOCK_ID_VERSION;
  default_value.reserved_ = 0;
  return default_value;
}

ObBackupDeviceMacroBlockId &ObBackupDeviceMacroBlockId::operator=(const ObBackupDeviceMacroBlockId &other)
{
  first_id_ = other.first_id_;
  second_id_ = other.second_id_;
  third_id_ = other.third_id_;
  return *this;
}

bool ObBackupDeviceMacroBlockId::operator==(const ObBackupDeviceMacroBlockId &other) const
{
  return first_id_ == other.first_id_ && second_id_ == other.second_id_ && third_id_ == other.third_id_;
}

bool ObBackupDeviceMacroBlockId::operator!=(const ObBackupDeviceMacroBlockId &other) const
{
  return !(other == *this);
}

int ObBackupDeviceMacroBlockId::get_intermediate_tree_type(ObBackupIntermediateTreeType &tree_type) const
{
  int ret = OB_SUCCESS;
  if (INDEX_TREE_BLOCK == block_type_) {
    tree_type = ObBackupIntermediateTreeType::BACKUP_INDEX_TREE;
  } else if (META_TREE_BLOCK == block_type_) {
    tree_type = ObBackupIntermediateTreeType::BACKUP_META_TREE;
  } else  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block type is not expected", K(ret), K(block_type_));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupDeviceMacroBlockId, first_id_, second_id_, third_id_);

}  // namespace backup
}  // namespace oceanbase
