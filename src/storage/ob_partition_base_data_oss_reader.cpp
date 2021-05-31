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
#include "lib/utility/ob_tracepoint.h"
#include "lib/restore/ob_storage.h"
#include "share/restore/ob_restore_args.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "ob_partition_base_data_oss_reader.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace blocksstable;
namespace storage {
////////////////////////////////ObPartitionMetaOssBaseReader///////////////////////////////////
ObPartitionMetaStorageReader::ObPartitionMetaStorageReader()
    : is_inited_(false),
      sstable_index_(0),
      data_size_(0),
      sstable_meta_array_(),
      table_keys_array_(),
      args_(NULL),
      meta_allocator_(ObModIds::OB_OSS),
      table_count_(0),
      pkey_()
{}

ObPartitionMetaStorageReader::~ObPartitionMetaStorageReader()
{
  sstable_meta_array_.reset();  // sstable_meta_array_ must reset before destruction of meta_allocator
}
// meta info (partition meta, sstable meta, part list). Incremental won't use it
// Each version will be backed up, and the data_version on the path is the curr_data_version at the time of the backup
int ObPartitionMetaStorageReader::init(const share::ObRestoreArgs& args, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  uint64_t backup_table_id = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already inited", K(ret));
  } else if (!args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(args));
  } else {
    pkey_ = pkey;
    args_ = &args;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_table_keys())) {
    OB_LOG(WARN, "fail to read table keys", K(ret));
  } else if (OB_FAIL(read_all_sstable_meta())) {
    OB_LOG(WARN, "fail to read all sstable meta", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionMetaStorageReader::read_all_sstable_meta()
{
  int ret = OB_SUCCESS;
  ObStoragePath sstable_meta_path;
  char* read_buf = NULL;
  int64_t read_size = 0;

  if (!pkey_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "pkey is invalid", K(ret), K(pkey_));
  } else if (OB_FAIL(ObStoragePathUtil::generate_sstable_meta_file_path(ObString::make_string(args_->get_uri_header()),
                 ObString::make_string(args_->get_cluster_name()),
                 args_->curr_data_version_,
                 args_->tenant_id_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 sstable_meta_path))) {
    OB_LOG(WARN, "fail to generate sstable meta path", K(ret), K(*args_));
  } else if (OB_FAIL(ObOssReaderUtil::read_one_file(sstable_meta_path, *args_, meta_allocator_, read_buf, read_size))) {
    OB_LOG(WARN, "fail to read file", K(ret), K(sstable_meta_path), K(*args_));
  } else if (read_size <= 0) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "sstable meta is not exist", K(ret), K(sstable_meta_path), K(*args_));
  } else {
    data_size_ += read_size;
  }

  if (OB_SUCC(ret)) {
    int64_t saved_pos = 0;
    int64_t pos = 0;
    ObSSTableBaseMeta meta(meta_allocator_);
    for (int64_t i = 0; OB_SUCC(ret) && i < table_count_; ++i) {
      meta.reset();
      int64_t meta_length = 0;
      if (OB_FAIL(serialization::decode_i64(read_buf, read_size, pos, &meta_length))) {
        OB_LOG(WARN, "failed to decode meta_length", K(ret), K(read_size));
      } else if (meta_length <= 0) {
        ret = OB_ERR_SYS;
        OB_LOG(ERROR, "invalid meta length", K(ret), K(pos), K(meta_length), K(read_size));
      } else if (pos + meta_length > read_size) {
        ret = OB_ERR_SYS;
        OB_LOG(ERROR, "invalid meta length", K(ret), K(pos), K(meta_length), K(read_size));
      } else {
        saved_pos = pos;
        if (OB_FAIL(meta.deserialize(read_buf, read_size, pos))) {
          OB_LOG(WARN, "fail to deserialize sstable meta", K(read_buf), K(pos), K(ret));
        } else if (saved_pos + meta_length != pos) {
          ret = OB_ERR_SYS;
          OB_LOG(ERROR, "sstable deserialize length not match", K(ret), K(saved_pos), K(meta_length), K(pos), K(meta));
        } else if (OB_FAIL(sstable_meta_array_.push_back(meta))) {
          OB_LOG(WARN, "fail to add sstable meta", K(meta), K(ret));
        } else {
          OB_LOG(DEBUG, "succ to add sstable meta", K(read_size), K(pos), K(meta));
        }
      }
    }  // end while

    if (OB_SUCC(ret)) {
      if (sstable_meta_array_.count() != table_keys_array_.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "sstable meta array count is not equal table keys array count",
            K(ret),
            K(sstable_meta_array_.count()),
            K(table_keys_array_.count()));
      }
    }
  }
  return ret;
}

int ObPartitionMetaStorageReader::read_partition_meta(ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;
  ObStoragePath pmeta_path;
  char* read_buf = NULL;
  int64_t read_size = 0;
  int64_t meta_length = 0;
  int64_t saved_pos = 0;
  int64_t pos = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss reader is not inited", K(ret));
  } else if (OB_FAIL(
                 ObStoragePathUtil::generate_partition_meta_file_path(ObString::make_string(args_->get_uri_header()),
                     ObString::make_string(args_->get_cluster_name()),
                     args_->curr_data_version_,
                     args_->tenant_id_,
                     pkey_.get_table_id(),
                     pkey_.get_partition_id(),
                     pmeta_path))) {
    OB_LOG(WARN, "fail to generate partition meta", K(ret), K(*args_));
  } else if (OB_FAIL(ObOssReaderUtil::read_one_file(pmeta_path, *args_, meta_allocator_, read_buf, read_size))) {
    OB_LOG(WARN, "fail to read partition meta", K(ret), K(pmeta_path), K(*args_));
  } else if (read_size <= 0) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "partition meta is not exist", K(ret), K(pmeta_path), K(*args_));
  } else if (OB_FAIL(serialization::decode_i64(read_buf, read_size, pos, &meta_length))) {
    OB_LOG(WARN, "failed to decode meta_length", K(ret));
  } else if (pos + meta_length > read_size) {
    ret = OB_ERR_SYS;
    OB_LOG(ERROR, "invalid meta length", K(ret), K(pos), K(meta_length), K(read_size));
  } else {
    data_size_ += read_size;
    saved_pos = pos;
    if (OB_FAIL(partition_store_meta.deserialize(read_buf, read_size, pos))) {
      OB_LOG(WARN, "fail to deserialize partition_meta", K(ret));
    } else if (saved_pos + meta_length != pos) {
      ret = OB_ERR_SYS;
      OB_LOG(ERROR,
          "partition_meta deserialize length not match",
          K(ret),
          K(saved_pos),
          K(meta_length),
          K(pos),
          K(partition_store_meta));
    } else {
      OB_LOG(DEBUG, "succ to read patition meta from oss", K(partition_store_meta));
    }
  }
  return ret;
}

int ObPartitionMetaStorageReader::read_sstable_meta(const uint64_t backup_index_id, ObSSTableBaseMeta& meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == backup_index_id) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "backup index id is invalid", K(ret), K(backup_index_id));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < sstable_meta_array_.count(); ++i) {
      if (backup_index_id == sstable_meta_array_.at(i).index_id_) {
        if (OB_FAIL(meta.assign(sstable_meta_array_.at(i)))) {
          OB_LOG(WARN, "fail to assign sstable meta", K(ret));
        } else {
          found = true;
        }
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "can not find backup index id sstable meta", K(backup_index_id), K(sstable_meta_array_.count()));
    }
  }

  return ret;
}

int ObPartitionMetaStorageReader::read_sstable_pair_list(const uint64_t index_tid, ObIArray<ObSSTablePair>& part_list)
{
  int ret = OB_SUCCESS;
  ObStoragePath part_list_path;
  ObArenaAllocator allocator(ObModIds::RESTORE);
  char* read_buf = NULL;
  int64_t read_size = 0;
  part_list.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(ObStoragePathUtil::generate_part_list_file_path(ObString::make_string(args_->get_uri_header()),
                 ObString::make_string(args_->get_cluster_name()),
                 args_->curr_data_version_,
                 args_->tenant_id_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 index_tid,
                 part_list_path))) {
    OB_LOG(WARN, "fail to generate part list path", K(ret), K(*args_), K(index_tid));
  } else if (OB_FAIL(ObOssReaderUtil::read_one_file(part_list_path, *args_, allocator, read_buf, read_size))) {
    OB_LOG(WARN, "fail to read part list", K(ret), K(*args_));
  } else {
    data_size_ += read_size;
  }
  // part list is possible empty
  if (OB_SUCC(ret) && read_size > 0) {
    char* save_ptr = NULL;
    char* line = strtok_r(read_buf, "\n", &save_ptr);
    ObSSTablePair pair;

    while (OB_SUCC(ret) && NULL != line) {
      if (OB_UNLIKELY(2 != sscanf(line, "%ld_%ld", &pair.data_version_, &pair.data_seq_))) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "fail to get data version and data sequence", K(line), K(ret));
      } else if (OB_FAIL(part_list.push_back(pair))) {
        OB_LOG(WARN, "fail to add sstable pair", K(pair), K(ret));
      } else {
        OB_LOG(DEBUG, "succ to add sstable pair", K(pair));
        line = strtok_r(NULL, "\n", &save_ptr);
      }
    }
  }
  return ret;
}

int ObPartitionMetaStorageReader::read_table_keys()
{
  int ret = OB_SUCCESS;
  ObStoragePath pmeta_path;
  char* read_buf = NULL;
  int64_t read_size = 0;
  int64_t pos = 0;
  ;
  int64_t table_count = 0;
  if (OB_FAIL(ObStoragePathUtil::generate_table_keys_file_path(ObString::make_string(args_->get_uri_header()),
          ObString::make_string(args_->get_cluster_name()),
          args_->curr_data_version_,
          args_->tenant_id_,
          pkey_.get_table_id(),
          pkey_.get_partition_id(),
          pmeta_path))) {
    OB_LOG(WARN, "fail to generate table keys", K(ret), K(*args_));
  } else if (OB_FAIL(ObOssReaderUtil::read_one_file(pmeta_path, *args_, meta_allocator_, read_buf, read_size))) {
    OB_LOG(WARN, "fail to read table keys", K(ret), K(pmeta_path), K(*args_));
  } else if (read_size <= 0) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "table keys is not exist", K(ret), K(pmeta_path), K(*args_));
  } else if (OB_FAIL(serialization::decode_i64(read_buf, read_size, pos, &table_count))) {
    OB_LOG(WARN, "fail to decode table_count", K(ret));
  } else if (table_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "table count is invalid", K(ret), K(table_count));
  } else {
    table_count_ = table_count;
  }

  if (OB_SUCC(ret)) {
    ObITable::TableKey table_key;
    for (int64_t table_idx = 0; OB_SUCC(ret) && table_idx < table_count; ++table_idx) {
      table_key.reset();
      int64_t serialize_size = 0;
      if (OB_FAIL(serialization::decode_i64(read_buf, read_size, pos, &serialize_size))) {
        OB_LOG(WARN, "fail to deserialize table key size", K(ret), K(table_idx), K(table_count), K(read_size));
      } else if (OB_FAIL(table_key.deserialize(read_buf, read_size, pos))) {
        OB_LOG(WARN, "fail to deserialize table key", K(ret), K(table_idx), K(table_count), K(read_size));
      } else if (serialize_size != table_key.get_serialize_size()) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "table keys serialize size is not match", K(ret), K(serialize_size), K(table_key));
      } else if (OB_FAIL(table_keys_array_.push_back(table_key))) {
        OB_LOG(WARN, "fail to push backup table key", K(ret), K(table_key));
      } else {
        OB_LOG(DEBUG, "succ to add table key meta", K(read_size), K(pos), K(table_key));
      }
    }
  }

  return ret;
}

int ObPartitionMetaStorageReader::read_table_ids(ObIArray<uint64_t>& table_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss reader is not inited", K(ret));
  } else if (get_sstable_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "sstable meta array count is unexpected", K(ret), K(get_sstable_count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sstable_count(); ++i) {
      if (OB_FAIL(table_id_array.push_back(static_cast<uint64_t>(sstable_meta_array_.at(i).index_id_)))) {
        OB_LOG(WARN, "fail to push table id in to array", K(ret), K(sstable_meta_array_.at(i).index_id_));
      }
    }
  }
  return ret;
}

int ObPartitionMetaStorageReader::read_table_keys_by_table_id(
    const uint64_t table_id, ObIArray<ObITable::TableKey>& table_keys_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss reader is not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "table id is invalid", K(ret), K(table_id));
  } else if (table_keys_array_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "table_keys_array count is unexpected", K(ret), K(table_keys_array_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_keys_array_.count(); ++i) {
      ObITable::TableKey table_key = table_keys_array_.at(i);
      if (table_id == table_key.table_id_) {
        if (OB_FAIL(table_keys_array.push_back(table_key))) {
          OB_LOG(WARN, "fail to put table key into table array", K(ret), K(table_key));
        }
      }
    }
  }
  return ret;
}

//////////////////////////////////////ObMacroBlockOssBaseReader////////////////////////////////
ObMacroBlockStorageReader::ObMacroBlockStorageReader() : allocator_(ObModIds::RESTORE_MACRO_BLOCK)
{
  is_inited_ = false;
  args_ = NULL;
  allocator_.reset();
  data_size_ = 0;
  result_code_ = OB_SUCCESS;
  is_data_ready_ = false;
  backup_pair_.data_version_ = 0;
  backup_pair_.data_seq_ = 0;
  backup_index_tid_ = 0;
  meta_ = NULL;
  data_.assign(NULL, 0, 0);
  init_ts_ = 0;
  finish_ts_ = 0;
  is_scheduled_ = false;
  bandwidth_throttle_ = NULL;
  pkey_.reset();
}

ObMacroBlockStorageReader::~ObMacroBlockStorageReader()
{
  reset();
}

void ObMacroBlockStorageReader::reset()
{
  int tmp_ret = wait_finish();
  if (OB_SUCCESS != tmp_ret) {
    OB_LOG(WARN, "failed to wait finish", K(tmp_ret), K(*this));
  }

  is_inited_ = false;
  args_ = NULL;
  allocator_.reset();
  data_size_ = 0;
  result_code_ = OB_SUCCESS;
  is_data_ready_ = false;
  backup_pair_.data_version_ = 0;
  backup_pair_.data_seq_ = 0;
  backup_index_tid_ = 0;
  meta_ = NULL;
  data_.assign(NULL, 0, 0);
  init_ts_ = 0;
  finish_ts_ = 0;
  is_scheduled_ = false;
  pkey_.reset();
  // no need to reset bandwidth_throttle_
}

// A single macro block data will be reused when the data information is incremented or rebuild, and the entire amount
// will not be reused. The data_version on the path is the base_data_version of the reused version
int ObMacroBlockStorageReader::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const share::ObRestoreArgs& args, const ObPartitionKey& pkey, const uint64_t backup_index_tid,
    const blocksstable::ObSSTablePair& backup_pair)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "has inited", K(ret));
  } else if (!args.is_valid() || backup_pair.data_seq_ < 0 || backup_pair.data_version_ <= 0 || backup_index_tid <= 0 ||
             !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(
        WARN, "argument is invalid", K(ret), K(args.is_valid()), K(backup_pair), K(backup_index_tid), K(args), K(pkey));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::RESTORE_READER_COND_WAIT))) {
    OB_LOG(WARN, "failed to init cond_", K(ret));
  } else {
    is_inited_ = true;
    args_ = &args;
    allocator_.reset();
    is_data_ready_ = false;
    data_size_ = 0;
    backup_pair_ = backup_pair;
    backup_index_tid_ = backup_index_tid;
    meta_ = NULL;
    data_.assign(NULL, 0, 0);  // set to init stat
    result_code_ = OB_SUCCESS;
    bandwidth_throttle_ = &bandwidth_throttle;
    init_ts_ = ObTimeUtility::current_time();
    pkey_ = pkey;
    is_scheduled_ = false;
  }
  return ret;
}

int ObMacroBlockStorageReader::process(const bool& is_stop)
{
  int ret = OB_SUCCESS;
  ObStoragePath file_path;
  blocksstable::ObMacroBlockMeta tmp_meta;
  common::ObObj* endkey = NULL;
  int64_t meta_length = 0;
  int64_t macro_block_size = 0;
  char* read_buf = NULL;
  ObStorageUtil util(true /*need retry*/);
  int64_t read_size = 0;

  data_size_ = 0;
  allocator_.reuse();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not inited", K(ret), K(*this));
  } else if (is_stop) {
    ret = OB_IN_STOP_STATE;
    OB_LOG(WARN, "cannot fetch macro block in stop state", K(ret), K(*this));
  } else if (is_data_ready_) {
    ret = OB_ERR_SYS;
    OB_LOG(WARN, "cannot process twice", K(ret));
  } else if (OB_FAIL(
                 ObStoragePathUtil::generate_physic_macro_data_file_path(ObString::make_string(args_->get_uri_header()),
                     ObString::make_string(args_->get_cluster_name()),
                     args_->base_data_version_,
                     args_->tenant_id_,
                     pkey_.get_table_id(),
                     pkey_.get_partition_id(),
                     backup_index_tid_,
                     backup_pair_.data_version_,
                     backup_pair_.data_seq_,
                     file_path))) {
    OB_LOG(WARN, "fail to generate macro data file path", K(ret), K(args_));
  } else if (OB_FAIL(util.get_file_length(file_path.get_obstring(), args_->get_storage_info(), data_size_))) {
    OB_LOG(WARN, "fail to get file len", K(ret), K(file_path), K(*this));
  } else if (OB_UNLIKELY(data_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "file len is invalid", K(ret), K(file_path), K(*this));
  } else if (OB_ISNULL(read_buf = static_cast<char*>(allocator_.alloc(data_size_ + DIO_READ_ALIGN_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc memory for reading macro block", K(ret), K(*this));
  } else if (OB_FAIL(util.read_single_file(
                 file_path.get_obstring(), args_->get_storage_info(), read_buf, data_size_, read_size))) {
    OB_LOG(WARN, "fail to read macro block from oss", K(ret), K(file_path), K(*this));
  } else if (OB_ISNULL(read_buf) || OB_UNLIKELY(read_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "read buf is invalid", K(read_buf), K(read_size), K(ret), K(*this));
  } else if (OB_ISNULL(
                 endkey = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * common::OB_MAX_COLUMN_NUMBER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc memory for macro block end key", K(ret), K(*this));
  } else {
    endkey = new (endkey) ObObj[common::OB_MAX_COLUMN_NUMBER];
    tmp_meta.endkey_ = endkey;
    int64_t pos = 0;
    if (OB_FAIL(serialization::decode_i64(read_buf, read_size, pos, &meta_length))) {
      OB_LOG(WARN, "failed to decode meta_length", K(ret), K(*this));
    } else if (pos + meta_length > read_size) {
      ret = OB_ERR_SYS;
      OB_LOG(ERROR, "invalid meta length", K(ret), K(pos), K(meta_length), K(read_size), K(*this));
    } else if (OB_FAIL(tmp_meta.deserialize(read_buf, pos + meta_length, pos))) {
      OB_LOG(WARN, "fail to deserialize macro block meta", K(read_buf), K(pos), K(ret), K(*this));
    } else if (OB_FAIL(tmp_meta.deep_copy(meta_, allocator_))) {
      OB_LOG(WARN, "failed to deep copy tmp_meta", K(ret), K(*this));
    } else if (OB_ISNULL(meta_)) {
      ret = OB_ERR_SYS;
      OB_LOG(ERROR, "meta must not null", K(tmp_meta), K(ret), K(*this));
    } else if (OB_FAIL(serialization::decode_i64(read_buf, read_size, pos, &macro_block_size))) {
      OB_LOG(WARN, "failed to decode macro_block_size", K(ret), K(*this));
    } else if (pos + macro_block_size != read_size || read_size != data_size_) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(
          ERROR, "invalid macro_block_size", K(ret), K(*meta_), K(pos), K(macro_block_size), K(read_size), K(*this));
    } else {
      int64_t data_size = read_size - pos;
      int64_t write_data_size = upper_align(data_size, DIO_READ_ALIGN_SIZE);
      if (write_data_size + pos <= data_size_ + DIO_READ_ALIGN_SIZE) {
        data_.assign(read_buf + pos, write_data_size, data_size);
      } else {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "upper align error", K(ret), K(data_size), K(write_data_size), K(pos), K(*this));
      }
    }
  }

  if (NULL != bandwidth_throttle_) {
    finish_ts_ = ObTimeUtility::current_time();
    bandwidth_throttle_->limit_in_and_sleep(read_size, finish_ts_, DEFAULT_WAIT_TIME);
  }
  if (OB_SUCC(ret)) {
    is_data_ready_ = true;
  } else {
    result_code_ = ret;
  }

  ObThreadCondGuard guard(cond_);
  cond_.broadcast();

  return ret;
}

int ObMacroBlockStorageReader::get_macro_block_meta(
    blocksstable::ObMacroBlockMeta*& meta, blocksstable::ObBufferReader& data)
{
  int ret = OB_SUCCESS;
  meta = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not inited", K(ret), K(*this));
  } else if (OB_FAIL(wait_finish())) {
    OB_LOG(WARN, "failed to wait finish", K(ret), K(*this));
  } else if (OB_FAIL(result_code_)) {
    OB_LOG(WARN, "error happened during fetch macro block", K(ret), K(*this));
  } else if (!is_data_ready_) {
    ret = OB_ERR_SYS;
    OB_LOG(ERROR, "copy macro block is success, but data is not ready, fatal error", K(ret), K(*this));
  } else {
    meta = meta_;
    data = data_;
  }
  return ret;
}

int ObMacroBlockStorageReader::wait_finish()
{
  int ret = OB_SUCCESS;

  if (is_scheduled_) {
    while (OB_SUCC(ret) && OB_SUCCESS == result_code_ && !is_data_ready_) {
      ObThreadCondGuard guard(cond_);
      if (OB_FAIL(cond_.wait_us(DEFAULT_WAIT_TIME))) {
        if (OB_TIMEOUT == ret) {
          ret = OB_SUCCESS;
          OB_LOG(WARN, "macro block restore is too slow", K(ret), K(*this));
        }
      }
    }
  }
  return ret;
}

//////////////////////////////////////ObPartitionGroupMetaReader////////////////////////////////

ObPartitionGroupMetaReader::ObPartitionGroupMetaReader()
    : is_inited_(false), data_size_(0), args_(NULL), meta_allocator_(ObModIds::OB_OSS)
{}

int ObPartitionGroupMetaReader::init(const share::ObRestoreArgs& args)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already inited", K(ret));
  } else if (!args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(args));
  } else {
    args_ = &args;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionGroupMetaReader::read_partition_group_meta(ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  ObStoragePath pg_meta_path;
  char* read_buf = NULL;
  int64_t read_size = 0;
  int64_t meta_length = 0;
  int64_t saved_pos = 0;
  int64_t pos = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss reader is not inited", K(ret));
  } else if (OB_FAIL(ObStoragePathUtil::generate_partition_group_meta_file_path(
                 ObString::make_string(args_->get_uri_header()),
                 ObString::make_string(args_->get_cluster_name()),
                 args_->curr_data_version_,
                 args_->tenant_id_,
                 args_->backup_schema_id_,
                 args_->partition_id_,
                 pg_meta_path))) {
    OB_LOG(WARN, "fail to generate partition meta", K(ret), K(*args_));
  } else if (OB_FAIL(ObOssReaderUtil::read_one_file(pg_meta_path, *args_, meta_allocator_, read_buf, read_size))) {
    OB_LOG(WARN, "fail to read partition meta", K(ret), K(pg_meta_path), K(*args_));
  } else if (read_size <= 0) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "partition meta is not exist", K(ret), K(pg_meta_path), K(*args_));
  } else if (OB_FAIL(serialization::decode_i64(read_buf, read_size, pos, &meta_length))) {
    OB_LOG(WARN, "failed to decode meta_length", K(ret));
  } else if (pos + meta_length > read_size) {
    ret = OB_ERR_SYS;
    OB_LOG(ERROR, "invalid meta length", K(ret), K(pos), K(meta_length), K(read_size));
  } else {
    data_size_ += read_size;
    saved_pos = pos;
    if (OB_FAIL(pg_meta.deserialize(read_buf, read_size, pos))) {
      OB_LOG(WARN, "fail to deserialize partition_meta", K(ret));
    } else if (saved_pos + meta_length != pos) {
      ret = OB_ERR_SYS;
      OB_LOG(ERROR,
          "partition_meta deserialize length not match",
          K(ret),
          K(saved_pos),
          K(meta_length),
          K(pos),
          K(pg_meta));
    } else {
      OB_LOG(DEBUG, "succ to read patition meta from oss", K(pg_meta));
    }
  }
  return ret;
}

//////////////////////////////////////ObOssReaderUtil////////////////////////////////

int ObOssReaderUtil::read_one_file(const common::ObStoragePath& path, const share::ObRestoreArgs& args,
    ObIAllocator& allocator, char*& buf, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true /*need retry*/);
  int64_t file_len = 0;
  read_size = 0;
  int64_t buffer_len = 0;

  if (!path.is_valid() || !args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(path), K(args));
  } else if (OB_FAIL(util.get_file_length(path.get_obstring(), args.get_storage_info(), file_len))) {
    OB_LOG(WARN, "fail to get file len", K(ret), K(path), K(args));
  } else if (file_len <= 0) {  // part_list is possible empty, just return success
    OB_LOG(INFO, "this file is empty", K(path), K(args), K(file_len));
  } else {
    buffer_len = file_len + 1;  // last for '\0'
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(ERROR, "fail to alloc memory", K(ret), K(file_len), K(buffer_len));
    } else if (OB_FAIL(util.read_single_file(path.get_obstring(), args.get_storage_info(), buf, file_len, read_size))) {
      OB_LOG(WARN, "fail to read all data", K(ret), K(path));
    } else if (OB_UNLIKELY(read_size != file_len)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "read size is invalid", K(ret), K(read_size), K(file_len), K(buffer_len));
    } else {
      buf[file_len] = '\0';
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
