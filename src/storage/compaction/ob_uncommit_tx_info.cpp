/**
 * Copyright (c) 2025 OceanBase
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
#include "ob_uncommit_tx_info.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"
#include  "storage/blocksstable/ob_data_store_desc.h"
#include  "storage/blocksstable/ob_datum_row.h"
#include  "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include  "storage/tx/ob_tx_data_define.h"
#include  "storage/tx/ob_tx_data_functor.h"
#include  "storage/ob_gc_upper_trans_helper.h"
#include  "storage/compaction/ob_partition_merge_iter.h"

# define TOSTRING(CLS, COUNT) \
int64_t CLS::to_string(char *buf, const int64_t buf_len) const \
{  \
  int64_t pos = 0;\
  if (OB_ISNULL(buf) || buf_len <= 0) { \
  } else {   \
    J_OBJ_START(); \
    J_KV(KP(this), K_(version), K_(info_status), K_(total_uncommit_row_count), K(COUNT));\
    if (COUNT > 0) { \
      J_COMMA(); \
      J_ARRAY_START();\
      for (int64_t i = 0; i < COUNT; ++i) {\
        const ObUncommitTxDesc tx_desc = tx_infos_[i];\
        J_OBJ_START();\
        J_KV(K(i), K(tx_desc));\
        J_OBJ_END();\
        if (i != COUNT -1) {\
          J_COMMA();\
        }\
      }\
      J_ARRAY_END();\
    }\
    J_OBJ_END();\
  }\
  return pos;\
}


namespace oceanbase
{
namespace compaction
{

OB_SERIALIZE_MEMBER_SIMPLE(ObUncommitTxDesc, tx_id_, sql_seq_, flag_);

bool ObUncommitTxDesc::operator ==(const ObUncommitTxDesc &other) const
{
  return tx_id_ == other.tx_id_ && sql_seq_ == other.sql_seq_;
}

void ObUncommitTxDesc::set_key_status_txid()
{
  sql_seq_ = 0;
  key_status_ = ObUncommitTxDesc::TXID;
}

int64_t ObUncommitTxDesc::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    J_OBJ_START();
    J_KV(K_(tx_id));
    if (is_commit_version()) {
      J_COMMA();
      J_KV("commit_version", commit_version_);
    } else if (is_sql_seq()) {
      J_COMMA();
      J_KV("sql_seq", sql_seq_);
    }
    J_OBJ_END();
  }
  return pos;
}

int ObMetaUncommitTxInfo::push_back(const ObUncommitTxDesc &tx_seq_key)
{
  int ret = OB_SUCCESS;
  if (uncommit_tx_desc_count_ >= MAX_TX_INFO_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("uncommit txinfo array overflow", K(ret), K(uncommit_tx_desc_count_));
  } else {
    tx_infos_[uncommit_tx_desc_count_++] = tx_seq_key;
  }
  return ret;
}


int ObMetaUncommitTxInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, summary_))) {
    STORAGE_LOG(WARN, "fail to encode summary", K(ret), K(buf_len), K(pos), K(summary_));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, total_uncommit_row_count_))) {
    STORAGE_LOG(WARN, "fail to encode total uncommit row count", K(ret), K(buf_len), K(pos) ,K(total_uncommit_row_count_));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, uncommit_tx_desc_count_))) {
    STORAGE_LOG(WARN, "fail to encode uncommit tx info count", K(ret), K(buf_len), K(pos), K(uncommit_tx_desc_count_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < uncommit_tx_desc_count_; i ++) {
      if (OB_FAIL(serialization::encode(buf, buf_len, pos, tx_infos_[i]))) {
        STORAGE_LOG(WARN, "fail to encode uncommit tx info array", K(ret), K(buf_len), K(pos));
      }
    }
  }
  return ret;
}

int64_t ObMetaUncommitTxInfo::get_deep_copy_size() const
{
  return sizeof(ObUncommitTxDesc) * uncommit_tx_desc_count_;
}

int ObMetaUncommitTxInfo::deep_copy(char *buf, const int64_t buf_len, int64_t &pos, ObMetaUncommitTxInfo &dest) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size + pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_copy_size), K(pos));
  } else {
    dest.summary_ = summary_;
    dest.total_uncommit_row_count_ = total_uncommit_row_count_;
    dest.uncommit_tx_desc_count_ = uncommit_tx_desc_count_;
    if (uncommit_tx_desc_count_ > 0) {
      dest.tx_infos_ = reinterpret_cast<ObUncommitTxDesc *>(buf + pos);
      MEMCPY(dest.tx_infos_, tx_infos_, deep_copy_size);
      pos += deep_copy_size;
    } else {
      dest.tx_infos_ = nullptr;
    }
  }
  return ret;
}

int ObMetaUncommitTxInfo::deserialize(ObArenaAllocator &allocator, const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_vi32(buf, buf_len, pos, &summary_))) {
    STORAGE_LOG(WARN, "fail to decode summary", K(ret), K(buf_len), K(pos), K(summary_));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &total_uncommit_row_count_))) {
    STORAGE_LOG(WARN, "fail to decode total uncommit row count", K(ret), K(buf_len), K(pos), K(total_uncommit_row_count_));
  } else if (OB_FAIL(serialization::decode_vi32(buf, buf_len, pos, &uncommit_tx_desc_count_))) {
    STORAGE_LOG(WARN, "fail to decode uncommit tx info count", K(ret), K(buf_len), K(pos), K(uncommit_tx_desc_count_));
  } else if (0 == uncommit_tx_desc_count_) { // skip
  } else if (OB_ISNULL(tx_infos_ = static_cast<ObUncommitTxDesc *>(allocator.alloc(sizeof(ObUncommitTxDesc) * uncommit_tx_desc_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate column checksum memory", K(ret), K(buf_len), K(pos), K(uncommit_tx_desc_count_));
  } else if (OB_FAIL(deserialize(buf, buf_len, pos))){
    STORAGE_LOG(WARN, "fail to decode tx infos array", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

// Only use to decode uncommit tx infos array
int ObMetaUncommitTxInfo::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < uncommit_tx_desc_count_; i ++) {
    if (OB_FAIL(serialization::decode(buf, buf_len, pos, tx_infos_[i]))) {
      STORAGE_LOG(WARN, "fail to decode uncommit tx info array", K(ret), K(buf_len), K(pos));
    }
  }
  return ret;
}

int64_t ObMetaUncommitTxInfo::get_serialize_size() const
{
  int64_t size = serialization::encoded_length_vi32(summary_);
  size += serialization::encoded_length_vi32(uncommit_tx_desc_count_);
  size += serialization::encoded_length_vi64(total_uncommit_row_count_);
  for (int64_t i = 0; i < uncommit_tx_desc_count_; i++) {
    size += serialization::encoded_length(tx_infos_[i]);
  }
  return size;
}

TOSTRING(ObMetaUncommitTxInfo, uncommit_tx_desc_count_);


void ObMetaUncommitTxInfo::reset()
{
  info_status_ = INFO_OVERFLOW;
  uncommit_tx_desc_count_ = 0;
  total_uncommit_row_count_ = 0;
  reserve_ = 0;
  version_ = UNCOMMIT_TX_VERSION_V1;
  tx_infos_ = nullptr;
}

int ObMetaUncommitTxInfo::init(common::ObArenaAllocator &allocator, const ObBasicUncommitTxInfo &tmp_uncommit_info)
{
  int ret = OB_SUCCESS;
  int64_t array_size = tmp_uncommit_info.get_info_count();
  void *tx_infos_buf = nullptr;
  if (array_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array size can not smaller than 0", K(ret), K(array_size), K(tmp_uncommit_info));
  } else if (array_size == 0) {
    summary_ = tmp_uncommit_info.summary_;
  } else if (OB_ISNULL(tx_infos_buf = static_cast<void *>(allocator.alloc(array_size * sizeof(ObUncommitTxDesc))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for uncommit txinfo", K(ret), K(array_size), KP(tx_infos_buf), K(tmp_uncommit_info));
  } else {
    tx_infos_ = new (tx_infos_buf) ObUncommitTxDesc[array_size];
    summary_ = tmp_uncommit_info.summary_;
    total_uncommit_row_count_ = tmp_uncommit_info.total_uncommit_row_count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < array_size; ++i) {
      ObUncommitTxDesc tx_desc;
      if (OB_FAIL(tmp_uncommit_info.get_uncommit_tx_desc(tx_desc, i))) {
        LOG_WARN("unable to get uncommit tx desc", K(ret), K(tmp_uncommit_info), K(i), K(tx_desc));
      } else if (OB_FAIL(push_back(tx_desc))) {
        LOG_WARN("push back into ObMemUncommitTxInfo error", K(ret), K(tmp_uncommit_info), K(i), K(tx_desc));
      }
    }
    if (OB_FAIL(ret)) {
      allocator.free(tx_infos_buf);
      reset();
    }
  }
  return ret;
}

int64_t ObMetaUncommitTxInfo::get_info_count() const
{
  return static_cast<int64_t> (uncommit_tx_desc_count_);
}

int ObMetaUncommitTxInfo::get_uncommit_tx_desc(ObUncommitTxDesc &uncommit_tx_desc, int64_t idx) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= uncommit_tx_desc_count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "idx passed in is invalid", K(ret), K(idx), K(uncommit_tx_desc_count_));
  } else {
    uncommit_tx_desc = tx_infos_[idx];
  }
  return ret;
}

int ObMemUncommitTxInfo::push_back(const ObUncommitTxDesc &tx_seq_key)
{
  int ret = OB_SUCCESS;
  if (tx_infos_.count() >= MAX_TX_INFO_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("uncommit txinfo array overflow", K(ret), K(tx_infos_.count()));
  } else if (OB_FAIL(tx_infos_.push_back(tx_seq_key))) {
    LOG_WARN("unable to push back to SEArray", K(ret), K(tx_infos_));
  }
  return ret;
}

TOSTRING(ObMemUncommitTxInfo, tx_infos_.count());

int ObMemUncommitTxInfo::assign(const ObBasicUncommitTxInfo &uncommit_tx_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!uncommit_tx_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommit tx info passed in is invalid", K(ret), K(uncommit_tx_info));
  } else {
    summary_ = uncommit_tx_info.summary_;
    total_uncommit_row_count_ = uncommit_tx_info.total_uncommit_row_count_;
    int64_t array_size = uncommit_tx_info.get_info_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < array_size; ++i) {
      ObUncommitTxDesc tx_desc;
      if (OB_FAIL(uncommit_tx_info.get_uncommit_tx_desc(tx_desc, i))) {
        LOG_WARN("unable to get uncommit tx desc", K(ret));
      } else if (OB_FAIL(push_back(tx_desc))) {
        LOG_WARN("push back into ObMemUncommitTxInfo error", K(ret));
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMemUncommitTxInfo, summary_, total_uncommit_row_count_, tx_infos_);

void ObMemUncommitTxInfo::reuse()
{
  info_status_ = RECORD_SEQ;
  total_uncommit_row_count_ = 0;
  tx_infos_.reuse();
}

void ObMemUncommitTxInfo::reset()
{
  info_status_ = INFO_OVERFLOW;
  total_uncommit_row_count_ = 0;
  reserve_ = 0;
  version_ = UNCOMMIT_TX_VERSION_V1;
  tx_infos_.reset();
}

bool ObMemUncommitTxInfo::need_collect_uncommit_tx_info(const blocksstable::ObSSTable* sstable)
{
  return !sstable->get_key().tablet_id_.is_inner_tablet() && (sstable->is_minor_sstable() || sstable->is_inc_major_type_sstable() || sstable->is_inc_major_ddl_sstable());
}

int64_t ObMemUncommitTxInfo::get_info_count() const
{
  return tx_infos_.count();
}

int ObMemUncommitTxInfo::get_uncommit_tx_desc(ObUncommitTxDesc &uncommit_tx_desc, int64_t idx) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= tx_infos_.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "idx passed in is invalid", K(ret), K(idx), K(tx_infos_.count()));
  } else {
    uncommit_tx_desc = tx_infos_[idx];
  }
  return ret;
}

int ObUncommitTxInfoCollector::add_into_set(ObUncommitTxDesc &tx_seq_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tx_seq_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx_seq_key passed is invalid", K(ret), K(tx_seq_key), K(*this));
  } else if (uncommit_tx_info_.is_info_overflow()) {
    // Expected situation, not need to print log
    ret = OB_SIZE_OVERFLOW;
  } else {
    // Avoid using is_only_txid() here, because get_info_count() may return 0 when tx info status changes
    if (uncommit_tx_info_.info_status_ ==  ObBasicUncommitTxInfo::ONLY_TXID && !tx_seq_key.is_commit_version()) {
      tx_seq_key.set_key_status_txid();
    }
    if (OB_SUCC(ret)) {
      ret = txinfo_set_.exist_refactored(tx_seq_key);
      if (OB_HASH_NOT_EXIST == ret) {
        if (uncommit_tx_info_.tx_infos_.count() < ObBasicUncommitTxInfo::MAX_TX_INFO_COUNT) {
          if (OB_FAIL(txinfo_set_.set_refactored(tx_seq_key, 0 /* not return OB_HASH_EXIST when flag !=0 */))) {
            LOG_WARN("add into txinfo set error", K(ret), K(*this));
          }
        } else if (uncommit_tx_info_.is_record_seq()) {
          if (OB_FAIL(convert_set_to_only_txid())) {
            LOG_WARN("unable to convert set to only txid", K(ret));
          } else {
            // need to add into set again to check if tx_seq_key duplicated.
            if (OB_FAIL(add_into_set(tx_seq_key))) {
              if (ret != OB_HASH_EXIST && ret != OB_SIZE_OVERFLOW) {
                LOG_WARN("ERROR occured when add into tx info set", K(ret), K(*this));
              }
            }
          }
        } else {
          ret = OB_SIZE_OVERFLOW;
          // when INFO_OVERFLOW, abort uncommit tx info.
          reset();
          uncommit_tx_info_.set_info_status_overflow();
        }
      } else if (ret != OB_HASH_EXIST) {
        // exist_refactored returns OB_HASH_EXIST or OB_HASH_NOT_EXIST; otherwise, an error occurs.
        LOG_WARN("ERROR occured when exist refactored", K(ret), K(*this));
      }
    }
  }
  return ret;
}

int ObUncommitTxInfoCollector::convert_set_to_only_txid()
{
  int ret = OB_SUCCESS;
  ObMemUncommitTxInfo tmp_uncommit_info;
  if (OB_FAIL(tmp_uncommit_info.assign(uncommit_tx_info_))) {
    LOG_WARN("tmp_uncommit_info assign fail", K(ret), K(*this));
  } else {
    reuse();
    uncommit_tx_info_.set_info_status_only_tx_id();
    if (OB_FAIL(push_back(tmp_uncommit_info))) {
      LOG_WARN("push back to txinfos error", K(ret), K(*this));
    }
  }
  return ret;
}

int ObUncommitTxInfoCollector::push_back(const ObBasicUncommitTxInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed is invalid", K(ret), K(other));
  } else if (!uncommit_tx_info_.is_info_overflow() && !other.is_info_overflow()) {
    if (uncommit_tx_info_.is_record_seq() && other.is_only_txid()) {
      if (OB_FAIL(convert_set_to_only_txid())) {
        LOG_WARN("convert set to only txid failed", K(ret));
      }
    }
    int64_t array_size = other.get_info_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < array_size; ++i) {
      ObUncommitTxDesc tx_desc;
      if (OB_FAIL(other.get_uncommit_tx_desc(tx_desc, i))) {
        LOG_WARN("unable to get uncommit tx desc", K(ret));
      } else if (OB_FAIL(push_back(tx_desc))) {
        LOG_WARN("push back into ObMemUncommitTxInfo error", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!uncommit_tx_info_.is_info_overflow()) {
        uncommit_tx_info_.total_uncommit_row_count_ += other.total_uncommit_row_count_;
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("push back into ObMemUncommitTxInfo error", K(ret));
      }
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    // when INFO_OVERFLOW, abort uncommit tx info
    reset();
    uncommit_tx_info_.set_info_status_overflow();
    LOG_WARN("uncommit tx info passed in is overflow", K(ret), K(other));
  }
  return ret;
}

int ObUncommitTxInfoCollector::push_back(const ObUncommitTxDesc &tx_seq_key)
{
  int ret = OB_SUCCESS;
  ObUncommitTxDesc tmp_tx_seq_key = tx_seq_key;
  if (OB_UNLIKELY(!tx_seq_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommit txinfo now is invalid", K(ret) , K(*this));
  } else if (uncommit_tx_info_.is_info_overflow()) {
    // when INFO_OVERFLOW, do nothing
  } else {
    if (OB_FAIL(add_into_set(tmp_tx_seq_key))) {
      // expected situation, should not return errorcode
      if (ret == OB_SIZE_OVERFLOW || ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("unable to add into txinfo set", K(ret) , K(*this));
      }
    } else if (OB_FAIL(uncommit_tx_info_.push_back(tmp_tx_seq_key))) {
      LOG_WARN("SEarray tx infos pushback error", K(ret));
    }
  }
  return ret;
}

int ObUncommitTxInfoCollector::collect_uncommit_tx_info(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (sql_seq_col_index_ >= row.count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sql sequence col idx should not greater than or equal to column count", K(ret), K(sql_seq_col_index_), K(row.count_));
  } else if (row.mvcc_row_flag_.is_uncommitted_) {
    ++uncommit_tx_info_.total_uncommit_row_count_;
    int64_t tx_id = row.trans_id_.get_id();
    int64_t sql_seq = row.storage_datums_[sql_seq_col_index_].get_int();
    ObUncommitTxDesc txseqkey(tx_id, sql_seq);
    if (OB_FAIL(push_back(txseqkey))) {
      STORAGE_LOG(WARN, "Fail to collect tx info", K(ret), K(txseqkey));
    }
  }
  return ret;
}

int ObUncommitTxInfoCollector::init(const int64_t sql_seq_col_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sql_seq_col_index <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sql seq col index can not less than or equal to 0", K(ret), K(sql_seq_col_index));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "TxInfoSet has been inited twice", K(ret), K(*this));
  } else if (OB_FAIL(txinfo_set_.create(43/* 32/0.75 */, "TxInfoSet", "TxInfoSet"))) {
    STORAGE_LOG(WARN, "TxInfoSet init failed", K(ret), K(*this));
  } else {
    uncommit_tx_info_.reuse();
    is_inited_ = true;
    sql_seq_col_index_ = sql_seq_col_index;
  }
  return ret;
}

void ObUncommitTxInfoCollector::reuse()
{
  uncommit_tx_info_.reuse();
  txinfo_set_.clear();
}

void ObUncommitTxInfoCollector::reset()
{
  uncommit_tx_info_.reset();
  txinfo_set_.destroy();
}

} // compaction
} // oceanbase
