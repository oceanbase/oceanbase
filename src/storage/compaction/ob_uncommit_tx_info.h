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
#ifndef OB_STORAGE_COMPACTION_UNCOMMIT_TX_INFO_H_
#define OB_STORAGE_COMPACTION_UNCOMMIT_TX_INFO_H_

#include <stdint.h>
#include <lib/container/ob_se_array.h>
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
class ObSSTable;
}
namespace compaction
{
struct ObUncommitTxDesc {
  OB_UNIS_VERSION(1);
public:
  enum KeyStatus: int8_t {
    SQL_SEQ = 0,
    TXID = 1,
    COMMIT_VERSION = 2,
    INVALID
  };
  ObUncommitTxDesc()
    : tx_id_(0),
      sql_seq_(0),
      key_status_(SQL_SEQ)
  {}
  ObUncommitTxDesc(const int64_t tx_id, const int64_t sql_seq, KeyStatus key_status = SQL_SEQ)
    : tx_id_(tx_id),
      sql_seq_(sql_seq),
      key_status_(key_status)
  {}
  ObUncommitTxDesc(const ObUncommitTxDesc& other)
    : tx_id_(other.tx_id_),
      sql_seq_(other.sql_seq_),
      key_status_(other.key_status_)
  {}
  ~ObUncommitTxDesc() = default;
  bool operator==(const ObUncommitTxDesc &other) const;
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&tx_id_, sizeof(tx_id_), hash_val);
    hash_val = murmurhash(&sql_seq_, sizeof(sql_seq_), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool is_valid() const {
    return tx_id_ >= 0;
  }
  void set_key_status_txid();
  bool is_commit_version() const { return key_status_ == COMMIT_VERSION; }
  bool is_txid() const { return key_status_ == TXID; }
  bool is_sql_seq() const { return key_status_ == SQL_SEQ; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
public:
  int64_t tx_id_;
  union {
    int64_t sql_seq_;
    int64_t commit_version_;
  };
  union {
    int64_t flag_;
    struct
    {
      KeyStatus key_status_  ;
      int64_t reserve_    :56;
    };
  };
};

// common base
struct ObBasicUncommitTxInfo
{
public:
  ObBasicUncommitTxInfo()
    : version_(UNCOMMIT_TX_VERSION_V1),
      info_status_(INFO_OVERFLOW),
      reserve_(0),
      total_uncommit_row_count_(0)
  {}
  virtual ~ObBasicUncommitTxInfo() = default;
  enum UncommitTxStatus: uint8_t {
    RECORD_SEQ = 0,
    ONLY_TXID = 1,
    INFO_OVERFLOW = 2,
    INFO_MAX
  };
  virtual bool is_valid() const { return is_empty() || is_info_overflow() || is_only_txid() || is_record_seq(); }
  virtual int push_back(const ObUncommitTxDesc &tx_seq_key) = 0;
  void set_info_status_overflow() { info_status_ = INFO_OVERFLOW; }
  void set_info_status_only_tx_id() { info_status_ = ONLY_TXID; }
  void set_info_status_record_seq() { info_status_ = RECORD_SEQ; }
  virtual int64_t to_string(char *buf, const int64_t buf_len, const bool is_simplified = false) const = 0;
  virtual int64_t get_info_count() const = 0;
  virtual bool is_empty() const  { return info_status_ == RECORD_SEQ && get_info_count() ==  0;}
  virtual bool is_info_overflow() const  { return info_status_ == INFO_OVERFLOW && get_info_count() ==  0;}
  virtual bool is_record_seq() const  { return info_status_ == RECORD_SEQ && get_info_count() >= 1 && get_info_count() <= MAX_TX_INFO_COUNT;}
  virtual bool is_only_txid() const  { return info_status_ == ONLY_TXID && get_info_count() >= 1 && get_info_count() <= MAX_TX_INFO_COUNT;}
  virtual int get_uncommit_tx_desc(ObUncommitTxDesc &uncommit_tx_desc, int64_t idx) const = 0;
public:
  static const int64_t MAX_TX_INFO_COUNT = 32;
  static constexpr float MINOR_DELAY_UNCOMMIT_ROW_RATIO = 0.3;
  static const int64_t UNCOMMIT_TX_VERSION_V1 = 1;
  union {
    int32_t summary_;
    struct {
      uint8_t version_     ;
      uint8_t info_status_ ;
      uint16_t reserve_    ;
    };
  };
  int64_t total_uncommit_row_count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBasicUncommitTxInfo);
};

// used on sstable meta
struct ObMetaUncommitTxInfo : public ObBasicUncommitTxInfo
{
  OB_UNIS_VERSION(1);
public:
  ObMetaUncommitTxInfo()
  : ObBasicUncommitTxInfo(),
    uncommit_tx_desc_count_(0),
    tx_infos_(nullptr)
  {}
  virtual ~ObMetaUncommitTxInfo() = default;
  int init(common::ObArenaAllocator &allocator, const ObBasicUncommitTxInfo &tmp_uncommit_info);
  virtual int push_back(const ObUncommitTxDesc &tx_seq_key) override;
  virtual int64_t to_string(char *buf, const int64_t buf_len, const bool is_simplified = false) const override;
  void reset();
  int64_t get_deep_copy_size() const;
  int deep_copy(char *buf, const int64_t buf_len, int64_t &pos, ObMetaUncommitTxInfo &dest) const;
  int deserialize(ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  virtual int64_t get_info_count() const override;
  virtual int get_uncommit_tx_desc(ObUncommitTxDesc &uncommit_tx_desc, int64_t idx) const override;
public:
  int32_t uncommit_tx_desc_count_;
  ObUncommitTxDesc *tx_infos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMetaUncommitTxInfo);
};

//Used on mini/minor and ObMigrationSSTableParam
struct ObMemUncommitTxInfo : public ObBasicUncommitTxInfo
{
  OB_UNIS_VERSION(1);
public:
  ObMemUncommitTxInfo() : ObBasicUncommitTxInfo() {}
  virtual ~ObMemUncommitTxInfo() = default;
  virtual int push_back(const ObUncommitTxDesc &tx_seq_key) override;
  virtual int64_t to_string(char *buf, const int64_t buf_len, const bool is_simplified = false) const override;
  int assign(const ObBasicUncommitTxInfo &uncommit_tx_info);
  void reuse();
  void reset();
  static bool need_collect_uncommit_tx_info(const blocksstable::ObSSTable* sstable);
  virtual int64_t get_info_count() const override;
  virtual int get_uncommit_tx_desc(ObUncommitTxDesc &uncommit_tx_desc, int64_t idx) const override;
public:
  common::ObSEArray<ObUncommitTxDesc, MAX_TX_INFO_COUNT> tx_infos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemUncommitTxInfo);
};

class ObUncommitTxInfoCollector
{
public:
  ObUncommitTxInfoCollector()
    : sql_seq_col_index_(-1),
      is_inited_(false),
      uncommit_tx_info_(),
      txinfo_set_()
  {}
  ~ObUncommitTxInfoCollector() {}
  int assign(const ObUncommitTxInfoCollector &other);
  int add_into_set(ObUncommitTxDesc &tx_seq_key);
  int push_back(const ObUncommitTxDesc &tx_seq_key);
  int push_back(const ObBasicUncommitTxInfo &other);
  int collect_uncommit_tx_info(const blocksstable::ObDatumRow &row);
  int convert_set_to_only_txid();
  int init(const int64_t sql_seq_col_index);
  void reuse();
  void reset();
public:
  int64_t sql_seq_col_index_;
  bool is_inited_;
  ObMemUncommitTxInfo uncommit_tx_info_;
  common::hash::ObHashSet<ObUncommitTxDesc> txinfo_set_;
  TO_STRING_KV(K(uncommit_tx_info_));
};

} // compaction
} // oceanbase
#endif // OB_STORAGE_COMPACTION_UNCOMMIT_TX_INFO_H_
