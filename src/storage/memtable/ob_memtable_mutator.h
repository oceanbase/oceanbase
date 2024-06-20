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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_MUTATOR_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_MUTATOR_

#include "share/ob_define.h"

#include "common/rowkey/ob_rowkey.h"
#include "common/ob_tablet_id.h"
#include "common/object/ob_object.h"

#include "storage/ob_i_store.h"
#include "storage/memtable/mvcc/ob_crtp_util.h"
#include "storage/memtable/mvcc/ob_row_data.h"
#include "storage/tx/ob_clog_encrypt_info.h"
#include "storage/tablelock/ob_table_lock_common.h"

#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
};
// namespace obrpc
// {
// class ObBatchCreateTabletArg;
// class ObBatchRemoveTabletArg;
// }
namespace memtable
{

struct RedoDataNode;
struct TableLockRedoDataNode;

class ObMemtableMutatorMeta
{
  static const uint64_t MMB_MAGIC = 0x6174756d; // #muta
  static const int64_t MIN_META_SIZE = 28; // sizeof(MutatorMetaV1)
public:
  ObMemtableMutatorMeta();
  ~ObMemtableMutatorMeta();
public:
  int inc_row_count();
  int64_t get_row_count() const;
  int fill_header(const char *buf, const int64_t data_len);
  void generate_new_header();
  int set_flags(const uint8_t row_flag);
  uint8_t get_flags() const { return flags_; }
  void add_encrypt_flag();
  void remove_encrypt_flag();
  int check_data_integrity(const char *buf, const int64_t data_len);
  int64_t get_total_size() const { return meta_size_ + data_size_; }
  int64_t get_meta_size() const { return meta_size_; }
  int64_t get_data_size() const { return data_size_; }
  bool is_row_start() const;

public:
  int64_t get_serialize_size() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos);
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
public:
  int64_t to_string(char *buffer, const int64_t length) const;
private:
  bool check_magic();
  uint32_t calc_meta_crc(const char* buf);
private:
  uint32_t magic_;
  uint32_t meta_crc_;
  int16_t meta_size_;
  uint8_t version_;
  uint8_t flags_;
  uint32_t data_crc_;
  uint32_t data_size_;
  uint32_t row_count_;
  uint32_t unused_;

  DISALLOW_COPY_AND_ASSIGN(ObMemtableMutatorMeta);
};

struct ObEncryptRowBuf
{
  static const int64_t TMP_ENCRYPT_BUF_LEN = 128;//1k
  char buf_[TMP_ENCRYPT_BUF_LEN];
  char *ptr_;
  ObEncryptRowBuf();
  ~ObEncryptRowBuf();
  int alloc(const int64_t size);
};

enum class MutatorType  
{
  MUTATOR_ROW = 0,
  MUTATOR_TABLE_LOCK = 1,
  MUTATOR_ROW_EXT_INFO = 2,
};

const char * get_mutator_type_str(MutatorType mutator_type);

struct ObMutatorRowHeader
{
public:
  ObMutatorRowHeader() : mutator_type_(MutatorType::MUTATOR_ROW) {}
  void reset() { mutator_type_ = MutatorType::MUTATOR_ROW; }
  int serialize(char *buf, const int64_t serialize_size, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  // this may be equal to the length of mutator row.
  // so, MAGIC_NUM should bigger than OB_MAX_LOG_ALLOWED_SIZE.
  // OB_MAX_LOG_ALLOWED_SIZE = 1965056L;
  // 2147483647-4096=2147479551 0x7FFFEFFF
  static const int32_t MAGIC_NUM = INT32_MAX - 4096;
  MutatorType mutator_type_;
  common::ObTabletID tablet_id_;

  TO_STRING_KV("mutator_type_str_",
               get_mutator_type_str(mutator_type_),
               K(mutator_type_),
               K(tablet_id_));
};

struct ObMutator
{
public:
  ObMutator();
  ObMutator(const uint64_t table_id,
            const ObStoreRowkey &rowkey,
            const int64_t table_version,
            const transaction::ObTxSEQ seq_no);
  void reset();
public:
  char obj_array_[sizeof(common::ObObj) * common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  common::ObStoreRowkey rowkey_;
  uint32_t row_size_;
  uint64_t table_id_;
  int64_t table_version_;
  transaction::ObTxSEQ seq_no_;
};

class ObMemtableMutatorRow : public ObMutator
{
public:
  ObMemtableMutatorRow();
  ObMemtableMutatorRow(const uint64_t table_id,
                       const common::ObStoreRowkey &rowkey,
                       const int64_t table_version,
                       const ObRowData &new_row,
                       const ObRowData &old_row,
                       const blocksstable::ObDmlFlag dml_flag,
                       const uint32_t modify_count,
                       const uint32_t acc_checksum,
                       const int64_t version,
                       const int32_t flag,
                       const transaction::ObTxSEQ seq_no,
                       const int64_t column_cnt);
  virtual ~ObMemtableMutatorRow();
  void reset();
  int copy(uint64_t &table_id,
           common::ObStoreRowkey &rowkey,
           int64_t &table_version,
           ObRowData &new_row,
           ObRowData &old_row,
           blocksstable::ObDmlFlag &dml_flag,
           uint32_t &modify_count,
           uint32_t &acc_checksum,
           int64_t &version,
           int32_t &flag,
           transaction::ObTxSEQ &seq_no,
           int64_t &column_cnt) const;
  int serialize(char *buf, int64_t &buf_len, int64_t &pos,
                const transaction::ObTxEncryptMeta *encrypt_meta,
                transaction::ObCLogEncryptInfo &new_encrypt_info,
                const bool is_big_row = false);
  // the deserialize function need to be virtual function so as for
  // the extended classes to implement their own deserializaation logic
  virtual int deserialize(const char *buf, const int64_t data_len, int64_t &pos,
                          ObEncryptRowBuf &decrypt_buf,
                          const transaction::ObCLogEncryptInfo &encrypt_info,
                          const bool need_extract_encrypt_meta,
                          share::ObEncryptMeta &final_encrypt_meta,
                          share::ObCLogEncryptStatMap &encrypt_stat_map,
                          const bool is_big_row = false);
  uint32_t get_flag() const { return flag_; };
  int64_t get_column_cnt() const { return column_cnt_; }

  TO_STRING_KV(K_(row_size),
               K_(table_id),
               K_(rowkey),
               K_(table_version),
               K_(dml_flag),
               K_(update_seq),
               K_(new_row),
               K_(old_row),
               K_(acc_checksum),
               K_(version),
               K_(flag),
               K_(seq_no),
               K_(column_cnt));

public:
  blocksstable::ObDmlFlag dml_flag_;
  uint32_t update_seq_;
  ObRowData new_row_;
  ObRowData old_row_;
  uint32_t acc_checksum_;
  int64_t version_;
  int32_t flag_; // currently, unused
  uint8_t rowid_version_;
  int64_t column_cnt_;
#ifdef OB_BUILD_TDE_SECURITY
private:
  int handle_encrypt_row_(char *buf, const int64_t buf_len,
                          const int64_t start_pos, int64_t &end_pos,
                          const share::ObEncryptMeta &meta);
  int handle_decrypt_row_(const char *buf, const int64_t start_pos,
    const int64_t end_pos, ObEncryptRowBuf &row_buf, const char* &out_buf, int64_t &out_len,
    const share::ObEncryptMeta &meta);
  static const int64_t OB_MAX_COUNT_NEED_BYTES = 10; //max int64_t size need bytes
#endif
};

class ObMutatorTableLock : public ObMutator
{
public:
  ObMutatorTableLock();
  ObMutatorTableLock(const uint64_t table_id,
                     const common::ObStoreRowkey &rowkey,
                     const int64_t table_version,
                     const transaction::tablelock::ObLockID &lock_id,
                     const transaction::tablelock::ObTableLockOwnerID owner_id,
                     const transaction::tablelock::ObTableLockMode lock_mode,
                     const transaction::tablelock::ObTableLockOpType lock_op_type,
                     const transaction::ObTxSEQ seq_no,
                     const int64_t create_timestamp,
                     const int64_t create_schema_version);
  virtual ~ObMutatorTableLock();
  void reset();
  bool is_valid() const;
  int copy(transaction::tablelock::ObLockID &lock_id,
           transaction::tablelock::ObTableLockOwnerID &owner_id,
           transaction::tablelock::ObTableLockMode &lock_mode,
           transaction::tablelock::ObTableLockOpType &lock_op_type,
           transaction::ObTxSEQ &seq_no,
           int64_t &create_timestamp,
           int64_t &create_schema_version) const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos);
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);

  TO_STRING_KV(K_(row_size),
               K_(table_id),
               K_(table_version),
               K_(lock_id),
               K_(owner_id),
               K_(mode),
               K_(lock_type),
               K_(seq_no),
               K_(create_timestamp),
               K_(create_schema_version));

public:
  transaction::tablelock::ObLockID lock_id_;
  transaction::tablelock::ObTableLockOwnerID owner_id_;
  transaction::tablelock::ObTableLockMode mode_;
  transaction::tablelock::ObTableLockOpType lock_type_;
  int64_t create_timestamp_;
  int64_t create_schema_version_;
};

class ObMutatorWriter
{
public:
  ObMutatorWriter();
  ~ObMutatorWriter();
public:
  int set_buffer(char *buf, const int64_t buf_len);
  int append_table_lock_kv(
      const int64_t table_version,
      const TableLockRedoDataNode &redo);

  int append_ext_info_log_kv(
      const int64_t table_version,
      const RedoDataNode &redo,
      const bool is_big_row);

  int append_row_kv(
      const int64_t table_version,
      const RedoDataNode &redo,
      const transaction::ObTxEncryptMeta *encrypt_meta,
      transaction::ObCLogEncryptInfo &encrypt_info,
      const bool is_big_row = false);
  int append_row(
      ObMemtableMutatorRow &row,
      transaction::ObCLogEncryptInfo &encrypt_info,
      const bool is_big_row = false,
      const bool is_with_head = false);
  int append_row_buf(const char *buf, const int64_t buf_len);
  int serialize(const uint8_t row_flag, int64_t &res_len,
                transaction::ObCLogEncryptInfo &encrypt_info);
  ObMemtableMutatorMeta& get_meta() { return meta_; }
  int64_t get_serialize_size() const;
#ifdef OB_BUILD_TDE_SECURITY
  static int encrypt_big_row_data(
    const char *in_buf, const int64_t in_buf_len, int64_t &in_buf_pos,
    char *out_buf, const int64_t out_buf_len, int64_t &out_buf_pos,
    const int64_t table_id, const transaction::ObCLogEncryptInfo &encrypt_info,
    bool &need_encrypt);
#endif
private:
  ObMemtableMutatorMeta meta_;
  common::ObDataBuffer buf_;
  int64_t row_capacity_;

  DISALLOW_COPY_AND_ASSIGN(ObMutatorWriter);
};

class ObMemtableMutatorIterator
{
public:
  ObMemtableMutatorIterator();
  ~ObMemtableMutatorIterator();
  void reset();
public:
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos,
      transaction::ObCLogEncryptInfo &encrypt_info);
  bool is_iter_end() const { return buf_.get_remain() <= 0; }
  const ObMemtableMutatorMeta &get_meta() const { return meta_; }

  //4.0 new interface for replay
  int iterate_next_row(ObEncryptRowBuf &decrypt_buf,
      const transaction::ObCLogEncryptInfo &encrypt_info);
  const ObMutatorRowHeader &get_row_head();
  const ObMemtableMutatorRow &get_mutator_row();
  const ObMutatorTableLock &get_table_lock_row();
  transaction::ObTxSEQ get_row_seq_no() const { return row_seq_no_; }
  TO_STRING_KV(K_(meta), K_(row_seq_no), K(buf_.get_position()),K(buf_.get_limit()));
private:

private:
  ObMemtableMutatorMeta meta_;
  common::ObDataBuffer buf_;
  ObMutatorRowHeader row_header_;
  ObMemtableMutatorRow row_;
  ObMutatorTableLock table_lock_;
  transaction::ObTxSEQ row_seq_no_;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableMutatorIterator);
};
}//namespace memtable
}//namespace oceanbase
#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_MUTATOR_
