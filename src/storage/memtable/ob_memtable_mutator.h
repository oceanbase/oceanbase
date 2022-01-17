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
#include "common/ob_partition_key.h"
#include "common/object/ob_object.h"

#include "storage/ob_i_store.h"
#include "storage/memtable/mvcc/ob_crtp_util.h"
#include "storage/memtable/mvcc/ob_row_data.h"

namespace oceanbase {
namespace memtable {

class RedoDataNode;

class ObMemtableMutatorMeta {
  static const uint64_t MMB_MAGIC = 0x6174756d;  // #muta
  static const int64_t MIN_META_SIZE = 24;       // sizeof(MutatorMetaV1)
public:
  ObMemtableMutatorMeta();
  ~ObMemtableMutatorMeta();

public:
  int inc_row_count();
  int64_t get_row_count() const;
  int fill_header(const char* buf, const int64_t data_len);
  void generate_new_header();
  int set_flags(const uint8_t row_flag);
  uint8_t get_flags() const
  {
    return flags_;
  }
  int check_data_integrity(const char* buf, const int64_t data_len);
  int64_t get_total_size() const
  {
    return meta_size_ + data_size_;
  }
  int64_t get_meta_size() const
  {
    return meta_size_;
  }
  int64_t get_data_size() const
  {
    return data_size_;
  }
  bool is_row_start() const;

public:
  int64_t get_serialize_size() const;
  int serialize(char* buf, const int64_t buf_len, int64_t& pos);
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);

public:
  int64_t to_string(char* buffer, const int64_t length) const;

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

  DISALLOW_COPY_AND_ASSIGN(ObMemtableMutatorMeta);
};

class ObMemtableMutatorRow {
public:
  ObMemtableMutatorRow();
  ObMemtableMutatorRow(const uint64_t table_id, const common::ObStoreRowkey& rowkey, const int64_t table_version,
      const ObRowData& new_row, const ObRowData& old_row, const storage::ObRowDml dml_type, const uint32_t modify_count,
      const uint32_t acc_checksum, const int64_t version, const int32_t sql_no, const int32_t flag);
  virtual ~ObMemtableMutatorRow();
  void reset();
  int copy(uint64_t& table_id, common::ObStoreRowkey& rowkey, int64_t& table_version, ObRowData& new_row,
      ObRowData& old_row, storage::ObRowDml& dml_type, uint32_t& modify_count, uint32_t& acc_checksum, int64_t& version,
      int32_t& sql_no, int32_t& flag) const;

  int serialize(char* buf, const int64_t buf_len, int64_t& pos);
  // the deserialize function need to be virtual function so as for
  // the extended classes to implement their own deserializaation logic
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  uint64_t get_tenant_id() const
  {
    return extract_tenant_id(table_id_);
  };
  int try_replace_tenant_id(const uint64_t new_tenant_id);

  TO_STRING_KV(K_(row_size), K_(table_id), K_(rowkey), K_(table_version), K_(dml_type), K_(update_seq), K_(new_row),
      K_(old_row), K_(acc_checksum), K_(version), K_(sql_no), K_(flag));

public:
  char obj_array_[sizeof(common::ObObj) * common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  uint32_t row_size_;
  uint64_t table_id_;
  common::ObStoreRowkey rowkey_;
  int64_t table_version_;
  storage::ObRowDml dml_type_;
  uint32_t update_seq_;
  ObRowData new_row_;
  ObRowData old_row_;
  uint32_t acc_checksum_;
  int64_t version_;
  int32_t sql_no_;
  int32_t flag_;
};

class ObMemtableMutatorWriter {
public:
  ObMemtableMutatorWriter();
  ~ObMemtableMutatorWriter();

public:
  int set_buffer(char* buf, const int64_t buf_len);
  int append_kv(const uint64_t table_id, const common::ObStoreRowkey& rowkey, const int64_t table_version,
      const RedoDataNode& redo);
  int append_row(ObMemtableMutatorRow& row);
  int serialize(const uint8_t row_flag, int64_t& res_len);
  ObMemtableMutatorMeta& get_meta()
  {
    return meta_;
  }
  int64_t get_serialize_size() const;
  static int handle_big_row_data(const char* in_buf, const int64_t in_buf_len, int64_t& in_buf_pos, char* out_buf,
      const int64_t out_buf_len, int64_t& out_buf_pos);

private:
  ObMemtableMutatorMeta meta_;
  common::ObDataBuffer buf_;

  DISALLOW_COPY_AND_ASSIGN(ObMemtableMutatorWriter);
};

class ObMemtableMutatorIterator {
public:
  ObMemtableMutatorIterator();
  ~ObMemtableMutatorIterator();
  void reset();

public:
  int set_tenant_id(const uint64_t tenant_id);
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  // for replay
  int get_next_row(uint64_t& table_id, common::ObStoreRowkey& rowkey, int64_t& table_version, ObRowData& new_row,
      storage::ObRowDml& dml_type, uint32_t& modify_count, uint32_t& acc_checksum, int64_t& version, int32_t& sql_no,
      int32_t& flag);
  // for fetch_log
  int get_next_row(ObMemtableMutatorRow& row);
  bool is_iter_end() const
  {
    return buf_.get_remain() <= 0 || (big_row_ && big_row_pos_ < meta_.get_total_size());
  }
  const ObMemtableMutatorMeta& get_meta() const
  {
    return meta_;
  }
  bool is_big_row() const
  {
    return big_row_;
  }
  int undo_redo_log(const int64_t data_len);
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  };
  int try_replace_tenant_id(const uint64_t new_tenant_id);
  bool need_undo_redo_log() const
  {
    return big_row_ && need_undo_redo_log_;
  }
  TO_STRING_KV(K_(meta), K_(big_row), K_(big_row_pos));

private:
  void reset_big_row_args_();

private:
  ObMemtableMutatorMeta meta_;
  common::ObDataBuffer buf_;
  ObMemtableMutatorRow row_;
  bool big_row_;
  uint64_t tenant_id_;
  int64_t big_row_pos_;
  int64_t big_row_last_pos_;
  bool need_undo_redo_log_;

  DISALLOW_COPY_AND_ASSIGN(ObMemtableMutatorIterator);
};
}  // namespace memtable
}  // namespace oceanbase
#endif  // OCEANBASE_MEMTABLE_OB_MEMTABLE_MUTATOR_
