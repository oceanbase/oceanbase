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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_KEY2_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_KEY2_

#include "common/object/ob_object.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/container/ob_iarray.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase {
namespace memtable {
class ObMemtableKey {
public:
  ObMemtableKey(uint64_t table_id, const common::ObStoreRowkey* rowkey)
      : table_id_(table_id), rowkey_(const_cast<common::ObStoreRowkey*>(rowkey)), hash_val_(0)
  {
    calc_hash();
  }
  ObMemtableKey() : table_id_(0), rowkey_(nullptr), hash_val_(0)
  {}
  ~ObMemtableKey()
  {}
  int encode(const ObMemtableKey& key)
  {
    int ret = common::OB_SUCCESS;
    table_id_ = key.table_id_;
    rowkey_ = key.rowkey_;
    hash_val_ = key.hash_val_;
    return ret;
  }
  int encode(const uint64_t table_id, const common::ObStoreRowkey* rowkey)
  {
    int ret = common::OB_SUCCESS;
    table_id_ = table_id;
    rowkey_ = const_cast<common::ObStoreRowkey*>(rowkey);
    calc_hash();
    return ret;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  const common::ObStoreRowkey* get_rowkey() const
  {
    return (const common::ObStoreRowkey*)rowkey_;
  }
  OB_INLINE void get_rowkey(const common::ObStoreRowkey*& rowkey) const
  {
    rowkey = rowkey_;
  }
  OB_INLINE int decode(uint64_t& table_id, common::ObStoreRowkey& rowkey) const
  {
    int ret = common::OB_SUCCESS;
    table_id = table_id_;
    rowkey.assign(rowkey_->get_obj_ptr(), rowkey_->get_obj_cnt());
    return ret;
  }

  inline void reset()
  {
    table_id_ = 0;
    rowkey_ = nullptr;
    hash_val_ = 0;
  }
  static const ObMemtableKey& get_min_key()
  {
    static ObMemtableKey key(0, &common::ObStoreRowkey::MIN_STORE_ROWKEY);
    return key;
  }
  static const ObMemtableKey& get_max_key()
  {
    static ObMemtableKey key(UINT64_MAX, &common::ObStoreRowkey::MAX_STORE_ROWKEY);
    return key;
  }

public:
  int compare(const ObMemtableKey& other, int& cmp) const
  {
    int ret = common::OB_SUCCESS;
    if (table_id_ > other.table_id_) {
      cmp = 1;
    } else if (table_id_ < other.table_id_) {
      cmp = -1;
    } else {
      // FIXME-: pass real column orders for comparison
      ret = rowkey_->compare(*other.rowkey_, cmp);
    }
    return ret;
  }

  // TODO : remove this function
  int compare(const ObMemtableKey& other) const
  {
    int ret = 0;
    if (table_id_ > other.table_id_) {
      ret = 1;
    } else if (table_id_ < other.table_id_) {
      ret = -1;
    } else {
      // FIXME-: pass real column orders for comparison
      ret = rowkey_->compare(*other.rowkey_);
    }
    return ret;
  }

  int equal(const ObMemtableKey& other, bool& is_equal) const
  {
    int ret = common::OB_SUCCESS;
    if (hash() != other.hash()) {
      is_equal = false;
    } else if (table_id_ != other.table_id_) {
      is_equal = false;
    } else if (OB_FAIL(rowkey_->equal(*other.rowkey_, is_equal))) {
      TRANS_LOG(ERROR, "failed to compare", KR(ret), K(rowkey_), K(*other.rowkey_));
    } else {
      // do nothing
    }
    return ret;
  }

  // TODO : remove this function
  bool equal(const ObMemtableKey& other) const
  {
    bool ret = true;
    if (0 == hash_val_ || 0 == other.hash_val_) {
      ret = (table_id_ == other.table_id_) && rowkey_->simple_equal(*other.rowkey_);
    } else {
      ret = (hash_val_ == other.hash_val_);
    }
    return ret;
  }

  bool operator==(const ObMemtableKey& other) const
  {
    return equal(other);
  }

  uint64_t hash() const
  {
    return 0 == hash_val_ ? calc_hash() : hash_val_;
  }

  uint64_t calc_hash() const
  {
    hash_val_ = 0;
    if (OB_NOT_NULL(rowkey_)) {
      hash_val_ = rowkey_->murmurhash(table_id_);
    }
    return hash_val_;
  }

  int checksum(common::ObBatchChecksum& bc) const
  {
    int ret = common::OB_SUCCESS;
    bc.fill(&table_id_, sizeof(table_id_));
    rowkey_->checksum(bc);
    return ret;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (OB_NOT_NULL(rowkey_)) {
      common::databuff_printf(
          buf, buf_len, pos, "table_id=%lu rowkey_object=[%s] ", table_id_, common::to_cstring(*rowkey_));
    } else {
      common::databuff_printf(buf, buf_len, pos, "table_id=%lu rowkey_object=NULL", table_id_);
    }
    return pos;
  }
  const char* repr() const
  {
    return common::to_cstring(*this);
  }

  template <class Allocator>
  int dup(ObMemtableKey*& new_key, Allocator& allocator) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_SUCC(dup_without_hash(new_key, allocator))) {
      new_key->calc_hash();
    }
    return ret;
  }

  template <class Allocator>
  int dup_without_hash(ObMemtableKey*& new_key, Allocator& allocator) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(new_key = (ObMemtableKey*)allocator.alloc(sizeof(*new_key))) ||
        OB_ISNULL(new (new_key) ObMemtableKey())) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc memory for MemtableKey fail");
    } else if (OB_ISNULL(new_key->rowkey_ = (common::ObStoreRowkey*)allocator.alloc(sizeof(common::ObStoreRowkey))) ||
               OB_ISNULL(new (new_key->rowkey_) common::ObStoreRowkey())) {
      allocator.free((char*)new_key);
      new_key = nullptr;
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc memory for StoreRowkey fail");
    } else if (OB_FAIL(rowkey_->deep_copy(*(new_key->rowkey_), allocator))) {
      allocator.free(new_key->rowkey_);
      new_key->rowkey_ = nullptr;
      allocator.free((char*)new_key);
      new_key = nullptr;
      TRANS_LOG(ERROR, "rowkey deep_copy fail", KR(ret), K(*this));
    } else {
      new_key->table_id_ = table_id_;
      new_key->hash_val_ = hash_val_;
    }
    return ret;
  }

  int encode(const uint64_t table_id, const common::ObIArray<share::schema::ObColDesc>& columns,
      const common::ObStoreRowkey* rowkey)
  {
    int ret = common::OB_SUCCESS;

    if (!rowkey->is_regular()) {
      ret = common::OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trying to encode an irregular rowkey", K(rowkey), K(ret));
    } else if (rowkey->get_obj_cnt() > columns.count()) {
      TRANS_LOG(WARN, "size does not match", "rowkey_len", rowkey->get_obj_cnt(), "columns_size", columns.count());
      ret = common::OB_ERR_UNEXPECTED;
    }
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < rowkey->get_obj_cnt(); ++i) {
      common::ObObj& value = const_cast<common::ObObj&>(rowkey->get_obj_ptr()[i]);
      const common::ObObjMeta& schema_meta = columns.at(i).col_type_;
      if (common::ObNullType != value.get_type() && common::ObExtendType != value.get_type() &&
          schema_meta.get_type() != value.get_type()) {
        TRANS_LOG(WARN,
            "data/schema type does not match",
            "index",
            i,
            "data_type",
            value.get_type(),
            "schema_type",
            schema_meta.get_type(),
            KP(this));
        ret = common::OB_ERR_UNEXPECTED;
      } else if ((common::ObVarcharType == schema_meta.get_type() || common::ObCharType == schema_meta.get_type()) &&
                 common::ObNullType != value.get_type() && common::ObExtendType != value.get_type() &&
                 value.get_collation_type() != schema_meta.get_collation_type()) {
        TRANS_LOG(WARN, "data/schema collation_type not match", K(value.get_meta()), K(schema_meta), K(value));
        // TODO: return OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      ret = encode(table_id, rowkey);
    }
    return ret;
  }

  int encode_without_hash(const uint64_t table_id, const common::ObIArray<share::schema::ObColDesc>& columns,
      const common::ObStoreRowkey* rowkey)
  {
    int ret = common::OB_SUCCESS;

    if (!rowkey->is_regular()) {
      ret = common::OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trying to encode an irregular rowkey", K(rowkey), K(ret));
    } else if (rowkey->get_obj_cnt() > columns.count()) {
      TRANS_LOG(WARN, "size does not match", "rowkey_len", rowkey->get_obj_cnt(), "columns_size", columns.count());
      ret = common::OB_ERR_UNEXPECTED;
    }
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < rowkey->get_obj_cnt(); ++i) {
      common::ObObj& value = const_cast<common::ObObj&>(rowkey->get_obj_ptr()[i]);
      const common::ObObjMeta& schema_meta = columns.at(i).col_type_;
      if (common::ObNullType != value.get_type() && common::ObExtendType != value.get_type() &&
          schema_meta.get_type() != value.get_type()) {
        TRANS_LOG(WARN,
            "data/schema type does not match",
            "index",
            i,
            "data_type",
            value.get_type(),
            "schema_type",
            schema_meta.get_type());
        ret = common::OB_ERR_UNEXPECTED;
      } else if ((common::ObVarcharType == schema_meta.get_type() || common::ObCharType == schema_meta.get_type()) &&
                 common::ObNullType != value.get_type() && common::ObExtendType != value.get_type() &&
                 value.get_collation_type() != schema_meta.get_collation_type()) {
        TRANS_LOG(WARN, "data/schema collation_type not match", K(value.get_meta()), K(schema_meta), K(value));
        // TODO: return OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      table_id_ = table_id;
      rowkey_ = const_cast<common::ObStoreRowkey*>(rowkey);
    }
    return ret;
  }

  template <class Allocator>
  static int build(ObMemtableKey*& new_key, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const common::ObStoreRowkey* rowkey,
      Allocator& allocator)
  {
    int ret = common::OB_SUCCESS;
    ObMemtableKey tmp_key;
    if (OB_FAIL(tmp_key.encode(table_id, columns, rowkey))) {
      TRANS_LOG(WARN, "ObMemtableKey encode fail", "ret", ret);
    } else if (OB_FAIL(tmp_key.dup_without_hash(new_key, allocator))) {
      TRANS_LOG(WARN, "ObMemtableKey dup fail", K(ret));
    } else {
      // do nothing
    }
    return ret;
  }

  template <class Allocator>
  static int build_without_hash(ObMemtableKey*& new_key, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const common::ObStoreRowkey* rowkey,
      Allocator& allocator)
  {
    int ret = common::OB_SUCCESS;
    ObMemtableKey tmp_key;
    if (OB_FAIL(tmp_key.encode_without_hash(table_id, columns, rowkey))) {
      TRANS_LOG(WARN, "ObMemtableKey encode fail", "ret", ret);
    } else if (OB_FAIL(tmp_key.dup_without_hash(new_key, allocator))) {
      TRANS_LOG(WARN, "ObMemtableKey dup fail", K(ret));
    } else {
      // do nothing
    }
    return ret;
  }

protected:
  uint64_t table_id_;
  common::ObStoreRowkey* rowkey_;
  mutable uint64_t hash_val_;  // Perf optimization.
  DISALLOW_COPY_AND_ASSIGN(ObMemtableKey);
};

class ObStoreRowkeyWrapper {
public:
  ObStoreRowkeyWrapper() : rowkey_(nullptr)
  {}
  ObStoreRowkeyWrapper(const common::ObStoreRowkey* rowkey) : rowkey_(rowkey)
  {}
  ~ObStoreRowkeyWrapper()
  {}

  const common::ObStoreRowkey* get_rowkey() const
  {
    return rowkey_;
  }
  common::ObStoreRowkey*& get_rowkey()
  {
    return (common::ObStoreRowkey*&)rowkey_;
  }
  void get_rowkey(const common::ObStoreRowkey*& rowkey) const
  {
    rowkey = rowkey_;
  }
  void reset()
  {
    rowkey_ = nullptr;
  }
  int compare(const ObStoreRowkeyWrapper& other, int& cmp) const
  {
    return rowkey_->compare(*(other.get_rowkey()), cmp);
  }
  int equal(const ObStoreRowkeyWrapper& other, bool& is_equal) const
  {
    return rowkey_->equal(*(other.get_rowkey()), is_equal);
  }
  uint64_t hash() const
  {
    return rowkey_->hash();
  }
  int checksum(common::ObBatchChecksum& bc) const
  {
    return rowkey_->checksum(bc);
  }
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    return rowkey_->to_string(buf, buf_len);
  }
  const char* repr() const
  {
    return rowkey_->repr();
  }
  static const ObStoreRowkeyWrapper& get_min_key()
  {
    static ObStoreRowkeyWrapper key_wrapper(&common::ObStoreRowkey::MIN_STORE_ROWKEY);
    return key_wrapper;
  }
  static const ObStoreRowkeyWrapper& get_max_key()
  {
    static ObStoreRowkeyWrapper key_wrapper(&common::ObStoreRowkey::MAX_STORE_ROWKEY);
    return key_wrapper;
  }

public:
  const common::ObStoreRowkey* rowkey_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_OB_MEMTABLE_KEY2_
