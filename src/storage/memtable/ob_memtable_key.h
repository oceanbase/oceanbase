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

namespace oceanbase
{
namespace memtable
{
class ObMemtableKey
{
public:
  ObMemtableKey(const common::ObStoreRowkey *rowkey)
    : rowkey_(const_cast<common::ObStoreRowkey *>(rowkey)), hash_val_(0)
  {}
  ObMemtableKey() : rowkey_(nullptr), hash_val_(0) {}
  ~ObMemtableKey() {}
  int encode(const ObMemtableKey &key)
  {
    int ret = common::OB_SUCCESS;
    rowkey_ = key.rowkey_;
    hash_val_ = key.hash_val_;
    return ret;
  }
  int encode(const common::ObStoreRowkey *rowkey)
  {
    int ret = common::OB_SUCCESS;
    rowkey_ = const_cast<common::ObStoreRowkey *>(rowkey);
    return ret;
  }
  const common::ObStoreRowkey *get_rowkey() const { return (const common::ObStoreRowkey *)rowkey_; }
  OB_INLINE void get_rowkey(const common::ObStoreRowkey *&rowkey) const { rowkey = rowkey_; }
  OB_INLINE int decode(common::ObStoreRowkey &rowkey) const
  {
    int ret = common::OB_SUCCESS;
    ret = rowkey.assign(rowkey_->get_obj_ptr(), rowkey_->get_obj_cnt());
    return ret;
  }

  inline void reset()
  {
    rowkey_ = nullptr;
    hash_val_ = 0;
  }
public:
  int compare(const ObMemtableKey &other, int &cmp) const
  {
    int ret = common::OB_SUCCESS;
    // FIXME-yangsuli: pass real column orders for comparison
    ret = rowkey_->compare(*other.rowkey_, cmp);
    return ret;
  }

  // TODO by fengshuo.fs: remove this function
  int compare(const ObMemtableKey &other) const
  {
    int ret = 0;
    // FIXME-yangsuli: pass real column orders for comparison
    ret = rowkey_->compare(*other.rowkey_);
    return ret;
  }

  int equal(const ObMemtableKey &other, bool &is_equal) const
  {
    int ret = common::OB_SUCCESS;
    if (hash() != other.hash()) {
      is_equal = false;
    } else if (OB_FAIL(rowkey_->equal(*other.rowkey_, is_equal))) {
      TRANS_LOG(ERROR, "failed to compare", KR(ret), K(rowkey_), K(*other.rowkey_));
    } else {
      // do nothing
    }
    return ret;
  }

  uint64_t hash() const { return 0 == hash_val_ ? calc_hash() : hash_val_; }

  uint64_t calc_hash() const
  {
    hash_val_ = 0;
    if (OB_NOT_NULL(rowkey_)) {
      hash_val_ = rowkey_->murmurhash(0);
    }
    return hash_val_;
  }

  int checksum(common::ObBatchChecksum &bc) const
  {
    int ret = common::OB_SUCCESS;
    rowkey_->checksum(bc);
    return ret;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (OB_NOT_NULL(rowkey_)) {
      common::databuff_printf(buf, buf_len, pos, "%s",
                              common::to_cstring(*rowkey_));
    } else {
      common::databuff_printf(buf, buf_len, pos, "NULL");
    }
    return pos;
  }
  const char *repr() const { return common::to_cstring(*this); }

  template <class Allocator>
  int dup(ObMemtableKey *&new_key, Allocator &allocator) const
  {
    return dup_without_hash(new_key, allocator);
  }

  template <class Allocator>
  int dup_without_hash(ObMemtableKey *&new_key, Allocator &allocator) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(new_key = (ObMemtableKey *)allocator.alloc(sizeof(*new_key)))
        || OB_ISNULL(new(new_key) ObMemtableKey())) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc memory for MemtableKey fail");
    } else if (OB_ISNULL(new_key->rowkey_ = (common::ObStoreRowkey *)allocator.alloc(sizeof(common::ObStoreRowkey)))
               || OB_ISNULL(new(new_key->rowkey_) common::ObStoreRowkey())) {
      allocator.free((char *)new_key);
      new_key = nullptr;
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc memory for StoreRowkey fail");
    } else if (OB_FAIL(rowkey_->deep_copy(*(new_key->rowkey_), allocator))) {
      allocator.free(new_key->rowkey_);
      new_key->rowkey_ = nullptr;
      allocator.free((char *)new_key);
      new_key = nullptr;
      TRANS_LOG(ERROR, "rowkey deep_copy fail", KR(ret), K(*this));
    } else {
      new_key->hash_val_ = hash_val_;
    }
    return ret;
  }

  int encode(const common::ObIArray<share::schema::ObColDesc> &columns,
             const common::ObStoreRowkey *rowkey)
  {
    int ret = common::OB_SUCCESS;

    if (!rowkey->is_regular()) {
      ret = common::OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trying to encode an irregular rowkey", K(rowkey), K(ret));
    } else if (rowkey->get_obj_cnt() > columns.count()) {
      TRANS_LOG(WARN, "size does not match",
                "rowkey_len", rowkey->get_obj_cnt(),
                "columns_size", columns.count());
      ret = common::OB_ERR_UNEXPECTED;
    }
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < rowkey->get_obj_cnt(); ++i) {
      common::ObObj &value = const_cast<common::ObObj &>(rowkey->get_obj_ptr()[i]);
      const common::ObObjMeta &schema_meta = columns.at(i).col_type_;
      if (common::ObNullType != value.get_type()
          && common::ObExtendType != value.get_type()
          && schema_meta.get_type() != value.get_type()
          && !(lib::is_mysql_mode()
            && (common::is_match_alter_integer_column_online_ddl_rules(schema_meta, value.get_meta())
              || common::is_match_alter_integer_column_online_ddl_rules(value.get_meta(), schema_meta))) // small integer -> big integer; mysql mode;
          && !(lib::is_oracle_mode()
            && ((common::ObNumberType == schema_meta.get_type() && common::ObNumberFloatType == value.get_type())
              || (common::ObNumberType == value.get_type() && common::ObNumberFloatType == schema_meta.get_type())))) { // number -> float; oracle mode;
        TRANS_LOG(WARN, "data/schema type does not match",
                  "index", i,
                  "data_type", value.get_type(),
                  "schema_type", schema_meta.get_type(), KP(this));
        ret = common::OB_ERR_UNEXPECTED;
      } else if ((common::ObVarcharType == schema_meta.get_type()
                  || common::ObCharType == schema_meta.get_type())
                 && common::ObNullType != value.get_type()
                 && common::ObExtendType != value.get_type()
                 && value.get_collation_type() != schema_meta.get_collation_type()) {
        TRANS_LOG(WARN, "data/schema collation_type not match", K(value.get_meta()), K(schema_meta),
                  K(value));
        //TODO: return OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      ret = encode(rowkey);
    }
    return ret;
  }

  int encode_without_hash(const common::ObIArray<share::schema::ObColDesc> &columns,
                          const common::ObStoreRowkey *rowkey)
  {
    int ret = common::OB_SUCCESS;

    if (!rowkey->is_regular()) {
       ret = common::OB_ERR_UNEXPECTED;
       TRANS_LOG(WARN, "trying to encode an irregular rowkey", K(rowkey), K(ret));
    } else if (rowkey->get_obj_cnt() > columns.count()) {
      TRANS_LOG(WARN, "size does not match",
                "rowkey_len", rowkey->get_obj_cnt(),
                "columns_size", columns.count());
      ret = common::OB_ERR_UNEXPECTED;
    }
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < rowkey->get_obj_cnt(); ++i) {
      common::ObObj &value = const_cast<common::ObObj &>(rowkey->get_obj_ptr()[i]);
      const common::ObObjMeta &schema_meta = columns.at(i).col_type_;
      if (common::ObNullType != value.get_type()
          && common::ObExtendType != value.get_type()
          && schema_meta.get_type() != value.get_type()) {
        TRANS_LOG(WARN, "data/schema type does not match",
                  "index", i,
                  "data_type", value.get_type(),
                  "schema_type", schema_meta.get_type());
        ret = common::OB_ERR_UNEXPECTED;
      } else if ((common::ObVarcharType == schema_meta.get_type()
                  || common::ObCharType == schema_meta.get_type())
                 && common::ObNullType != value.get_type()
                 && common::ObExtendType != value.get_type()
                 && value.get_collation_type() != schema_meta.get_collation_type()) {
        TRANS_LOG(WARN, "data/schema collation_type not match", K(value.get_meta()), K(schema_meta),
                  K(value));
        //TODO: return OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      rowkey_ = const_cast<common::ObStoreRowkey *>(rowkey);
    }
    return ret;
  }

  template <class Allocator>
  static int build(
      ObMemtableKey  *&new_key,
      const common::ObIArray<share::schema::ObColDesc> &columns,
      const common::ObStoreRowkey *rowkey,
      Allocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    ObMemtableKey tmp_key;
    if (OB_FAIL(tmp_key.encode(columns, rowkey))) {
      TRANS_LOG(WARN, "ObMemtableKey encode fail", "ret", ret);
    } else if (OB_FAIL(tmp_key.dup_without_hash(new_key, allocator))) {
      TRANS_LOG(WARN, "ObMemtableKey dup fail", K(ret));
    } else {
      // do nothing
    }
    return ret;
  }

  template <class Allocator>
  static int build_without_hash(
      ObMemtableKey  *&new_key,
      const common::ObIArray<share::schema::ObColDesc> &columns,
      const common::ObStoreRowkey *rowkey,
      Allocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    ObMemtableKey tmp_key;
    if (OB_FAIL(tmp_key.encode_without_hash(columns, rowkey))) {
      TRANS_LOG(WARN, "ObMemtableKey encode fail", "ret", ret);
    } else if (OB_FAIL(tmp_key.dup_without_hash(new_key, allocator))) {
      TRANS_LOG(WARN, "ObMemtableKey dup fail", K(ret));
    } else {
      // do nothing
    }
    return ret;
  }

private:
  common::ObStoreRowkey *rowkey_;
  mutable uint64_t hash_val_; // Perf optimization.
  DISALLOW_COPY_AND_ASSIGN(ObMemtableKey);
};

class ObStoreRowkeyWrapper
{
public:
  ObStoreRowkeyWrapper() : rowkey_(nullptr) {}
  ObStoreRowkeyWrapper(const common::ObStoreRowkey *rowkey) : rowkey_(rowkey) {}
  ~ObStoreRowkeyWrapper() {}

  const common::ObStoreRowkey *get_rowkey() const { return rowkey_; }
  common::ObStoreRowkey *&get_rowkey() { return (common::ObStoreRowkey *&)rowkey_; }
  void get_rowkey(const common::ObStoreRowkey *&rowkey) const { rowkey = rowkey_; }
  void reset() { rowkey_ = nullptr; }
  int compare(const ObStoreRowkeyWrapper &other, int &cmp) const { return rowkey_->compare(*(other.get_rowkey()), cmp); }
  int equal(const ObStoreRowkeyWrapper &other, bool &is_equal) const { return rowkey_->equal(*(other.get_rowkey()), is_equal); }
  uint64_t hash() const { return rowkey_->hash(); }
  int checksum(common::ObBatchChecksum &bc) const { return rowkey_->checksum(bc); }
  int64_t to_string(char *buf, const int64_t buf_len) const { return rowkey_->to_string(buf, buf_len); }
  const ObObj *get_ptr() const { return rowkey_->get_obj_ptr(); }
  const char *repr() const { return rowkey_->repr(); }
public:
  const common::ObStoreRowkey *rowkey_;
};


// this is for multi_set pre alloc memory to generate memtable key
class ObMemtableKeyGenerator {// RAII
  static constexpr int64_t STACK_BUFFER_SIZE = 32;
public:
  ObMemtableKeyGenerator() : p_extra_store_row_keys_(nullptr), p_extra_memtable_keys_(nullptr), size_(0) {}
  ~ObMemtableKeyGenerator();
  int init(const storage::ObStoreRow *rows,
           const int64_t row_count,
           const int64_t schema_rowkey_count,
           const common::ObIArray<share::schema::ObColDesc> &columns);
  void reset();
  int64_t count() const { return size_; }
  ObMemtableKey &operator[](int64_t idx);
  const ObMemtableKey &operator[](int64_t idx) const;
private:
  // this is for avoid memory allocation when rows not so much
  ObStoreRowkey store_row_key_buffer_[STACK_BUFFER_SIZE];
  ObMemtableKey memtable_key_buffer_[STACK_BUFFER_SIZE];
  ObStoreRowkey *p_extra_store_row_keys_;
  ObMemtableKey *p_extra_memtable_keys_;
  int64_t size_;
};

}
}

#endif // OCEANBASE_MEMTABLE_OB_MEMTABLE_KEY2_
