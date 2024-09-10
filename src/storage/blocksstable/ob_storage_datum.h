/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_BLOCKSSTABLE_STORAGE_DATUM_H
#define OB_STORAGE_BLOCKSSTABLE_STORAGE_DATUM_H

#include "common/ob_common_types.h"
#include "common/ob_tablet_id.h"
#include "share/datum/ob_datum.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/meta_mem/ob_fixed_meta_obj_array.h"
#include "storage/tx/ob_trans_define.h"
#include "common/row/ob_row.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace share{
namespace schema
{
struct ObColDesc;
}
}
namespace storage
{
struct ObStoreRow;
}
namespace blocksstable
{

//TODO optimize number buffer
struct ObStorageDatum : public common::ObDatum
{
  ObStorageDatum() { set_nop(); }
  ObStorageDatum(const ObStorageDatum &datum) { reuse(); *this = datum; }

  ~ObStorageDatum() = default;
  // ext value section
  OB_INLINE void reuse() { ptr_ = buf_; reserved_ = 0; pack_ = 0; }
  OB_INLINE void set_ext_value(const int64_t ext_value)
  { reuse(); set_ext(); no_cv(extend_obj_)->set_ext(ext_value); }
  OB_INLINE void set_nop() { set_ext_value(ObActionFlag::OP_NOP); }
  OB_INLINE void set_min() { set_ext_value(common::ObObj::MIN_OBJECT_VALUE); }
  OB_INLINE void set_max() { set_ext_value(common::ObObj::MAX_OBJECT_VALUE); }
  OB_INLINE bool is_nop_value() const { return is_nop(); } // temp solution
  // transfer section
  OB_INLINE bool is_local_buf() const { return ptr_ == buf_; }
  OB_INLINE int from_buf_enhance(const char *buf, const int64_t buf_len);
  OB_INLINE int from_obj_enhance(const common::ObObj &obj);
  OB_INLINE int to_obj_enhance(common::ObObj &obj, const common::ObObjMeta &meta) const;
  OB_INLINE int deep_copy(const ObStorageDatum &src, common::ObIAllocator &allocator);
  OB_INLINE int deep_copy(const ObStorageDatum &src, char * buf, const int64_t buf_len, int64_t &pos);
  OB_INLINE void shallow_copy_from_datum(const ObDatum &src);
  OB_INLINE int64_t get_deep_copy_size() const;
  OB_INLINE ObStorageDatum& operator=(const ObStorageDatum &other);
  OB_INLINE int64_t storage_to_string(char *buf, int64_t buf_len, const bool for_dump = false) const;
  OB_INLINE bool need_copy_for_encoding_column_with_flat_format(const ObObjDatumMapType map_type) const;
  OB_INLINE const char *to_cstring(const bool for_dump = false) const;
  //only for unittest
  OB_INLINE bool operator==(const ObStorageDatum &other) const;
  OB_INLINE bool operator==(const ObObj &other) const;

  //datum 12 byte
  int32_t reserved_;
  // buf 16 byte
  char buf_[common::OBJ_DATUM_NUMBER_RES_SIZE];
};

struct ObStorageDatumCmpFunc
{
public:
  ObStorageDatumCmpFunc(common::ObCmpFunc &cmp_func) : cmp_func_(cmp_func) {}
  ObStorageDatumCmpFunc() = default;
  ~ObStorageDatumCmpFunc() = default;
  int compare(const ObStorageDatum &left, const ObStorageDatum &right, int &cmp_ret) const;
  OB_INLINE const common::ObCmpFunc &get_cmp_func() const { return cmp_func_; }
  TO_STRING_KV(K_(cmp_func));
private:
  common::ObCmpFunc cmp_func_;
};
typedef storage::ObFixedMetaObjArray<ObStorageDatumCmpFunc> ObStoreCmpFuncs;
typedef storage::ObFixedMetaObjArray<common::ObHashFunc> ObStoreHashFuncs;
struct ObStorageDatumUtils
{
public:
  ObStorageDatumUtils();
  ~ObStorageDatumUtils();
  // init with array memory from allocator
  int init(const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const int64_t schema_rowkey_cnt,
           const bool is_oracle_mode,
           common::ObIAllocator &allocator,
           const bool is_column_store = false);
  // init with array memory on fixed size memory buffer
  int init(const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const int64_t schema_rowkey_cnt,
           const bool is_oracle_mode,
           const int64_t arr_buf_len,
           char *arr_buf);
  int assign(const ObStorageDatumUtils &other_utils, common::ObIAllocator &allocator);
  void reset();
  OB_INLINE bool is_valid() const
  {
    return is_inited_ && cmp_funcs_.count() >= rowkey_cnt_ && hash_funcs_.count() >= rowkey_cnt_;
  }
  OB_INLINE bool is_oracle_mode() const { return is_oracle_mode_; }
  OB_INLINE int64_t get_rowkey_count() const { return rowkey_cnt_; }
  OB_INLINE const ObStoreCmpFuncs &get_cmp_funcs() const { return cmp_funcs_; }
  OB_INLINE const ObStoreHashFuncs &get_hash_funcs() const { return hash_funcs_; }
  OB_INLINE const common::ObHashFunc &get_ext_hash_funcs() const { return ext_hash_func_; }
  int64_t get_deep_copy_size() const;
  TO_STRING_KV(K_(is_oracle_mode), K_(rowkey_cnt), K_(is_inited), K_(is_oracle_mode));
private:
  //TODO to be removed by @hanhui
  int transform_multi_version_col_desc(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                       const int64_t schema_rowkey_cnt,
                                       common::ObIArray<share::schema::ObColDesc> &mv_col_descs);
  int inner_init(
      const common::ObIArray<share::schema::ObColDesc> &mv_col_descs,
      const int64_t mv_rowkey_col_cnt,
      const bool is_oracle_mode);
private:
  int32_t rowkey_cnt_;  // multi version rowkey
  ObStoreCmpFuncs cmp_funcs_; // multi version rowkey cmp funcs
  ObStoreHashFuncs hash_funcs_;  // multi version rowkey cmp funcs
  common::ObHashFunc ext_hash_func_;
  bool is_oracle_mode_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageDatumUtils);
};

struct ObStorageDatumBuffer
{
public:
  ObStorageDatumBuffer(common::ObIAllocator *allocator = nullptr);
  ~ObStorageDatumBuffer();
  void reset();
  int init(common::ObIAllocator &allocator);
  int reserve(const int64_t count, const bool keep_data = false);
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE ObStorageDatum *get_datums() { return datums_; }
  OB_INLINE int64_t get_capacity() const { return capacity_; }
  TO_STRING_KV(K_(capacity), KP_(datums), KP_(local_datums));
private:
  static const int64_t LOCAL_BUFFER_ARRAY = common::OB_ROW_DEFAULT_COLUMNS_COUNT >> 1;
  int64_t capacity_;
  ObStorageDatum local_datums_[LOCAL_BUFFER_ARRAY];
  ObStorageDatum *datums_;
  common::ObIAllocator *allocator_;
  bool is_inited_;
};


OB_INLINE int ObStorageDatum::deep_copy(const ObStorageDatum &src, common::ObIAllocator &allocator)
{
  int ret = common::OB_SUCCESS;

  reuse();
  pack_ = src.pack_;
  if (is_null()) {
  } else if (src.len_ == 0) {
  } else if (src.is_local_buf()) {
    OB_ASSERT(src.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
    MEMCPY(buf_, src.ptr_, src.len_);
    ptr_ = buf_;
  } else {
    char * buf = static_cast<char *>(allocator.alloc(src.len_));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(src));
      pack_ = 0;
    } else {
      MEMCPY(buf, src.ptr_, src.len_);
      // need set ptr_ after memory copy, if this == &src
      ptr_ = buf;
    }
  }
  return ret;
}

OB_INLINE int ObStorageDatum::deep_copy(const ObStorageDatum &src, char * buf, const int64_t buf_len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;

  reuse();
  pack_ = src.pack_;
  if (is_null()) {
  } else if (src.len_ == 0) {
  } else if (src.is_local_buf()) {
    OB_ASSERT(src.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
    MEMCPY(buf_, src.ptr_, src.len_);
    ptr_ = buf_;
  } else if (OB_UNLIKELY(nullptr == buf || buf_len < pos + src.len_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy datum", K(ret), K(src), KP(buf), K(buf_len), K(pos));
    pack_ = 0;
  } else {
    MEMCPY(buf + pos, src.ptr_, src.len_);
    // need set ptr_ after memory copy, if this == &src
    ptr_ = buf + pos;
    pos += src.len_;
  }

  return ret;
}

OB_INLINE void ObStorageDatum::shallow_copy_from_datum(const ObDatum &src)
{
  if (this != &src) {
    reuse();
    pack_ = src.pack_;
    if (is_null()) {
    } else if (src.len_ == 0) {
    } else {
      ptr_ = src.ptr_;
    }
  }
}

OB_INLINE int64_t ObStorageDatum::get_deep_copy_size() const
{
  int64_t deep_copy_len = 0;
  if (is_null()) {
  } else if (is_local_buf()) {
    OB_ASSERT(len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
  } else {
    deep_copy_len = len_;
  }
  return deep_copy_len;
}

OB_INLINE int ObStorageDatum::from_buf_enhance(const char *buf, const int64_t buf_len)
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == buf || buf_len < 0 || buf_len > UINT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer from buf", K(ret), KP(buf), K(buf_len));
  } else {
    reuse();
    len_ = static_cast<uint32_t>(buf_len);
    if (buf_len > 0) {
      ptr_ = buf;
    }
  }


  return ret;
}

OB_INLINE int ObStorageDatum::from_obj_enhance(const common::ObObj &obj)
{
  int ret = common::OB_SUCCESS;

  reuse();
  if (obj.is_ext()) {
    set_ext_value(obj.get_ext());
  } else if (OB_FAIL(from_obj(obj))) {
    STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(obj));
  }
  STORAGE_LOG(DEBUG, "chaser debug from obj", K(obj), K(*this));

  return ret;
}


OB_INLINE int ObStorageDatum::to_obj_enhance(common::ObObj &obj, const common::ObObjMeta &meta) const
{
  int ret = common::OB_SUCCESS;
  if (is_outrow()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "lob should not set outrow in datum", K(ret), K(*this), K(obj), K(meta));
  } else if (is_ext()) {
    obj.set_ext(get_ext());
  } else if (OB_FAIL(to_obj(obj, meta))) {
    STORAGE_LOG(WARN, "Failed to transfer datum to obj", K(ret), K(*this), K(obj), K(meta));
  }

  return ret;
}

OB_INLINE ObStorageDatum& ObStorageDatum::operator=(const ObStorageDatum &other)
{
  if (&other != this) {
    reuse();
    pack_ = other.pack_;
    if (is_null()) {
    } else if (len_ == 0) {
    } else if (other.is_local_buf()) {
      OB_ASSERT(other.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
      MEMCPY(buf_, other.ptr_, other.len_);
      ptr_ = buf_;
    } else {
      ptr_ = other.ptr_;
    }
  }
  return *this;
}

OB_INLINE bool ObStorageDatum::operator==(const ObStorageDatum &other) const
{
  bool bret = true;
  if (is_null()) {
    bret = other.is_null();
  } else if (is_ext()) {
    bret = other.is_ext() && extend_obj_->get_ext() == other.extend_obj_->get_ext();
  } else {
    bret = ObDatum::binary_equal(*this, other);
  }
  if (!bret) {
    STORAGE_LOG(DEBUG, "datum and datum no equal", K(other), K(*this));
  }
  return bret;

}

OB_INLINE bool ObStorageDatum::operator==(const common::ObObj &other) const
{

  int ret = OB_SUCCESS;
  bool bret = true;
  ObStorageDatum datum;
  if (OB_FAIL(datum.from_obj_enhance(other))) {
    STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(other), K(datum));
  } else {
    bret = *this == datum;
  }
  if (!bret) {
    STORAGE_LOG(DEBUG, "obj and datum no equal", K(other), K(datum), KPC(this));
  }
  return bret;
}

OB_INLINE int64_t ObStorageDatum::storage_to_string(char *buf, int64_t buf_len, const bool for_dump) const
{
  int64_t pos = 0;
  if (is_ext()) {
    if (is_nop()) {
      J_NOP();
    } else if (is_max()) {
      BUF_PRINTF("MAX_OBJ");
    } else if (is_min()) {
      BUF_PRINTF("MIN_OBJ");
    }
  } else if(!for_dump) {
    pos = to_string(buf, buf_len);
  } else {
    int ret = OB_SUCCESS;
    const static int64_t STR_MAX_PRINT_LEN = 128L;
    if (null_) {
      J_NULL();
    } else {
      J_OBJ_START();
      BUF_PRINTF("len: %d, flag: %d, null: %d", len_, flag_, null_);
      if (len_ > 0) {
        OB_ASSERT(NULL != ptr_);
        const int64_t plen = std::min(static_cast<int64_t>(len_),
            static_cast<int64_t>(STR_MAX_PRINT_LEN));
        // print hex value
        BUF_PRINTF(", hex: ");
        if (OB_FAIL(hex_print(ptr_, plen, buf, buf_len, pos))) {
          // no logging in to_string function.
        } else {
          // maybe ObIntTC
          if (sizeof(int64_t) == len_) {
            BUF_PRINTF(", int: %ld", *int_);
            // maybe number with one digit
            if (1 == num_->desc_.len_) {
              BUF_PRINTF(", num_digit0: %u", num_->digits_[0]);
            }
          }
          // maybe printable C string
          int64_t idx = 0;
          while (idx < plen && isprint(ptr_[idx])) {
            idx++;
          }
          if (idx >= plen) {
            BUF_PRINTF(", cstr: %.*s", static_cast<int>(plen), ptr_);
          }
        }
      }
      J_OBJ_END();
    }
  }

  return pos;
}

OB_INLINE const char *ObStorageDatum::to_cstring(const bool for_dump) const
{
  char *buffer = NULL;
  int64_t str_len = 0;
  CStringBufMgr &mgr = CStringBufMgr::get_thread_local_instance();
  mgr.inc_level();
  const int64_t buf_len = mgr.acquire(buffer);
  if (OB_ISNULL(buffer)) {
    LIB_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "buffer is NULL");
  } else {
    str_len = storage_to_string(buffer, buf_len -1, for_dump);
    if (str_len >= 0 && str_len < buf_len) {
      buffer[str_len] = '\0';
    } else {
      buffer[0] = '\0';
    }
    mgr.update_position(str_len + 1);
  }
  mgr.try_clear_list();
  mgr.dec_level();
  return buffer;
}

OB_INLINE bool ObStorageDatum::need_copy_for_encoding_column_with_flat_format(const ObObjDatumMapType map_type) const
{
  return OBJ_DATUM_STRING == map_type && sizeof(uint64_t) == len_ && is_local_buf();
}
} // namespace blocksstable
} // namespace oceanbase
#endif
