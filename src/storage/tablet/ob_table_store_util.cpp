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

#include "ob_table_store_util.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "observer/ob_server_struct.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "share/scn.h"


using namespace oceanbase;
using namespace share;
using namespace common;
using namespace storage;
using namespace blocksstable;

void ObSSTableArray::reset()
{
  if (OB_LIKELY(nullptr != sstable_array_)) {
    for (int64_t i = 0; i < cnt_; i++) {
      ObSSTable *sstable = sstable_array_[i];
      if (OB_LIKELY(nullptr != sstable)) {
        sstable->~ObSSTable();
        sstable = nullptr;
      }
    }
  }
  cnt_ = 0;
  sstable_array_ = nullptr;
  serialize_table_type_ = false;
  is_inited_ = false;
}

int ObSSTableArray::init(
    ObArenaAllocator &allocator,
    const ObIArray<ObITable *> &tables,
    const int64_t start_pos)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(start_pos > tables.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start pos", K(ret), K(start_pos), K(tables.count()));
  } else if (OB_FAIL(inner_init(allocator, tables, start_pos, tables.count() - start_pos))) {
    LOG_WARN("fail to init sstable array", K(ret), K(tables));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableArray::init(ObArenaAllocator &allocator, const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(!sstable->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try to init sstable array with an unserialized sstable", K(ret), KPC(sstable));
  } else {
    sstable_array_ = reinterpret_cast<ObSSTable **>(allocator.alloc(sizeof(ObSSTable *)));
    if (OB_ISNULL(sstable_array_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for sstable address array", K(ret), K_(cnt));
    } else if (OB_FAIL(sstable->deep_copy(allocator, sstable_array_[0]))) {
      LOG_WARN("fail to deep copy sstable address", K(ret), KPC(sstable));
    } else {
      cnt_ = 1;
      serialize_table_type_ = sstable->is_co_sstable();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTableArray::init(
    ObArenaAllocator &allocator,
    const ObIArray<ObITable *> &tables,
    const ObIArray<ObMetaDiskAddr> &addrs,
    const int64_t start_pos,
    const int64_t cnt)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_FAIL(inner_init(allocator, tables, start_pos, cnt))) {
    LOG_WARN("fail to inner init sstable array", K(ret), K(tables));
  } else {
    for (int64_t i = start_pos; OB_SUCC(ret) && i < start_pos + cnt; ++i) {
      ObSSTable *sstable = sstable_array_[i - start_pos];
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sstable pointer", K(ret), K(tables), K(addrs), K(start_pos), K(cnt));
      } else if (OB_FAIL(sstable->set_addr(addrs.at(i)))) {
        LOG_WARN("fail to set sstable meta disk address", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTableArray::init(ObArenaAllocator &allocator, const ObSSTableArray &other)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> tables;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_FAIL(other.get_all_tables(tables))) {
    LOG_WARN("fail to get all tables from old array", K(ret), K(other));
  } else if (OB_FAIL(inner_init(allocator, tables, 0, tables.count()))) {
    LOG_WARN("fail to inner init sstable array", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableArray::init_empty_array_for_cg(
    common::ObArenaAllocator &allocator,
    const int64_t count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(count), K(allocator));
  } else if (OB_ISNULL(sstable_array_ = static_cast<ObSSTable **>(allocator.alloc(sizeof(ObSSTable *) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for sstable address array", K(ret), K_(cnt));
  } else {
    cnt_ = count;
    MEMSET(sstable_array_, 0, sizeof(ObSSTable *) * count);
    serialize_table_type_ = false;
    is_inited_ = false;
  }
  return ret;
}

int ObSSTableArray::add_tables_for_cg(
    common::ObArenaAllocator &allocator,
    const ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 == cnt_ || NULL == sstable_array_ || is_inited_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("this table array can't add tables", K(ret), KPC(this));
  } else if (OB_UNLIKELY(cnt_ != tables.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KPC(this), K(tables.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    ObITable *table = tables.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table ptr", K(ret));
    } else if (OB_UNLIKELY(!table->is_cg_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table type", K(ret), KPC(table));
    } else if (OB_FAIL(static_cast<ObSSTable *>(table)->deep_copy(allocator, sstable_array_[i]))) {
      LOG_WARN("fail to copy sstable", K(ret), KPC(static_cast<ObSSTable *>(table)));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableArray::add_tables_for_cg_without_deep_copy(
    const ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == cnt_ || NULL == sstable_array_ || is_inited_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("this table array can't add tables", K(ret), KPC(this));
  } else if (OB_UNLIKELY(cnt_ != tables.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KPC(this), K(tables.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      ObITable *table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null table ptr", K(ret));
      } else if (OB_UNLIKELY(!table->is_cg_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table type", K(ret), KPC(table));
      } else {
        sstable_array_[i] = static_cast<ObSSTable *>(table);
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTableArray::inner_init(
    ObArenaAllocator &allocator,
    const ObIArray<ObITable *> &tables,
    const int64_t start_pos,
    const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      tables.count() < 0
      || count < 0
      || start_pos + count > tables.count()
      || start_pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments to init sstable array for serialize", K(ret),
        K(tables), K(start_pos), K(count));
  } else if (0 == count) {
    // nothing to do.
    cnt_ = 0;
    sstable_array_ = nullptr;
    serialize_table_type_ = false;
  } else {
    sstable_array_ = static_cast<ObSSTable **>(allocator.alloc(sizeof(ObSSTable *) * count));
    if (OB_ISNULL(sstable_array_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for sstable address array", K(ret), K_(cnt));
    }
    int64_t i = start_pos;
    for (; OB_SUCC(ret) && i < start_pos + count; ++i) {
      ObITable *table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null table ptr", K(ret));
      } else if (OB_UNLIKELY(!table->is_sstable() && !table->is_ddl_mem_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table type", K(ret), KPC(table));
      } else if (OB_FAIL(static_cast<ObSSTable *>(table)->deep_copy(allocator, sstable_array_[i - start_pos]))) {
        LOG_WARN("fail to copy sstable", K(ret), KPC(static_cast<ObSSTable *>(table)));
      } else if (table->is_co_sstable()) {
        serialize_table_type_ = true;
      }
    }
    if (OB_FAIL(ret)) {
      // So the current one is not rolled back, because it may not go to the deep copy or go to
      // the deep copy internal rollback.
      for (int64_t j = i - start_pos - 2; j >= 0; --j) {
        sstable_array_[j]->~ObSSTable();
        sstable_array_[j] = nullptr;
      }
      if (nullptr != sstable_array_) {
        allocator.free(sstable_array_);
        sstable_array_ = nullptr;
      }
    } else {
      cnt_ = count;
    }
  }
  return ret;
}

int64_t ObSSTableArray::get_serialize_size() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  len += serialization::encoded_length_i64(cnt_);
  len += serialization::encoded_length_bool(serialize_table_type_);
  if (cnt_ > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      ObSSTable *sstable = sstable_array_[i];
      if (OB_ISNULL(sstable) ||
          OB_UNLIKELY(sstable->is_co_sstable() && !sstable->get_addr().is_valid())) {
        len = 0;
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid sstable", K(ret), KPC(sstable));
      } else {
        if (serialize_table_type_) {
          len += serialization::encoded_length_bool(sstable->is_co_sstable());
        }
        len += sstable->get_serialize_size();
      }
    }
  }
  return len;
}

int ObSSTableArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos;

  if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, cnt_))) {
    LOG_WARN("fail to encode count", K(ret), K_(cnt));
  } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, serialize_table_type_))) {
    LOG_WARN("fail to encode is co table array flag", K(ret), K_(serialize_table_type));
  } else if (0 == cnt_) {
    // only serialize count for empty array
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      ObSSTable *sstable = sstable_array_[i];
      if (OB_ISNULL(sstable) || OB_UNLIKELY(sstable->is_co_sstable() && !sstable->get_addr().is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid sstable address", K(ret), KPC(sstable));
      } else if (serialize_table_type_ &&
            OB_FAIL(serialization::encode_bool(buf, buf_len, pos, sstable->is_co_sstable()))) {
        LOG_WARN("fail to encode is co table array flag", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(sstable->serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize sstable", K(ret), KPC(sstable));
      }
    }
  }

  return ret;
}

int ObSSTableArray::deserialize(
    ObArenaAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos,
    const bool is_compat_deserialize)
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || data_len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &cnt_))) {
    LOG_WARN("fail to decode count", K(ret));
  } else if (OB_UNLIKELY(cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized array count less than 0", K(ret), K_(cnt));
  } else if (is_compat_deserialize) {
    // no co table type for elder version
    serialize_table_type_ = false;
  } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &serialize_table_type_))) {
    LOG_WARN("fail to decode is hyper table array flag", K(ret), KP(buf), K(data_len), K(pos));
  }
  if (OB_FAIL(ret) || cnt_ <= 0) {
  } else if (OB_FAIL(inner_deserialize_tables(allocator, serialize_table_type_, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize co tables", K(ret), K(cnt_), K(data_len), K(pos), K(old_pos));
  }

  if (OB_FAIL(ret)) {
    reset();
  } else {
    is_inited_ = true;
    LOG_DEBUG("deserialize sstable array", K(ret), K(is_compat_deserialize), KPC(this));
  }
  return ret;
}

int ObSSTableArray::inner_deserialize_tables(
    ObArenaAllocator &allocator,
    const bool serialize_table_type,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t ptr_array_size = sizeof(ObSSTable *) * cnt_;
  char *tmp_buf = nullptr;

  if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator.alloc(ptr_array_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for sstable array", K(ret), K(cnt_), K(ptr_array_size));
  } else {
    MEMSET(tmp_buf, 0, ptr_array_size);
    sstable_array_ = reinterpret_cast<ObSSTable **>(tmp_buf);

    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      bool is_co_sstable = false;
      if (serialize_table_type && OB_FAIL(serialization::decode_bool(buf, data_len, pos, &is_co_sstable))) {
        LOG_WARN("fail to decode is_co_sstable flag", K(ret), K(i),KP(buf), K(data_len), K(pos));
      } else if (is_co_sstable) {
        if (OB_FAIL(deserialize_table<ObCOSSTableV2>(allocator, buf, data_len, pos, sstable_array_[i]))) {
          LOG_WARN("fail to deserialize_table", K(ret), K(i), KP(buf), K(data_len), K(pos));
        }
      } else if (OB_FAIL(deserialize_table(allocator, buf, data_len, pos, sstable_array_[i]))) {
        LOG_WARN("fail to deserialize_table", K(ret), K(i), KP(buf), K(data_len), K(pos));
      }
    }
  }
  return ret;
}

template<class T>
int ObSSTableArray::deserialize_table(
    ObArenaAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos,
    ObSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = nullptr;
  if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator.alloc(sizeof(T))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for sstable array", K(ret));
  } else {
    T *table = nullptr;
    table = new (tmp_buf) T;
    if (OB_FAIL(table->deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize sstable", K(ret));
    } else {
      sstable = table;
    }

    if (OB_FAIL(ret)) {
      table->~T();
      allocator.free(tmp_buf);
      sstable = nullptr;
    }
  }
  return ret;
}

int64_t ObSSTableArray::get_deep_copy_size() const
{
  int64_t len = 0;
  len += cnt_ * sizeof(ObSSTable *);
  for (int64_t i = 0; i < cnt_; ++i) {
    if (OB_ISNULL(sstable_array_[i])) {
      len = 0;
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected null pointer for sstable array", KPC(this), K(i), K_(cnt));
      break;
    } else {
#if __aarch64__
      len += ObSSTable::AARCH64_CP_BUF_ALIGN;
#endif
      len += sstable_array_[i]->get_deep_copy_size();
    }
  }
  return len;
}

int ObSSTableArray::deep_copy(
    char *dst_buf,
    const int64_t buf_size,
    int64_t &pos,
    ObSSTableArray &dst_array) const
{
  int ret = OB_SUCCESS;
  const int64_t memory_size = get_deep_copy_size();
  if (OB_ISNULL(dst_buf) || OB_UNLIKELY(buf_size - pos < memory_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalue argument", K(ret), KP(dst_buf), K(buf_size), K(pos), K_(cnt), K(memory_size));
  } else {
    dst_array.cnt_ = cnt_;
    dst_array.serialize_table_type_ = serialize_table_type_;
    dst_array.sstable_array_ = 0 == cnt_ ? nullptr : reinterpret_cast<ObSSTable **>(dst_buf + pos);
    const int64_t array_size = cnt_ * sizeof(ObSSTable *);
    pos += array_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      int64_t sstable_copy_size = 0;
      ObIStorageMetaObj *new_sstable = nullptr;
      char *sstable_copy_buf = dst_buf + pos;
#if __aarch64__
      sstable_copy_buf = reinterpret_cast<char *>(common::upper_align(
          reinterpret_cast<int64_t>(sstable_copy_buf), ObSSTable::AARCH64_CP_BUF_ALIGN));
      pos = reinterpret_cast<int64_t>(sstable_copy_buf) - reinterpret_cast<int64_t>(dst_buf);
#endif
      if (OB_ISNULL(sstable_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sstable pointer", K(ret), KPC(this), K(i));
      } else if (FALSE_IT(sstable_copy_size = sstable_array_[i]->get_deep_copy_size())) {
      } else if (OB_FAIL(sstable_array_[i]->deep_copy(sstable_copy_buf, buf_size - pos, new_sstable))) {
        LOG_WARN("fail to deep copy sstable addr", K(ret));
      } else {
        dst_array.sstable_array_[i] = static_cast<ObSSTable *>(new_sstable);
        pos += sstable_copy_size;
      }
    }
    if (OB_SUCC(ret)) {
      dst_array.is_inited_ = is_inited_;
    }
  }
  return ret;
}

ObSSTable *ObSSTableArray::operator[](const int64_t pos) const
{
  return at(pos);
}

blocksstable::ObSSTable *ObSSTableArray::at(const int64_t pos) const
{
  ObSSTable *sstable = nullptr;
  if (!is_valid() || pos >= cnt_ || pos < 0) {
    sstable = nullptr;
  } else {
    sstable = sstable_array_[pos];
  }
  return sstable;
}

ObITable *ObSSTableArray::get_boundary_table(const bool is_last) const
{
  ObSSTable *sstable = nullptr;
  if (!is_valid() || 0 == cnt_) {
    sstable = nullptr;
  } else {
    sstable = is_last ? sstable_array_[cnt_ - 1] : sstable_array_[0];
  }
  return sstable;
}

int ObSSTableArray::get_all_tables(ObIArray<ObITable *> &tables) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSSTable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      if (OB_ISNULL(table = sstable_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret));
      } else if (OB_FAIL(tables.push_back(table))) {
        LOG_WARN("fail to push sstable address into array", K(ret), K(i), K(tables));
      }
    }
  }
  return ret;
}

int ObSSTableArray::replace_twin_majors_and_build_new(
    const ObIArray<ObITable *> &tables_array,
    ObIArray<ObITable *> &major_tables) const
{
  int ret = OB_SUCCESS;
  ObITable* new_co_major = nullptr; // new co major to replace old row store major
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tables_array.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid major table cnt for replacing old row store", K(ret), K(tables_array));
  } else if (FALSE_IT(new_co_major = tables_array.at(0))) {
  } else if (OB_UNLIKELY(nullptr == new_co_major || !new_co_major->is_column_store_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected new co table", K(ret), KPC(new_co_major));
  } else {
    ObSSTable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      if (OB_ISNULL(table = sstable_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret));
      } else if (ObITable::is_twin_major_sstable(table->get_key(), new_co_major->get_key())) {
        // skip the old row store major
      } else if (OB_FAIL(major_tables.push_back(table))) {
        LOG_WARN("fail to push sstable address into array", K(ret), K(i), K(major_tables));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(major_tables.push_back(new_co_major))) {
      LOG_WARN("fail to push sstable address into array", K(ret), KPC(new_co_major), K(major_tables));
    }
  }
  return ret;
}

int ObSSTableArray::get_all_table_wrappers(
    ObIArray<ObSSTableWrapper> &table_wrappers,
    const bool need_unpack) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSSTable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      ObSSTableWrapper wrapper;
      if (OB_ISNULL(table = sstable_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret));
      } else if (table->is_co_sstable() && need_unpack) {
        if (OB_FAIL(static_cast<ObCOSSTableV2 *>(table)->get_all_tables(table_wrappers))) {
          LOG_WARN("failed to get all cg tables from co table", K(ret), KPC(table));
        }
      } else if (OB_FAIL(wrapper.set_sstable(table))) {
        LOG_WARN("failed to set sstable", K(ret), KPC(table));
      } else if (OB_FAIL(table_wrappers.push_back(wrapper))) {
        LOG_WARN("fail to push sstable address into array", K(ret), K(i), K(table_wrappers));
      }
    }
  }
  return ret;
}

int ObSSTableArray::get_table(
    const ObITable::TableKey &table_key,
    ObSSTableWrapper &wrapper) const
{
  int ret = OB_SUCCESS;
  wrapper.reset();

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(table_key));
  } else if (OB_UNLIKELY(!table_key.is_valid() || table_key.is_memtable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table key", K(ret), K(table_key));
  }

  ObSSTable *cur_table = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
    if (OB_ISNULL(cur_table = sstable_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sstable pointer", K(ret), KPC(this));
    } else if (table_key.is_cg_sstable()) { // should get cg table from co sstable
      ObCOSSTableV2 *co_sstable = nullptr;
      if (table_key.get_snapshot_version() != cur_table->get_snapshot_version()) {
        // do nothing
      } else if (OB_UNLIKELY(!cur_table->is_co_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should be co sstable", K(ret), K(table_key), KPC(cur_table));
      } else if (FALSE_IT(co_sstable = static_cast<ObCOSSTableV2 *>(cur_table))) {
      } else if (co_sstable->is_cgs_empty_co_table()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty co table has no cg table", K(ret), K(table_key), KPC(co_sstable), KPC(this));
        ob_abort(); // tmp debug code
      } else if (OB_FAIL(co_sstable->get_cg_sstable(table_key.get_column_group_id(), wrapper))) {
        LOG_WARN("failed to get cg table from co sstable", K(ret), K(table_key), KPC(co_sstable));
      } else {
        break;
      }
    } else if (table_key == cur_table->get_key()) {
      if (OB_FAIL(wrapper.set_sstable(cur_table))) {
        LOG_WARN("failed to set sstable", K(ret), KPC(cur_table));
      }
      break;
    }
  }
  return ret;
}

int ObSSTableArray::inc_macro_ref(bool &is_success) const
{
  int ret = OB_SUCCESS;
  is_success = false;
  bool inc_data_success = false;
  bool inc_meta_success = false;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inc_data_ref_cnt(inc_data_success))) {
    LOG_WARN("fail to increase sstables' data ref cnt", K(ret));
  } else if (OB_FAIL(inc_meta_ref_cnt(inc_meta_success))) {
    LOG_WARN("fail to increase sstables' meta ref cnt", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (inc_data_success) {
      dec_data_ref_cnt();
    }
    if (inc_meta_success) {
      dec_meta_ref_cnt();
    }
  } else {
    is_success = true;
  }
  return ret;
}

void ObSSTableArray::dec_macro_ref() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    dec_data_ref_cnt();
    dec_meta_ref_cnt();
  }
}

int ObSSTableArray::inc_meta_ref_cnt(bool &inc_success) const
{
  int ret = OB_SUCCESS;
  inc_success = false;
  ObITable *table = nullptr;
  int64_t sstable_cnt = 0;
  ObMetaDiskAddr addr;
  MacroBlockId macro_id;
  int64_t offset = 0;
  int64_t size = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
    table = sstable_array_[i];
    if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table is invalid", K(ret), KPC(table));
    } else {
      ObSSTable *sstable = reinterpret_cast<ObSSTable *>(table);
      addr = sstable->get_addr();
      if (addr.is_memory()) {
        // full/old tablet, skip increase
        sstable_cnt++;
      } else if (OB_UNLIKELY(!addr.is_block() || !addr.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("addr is invalid", K(ret), K(addr));
      } else if (OB_FAIL(addr.get_block_addr(macro_id, offset, size))) {
        LOG_WARN("fail to get macro id from addr", K(ret), K(addr));
      } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(macro_id))) {
        LOG_ERROR("fail to increase ref cnt for sstable meta's macro block", K(ret), K(macro_id));
      } else {
        sstable_cnt++;
        LOG_DEBUG("inc sstable meta's macro ref", K(ret), K(macro_id), KPC(sstable));
      }
    }
  }

  if (OB_FAIL(ret) && 0 != sstable_cnt) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < sstable_cnt; i++) {
      table = sstable_array_[i];
      if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table is invalid", K(tmp_ret), KPC(table));
      } else {
        ObSSTable *sstable = reinterpret_cast<ObSSTable *>(table);
        addr = sstable->get_addr();
        if (addr.is_memory()) {
          // full/old tablet, skip increase
        } else if (OB_UNLIKELY(!addr.is_block() || !addr.is_valid())) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("addr is invalid", K(tmp_ret), K(addr));
        } else if (OB_TMP_FAIL(addr.get_block_addr(macro_id, offset, size))) {
          LOG_ERROR("fail to get macro id from addr", K(tmp_ret), K(addr));
        } else if (OB_TMP_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to decrease ref cnt for sstable meta's macro block", K(tmp_ret), K(macro_id));
        } else {
          LOG_DEBUG("decrease sstable meta's macro ref", K(tmp_ret), K(addr), K(macro_id), KPC(sstable));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    inc_success = true;
  }

  LOG_DEBUG("the number of sstables that increase meta ref cnt", K(ret), K(sstable_cnt), K(lbt()));

  return ret;
}

int ObSSTableArray::inc_data_ref_cnt(bool &inc_success) const
{
  int ret = OB_SUCCESS;
  inc_success = false;
  ObITable *table = nullptr;
  int64_t sstable_cnt = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
    bool inc_data_block_success = false;
    table = sstable_array_[i];
    if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table is invalid", K(ret), KPC(table));
    } else {
      ObSSTable *sstable = reinterpret_cast<ObSSTable *>(table);
      if (OB_FAIL(sstable->inc_macro_ref(inc_data_block_success))) {
        LOG_WARN("fail to increase ref cnt for sstable", K(ret), KPC(sstable), K(inc_data_block_success));
      } else {
        sstable_cnt++;
      }
      LOG_DEBUG("increase sstable data macro ref", K(ret), KPC(sstable));
    }
  }

  if (OB_FAIL(ret) && 0 != sstable_cnt) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < sstable_cnt; i++) {
      table = sstable_array_[i];
      if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table is invalid", K(tmp_ret), KPC(table));
      } else {
        ObSSTable *sstable = reinterpret_cast<ObSSTable *>(table);
        sstable->dec_macro_ref();
        LOG_DEBUG("decrease sstable data macro ref", K(tmp_ret), KPC(sstable));
      }
    }
  }

  if (OB_SUCC(ret)) {
    inc_success = true;
  }

  return ret;
}

void ObSSTableArray::dec_meta_ref_cnt() const
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  ObMetaDiskAddr addr;
  MacroBlockId macro_id;
  int64_t offset = 0;
  int64_t size = 0;
  int64_t sstable_cnt = 0;

  for (int64_t i = 0; i < cnt_; i++) {
    table = sstable_array_[i];
    if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table is invalid", K(ret), KPC(table));
    } else {
      ObSSTable *sstable = reinterpret_cast<ObSSTable *>(table);
      addr = sstable->get_addr();
      if (addr.is_memory()) {
        // full/old tablet, skip decrease
      } else if (OB_UNLIKELY(!addr.is_block() || !addr.is_valid())) {
        LOG_ERROR("addr is invalid", K(ret), K(addr));
      } else if (OB_FAIL(addr.get_block_addr(macro_id, offset, size))) {
        LOG_ERROR("fail to get macro id from addr", K(ret), K(addr));
      } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(macro_id))) {
        LOG_ERROR("fail to decrease ref cnt for sstable meta's macro block", K(ret), K(macro_id));
      } else {
        sstable_cnt++;
        LOG_DEBUG("decrease sstable meta's macro ref", K(ret), K(macro_id), KPC(sstable));
      }
    }
  }

  LOG_DEBUG("the number of sstables that decrease meta ref cnt", K(ret), K(sstable_cnt), K(lbt()));
}

void ObSSTableArray::dec_data_ref_cnt() const
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  int64_t sstable_cnt = 0;

  for (int64_t i = 0; i < cnt_; i++) {
    table = sstable_array_[i];
    if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table is invalid", K(ret), KPC(table));
    } else {
      sstable_cnt++;
      ObSSTable *sstable = reinterpret_cast<ObSSTable *>(table);
      sstable->dec_macro_ref();
      LOG_DEBUG("decrease sstable data's macro ref", K(ret), KPC(sstable));
    }
  }
}

int ObMemtableArray::assign(ObMemtableArray &dst_array) const
{
  int ret = OB_SUCCESS;
  dst_array.count_ = count_;
  for (int64_t i = 0; i < MAX_MEMSTORE_CNT; ++i) {
    dst_array.memtable_array_[i] = memtable_array_[i];
  }
  return ret;
}

int ObMemtableArray::build(common::ObIArray<ObITable *> &table_array, const int64_t start_pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 != count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable array is not empry", K(ret));
  } else if (OB_UNLIKELY(start_pos < 0 || start_pos >= table_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(start_pos), K(table_array));
  }

  ObITable *table = nullptr;
  for (int64_t i = start_pos; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObIMemtable *memtable = nullptr;
    table = table_array.at(i);
    if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
    } else if (FALSE_IT(memtable = reinterpret_cast<ObIMemtable *>(table))) {
    } else if (memtable->is_empty()) {
      FLOG_INFO("empty memtable discarded", KPC(memtable));
    } else if (OB_UNLIKELY(count_ == MAX_MEMSTORE_CNT)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LOG_WARN("too many elements for memtable array", K(ret));
    } else {
      memtable_array_[count_] = memtable;
      ++count_;
    }
  }

  if (OB_FAIL(ret)) {
    count_ = 0;
  }
  return ret;
}

int ObMemtableArray::rebuild(const common::ObIArray<ObITable *> &table_array)
{
  int ret = OB_SUCCESS;

  const ObIMemtable *last_memtable = count_ > 0 ? memtable_array_[count_ - 1] : nullptr;
  const share::SCN endscn = (NULL == last_memtable) ? share::SCN::min_scn() : last_memtable->get_end_scn();

  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObIMemtable *memtable = nullptr;
    ObITable *table = table_array.at(i);
    if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
    } else if (FALSE_IT(memtable = static_cast<ObIMemtable *>(table))) {
    } else if (memtable->is_empty()) {
      FLOG_INFO("Empty memtable discarded", KPC(memtable));
    } else if (table->get_end_scn() < endscn) {
    } else if (exist_memtable_with_end_scn(table, endscn)) {
      FLOG_INFO("duplicated memtable with same end_scn discarded", KPC(table), K(endscn));
    } else if (OB_UNLIKELY(count_ == MAX_MEMSTORE_CNT)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("too many elements for memtable array", K(ret));
    } else {
      memtable_array_[count_] = memtable;
      ++count_;
    }
  }

  return ret;
}

int ObMemtableArray::rebuild(
    const share::SCN &clog_checkpoint_scn,
    const common::ObIArray<ObITable *> &table_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current memtable array is not empty", K(ret), K(clog_checkpoint_scn), KPC(this));
  } else {
    // use clog checkpoint scn to filter memtable array
    for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
      ObIMemtable *memtable = nullptr;
      ObITable *table = table_array.at(i);
      if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
      } else if (FALSE_IT(memtable = static_cast<ObIMemtable *>(table))) {
      } else if (memtable->is_empty()) {
        FLOG_INFO("Empty memtable discarded", K(ret), KPC(memtable));
      } else if (table->get_end_scn() <= clog_checkpoint_scn) {
        FLOG_INFO("memtable end scn no greater than clog checkpoint scn, should be discarded", K(ret),
            "end_scn", table->get_end_scn(), K(clog_checkpoint_scn));
      } else if (OB_UNLIKELY(count_ == MAX_MEMSTORE_CNT)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("too many elements for memtable array", K(ret));
      } else {
        memtable_array_[count_] = memtable;
        ++count_;
      }
    }
  }
  return ret;
}

int ObMemtableArray::find(const ObITable::TableKey &table_key, ObITable *&table) const
{
  int ret = OB_SUCCESS;
  table = nullptr;

  if (0 == count_) {
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
    if (OB_ISNULL(memtable_array_[i])) {
    } else if (table_key == memtable_array_[i]->get_key()) {
      table = memtable_array_[i];
      break;
    }
  }

  return ret;
}

int ObMemtableArray::find(
    const share::SCN &start_scn,
    const int64_t base_version,
    ObITable *&table,
    int64_t &mem_pos) const
{
  int ret = OB_SUCCESS;
  mem_pos = -1;
  table = nullptr;

  if (OB_UNLIKELY(0 == count_)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no memtable", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!start_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(start_scn), K(base_version));
  } else if (share::SCN::min_scn() == start_scn) {
    mem_pos = 0;
    table = memtable_array_[0];
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      ObITable *memtable = memtable_array_[i];
      if (OB_ISNULL(memtable)) {
        ret = OB_ERR_SYS;
        LOG_WARN("table must not null", K(ret), KPC(memtable), KPC(this));
      } else if (memtable->get_end_scn() == start_scn) {
        if (memtable->get_snapshot_version() > base_version) {
          mem_pos = i;
          table = memtable;
          break;
        }
      } else if (memtable->get_end_scn() > start_scn) {
        mem_pos = i;
        table = memtable;
        break;
      }
    }
  }
  return ret;
}

bool ObMemtableArray::exist_memtable_with_end_scn(const ObITable *table, const SCN &end_scn)
{
  // when frozen memtable's log was not committed, its right boundary is open (end_scn == MAX)
  // the right boundary would be refined asynchronuously
  // we need to make sure duplicate memtable was not added to tablet,
  // and ensure active memtable could be added to tablet
  bool is_exist = false;
  if (0 >= count_) {
  } else if (table->get_end_scn() == end_scn || end_scn.is_max()) {
    // Pay Attention!!!
    // The end scn of memtable can only be max or a certain value.
    for (int64_t i = count_ - 1; i >= 0 ; --i) {
      const ObITable *memtable = memtable_array_[i];
      if (memtable == table) {
        is_exist = true;
        break;
      }
    }
  }
  return is_exist;
}

int ObDDLKVArray::init(
    ObArenaAllocator &allocator,
    common::ObIArray<ObDDLKV *> &ddl_kvs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initialize twice", K(ret), KPC(this));
  } else {
    count_ = 0;
    ddl_kvs_ = nullptr;
    if (0 != ddl_kvs.count()) {
      const int64_t size = sizeof(ObDDLKV *) * ddl_kvs.count();
      if (OB_ISNULL(ddl_kvs_ = static_cast<ObDDLKV **>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate ddl kv pointer arrays", K(ret), K(size));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kvs.count(); ++i) {
          ObDDLKV *table = ddl_kvs.at(i);
          if (OB_UNLIKELY(nullptr == table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table must be ddl kv", K(ret), K(i), KPC(table));
          } else {
            ddl_kvs_[count_] = table;
            ++count_;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObDDLKVArray::deep_copy(
    char *dst_buf,
    const int64_t buf_size,
    int64_t &pos,
    ObDDLKVArray &dst) const
{
  int ret = OB_SUCCESS;
  dst.reset();
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(dst_buf) || OB_UNLIKELY(buf_size - pos < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("finvalid argument", K(ret), KP(dst_buf), K(buf_size), K(pos), K(deep_copy_size), K(count_));
  } else {
    dst.ddl_kvs_ = 0 == count_ ? nullptr : reinterpret_cast<ObDDLKV **>(dst_buf + pos);
    const int64_t array_size = count_ * sizeof(ObDDLKV *);
    pos += array_size;
    for (int64_t i = 0; i < count_; ++i) {
      dst.ddl_kvs_[i] = ddl_kvs_[i];
    }
    dst.count_ = count_;
    dst.is_inited_ = is_inited_;
  }
  return ret;
}

int64_t ObDDLKVArray::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObDDLKVArray");
    J_KV(KP(this),
        K_(count),
        "ddl_kv_ptr_array", ObArrayWrap<ObDDLKV *>(ddl_kvs_, count_));
    J_OBJ_END();
  }
  return pos;
}

/* ObTableStoreUtil Section */
bool ObTableStoreUtil::ObITableLogTsRangeCompare::operator()(
     const ObITable *ltable, const ObITable *rtable) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_SUCCESS != (result_code_ = compare_table_by_scn_range(ltable, rtable, true/*is_ascend*/, bret))) {
    LOG_WARN_RET(result_code_, "failed to compare table with LogTsRange", K(result_code_), KPC(ltable), KPC(rtable));
  }
  return bret;
}

bool ObTableStoreUtil::ObITableSnapshotVersionCompare::operator()(
     const ObITable *ltable, const ObITable *rtable) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_SUCCESS != (result_code_ = compare_table_by_snapshot_version(ltable, rtable, bret))) {
    LOG_WARN_RET(result_code_, "failed to compare table with SnapshotVersion", K(result_code_), KPC(ltable), KPC(rtable));
  }
  return bret;
}

bool ObTableStoreUtil::ObITableEndScnCompare::operator()(
     const ObITable *ltable, const ObITable *rtable) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_SUCCESS != (result_code_ = compare_table_by_end_scn(ltable, rtable, bret))) {
    LOG_WARN_RET(result_code_, "failed to compare table with end scn", K(result_code_), KPC(ltable), KPC(rtable));
  }
  return bret;
}

bool ObTableStoreUtil::ObTableHandleV2LogTsRangeCompare::operator()(
     const ObTableHandleV2 &lhandle, const ObTableHandleV2 &rhandle) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else {
    const ObITable *ltable = lhandle.get_table();
    const ObITable *rtable = rhandle.get_table();
    if (OB_SUCCESS != (result_code_ = compare_table_by_scn_range(ltable, rtable, true/*is_ascend*/, bret))) {
      LOG_WARN_RET(result_code_, "failed to compare table with LogTsRange", K(result_code_), KPC(ltable), KPC(rtable));
    }
  }
  return bret;
}

bool ObTableStoreUtil::ObTableHandleV2LogTsRangeReverseCompare::operator()(
    const ObTableHandleV2 &lhandle, const ObTableHandleV2 &rhandle) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else {
    const ObITable *ltable = lhandle.get_table();
    const ObITable *rtable = rhandle.get_table();
    if (OB_SUCCESS != (result_code_ = compare_table_by_scn_range(ltable, rtable, false/*is_ascend*/, bret))) {
      LOG_WARN_RET(result_code_, "failed to compare table with LogTsRange", K(result_code_), KPC(ltable), KPC(rtable));
    }
  }
  return bret;
}

bool ObTableStoreUtil::ObTableHandleV2SnapshotVersionCompare::operator()(
     const ObTableHandleV2 &lhandle, const ObTableHandleV2 &rhandle) const
{
  bool bret = false;

  if (OB_SUCCESS != result_code_) {
  } else {
    const ObITable *ltable = lhandle.get_table();
    const ObITable *rtable = rhandle.get_table();
    if (OB_SUCCESS != (result_code_ = compare_table_by_snapshot_version(ltable, rtable, bret))) {
      LOG_WARN_RET(result_code_, "failed to compare table with SnapshotVersion", K(result_code_), KPC(ltable), KPC(rtable));
    }
  }
  return bret;
}


int ObTableStoreUtil::compare_table_by_scn_range(const ObITable *ltable, const ObITable *rtable, const bool is_ascend, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (NULL == ltable) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left store must not null", K(ret));
  } else if (NULL == rtable) {
    bret = true;
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("right store must not null", K(ret));
  } else if (ltable->is_major_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ltable type", K(ret), KPC(ltable));
  } else if (rtable->is_major_sstable()) {
    bret = true;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid rtable type", K(ret), KPC(rtable));
  } else if (ltable->get_end_scn() == rtable->get_end_scn()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table end log ts shouldn't be equal", KPC(ltable), KPC(rtable));
  } else if (is_ascend) {
    // log ts not equal
    bret = ltable->get_end_scn() < rtable->get_end_scn();
  } else {
    bret = ltable->get_end_scn() > rtable->get_end_scn();
  }
  return ret;
}

int ObTableStoreUtil::compare_table_by_snapshot_version(const ObITable *ltable, const ObITable *rtable, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (OB_ISNULL(ltable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left store must not null", K(ret));
  } else if (OB_ISNULL(rtable)) {
    bret = true;
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("right store must not null", K(ret));
  } else if (OB_UNLIKELY(!ltable->is_major_sstable() || !rtable->is_major_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left/right store must be major", K(ret));
  } else {
    bret = ltable->get_snapshot_version() < rtable->get_snapshot_version();
    if (ltable->get_snapshot_version() == rtable->get_snapshot_version()) {
      bret = ltable->get_column_group_id() < rtable->get_column_group_id();
    }
  }
  return ret;
}

int ObTableStoreUtil::compare_table_by_end_scn(const ObITable *ltable, const ObITable *rtable, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (OB_ISNULL(ltable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left store must not null", K(ret));
  } else if (OB_ISNULL(rtable)) {
    bret = true;
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("right store must not null", K(ret));
  } else {
    bret = ltable->get_end_scn() < rtable->get_end_scn();
    if (ltable->get_end_scn() == rtable->get_end_scn()) {
      bret = ltable->get_column_group_id() < rtable->get_column_group_id();
    }
  }
  return ret;
}

int ObTableStoreUtil::sort_major_tables(ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> &tables)
{
  int ret = OB_SUCCESS;

  if (tables.empty()) {
    // no need sort
  } else {
    ObITableSnapshotVersionCompare comp(ret);
    lib::ob_sort(tables.begin(), tables.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort tables", K(ret), K(tables));
    }
  }
  return ret;
}

int ObTableStoreUtil::sort_column_store_tables(ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> &tables)
{
  int ret = OB_SUCCESS;

  if (tables.empty()) {
    // no need sort
  } else {
    ObITableEndScnCompare comp(ret);
    lib::ob_sort(tables.begin(), tables.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort tables", K(ret), K(tables));
    }
  }
  return ret;
}

int ObTableStoreUtil::sort_minor_tables(ObArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;

  if (tables.empty()) {
    // no need sort
  } else {
    // There is an assumption: either all tables are with scn range, or none
    ObITableLogTsRangeCompare comp(ret);
    lib::ob_sort(tables.begin(), tables.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort tables", K(ret), K(tables));
    }
  }
  return ret;
}

int ObTableStoreUtil::reverse_sort_minor_table_handles(ObArray<ObTableHandleV2> &table_handles)
{
  int ret = OB_SUCCESS;

  if (table_handles.empty()) {
    // no need sort
  } else {
    // There is an assumption: either all tables are with scn range, or none
    ObTableHandleV2LogTsRangeReverseCompare comp(ret);
    lib::ob_sort(table_handles.begin(), table_handles.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort tables", K(ret), K(table_handles));
    }
  }
  return ret;
}

bool ObTableStoreUtil::check_include_by_scn_range(const ObITable &a, const ObITable &b)
{
  bool bret = false;
  if (a.get_end_scn() >= b.get_end_scn() && a.get_start_scn() <= b.get_start_scn()) {
    bret = true;
  }
  return bret;
}

bool ObTableStoreUtil::check_intersect_by_scn_range(const ObITable &a, const ObITable &b)
{
  bool bret = false;
  if (!(a.get_end_scn() <= b.get_start_scn()
        || a.get_start_scn() >= b.get_end_scn())) {
    bret = true;
  }
  return bret;
}


int ObTableStoreUtil::check_has_backup_macro_block(const ObITable *table, bool &has_backup_macro)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle sst_meta_hdl;
  common::ObArray<ObSSTableWrapper> cg_sstable_array;
  has_backup_macro = false;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("table is null", K(ret));
  } else if (table->is_memtable()) {
    // memtable has no backup macro block
  } else if (OB_FAIL(static_cast<const ObSSTable *>(table)->get_meta(sst_meta_hdl))) {
    LOG_WARN("failed to get new sstable meta handle", K(ret), KPC(table));
  } else if (sst_meta_hdl.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup()) {
    has_backup_macro = true;
  } else if (!table->is_co_sstable()) {
  } else if (!static_cast<const ObCOSSTableV2 *>(table)->is_inited()) {
  } else if (OB_FAIL(static_cast<const ObCOSSTableV2 *>(table)->get_all_tables(cg_sstable_array))) {
    LOG_WARN("failed to get all cg tables from co table", K(ret), KPC(table));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_sstable_array.count(); ++i) {
      ObSSTableWrapper &sstable_wrapper = cg_sstable_array.at(i);
      ObSSTable *sstable = NULL;
      if (OB_ISNULL(sstable = sstable_wrapper.get_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
        LOG_WARN("failed to get sstable meta handle", K(ret), KPC(sstable));
      } else if (sst_meta_hdl.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup()) {
        has_backup_macro = true;
        break;
      }
    }
  }
  return ret;
}
