/**
 * Copyright (c) 2022 OceanBase
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
#define PRINT_TS(x) (ObPrintTableStore(x))

#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/memtable/ob_memtable.h"
#include "share/scn.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace oceanbase::share;
namespace storage
{

ObTabletTableStore::ObTabletTableStore()
  : version_(TABLE_STORE_VERSION_V2),
    major_tables_(),
    minor_tables_(),
    ddl_sstables_(),
    meta_major_tables_(),
    memtables_(),
    ddl_mem_sstables_(),
    memtables_lock_(),
    is_ready_for_read_(false),
    is_inited_(false)
{
#if defined(__x86_64__)
  static_assert(sizeof(ObTabletTableStore) == 320, "The size of ObTabletTableStore will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

ObTabletTableStore::~ObTabletTableStore()
{
  reset();
}

void ObTabletTableStore::reset()
{
  // all sub structs of table store will not actively release memory on reset
  major_tables_.reset();
  minor_tables_.reset();
  ddl_sstables_.reset();
  meta_major_tables_.reset();
  memtables_.reset();
  ddl_mem_sstables_.reset();
  is_ready_for_read_ = false;
  is_inited_ = false;
}

int ObTabletTableStore::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t serialized_length = get_serialize_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, version_))) {
    LOG_WARN("failed to serialize table_store_version", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, serialized_length))) {
    LOG_WARN("failed to seriazlie serialized_length", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        major_tables_,
        minor_tables_,
        ddl_sstables_,
        meta_major_tables_);
  }
  return ret;
}

int ObTabletTableStore::deserialize(
    ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int start_pos = pos;
  int64_t serialized_length = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(data_len < 0 || data_len < pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &version_))) {
    LOG_WARN("failed to deserialize table_store_version", K(ret));
  } else if (OB_UNLIKELY(version_ != TABLE_STORE_VERSION_V1 && version_ != TABLE_STORE_VERSION_V2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected deserialized version", K(ret), K(data_len), K(pos), K_(version), KPHEX(buf, data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &serialized_length))) {
    LOG_WARN("failed to deserialize serialized_length", K(ret));
  } else if (OB_FAIL(major_tables_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize major sstables", K(ret));
  } else if (OB_FAIL(minor_tables_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize minor sstables", K(ret));
  } else if (OB_FAIL(ddl_sstables_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize ddl sstables", K(ret));
  } else if (OB_FAIL(meta_major_tables_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize extend sstables", K(ret));
  } else if (OB_FAIL(build_memtable_array(tablet))) {
    LOG_WARN("fail to pull memtables from tablet", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator, tablet))) {
    LOG_WARN("pull_ddl_memtables failed", K(ret));
  } else if (OB_UNLIKELY(pos - start_pos != serialized_length)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected deserialization length not match", K(ret), K(pos), K(start_pos), K(serialized_length));
  } else {
    version_ = TABLE_STORE_VERSION_V2;
    is_inited_ = true;
    if (OB_FAIL(check_ready_for_read(tablet))) {
      LOG_WARN("fail to check if tablet is ready for read", K(ret));
    }
    if (!memtables_.empty()) {
      FLOG_INFO("succeed to deserialize table store", K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
    }
  }
  return ret;
}

int64_t ObTabletTableStore::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(version_);
  len += serialization::encoded_length_i64(len);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      major_tables_,
      minor_tables_,
      ddl_sstables_,
      meta_major_tables_);
  return len;
}

int ObTabletTableStore::init(
    ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletTableStore init twice", K(ret), K(*this));
  } else if (OB_ISNULL(sstable)) {
    // skip
  } else if (OB_FAIL(build_memtable_array(tablet))) {
    LOG_WARN("failed to build memtable array", K(ret));
  } else if (OB_UNLIKELY(!sstable->is_major_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table", K(ret), KPC(sstable));
  } else if (OB_FAIL(major_tables_.init(allocator, sstable))) {
    LOG_WARN("failed to init major tables", K(ret));
  } else {
    is_ready_for_read_ = true; // exist major sstable and no minor sstable, must be ready for read
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_cache_local_sstables(allocator))) {
      LOG_WARN("failed to cache local sstables", K(tmp_ret));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObTabletTableStore::init(
    ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !old_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(param), K(old_store));
  } else if (OB_FAIL(build_new_table_store(allocator, tablet, param, old_store))) {
    LOG_WARN("failed to build new table store with old store", K(ret), K(old_store), K(*this));
  }
  return ret;
}

int ObTabletTableStore::init(
    ObArenaAllocator &allocator,
    common::ObIArray<ObITable *> &sstable_array,
    common::ObIArray<ObMetaDiskAddr> &addr_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(sstable_array.count() != addr_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sstable array", K(ret), K(sstable_array.count()), K(addr_array.count()));
  } else {
    int64_t meta_major_start_pos = -1, major_start_pos = -1, minor_start_pos = -1, ddl_start_pos = -1;
    int64_t meta_major_cnt = 0, major_cnt = 0, minor_cnt = 0, ddl_cnt = 0;
    // traverse sstable array and mark boundary for different type of sstables
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      ObITable *table = sstable_array.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null table pointer", K(ret));
      } else if (table->is_meta_major_sstable()) {
        if (OB_UNLIKELY(0 != i || meta_major_cnt != 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("encounter a meta major sstable at not-first position in iter", K(ret), K(i));
        } else {
          meta_major_start_pos = 0;
          ++meta_major_cnt;
        }
      } else if (table->is_major_sstable()) {
        if (OB_UNLIKELY(0 != minor_cnt || 0 != ddl_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("encounter a major sstable after minor/ddl sstable is iterated",
              K(ret), K(sstable_array), K(addr_array));
        } else {
          major_start_pos = 0 == major_cnt ? i : major_start_pos;
          ++major_cnt;
        }
      } else if (table->is_minor_sstable()) {
        if (OB_UNLIKELY(0 != ddl_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("encounter a minor sstable after ddl sstables",
              K(ret), K(sstable_array), K(addr_array));
        } else {
          minor_start_pos = 0 == minor_cnt ? i : minor_start_pos;
          ++minor_cnt;
        }
      } else if (table->is_ddl_sstable()) {
        ddl_start_pos = 0 == ddl_cnt ? i : ddl_start_pos;
        ++ddl_cnt;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table type to init table store for serialize", K(ret), KPC(table));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (0 != meta_major_cnt && OB_FAIL(meta_major_tables_.init(
        allocator, sstable_array, addr_array, meta_major_start_pos, meta_major_cnt))) {
      LOG_WARN("fail to init meta major tables for serialize", K(ret), K(sstable_array), K(addr_array));
    } else if (0 != major_cnt && OB_FAIL(major_tables_.init(
        allocator, sstable_array, addr_array, major_start_pos, major_cnt))) {
      LOG_WARN("fail to init major sstables for serialize", K(ret), K(major_start_pos),
          K(major_cnt), K(sstable_array), K(addr_array));
    } else if (0 != minor_cnt && OB_FAIL(minor_tables_.init(
        allocator, sstable_array, addr_array, minor_start_pos, minor_cnt))) {
      LOG_WARN("fail to init minor sstables for serialize", K(ret), K(sstable_array), K(addr_array));
    } else if (0 != ddl_cnt && OB_FAIL(ddl_sstables_.init(
        allocator, sstable_array, addr_array, ddl_start_pos, ddl_cnt))) {
      LOG_WARN("fail to init ddl sstables for serialize", K(ret), K(sstable_array), K(addr_array));
    } else {
      is_ready_for_read_ = false; // can not read temp table store for serialize
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTabletTableStore::init(
    ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObIArray<ObITable *> &replace_sstable_array,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(!old_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table store get invalid argument", K(ret), K(tablet), K(old_store));
  } else if (OB_FAIL(inner_replace_sstables(allocator, replace_sstable_array, old_store))) {
    LOG_WARN("failed to build new table store with old store", K(ret));
  } else if (OB_FAIL(build_memtable_array(tablet))) {
    LOG_WARN("failed to pull memtable from memtable_mgr", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator, tablet))) {
    LOG_WARN("pull_ddl_memtables failed", K(ret));
  } else {
    is_inited_ = true;
    if (OB_FAIL(check_ready_for_read(tablet))) {
      LOG_WARN("failed to check ready for read", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(try_cache_local_sstables(allocator))) {
        LOG_WARN("failed to cache local sstables", K(tmp_ret));
      }
      FLOG_INFO("succeed to batch replace table store", K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
    }
  }
  return ret;
}

int ObTabletTableStore::init(
    ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(!old_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table store get invalid argument", K(ret), K(tablet), K(old_store));
  } else if (OB_FAIL(build_memtable_array(tablet))) {
    LOG_WARN("failed to build memtable array", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator, tablet))) {
    LOG_WARN("pull_ddl_memtables failed", K(ret));
  } else if (OB_FAIL(major_tables_.init(allocator, old_store.major_tables_))) {
    LOG_WARN("failed to init major tables", K(ret), K(old_store.major_tables_));
  } else if (OB_FAIL(minor_tables_.init(allocator, old_store.minor_tables_))) {
    LOG_WARN("failed to init minor tables", K(ret), K(old_store.minor_tables_));
  } else if (OB_FAIL(ddl_sstables_.init(allocator, old_store.ddl_sstables_))) {
    LOG_WARN("failed to init ddl tables", K(ret), K(old_store.ddl_sstables_));
  } else if (OB_FAIL(meta_major_tables_.init(allocator, old_store.meta_major_tables_))) {
    LOG_WARN("failede to init meta major tables", K(ret), K(old_store.meta_major_tables_));
  } else {
    is_inited_ = true;
    if (OB_FAIL(check_ready_for_read(tablet))) {
      LOG_WARN("failed to check ready for read", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(try_cache_local_sstables(allocator))) {
        LOG_WARN("failed to cache read iterator", K(tmp_ret));
      }
      FLOG_INFO("succeed to assign table store", K(ret), K(PRINT_TS(*this)));
    }
  }
  return ret;
}

int ObTabletTableStore::inner_replace_sstables(
    common::ObArenaAllocator &allocator,
    const ObIArray<ObITable *> &replace_sstable_array,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  // check table key first
  ObITable *tmp_table = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < replace_sstable_array.count(); ++i) {
    const ObITable *table = replace_sstable_array.at(i);
    if (OB_UNLIKELY(nullptr == table || !table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must be sstable", K(ret), KPC(table));
    } else if (OB_FAIL(old_store.get_table(table->get_key(), tmp_table))) {
      LOG_WARN("failed to get the same key sstable in old store", K(ret), KPC(table), K(old_store));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replace_sstables(allocator, replace_sstable_array, old_store.major_tables_, major_tables_))) {
    LOG_WARN("failed to get replaced major tables", K(ret));
  } else if (OB_FAIL(replace_sstables(allocator, replace_sstable_array, old_store.minor_tables_, minor_tables_))) {
    LOG_WARN("failed to get replaced minor tables", K(ret));
  } else if (OB_FAIL(replace_sstables(allocator, replace_sstable_array, old_store.ddl_sstables_, ddl_sstables_))) {
    LOG_WARN("failed to get replaced ddl tables", K(ret));
  } else if (OB_FAIL(meta_major_tables_.init(allocator, old_store.meta_major_tables_))) {
    LOG_WARN("failed to init meta major tables for new table store", K(ret));
  }
  return ret;
}

int ObTabletTableStore::replace_sstables(
    common::ObArenaAllocator &allocator,
    const ObIArray<ObITable *> &replace_sstable_array,
    const ObSSTableArray &old_tables,
    ObSSTableArray &new_tables) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> replaced_tables;
  if (OB_FAIL(old_tables.get_all_tables(replaced_tables))) {
    LOG_WARN("failed to get all table from old tables", K(ret), K(old_tables));
  }
  ObITable *table = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < replaced_tables.count(); ++idx) {
    if (OB_ISNULL(table = replaced_tables.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(replaced_tables));
    }

    for (int64_t pos = 0; OB_SUCC(ret) && pos < replace_sstable_array.count(); ++pos) {
      if (table->get_key() == replace_sstable_array.at(pos)->get_key()) {
        replaced_tables.at(idx) = replace_sstable_array.at(pos);
        break;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!replaced_tables.empty() && OB_FAIL(new_tables.init(allocator, replaced_tables))) {
    LOG_WARN("failed to init sstable array", K(ret), K(new_tables), K(replaced_tables));
  }
  return ret;
}

int64_t ObTabletTableStore::get_deep_copy_size() const
{
  return (sizeof(ObTabletTableStore)
        + major_tables_.get_deep_copy_size()
        + minor_tables_.get_deep_copy_size()
        + ddl_mem_sstables_.get_deep_copy_size()
        + ddl_sstables_.get_deep_copy_size()
        + meta_major_tables_.get_deep_copy_size());
}

int ObTabletTableStore::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIStorageMetaObj *&value) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < get_deep_copy_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    ObTabletTableStore *new_table_store = new (buf) ObTabletTableStore();
    pos = sizeof(ObTabletTableStore);
    if (OB_FAIL(major_tables_.deep_copy(buf, buf_len, pos, new_table_store->major_tables_))) {
      LOG_WARN("fail to deep copy major sstables", K(ret));
    } else if (OB_FAIL(minor_tables_.deep_copy(buf, buf_len, pos, new_table_store->minor_tables_))) {
      LOG_WARN("fail to deep copy minor sstables", K(ret));
    } else if (OB_FAIL(ddl_mem_sstables_.deep_copy(buf, buf_len, pos, new_table_store->ddl_mem_sstables_))) {
      LOG_WARN("fail to deep copy ddl mem sstables", K(ret));
    } else if (OB_FAIL(ddl_sstables_.deep_copy(buf, buf_len, pos, new_table_store->ddl_sstables_))) {
      LOG_WARN("fail to deep copy ddl sstables", K(ret));
    } else if (OB_FAIL(meta_major_tables_.deep_copy(buf, buf_len, pos, new_table_store->meta_major_tables_))) {
      LOG_WARN("fail to deep copy meta major sstables", K(ret));
    } else if (OB_FAIL(memtables_.assign(new_table_store->memtables_))) {
      LOG_WARN("fail to assign memtable pointers to new table store", K(ret));
    } else {
      new_table_store->version_ = version_;
      new_table_store->is_inited_ = is_inited_;
      new_table_store->is_ready_for_read_ = is_ready_for_read_;
      value = new_table_store;
      LOG_DEBUG("succeed to deep_copy table store", K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
    }
  }
  return ret;
}

int ObTabletTableStore::get_local_sstable_size_limit(
    const int64_t table_store_mem_ctx_size,
    int64_t &local_sstable_size_limit) const
{
  int ret = OB_SUCCESS;
  const int64_t table_store_object_size = get_deep_copy_size();
  if (OB_UNLIKELY(table_store_mem_ctx_size < table_store_object_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store memory context size", K(ret), K(table_store_mem_ctx_size));
  } else {
    local_sstable_size_limit = table_store_mem_ctx_size - table_store_object_size;
  }
  return ret;
}

int ObTabletTableStore::try_cache_local_sstables(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t local_sstable_size_limit = 0;
  int64_t local_sstable_meta_size = 0;
  if (OB_FAIL(get_local_sstable_size_limit(MAX_TABLE_STORE_MEMORY_SIZE, local_sstable_size_limit))) {
    LOG_WARN("failed to get local sstable size limit", K(ret));
  } else if (OB_FAIL(try_cache_local_sstable_meta(
      allocator, meta_major_tables_, local_sstable_size_limit, local_sstable_meta_size))) {
    if (OB_UNLIKELY(ret != OB_BUF_NOT_ENOUGH)) {
      LOG_WARN("fail to cache local meta major sstable meta", K(ret));
    }
  } else if (OB_FAIL(try_cache_local_sstable_meta(
      allocator, major_tables_, local_sstable_size_limit, local_sstable_meta_size))) {
    if (OB_UNLIKELY(ret != OB_BUF_NOT_ENOUGH)) {
      LOG_WARN("fail to cache local major sstable meta", K(ret));
    }
  } else if (OB_FAIL(try_cache_local_sstable_meta(
      allocator, minor_tables_, local_sstable_size_limit, local_sstable_meta_size))) {
    if (OB_UNLIKELY(ret != OB_BUF_NOT_ENOUGH)) {
      LOG_WARN("fail to cache local minor sstable meta", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::cache_local_sstable_meta(
    ObArenaAllocator &allocator,
    blocksstable::ObSSTable *array_sstable,
    const blocksstable::ObSSTable *loaded_sstable,
    const int64_t local_sstable_size_limit,
    int64_t &local_sstable_meta_size)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle sst_meta_hdl;
  if (OB_ISNULL(array_sstable) || OB_ISNULL(loaded_sstable)
      || OB_UNLIKELY(!loaded_sstable->is_loaded())
      || OB_UNLIKELY(array_sstable->get_key() != loaded_sstable->get_key())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(array_sstable), KPC(loaded_sstable),
        K(local_sstable_size_limit), K(local_sstable_meta_size));
  } else if (local_sstable_meta_size >= local_sstable_size_limit) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (array_sstable->is_loaded()) {
    // sstable in table store array already loaded
    if (OB_FAIL(array_sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("fail to get sstable meta", K(ret));
    } else {
      local_sstable_meta_size += sst_meta_hdl.get_sstable_meta().get_deep_copy_size();
    }
  } else if (OB_FAIL(loaded_sstable->get_meta(sst_meta_hdl))) {
    LOG_WARN("fail to get sstable meta", K(ret));
  } else {
    const int64_t deep_copy_size = sst_meta_hdl.get_sstable_meta().get_deep_copy_size();
    ObSSTableMeta *copied_sstable_meta = nullptr;
    int64_t pos = 0;
    if (local_sstable_meta_size + deep_copy_size <= local_sstable_size_limit) {
      // cache local sstable meta
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(deep_copy_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for sstable", K(ret));
      } else if (OB_FAIL(sst_meta_hdl.get_sstable_meta().deep_copy(buf, deep_copy_size, pos, copied_sstable_meta))) {
        LOG_WARN("fail to copy read cache to local sstable meta array", K(ret));
      } else if (OB_FAIL(array_sstable->assign_meta(copied_sstable_meta))) {
        LOG_WARN("fail to assign cached sstable meta to sstable", K(ret), KP(array_sstable));
      } else {
        local_sstable_meta_size += deep_copy_size;
      }
    } else {
      // size of sstables exceed limit
      ret = OB_BUF_NOT_ENOUGH;
    }
  }
  return ret;
}

int ObTabletTableStore::try_cache_local_sstable_meta(
    ObArenaAllocator &allocator,
    ObSSTableArray &sstable_array,
    const int64_t local_sstable_size_limit,
    int64_t &local_sstable_meta_size)
{
  // TODO: now load sstable synchronously, async it
  int ret = OB_SUCCESS;
  if (local_sstable_meta_size >= local_sstable_size_limit) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ObStorageMetaHandle sstable_handle;
    // try to cache major sstables
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      sstable_handle.reset();
      ObSSTable *loaded_sstable = nullptr;
      ObSSTable *array_sstable = sstable_array[i];
      if (array_sstable->is_loaded()) {
        // sstable is already loaded to memory
      } else if (OB_FAIL(load_sstable(array_sstable->get_addr(), sstable_handle))) {
        LOG_WARN("fail to load sstable", K(ret), KPC(array_sstable));
      } else if (OB_FAIL(sstable_handle.get_sstable(loaded_sstable))) {
        LOG_WARN("fail to get sstable value", K(ret), K(sstable_handle));
      } else if (OB_FAIL(cache_local_sstable_meta(
          allocator,
          array_sstable,
          loaded_sstable,
          local_sstable_size_limit,
          local_sstable_meta_size))) {
        if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
          LOG_WARN("fail to cache local sstable meta", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTabletTableStore::inc_macro_ref() const
{
  int ret = OB_SUCCESS;
  bool major_success = false;
  bool meta_major_success = false;
  bool minor_success = false;
  bool ddl_success = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet store hasn't been inited", K(ret));
  } else if (!major_tables_.empty()
      && OB_FAIL(major_tables_.inc_macro_ref(major_success))) {
    LOG_WARN("fail to increase major sstables' ref cnt", K(ret), K(major_tables_));
  } else if (!meta_major_tables_.empty()
      && OB_FAIL(meta_major_tables_.inc_macro_ref(meta_major_success))) {
    LOG_WARN("fail to increase meta major sstables' ref cnt", K(ret), K(meta_major_tables_));
  } else if (!minor_tables_.empty()
      && OB_FAIL(minor_tables_.inc_macro_ref(minor_success))) {
    LOG_WARN("fail to increase minor sstables' ref cnt", K(ret), K(minor_tables_));
  } else if (!ddl_sstables_.empty()
      && OB_FAIL(ddl_sstables_.inc_macro_ref(ddl_success))) {
    LOG_WARN("fail to increase ddl sstables' ref cnt", K(ret), K(ddl_sstables_));
  }

  if (OB_FAIL(ret)) {
    if (major_success) {
      major_tables_.dec_macro_ref();
    }
    if (meta_major_success) {
      meta_major_tables_.dec_macro_ref();
    }
    if (minor_success) {
      minor_tables_.dec_macro_ref();
    }
    if (ddl_success) {
      ddl_sstables_.dec_macro_ref();
    }
  }

  return ret;
}

void ObTabletTableStore::dec_macro_ref() const
{
  if (!major_tables_.empty()) {
    major_tables_.dec_macro_ref();
  }
  if (!meta_major_tables_.empty()) {
    meta_major_tables_.dec_macro_ref();
  }
  if (!minor_tables_.empty()) {
    minor_tables_.dec_macro_ref();
  }
  if (!ddl_sstables_.empty()) {
    ddl_sstables_.dec_macro_ref();
  }
}

bool ObTabletTableStore::check_read_tables(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    const ObSSTable *base_table) const
{
  bool contain_snapshot_version = false;
  if (OB_ISNULL(base_table)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "null base table, must not contain snapshot_version", KP(base_table));
  } else {
    if (base_table->is_major_sstable()) {
      contain_snapshot_version = base_table->get_snapshot_version() == snapshot_version;
    }
    if (!contain_snapshot_version && snapshot_version >= tablet.get_multi_version_start()) {
      contain_snapshot_version = true;
    }
  }
  if (!contain_snapshot_version) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "table store has no contain snapshot version",
        K(snapshot_version), K(tablet), KPC(base_table), KPC(this));
  }
  return contain_snapshot_version;
}

int ObTabletTableStore::calculate_read_tables(
    const int64_t snapshot_version,
    const ObTablet &tablet,
    ObTableStoreIterator &iterator,
    const bool allow_no_ready_read) const
{
  int ret = OB_SUCCESS;
  const ObSSTable *base_table = nullptr;

  if (!meta_major_tables_.empty() && meta_major_tables_.at(0)->get_snapshot_version() < snapshot_version) {
    base_table = meta_major_tables_.at(0);
    if (OB_FAIL(iterator.add_table(meta_major_tables_.at(0)))) {
      LOG_WARN("failed to add meta major table to iterator", K(ret), K(meta_major_tables_));
    }
  } else if (!is_major_sstable_empty(tablet)) {
    if (!major_tables_.empty()) {
      for (int64_t i = major_tables_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        if (major_tables_[i]->get_snapshot_version() <= snapshot_version) {
          base_table = major_tables_[i];
          if (OB_FAIL(iterator.add_table(major_tables_[i]))) {
            LOG_WARN("failed to add major table to iterator", K(ret));
          }
          break;
        }
      }
    } else {
      ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> ddl_major_sstables;
      if (OB_FAIL(get_ddl_major_sstables(ddl_major_sstables))) {
        LOG_WARN("get ddl major sstable failed", K(ret));
      } else if (OB_UNLIKELY(ddl_major_sstables.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl major sstable is empty", K(ret));
      } else {
        ObSSTable *first_ddl_sstable = static_cast<ObSSTable *>(ddl_major_sstables.at(0));
        if (first_ddl_sstable->get_data_version() <= snapshot_version) {
          for (int64_t i = 0; OB_SUCC(ret) && i < ddl_major_sstables.count(); ++i) {
            if (OB_FAIL(iterator.add_table(ddl_major_sstables.at(i)))) {
              LOG_WARN("add ddl major sstable failed", K(ret), K(i), K(ddl_major_sstables));
            }
          }
          if (OB_SUCC(ret)) {
            base_table = first_ddl_sstable;
          }
        } else {
          LOG_DEBUG("the snapshot_version of ddl major sstable is not match",
              "ddl_major_sstable_version", first_ddl_sstable->get_data_version(), K(snapshot_version));
        }
      }
    }
  } else { // no major table, not ready for reading
    LOG_INFO("no base tables in table store, no ready for reading", K(ret), K(snapshot_version), K(*this));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(base_table)) { // find read minor tables
    const ObSSTable *table = nullptr;
    int64_t inc_pos = -1;
    // TODO@wenqu: better abstract to calculate read tables in ddl path
    SCN last_scn = base_table->is_major_sstable() || base_table->is_ddl_sstable() ? SCN::max_scn() : base_table->get_end_scn();
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables_.count(); ++i) {
      table = minor_tables_[i];
      if (((base_table->is_major_sstable() || base_table->is_ddl_sstable()) && table->get_upper_trans_version() >= base_table->get_data_version())
          || table->get_end_scn() >= last_scn) {
        inc_pos = i;
        break;
      }
    }
    if (OB_SUCC(ret) && inc_pos >= 0
        && OB_FAIL(iterator.add_tables(minor_tables_, inc_pos, minor_tables_.count() - inc_pos))) {
      LOG_WARN("failed add table to iterator", K(ret));
    } else { // try to add memtables for reading
      if (OB_FAIL(calculate_read_memtables(tablet, iterator))) {
        LOG_WARN("failed to calculate read memtables", K(ret), K(snapshot_version), K(iterator), KPC(this));
      }
    }
    if (OB_SUCC(ret) && !check_read_tables(tablet, snapshot_version, base_table)) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("exist base table, but no read table found for specific version",
          K(ret), K(snapshot_version), K(iterator), K(PRINT_TS(*this)));
    }
  } else { // not find base table
    if (!allow_no_ready_read) {
      if (is_major_sstable_empty(tablet)) {
        ret = OB_REPLICA_NOT_READABLE;
        LOG_WARN("no base table, not allow no ready read, tablet is not readable",
                 K(ret), K(snapshot_version), K(allow_no_ready_read), K(PRINT_TS(*this)));
      } else {
        ret = OB_SNAPSHOT_DISCARDED;
        LOG_WARN("no base table found for specific version",
                 K(ret), K(snapshot_version), K(allow_no_ready_read), K(PRINT_TS(*this)));
      }
    } else if (!minor_tables_.empty() && OB_FAIL(iterator.add_tables(
        minor_tables_, 0, minor_tables_.count()))) {
      LOG_WARN("failed to add all minor tables to iterator", K(ret));
    } else {
      if (OB_FAIL(calculate_read_memtables(tablet, iterator))) {
        LOG_WARN("no base table, but allow no ready read, failed to calculate read memtables",
                 K(ret), K(snapshot_version), K(memtables_), K(PRINT_TS(*this)));
      }
    }
  }

  return ret;
}

int ObTabletTableStore::calculate_read_memtables(
    const ObTablet &tablet,
    ObTableStoreIterator &iterator) const
{
  int ret = OB_SUCCESS;
  int64_t start_snapshot_version = tablet.get_snapshot_version();
  share::SCN start_scn = tablet.get_clog_checkpoint_scn();
  int64_t mem_pos = -1;
  ObITable *memtable = nullptr;

  if (memtables_.empty()) {
  } else if (OB_FAIL(memtables_.find(start_scn, start_snapshot_version, memtable, mem_pos))) {
    LOG_WARN("failed to find memtable", K(ret), K(*this));
  } else if (-1 == mem_pos) {
  } else if (OB_FAIL(iterator.add_tables(memtables_, mem_pos))) {
    LOG_WARN("failed to add memtable to iterator", K(ret));
  }
  return ret;
}

int ObTabletTableStore::get_table(
    const ObStorageMetaHandle &table_store_handle,
    const ObITable::TableKey &table_key,
    ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  handle.reset();
  ObITable *table = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is unexpected invalid", K(ret), KPC(this));
  } else if (OB_FAIL(get_table(table_key, table))) {
    LOG_WARN("fail to get table pointer", K(ret));
  } else if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("found null table pointer", K(ret), K(table_key));
  } else if (table->is_memtable()) {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
    if (OB_FAIL(handle.set_table(table, t3m, table->get_key().table_type_))) {
      LOG_WARN("Failed to set memtable to handle", K(ret), K(handle), K(table_key), KPC(table));
    }
  } else if (static_cast<ObSSTable *>(table)->is_loaded()) {
    if (!table_store_handle.is_valid()) {
      // table store object on tablet meta memory
      if (OB_FAIL(handle.set_sstable_with_tablet(table))) {
        LOG_WARN("failed to set sstable to handle", K(ret));
      }
    } else if (OB_FAIL(handle.set_sstable(table, table_store_handle))) {
      LOG_WARN("failed to set sstable to handle", K(ret));
    }
  } else {
    ObStorageMetaHandle sst_handle;
    ObSSTable *sstable = nullptr;
    if (OB_FAIL(load_sstable(
        static_cast<ObSSTable *>(table)->get_addr(), sst_handle))) {
      LOG_WARN("fail to load sstable", K(ret), KPC(table));
    } else if (OB_FAIL(sst_handle.get_sstable(sstable))) {
      LOG_WARN("fail to get loaded sstable", K(ret));
    } else if (OB_FAIL(handle.set_sstable(sstable, sst_handle))) {
      LOG_WARN("fail to set sstable to handle", K(ret), KPC(sstable), K(handle));
    }
  }
  return ret;
}

int ObTabletTableStore::get_table(const ObITable::TableKey &table_key, ObITable *&table) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key));
  } else {
    table = nullptr;
    const ObSSTableArray *sst_array = nullptr;
    if (table_key.is_major_sstable()) {
      sst_array = &major_tables_;
    } else if (table_key.is_minor_sstable()) {
      sst_array = &minor_tables_;
    } else if (table_key.is_ddl_sstable()) {
      sst_array = &ddl_sstables_;
    } else if (table_key.is_meta_major_sstable()) {
      sst_array = &meta_major_tables_;
    }

    if (table_key.is_memtable()) {
      common::SpinRLockGuard guard(memtables_lock_);
      if (OB_FAIL(memtables_.find(table_key, table))) {
        LOG_WARN("fail to get memtable", K(ret), K(table_key), K_(memtables));
      }
    } else if (OB_ISNULL(sst_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sstable array", K(ret), K(table_key));
    } else if (sst_array->empty()) {
      // not found
    } else if (OB_FAIL(sst_array->get_table(table_key, table))) {
      LOG_WARN("fail to get table from sstable array", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(table)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("table not found", K(ret), K(table_key));
    }
  }
  return ret;
}

int ObTabletTableStore::get_read_tables(
    const int64_t snapshot_version,
    const ObTablet &tablet,
    ObTableStoreIterator &iterator,
    const bool allow_no_ready_read) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(memtables_lock_);
  if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(is_major_sstable_empty(tablet) && minor_tables_.empty() && allow_no_ready_read)) {
    if (memtables_.empty()) {
      LOG_INFO("no table in table store, cannot read", K(ret), K(*this));
    } else if (OB_FAIL(iterator.add_tables(memtables_))) {
      LOG_WARN("failed to add tables to iterator", K(ret));
    }
  } else if (OB_UNLIKELY(!allow_no_ready_read && !is_ready_for_read_)) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("table store not ready for read", K(ret), K(allow_no_ready_read), K(PRINT_TS(*this)));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(snapshot_version), K(*this));
  } else if (OB_FAIL(calculate_read_tables(snapshot_version, tablet, iterator, allow_no_ready_read))) {
    LOG_WARN("failed to get read tables", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.set_retire_check())) {
      LOG_WARN("failed to set retire check to iterator", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::get_read_major_sstable(
    const int64_t snapshot_version,
    ObTableStoreIterator &iterator) const
{
  int ret = OB_SUCCESS;
  iterator.reset();

  if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < major_tables_.count(); ++i) {
      ObSSTable *sstable = major_tables_[i];
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sstable pointer", K(ret));
      } else if (snapshot_version < sstable->get_snapshot_version()) {
        break;
      } else if (snapshot_version == sstable->get_snapshot_version()) {
        if (OB_FAIL(iterator.add_table(sstable))) {
          LOG_WARN("failed to add major table to iterator", K(ret), KPC(sstable));
        }
      }
    }
  }

  return ret;
}

int ObTabletTableStore::get_all_sstable(ObTableStoreIterator &iter) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!meta_major_tables_.empty() && OB_FAIL(iter.add_tables(meta_major_tables_, 0, meta_major_tables_.count()))) {
    LOG_WARN("fail to add all meta major tables to iterator", K(ret), K_(meta_major_tables));
  } else if (!major_tables_.empty() && OB_FAIL(iter.add_tables(major_tables_, 0, major_tables_.count()))) {
    LOG_WARN("fail to add all major tables to iterator", K(ret), K_(major_tables));
  } else if (!minor_tables_.empty() && OB_FAIL(iter.add_tables(minor_tables_, 0, minor_tables_.count()))) {
    LOG_WARN("fail to add all minor tables to iterator", K(ret), K_(major_tables));
  } else if (!ddl_sstables_.empty() && OB_FAIL(iter.add_tables(ddl_sstables_, 0, ddl_sstables_.count()))) {
    LOG_WARN("fail to add all ddl sstables to iterator", K(ret), K_(ddl_sstables));
  }
  return ret;
}

int ObTabletTableStore::get_memtables(
    common::ObIArray<storage::ObITable *> &memtables,
    const bool need_active) const
{
  common::SpinRLockGuard guard(memtables_lock_);
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < memtables_.count(); ++i) {
    if (OB_ISNULL(memtables_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memtable must not null", K(ret), K(memtables_));
    } else if (!need_active && memtables_[i]->is_active_memtable()) {
      continue;
    } else if (OB_FAIL(memtables.push_back(memtables_[i]))) {
      LOG_WARN("failed to add memtables", K(ret), K(*this));
    }
  }
  return ret;
}

int ObTabletTableStore::update_memtables(const common::ObIArray<storage::ObITable *> &memtables)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(memtables_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(memtables_.rebuild(memtables))) {
    LOG_ERROR("failed to rebuild table store memtables", K(ret), K(memtables), KPC(this));
  }
  return ret;
}

int ObTabletTableStore::clear_memtables()
{
  common::SpinWLockGuard guard(memtables_lock_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is unexpected invalid", K(ret), KPC(this));
  } else {
    memtables_.reset();
  }
  return ret;
}

int ObTabletTableStore::get_first_frozen_memtable(ObITable *&table) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(memtables_lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < memtables_.count(); ++i) {
    if (OB_ISNULL(memtables_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memtable must not null", K(ret), K(memtables_));
    } else if (memtables_[i]->is_frozen_memtable()) {
      table = memtables_[i];
      break;
    }
  }
  return ret;
}

int ObTabletTableStore::get_ddl_sstables(ObTableStoreIterator &iter) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (OB_FAIL(iter.add_tables(ddl_sstables_, 0, ddl_sstables_.count()))) {
    LOG_WARN("failed to add ddl sstables to iterator", K(ret));
  }
  return ret;
}

int ObTabletTableStore::get_ha_tables(ObTableStoreIterator &iter, bool &is_ready_for_read) const
{
  int ret = OB_SUCCESS;
  iter.reset();
  is_ready_for_read = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (FALSE_IT(is_ready_for_read = is_ready_for_read_)) {
  } else if (!major_tables_.empty() && OB_FAIL(iter.add_tables(major_tables_, 0, major_tables_.count()))) {
    LOG_WARN("failed to add major table to iterator", K(ret));
  } else if (!minor_tables_.empty() && OB_FAIL(iter.add_tables(minor_tables_, 0, minor_tables_.count()))) {
    LOG_WARN("failed to add minor table to iterator", K(ret));
  } else if (!ddl_sstables_.empty() && OB_FAIL(iter.add_tables(ddl_sstables_, 0, ddl_sstables_.count()))) {
    LOG_WARN("failed to add ddl table to iterator", K(ret));
  } else if (OB_FAIL(iter.set_retire_check())) {
    LOG_WARN("failed to set retire check to iterator", K(ret));
  } else {
    LOG_INFO("succeed to get ha tables", K(major_tables_), K(minor_tables_), K(ddl_sstables_));
  }
  return ret;
}

int ObTabletTableStore::get_mini_minor_sstables(
    const bool is_ha_data_status_complete,
    ObTableStoreIterator &iter) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (is_ha_data_status_complete) {
    if (OB_FAIL(iter.add_tables(minor_tables_, 0, minor_tables_.count()))) {
      LOG_WARN("failed to get all minor tables", K(ret));
    }
  } else if (OB_FAIL(get_ha_mini_minor_sstables_(iter))) {
    LOG_WARN("failed to get ha mini minor sstables", K(ret), K(minor_tables_));
  }
  return ret;
}

int ObTabletTableStore::get_recycle_version(
    const int64_t multi_version_start,
    int64_t &recycle_version) const
{
  int ret = OB_SUCCESS;
  recycle_version = 0;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_tables;
  if (major_tables_.empty()) {
  } else if (OB_FAIL(major_tables_.get_all_tables(major_tables))) {
    LOG_WARN("failed to get major tables from old store", K(ret), KPC(this));
  } else if (OB_FAIL(ObTableStoreUtil::sort_major_tables(major_tables))) {
    LOG_WARN("failed to sort major tables", K(ret));
  } else {
    for (int64_t i = major_tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (major_tables.at(i)->get_snapshot_version() <= multi_version_start) {
        recycle_version = major_tables.at(i)->get_snapshot_version();
        break;
      }
    }
    if (0 == recycle_version && major_tables.count() > 0) {
      recycle_version = major_tables.at(0)->get_snapshot_version();
      LOG_WARN("not found inc base snapshot version, use the oldest major table", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::load_sstable(const ObMetaDiskAddr &addr, ObStorageMetaHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!addr.is_valid() || addr.is_none() || addr.is_memory())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    ObStorageMetaCache &meta_cache = OB_STORE_CACHE.get_storage_meta_cache();
    ObStorageMetaKey meta_key(MTL_ID(), addr);
    if (OB_FAIL(meta_cache.get_meta(ObStorageMetaValue::MetaType::SSTABLE, meta_key, handle, nullptr))) {
      LOG_WARN("fail to retrieve sstable meta from meta cache", K(ret), K(addr));
    }
  }
  return ret;
}

int ObTabletTableStore::load_sstable_on_demand(
    const ObStorageMetaHandle &table_store_handle,
    blocksstable::ObSSTable &orig_sstable,
    ObStorageMetaHandle &loaded_sstable_handle,
    blocksstable::ObSSTable *&loaded_sstable)
{
  int ret = OB_SUCCESS;
  if (orig_sstable.is_loaded()) {
    // sstable is already loaded, life time guaranteed by table store.
    loaded_sstable = &orig_sstable;
    loaded_sstable_handle = table_store_handle;
  } else if (OB_FAIL(load_sstable(orig_sstable.get_addr(), loaded_sstable_handle))) {
    LOG_WARN("fail to load sstable", K(ret), K(orig_sstable));
  } else if (OB_FAIL(loaded_sstable_handle.get_sstable(loaded_sstable))) {
    LOG_WARN("fail to get loaded sstable from storage meta handle", K(ret), K(loaded_sstable_handle));
  }
  return ret;
}

int ObTabletTableStore::build_new_table_store(
    ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  const ObITable *new_table = static_cast<ObITable *>(const_cast<ObSSTable *>(param.sstable_)); //table can be null
  int64_t inc_base_snapshot_version = -1;

  if (OB_UNLIKELY(!major_tables_.empty() || !minor_tables_.empty())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("already exists sstable, cannot build new table store", K(ret), KPC(this));
  } else if (OB_ISNULL(new_table)) {
    // skip range check
  } else if (OB_UNLIKELY(!new_table->is_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table type", K(ret), KPC(new_table));
  } else if (new_table->is_meta_major_sstable()) {
    if (!old_store.meta_major_tables_.empty()
        && new_table->get_end_scn() <= old_store.meta_major_tables_.at(0)->get_end_scn()) {
      ret= OB_MINOR_SSTABLE_RANGE_CROSS;
      LOG_WARN("new meta major table is covered by old one", K(ret), KPC(new_table), K(old_store));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_major_tables(allocator, param, old_store, inc_base_snapshot_version))) {
    LOG_WARN("failed to build major_tables", K(ret));
  } else if (OB_FAIL(build_minor_tables(allocator, param, old_store, inc_base_snapshot_version))) {
    if (OB_UNLIKELY(OB_NO_NEED_MERGE != ret)) {
      LOG_WARN("failed to build minor_tables", K(ret));
    }
  } else if (OB_FAIL(build_ddl_sstables(allocator, tablet, param, old_store))) {
    LOG_WARN("failed to add ddl minor sstable", K(ret));
  } else if (OB_FAIL(build_meta_major_table(allocator, param.sstable_, old_store))) {
    LOG_WARN("failed to build meta major tables", K(ret));
  } else if (OB_FAIL(build_memtable_array(tablet))) {
    LOG_WARN("failed to pull memtable from memtable_mgr", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator, tablet))) {
    LOG_WARN("pull_ddl_memtables failed", K(ret));
  } else {
    is_inited_ = true;
    if (OB_FAIL(check_ready_for_read(tablet))) {
      LOG_WARN("failed to check ready for read", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(try_cache_local_sstables(allocator))) {
        LOG_WARN("failed to try cache local sstables", K(tmp_ret));
      }
      FLOG_INFO("succeed to build new table store", K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
    }
  }
  return ret;
}

int ObTabletTableStore::build_major_tables(
    ObArenaAllocator &allocator,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    int64_t &inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  inc_base_snapshot_version = -1;
  ObITable *new_table = static_cast<ObITable *>(const_cast<ObSSTable *>(param.sstable_)); //table can be null
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> major_tables;
  if (OB_NOT_NULL(new_table) && OB_FAIL(major_tables.push_back(new_table))) {
    LOG_WARN("failed to add table into tables handle", K(ret), K(param));
  } else if (OB_FAIL(inner_build_major_tables_(allocator, old_store, major_tables,
      param.multi_version_start_, param.allow_duplicate_sstable_, inc_base_snapshot_version))) {
    LOG_WARN("failed to inner build major tables", K(ret), K(param), K(major_tables));
  }
  return ret;
}

int ObTabletTableStore::inner_build_major_tables_(
    common::ObArenaAllocator &allocator,
    const ObTabletTableStore &old_store,
    const ObIArray<ObITable *> &tables_array,
    const int64_t multi_version_start,
    const bool allow_duplicate_sstable,
    int64_t &inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  inc_base_snapshot_version = -1;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_tables;
  bool need_add = true;

  if (!old_store.major_tables_.empty() && OB_FAIL(old_store.major_tables_.get_all_tables(major_tables))) {
    LOG_WARN("failed to get major tables from old store", K(ret), K(old_store));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_array.count(); ++i) {
    ObITable *new_table = tables_array.at(i);
    need_add = true;
    if (OB_NOT_NULL(new_table) && new_table->is_major_sstable()) {
      for (int64_t j = 0; OB_SUCC(ret) && j < major_tables.count(); ++j) {
        ObITable *table = major_tables.at(j);
        if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_major_sstable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table", K(ret), KPC(table));
        } else if (new_table->get_key() == table->get_key()) {
          if (OB_UNLIKELY(!allow_duplicate_sstable)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected new major table which has same range with old major sstable",
                K(ret), KPC(new_table), KPC(table));
          } else {
            ObSSTable *new_sstable = static_cast<ObSSTable *>(new_table);
            ObSSTable *old_sstable = static_cast<ObSSTable *>(table);
            ObSSTableMetaHandle new_sst_meta_hdl;
            ObSSTableMetaHandle old_sst_meta_hdl;
            if (OB_FAIL(new_sstable->get_meta(new_sst_meta_hdl))) {
              LOG_WARN("failed to get new sstable meta handle", K(ret));
            } else if (OB_FAIL(old_sstable->get_meta(old_sst_meta_hdl))) {
              LOG_WARN("failed to get old sstable meta handle", K(ret));
            } else if (OB_FAIL(ObSSTableMetaChecker::check_sstable_meta(
                new_sst_meta_hdl.get_sstable_meta(), old_sst_meta_hdl.get_sstable_meta()))) {
              LOG_WARN("failed to check sstable meta", K(ret), KPC(new_sstable), KPC(old_sstable));
            } else {
              need_add = false;
            }
          }
        }
      }

      if (OB_SUCC(ret) && need_add && OB_FAIL(major_tables.push_back(new_table))) {
        LOG_WARN("failed to push new table into array", K(ret), KPC(new_table), K(major_tables));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTableStoreUtil::sort_major_tables(major_tables))) {
    LOG_WARN("failed to sort major tables", K(ret));
  } else {
    int64_t start_pos = 0;
    for (int64_t i = major_tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (major_tables.at(i)->get_snapshot_version() <= multi_version_start) {
        start_pos = i;
        inc_base_snapshot_version = major_tables.at(i)->get_snapshot_version();
        break;
      }
    }
    if (-1 == inc_base_snapshot_version && major_tables.count() > 0) {
      inc_base_snapshot_version = major_tables.at(0)->get_snapshot_version();
      LOG_WARN("not found inc base snapshot version, use the oldest major table", K(ret));
    }

    if (major_tables.empty()) {
      LOG_INFO("major tables is empty", K(major_tables));
    } else if (OB_FAIL(major_tables_.init(allocator, major_tables, start_pos))) {
      LOG_WARN("failed to init major_tables", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::build_minor_tables(
    common::ObArenaAllocator &allocator,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    const int64_t inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObSSTable *new_sstable = const_cast<ObSSTable *>(param.sstable_);
  ObITable *new_table = static_cast<ObITable *>(new_sstable); //table can be null
  ObArray<ObITable *> minor_tables;

  if (NULL == new_table || !new_table->is_minor_sstable()) {
  } else if (!new_table->get_key().scn_range_.is_valid() ||
             new_table->get_key().scn_range_.is_empty()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("meet invalid or empty scn_range sstable", KPC(new_table));
  } else if (param.need_check_sstable_) { // fix issue 45431762
    const ObSSTableArray &old_minor_tables = old_store.minor_tables_;
    if (old_minor_tables.empty()) {
      // no minor tables to override new_table, skip to add new_table
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No minor tables in old store, cannot add a minor sstable", K(ret), K(param), KPC(new_table), K(old_store));
    } else if (new_table->get_end_scn() < old_minor_tables.get_boundary_table(false/*first*/)->get_start_scn()
            || new_table->get_start_scn() > old_minor_tables.get_boundary_table(true/*last*/)->get_end_scn()) {
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No minor tables covered by new minor table in old store, cannot add the new minor table",
          K(ret), K(param), KPC(new_table), K(old_store));
    }
  }

  if (OB_SUCC(ret)) {
    ObITable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < old_store.minor_tables_.count(); ++i) {
      table = old_store.minor_tables_[i];
      bool need_add = true;
      if (OB_UNLIKELY(NULL == table || !table->is_minor_sstable())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must be minor sstable", K(ret), KPC(table));
      } else if (OB_NOT_NULL(new_table) && new_table->is_minor_sstable()) {
        if (new_table->get_key() == table->get_key()) {
          ObSSTable *sstable = static_cast<ObSSTable *>(table);
          if (sstable->get_max_merged_trans_version() <= new_sstable->get_max_merged_trans_version()) {
            need_add = false;
            LOG_INFO("new table's max merge trans version is not less than the old table, "
                "add new table when table key is same", KPC(sstable), KPC(new_sstable));
          } else {
            ret = OB_NO_NEED_MERGE;
            LOG_WARN("new table with old max merged trans version, no need to merge", K(ret), KPC(sstable), KPC(new_sstable));
          }
        } else if (ObTableStoreUtil::check_include_by_scn_range(*new_table, *table)) {
          LOG_DEBUG("table purged", K(*new_table), K(*table));
          continue;
        } else if (ObTableStoreUtil::check_include_by_scn_range(*table, *new_table)) {
          ret = OB_MINOR_SSTABLE_RANGE_CROSS;
          LOG_WARN("new_table is contained by existing table", K(ret), KPC(new_table), KPC(table));
        } else if (ObTableStoreUtil::check_intersect_by_scn_range(*table, *new_table)) {
          ret = OB_MINOR_SSTABLE_RANGE_CROSS;
          LOG_WARN("new table's range is crossed with existing table", K(ret), K(*new_table), K(*table));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (need_add && OB_FAIL(minor_tables.push_back(table))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(new_table) && new_table->is_minor_sstable()
        && (OB_FAIL(minor_tables.push_back(new_table)))) {
      LOG_WARN("failed to add new minor table", K(ret), KPC(new_table));
    } else if (minor_tables.empty()) { // no minor tables
    } else if (minor_tables.count() == old_store.minor_tables_.count() && minor_tables.count() >= MAX_SSTABLE_CNT) {
      ret = OB_MINOR_MERGE_NOT_ALLOW;
      LOG_WARN("too many sstables, cannot add new minor sstable", K(ret), K(new_table));
    } else if (OB_FAIL(ObTableStoreUtil::sort_minor_tables(minor_tables))) {
      LOG_WARN("failed to sort minor tables", K(ret));
    } else {
      int64_t inc_pos = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
        if (minor_tables.at(i)->get_upper_trans_version() > inc_base_snapshot_version) {
          inc_pos = i;
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (inc_pos >= 0 && OB_FAIL(minor_tables_.init(allocator, minor_tables, inc_pos))) {
        LOG_WARN("failed to init minor_tables", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::build_meta_major_table(
    common::ObArenaAllocator &allocator,
    const blocksstable::ObSSTable *new_sstable,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObITable *new_table = static_cast<ObITable *>(const_cast<ObSSTable *>(new_sstable));
  ObITable *last_major = nullptr;
  meta_major_tables_.reset();

  if (OB_ISNULL(last_major = major_tables_.get_boundary_table(true))) {
    LOG_INFO("no major sstable exists, skip to try to build meta sstable", K(*this));
  } else if (OB_NOT_NULL(new_table) && new_table->is_meta_major_sstable()) { // new meta sstable must be newer than old meta
    if (new_table->get_max_merged_trans_version() <= last_major->get_snapshot_version()) {
      ret= OB_MINOR_SSTABLE_RANGE_CROSS;
      LOG_WARN("the new meta merge sstable is covered by major", K(ret), KPC(new_table), KPC(last_major));
    } else if (OB_FAIL(meta_major_tables_.init(allocator, static_cast<ObSSTable *>(new_table)))) {
      LOG_WARN("failed to init meta major tables", K(ret));
    }
  } else if (OB_NOT_NULL(new_table) && new_table->is_major_sstable()) {
    // new table is major sstable, retire old meta sstable.
  } else if (!old_store.meta_major_tables_.empty()) {
    ObITable *old_meta_major = old_store.meta_major_tables_.at(0);
    if (old_meta_major->get_snapshot_version() <= last_major->get_snapshot_version()) { // new table is not meta sstable
      FLOG_INFO("meta sstable is covered by major sstable", KPC(last_major), KPC(old_meta_major));
    } else if (OB_FAIL(meta_major_tables_.init(allocator, static_cast<ObSSTable *>(old_meta_major)))) {
      LOG_WARN("failed to init meta major tables", K(ret));
    }
  }
  return ret;
}

bool ObTabletTableStore::is_major_sstable_empty(const ObTablet &tablet) const
{
  return major_tables_.empty()
    && !tablet.get_tablet_meta().ddl_commit_scn_.is_valid_and_not_min(); // ddl logic major sstable require commit scn valid
}

int ObTabletTableStore::get_ddl_major_sstables(ObIArray<ObITable *> &ddl_major_sstables) const
{
  int ret = OB_SUCCESS;
  ddl_major_sstables.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t ddl_sstable_count = ddl_sstables_.count() + ddl_mem_sstables_.count();
    if (ddl_sstable_count > 0 && OB_FAIL(ddl_major_sstables.reserve(ddl_sstable_count))) {
      LOG_WARN("reserve ddl sstable array failed", K(ret), K(ddl_sstable_count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables_.count(); ++i) {
      if (OB_FAIL(ddl_major_sstables.push_back(ddl_sstables_[i]))) {
        LOG_WARN("push back ddl dump sstable failed", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_mem_sstables_.count(); ++i) {
      if (OB_FAIL(ddl_major_sstables.push_back(ddl_mem_sstables_[i]))) {
        LOG_WARN("push back old ddl sstable failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::pull_ddl_memtables(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> ddl_memtables;
  if (OB_FAIL(tablet.get_ddl_memtables(ddl_memtables))) {
    LOG_WARN("failed to get ddl memtables array from tablet", K(ret));
  } else if (!ddl_memtables.empty() && OB_FAIL(ddl_mem_sstables_.init(allocator, ddl_memtables))) {
    LOG_WARN("assign ddl memtables failed", K(ret), K(ddl_memtables));
  }
  return ret;
}

int ObTabletTableStore::build_ddl_sstables(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObITable *new_table = static_cast<ObITable *>(const_cast<ObSSTable *>(param.sstable_));
  ObSSTableMetaHandle new_table_meta_handle;
  ObArray<ObITable *> ddl_dump_sstables;
  ObArray<ObITable *> ddl_mem_sstables;

  bool is_new_table_valid_ddl_sstable = false;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_ISNULL(new_table)) {
    is_new_table_valid_ddl_sstable = false;
  } else if (OB_FAIL(static_cast<ObSSTable *>(new_table)->get_meta(new_table_meta_handle))) {
    LOG_WARN("fail to get sstable meta handle", K(ret));
  } else {
    is_new_table_valid_ddl_sstable = nullptr != new_table && new_table->is_ddl_dump_sstable()
        && new_table_meta_handle.get_sstable_meta().get_basic_meta().ddl_scn_ >= tablet.get_tablet_meta().ddl_start_scn_;
  }

  if (OB_FAIL(ret)) {
  } else if (param.ddl_info_.keep_old_ddl_sstable_) {
    if (!is_new_table_valid_ddl_sstable) { // not valid ddl sstable, simple keep old ddl sstable
      if (OB_FAIL(old_store.ddl_sstables_.get_all_tables(ddl_dump_sstables))) {
        LOG_WARN("get ddl dump sstables failed", K(ret));
      }
    } else { // new_table is valid, calculate new ddl sstables
      // since the scn is continuous in each table, so we can make it like this:
      // collect all ddl sstable and sort by:
      //   start_scn asc, means smaller start_scn first
      //   end_scn desc, means larger scn range first
      //   prefer ddl dump sstable
      // compare end_scn of this table with max end_scn, filter out the table whose end_scn is smaller
      ObArray<ObITable *> ddl_sstables;
      const int64_t ddl_sstable_count = old_store.ddl_sstables_.count() + 1;
      if (OB_FAIL(ddl_sstables.reserve(ddl_sstable_count))) {
        LOG_WARN("reserve ddl sstable array failed", K(ret), K(ddl_sstable_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < old_store.ddl_sstables_.count(); ++i) {
        if (OB_FAIL(ddl_sstables.push_back(old_store.ddl_sstables_.at(i)))) {
          LOG_WARN("push back old ddl sstable failed", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_sstables.push_back(new_table))) {
          LOG_WARN("push back new ddl sstable failed", K(ret), KPC(new_table));
        } else {
          std::sort(ddl_sstables.begin(), ddl_sstables.end(), [](ObITable *left, ObITable *right) {
              return left->get_start_scn() != right->get_start_scn() ?
              left->get_start_scn() < right->get_start_scn() : (left->get_end_scn() != right->get_end_scn() ?
               left->get_end_scn() > right->get_end_scn() : left->get_key().table_type_ < right->get_key().table_type_);
              });
        }
      }
      SCN max_end_scn = SCN::min_scn();
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables.count(); ++i) {
        ObITable *cur_table = ddl_sstables.at(i);
        if (cur_table->get_end_scn() <= max_end_scn) {
          // drop it
          LOG_INFO("drop ddl sstable", K(i), K(max_end_scn), KPC(cur_table));
        } else if (OB_FAIL(ddl_dump_sstables.push_back(cur_table))) {
          LOG_WARN("push back ddl dump sstable failed", K(ret), K(i));
        } else {
          max_end_scn = cur_table->get_end_scn();
        }
      }
    }
  } else { // remove old ddl sstable
    if (is_new_table_valid_ddl_sstable) {
      if (OB_FAIL(ddl_dump_sstables.push_back(new_table))) {
        LOG_WARN("push back ddl dump table failed", K(ret), KPC(new_table));
      } else {
        LOG_INFO("push back ddl dump sstable success after clean up", KPC(new_table));
      }
    }
  }

  if (OB_SUCC(ret) && !ddl_dump_sstables.empty()) {
    if (OB_FAIL(ddl_sstables_.init(allocator, ddl_dump_sstables))) {
      LOG_WARN("failed to init ddl_sstables", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::build_memtable_array(const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, MAX_MEMSTORE_CNT> memtable_array;
  if (OB_FAIL(tablet.get_memtables(memtable_array, true/*need_active*/))) {
    LOG_WARN("failed to get all memtables from memtable_mgr", K(ret));
  } else if (memtable_array.empty()) {
    // empty memtable array
  } else if (OB_FAIL(memtables_.build(memtable_array))) {
    LOG_WARN("failed to build memtable array", K(ret), K(memtable_array));
  }
  return ret;
}

int ObTabletTableStore::check_ready_for_read(const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  is_ready_for_read_ = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KPC(this));
  } else if (is_major_sstable_empty(tablet)) {
    LOG_INFO("no valid major sstable, not ready for read", K(*this));
  } else if (OB_FAIL(check_continuous())) {
    LOG_WARN("failed to check continuous of tables", K(ret));
  } else if (minor_tables_.count() + 1 > MAX_SSTABLE_CNT_IN_STORAGE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Too Many sstables in table store", K(ret), KPC(this), K(tablet));
    MTL(concurrency_control::ObMultiVersionGarbageCollector *)->report_sstable_overflow();
  } else if (get_table_count() > ObTabletTableStore::MAX_SSTABLE_CNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Too Many sstables, cannot add another sstable any more", K(ret), KPC(this), K(tablet));
    MTL(concurrency_control::ObMultiVersionGarbageCollector *)->report_sstable_overflow();
    compaction::ObPartitionMergePolicy::diagnose_table_count_unsafe(MAJOR_MERGE, tablet);
  } else if (minor_tables_.empty()) {
    is_ready_for_read_ = true;
  } else {
    const SCN &clog_checkpoint_scn = tablet.get_clog_checkpoint_scn();
    const SCN &last_minor_end_scn = minor_tables_.get_boundary_table(true/*last*/)->get_end_scn();
    if (OB_UNLIKELY(clog_checkpoint_scn != last_minor_end_scn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last minor table's end_scn must be equal to clog_checkpoint_scn",
          K(ret), K(last_minor_end_scn), K(clog_checkpoint_scn), KPC(this), K(tablet));
    } else {
      is_ready_for_read_ = true;
    }
  }

  if (OB_SUCC(ret) && get_table_count() > EMERGENCY_SSTABLE_CNT) {
    int tmp_ret = OB_TOO_MANY_SSTABLE;
    LOG_WARN("Emergency SSTable count, maybe frequency freeze occurs, or maybe multi_version_start not adavanced.",
             K(tmp_ret),
             "major table count: ", major_tables_.count(),
             "minor table count: ", minor_tables_.count());
  }
  return ret;
}

int ObTabletTableStore::check_continuous() const
{
  int ret = OB_SUCCESS;
  ObITable *prev_table = nullptr;
  ObITable *table = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < major_tables_.count(); ++i) {
      if (OB_UNLIKELY(NULL == (table = major_tables_[i]) || !table->is_major_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must be major table", K(ret), K(i), KPC(table));
      } else if (OB_ISNULL(prev_table)) {
      } else if (table->get_snapshot_version() < prev_table->get_snapshot_version()) {
        // recover ddl task may create new sstable with same major version and table key.
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("table version is invalid", K(ret), K(i), KPC(table), KPC(prev_table));
      }
      prev_table = table;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_minor_tables_continue_(minor_tables_))) {
        LOG_WARN("failed to check minor tables continue", K(ret), K(minor_tables_));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::batch_cache_sstable_meta(
    common::ObArenaAllocator &allocator,
    const int64_t remain_size)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_allocator(common::ObMemAttr(MTL_ID(), "CacheSSTable"));
  ObSafeArenaAllocator safe_allocator(tmp_allocator);
  common::ObSEArray<ObStorageMetaKey, 4> cache_keys;
  common::ObSEArray<ObSSTable *, 4> sstables;
  common::ObSEArray<ObStorageMetaHandle, 4> cache_handles;
  if (OB_UNLIKELY(remain_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(remain_size));
  } else if (OB_UNLIKELY(remain_size < sizeof(ObSSTableMeta))) {
    // The remain_size is too small to hold an sstable meta.
  } else if (OB_FAIL(get_need_to_cache_sstables(cache_keys, sstables))) {
    LOG_WARN("fail to get need to cache keys", K(ret));
  } else if (OB_UNLIKELY(0 == cache_keys.count())) {
  } else if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().batch_get_meta_and_bypass_cache(
      ObStorageMetaValue::SSTABLE, cache_keys, safe_allocator, cache_handles))) {
    LOG_WARN("fail to batch get meta and bypass cache", K(ret), K(cache_keys));
  } else if (OB_FAIL(batch_cache_sstable_meta(allocator, remain_size, sstables, cache_handles))) {
    LOG_WARN("fail to cache sstable meta", K(ret), K(remain_size), K(sstables), K(cache_handles));
  }
  return ret;
}

int ObTabletTableStore::get_need_to_cache_sstables(
    common::ObIArray<ObStorageMetaKey> &keys,
    common::ObIArray<blocksstable::ObSSTable *> &sstables)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_need_to_cache_sstables(meta_major_tables_, keys, sstables))) {
    LOG_WARN("fail to get need to cache sstables", K(ret), K(meta_major_tables_));
  } else if (OB_FAIL(get_need_to_cache_sstables(major_tables_, keys, sstables))) {
    LOG_WARN("fail to get need to cache sstables", K(ret), K(major_tables_));
  } else if (OB_FAIL(get_need_to_cache_sstables(minor_tables_, keys, sstables))) {
    LOG_WARN("fail to get need to cache sstables", K(ret), K(minor_tables_));
  } else if (OB_FAIL(get_need_to_cache_sstables(ddl_sstables_, keys, sstables))) {
    LOG_WARN("fail to get need to cache sstables", K(ret), K(ddl_sstables_));
  } else if (OB_UNLIKELY(keys.count() != sstables.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, the number of keys and sstables is not equal", K(ret), K(keys), K(sstables));
  }
  return ret;
}

int ObTabletTableStore::get_need_to_cache_sstables(
    const ObSSTableArray &sstable_array,
    common::ObIArray<ObStorageMetaKey> &keys,
    common::ObIArray<blocksstable::ObSSTable *> &sstables)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    ObSSTable *sstable = sstable_array[i];
    if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, sstable is nullptr", K(ret), KP(sstable));
    } else if (sstable->is_loaded()) { // sstable is already loaded to memory
    } else {
      ObStorageMetaKey key(MTL_ID(), sstable->get_addr());
      if (OB_FAIL(keys.push_back(key))) {
        LOG_WARN("fail to push back meta cache key", K(ret), K(key));
      } else if (OB_FAIL(sstables.push_back(sstable))) {
        LOG_WARN("fail to push back sstable ptr", K(ret), KPC(sstable));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::batch_cache_sstable_meta(
    common::ObArenaAllocator &allocator,
    const int64_t limit_size,
    common::ObIArray<blocksstable::ObSSTable *> &sstables,
    common::ObIArray<ObStorageMetaHandle> &handles)
{
  int ret = OB_SUCCESS;
  int64_t remain_size = limit_size;
  if (OB_UNLIKELY(limit_size <= 0 || sstables.count() != handles.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(remain_size), K(sstables), K(handles));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); ++i) {
      ObStorageMetaHandle &handle = handles.at(i);
      ObSSTableMetaHandle sst_meta_hdl;
      blocksstable::ObSSTable *sstable = sstables.at(i);
      blocksstable::ObSSTable *tmp_sstable = nullptr;
      if (OB_ISNULL(sstable) || OB_UNLIKELY(!handle.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), KP(sstable), K(handle));
      } else if (OB_FAIL(handle.get_sstable(tmp_sstable))) {
        LOG_WARN("fail to get sstable", K(ret), K(handle));
      } else if (OB_FAIL(tmp_sstable->get_meta(sst_meta_hdl))) {
        LOG_WARN("fail to get sstable meta", K(ret));
      } else {
        const int64_t deep_copy_size = sst_meta_hdl.get_sstable_meta().get_deep_copy_size();
        int64_t pos = 0;
        if (0  <= remain_size - deep_copy_size) {
          // cache local sstable meta
          char *buf = nullptr;
          ObSSTableMeta *copied_sstable_meta = nullptr;
          if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(deep_copy_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for sstable", K(ret), K(deep_copy_size));
          } else if (OB_FAIL(sst_meta_hdl.get_sstable_meta().deep_copy(buf, deep_copy_size, pos, copied_sstable_meta))) {
            LOG_WARN("fail to copy read cache to local sstable meta array", K(ret));
          } else if (OB_FAIL(sstable->assign_meta(copied_sstable_meta))) {
            LOG_WARN("fail to assign cached sstable meta to sstable", K(ret), KPC(sstable));
          } else {
            remain_size -= deep_copy_size;
          }
        } else {
          break; // The remaining size is not enough to hold the sstable meta.
        }
      }
    }
  }
  return ret;
}

template <class T>
int ObTabletTableStore::check_minor_tables_continue_(T &minor_tables) const
{
  int ret = OB_SUCCESS;
  ObITable *prev_table = nullptr;
  prev_table = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
    ObITable *table =  minor_tables.at(i);
    if (OB_FAIL(check_minor_table_continue_(table, prev_table))) {
      LOG_WARN("failed to check minor table continue", K(ret), KPC(table));
    }
  }
  return ret;
}

int ObTabletTableStore::need_remove_old_table(
    const int64_t multi_version_start,
    bool &need_remove) const
{
  int ret = OB_SUCCESS;
  need_remove = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletTableStore not init", K(ret), K(*this));
  } else if (major_tables_.empty()) {
    // do nothing
  } else if (multi_version_start <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(multi_version_start));
  } else if (minor_tables_.count() > 0 && minor_tables_[0]->get_upper_trans_version() <= major_tables_[0]->get_snapshot_version()) {
    // at least one minor sstable is coverd by major sstable
    // don't need to care about kept_multi_version_start here
    // becase major_tables_[0]::snapshot_version must <= kept_multi_version_start
    need_remove = true;
    LOG_INFO("need recycle unused minor table", K(ret), KPC(minor_tables_[0]), KPC(major_tables_[0]));
  } else if (major_tables_.count() > 1 && major_tables_[1]->get_snapshot_version() <= multi_version_start) {
    need_remove = true;
    LOG_INFO("need recycle oldest major sstable", K(ret), K(multi_version_start), KPC(major_tables_[1]));
  }
  return ret;
}

int ObTabletTableStore::build_ha_new_table_store(
    common::ObArenaAllocator &allocator,
    ObTablet &tablet,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !old_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table store get invalid argument", K(ret), K(tablet), K(param), K(old_store));
  } else if (OB_FAIL(init(allocator, tablet))) {
    LOG_WARN("failed to init a new empty table store", K(ret));
  } else if (OB_FAIL(build_ha_new_table_store_(allocator, tablet, param, old_store))) {
    LOG_WARN("failed to build new table store with old store", K(ret));
  }
  return ret;
}

int ObTabletTableStore::build_ha_new_table_store_(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  const ObSSTableArray &meta_major_table = old_store.meta_major_tables_;
  int64_t inc_base_snapshot_version = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !old_store.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("build ha new table store get invalid argument", K(ret), K(param), K(old_store));
  } else if (OB_FAIL(build_ha_major_tables_(allocator, param, old_store, inc_base_snapshot_version))) {
    LOG_WARN("failed to build ha major tables", K(ret), K(param), K(old_store));
  } else if (OB_FAIL(build_ha_minor_tables_(allocator, tablet, param, old_store, inc_base_snapshot_version))) {
    LOG_WARN("failed to build ha minor tables", K(ret), K(param), K(old_store));
  } else if (OB_FAIL(build_ha_ddl_tables_(allocator, tablet, param, old_store))) {
    LOG_WARN("failed to build ha ddl tables", K(ret), K(param), K(old_store));
  } else if (!meta_major_table.empty() && OB_FAIL(build_meta_major_table(allocator, nullptr/*new sstable*/, old_store))) {
    LOG_WARN("failed to build meta major table", K(ret), K(old_store));
  } else if (OB_FAIL(build_memtable_array(tablet))) {
    LOG_WARN("failed to pull memtable from memtable_mgr", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator, tablet))) {
    LOG_WARN("pull_ddl_memtables failed", K(ret));
  } else {
    is_inited_ = true;
    if (OB_FAIL(check_ready_for_read(tablet))) {
      LOG_WARN("failed to check ready for read", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(try_cache_local_sstables(allocator))) {
        LOG_WARN("failed to try cache local sstables", K(tmp_ret));
      }
      FLOG_INFO("succeed to build ha new table store",
          K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
    }
  }

  return ret;
}

int ObTabletTableStore::build_ha_major_tables_(
    common::ObArenaAllocator &allocator,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    int64_t &inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  inc_base_snapshot_version = -1;
  ObArray<ObITable *> major_tables;
  const bool allow_duplicate_sstable = true;
  const int64_t multi_version_start = 0;

  if (!param.tables_handle_.empty() && OB_FAIL(param.tables_handle_.get_tables(major_tables))) {
    LOG_WARN("failed to get major tables from param", K(ret));
  } else if (OB_FAIL(inner_build_major_tables_(allocator, old_store, major_tables,
      multi_version_start, allow_duplicate_sstable, inc_base_snapshot_version))) {
    LOG_WARN("failed to inner build major tables", K(ret), K(param));
  }
  return ret;
}

int ObTabletTableStore::replace_transfer_minor_sstables_(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> new_minor_tables;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT> need_add_minor_tables;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT> old_minor_tables;
  const int64_t inc_pos = 0;
  ObArray<ObITable *> cut_minor_tables;

  if (OB_FAIL(param.tables_handle_.get_all_minor_sstables(need_add_minor_tables))) {
    LOG_WARN("failed to add need add minor tables", K(ret), K(param));
  } else if (OB_FAIL(old_store.minor_tables_.get_all_tables(old_minor_tables))) {
    LOG_WARN("failed to get old minor tables", K(ret), K(old_store));
  } else if (OB_FAIL(check_old_store_minor_sstables_(old_minor_tables))) {
    LOG_WARN("failed to check old store minor sstables", K(ret), K(old_minor_tables));
  } else if (OB_FAIL(combine_transfer_minor_sstables_(allocator, tablet, old_minor_tables,
      need_add_minor_tables, param, new_minor_tables))) {
    LOG_WARN("failed to combine transfer minor sstables", K(ret), K(old_store), K(param));
  }

  if (OB_FAIL(ret)) {
  } else if (new_minor_tables.empty()) {
    LOG_INFO("minor tables is empty, skip it", K(ret), K(new_minor_tables));
  } else if (OB_FAIL(ObTableStoreUtil::sort_minor_tables(new_minor_tables))) {
    LOG_WARN("failed to sort minor tables", K(ret));
  } else if (OB_FAIL(cut_ha_sstable_scn_range_(allocator, new_minor_tables, cut_minor_tables))) {
    LOG_WARN("failed to cut ha sstable log ts range", K(ret), K(old_store), K(param));
  } else if (OB_FAIL(check_minor_tables_continue_(cut_minor_tables.count(), cut_minor_tables.get_data()))) {
    LOG_WARN("minor tables is not continue", K(ret), K(param), K(new_minor_tables), K(old_store));
  } else if (tablet.get_tablet_meta().ha_status_.is_data_status_complete()
      && cut_minor_tables.at(cut_minor_tables.count() - 1)->get_end_scn() != tablet.get_tablet_meta().clog_checkpoint_scn_) {
    // When the tablet's is_data_status_complete=false, it means that the data of the tablet has not been copied during the migration process.
    // In this scenario, no check is allowed.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta is not match with minor sstables", K(ret), K(cut_minor_tables), K(param), K(tablet.get_tablet_meta()));
  } else if (OB_FAIL(minor_tables_.init(allocator, cut_minor_tables, inc_pos))) {
    LOG_WARN("failed to init minor_tables", K(ret));
  } else {
    LOG_INFO("succeed build transfer minor sstables", K(old_store), K(cut_minor_tables));
  }
  return ret;
}

//migration will add tables with ddl and minor tables. replace_ha_minor_sstables_ will copy old minor
//tables when add ddl tables with param.update_ddl_sstable_ = true.
//new_minor_tables is empty
//   The corresponding reason may be that there is no minor at the source or the transfer_in tablet still exists in the transfer table
//new_minor_tables is not empty
//   Tablet still has a transfer table, so need to check the continuity of the transfer table and minor sstable
int ObTabletTableStore::replace_ha_minor_sstables_(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    const int64_t inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> new_minor_tables;
  UNUSED(inc_base_snapshot_version);
  ObSEArray<ObITable *, MAX_SSTABLE_CNT> need_add_minor_tables;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT> old_minor_tables;
  const int64_t inc_pos = 0;
  ObArray<ObITable *> cut_minor_tables;

  if (OB_FAIL(param.tables_handle_.get_all_minor_sstables(need_add_minor_tables))) {
    LOG_WARN("failed to add need add minor tables", K(ret), K(param));
  } else if (OB_FAIL(old_store.minor_tables_.get_all_tables(old_minor_tables))) {
    LOG_WARN("failed to get old minor tables", K(ret), K(old_store));
  } else if (OB_FAIL(check_old_store_minor_sstables_(old_minor_tables))) {
    LOG_WARN("failed to check old store minor sstables", K(ret), K(old_minor_tables));
  } else if (param.update_ddl_sstable_) {
    if (!need_add_minor_tables.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update ddl sstable but minor tables is not empty", K(ret), K(param), K(old_store));
    } else if (old_minor_tables.empty()) {
    } else if (OB_FAIL(minor_tables_.init(allocator, old_minor_tables, inc_pos))) {
      LOG_WARN("failed to init minor_tables", K(ret));
    }
  } else if (OB_FAIL(combine_ha_minor_sstables_(
      tablet, old_minor_tables, need_add_minor_tables, new_minor_tables))) {
    LOG_WARN("failed to combin ha minor sstables", K(ret), K(old_store), K(param));
  } else if (new_minor_tables.empty()) { // no minor tables
    if (tablet.get_tablet_meta().has_transfer_table()
        && tablet.get_tablet_meta().transfer_info_.transfer_start_scn_ != tablet.get_tablet_meta().clog_checkpoint_scn_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet meta is not match with minor sstables", K(ret), K(new_minor_tables), K(param), K(old_store));
    } else {
      LOG_INFO("minor tables is empty, skip it", K(ret), K(new_minor_tables));
    }
  } else if (OB_FAIL(ObTableStoreUtil::sort_minor_tables(new_minor_tables))) {
    LOG_WARN("failed to sort minor tables", K(ret));
  } else if (OB_FAIL(cut_ha_sstable_scn_range_(allocator, new_minor_tables, cut_minor_tables))) {
    LOG_WARN("failed to cut ha sstable log ts range", K(ret), K(old_store), K(param));
  } else if (OB_FAIL(check_minor_tables_continue_(cut_minor_tables.count(), cut_minor_tables.get_data()))) {
    LOG_WARN("minor tables is not continue", K(ret), K(param), K(cut_minor_tables), K(old_store));
  } else if (cut_minor_tables.at(cut_minor_tables.count() - 1)->get_end_scn() != tablet.get_tablet_meta().clog_checkpoint_scn_
      || (tablet.get_tablet_meta().has_transfer_table()
          && tablet.get_tablet_meta().transfer_info_.transfer_start_scn_ != cut_minor_tables.at(0)->get_start_scn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta is not match with minor sstables", K(ret), K(cut_minor_tables), K(param), K(old_store), "tablet_meta", tablet.get_tablet_meta());
  } else if (OB_FAIL(minor_tables_.init(allocator, cut_minor_tables, inc_pos))) {
    LOG_WARN("failed to init minor_tables", K(ret));
  } else {
    LOG_INFO("succeed build ha minor sstables", K(old_store), K(cut_minor_tables));
  }
  return ret;
}

int ObTabletTableStore::build_ha_ddl_tables_(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  //TODO(muwei.ym) need reuse local minor sstable and cut sstable log ts 4.3
  int ret = OB_SUCCESS;
  ObArray<ObITable *> ddl_tables;
  ObITable *new_table = nullptr;
  ObITable *last_ddl_table = nullptr;
  bool need_add_ddl_tables = true;
  ObSSTableMetaHandle new_meta_handle;

  if (!old_store.major_tables_.empty()) {
    need_add_ddl_tables = false;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < param.tables_handle_.get_count() && need_add_ddl_tables; ++i) {
    new_table = param.tables_handle_.get_table(i);
    if (OB_ISNULL(new_table) || !new_table->is_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table is null or table type is unexpected", K(ret), KPC(new_table));
    } else if (new_table->is_major_sstable()) {
      need_add_ddl_tables = false;
      break;
    } else if (!new_table->is_ddl_sstable()) {
      //do nothing
    } else if (OB_FAIL(static_cast<ObSSTable *>(new_table)->get_meta(new_meta_handle))) {
      LOG_WARN("get new table meta fail", K(ret), KPC(new_table));
    } else if (new_meta_handle.get_sstable_meta().get_basic_meta().ddl_scn_ < tablet.get_tablet_meta().ddl_start_scn_) {
      // the ddl start scn is old, drop it
    } else if (OB_NOT_NULL(last_ddl_table) && new_table->get_start_scn() != last_ddl_table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl table is not continue", K(ret), K(param), K(old_store));
    } else if (OB_FAIL(ddl_tables.push_back(new_table))) {
      LOG_WARN("failed to push new table into array", K(ret), KPC(new_table));
    } else {
      last_ddl_table = new_table;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < old_store.ddl_sstables_.count() && need_add_ddl_tables; ++i) {
    new_table = old_store.ddl_sstables_[i];
    if (OB_ISNULL(new_table) || !new_table->is_ddl_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table is null or table type is unexpected", K(ret), KPC(new_table));
    } else if (OB_NOT_NULL(last_ddl_table) && new_table->get_start_scn() != last_ddl_table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl table is not continue", K(ret), K(param), K(old_store));
    } else if (OB_FAIL(ddl_tables.push_back(new_table))) {
      LOG_WARN("failed to push new table into array", K(ret), KPC(new_table));
    } else {
      last_ddl_table = new_table;
    }
  }

  if (OB_SUCC(ret)) {
    if (!need_add_ddl_tables) {
      LOG_INFO("has major sstable ,no need add ddl sstable", K(param), K(old_store));
    } else if (ddl_tables.empty()) { // no minor tables
      LOG_INFO("ddl tables is empty, skip it", K(ret), K(ddl_tables));
    } else if (OB_FAIL(ddl_sstables_.init(allocator, ddl_tables))) {
      LOG_WARN("failed to init minor_tables", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::cut_ha_sstable_scn_range_(
    common::ObArenaAllocator &allocator,
    common::ObIArray<ObITable *> &orig_minor_sstables,
    common::ObIArray<ObITable *> &cut_minor_sstables)
{
  int ret = OB_SUCCESS;
  SCN last_end_scn = SCN::min_scn();
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_minor_sstables.count(); ++i) {
    ObITable *table = orig_minor_sstables.at(i);

    if (OB_ISNULL(table) || !table->is_multi_version_minor_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
    } else if (0 == i) {
      last_end_scn = table->get_end_scn();
    } else if (last_end_scn < table->get_start_scn() || last_end_scn >= table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("minor sstable log ts is not continue or scn has overlap", K(ret), K(orig_minor_sstables));
    } else if (last_end_scn == table->get_start_scn()) {
      last_end_scn = table->get_end_scn();
    } else {
      ObSSTable *sstable = static_cast<ObSSTable *>(table);
      ObSSTable *orig_sstable = nullptr;
      ObSSTable *copied_sstable = nullptr;
      ObMetaDiskAddr addr;
      addr.set_mem_addr(0, sizeof(ObSSTable));
      ObStorageMetaHandle sstable_handle;
      if (sstable->is_loaded()) {
        orig_sstable = sstable;
      } else if (OB_FAIL(ObTabletTableStore::load_sstable(sstable->get_addr(), sstable_handle))) {
        LOG_WARN("failed to load sstable", K(ret), KPC(sstable));
      } else if (OB_FAIL(sstable_handle.get_sstable(orig_sstable))) {
        LOG_WARN("failed to get sstable from sstable handle", K(ret), K(sstable_handle));
      }

      if (FAILEDx(orig_sstable->deep_copy(allocator, copied_sstable))) {
        LOG_WARN("failed to deep copy sstable", K(ret), KPC(orig_sstable), KP(copied_sstable));
      } else if (OB_FAIL(copied_sstable->set_addr(addr))) {
        LOG_WARN("failed to set sstable addr", K(ret), K(addr), KPC(copied_sstable));
      } else {
        table = copied_sstable;
        ObScnRange new_scn_range;
        ObScnRange original_scn_range = table->get_scn_range();
        new_scn_range.start_scn_ = last_end_scn;
        new_scn_range.end_scn_ = table->get_end_scn();

        table->set_scn_range(new_scn_range);
        last_end_scn = table->get_end_scn();
        LOG_INFO("cut ha sstable log ts range", KPC(orig_sstable), KPC(copied_sstable),
            K(new_scn_range), K(original_scn_range));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table));
      } else if (OB_FAIL(cut_minor_sstables.push_back(table))) {
        LOG_WARN("failed to add table into array", K(ret), KPC(table));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::check_minor_table_continue_(
    ObITable *table,
    ObITable *prev_table) const
{
  int ret = OB_SUCCESS;
  ObSSTable *curr_sstable = nullptr;
  ObSSTable *prev_sstable = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(table) || !table->is_multi_version_minor_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table must be multi version minor table", K(ret), KPC(table));
  } else if (OB_ISNULL(prev_table)) {
    // do nothing
  } else if (table->get_start_scn() > prev_table->get_end_scn()
      || table->get_end_scn() <= prev_table->get_end_scn()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table scn range not continuous or overlap", K(ret), KPC(table), KPC(prev_table));
  } else if (FALSE_IT(curr_sstable = static_cast<ObSSTable *>(table))) {
  } else if (FALSE_IT(prev_sstable = static_cast<ObSSTable *>(prev_table))) {
  } else if (table->get_key().tablet_id_.is_ls_inner_tablet() || prev_sstable->get_filled_tx_scn().is_max()) {
    // do nothing
  } else if (curr_sstable->get_filled_tx_scn() < prev_sstable->get_filled_tx_scn()) {
    ret = OB_ERR_SYS;
    LOG_WARN("sstable's filled_tx_scn is out of order", K(ret), KPC(table), KP(prev_table),
        "curr_filled_tx_scn", curr_sstable->get_filled_tx_scn(), "prev_filled_tx_scn", prev_sstable->get_filled_tx_scn());
  }
  prev_table = table;
  return ret;
}

int ObTabletTableStore::check_minor_tables_continue_(
    const int64_t count,
    ObITable **minor_sstables) const
{
  int ret = OB_SUCCESS;
  ObITable *prev_table = nullptr;
  prev_table = nullptr;
  if (count > 0 && OB_ISNULL(minor_sstables)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check minor tables continue minor sstables is unexpected", K(ret), KP(minor_sstables), K(count));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObITable *table =  minor_sstables[i];
    if (OB_FAIL(check_minor_table_continue_(table, prev_table))) {
      LOG_WARN("failed to check minor table continue", K(ret), KPC(table));
    }
  }
  return ret;
}

int ObTabletTableStore::combine_ha_minor_sstables_(
    const ObTablet &tablet,
    common::ObIArray<ObITable *> &old_store_minor_sstables,
    common::ObIArray<ObITable *> &need_add_minor_sstables,
    common::ObIArray<ObITable *> &new_minor_sstables)
{
  int ret = OB_SUCCESS;
  //TODO(muwei.ym) remove logical sstable in 4.2 RC3
  //1.ha now will not reuse minor sstable so it need add minor sstable which is from src and end_scn <= clog_checkpoint_scn
  //2.old store minor sstables contains remote logical sstable and after clog_checkpoint_scn sstables.
  SCN max_copy_end_scn;
  max_copy_end_scn.set_min();
  ObArray<ObITable *> tmp_minor_sstables;
  const SCN clog_checkpoint_scn = tablet.get_clog_checkpoint_scn();

  for (int64_t i = 0; OB_SUCC(ret) && i < need_add_minor_sstables.count(); ++i) {
    ObITable *table = need_add_minor_sstables.at(i);
    if (OB_FAIL(tmp_minor_sstables.push_back(table))) {
      LOG_WARN("failed to push table into array", K(ret), KPC(table));
    } else {
      max_copy_end_scn = table->get_end_scn();
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < old_store_minor_sstables.count(); ++i) {
    ObITable *table = old_store_minor_sstables.at(i);
    if (table->is_remote_logical_minor_sstable()) {
      if (max_copy_end_scn < table->get_end_scn() && !max_copy_end_scn.is_min()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remote logical minor sstable end scn is bigger than max copy end scn, unexpected",
            K(ret), K(max_copy_end_scn), KPC(table));
      }
    } else if (table->get_end_scn() <= max_copy_end_scn) {
      //do nothing
    } else if (OB_FAIL(tmp_minor_sstables.push_back(table))) {
      LOG_WARN("failed to push table into array", K(ret), KPC(table));
    }
  }

  if (OB_SUCC(ret)) {
    //TODO(muwei.ym) remove compare with clog checkpoint scn in 4.2 RC3
    if (tmp_minor_sstables.empty()) {
      //do nothing
    } else if (OB_FAIL(ObTableStoreUtil::sort_minor_tables(tmp_minor_sstables))) {
      LOG_WARN("failed to sort minor tables", K(ret), K(tmp_minor_sstables));
    } else if (clog_checkpoint_scn > tmp_minor_sstables.at(tmp_minor_sstables.count() - 1)->get_end_scn()) {
      FLOG_INFO("tablet clog checkpoint scn is bigger than all minor sstables end scn, no need to keep it",
          K(clog_checkpoint_scn), K(tmp_minor_sstables), K(major_tables_));
    } else if (OB_FAIL(new_minor_sstables.assign(tmp_minor_sstables))) {
      LOG_WARN("failed to assign minor sstables", K(ret), K(tmp_minor_sstables));
    }
  }

  return ret;
}

int ObTabletTableStore::combine_transfer_minor_sstables_(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    common::ObIArray<ObITable *> &old_store_minor_sstables,
    common::ObIArray<ObITable *> &need_add_minor_sstables,
    const ObBatchUpdateTableStoreParam &param,
    common::ObIArray<ObITable *> &new_minor_sstables)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < need_add_minor_sstables.count(); ++i) {
    ObITable *table = need_add_minor_sstables.at(i);
    if (OB_ISNULL(table) || !table->is_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new table is null or table type is unexpected", K(ret), KPC(table));
    } else if (table->get_start_scn() >= tablet.get_tablet_meta().transfer_info_.transfer_start_scn_) {
      //do nothing
    } else if (table->get_end_scn() <= tablet.get_tablet_meta().transfer_info_.transfer_start_scn_) {
      if (OB_FAIL(new_minor_sstables.push_back(table))) {
        LOG_WARN("failed to push table into array", K(ret), KPC(table));
      }
    } else {
      ObSSTable *old_sstable = static_cast<ObSSTable *>(table);
      ObSSTable *sstable = NULL;
      if (OB_FAIL(old_sstable->deep_copy(allocator, sstable))) {
        LOG_WARN("failed to copy sstable", K(ret), KP(old_sstable));
      } else {
        ObScnRange new_scn_range;
        ObScnRange original_scn_range = sstable->get_scn_range();
        new_scn_range.start_scn_ = original_scn_range.start_scn_;
        new_scn_range.end_scn_ = tablet.get_tablet_meta().transfer_info_.transfer_start_scn_;
        sstable->set_scn_range(new_scn_range);
        FLOG_INFO("cut ha sstable log ts range", KPC(sstable), K(new_scn_range), K(original_scn_range), K(param));

        if (OB_FAIL(new_minor_sstables.push_back(sstable))) {
          LOG_WARN("failed to push table into array", K(ret), KPC(sstable));
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < old_store_minor_sstables.count(); ++i) {
    ObITable *table = old_store_minor_sstables.at(i);
    if (OB_ISNULL(table) || !table->is_minor_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
    } else if (OB_FAIL(new_minor_sstables.push_back(table))) {
      LOG_WARN("failed to push minor table into array", K(ret), K(old_store_minor_sstables), KPC(table));
    }
  }

  return ret;
}

int ObTabletTableStore::check_old_store_minor_sstables_(
    common::ObIArray<ObITable *> &old_store_minor_sstables)
{
  int ret = OB_SUCCESS;
  int64_t remote_logical_minor_sstable_count = 0;

  if (OB_FAIL(check_minor_tables_continue_(old_store_minor_sstables))) {
    LOG_WARN("failed to check minor tables continue", K(ret), K(old_store_minor_sstables));
  }

  //check old store remote logical minor sstable count should be less than 1
  for (int64_t i = 0; OB_SUCC(ret) && i < old_store_minor_sstables.count(); ++i) {
    ObITable *table = old_store_minor_sstables.at(i);
    if (OB_ISNULL(table) || !table->is_multi_version_minor_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
    } else if (table->is_remote_logical_minor_sstable()) {
      remote_logical_minor_sstable_count++;
    }
  }
  if (OB_SUCC(ret) && remote_logical_minor_sstable_count > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old table store remote logical minor sstable count more than 1", K(ret), K(old_store_minor_sstables));
  }
  return ret;
}

int ObTabletTableStore::get_ha_mini_minor_sstables_(ObTableStoreIterator &iter) const
{
  int ret = OB_SUCCESS;
  int64_t index = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables_.count(); ++i) {
    ObSSTable *table = minor_tables_[i];
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table should not be NULL", K(ret), K(minor_tables_), KP(table));
    } else if (table->is_remote_logical_minor_sstable()) {
      index = i + 1;
      break;
    }
  }

  for (int64_t i = index; OB_SUCC(ret) && i < minor_tables_.count(); ++i) {
    ObSSTable *table = minor_tables_[i];
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table should not be NULL", K(ret), K(minor_tables_), KP(table));
    } else if (table->is_remote_logical_minor_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table store has multi remote logical minor sstable, unexpected !!!", K(ret), K(minor_tables_));
    } else if (OB_FAIL(iter.add_table(table))) {
      LOG_WARN("failed to push table into minor sstables array", K(ret), KPC(table), K(minor_tables_));
    }
  }
  return ret;
}

int ObTabletTableStore::update_ha_minor_sstables_(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> new_minor_tables;
  const ObSSTableArray &old_minor_tables = old_store.minor_tables_;

  if (param.start_scn_ >= tablet.get_clog_checkpoint_scn()) {
    //no need keep local minor sstable
    LOG_INFO("start scn is bigger than clog checkpoint ts, no need keep local minor sstable", K(old_store));
  } else {
    int64_t index = 0;
    bool has_remote_logical_sstable = false;
    for (int64_t i = 0; i < old_minor_tables.count(); ++i) {
      const ObITable *table = old_minor_tables[i];
      if (table->is_remote_logical_minor_sstable()) {
        has_remote_logical_sstable = true;
        index = i;
        break;
      }
    }

    if (has_remote_logical_sstable) {
      ObITable *table = old_minor_tables[index];
      if (!table->is_remote_logical_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table type is unexpected", K(ret), KPC(table), K(old_store), K(param));
      } else if (param.start_scn_ >= table->get_end_scn()) {
        //no need remote logical sstable
        index = index + 1;
      } else {
        ObSSTable *sstable = static_cast<ObSSTable *>(table);
        share::ObScnRange new_scn_range;
        share::ObScnRange original_scn_range = sstable->get_scn_range();
        new_scn_range.start_scn_ = param.start_scn_;
        new_scn_range.end_scn_ = table->get_end_scn();
        sstable->set_scn_range(new_scn_range);
        LOG_INFO("cut ha remote logical sstable log ts range", KPC(sstable), K(new_scn_range), K(original_scn_range));
      }
    } else {
      //local minor sstable contain param.start_scn, reuse local sstable
      //index = 0
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(old_minor_tables.get_all_tables(new_minor_tables))) {
        LOG_WARN("failed to get all minor tables", K(ret), K(old_minor_tables));
      } else if (index >= new_minor_tables.count()) {
        //reuse nothing, copy from src
      } else if (OB_FAIL(minor_tables_.init(allocator, new_minor_tables, index))) {
        LOG_WARN("failed to init minor_tables", K(ret), K(new_minor_tables));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::build_ha_minor_tables_(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    const int64_t inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (param.update_logical_minor_sstable_) {
    if (OB_FAIL(update_ha_minor_sstables_(allocator, tablet, param, old_store))) {
      LOG_WARN("failed to update ha minor sstables", K(ret), K(param), K(old_store));
    }
  } else if (param.is_transfer_replace_) {
    if (OB_FAIL(replace_transfer_minor_sstables_(allocator, tablet, param, old_store))) {
      LOG_WARN("failed to replace transfer minor tables", K(ret), K(param), K(old_store));
    }
  } else {
    if (OB_FAIL(replace_ha_minor_sstables_(allocator, tablet, param, old_store, inc_base_snapshot_version))) {
      LOG_WARN("failed to replace ha minor tables", K(ret), K(param), K(old_store));
    }
  }
  return ret;
}

int64_t ObTabletTableStore::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    J_OBJ_START();
    J_KV(KP(this), K_(version), K_(major_tables), K_(minor_tables), K_(ddl_sstables), K_(meta_major_tables), K_(memtables), K_(is_ready_for_read));
    J_COMMA();

    BUF_PRINTF("sstable_arrays:");
    J_OBJ_START();

    BUF_PRINTF("major_tables:{");
    if (0 == major_tables_.count()) {
      BUF_PRINTF("null");
    } else {
      for (int64_t i = 0; i < major_tables_.count(); ++i) {
        const ObSSTable *table = major_tables_[i];
        if (0 != i) {
          J_COMMA();
        }
        J_OBJ_START();
        J_KV(K(i), "addr", table->get_addr(),
            "type", ObITable::get_table_type_name(table->get_key().table_type_),
            "tablet_id", table->get_key().tablet_id_,
            "scn_range", table->get_key().scn_range_,
            "snapshot_version", table->get_snapshot_version(),
            "max_merge_version", table->get_max_merged_trans_version());
        J_OBJ_END();
      }
    }
    BUF_PRINTF("}");

    J_COMMA();
    BUF_PRINTF("minor_tables:{");
    if (0 == minor_tables_.count()) {
      BUF_PRINTF("null");
    } else {
      for (int64_t i = 0; i < minor_tables_.count(); ++i) {
        const ObSSTable *table = minor_tables_[i];
        if (0 != i) {
          J_COMMA();
        }
        J_OBJ_START();
        J_KV(K(i), "addr", table->get_addr(),
              "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "contain_uncommitted_row", table->contain_uncommitted_row() ? "yes" : "no",
              "max_merge_version", table->get_max_merged_trans_version(),
              "upper_trans_version", table->get_upper_trans_version());
        J_OBJ_END();
      }
    }
    BUF_PRINTF("}");

    J_COMMA();
    BUF_PRINTF("ddl_sstables:{");
    if (0 == ddl_sstables_.count()) {
      BUF_PRINTF("null");
    } else {
      for (int64_t i = 0; i < ddl_sstables_.count(); ++i) {
        const ObSSTable *table = ddl_sstables_[i];
        if (0 != i) {
          J_COMMA();
        }
        J_OBJ_START();
        J_KV(K(i), "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "max_merge_version", table->get_max_merged_trans_version());
        J_OBJ_END();
      }
    }
    BUF_PRINTF("}");

    J_COMMA();
    BUF_PRINTF("ddl_mem_sstables:{");
    if (0 == ddl_mem_sstables_.count()) {
      BUF_PRINTF("null");
    } else {
      for (int64_t i = 0; i < ddl_mem_sstables_.count(); ++i) {
        ObITable *table = ddl_mem_sstables_[i];
        if (NULL != table && table->is_sstable()) {
          if (0 != i) {
            J_COMMA();
          }
          J_OBJ_START();
          J_KV(K(i), "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "ref", table->get_ref(),
              "max_merge_version", static_cast<ObSSTable *>(table)->get_max_merged_trans_version());
          J_OBJ_END();
        }
      }
    }
    BUF_PRINTF("}");

    J_COMMA();
    BUF_PRINTF("meta_major_tables:{");
    if (0 == meta_major_tables_.count()) {
      BUF_PRINTF("null");
    } else {
      for (int64_t i = 0; i < meta_major_tables_.count(); ++i) {
        const ObSSTable *table = meta_major_tables_[i];
        if (0 != i) {
          J_COMMA();
        }
        J_OBJ_START();
        J_KV(K(i), "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "max_merge_version", table->get_max_merged_trans_version());
        J_OBJ_END();
        J_COMMA();
      }
    }
    BUF_PRINTF("}");

    J_COMMA();
    BUF_PRINTF("memtables_:{");
    if (0 == memtables_.count()) {
      BUF_PRINTF("null");
    } else {
      for (int64_t i = 0; i < memtables_.count(); ++i) {
        const memtable::ObIMemtable *memtable = memtables_[i];
        if (0 != i) {
          J_COMMA();
        }
        J_OBJ_START();
        J_KV(K(i), KPC(memtable));
        J_OBJ_END();
      }
    }
    BUF_PRINTF("}");

    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}

ObPrintTableStore::ObPrintTableStore(const ObTabletTableStore &table_store)
  : major_tables_(table_store.major_tables_),
    minor_tables_(table_store.minor_tables_),
    memtables_(table_store.memtables_),
    ddl_mem_sstables_(table_store.ddl_mem_sstables_),
    ddl_sstables_(table_store.ddl_sstables_),
    meta_major_tables_(table_store.meta_major_tables_),
    is_ready_for_read_(table_store.is_ready_for_read_) {}

int64_t ObPrintTableStore::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObTabletTableStore_Pretty");
    J_COLON();
    J_KV(KP(this), K_(major_tables), K_(minor_tables), K_(memtables), K_(is_ready_for_read));
    J_COMMA();
    BUF_PRINTF("table_array");
    J_COLON();
    J_OBJ_START();
    if (!major_tables_.empty() || !minor_tables_.empty() || !memtables_.empty() || !ddl_mem_sstables_.empty()
        || !ddl_sstables_.empty() || !meta_major_tables_.empty()) {
      ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
      J_NEWLINE();
      // table_type|max_merge_version
      //      |upper_trans_version|start_scn|end_scn|ref|buffer_minor
      BUF_PRINTF("[%ld] [ ", GETTID());
      BUF_PRINTO(PC(trace_id));
      BUF_PRINTF(" ] ");
      BUF_PRINTF(" %-10s %-10s %-19s %-19s %-19s %-19s %-4s %-16s \n",
          "table_arr", "table_type", "upper_trans_ver", "max_merge_ver",
          "start_scn", "end_scn", "ref", "uncommit_row");
      bool is_print = false;
      print_arr(major_tables_, "MAJOR", buf, buf_len, pos, is_print);
      print_arr(minor_tables_, "MINOR", buf, buf_len, pos, is_print);
      print_arr(ddl_sstables_, "DDL_DUMP", buf, buf_len, pos, is_print);
      print_ddl_mem(ddl_mem_sstables_, "DDL_MEM", buf, buf_len, pos, is_print);
      print_mem(memtables_, "MEM", buf, buf_len, pos, is_print);
      print_arr(meta_major_tables_, "META_MAJOR", buf, buf_len, pos, is_print);
    } else {
      J_EMPTY_OBJ();
    }
    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}

void ObPrintTableStore::print_mem(
    const ObMemtableArray &tables,
    const char* table_arr,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    bool &is_print) const
{
  for (int64_t i = 0; i < tables.count(); ++i) {
    if (is_print && 0 == i) {
      J_NEWLINE();
    }
    table_to_string(tables[i], i == 0 ? table_arr : " ", buf, buf_len, pos);
    if (i < tables.count() - 1) {
      J_NEWLINE();
    }
  }
  if (tables.count() > 0) {
    is_print = true;
  }
}
void ObPrintTableStore::print_ddl_mem(
    const ObDDLKVArray &tables,
    const char* table_arr,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    bool &is_print) const
{
  for (int64_t i = 0; i < tables.count(); ++i) {
    if (is_print && 0 == i) {
      J_NEWLINE();
    }
    table_to_string(tables[i], i == 0 ? table_arr : " ", buf, buf_len, pos);
    if (i < tables.count() - 1) {
      J_NEWLINE();
    }
  }
  if (tables.count() > 0) {
    is_print = true;
  }
}

void ObPrintTableStore::print_arr(
    const ObSSTableArray &tables,
    const char* table_arr,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    bool &is_print) const
{
  for (int64_t i = 0; i < tables.count(); ++i) {
    if (is_print && 0 == i) {
      J_NEWLINE();
    }
    table_to_string(tables[i], i == 0 ? table_arr : " ", buf, buf_len, pos);
    if (i < tables.count() - 1) {
      J_NEWLINE();
    }
  }
  if (tables.count() > 0) {
    is_print = true;
  }
}

void ObPrintTableStore::table_to_string(
     ObITable *table,
     const char* table_arr,
     char *buf,
     const int64_t buf_len,
     int64_t &pos) const
{
  if (nullptr != table) {
    ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
    BUF_PRINTF("[%ld] [ ", GETTID());
    BUF_PRINTO(PC(trace_id));
    BUF_PRINTF(" ] ");
    const char* table_name = table->is_sstable()
      ? ObITable::get_table_type_name(table->get_key().table_type_)
      : (table->is_active_memtable() ? "ACTIVE" : "FROZEN");
    const char * uncommit_row = table->is_sstable()
      ? (static_cast<ObSSTable *>(table)->contain_uncommitted_row() ? "true" : "false")
      : "unused";

    BUF_PRINTF(" %-10s %-10s %-19lu %-19lu %-10s %-10s %-4ld %-16s ",
      table_arr,
      table_name,
      table->get_upper_trans_version(),
      table->get_max_merged_trans_version(),
      to_cstring(table->get_start_scn()),
      to_cstring(table->get_end_scn()),
      table->get_ref(),
      uncommit_row);
  }
}

} // namespace storage
} // namespace oceanbase
