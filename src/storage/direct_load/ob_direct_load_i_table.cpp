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

#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_sstable.h"

namespace oceanbase
{
namespace storage
{
DEFINE_ENUM_FUNC(ObDirectLoadTableType::Type, type, OB_DIRECT_LOAD_TABLE_TYPE_DEF, ObDirectLoadTableType::);

ObDirectLoadTableHandle::ObDirectLoadTableHandle() : table_(nullptr), table_mgr_(nullptr) {}

ObDirectLoadTableHandle::~ObDirectLoadTableHandle() { reset(); }

void ObDirectLoadTableHandle::reset()
{
  if (is_valid()) {
    int64_t ret_count = table_->dec_ref();
    if (0 == ret_count) {
      table_mgr_->release_table(table_);
    }
    table_ = nullptr;
    table_mgr_ = nullptr;
  }
}

ObDirectLoadTableHandle::ObDirectLoadTableHandle(const ObDirectLoadTableHandle &other)
  : table_(nullptr), table_mgr_(nullptr)
{
  *this = other;
}

ObDirectLoadTableHandle &ObDirectLoadTableHandle::operator=(const ObDirectLoadTableHandle &other)
{
  if (this != &other) {
    reset();
    if (other.is_valid()) {
      table_ = other.table_;
      table_mgr_ = other.table_mgr_;
      table_->inc_ref();
    }
  }
  return *this;
}

int ObDirectLoadTableHandle::set_table(ObDirectLoadITable *table,
                                       ObDirectLoadTableManager *table_mgr)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_ISNULL(table) || OB_ISNULL(table_mgr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(table), KP(table_mgr));
  } else {
    table->inc_ref();
    table_ = table;
    table_mgr_ = table_mgr;
  }
  return ret;
}

ObDirectLoadTableHandleArray::ObDirectLoadTableHandleArray() { tables_.set_tenant_id(MTL_ID()); }

ObDirectLoadTableHandleArray::~ObDirectLoadTableHandleArray() { reset(); }

void ObDirectLoadTableHandleArray::reset() { tables_.reset(); }

int ObDirectLoadTableHandleArray::assign(const ObDirectLoadTableHandleArray &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(tables_.assign(other.tables_))) {
    LOG_WARN("fail to assign tables", KR(ret));
  }
  return ret;
}

int ObDirectLoadTableHandleArray::add(const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_handle));
  } else if (OB_FAIL(tables_.push_back(table_handle))) {
    LOG_WARN("fail to add table", KR(ret));
  }
  return ret;
}

int ObDirectLoadTableHandleArray::add(const ObDirectLoadTableHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  if (tables_handle.empty()) {
  } else if (OB_FAIL(tables_.push_back(tables_handle.tables_))) {
    LOG_WARN("fail to add tables", KR(ret));
  }
  return ret;
}

int ObDirectLoadTableHandleArray::get_table(int64_t idx,
                                            ObDirectLoadTableHandle &table_handle) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx >= tables_.count() || idx < 0)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("idx overflow", KR(ret), K(idx), K(tables_.count()));
  } else {
    table_handle = tables_.at(idx);
  }
  return ret;
}

ObDirectLoadTableManager::ObDirectLoadTableManager() : is_inited_(false) {}

ObDirectLoadTableManager::~ObDirectLoadTableManager() {}

int ObDirectLoadTableManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadTableManager init twice", KR(ret), KP(this));
  } else {
#define INIT_TABLE_ALLOCATOR(type, classType, name, shortName)                                   \
  if (OB_SUCC(ret)) {                                                                            \
    if (OB_FAIL(allocators_[type].init(sizeof(classType), "TLD_" shortName "Pool", MTL_ID()))) { \
      LOG_WARN("fail to init allocator", KR(ret));                                               \
    }                                                                                            \
  }

    OB_DIRECT_LOAD_TABLE_DEF(INIT_TABLE_ALLOCATOR);

#undef INIT_TABLE_ALLOCATOR

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

#define DEFINE_OBJ_ALLOC_IMPL(type, ValueType, name, shortName)                     \
  int ObDirectLoadTableManager::alloc_##name(ObDirectLoadTableHandle &table_handle) \
  {                                                                                 \
    int ret = OB_SUCCESS;                                                           \
    if (IS_NOT_INIT) {                                                              \
      ret = OB_NOT_INIT;                                                            \
      LOG_WARN("not init", K(ret));                                                 \
    } else {                                                                        \
      ValueType *value = nullptr;                                                   \
      void *ptr;                                                                    \
      if (OB_ISNULL(ptr = allocators_[type].alloc())) {                             \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                            \
        LOG_WARN("fail to alloc table ctx", KR(ret));                               \
      } else if (FALSE_IT(value = new (ptr) ValueType())) {                         \
      } else if (OB_FAIL(table_handle.set_table(value, this))) {                    \
        LOG_WARN("fail to set table", KR(ret));                                     \
      }                                                                             \
      if (OB_FAIL(ret)) {                                                           \
        if (nullptr != value) {                                                     \
          value->~ValueType();                                                      \
          allocators_[type].free(value);                                            \
          value = nullptr;                                                          \
        }                                                                           \
      }                                                                             \
    }                                                                               \
    return ret;                                                                     \
  }

OB_DIRECT_LOAD_TABLE_DEF(DEFINE_OBJ_ALLOC_IMPL);

#undef DEFINE_OBJ_ALLOC_IMPL

void ObDirectLoadTableManager::release_table(ObDirectLoadITable *table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table ||
                  !ObDirectLoadTableType::is_type_valid(table->get_table_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(table));
  } else {
    const ObDirectLoadTableType::Type table_type = table->get_table_type();
    table->~ObDirectLoadITable();
    allocators_[table_type].free(table);
  }
}

} // namespace storage
} // namespace oceanbase