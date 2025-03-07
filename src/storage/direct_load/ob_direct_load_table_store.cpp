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

#define USING_LOG_PREFIX SERVER

#include "storage/direct_load/ob_direct_load_table_store.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
ObDirectLoadTableStore::ObDirectLoadTableStore()
  : allocator_("TLD_TableStore"),
    table_data_desc_(),
    table_type_(ObDirectLoadTableType::INVALID_TABLE_TYPE)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadTableStore::~ObDirectLoadTableStore()
{
  clear();
  tablet_table_map_.destroy();
}

void ObDirectLoadTableStore::clear()
{
  table_data_desc_.reset();
  table_type_ = ObDirectLoadTableType::INVALID_TABLE_TYPE;
  FOREACH(it, tablet_table_map_)
  {
    ObDirectLoadTableHandleArray *table_handle_array = it->second;
    table_handle_array->~ObDirectLoadTableHandleArray();
    allocator_.free(table_handle_array);
  }
  tablet_table_map_.reuse();
  allocator_.reset();
}

int ObDirectLoadTableStore::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  if (OB_FAIL(tablet_table_map_.create(bucket_num, "TLD_TableMap", "TLD_TableMap", MTL_ID()))) {
    LOG_WARN("fail to init hash map", KR(ret));
  }
  return ret;
}

bool ObDirectLoadTableStore::is_valid() const
{
  return table_data_desc_.is_valid() && ObDirectLoadTableType::is_type_valid(table_type_);
}

int ObDirectLoadTableStore::get_tablet_tables(const ObTabletID &tablet_id,
                                              ObDirectLoadTableHandleArray *&table_handle_array)
{
  int ret = OB_SUCCESS;
  table_handle_array = nullptr;
  if (OB_FAIL(tablet_table_map_.get_refactored(tablet_id, table_handle_array))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("fail to get", KR(ret));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObDirectLoadTableStore::get_or_create_tablet_tables(
  const ObTabletID &tablet_id, ObDirectLoadTableHandleArray *&table_handle_array)
{
  int ret = OB_SUCCESS;
  table_handle_array = nullptr;
  if (OB_FAIL(tablet_table_map_.get_refactored(tablet_id, table_handle_array))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("fail to get", KR(ret));
    } else {
      ret = OB_SUCCESS;
      if (OB_ISNULL(table_handle_array = OB_NEWx(ObDirectLoadTableHandleArray, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadTableHandleArray", KR(ret));
      } else if (OB_FAIL(tablet_table_map_.set_refactored(tablet_id, table_handle_array))) {
        LOG_WARN("fail to create", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != table_handle_array) {
          table_handle_array->~ObDirectLoadTableHandleArray();
          allocator_.free(table_handle_array);
          table_handle_array = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadTableStore::add_table(const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table store invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!table_handle.is_valid() ||
                         table_handle.get_table()->get_table_type() != table_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_type_), K(table_handle));
  } else {
    const ObTabletID &tablet_id = table_handle.get_table()->get_tablet_id();
    ObDirectLoadTableHandleArray *tablet_table_array = nullptr;
    if (OB_FAIL(get_or_create_tablet_tables(tablet_id, tablet_table_array))) {
      LOG_WARN("fail to get or create tablet tables", KR(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_table_array->add(table_handle))) {
      LOG_WARN("fail to add table", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTableStore::add_tables(const ObDirectLoadTableHandleArray &table_handle_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_handle_array.count(); ++i) {
    if (OB_FAIL(add_table(table_handle_array.at(i)))) {
      LOG_WARN("fail to add table", KR(ret), K(i));
    }
  }
  return ret;
}

int ObDirectLoadTableStore::add_tablet_tables(
  const ObTabletID &tablet_id, const ObDirectLoadTableHandleArray &table_handle_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table store invalid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(table_handle_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_handle_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_handle_array.count(); ++i) {
      const ObDirectLoadTableHandle &table_handle = table_handle_array.at(i);
      if (OB_UNLIKELY(!table_handle.is_valid() ||
                      table_handle.get_table()->get_table_type() != table_type_ ||
                      table_handle.get_table()->get_tablet_id() != tablet_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", KR(ret), K(table_type_), K(tablet_id), K(i), K(table_handle));
      }
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadTableHandleArray *tablet_table_array = nullptr;
      if (OB_FAIL(get_or_create_tablet_tables(tablet_id, tablet_table_array))) {
        LOG_WARN("fail to get or create tablet tables", KR(ret), K(tablet_id));
      } else if (OB_FAIL(tablet_table_array->add(table_handle_array))) {
        LOG_WARN("fail to add tables", KR(ret));
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObDirectLoadTableStore)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_data_desc));
  J_KV("table_type", ObDirectLoadTableType::get_type_string(table_type_));
  J_COMMA();
  J_NAME("tablet_table_map");
  J_COLON();
  if (!tablet_table_map_.created()) {
    J_NOP();
  } else if (tablet_table_map_.empty()) {
    J_EMPTY_OBJ();
  } else {
    // { xxx : [] }
    J_OBJ_START();
    FOREACH(it, tablet_table_map_)
    {
      const ObTabletID &tablet_id = it->first;
      ObDirectLoadTableHandleArray *table_handle_array = it->second;
      BUF_PRINTO(tablet_id.id());
      J_COLON();
      J_ARRAY_START();
      for (int64_t i = 0; i < table_handle_array->count(); ++i) {
        BUF_PRINTO(table_handle_array->at(i));
        J_COMMA();
      }
      J_ARRAY_END();
    }
    J_OBJ_END();
  }
  J_OBJ_END();
  return pos;
}

} // namespace storage
} // namespace oceanbase
