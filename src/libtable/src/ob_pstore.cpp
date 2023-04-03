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

#define USING_LOG_PREFIX CLIENT
#include "ob_pstore.h"
#include "ob_hkv_table.h"
#include "ob_table_service_client.h"

using namespace oceanbase::common;
using namespace oceanbase::table;
ObPStore::ObPStore()
    :inited_(false),
     client_(NULL)
{}

ObPStore::~ObPStore()
{
  destroy();
}

int ObPStore::init(ObTableServiceClient &client)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    client_ = &client;
    inited_ = true;
  }
  return ret;
}

void ObPStore::destroy()
{
  client_ = NULL;
  inited_ = false;
}

#define KV_TABLE_BEGIN()\
  ObHKVTable *kv_table = NULL;\
  char name_buff[256];\
  ret = databuff_printf(name_buff, 256, "%.*s_%.*s", table_name.length(), table_name.ptr(),\
                        column_family.length(), column_family.ptr());\
  if (OB_FAIL(ret)) {\
    LOG_WARN("failed to cons table name", K(ret));\
  } else if (OB_UNLIKELY(!inited_)) {\
    ret = OB_NOT_INIT;\
  } else if (OB_FAIL(client_->alloc_hkv_table(ObString::make_string(name_buff), kv_table))) {\
    LOG_WARN("failed to alloc kvtable", K(ret));\
  }

#define KV_TABLE_END()\
  if (NULL != kv_table) {\
    client_->free_hkv_table(kv_table);\
    kv_table = NULL;\
  }

int ObPStore::get(const ObString &table_name, const ObString &column_family, const ObHKVTable::Key &key, ObHKVTable::Value &value)
{
  int ret = OB_SUCCESS;
  KV_TABLE_BEGIN();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(kv_table->get(key, value))) {
    LOG_WARN("failed to get kv table", K(ret), K(key));
  } else {
    LOG_DEBUG("get succ", K(table_name), K(column_family), K(key), K(value));
  }
  KV_TABLE_END();
  return ret;
}

int ObPStore::put(const ObString &table_name, const ObString &column_family, const ObHKVTable::Key &key, const ObHKVTable::Value &value)
{
  int ret = OB_SUCCESS;
  KV_TABLE_BEGIN();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(kv_table->put(key, value))) {
    LOG_WARN("failed to get kv table", K(ret), K(key));
  } else {
    LOG_DEBUG("put succ", K(table_name), K(column_family), K(key), K(value));
  }
  KV_TABLE_END();
  return ret;
}

int ObPStore::remove(const ObString &table_name, const ObString &column_family, const ObHKVTable::Key &key)
{
  int ret = OB_SUCCESS;
  KV_TABLE_BEGIN();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(kv_table->remove(key))) {
    LOG_WARN("failed to get kv table", K(ret), K(key));
  } else {
    LOG_DEBUG("remove succ", K(table_name), K(column_family), K(key));
  }
  KV_TABLE_END();
  return ret;
}

int ObPStore::multi_get(const ObString &table_name, const ObString &column_family, const ObHKVTable::IKeys &keys, ObHKVTable::IValues &values)
{
  int ret = OB_SUCCESS;
  KV_TABLE_BEGIN();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(kv_table->multi_get(keys, values))) {
    LOG_WARN("failed to multi_get", K(ret));
  } else {
    LOG_DEBUG("multi_get succ", K(table_name), K(column_family), "count", keys.count());
  }
  KV_TABLE_END();
  return ret;
}

int ObPStore::multi_put(const ObString &table_name, const ObString &column_family, const ObHKVTable::IKeys &keys, const ObHKVTable::IValues &values)
{
  int ret = OB_SUCCESS;
  KV_TABLE_BEGIN();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(kv_table->multi_put(keys, values))) {
    LOG_WARN("failed to multi_put", K(ret));
  } else {
    LOG_DEBUG("multi_put succ", K(table_name), K(column_family), "count", keys.count());
  }
  KV_TABLE_END();
  return ret;
}

int ObPStore::multi_put(const ObString &table_name, const ObString &column_family, ObHKVTable::IEntities &entities)
{
  int ret = OB_SUCCESS;
  KV_TABLE_BEGIN();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(kv_table->multi_put(entities))) {
    LOG_WARN("failed to multi_put", K(ret));
  } else {
    LOG_DEBUG("multi_put succ", K(table_name), K(column_family), "count", entities.count());
  }
  KV_TABLE_END();
  return ret;
}

int ObPStore::multi_remove(const ObString &table_name, const ObString &column_family, const ObHKVTable::IKeys &keys)
{
  int ret = OB_SUCCESS;
  KV_TABLE_BEGIN();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(kv_table->multi_remove(keys))) {
    LOG_WARN("failed to multi_remove", K(ret));
  } else {
    LOG_DEBUG("multi_remove succ", K(table_name), K(column_family), "count", keys.count());
  }
  KV_TABLE_END();
  return ret;
}
