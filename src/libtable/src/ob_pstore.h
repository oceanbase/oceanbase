/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_PSTORE_H
#define _OB_PSTORE_H 1
#include "ob_hkv_table.h"
namespace oceanbase
{
namespace table
{
/// Interface for PStore.
class ObPStore
{
public:
  ObPStore();
  virtual ~ObPStore();

  int init(ObTableServiceClient &client);
  void destroy();

  int get(const ObString &table_name, const ObString &column_family, const ObHKVTable::Key &key, ObHKVTable::Value &value);
  int multi_get(const ObString &table_name, const ObString &column_family, const ObHKVTable::IKeys &keys, ObHKVTable::IValues &values);

  int put(const ObString &table_name, const ObString &column_family, const ObHKVTable::Key &key, const ObHKVTable::Value &value);
  int multi_put(const ObString &table_name, const ObString &column_family, const ObHKVTable::IKeys &keys, const ObHKVTable::IValues &values);
  int multi_put(const ObString &table_name, const ObString &column_family, ObHKVTable::IEntities &entities);

  int remove(const ObString &table_name, const ObString &column_family, const ObHKVTable::Key &key);
  int multi_remove(const ObString &table_name, const ObString &column_family, const ObHKVTable::IKeys &keys);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPStore);
private:
  // data members
  bool inited_;
  ObTableServiceClient *client_;
};

/**
 * @example ob_pstore_example.cpp
 * This is an example of how to use the ObPStore class.
 *
 */
} // end namespace table
} // end namespace oceanbase

#endif /* _OB_PSTORE_H */
