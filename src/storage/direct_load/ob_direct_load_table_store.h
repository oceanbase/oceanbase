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

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTableStore
{
public:
  typedef hash::ObHashMap<common::ObTabletID, ObDirectLoadTableHandleArray *> TabletTableMap;
  typedef typename TabletTableMap::iterator iterator;
  typedef typename TabletTableMap::const_iterator const_iterator;
  ObDirectLoadTableStore();
  ~ObDirectLoadTableStore();

  void clear();
  int init();
  bool is_valid() const;

#define DEFINE_TABLE_TYPE_INTERFACE(type, classType, name, shortName) \
  void set_##name() { table_type_ = type; }                           \
  bool is_##name() const { return ObDirectLoadTableType::is_##name(table_type_); }

  OB_DIRECT_LOAD_TABLE_DEF(DEFINE_TABLE_TYPE_INTERFACE);

#undef DEFINE_TABLE_TYPE_INTERFACE

  int get_tablet_tables(const ObTabletID &tablet_id,
                        ObDirectLoadTableHandleArray *&table_handle_array);
  // 添加table之前需要先设置 table_data_desc_, table_type_
  int add_table(const ObDirectLoadTableHandle &table_handle);
  int add_tables(const ObDirectLoadTableHandleArray &table_handle_array);
  int add_tablet_tables(const ObTabletID &tablet_id,
                        const ObDirectLoadTableHandleArray &table_handle_array);

  void set_table_data_desc(const ObDirectLoadTableDataDesc &table_data_desc)
  {
    table_data_desc_ = table_data_desc;
  }
  const ObDirectLoadTableDataDesc &get_table_data_desc() const { return table_data_desc_; }

  bool empty() const { return tablet_table_map_.empty(); }
  int64_t size() const { return tablet_table_map_.size(); }
  iterator begin() { return tablet_table_map_.begin(); }
  const_iterator begin() const { return tablet_table_map_.begin(); }
  iterator end() { return tablet_table_map_.end(); }
  const_iterator end() const { return tablet_table_map_.end(); }

  DECLARE_TO_STRING;

private:
  int get_or_create_tablet_tables(const ObTabletID &tablet_id,
                                  ObDirectLoadTableHandleArray *&table_handle_array);

private:
  ObArenaAllocator allocator_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadTableType::Type table_type_;
  TabletTableMap tablet_table_map_;
};

} // namespace storage
} // namespace oceanbase
