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
#include "ob_htable.h"
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::table::hbase;
int ObHTable::put(const ObHPut &put)
{
  int ret = OB_SUCCESS;
  int64_t N = put.get_column_count();
  ObHKVTable::Entities entities;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    ObHCell cell;
    put.get_column(i, cell);

    ObHKVTable::Key key;
    key.rowkey_ = put.get_row();
    // @todo ignore cell.get_column_family();
    key.column_qualifier_ = cell.get_column_qualifier();
    key.version_ = cell.get_timestamp();
    ObHKVTable::Value value;
    value.set_varbinary(cell.get_value());
    OHKVTable::Entity entity;
    entity.set_key(key);
    entity.set_value(value);

    entities.push_back(entity);
  } // end for
  ret = hkv_table_->multi_put(entities);
  return ret;
}

int ObHTable::multi_put(const ObIArray<ObHPut> &puts)
{
  int ret = OB_SUCCESS;
  const int64_t N = puts.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {

  } // end for
  return ret;
}

int ObHTable::del(const ObHDelete &del)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObHTable::multi_del(const ObIArray<ObHDelete> &deletes);
int ObHTable::mutate_row(const ObHRowMutations &row_mutations);

// with timestamp range or no timestamp
int ObHTable::get_by_mscan_with_time_range(const ObHGet &get)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;

  query.add_select_column(ObHKVTable::CQ_CNAME_STR);
  query.add_select_column(ObHKVTable::VERSION_CNAME_STR);
  query.add_select_column(ObHKVTable::VALUE_CNAME_STR);
  query.set_scan_order(ObQueryFlag::Forward);

  ObHTableFilter htable_filter;
  htable_filter.set_max_versions(scan.get_max_versions());
  htable_filter.set_limit(scan.get_limit());

  const int64_t N = get.get_column_count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    htable_filter.add_column(scan.get_qualifier(i));
  }
  query.set_htable_filter(htable_filter);

  ObObj pk_objs_start[3];
  pk_objs_start[0].set_varbinary(get.get_row());
  pk_objs_start[1].set_min_value();
  pk_objs_start[2].set_min_value();
  ObObj pk_objs_end[3];
  pk_objs_end[0].set_varbinary(get.get_row());
  pk_objs_start[1].set_max_value();
  pk_objs_start[2].set_max_value();
  ObNewRange range;
  range.start_key_.assign(pk_objs_start, 3);
  range.end_key_.assign(pk_objs_end, 3);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  query.add_scan_range(range);

  ObTableEntityIterator *iter = nullptr;
  ret = hkv_table_->execute_query(query, iter);
  return ret;
}

// with specific timestamp
int ObHTable::get_by_mget(const ObHGet &get)
{
  int ret = OB_SUCCESS;
  int64_t N = get.get_column_count();
  ObHKVTable::Keys keys;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    ObHCell cell;
    get.get_column(i, cell);

    ObHKVTable::Key key;
    key.rowkey_ = put.get_row();
    // @todo ignore cell.get_column_family();
    key.column_qualifier_ = cell.get_column_qualifier();
    key.version_ = cell.get_timestamp();
    keys.push_back(entity);
  } // end for
  ObHKVTable::Values values;
  ret = hkv_table_->multi_get(keys, values);
  return ret;
}

int ObHTable::multi_get(const ObIArray<ObHGet> &gets);
int ObHTable::scan(const ObHScan &scan)
{
  int ret = OB_SUCCESS;

  ObTableQuery query;

  query.add_select_column(ObHKVTable::ROWKEY_CNAME_STR);
  query.add_select_column(ObHKVTable::CQ_CNAME_STR);
  query.add_select_column(ObHKVTable::VERSION_CNAME_STR);
  query.add_select_column(ObHKVTable::VALUE_CNAME_STR);

  query.set_scan_order(scan.reversed() ? ObQueryFlag::Reversed : ObQueryFlag::Forward);

  ObHTableFilter htable_filter;
  htable_filter.set_max_versions(scan.get_max_versions());
  htable_filter.set_limit(scan.get_limit());
  const int64_t N = scan.get_column_count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    htable_filter.add_column(scan.get_qualifier(i));
  }
  query.set_htable_filter(htable_filter);

  ObObj pk_objs_start[3];
  pk_objs_start[0].set_varbinary(scan.get_start_row());
  pk_objs_start[1].set_min_value();
  pk_objs_start[2].set_min_value();
  ObObj pk_objs_end[3];
  pk_objs_end[0].set_varbinary(scan.get_stop_row());
  pk_objs_start[1].set_max_value();
  pk_objs_start[2].set_max_value();
  ObNewRange range;
  range.start_key_.assign(pk_objs_start, 3);
  range.end_key_.assign(pk_objs_end, 3);

  if (scan.include_start_row()) {
    range.border_flag_.set_inclusive_start();
  } else {
    range.border_flag_.unset_inclusive_start();
  }
  if (scan.include_stop_row()) {
    range.border_flag_.set_inclusive_end();
  } else {
    range.border_flag_.unset_inclusive_end();
  }
  query.add_scan_range(range);
  query.set_batch(scan.get_batch());

  ObTableEntityIterator *iter = nullptr;
  ret = hkv_table_->execute_query(query, iter);
  return ret;
}
