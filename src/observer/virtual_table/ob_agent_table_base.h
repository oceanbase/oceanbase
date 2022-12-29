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

#ifndef OCEANBASE_VIRTUAL_TABLE_OB_AGENT_TABLE_BASE_H_
#define OCEANBASE_VIRTUAL_TABLE_OB_AGENT_TABLE_BASE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace common
{
class ObVTableScanParam;
class ObSqlString;
}

namespace share
{
namespace schema
{
class ObTableSchema;
class ObSchemaGetterGuard;
class ObColumnSchemaV2;
}
}
namespace observer
{

class ObInnerSQLResult;

class ObAgentTableBase : public common::ObVirtualTableScannerIterator
{
public:
  typedef int (*convert_func_t)(const common::ObObj &src, common::ObObj &dst, common::ObIAllocator &);
  struct MapItem
  {
    MapItem() : base_col_name_(), convert_func_(NULL), combine_tenant_id_(false) {}
    TO_STRING_KV(K(base_col_name_),
                 KP(convert_func_),
                 "combine_tenant_id_", combine_tenant_id_ ? "true" : "false");

    common::ObString base_col_name_;
    convert_func_t convert_func_;
    bool combine_tenant_id_;
  };

  ObAgentTableBase();
  virtual ~ObAgentTableBase();

  int init(
      const uint64_t base_tenant_id,
      const uint64_t base_table_id,
      const share::schema::ObTableSchema *index_table,
      const ObVTableScanParam &scan_param);

  virtual int inner_open() override
  {
    // do open work in do_open(), delay to get_next_row() to avoid unnecessary sql execution.
    return common::OB_SUCCESS;
  }
  virtual int do_open();
  virtual int inner_close() override;

  void destroy();

protected:
  int build_base_table_mapping();

  // init map item, which column not exist in base table.
  virtual int init_non_exist_map_item(MapItem &item, const share::schema::ObColumnSchemaV2 &col) = 0;

  // set convert function for object convert. (do nothing if conversion needed)
  virtual int set_convert_func(convert_func_t &func,
      const share::schema::ObColumnSchemaV2 &col, const share::schema::ObColumnSchemaV2 &base_col);

  // add extra condition to sql.
  virtual int add_extra_condition(common::ObSqlString &sql);

  // set initial rowkey condition when converting key ranges to sql conditions.
  virtual int setup_inital_rowkey_condition(
      common::ObSqlString &cols, common::ObSqlString &vals);

  int construct_sql(const uint64_t exec_tenant_id, common::ObSqlString &sql);
  int construct_columns(
      const uint64_t exec_tenant_id,
      common::ObSqlString &sql);
  int cast_as_default_value(
      const bool first_column,
      char *buf,
      const int64_t buf_len,
      const common::ObString &name,
      const common::ObString &col_name,
      common::ObSqlString &sql);
  // convert scan_param_->key_ranges_ to sql conditions
  int append_sql_condition(common::ObSqlString &sql);
  int rowkey2condition(common::ObSqlString &cols,  common::ObSqlString &vals,
      const common::ObRowkey &rowkey, const common::ObRowkeyInfo &rowkey_info);
  int append_sql_orderby(common::ObSqlString &sql);

  virtual int change_column_value(const MapItem &item,
                                  ObIAllocator &allocator,
                                  ObObj &new_value);

  virtual int get_base_tenant_id() const { return base_tenant_id_; }

protected:
  // Allocator used in inner_open() to inner_close() duration.
  common::ObArenaAllocator inner_allocator_;
  // save value to %allocator_bak_ before switch allocator_ to %inner_allocator_
  common::ObIAllocator *allocator_bak_;
  uint64_t base_tenant_id_;
  uint64_t base_table_id_;
  const share::schema::ObTableSchema *base_table_;

  // %table_schema_ defined in ObVirtualTableIterator is schema of this table.
  const share::schema::ObTableSchema *index_table_;
  const ObVTableScanParam *scan_param_;

  // base table's primary key/index may be suffix of agent table's primary key/index.
  // %base_rowkey_offset_ is the suffix offset.
  int64_t base_rowkey_offset_;


  common::ObMySQLProxy::MySQLResult *sql_res_;
  ObInnerSQLResult *inner_sql_res_;

  // mapping column id to base table's column name
  common::ObArray<MapItem, common::ObWrapperAllocator> mapping_;

  common::ObArenaAllocator convert_alloc_;
  bool opened_;
};

} // end namespace observer
} // end namespace oceanbase


#endif // OCEANBASE_VIRTUAL_TABLE_OB_AGENT_TABLE_BASE_H_
