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

#ifndef OCEANBASE_COMMON_OB_ALL_VIRTUAL_TABLE_PROJECTOR_H_
#define OCEANBASE_COMMON_OB_ALL_VIRTUAL_TABLE_PROJECTOR_H_

#include "lib/container/ob_iarray.h"
#include "common/object/ob_object.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_schema_getter_guard.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace common
{
class ObVirtualTableProjector : public common::ObVirtualTableScannerIterator
{
public:
  ObVirtualTableProjector() {}
  virtual ~ObVirtualTableProjector() {}
protected:
  struct Column {
    Column() : column_id_(common::OB_INVALID_ID), column_value_() {}

    TO_STRING_KV(K_(column_id), K_(column_value));

    uint64_t column_id_;
    common::ObObj column_value_;
  };
  virtual int project_row(const common::ObIArray<Column> &columns, common::ObNewRow &row,
                          const bool full_columns = true);
  virtual int check_column_exist(const share::schema::ObTableSchema *table,
                                 const char* column_name,
                                 bool &exist) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObVirtualTableProjector);
};

#define ADD_TEXT_COLUMN(type, table_schema, column_name, column_value, columns) \
  do { \
    Column column; \
    const ObColumnSchemaV2 *column_schema = NULL; \
    if (OB_FAIL(ret)) { \
    } else if (NULL == table_schema || NULL == column_name) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN("invalid argument", KP(table_schema), KP(column_name), K(ret)); \
    } else if (NULL == (column_schema = table_schema->get_column_schema(column_name))) { \
      LOG_WARN("column not exist, ignore this column", K(column_name), K(ret)); \
    } else { \
      column.column_value_.set_lob_value(type, column_value.ptr(), column_value.length()); \
      column.column_value_.set_collation_type(column_schema->get_collation_type()); \
      column.column_id_ = column_schema->get_column_id(); \
      if (OB_FAIL(columns.push_back(column))) { \
        LOG_WARN("push_back failed", K(ret)); \
      } \
    } \
  } while (false)

#define ADD_COLUMN(set_value_func, table_schema, column_name, column_value, columns) \
  do { \
    Column column; \
    const ObColumnSchemaV2 *column_schema = NULL; \
    if (OB_FAIL(ret)) { \
    } else if (NULL == table_schema || NULL == column_name) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN("invalid argument", KP(table_schema), KP(column_name), K(ret)); \
    } else if (NULL == (column_schema = table_schema->get_column_schema(column_name))) { \
      LOG_WARN("column not exist, ignore this column", K(column_name), K(ret)); \
    } else { \
      column.column_value_.set_value_func(column_value); \
      column.column_value_.set_collation_type(column_schema->get_collation_type()); \
      column.column_id_ = column_schema->get_column_id(); \
      if (OB_FAIL(columns.push_back(column))) { \
        LOG_WARN("push_back failed", K(ret)); \
      } \
    } \
  } while (false)

#define ADD_NULL_COLUMN(table_schema, column_name, columns) \
  do { \
    Column column; \
    const ObColumnSchemaV2 *column_schema = NULL; \
    if (OB_FAIL(ret)) { \
    } else if (NULL == table_schema || NULL == column_name) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN("invalid argument", KP(table_schema), KP(column_name), K(ret)); \
    } else if (NULL == (column_schema = table_schema->get_column_schema(column_name))) { \
      LOG_WARN("column not exist, ignore this column", K(column_name), K(ret)); \
    } else { \
      column.column_value_.set_null(); \
      column.column_id_ = column_schema->get_column_id(); \
      if (OB_FAIL(columns.push_back(column))) { \
        LOG_WARN("push_back failed", K(ret)); \
      } \
    } \
  } while (false)

class ObSimpleVirtualTableIterator: public ObVirtualTableProjector
{
public:
  ObSimpleVirtualTableIterator(uint64_t tenant_id, uint64_t table_id);
  virtual ~ObSimpleVirtualTableIterator() {}

  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

  // interface for subclass:
  int init(share::schema::ObMultiVersionSchemaService *schema_service);
  virtual int get_next_full_row(const share::schema::ObTableSchema *table,
                            common::ObIArray<Column> &columns) = 0;
  virtual int init_all_data() { return common::OB_SUCCESS; }
private:
  // types and constants
  static const int64_t MAX_COLUMN_NUM = 32;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSimpleVirtualTableIterator);
  // function members
  int get_table_schema(uint64_t tenant_id, uint64_t table_id);
private:
  // data members
  uint64_t tenant_id_;
  uint64_t table_id_;
  common::ObSEArray<Column, MAX_COLUMN_NUM> columns_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  const share::schema::ObTableSchema *table_schema_;
};

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_ALL_VIRTUAL_TABLE_PROJECTOR_H_
