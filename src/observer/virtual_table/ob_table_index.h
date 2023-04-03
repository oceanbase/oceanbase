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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_INDEX_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_INDEX_

#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"
namespace oceanbase
{
namespace common
{
class ObString;
class ObObj;
}
namespace share
{
namespace schema
{
class ObTableSchema;
class ObDatabaseSchema;
}
}
namespace observer
{
class ObTableIndex : public common::ObVirtualTableScannerIterator
{
public:
  ObTableIndex();
  virtual ~ObTableIndex();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
private:

  int add_table_indexes(const share::schema::ObTableSchema &table_schema,
                        const common::ObString &database_name,
                        common::ObObj *cells,
                        int64_t col_count,
                        bool &is_end);
  int add_database_indexes(const share::schema::ObDatabaseSchema &database_schema,
                           common::ObObj *cells,
                           int64_t col_count,
                           bool &is_end);

  int add_rowkey_indexes(const share::schema::ObTableSchema &table_schema,
                         const common::ObString &database_name,
                         common::ObObj *cells,
                         int64_t col_count,
                         bool &is_end);
  int add_normal_indexes(const share::schema::ObTableSchema &table_schema,
                         const common::ObString &database_name,
                         common::ObObj *cells,
                         int64_t col_count,
                         bool &is_end);
  int add_normal_index_column(const common::ObString &database_name,
                              const share::schema::ObTableSchema &table_schema,
                              const share::schema::ObTableSchema *index_schema,
                              common::ObObj *cells,
                              int64_t col_count,
                              bool &is_end);
  int add_fulltext_index_column(const common::ObString &database_name,
                                const share::schema::ObTableSchema &table_schema,
                                const share::schema::ObTableSchema *index_schema,
                                common::ObObj *cells,
                                int64_t col_count,
                                const uint64_t column_id);
  int get_show_column_name(const share::schema::ObTableSchema &table_schema,
                           const share::schema::ObColumnSchemaV2 &column_schema,
                           common::ObString &column_name);
private:
  uint64_t tenant_id_;
  uint64_t show_table_id_;
  common::ObSArray<const share::schema::ObDatabaseSchema *> database_schemas_;
  int64_t database_schema_idx_;
  common::ObSArray<const share::schema::ObTableSchema *> table_schemas_;
  int64_t table_schema_idx_;
  int64_t rowkey_info_idx_;
  int64_t index_tid_array_idx_;
  int64_t index_column_idx_;
  common::ObSArray<share::schema::ObAuxTableMetaInfo> simple_index_infos_;
  bool is_rowkey_end_;
  bool is_normal_end_;
  int64_t ft_dep_col_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObTableIndex);
};
}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_INDEX_ */
