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

#ifndef OB_TABLE_DML_PARAM_H_
#define OB_TABLE_DML_PARAM_H_

#include "ob_schema_struct.h"
#include "ob_table_schema.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObColumnParam;
class ObTableSchemaParam {
  OB_UNIS_VERSION_V(1);

public:
  typedef common::ObFixedArray<common::ObRowkeyInfo*, common::ObIAllocator> RowKeys;
  typedef common::ObFixedArray<ObColumnParam*, common::ObIAllocator> Columns;

  explicit ObTableSchemaParam(common::ObIAllocator& allocator);
  virtual ~ObTableSchemaParam();
  void reset();
  int convert(const ObTableSchema* schema, const common::ObIArray<uint64_t>& col_ids);
  OB_INLINE bool is_valid() const
  {
    return common::OB_INVALID_ID != table_id_;
  }
  OB_INLINE bool is_global_index_table() const
  {
    return ObSimpleTableSchemaV2::is_global_index_table(index_type_);
  }
  OB_INLINE uint64_t get_table_id() const
  {
    return table_id_;
  }
  OB_INLINE int64_t get_schema_version() const
  {
    return schema_version_;
  }
  OB_INLINE ObTableType get_table_type() const
  {
    return table_type_;
  }
  OB_INLINE ObIndexType get_index_type() const
  {
    return index_type_;
  }
  OB_INLINE ObIndexStatus get_index_status() const
  {
    return index_status_;
  }
  OB_INLINE bool is_dropped_schema() const
  {
    return is_dropped_schema_;
  }
  OB_INLINE int64_t get_rowkey_column_num() const
  {
    return rowkey_column_num_;
  }
  OB_INLINE int64_t get_shadow_rowkey_column_num() const
  {
    return shadow_rowkey_column_num_;
  }
  OB_INLINE int64_t get_fulltext_col_id() const
  {
    return fulltext_col_id_;
  }
  OB_INLINE int64_t get_column_count() const
  {
    return columns_.count();
  }
  OB_INLINE bool is_index_table() const
  {
    return ObTableSchema::is_index_table(table_type_);
  }
  OB_INLINE bool is_materialized_view() const
  {
    return ObTableSchema::is_materialized_view(table_type_);
  }
  OB_INLINE bool is_storage_index_table() const
  {
    return is_index_table() || is_materialized_view();
  }
  OB_INLINE bool can_read_index() const
  {
    return ObTableSchema::can_read_index(index_status_, is_dropped_schema_);
  }
  OB_INLINE bool is_unique_index() const
  {
    return ObTableSchema::is_unique_index(index_type_);
  }
  OB_INLINE bool is_domain_index() const
  {
    return ObTableSchema::is_domain_index(index_type_);
  }
  int is_rowkey_column(const uint64_t column_id, bool& is_rowkey) const;
  int is_column_nullable(const uint64_t column_id, bool& is_nullable) const;

  const ObColumnParam* get_column(const uint64_t column_id) const;
  const ObColumnParam* get_column_by_idx(const int64_t idx) const;
  const ObColumnParam* get_rowkey_column_by_idx(const int64_t idx) const;
  int get_rowkey_column_ids(common::ObIArray<ObColDesc>& column_ids) const;
  int get_index_name(common::ObString& index_name) const;
  const common::ObString& get_pk_name() const;
  bool is_depend_column(uint64_t column_id) const;
  DECLARE_TO_STRING;

private:
  ObTableSchemaParam();
  DISALLOW_COPY_AND_ASSIGN(ObTableSchemaParam);

private:
  common::ObIAllocator& allocator_;
  uint64_t table_id_;
  int64_t schema_version_;
  ObTableType table_type_;
  ObIndexType index_type_;
  ObIndexStatus index_status_;
  int64_t rowkey_column_num_;
  int64_t shadow_rowkey_column_num_;
  uint64_t fulltext_col_id_;
  common::ObString index_name_;
  Columns columns_;
  ColumnMap col_map_;
  bool is_dropped_schema_;
  common::ObString pk_name_;  // use for printing error msg in oracle mode
};

class ObTableDMLParam {
  OB_UNIS_VERSION_V(1);

public:
  typedef common::ObFixedArray<ObTableSchemaParam*, common::ObIAllocator> TableSchemas;
  typedef common::ObFixedArray<ObColDesc, common::ObIAllocator> ObColDescArray;

  explicit ObTableDMLParam(common::ObIAllocator& allocator);
  virtual ~ObTableDMLParam();
  void reset();
  int convert(const ObTableSchema* table_schema, const common::ObIArray<const ObTableSchema*>& index_schemas,
      const int64_t tenant_schema_version, const common::ObIArray<uint64_t>& column_ids);
  // storage param is generated from other param, they won't be serialized.
  // it is called in convert or after deserialization
  int prepare_storage_param(const common::ObIArray<uint64_t>& column_ids);
  OB_INLINE bool is_valid() const
  {
    return data_table_.is_valid();
  }
  OB_INLINE int64_t get_tenant_schema_version() const
  {
    return tenant_schema_version_;
  }
  OB_INLINE const ObTableSchemaParam& get_data_table() const
  {
    return data_table_;
  }
  OB_INLINE const TableSchemas& get_index_tables() const
  {
    return index_tables_;
  }
  OB_INLINE const ObColDescArray& get_col_descs() const
  {
    return col_descs_;
  }
  OB_INLINE const ColumnMap& get_col_map() const
  {
    return col_map_;
  }
  DECLARE_TO_STRING;

private:
  ObTableDMLParam();
  DISALLOW_COPY_AND_ASSIGN(ObTableDMLParam);

  int serialize_schemas(const TableSchemas& schemas, char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize_schemas(const char* buf, const int64_t data_len, int64_t& pos, TableSchemas& schemas);
  int get_schemas_serialize_size(const TableSchemas& schemas, int64_t& size) const;
  ObTableSchemaParam* alloc_schema();

private:
  common::ObIAllocator& allocator_;
  int64_t tenant_schema_version_;
  ObTableSchemaParam data_table_;
  TableSchemas index_tables_;

  // generated storage param from columns_ids_ in ObTableModify, for performance improvement
  ObColDescArray col_descs_;
  ColumnMap col_map_;
};
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* OB_TABLE_DML_PARAM_H_ */
