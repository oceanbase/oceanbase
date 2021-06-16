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

#ifndef OB_TABLE_PARAM_H_
#define OB_TABLE_PARAM_H_

#include <stdint.h>
#include "share/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/object/ob_object.h"

namespace oceanbase {
namespace share {
namespace schema {
// A customized hash map to store schema information in plan
// 1. Key && value type is fixed
// 2. Memory is managed by the pass in allocator, doesn't release inside
// 3. Can not be reused
class ColumnHashMap {
  struct HashNode {
    uint64_t key_;
    int32_t value_;
    struct HashNode* next_;
  };

public:
  ColumnHashMap(common::ObIAllocator& allocator)
      : allocator_(allocator), bucket_num_(0), buckets_(NULL), is_inited_(false)
  {}
  virtual ~ColumnHashMap()
  {}  // not free memory
  int clear();
  int init(const int64_t bucket_num);
  int set(const uint64_t key, const int32_t value);
  int get(const uint64_t key, int32_t& value) const;

private:
  ColumnHashMap();
  ColumnHashMap(const ColumnHashMap& other);
  ColumnHashMap& operator=(const ColumnHashMap& other);

private:
  bool is_inited() const
  {
    return is_inited_;
  }
  int find_node(const uint64_t key, HashNode* head, HashNode*& node) const;

public:
  common::ObIAllocator& allocator_;
  int64_t bucket_num_;
  HashNode** buckets_;
  bool is_inited_;
};

struct ObColDesc {
  int64_t to_string(char* buffer, const int64_t length) const
  {
    int64_t pos = 0;
    (void)common::databuff_printf(buffer, length, pos, "column_id=%lu ", col_id_);
    pos += col_type_.to_string(buffer + pos, length - pos);
    (void)common::databuff_printf(buffer, length, pos, " order=%d", col_order_);
    return pos;
  }
  int assign(const ObColDesc& other)
  {
    int ret = common::OB_SUCCESS;
    col_id_ = other.col_id_;
    col_type_ = other.col_type_;
    col_order_ = other.col_order_;
    return ret;
  }
  void reset()
  {
    col_id_ = common::OB_INVALID_ID;
    col_type_.reset();
    col_order_ = common::ObOrderType::ASC;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;
  uint64_t col_id_;
  common::ObObjMeta col_type_;
  common::ObOrderType col_order_;
};

/*
 * This class is for the mapping between column_id to offset.
 * It is constructed from the column array.
 * Minimum id of normal columns is 1.
 * Minimum id of shadow columns is OB_MIN_SHADOW_COLUMN_ID + 1.
 * 1. if max_column_id - offset < MAX_ARRAY_SIZE, use Array to store data, otherwise use HashMap
 * 2. Similar to normal columns, shadow columns is maintained in a separated Array or HashMap. So
 *    totally the class contains for struct: array_, map_, shadow_array_, shadow_map_
 * 3. Array and HashMap is constructed by requirements. By default, an unused array or hashmap won't
 *    be created, i.e. no additional memory would be reserved.
 * 4. If use Array, array_size = max_columm_id - offset + 1;
 *    If use HashMap, buckets_num = MAX_ARRAY_SIZE
 */
class ObColumnParam;
class ColumnMap {
  const static int64_t MAX_ARRAY_SIZE = common::OB_ROW_MAX_COLUMNS_COUNT;
  const static int64_t COLUMN_ID_OFFSET = 1;
  const static int64_t SHADOW_COLUMN_ID_OFFSET = common::OB_MIN_SHADOW_COLUMN_ID + 1;
  const static int64_t MAX_COLUMN_ID_USING_ARRAY = COLUMN_ID_OFFSET + MAX_ARRAY_SIZE - 1;
  const static int64_t MAX_SHADOW_COLUMN_ID_USING_ARRAY = SHADOW_COLUMN_ID_OFFSET + MAX_ARRAY_SIZE - 1;

  typedef common::ObFixedArray<int32_t, common::ObIAllocator> ColumnArray;
#define IS_SHADOW_COLUMN(column_id) (column_id >= OB_MIN_SHADOW_COLUMN_ID)

public:
  ColumnMap(common::ObIAllocator& allocator)
      : array_(allocator),
        shadow_array_(allocator),
        map_(allocator),
        shadow_map_(allocator),
        use_array_(false),
        shadow_use_array_(false),
        has_(false),
        has_shadow_(false),
        is_inited_(false)
  {}
  virtual ~ColumnMap()
  {}
  int clear();
  bool is_inited() const
  {
    return is_inited_;
  }
  int init(const common::ObIArray<ObColumnParam*>& column_params);
  int init(const common::ObIArray<ObColDesc>& column_descs);
  int init(const common::ObIArray<uint64_t>& column_ids);
  int get(const uint64_t column_id, int32_t& proj) const;
  TO_STRING_KV(K(is_inited_), K(use_array_), K(shadow_use_array_), K(has_), K(has_shadow_));

private:
  ColumnMap();
  ColumnMap(const ColumnMap& other);
  ColumnMap& operator=(const ColumnMap& other);

private:
  int create(const bool use_array, const int64_t array_size, const int64_t offset,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<int32_t>& column_indexes, ColumnArray& array,
      ColumnHashMap& map);

private:
  ColumnArray array_;
  ColumnArray shadow_array_;
  ColumnHashMap map_;
  ColumnHashMap shadow_map_;
  bool use_array_;
  bool shadow_use_array_;
  bool has_;
  bool has_shadow_;
  bool is_inited_;
};

class ObColumnParam {
  OB_UNIS_VERSION_V(1);

public:
  explicit ObColumnParam(common::ObIAllocator& allocator);
  virtual ~ObColumnParam();
  virtual void reset();

private:
  ObColumnParam();
  DISALLOW_COPY_AND_ASSIGN(ObColumnParam);

public:
  inline void set_column_id(const uint64_t column_id)
  {
    column_id_ = column_id;
  }
  inline void set_meta_type(const common::ObObjMeta meta_type)
  {
    meta_type_ = meta_type;
  }
  inline void set_column_order(const common::ObOrderType order)
  {
    order_ = order;
  }
  inline void set_accuracy(const common::ObAccuracy& accuracy)
  {
    accuracy_ = accuracy;
  }
  inline int set_orig_default_value(const common::ObObj& default_value)
  {
    return deep_copy_obj(default_value, orig_default_value_);
  }
  inline int set_cur_default_value(const common::ObObj& default_value)
  {
    return deep_copy_obj(default_value, cur_default_value_);
  }
  int32_t get_data_length() const;
  inline uint64_t get_column_id() const
  {
    return column_id_;
  }
  inline const common::ObObjMeta& get_meta_type() const
  {
    return meta_type_;
  }
  inline common::ObOrderType get_column_order() const
  {
    return order_;
  }
  inline const common::ObAccuracy& get_accuracy() const
  {
    return accuracy_;
  }
  inline const common::ObObj& get_orig_default_value() const
  {
    return orig_default_value_;
  }
  inline const common::ObObj& get_cur_default_value() const
  {
    return cur_default_value_;
  }
  inline bool is_nullable() const
  {
    return is_nullable_;
  }
  inline void set_nullable(const bool nullable)
  {
    is_nullable_ = nullable;
  }
  int assign(const ObColumnParam& other);

  TO_STRING_KV(K_(column_id), K_(meta_type), K_(order), K_(accuracy), K_(orig_default_value), K_(cur_default_value),
      K_(is_nullable));

private:
  int deep_copy_obj(const common::ObObj& src, common::ObObj& dest);

private:
  common::ObIAllocator& allocator_;
  uint64_t column_id_;
  common::ObObjMeta meta_type_;
  common::ObOrderType order_;
  common::ObAccuracy accuracy_;
  common::ObObj orig_default_value_;
  common::ObObj cur_default_value_;
  bool is_nullable_;
};

class ObTableSchema;
class ObColumnSchemaV2;
class ObTableSchemaParam;
class ObTableParam {
  OB_UNIS_VERSION_V(1);

public:
  typedef common::ObFixedArray<ObColumnParam*, common::ObIAllocator> Columns;
  typedef common::ObFixedArray<int32_t, common::ObIAllocator> Projector;
  typedef common::ObFixedArray<ObColDesc, common::ObIAllocator> ColDescArray;

public:
  explicit ObTableParam(common::ObIAllocator& allocator);
  virtual ~ObTableParam();
  virtual void reset();

private:
  ObTableParam();
  DISALLOW_COPY_AND_ASSIGN(ObTableParam);

public:
  int convert(const ObTableSchema& table_schema, const ObTableSchema& index_schema,
      const common::ObIArray<uint64_t>& output_column_ids, const bool index_back);

  // convert right table scan parameter of join MV scan.
  // (right table index back not supported)
  int convert_join_mv_rparam(const ObTableSchema& mv_schema, const ObTableSchema& right_schema,
      const common::ObIArray<uint64_t>& mv_column_ids);

  // convert from table schema param which is used in ObTableModify operators
  // used to get conflict row only by row keys
  int convert_schema_param(
      const share::schema::ObTableSchemaParam& schema_param, const common::ObIArray<uint64_t>& output_column_ids);

  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  inline uint64_t get_index_id() const
  {
    return index_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline int64_t get_index_schema_version() const
  {
    return index_schema_version_;
  }
  inline int64_t get_main_rowkey_cnt() const
  {
    return main_table_rowkey_cnt_;
  }
  inline int64_t get_index_rowkey_cnt() const
  {
    return index_table_rowkey_cnt_;
  }
  inline bool use_lob_locator() const
  {
    return use_lob_locator_;
  }
  inline int64_t get_rowid_version() const
  {
    return rowid_version_;
  }
  inline const common::ObIArray<int32_t>& get_rowid_projector() const
  {
    return rowid_projector_;
  }

  inline const common::ObIArray<ObColumnParam*>& get_columns() const
  {
    return cols_;
  }
  inline const common::ObIArray<ObColumnParam*>& get_full_columns() const
  {
    return full_cols_;
  }
  inline const common::ObIArray<int32_t>& get_full_projector() const
  {
    return full_projector_;
  }
  inline const ColumnMap& get_column_map() const
  {
    return col_map_;
  }
  inline const ColumnMap& get_full_column_map() const
  {
    return full_col_map_;
  }
  inline const common::ObIArray<int32_t>& get_projector() const
  {
    return projector_;
  }
  inline const common::ObIArray<int32_t>& get_output_projector() const
  {
    return output_projector_;
  }

  inline const common::ObIArray<ObColumnParam*>& get_index_columns() const
  {
    return index_cols_;
  }
  inline const ColumnMap& get_index_column_map() const
  {
    return index_col_map_;
  }
  inline const common::ObIArray<int32_t>& get_index_projector() const
  {
    return index_projector_;
  }
  inline const common::ObIArray<int32_t>& get_index_output_projector() const
  {
    return index_output_projector_;
  }

  inline const common::ObIArray<int32_t>& get_index_back_projector() const
  {
    return index_back_projector_;
  }
  inline const common::ObIArray<int32_t>& get_pad_col_projector() const
  {
    return pad_col_projector_;
  }
  inline void disable_padding()
  {
    pad_col_projector_.reset();
  }

  inline const common::ObIArray<int32_t>& get_join_key_projector() const
  {
    return join_key_projector_;
  }
  inline const common::ObIArray<int32_t>& get_right_key_projector() const
  {
    return right_key_projector_;
  }

  inline const common::ObIArray<ObColDesc>& get_col_descs() const
  {
    return col_descs_;
  }
  inline const common::ObIArray<ObColDesc>& get_index_col_descs() const
  {
    return index_col_descs_;
  }
  inline const common::ObIArray<ObColDesc>& get_full_col_descs() const
  {
    return full_col_descs_;
  }

  DECLARE_TO_STRING;

  static int convert_column_schema_to_param(const ObColumnSchemaV2& column_schema, ObColumnParam& column_param);
  static int get_columns_serialize_size(const Columns& columns, int64_t& size);
  static int serialize_columns(const Columns& columns, char* buf, const int64_t data_len, int64_t& pos);
  static int deserialize_columns(
      const char* buf, const int64_t data_len, int64_t& pos, Columns& columns, common::ObIAllocator& allocator);
  static int alloc_column(common::ObIAllocator& allocator, ObColumnParam*& col_ptr);
  static int create_column_map(const common::ObIArray<ObColumnParam*>& cols, ColumnMap& col_map);

private:
  int construct_columns_and_projector(const ObTableSchema& table_schema,
      const common::ObIArray<uint64_t>& output_column_ids, common::ObIArray<ObColumnParam*>& cols, ColumnMap& col_map,
      common::ObIArray<int32_t>& projector, common::ObIArray<int32_t>& output_projector);
  int construct_columns_and_projector_for_index(const ObTableSchema& table_schema, const ObTableSchema& index_schema,
      const common::ObIArray<uint64_t>& output_column_ids, Columns& cols, ColumnMap& col_map, Projector& projector,
      Projector& output_projector);
  int construct_full_columns_and_projector(const ObTableSchema& table_schema,
      common::ObIArray<ObColumnParam*>& full_cols, Projector& full_projector, ColumnMap& full_col_map);

  int filter_common_columns(
      const common::ObIArray<const ObColumnSchemaV2*>& columns, common::ObIArray<const ObColumnSchemaV2*>& new_columns);
  int construct_pad_projector(
      const Columns& dst_columns, const Projector& dst_output_projector, Projector& pad_projector);
  int construct_storage_param();

  // construct lob locator param for storage
  // @param [in] table_schema
  // @param [in] storage_project_columns
  // @param [in] access_projector
  // @param [out] use_lob_locator
  // @param [out] rowid_version
  // @param [out] rowid_projector
  int construct_lob_locator_param(const ObTableSchema& table_schema, const Columns& storage_project_columns,
      const Projector& access_projector, bool& use_lob_locator, int64_t& rowid_version, Projector& rowid_projector);

private:
  const static int64_t DEFAULT_COLUMN_MAP_BUCKET_NUM = 4;
  common::ObIAllocator& allocator_;
  uint64_t table_id_;
  uint64_t index_id_;
  int64_t schema_version_;
  int64_t main_table_rowkey_cnt_;
  int64_t index_table_rowkey_cnt_;

  Columns cols_;
  ColumnMap col_map_;
  Projector projector_;
  Projector output_projector_;

  Columns index_cols_;
  ColumnMap index_col_map_;
  Projector index_projector_;
  Projector index_output_projector_;

  Projector index_back_projector_;

  Projector pad_col_projector_;

  // join materialized view scan:
  // project left table's output row to join key for right table multiple get.
  // array values: left table ObTableScanParam::column_ids_ index
  Projector join_key_projector_;
  // join materialized view scan:
  // project left table's output row to right table's rowkey for row exist checking.
  // array values: left table ObTableScanParam::column_ids_ index
  Projector right_key_projector_;
  int64_t index_schema_version_;

  // for full column table scan
  Columns full_cols_;
  Projector full_projector_;
  ColumnMap full_col_map_;

  // generated members which are used in storage layer. Won't serialize.
  ColDescArray col_descs_;        // construct from cols_;
  ColDescArray index_col_descs_;  // construct from index_cols_;
  ColDescArray full_col_descs_;   // construct from full_cols_;

  // specified to use lob locator or not
  bool use_lob_locator_;
  int64_t rowid_version_;
  Projector rowid_projector_;
};
}  // namespace schema
}  // namespace share
}  // namespace oceanbase

#endif /* OB_TABLE_PARAM_H_ */
