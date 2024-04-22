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
#include "common/object/ob_object.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_define.h"
#include "storage/access/ob_table_read_info.h"
#include "sql/engine/basic/ob_pushdown_filter.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
// A customized hash map to store schema information in plan
// 1. Key && value type is fixed
// 2. Memory is managed by the pass in allocator, doesn't release inside
// 3. Can not be reused
class ColumnHashMap
{
  struct HashNode
  {
    uint64_t key_;
    int32_t value_;
    struct HashNode *next_;
  };
public:
  ColumnHashMap(common::ObIAllocator &allocator)
    : allocator_(allocator), bucket_num_(0), buckets_(NULL), is_inited_(false)
  {}
  virtual ~ColumnHashMap() {} // not free memory
  int clear();
  int init(const int64_t bucket_num);
  int set(const uint64_t key, const int32_t value);
  int get(const uint64_t key, int32_t &value) const;
private:
  ColumnHashMap();
  ColumnHashMap(const ColumnHashMap &other);
  ColumnHashMap &operator=(const ColumnHashMap &other);
private:
  bool is_inited() const { return is_inited_; }
  int find_node(const uint64_t key, HashNode *head, HashNode *&node) const;
public:
  common::ObIAllocator &allocator_;
  int64_t bucket_num_;
  HashNode **buckets_;
  bool is_inited_;
};

struct ObColDesc final
{
public:
  ObColDesc():col_id_(0), col_type_(), col_order_(common::ObOrderType::ASC) {};
  ~ObColDesc() = default;
  int64_t to_string(char *buffer, const int64_t length) const
  {
    int64_t pos = 0;
    (void)common::databuff_printf(buffer, length, pos, "column_id=%u ", col_id_);
    pos += col_type_.to_string(buffer + pos, length - pos);
    (void)common::databuff_printf(buffer, length, pos, " order=%d", col_order_);
    return pos;
  }
  int assign(const ObColDesc &other)
  {
    int ret = common::OB_SUCCESS;
    if (this != &other) {
      col_id_ = other.col_id_;
      col_type_ = other.col_type_;
      col_order_ = other.col_order_;
    }
    return ret;
  }
  void reset();

  NEED_SERIALIZE_AND_DESERIALIZE;

  uint32_t col_id_;
  common::ObObjMeta col_type_;
  common::ObOrderType col_order_;
};

struct ObColExtend final
{
  OB_UNIS_VERSION(1);
public:
  ObColExtend(): skip_index_attr_() {};
  ~ObColExtend() = default;
  int64_t to_string(char *buffer, const int64_t length) const
  {
    int64_t pos = 0;
    pos += skip_index_attr_.to_string(buffer + pos, length - pos);
    return pos;
  }
  int assign(const ObColExtend &other)
  {
    int ret = common::OB_SUCCESS;
    if (this != &other) {
      skip_index_attr_ = other.skip_index_attr_;
    }
    return ret;
  }
  void reset();
private:
  DISALLOW_COPY_AND_ASSIGN(ObColExtend);
public:
  ObSkipIndexColumnAttr skip_index_attr_;
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
class ColumnMap
{
  const static int64_t MAX_ARRAY_SIZE = common::OB_ROW_MAX_COLUMNS_COUNT;
  const static int64_t COLUMN_ID_OFFSET = 1;
  const static int64_t SHADOW_COLUMN_ID_OFFSET =
      common::OB_MIN_SHADOW_COLUMN_ID + 1;
  const static int64_t MAX_COLUMN_ID_USING_ARRAY =
      COLUMN_ID_OFFSET + MAX_ARRAY_SIZE - 1;
  const static int64_t MAX_SHADOW_COLUMN_ID_USING_ARRAY =
      SHADOW_COLUMN_ID_OFFSET + MAX_ARRAY_SIZE - 1;

  typedef common::ObFixedArray<int32_t, common::ObIAllocator> ColumnArray;
  #define IS_SHADOW_COLUMN(column_id) ((column_id >= OB_MIN_SHADOW_COLUMN_ID) && !common::is_mlog_special_column(column_id))

public:
  ColumnMap(common::ObIAllocator &allocator)
    : array_(allocator), shadow_array_(allocator),
      map_(allocator), shadow_map_(allocator),
      use_array_(false), shadow_use_array_(false),
      has_(false), has_shadow_(false), is_inited_(false)
  {}
  virtual ~ColumnMap() {}
  int clear();
  bool is_inited() const { return is_inited_; }
  int init(const common::ObIArray<ObColumnParam *> &column_params);
  int init(const common::ObIArray<ObColDesc> &column_descs);
  int init(const common::ObIArray<uint64_t> &column_ids);
  int get(const uint64_t column_id, int32_t &proj) const;
  TO_STRING_KV(K(is_inited_), K(use_array_), K(shadow_use_array_), K(has_), K(has_shadow_));
private:
  ColumnMap();
  ColumnMap(const ColumnMap &other);
  ColumnMap &operator=(const ColumnMap &other);
private:
  int create(const bool use_array,
             const int64_t array_size,
             const int64_t offset,
             const common::ObIArray<uint64_t> &column_ids,
             const common::ObIArray<int32_t> &column_indexes,
             ColumnArray &array,
             ColumnHashMap &map);
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

class ObColumnParam
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObColumnParam(common::ObIAllocator &allocator);
  virtual ~ObColumnParam();
  virtual void reset();
  void destroy(); // free allocated memory
private:
  ObColumnParam();
  DISALLOW_COPY_AND_ASSIGN(ObColumnParam);

public:
  inline void set_column_id(const uint64_t column_id) { column_id_ = column_id; }
  inline void set_meta_type(const common::ObObjMeta meta_type) { meta_type_ = meta_type; }
  inline void set_column_order(const common::ObOrderType order) { order_ = order; }
  inline void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_ = accuracy; }
  inline int set_orig_default_value(const common::ObObj &default_value)
  { return deep_copy_obj(default_value, orig_default_value_); }
  inline int set_cur_default_value(const common::ObObj &default_value)
  { return deep_copy_obj(default_value, cur_default_value_); }
  int32_t get_data_length() const;
  inline uint64_t get_column_id() const { return column_id_; }
  inline const common::ObObjMeta &get_meta_type() const { return meta_type_; }
  inline common::ObOrderType get_column_order() const { return order_; }
  inline const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  inline const common::ObObj &get_orig_default_value() const { return orig_default_value_; }
  inline const common::ObObj &get_cur_default_value() const { return cur_default_value_; }
  inline bool is_nullable_for_write() const { return is_nullable_for_write_; }
  inline void set_nullable_for_write(const bool nullable) { is_nullable_for_write_ = nullable; }
  inline bool is_nullable_for_read() const { return is_nullable_for_read_; }
  inline void set_nullable_for_read(const bool nullable) { is_nullable_for_read_ = nullable; }
  inline void set_gen_col_flag(const bool is_gen_col, const bool is_virtual)
  {
    is_gen_col_ = is_gen_col;
    is_virtual_gen_col_ = is_virtual;
  }
  inline bool is_gen_col() const { return is_gen_col_; }
  inline bool is_virtual_gen_col() const { return is_virtual_gen_col_; }
  inline bool is_gen_col_udf_expr() const { return is_gen_col_udf_expr_; }
  inline void set_gen_col_udf_expr(const bool flag) { is_gen_col_udf_expr_ = flag; }
  inline void set_is_hidden(const bool is_hidden) { is_hidden_ = is_hidden; }
  inline bool is_hidden() const { return is_hidden_; }
  int assign(const ObColumnParam &other);

  inline void set_lob_chunk_size(int64_t chunk_size) { lob_chunk_size_ = chunk_size; }
  inline int64_t get_lob_chunk_size() const { return lob_chunk_size_; }

  TO_STRING_KV(K_(column_id),
               K_(meta_type),
               K_(order),
               K_(accuracy),
               K_(orig_default_value),
               K_(cur_default_value),
               K_(is_nullable_for_write),
               K_(is_nullable_for_read),
               K_(is_gen_col),
               K_(is_virtual_gen_col),
               K_(is_gen_col_udf_expr),
               K_(is_hidden),
               K_(lob_chunk_size));
private:
  int deep_copy_obj(const common::ObObj &src, common::ObObj &dest);
private:
  common::ObIAllocator &allocator_;
  uint64_t column_id_;
  common::ObObjMeta meta_type_;
  common::ObOrderType order_;
  common::ObAccuracy accuracy_;
  common::ObObj orig_default_value_;
  common::ObObj cur_default_value_;
  bool is_nullable_for_write_;
  bool is_nullable_for_read_;
  bool is_gen_col_;
  bool is_virtual_gen_col_;
  bool is_gen_col_udf_expr_;
  bool is_hidden_;
  int64_t lob_chunk_size_;
};

typedef common::ObFixedArray<ObColumnParam *, common::ObIAllocator> Columns;
typedef common::ObFixedArray<int32_t, common::ObIAllocator> Projector;


class ObTableSchema;
class ObColumnSchemaV2;
class ObTableParam
{
  OB_UNIS_VERSION_V(1);
public:
public:
  explicit ObTableParam(common::ObIAllocator &allocator);
  virtual ~ObTableParam();
  virtual void reset();
private:
  ObTableParam();
  DISALLOW_COPY_AND_ASSIGN(ObTableParam);

public:
  int convert(const ObTableSchema &table_schema,
              const common::ObIArray<uint64_t> &output_column_ids,
              const sql::ObStoragePushdownFlag &pd_pushdown_flag,
              const common::ObIArray<uint64_t> *tsc_out_cols = NULL,
              const bool force_mysql_mode = false);

  // convert aggregate column projector from 'aggregate_column_ids' and 'output_projector_'
  // convert group by column projector from 'group_by_column_ids' and 'output_projector_'
  // must be called after 'output_projector_' has been generated.
  int convert_group_by(const ObTableSchema &table_schema,
                  const common::ObIArray<uint64_t> &output_column_ids,
                  const common::ObIArray<uint64_t> &aggregate_column_ids,
                  const common::ObIArray<uint64_t> &group_by_column_ids,
                  const sql::ObStoragePushdownFlag &pd_pushdown_flag);
  // convert right table scan parameter of join MV scan.
  // (right table index back not supported)
  inline uint64_t get_table_id() const { return table_id_; }
  inline int64_t is_spatial_index() const { return is_spatial_index_; }
  inline void set_is_spatial_index(bool is_spatial_index) { is_spatial_index_ = is_spatial_index; }
  inline bool is_fts_index() const { return is_fts_index_; }
  inline void set_is_fts_index(const bool is_fts_index) { is_fts_index_ = is_fts_index; }
  inline int64_t is_multivalue_index() const { return is_multivalue_index_; }
  inline void set_is_multivalue_index(bool is_multivalue_index) { is_multivalue_index_ = is_multivalue_index; }
  inline bool use_lob_locator() const { return use_lob_locator_; }
  inline bool enable_lob_locator_v2() const { return enable_lob_locator_v2_; }
  inline bool &get_enable_lob_locator_v2() { return enable_lob_locator_v2_; }
  inline bool has_virtual_column() const { return has_virtual_column_; }
  inline int64_t get_rowid_version() const { return rowid_version_; }
  inline const common::ObIArray<int32_t> &get_rowid_projector() const { return rowid_projector_; }
  inline const common::ObIArray<int32_t> &get_output_projector() const { return output_projector_; }
  inline const common::ObIArray<int32_t> &get_aggregate_projector() const { return aggregate_projector_; }
  inline const common::ObIArray<int32_t> &get_group_by_projector() const { return group_by_projector_; }
  inline const common::ObIArray<bool> &get_output_sel_mask() const { return output_sel_mask_; }
  inline const common::ObIArray<int32_t> &get_pad_col_projector() const { return pad_col_projector_; }
  inline void disable_padding() { pad_col_projector_.reset(); }
  inline const storage::ObTableReadInfo &get_read_info() const { return main_read_info_; }
  inline const ObString &get_parser_name() const { return parser_name_; }
  inline const common::ObIArray<storage::ObTableReadInfo *> *get_cg_read_infos() const
  { return cg_read_infos_.empty() ? nullptr : &cg_read_infos_; }

  DECLARE_TO_STRING;

  static int convert_column_schema_to_param(const ObColumnSchemaV2 &column_schema,
      ObColumnParam &column_param);
  static int get_columns_serialize_size(const Columns &columns, int64_t &size);
  static int serialize_columns(const Columns &columns, char *buf, const int64_t data_len,
                               int64_t &pos);
  static int deserialize_columns(const char *buf, const int64_t data_len,
                                 int64_t &pos, Columns &columns, common::ObIAllocator &allocator);
  static int alloc_column(common::ObIAllocator &allocator, ObColumnParam *& col_ptr);
private:
  int construct_columns_and_projector(const ObTableSchema &table_schema,
                                      const common::ObIArray<uint64_t> &output_column_ids,
                                      const common::ObIArray<uint64_t> *tsc_out_cols,
                                      const bool force_mysql_mode,
                                      const sql::ObStoragePushdownFlag &pd_pushdown_flag);

  int filter_common_columns(const common::ObIArray<const ObColumnSchemaV2 *> &columns,
                            common::ObIArray<const ObColumnSchemaV2 *> &new_columns);
  int construct_pad_projector(
      const ObIArray<ObColumnParam *> &dst_columns,
      const Projector &dst_output_projector,
      Projector &pad_projector);

  // construct lob locator param for storage
  // @param [in] table_schema
  // @param [in] storage_project_columns
  // @param [in] access_projector
  // @param [out] use_lob_locator
  // @param [out] rowid_version
  // @param [out] rowid_projector
  // @param [in] use_lob_locator_v2
  int construct_lob_locator_param(const ObTableSchema &table_schema,
                                  const ObIArray<ObColumnParam *> &storage_project_columns,
                                  const Projector &access_projector,
                                  bool &use_lob_locator,
                                  int64_t &rowid_version,
                                  Projector &rowid_projector,
                                  bool is_use_lob_locator_v2);
  int convert_fulltext_index_info(const ObTableSchema &table_schema);

private:
  const static int64_t DEFAULT_COLUMN_MAP_BUCKET_NUM = 4;
  common::ObIAllocator &allocator_;
  uint64_t table_id_;

  // projector from output columns to access main columns
  Projector output_projector_;
  Projector aggregate_projector_;
  Projector group_by_projector_;
  // Table access column exprs include columns in filters.
  // While in white box filters computing, these columns don't need to be in output projector.
  // output_sel_mask_ records whether each column is in table scan output(true)
  // or just in filters(false).
  common::ObFixedArray<bool, common::ObIAllocator> output_sel_mask_;
  // others
  Projector pad_col_projector_;

  // need to serialize
  // version of the mixture read info
  int16_t read_param_version_;
  storage::ObTableReadInfo main_read_info_;
  storage::ObFixedMetaObjArray<storage::ObTableReadInfo *> cg_read_infos_;

  bool has_virtual_column_;
  // specified to use lob locator or not
  bool use_lob_locator_;
  int64_t rowid_version_;
  Projector rowid_projector_;
  ObString parser_name_;
  // if min cluster version < 4.1 use lob locator v1, else use lob locator v2.
  // use enable_lob_locator_v2_ to avoid locator type sudden change while table scan is running
  bool enable_lob_locator_v2_;
  bool is_spatial_index_;
  bool is_fts_index_;
  bool is_multivalue_index_;
};
} //namespace schema
} //namespace share
} //namespace oceanbase

#endif /* OB_TABLE_PARAM_H_ */
