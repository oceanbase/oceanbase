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

#ifndef OCEANBASE_STORAGE_STORAGE_SCHEMA_
#define OCEANBASE_STORAGE_STORAGE_SCHEMA_

#include "lib/container/ob_fixed_array.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{

namespace blocksstable
{
struct ObSSTableColumnMeta;
class ObStorageDatum;
}

namespace storage
{

struct ObCreateSSTableParamExtraInfo;

struct ObStorageRowkeyColumnSchema
{
  OB_UNIS_VERSION(1);
public:
  ObStorageRowkeyColumnSchema();
  ~ObStorageRowkeyColumnSchema();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(column_idx), K_(meta_type), K_(order));

private:
  static const int32_t SRCS_ONE_BIT = 1;
  static const int32_t SRCS_RESERVED_BITS = 31;

public:
  union {
    uint32_t info_;
    struct {
      uint32_t order_   :SRCS_ONE_BIT;
      uint32_t reserved_:SRCS_RESERVED_BITS;
    };
  };
  uint32_t column_idx_;
  ObObjMeta meta_type_;
};

struct ObStorageColumnSchema
{
  OB_UNIS_VERSION(1);
public:
  ObStorageColumnSchema();
  ~ObStorageColumnSchema();

  void reset();
  void destroy(ObIAllocator &allocator);
  bool is_valid() const;
  int construct_column_param(share::schema::ObColumnParam &column_param) const;
  inline common::ColumnType get_data_type() const { return meta_type_.get_type(); }
  inline bool is_generated_column() const { return is_generated_column_; }
  inline const common::ObObj &get_orig_default_value()  const { return orig_default_value_; }
  int deep_copy_default_val(ObIAllocator &allocator, const ObObj &default_val);

  int legacy_deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int legacy_serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t legacy_serialize_len() const;

  TO_STRING_KV(K_(meta_type), K_(is_column_stored_in_sstable), K_(is_rowkey_column),
      K_(is_generated_column), K_(orig_default_value));

private:
  static const int32_t SCS_ONE_BIT = 1;
  static const int32_t SCS_RESERVED_BITS = 29;

public:
  union {
    uint32_t info_;
    struct {
      uint32_t is_column_stored_in_sstable_     :SCS_ONE_BIT;
      uint32_t is_rowkey_column_                :SCS_ONE_BIT;
      uint32_t is_generated_column_             :SCS_ONE_BIT;
      uint32_t reserved_                        :SCS_RESERVED_BITS;
    };
  };
  int64_t default_checksum_;
  ObObjMeta meta_type_;
  ObObj orig_default_value_;
};

struct ObStorageColumnGroupSchema
{
public:
  ObStorageColumnGroupSchema()
    : version_(COLUMN_GRUOP_SCHEMA_VERSION),
      type_(share::schema::MAX_COLUMN_GROUP),
      compressor_type_(INVALID_COMPRESSOR),
      row_store_type_(MAX_ROW_STORE),
      block_size_(0),
      schema_column_cnt_(0),
      rowkey_column_cnt_(0),
      schema_rowkey_column_cnt_(0),
      column_cnt_(0),
      column_idxs_(nullptr)
  {}
  ~ObStorageColumnGroupSchema() = default;
  OB_INLINE void reset() { MEMSET(this, 0, sizeof(ObStorageColumnGroupSchema)); }
  void destroy(ObIAllocator &allocator);
  OB_INLINE bool is_valid() const
  {
    return version_ == COLUMN_GRUOP_SCHEMA_VERSION && type_ < share::schema::MAX_COLUMN_GROUP
           && column_cnt_ <= OB_ROW_MAX_COLUMNS_COUNT
           && (nullptr != column_idxs_ || type_ == share::schema::ALL_COLUMN_GROUP || type_ == share::schema::DEFAULT_COLUMN_GROUP);
  }

  int copy_from(
      common::ObIArray<share::schema::ObColDesc> &column_ids,
      const int64_t schema_rowkey_cnt,
      const share::schema::ObColumnGroupSchema &cg_schema,
      common::ObIAllocator &allocator);
  uint16_t get_column_count() const { return column_cnt_;}
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int deep_copy(const ObStorageColumnGroupSchema &other, common::ObIAllocator &allocator);
  OB_INLINE bool is_all_column_group () const { return type_ == share::schema::ALL_COLUMN_GROUP; }
  OB_INLINE bool is_default_column_group () const { return type_ == share::schema::DEFAULT_COLUMN_GROUP; }
  OB_INLINE bool is_rowkey_column_group() const { return type_ == share::schema::ROWKEY_COLUMN_GROUP; }
  OB_INLINE bool is_single_column_group() const { return type_ == share::schema::SINGLE_COLUMN_GROUP; }
  OB_INLINE bool has_multi_version_column() const { return is_all_column_group() || is_rowkey_column_group(); }
  OB_INLINE bool is_inited() const { return row_store_type_ != MAX_ROW_STORE; };

  TO_STRING_KV(K_(version), K_(type), K_(compressor_type), K_(row_store_type), K_(block_size), K_(schema_column_cnt), K_(rowkey_column_cnt),
      K_(schema_rowkey_column_cnt), K_(column_cnt), "column_idxs", ObArrayWrap<uint16_t>(column_idxs_, column_cnt_));
public:
  static const int64_t COLUMN_GRUOP_SCHEMA_VERSION = 1;
  uint8_t version_;
  share::schema::ObColumnGroupType type_;
  ObCompressorType compressor_type_;
  ObRowStoreType row_store_type_;
  uint32_t block_size_;
  uint16_t schema_column_cnt_;
  uint16_t rowkey_column_cnt_;
  uint16_t schema_rowkey_column_cnt_;
  uint16_t column_cnt_;
  uint16_t *column_idxs_; //free the memory with the allocator outside
};

class ObStorageSchema : public share::schema::ObMergeSchema
{
public:
  ObStorageSchema();
  virtual ~ObStorageSchema();
  bool is_inited() const { return is_inited_; }
  int init(
      common::ObIAllocator &allocator,
      const share::schema::ObTableSchema &input_schema,
      const lib::Worker::CompatMode compat_mode,
      const bool skip_column_info = false,
      const int64_t compat_version = STORAGE_SCHEMA_VERSION_V3);
  int init(
      common::ObIAllocator &allocator,
      const ObStorageSchema &old_schema,
      const bool skip_column_info = false,
      const ObStorageSchema *column_group_schema = nullptr);
  int deep_copy_column_array(
      common::ObIAllocator &allocator,
      const ObStorageSchema &src_schema,
      const int64_t copy_array_cnt);
  int deep_copy_column_group_array(
      common::ObIAllocator &allocator,
      const ObStorageSchema &src_schema);
  void reset_column_group_array();
  void reset();
  virtual bool is_valid() const override;
  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  void update_column_cnt(const int64_t input_col_cnt);

  // for new mds
  int assign(common::ObIAllocator &allocator, const ObStorageSchema &other);

  //TODO @lixia use compact mode in storage schema to compaction
  inline bool is_oracle_mode() const { return compat_mode_ == static_cast<uint32_t>(lib::Worker::CompatMode::ORACLE); }
  inline lib::Worker::CompatMode get_compat_mode() const { return static_cast<lib::Worker::CompatMode>(compat_mode_);}
  /* merge related function*/
  virtual inline int64_t get_tablet_size() const override { return tablet_size_; }
  virtual inline int64_t get_rowkey_column_num() const override { return rowkey_array_.count(); }
  virtual inline int64_t get_schema_version() const override { return schema_version_; }
  virtual inline int64_t get_column_count() const override { return column_cnt_; }
  inline int64_t get_column_group_count() const { return column_group_array_.count(); }
  inline int64_t has_all_column_group() const { return has_all_column_group_; }
  inline bool is_row_store() const { return column_group_array_.count() <= 1; }
  virtual inline int64_t get_pctfree() const override { return pctfree_; }
  virtual inline int64_t get_progressive_merge_round() const override { return progressive_merge_round_; }
  virtual inline int64_t get_progressive_merge_num() const override { return progressive_merge_num_; }
  virtual inline uint64_t get_master_key_id() const override { return master_key_id_; }
  virtual inline bool is_use_bloomfilter() const override { return is_use_bloomfilter_; }
  virtual inline bool is_index_table() const override { return share::schema::is_index_table(table_type_); }
  virtual inline bool is_storage_index_table() const override
  {
    return share::schema::is_index_table(table_type_);
  }
  inline bool is_materialized_view() const { return share::schema::ObTableSchema::is_materialized_view(table_type_); }
  inline bool is_mlog_table() const { return share::schema::ObTableSchema::is_mlog_table(table_type_); }
  inline bool is_fts_index() const { return share::schema::is_fts_index(index_type_); }
  virtual inline bool is_global_index_table() const override { return share::schema::ObSimpleTableSchemaV2::is_global_index_table(index_type_); }
  virtual inline int64_t get_block_size() const override { return block_size_; }

  virtual int get_store_column_count(int64_t &column_count, const bool full_col) const override;
  int get_stored_column_count_in_sstable(int64_t &column_count) const;
  virtual int get_multi_version_column_descs(common::ObIArray<share::schema::ObColDesc> &column_descs) const override;
  virtual int get_rowkey_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids) const override;
  virtual int get_skip_index_col_attr(common::ObIArray<share::schema::ObSkipIndexColumnAttr> &skip_idx_metas) const override;
  virtual int get_encryption_id(int64_t &encrypt_id) const override;
  virtual const common::ObString &get_encryption_str() const override { return encryption_; }
  virtual bool need_encrypt() const override;
  virtual inline const common::ObString &get_encrypt_key() const override { return encrypt_key_; }
  virtual inline const char *get_encrypt_key_str() const override { return encrypt_key_.empty() ? "" : encrypt_key_.ptr(); }
  virtual inline int64_t get_encrypt_key_len() const override { return encrypt_key_.length(); }
  virtual inline share::schema::ObTableModeFlag get_table_mode_flag() const override
  { return (share::schema::ObTableModeFlag)table_mode_.mode_flag_; }
  virtual inline share::schema::ObTableMode get_table_mode_struct() const override { return table_mode_; }
  virtual inline share::schema::ObTableType get_table_type() const override { return table_type_; }
  virtual inline share::schema::ObIndexType get_index_type() const override { return index_type_; }
  const common::ObIArray<ObStorageColumnSchema> &get_store_column_schemas() const { return column_array_; }
  const common::ObIArray<ObStorageColumnGroupSchema> &get_column_groups() const { return column_group_array_; }
  virtual inline common::ObRowStoreType get_row_store_type() const override { return row_store_type_; }
  virtual inline const char *get_compress_func_name() const override {  return all_compressor_name[compressor_type_]; }
  virtual inline common::ObCompressorType get_compressor_type() const override { return compressor_type_; }
  virtual inline bool is_column_info_simplified() const override { return column_info_simplified_; }

  virtual int init_column_meta_array(
      common::ObIArray<blocksstable::ObSSTableColumnMeta> &meta_array) const override;
  int get_orig_default_row(const common::ObIArray<share::schema::ObColDesc> &column_ids,
                           bool need_trim,
                           blocksstable::ObDatumRow &default_row) const;
  const ObStorageColumnSchema *get_column_schema(const int64_t column_id) const;
  int mock_row_store_cg(ObStorageColumnGroupSchema &mocked_row_store_cg) const;
  int get_base_rowkey_column_group_index(int32_t &cg_idx) const;
  // This function only get cg idx for actually stored column
  int get_column_group_index(
      const uint64_t &column_id,
      const int32_t &column_idx,
      int32_t &cg_idx) const;
  // Use this comparison function to determine which schema has been updated later
  // true: input_schema is newer
  // false: current schema is newer
  bool compare_schema_newer(const ObStorageSchema &input_schema) const
  {
    return store_column_cnt_ < input_schema.store_column_cnt_
      || column_group_array_.count() < input_schema.column_group_array_.count();
  }

  inline bool is_aux_lob_meta_table() const { return share::schema::is_aux_lob_meta_table(table_type_); }
  inline bool is_aux_lob_piece_table() const { return share::schema::is_aux_lob_piece_table(table_type_); }
  OB_INLINE bool is_user_hidden_table() const { return share::schema::TABLE_STATE_IS_HIDDEN_MASK & table_mode_.state_flag_; }

  VIRTUAL_TO_STRING_KV(KP(this), K_(storage_schema_version), K_(version),
      K_(is_use_bloomfilter), K_(column_info_simplified), K_(compat_mode), K_(table_type), K_(index_type),
      K_(row_store_type), K_(schema_version),
      K_(column_cnt), K_(store_column_cnt), K_(tablet_size), K_(pctfree), K_(block_size), K_(progressive_merge_round),
      K_(master_key_id), K_(compressor_type), K_(encryption), K_(encrypt_key),
      "rowkey_cnt", rowkey_array_.count(), K_(rowkey_array), "column_cnt", column_array_.count(), K_(column_array),
      "skip_index_cnt", skip_idx_attr_array_.count(), K_(skip_idx_attr_array),
      "column_group_cnt", column_group_array_.count(), K_(column_group_array), K_(has_all_column_group));
public:
  static void trim(const ObCollationType type, blocksstable::ObStorageDatum &storage_datum);
private:
  void copy_from(const share::schema::ObMergeSchema &input_schema);
  int deep_copy_str(const ObString &src, ObString &dest);
  int add_column_group(const ObStorageColumnGroupSchema &column_group);
  inline bool is_view_table() const { return share::schema::ObTableType::USER_VIEW == table_type_ || share::schema::ObTableType::SYSTEM_VIEW == table_type_ || share::schema::ObTableType::MATERIALIZED_VIEW == table_type_; }

  int generate_str(const share::schema::ObTableSchema &input_schema);
  int generate_column_array(const share::schema::ObTableSchema &input_schema);
  int generate_column_group_array(const share::schema::ObTableSchema &input_schema, common::ObIAllocator &allocator);
  int get_column_ids_without_rowkey(
      common::ObIArray<share::schema::ObColDesc> &column_ids,
      bool no_virtual) const;
  void reset_string(ObString &str);
  int64_t get_store_column_count_by_column_array();

  /* serialize related function */
  template <typename T>
  int serialize_schema_array(
      char *buf, const int64_t data_len, int64_t &pos, const common::ObIArray<T> &array) const;
  int serialize_column_array(char *buf, const int64_t data_len, int64_t &pos) const;
  int deserialize_rowkey_column_array(const char *buf, const int64_t data_len, int64_t &pos);
  int deserialize_column_array(ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int deserialize_column_group_array(ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_column_array_serialize_length(const common::ObIArray<ObStorageColumnSchema> &array) const;
  int deserialize_skip_idx_attr_array(const char *buf, const int64_t data_len, int64_t &pos);
  int generate_all_column_group_schema(ObStorageColumnGroupSchema &column_group, const ObRowStoreType row_store_type) const;
  template <typename T>
  int64_t get_array_serialize_length(const common::ObIArray<T> &array) const;
  template <typename T>
  bool check_column_array_valid(const common::ObIArray<T> &array) const;

public:
  static const uint32_t INVALID_ID = UINT32_MAX;
  // The compatibility of the ObRowkeyColumnSchema&ObColumnSchema uses the version_ of the ObStorageSchema
  static const int32_t SS_ONE_BIT = 1;
  static const int32_t SS_HALF_BYTE = 4;
  static const int32_t SS_ONE_BYTE = 8;
  static const int32_t SS_RESERVED_BITS = 18;

  // STORAGE_SCHEMA_VERSION is for serde compatibility.
  // Currently we do not use "standard" serde function macro,
  // because we add "allocator" param in deserialize function,
  // so we should handle compatibility in the specified deserialize function,
  // thus we add a static variable STORAGE_SCHEMA_VERSION and a class member storage_schema_version_ here.
  // Compatibility code should be added if new variables occur in future
  static const int64_t STORAGE_SCHEMA_VERSION = 1;
  static const int64_t STORAGE_SCHEMA_VERSION_V2 = 2; // add for store_column_cnt_
  static const int64_t STORAGE_SCHEMA_VERSION_V3 = 3; // add for cg_group

  common::ObIAllocator *allocator_;
  int64_t storage_schema_version_;

  union {
    uint32_t info_;
    struct
    {
      uint32_t version_             :SS_ONE_BYTE;
      uint32_t compat_mode_         :SS_HALF_BYTE;
      uint32_t is_use_bloomfilter_  :SS_ONE_BIT;
      uint32_t column_info_simplified_ :SS_ONE_BIT;
      uint32_t reserved_            :SS_RESERVED_BITS;
    };
  };
  share::schema::ObTableType table_type_;
  share::schema::ObTableMode table_mode_;
  share::schema::ObIndexType index_type_;
  ObRowStoreType row_store_type_;
  int64_t schema_version_;
  int64_t column_cnt_; // include virtual generated column
  int64_t tablet_size_;
  int64_t pctfree_;
  int64_t block_size_; //KB
  int64_t progressive_merge_round_;
  int64_t progressive_merge_num_;
  uint64_t master_key_id_; // for encryption
  ObCompressorType compressor_type_;
  ObString encryption_; // for encryption
  ObString encrypt_key_; // for encryption
  common::ObFixedArray<ObStorageRowkeyColumnSchema, common::ObIAllocator> rowkey_array_; // rowkey column
  common::ObFixedArray<ObStorageColumnSchema, common::ObIAllocator> column_array_; // column schema
  common::ObFixedArray<ObStorageColumnGroupSchema, common::ObIAllocator> column_group_array_; // column group schema
  common::ObFixedArray<share::schema::ObSkipIndexAttrWithId, common::ObIAllocator> skip_idx_attr_array_;
  int64_t store_column_cnt_; // NOT include virtual generated column
  bool has_all_column_group_; // for column store, no need to serialize
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageSchema);
};

class ObCreateTabletSchema : public ObStorageSchema
{
public:
  ObCreateTabletSchema()
    : ObStorageSchema(),
      table_id_(common::OB_INVALID_ID),
      index_status_(share::schema::ObIndexStatus::INDEX_STATUS_UNAVAILABLE),
      truncate_version_(OB_INVALID_VERSION)
      {}

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  inline bool can_read_index() const
  { return share::schema::INDEX_STATUS_AVAILABLE == index_status_; }
  uint64_t get_table_id () const
  { return table_id_; }
  int64_t get_truncate_version() const
  { return truncate_version_; }
  bool is_valid() const
  {
    return ObStorageSchema::is_valid() && common::OB_INVALID_ID != table_id_;
  }
  int init(common::ObIAllocator &allocator,
      const share::schema::ObTableSchema &input_schema,
      const lib::Worker::CompatMode compat_mode,
      const bool skip_column_info,
      const int64_t compat_version);
  int init(common::ObIAllocator &allocator,
      const ObCreateTabletSchema &old_schema);
  INHERIT_TO_STRING_KV("ObStorageSchema", ObStorageSchema, K_(table_id), K_(index_status), K_(truncate_version));
private:
  // for cdc
  uint64_t table_id_;
  // for create index
  share::schema::ObIndexStatus index_status_;
  // for tablet throttling
  int64_t truncate_version_;
};

template <typename T>
int ObStorageSchema::serialize_schema_array(
    char *buf, const int64_t data_len, int64_t &pos, const common::ObIArray<T> &array) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, array.count()))) {
    STORAGE_LOG(WARN, "Fail to encode column count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
    if (OB_FAIL(array.at(i).serialize(buf, data_len, pos))) {
      STORAGE_LOG(WARN, "Fail to serialize column", K(ret));
    }
  }
  return ret;
}

template <typename T>
int64_t ObStorageSchema::get_array_serialize_length(const common::ObIArray<T> &array) const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(array.count());
  for (int64_t i = 0; i < array.count(); ++i) {
    len += array.at(i).get_serialize_size();
  }
  return len;
}

template <typename T>
bool ObStorageSchema::check_column_array_valid(const common::ObIArray<T> &array) const
{
  bool valid_ret = true;
  for (int64_t i = 0; valid_ret && i < array.count(); ++i) {
    if (!array.at(i).is_valid()) {
      valid_ret = false;
      STORAGE_LOG_RET(WARN, OB_INVALID_ERROR, "column item is invalid", K(i), K(array.at(i)));
    }
  }
  return valid_ret;
}

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_STORAGE_SCHEMA_ */
