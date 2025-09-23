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

#ifndef OCEANBASE_ENCODING_OB_SEMISTRUCT_JSON_H_
#define OCEANBASE_ENCODING_OB_SEMISTRUCT_JSON_H_

#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_path.h"
#include "share/semistruct/ob_sub_column_path.h"
#include "storage/blocksstable/cs_encoding/ob_dict_encoding_hash_table.h"
#include "storage/blocksstable/cs_encoding/ob_cs_encoding_allocator.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

class ObFlatJson
{
public:
  ObFlatJson():
    path_(),
    json_type_(ObJsonNodeType::J_NULL),
    obj_type_(ObObjType::ObNullType),
    value_(),
    prec_(-1),
    scale_(-1)
  {}

  void reuse()
  {
    path_.reuse();
  }

  int add_path_item(const share::ObSubColumnPathItem::Type type, ObString name)
  {
    return path_.add_path_item(type, name);
  }
  int add_path_item(const share::ObSubColumnPathItem::Type type, int64_t array_idx)
  {
    return path_.add_path_item(type, array_idx);
  }
  void pop_back()
  {
    path_.pop_back();
  }

  int set_value(const ObJsonNodeType &json_type, const ObObjType& obj_type, const ObDatum& value)
  {
    json_type_ = json_type;
    obj_type_ = obj_type;
    value_ = value;
    return OB_SUCCESS;
  }

  OB_INLINE void set_precision(ObPrecision prec) { prec_ = prec; }
  OB_INLINE ObPrecision get_precision() const { return prec_; }
  OB_INLINE void set_scale(ObScale scale) { scale_ = scale; }
  OB_INLINE ObScale get_scale() const { return scale_; }

  const ObDatum& get_value() const { return value_; }
  const share::ObSubColumnPath& get_path() const { return path_; }
  ObObjType obj_type() const { return obj_type_; }
  ObJsonNodeType json_type() const { return json_type_; }
  TO_STRING_KV(K_(path), K_(json_type), K_(obj_type), K_(value), K_(prec), K_(scale));

private:
  share::ObSubColumnPath path_;
  ObJsonNodeType json_type_;
  ObObjType obj_type_;
  ObDatum value_;
  // used for number type
  ObPrecision prec_;
  ObScale scale_;
};

struct ObSemiStructSubColumn
{
public:
  ObSemiStructSubColumn():
    flags_(0),
    json_type_(ObJsonNodeType::J_NULL),
    obj_type_(),
    sub_col_id_(0),
    path_(),
    prec_(-1),
    scale_(-1)
  {}

  int init(const share::ObSubColumnPath& path, const ObJsonNodeType json_type, const ObObjType type, const int64_t sub_col_id);
  void reset() { path_.reset(); }
  const share::ObSubColumnPath& get_path() const { return  path_; }
  share::ObSubColumnPath& get_path() { return  path_; }
  ObObjType get_obj_type() const { return obj_type_; }
  void set_obj_type(const ObObjType type) { obj_type_ = type; }
  void set_precision_and_scale(const ObPrecision prec, const ObScale scale)
  { 
    prec_ = prec;
    scale_ = scale;
  }
  OB_INLINE ObPrecision get_precision() const { return prec_; }
  OB_INLINE ObScale get_scale() const { return scale_; }
  ObJsonNodeType get_json_type() const { return json_type_; }
  void set_json_type(const ObJsonNodeType type) { json_type_ = type; }
  int compare(const ObSemiStructSubColumn& other, const bool use_lexicographical_order) const { return path_.compare(other.path_, use_lexicographical_order); }
  int32_t get_col_id() const { return sub_col_id_; }
  void set_col_id(const int32_t col_id) { sub_col_id_ = col_id; }
  int deep_copy(ObIAllocator& allocator, const ObSemiStructSubColumn &other);
  void set_is_spare_storage() { is_spare_storage_ = true; }
  bool is_spare_storage() const { return is_spare_storage_; }
  int encode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int decode(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const;
  TO_STRING_KV(KP(this), K_(sub_col_id), K_(json_type), K_(obj_type), K_(path), K_(is_spare_storage), K_(reserved), K_(prec), K_(scale));

private:
  union {
    struct {
      int8_t is_spare_storage_ : 1;
      int8_t reserved_ : 7;
    };
    uint8_t flags_;
  };
  ObJsonNodeType json_type_;
  ObObjType obj_type_;
  int32_t sub_col_id_;
  share::ObSubColumnPath path_;

  ObPrecision prec_;
  ObScale scale_;
};

class ObSubSchemaKeyDict
{
public:
  OB_UNIS_VERSION(1);
public:
  ObSubSchemaKeyDict():
    use_lexicographical_order_(false)
  {}

  void reset();
  int get(const ObString &key, int64_t &id) const;
  int get(const int64_t id, ObString &key) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  ObSEArray<ObString, 10>& get_array() { return array_; }
  bool use_lexicographical_order() const { return use_lexicographical_order_; }
  void set_use_lexicographical_order(const bool v) { use_lexicographical_order_ = v;}

private:
  ObSEArray<ObString, 10> array_;
  // do not serialize and deserialize
  bool use_lexicographical_order_;
};

class ObSemiStructSubSchema;
class ObSimpleSubSchema
{
public:
  static const int32_t DEFAUTL_FREQ_COLUMN_THRESHOLD = 100;
  static inline bool need_store_as_spare_column(const int32_t ocur_cnt, const int32_t row_cnt, const int32_t threshold)
  {
    return ocur_cnt * 100 / row_cnt < threshold;
  }

  static bool is_int_len(const int len)
  {
    return sizeof(uint8_t) == len || sizeof(uint16_t) == len || sizeof(uint32_t) == len || sizeof(uint64_t) == len;
  }

public:
  struct ObSimpleSubColumn
  {
    ObSimpleSubColumn(): col_(nullptr), cnt_(0), min_len_(0), max_len_(0) {}
    ObSimpleSubColumn(ObSemiStructSubColumn *col, int32_t cnt): col_(col), cnt_(cnt), min_len_(0), max_len_(0) {}
    void reset();
    int compare(const ObSimpleSubColumn &other, const bool use_lexicographical_order) const { return col_->compare(*other.col_, use_lexicographical_order); }
    ObJsonNodeType get_json_type() const { return col_->get_json_type(); }
    ObObjType get_obj_type() const { return col_->get_obj_type(); }
    TO_STRING_KV(K_(cnt), KPC_(col), K_(min_len), K_(max_len));

    ObSemiStructSubColumn* col_;
    int32_t cnt_;
    int32_t min_len_;
    int32_t max_len_;
  };

public:
  ObSimpleSubSchema(ObIAllocator *allocator):
    allocator_(allocator),
    freq_col_threshold_(DEFAUTL_FREQ_COLUMN_THRESHOLD),
    use_lexicographical_order_(false),
    columns_(*allocator)
  {}

  ~ObSimpleSubSchema();
  void reuse();
  void reset();
  int64_t get_column_count() const { return columns_.size(); }
  ObList<ObSimpleSubColumn, common::ObIAllocator>& get_columns() { return columns_; }
  int add_column(const ObFlatJson &flat_json);
  int add_column(const share::ObSubColumnPath& path, const ObJsonNodeType json_type, const ObObjType obj_type);
  int merge(ObSimpleSubSchema &other);
  int build_sub_schema(ObSemiStructSubSchema& sub_schema, const int64_t row_cnt) const;
  int build_freq_and_spare_cols(ObIAllocator &allocator, ObIArray<ObSemiStructSubColumn> &freq_cols, ObIArray<ObSemiStructSubColumn> &spare_cols, const int64_t row_cnt) const;
  int build_key_dict(ObSemiStructSubSchema& sub_schema, const int64_t row_cnt) const;
  int encode_column_path_with_dict(ObSubSchemaKeyDict &key_dict, ObSemiStructSubColumn &sub_column) const;
  int handle_null_type(ObSemiStructSubColumn& sub_column) const;
  int handle_string_to_uint(const int32_t len, ObSemiStructSubColumn& sub_column) const;
  void set_use_lexicographical_order(const bool v) { use_lexicographical_order_ = v; }

  TO_STRING_KV(K_(columns));

private:
  ObIAllocator *allocator_;
  int32_t freq_col_threshold_;
  bool use_lexicographical_order_;
  ObList<ObSimpleSubColumn, common::ObIAllocator> columns_;
};

class ObSemiStructSubSchema
{
public:
  friend class ObSimpleSubSchema; 
  static const int64_t SCHEMA_VERSION = 1;

  ObSemiStructSubSchema():
    is_inited_(false),
    version_(SCHEMA_VERSION),
    flags_(0),
    allocator_("SemiSchema", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}
  ~ObSemiStructSubSchema() {}
  void reset();
  bool is_inited() const { return is_inited_; }
  int get_store_column(const int64_t idx, const ObSemiStructSubColumn*& sub_column) const;
  int get_column(const int64_t col_idx, const share::ObSubColumnPath& path, const ObSemiStructSubColumn*& sub_column) const;
  int get_column(const share::ObSubColumnPath& path, const ObSemiStructSubColumn*& sub_column) const;
  int get_key_str(const int64_t idx, ObString &key) const { return key_dict_.get(idx, key); }
  int get_key_id(const ObString &key, int64_t &idx) const { return key_dict_.get(key, idx); }
  int make_column_path(const share::ObSubColumnPath& path, share::ObSubColumnPath& result) const;
  int64_t get_store_column_count() const { return columns_.count() + has_spare_column(); }
  int64_t get_freq_column_count() const { return columns_.count(); }
  int64_t get_spare_column_count() const { return spare_columns_.count(); }
  int find_column(const ObIArray<ObSemiStructSubColumn>& cols, const share::ObSubColumnPath& path, const ObSemiStructSubColumn*& sub_column) const;
  bool has_spare_column() const { return spare_columns_.count() > 0; }
  const ObIArray<ObSemiStructSubColumn> &get_freq_columns() const { return columns_; }
  const ObIArray<ObSemiStructSubColumn> &get_spare_columns() const { return spare_columns_; }
  bool has_key_dict() const { return has_key_dict_; }
  const ObSubSchemaKeyDict& get_key_dict() const { return key_dict_; }
  bool use_lexicographical_order() const { return use_lexicographical_order_; }

  // serialize(encode) & deserialize(decode)
  int encode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int decode(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const;

  TO_STRING_KV(KP(this), K_(is_inited), K_(version), K_(has_key_dict), K_(use_lexicographical_order), K_(flags), K_(key_dict), K_(columns), K_(spare_columns));

private:
  // not serialize & deserialize
  bool is_inited_;
  uint8_t version_;
  union {
    struct {
      uint32_t has_key_dict_ : 1;
      uint32_t use_lexicographical_order_ : 1;
      uint32_t reserved_ : 30;
    };
    uint32_t flags_;
  };
  ObArenaAllocator allocator_;
  ObSubSchemaKeyDict key_dict_;
  ObSEArray<ObSemiStructSubColumn, 10> columns_;
  ObSEArray<ObSemiStructSubColumn, 10> spare_columns_;
  // not serialize & deserialize
  ObSemiStructSubColumn spare_col_;
};

class ObJsonBinVisitor
{
public:
  ObJsonBinVisitor(ObIAllocator *allocator):
    allocator_(allocator),
    ptr_(nullptr),
    len_(0),
    pos_(0),
    meta_(),
    json_type_(ObJsonNodeType::J_ERROR),
    total_cnt_(0),
    null_cnt_(0),
    visited_bytes_(0),
    use_lexicographical_order_(false)
  {}

  int init(const ObString& data);
  void reset();
  void reuse();

  virtual int visit(const ObString& data) = 0;
  int do_visit(const ObString& data, ObFlatJson &flat_json);
  int visit_value(ObFlatJson &flat_json);
  int visit_object(ObFlatJson &flat_json);
  int visit_array(ObFlatJson &flat_json);
  int visit_scalar(ObFlatJson &flat_json);
  // virtual int handle(ObFlatJson &flat_json) = 0;
  int to_bin(ObJsonBin &bin);
  OB_INLINE bool get_boolean() const { return static_cast<bool>(uint_val_); }
  OB_INLINE double get_double() const { return double_val_; }
  OB_INLINE float get_float() const { return float_val_; };
  OB_INLINE int64_t get_int() const { return int_val_; }
  OB_INLINE uint64_t get_uint() const { return uint_val_; }
  OB_INLINE const char *get_data() const { return data_; }
  OB_INLINE uint64_t get_data_length() const
  {
    uint64_t data_length = element_count();
    ObJsonNodeType type = json_type();
    if (type == ObJsonNodeType::J_ARRAY || type == ObJsonNodeType::J_OBJECT) {
      data_length = meta_.bytes_;
    }
    return data_length;
  }
  OB_INLINE number::ObNumber get_decimal_data() const
  {
    number::ObNumber nmb;
    nmb.shadow_copy(number_);
    return nmb;
  }
  OB_INLINE ObPrecision get_decimal_precision() const { return prec_; }
  OB_INLINE ObScale get_decimal_scale() const { return scale_; }
  OB_INLINE uint64_t element_count() const { return meta_.element_count(); }
  OB_INLINE ObJsonNodeType json_type() const { return meta_.json_type(); }

  int flat_datums(const ObColDatums &datums);
  int flat_datum(const ObDatum &datum);

protected:
  virtual int add(const ObFlatJson &flat_json) = 0;
  virtual int handle_null() = 0;
  virtual int add_object_key(const ObString &key, ObFlatJson &flat_json);

private:
  int read_type();

  int deserialize();
  int deserialize_decimal();
  int deserialize_int();
  int deserialize_uint();
  int deserialize_string();
  int deserialize_opaque();
  int deserialize_boolean();
  int deserialize_doc_header();
  int deserialize_bin_header();
  static int get_key_entry(const ObJsonBinMeta &meta, const char* buf_ptr, int index, ObString &key);
  static int get_value_entry(
      const ObJsonBinMeta &meta, const char* buf_ptr,
      int index, uint64_t &value_offset, uint8_t &value_type);


protected:
  ObIAllocator *allocator_;
  const char* ptr_;
  int64_t len_;
  int64_t pos_;
  ObJsonBinMeta meta_;
  ObJsonNodeType json_type_;

  // aux data field
  const char *data_;
  union {
    int64_t int_val_;
    uint64_t uint_val_;
    double double_val_;
    float float_val_;
    uint64_t inline_value_;
  };
  number::ObNumber number_;
  ObPrecision prec_;
  ObScale scale_;
  ObString key_;

  int64_t array_idx_;
  int32_t total_cnt_;
  int32_t null_cnt_;
  int64_t visited_bytes_;
  bool use_lexicographical_order_;
  ObFlatJson flat_json_;
  ObStorageDatum datum_;
};

class ObJsonDataFlatter : public ObJsonBinVisitor
{
public:
  ObJsonDataFlatter(ObIAllocator *allocator):
    ObJsonBinVisitor(allocator),
    sub_schema_(nullptr),
    sub_col_datums_(nullptr),
    col_cnt_(0),
    spare_data_allocator_("SemiEncTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    spare_col_(nullptr)
  {}

  int init(const ObSemiStructSubSchema& sub_schema, ObArray<ObColDatums *> &sub_col_datums);
  virtual int visit(const ObString& data);

  TO_STRING_KV(KP(this), KP_(allocator), KPC_(sub_schema), KPC_(sub_col_datums), K_(col_cnt));

protected:
  virtual int add(const ObFlatJson &flat_json);
  virtual int add_object_key(const ObString &key, ObFlatJson &flat_json);
  virtual int handle_null();

private:
  int add_spare_col(const ObFlatJson &flat_json, const ObSemiStructSubColumn &sub_column);
  int copy_datum(const ObFlatJson &flat_json, ObDatum &dest);
private:
  const ObSemiStructSubSchema *sub_schema_;
  ObArray<ObColDatums *> *sub_col_datums_;
  int32_t col_cnt_;
  ObArenaAllocator spare_data_allocator_;
  ObJsonObject *spare_col_;
};

class ObJsonSchemaFlatter : public ObJsonBinVisitor
{
public:
  ObJsonSchemaFlatter(ObIAllocator *allocator):
    ObJsonBinVisitor(allocator),
    sub_schema_(allocator),
    tmp_sub_schema_(allocator)
  {}

  int init();
  int build_sub_schema(ObSemiStructSubSchema &result) const;
  const ObSimpleSubSchema& get_sub_schema() const { return sub_schema_; }
  virtual int visit(const ObString& data);

  TO_STRING_KV(KP(this), KP_(allocator), K_(sub_schema), K_(tmp_sub_schema));

protected:
  virtual int add(const ObFlatJson &flat_json);
  virtual int handle_null() { return OB_SUCCESS; };

private:
  ObSimpleSubSchema sub_schema_;
  ObSimpleSubSchema tmp_sub_schema_;
};

class ObSemiStructScalar;
class ObSemiStructObject : public ObJsonNode
{
public:
  friend class ObJsonReassembler;
public:
  ObSemiStructObject(ObIAllocator *allocator):
    ObJsonNode(allocator),
    cap_(0),
    real_child_cnt_(0),
    child_cnt_(0),
    real_childs_(nullptr),
    childs_(nullptr),
    allocator_(allocator),
    use_lexicographical_order_(false)
  {}

  int init(const int cap);
  int get_key(uint64_t idx, ObString &key_out) const;
  ObJsonNode* get_value(uint64_t idx) const override;
  int object_add(const common::ObString &key, ObIJsonBase *value);

  OB_INLINE bool is_scalar() const { return false; }
  OB_INLINE uint32_t depth() const override { return 1; }
  OB_INLINE uint64_t element_count() const { return real_child_cnt_; }
  OB_INLINE virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_OBJECT; }
  uint64_t get_serialize_size();
  bool use_lexicographical_order() const override { return use_lexicographical_order_; }
  void set_use_lexicographical_order(const bool v) { use_lexicographical_order_ = v;}
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const { return nullptr; }

private:
  int32_t cap_;
  int32_t real_child_cnt_;
  int32_t child_cnt_;
  ObJsonObjectPair *real_childs_;
  ObJsonObjectPair *childs_;
  ObIAllocator *allocator_;
  bool use_lexicographical_order_;

};

class ObSemiStructArray : public ObJsonNode
{
public:
  friend class ObJsonReassembler;
public:
  ObSemiStructArray(ObIAllocator *allocator):
    ObJsonNode(allocator),
    cap_(0),
    real_child_cnt_(0),
    child_cnt_(0),
    real_childs_(nullptr),
    childs_(nullptr),
    allocator_(allocator)
  {}

  int init(const int cap);
  ObJsonNode* get_value(uint64_t idx) const override;
  int array_append(ObIJsonBase *value);

  OB_INLINE bool is_scalar() const { return false; }
  OB_INLINE uint32_t depth() const override { return 1; }
  OB_INLINE uint64_t element_count() const { return real_child_cnt_; }
  OB_INLINE virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_ARRAY; }
  uint64_t get_serialize_size();

  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const { return nullptr; }


private:
  int32_t cap_;
  int32_t real_child_cnt_;
  int32_t child_cnt_;
  ObJsonNode **real_childs_;
  ObJsonNode **childs_;
  ObIAllocator *allocator_;

};

class ObSemiStructScalar : public ObJsonNode
{
public:
  static ObJsonNull null_;

public:
  ObSemiStructScalar(ObIAllocator *allocator, const ObObjType &obj_type, const ObJsonNodeType &json_type):
    ObJsonNode(allocator),
    obj_type_(obj_type),
    json_type_(json_type),
    json_node_(nullptr),
    has_value_(false),
    datum_(),
    allocator_(allocator),
    uint64_val_(0),
    prec_(-1),
    scale_(-1)
  {}

  virtual ~ObSemiStructScalar() {}
  int init(const ObSemiStructSubColumn& sub_column);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  OB_INLINE bool is_scalar() const { return true; }
  OB_INLINE uint32_t depth() const override { return 1; }
  OB_INLINE uint64_t element_count() const { return 1; }
  virtual ObJsonNodeType json_type() const override { return json_type_; }
  uint64_t get_serialize_size() { return datum_.is_null() ? sizeof(char) : json_node_->get_serialize_size(); }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const { return nullptr; }

  int64_t get_int() const { return datum_.get_int(); }
  uint64_t get_uint() const { return datum_.get_uint(); }
  double get_double() const { return datum_.get_double(); }
  uint64_t get_data_length() const { return datum_.len_; }
  const char* get_data() const { return datum_.ptr_; }
  void set_no_value() { has_value_ = false; }
  bool has_value() const { return has_value_; }

  int set_value(const ObDatum &datum);
  int set_value(ObJsonBin &bin);
  ObJsonNode* get_inner_node() { return datum_.is_null() ? &null_ : json_node_; }
  int handle_int_to_string(ObJsonNode &base_node, const ObDatum &datum);
private:
  ObObjType obj_type_;
  ObJsonNodeType json_type_;
  ObJsonNode *json_node_;
  bool has_value_;
  ObDatum datum_;
  ObIAllocator *allocator_;
  union {
    uint8_t uint8_val_;
    uint16_t uint16_val_;
    uint32_t uint32_val_;
    uint64_t uint64_val_;
  };
  ObPrecision prec_;
  ObScale scale_;
};

class ObJsonReassembler
{
public:
  ObJsonReassembler(ObSemiStructSubSchema *sub_schema, ObIAllocator* decode_allocator):
    allocator_("SemiJson", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    tmp_allocator_("SemiJsonTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    decode_allocator_(decode_allocator),
    sub_schema_(sub_schema),
    json_(nullptr)
  {}
  void reset();
  int init();
  int serialize(const ObDatumRow &row, ObString &result);
  ObDatumRow& get_sub_row() { return sub_row_; }

public:
  static int prepare_lob_common(ObJsonBuffer &result);

private:
  int fill_freq_column(const ObDatumRow &row);
  int fill_spare_column(const ObDatumRow &row);
  int alloc_container_node(const share::ObSubColumnPathItem& item, const int child_cnt, ObIJsonBase *&node);
  int alloc_scalar_json_node(const ObSemiStructSubColumn& sub_column, ObIJsonBase *&node);
  int add_child(ObIJsonBase *parent, ObIJsonBase *child, const share::ObSubColumnPathItem &item);
  bool is_heterigeneous_column(const share::ObSubColumnPathItem &item, ObIJsonBase *&current);
  int reassemble(const int start, const int end, const int depth, ObIJsonBase *&current, int &real_end);
  int merge_sub_cols();
  int reshape(ObIJsonBase *node);
  bool has_value(ObIJsonBase *node) const;

private:
  ObArenaAllocator allocator_;
  ObArenaAllocator tmp_allocator_;
  ObIAllocator* decode_allocator_;
  ObSemiStructSubSchema *sub_schema_;
  ObIJsonBase *json_;
  ObSEArray<ObSemiStructScalar*, 10> leaves_;
  ObSEArray<ObSemiStructScalar*, 10> spare_leaves_;
  ObDatumRow sub_row_;
  ObSEArray<const ObSemiStructSubColumn*, 10> sub_cols_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_SEMISTRUCT_JSON_H_