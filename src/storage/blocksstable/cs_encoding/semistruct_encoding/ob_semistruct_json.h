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

class ObSemiJsonNode;

class ObSemiObjectPair final
{
public:
ObSemiObjectPair() {}
  explicit ObSemiObjectPair(const ObString &key, ObSemiJsonNode *value)
      : key_(key),
        value_(value)
  {
  }
  ~ObSemiObjectPair() {}
  OB_INLINE common::ObString get_key() const { return key_; }
  OB_INLINE ObSemiJsonNode *get_value() const { return value_; }
  TO_STRING_KV(K_(key), KPC(value_));
private:
  common::ObString key_;
  ObSemiJsonNode *value_;
};

typedef common::ObList<ObSemiObjectPair, common::ObIAllocator> ObJsonObjectList;
typedef common::ObList<ObSemiJsonNode*, common::ObIAllocator> ObJsonArrayList;

class ObSemiSchemaNode
{
public:
  ObSemiSchemaNode():
    path_cnt_(1),
    col_id_(0),
    need_stop_visit_(false)
  {}

  virtual ~ObSemiSchemaNode() {}

  virtual ObJsonNodeType json_type() const = 0;
  virtual ObObjType obj_type() const = 0;
  virtual uint32_t get_path_cnt() { return path_cnt_; }
  virtual void inc_path_cnt() { path_cnt_++; }
  virtual void set_col_id(uint16_t col_id) { col_id_ = col_id; }
  uint16_t get_col_id() const { return col_id_; }
  virtual bool is_freq_column() const { return false; }
  virtual ObPrecision get_precision() const { return 0; }
  virtual ObScale get_scale() const { return 0; }
  bool check_need_stop_visit(int row_cnt, uint8_t threshold) {
    need_stop_visit_ = path_cnt_ * 100 / row_cnt < threshold;
    return need_stop_visit_;
  }
  bool get_need_stop_visit() const { return need_stop_visit_; }
  TO_STRING_KV(K_(path_cnt), K_(col_id), K_(need_stop_visit));

private:
  uint32_t path_cnt_;
  uint16_t col_id_;
  bool need_stop_visit_;
};

class ObSemiSchemaObject : public ObSemiSchemaNode
{
public:
  ObSemiSchemaObject(ObIAllocator &allocator):
    ObSemiSchemaNode(),
    object_list_(allocator),
    has_nop_(false)
  {}

  ~ObSemiSchemaObject() {}
  ObJsonObjectList &get_object_list() { return object_list_; }
  virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_OBJECT; }
  virtual ObObjType obj_type() const override { return ObJsonType; }
  bool has_nop() const { return has_nop_; }
  void set_has_nop() { has_nop_ = true; }
  INHERIT_TO_STRING_KV("ObSemiSchemaNode", ObSemiSchemaNode, K_(object_list), K_(has_nop));

private:
  ObJsonObjectList object_list_;
  bool has_nop_;
};

class ObSemiSchemaArray : public ObSemiSchemaNode
{
public:
  friend class ObJsonReassembler;
public:
  ObSemiSchemaArray(ObIAllocator &allocator):
    ObSemiSchemaNode(),
    array_list_(allocator),
    has_nop_(false)
  {}

  ~ObSemiSchemaArray() {}

  OB_INLINE ObJsonArrayList &get_array_list() { return array_list_; }
  virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_ARRAY; }
  virtual ObObjType obj_type() const override { return ObJsonType; }
  bool has_nop() const { return has_nop_; }
  void set_has_nop() { has_nop_ = true; }
  INHERIT_TO_STRING_KV("ObSemiSchemaNode", ObSemiSchemaNode, K_(array_list), K_(has_nop));

private:
  ObJsonArrayList array_list_;
  bool has_nop_;
};

class ObSemiSchemaScalar : public ObSemiSchemaNode
{
public:
  ObSemiSchemaScalar(const ObObjType &obj_type, const ObJsonNodeType &json_type):
    ObSemiSchemaNode(),
    obj_type_(obj_type),
    json_type_(json_type),
    min_len_(-1),
    max_len_(-1),
    prec_(INT16_MIN),
    scale_(INT16_MIN),
    is_freq_column_(false)
  {}

  virtual ~ObSemiSchemaScalar() {}
  virtual ObObjType obj_type() const override { return obj_type_; }
  void set_json_type(const ObJsonNodeType &json_type) { json_type_ = json_type; }
  virtual ObJsonNodeType json_type() const override { return json_type_; }
  void set_obj_type(const ObObjType obj_type) {  obj_type_ = obj_type; }
  void set_freq_column(bool is_freq_column) { is_freq_column_ = is_freq_column; }
  virtual bool is_freq_column() const override { return is_freq_column_; }
  OB_INLINE int32_t get_min_len() const { return min_len_; }
  OB_INLINE int32_t get_max_len() const { return max_len_; }
  void set_min_len(int32_t min_len) { min_len_ = min_len; }
  void set_max_len(int32_t max_len) { max_len_ = max_len; }
  virtual ObPrecision get_precision() const override { return prec_; }
  virtual ObScale get_scale() const override { return scale_; }
  void set_precision_and_scale(const ObPrecision prec, const ObScale scale) { prec_ = prec; scale_ = scale; }
  INHERIT_TO_STRING_KV("ObSemiSchemaNode", ObSemiSchemaNode, K_(obj_type), K_(json_type), K_(min_len), K_(max_len), K_(prec), K_(scale), K_(is_freq_column));
private:
  ObObjType obj_type_;
  ObJsonNodeType json_type_;
  int32_t min_len_;
  int32_t max_len_;
  ObPrecision prec_;
  ObScale scale_;
  bool is_freq_column_;
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

  ObSemiStructSubColumn(ObJsonNodeType json_type, ObObjType obj_type):
  flags_(0),
  json_type_(json_type),
  obj_type_(obj_type),
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
class ObSemiJsonNode
{
public:
  ObSemiJsonNode(ObIAllocator &allocator) :
    scalar_node_(nullptr),
    object_node_(nullptr),
    array_node_(nullptr),
    allocator_(allocator)
  {}

  ~ObSemiJsonNode()
  {
    reset();
  }

  void reset() {
    OB_DELETEx(ObSemiSchemaScalar, &allocator_, scalar_node_);
    OB_DELETEx(ObSemiSchemaObject, &allocator_, object_node_);
    OB_DELETEx(ObSemiSchemaArray, &allocator_, array_node_);
    use_lexicographical_order_ = false;
  }

public:
  bool contain_json_type(ObJsonNodeType json_type);
  void set_node(ObSemiSchemaNode *json_node);
  int inc_path_cnt(ObJsonNodeType &json_type);
  bool is_heterogeneous_column();
  TO_STRING_KV(K_(use_lexicographical_order), KPC(scalar_node_), KPC(object_node_), KPC(array_node_));

public:
  ObSemiSchemaScalar *scalar_node_;
  ObSemiSchemaObject *object_node_;
  ObSemiSchemaArray *array_node_;

private:
  ObIAllocator &allocator_;
  bool use_lexicographical_order_;
};

struct ObSemiSchemaInfo
{
  OB_UNIS_VERSION(1);
public:
  ObSemiSchemaInfo() : flags_(0) {}
  ObSemiSchemaInfo(int64_t flags) : flags_(flags) {}
  ~ObSemiSchemaInfo() {}
  ObJsonNodeType json_type() const { return ObJsonNodeType(json_type_); }
  int handle_string_to_uint(const int32_t len);

  union {
    struct {
      int64_t json_type_ : 8;
      int64_t obj_type_ : 8;
      int64_t is_freq_column_ : 1;
      int64_t prec_ : 16;
      int64_t scale_ : 16;
      int64_t reserved_ : 15;
    };
    int64_t flags_;
  };
  TO_STRING_KV(K_(json_type), K_(obj_type), K_(is_freq_column), K_(prec), K_(scale), K_(reserved));
};
static_assert(sizeof(ObSemiSchemaInfo) == 8, "size of ObSemiSchemaInfo isn't equal to 8");

class ObJsonSchemaFlatter;
class ObSemiSchemaAbstract
{
public:
  ObSemiSchemaAbstract(uint8_t version) :
    version_(version),
    allocator_("SemiSchema", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    base_node_(allocator_),
    is_inited_(false)
  {}

  virtual ~ObSemiSchemaAbstract() {}
  virtual void reset() {
    base_node_.reset();
    allocator_.reset();
    is_inited_ = false;
  }
  virtual uint16_t get_store_column_cnt() const = 0;
  virtual int get_sub_column_type(const uint16_t column_idx, ObObjType &type) const = 0;
  virtual uint16_t get_freq_column_cnt() const = 0;
  virtual int get_column_id(const share::ObSubColumnPath& path, uint16_t &sub_col_idx) const = 0;
  bool is_freq_column(uint16_t &sub_col_idx) {  return sub_col_idx >= 0 && sub_col_idx < get_freq_column_cnt(); }
  virtual bool use_lexicographical_order() const = 0;
  virtual void set_use_lexicographical_order(const bool v) = 0;
  virtual bool has_spare_column() const = 0;
  virtual int check_can_pushdown(const sql::ObSemiStructWhiteFilterNode &filter_node, uint16_t &sub_col_idx, bool &can_pushdown);
  virtual int encode(char *buf, const int64_t buf_len, int64_t &pos) const = 0;
  virtual int decode(const char *buf, const int64_t data_len, int64_t &pos) = 0;
  virtual int64_t get_encode_size() const = 0;
  OB_INLINE uint8_t get_version() const { return version_; }
  OB_INLINE ObIAllocator &get_allocator() { return allocator_; }
  OB_INLINE ObSemiJsonNode *get_base_node() { return &base_node_; }
  OB_INLINE bool is_inited() { return is_inited_; }
  void set_inited(bool v) { is_inited_ = v; }
  TO_STRING_KV(KP(this), K_(version), K(base_node_), K_(is_inited));

protected:
  uint8_t version_;
  ObArenaAllocator allocator_;
  ObSemiJsonNode base_node_;
  bool is_inited_;
};

class ObSemiNewSchema : public ObSemiSchemaAbstract
{
public:
  static const int64_t NEW_SCHEMA_VERSION = 2;
public:
  ObSemiNewSchema() :
    ObSemiSchemaAbstract(NEW_SCHEMA_VERSION),
    schema_header_(0),
    semi_schema_infos_(nullptr),
    schema_buf_()
  {}

  ~ObSemiNewSchema() {}

  virtual void reset() override;
  OB_INLINE uint16_t get_element_cnt() const { return element_cnt_; }
  virtual int get_sub_column_type(const uint16_t column_idx, ObObjType &type) const override;
  int get_column_id(const share::ObSubColumnPath& path, uint16_t &sub_col_idx) const override;
  OB_INLINE uint16_t get_freq_column_cnt() const override { return freq_column_cnt_; }
  OB_INLINE bool use_lexicographical_order() const override { return use_lexicographical_order_; }
  OB_INLINE bool col_id_valid(const uint16_t sub_col_id) const { return sub_col_id < element_cnt_; }
  int get_sub_column_json_type(const uint16_t sub_col_id, ObJsonNodeType &type) const;
  const ObSemiSchemaInfo *get_semi_schema_infos() const { return semi_schema_infos_; }
  OB_INLINE ObString get_schema_buf() const { return schema_buf_; }
  OB_INLINE int64_t get_schema_header() const { return schema_header_; }
  OB_INLINE uint16_t get_store_column_cnt() const override { return freq_column_cnt_ + (element_cnt_ != freq_column_cnt_ ? 1 : 0); }
  virtual bool has_spare_column() const override { return element_cnt_ != freq_column_cnt_; };
  OB_INLINE void set_element_cnt(uint16_t element_cnt) { element_cnt_ = element_cnt; }
  OB_INLINE void set_freq_column_cnt(uint16_t freq_column_cnt) { freq_column_cnt_ = freq_column_cnt; }
  OB_INLINE void set_use_lexicographical_order(const bool v) override { use_lexicographical_order_ = v; }
  OB_INLINE void set_schema_buf(ObString schema_buf) { schema_buf_.assign(schema_buf.ptr(), schema_buf.length()); }
  int set_schema_infos_ptr(ObSEArray<ObSemiSchemaInfo, 16> &schema_info_arr);

  int encode(char *buf, const int64_t buf_len, int64_t &pos) const override;
  int decode(const char *buf, const int64_t data_len, int64_t &pos) override;
  int64_t get_encode_size() const override;

  INHERIT_TO_STRING_KV("ObSemiSchemaAbstract", ObSemiSchemaAbstract, K_(element_cnt), K_(freq_column_cnt),
                K_(use_lexicographical_order), K_(reserved), KPC_(semi_schema_infos), K_(schema_buf));

private:
  union {
    struct {
      int64_t element_cnt_ : 16;
      int64_t freq_column_cnt_ : 16;
      int64_t  use_lexicographical_order_: 1;
      int64_t reserved_ : 31;
    };
    int64_t schema_header_;
  };
  const ObSemiSchemaInfo *semi_schema_infos_;
  ObString schema_buf_;
};

class ObSemiStructSubSchema : public ObSemiSchemaAbstract
{
public:
  static const int64_t SCHEMA_VERSION = 1;
public:
  ObSemiStructSubSchema() :
    ObSemiSchemaAbstract(SCHEMA_VERSION),
    flags_(0),
    spare_col_(ObJsonNodeType::J_OBJECT, ObJsonType)
  {}
  ~ObSemiStructSubSchema() {}
  virtual void reset() override;
  int get_store_column(const int64_t idx, const ObSemiStructSubColumn*& sub_column) const;
  virtual int get_sub_column_type(const uint16_t column_idx, ObObjType &type) const override;
  int get_column_id(const share::ObSubColumnPath& path, uint16_t &sub_col_idx) const override;
  int get_key_str(const int64_t idx, ObString &key) const { return key_dict_.get(idx, key); }
  int get_key_id(const ObString &key, int64_t &idx) const { return key_dict_.get(key, idx); }
  int handle_string_to_uint(const int32_t len, ObSemiStructSubColumn& sub_column) const;
  int handle_null_type(ObSemiStructSubColumn& sub_column) const;
  int add_freq_column(const ObJsonSchemaFlatter &flatter, ObSemiSchemaScalar *node);
  int add_spare_column(const ObJsonSchemaFlatter &flatter, ObSemiSchemaNode *node);
  int make_column_path(const share::ObSubColumnPath& path, share::ObSubColumnPath& result) const;
  virtual uint16_t get_store_column_cnt() const override { return columns_.count() + has_spare_column(); }
  virtual uint16_t get_freq_column_cnt() const override { return columns_.count(); }
  int find_column(const ObIArray<ObSemiStructSubColumn>& cols, const share::ObSubColumnPath& path, const ObSemiStructSubColumn*& sub_column) const;
  virtual bool has_spare_column() const override { return spare_columns_.count() > 0; }
  const ObIArray<ObSemiStructSubColumn> &get_freq_columns() const { return columns_; }
  const ObIArray<ObSemiStructSubColumn> &get_spare_columns() const { return spare_columns_; }
  bool has_key_dict() const { return has_key_dict_; }
  ObSubSchemaKeyDict& get_key_dict() { return key_dict_; }
  virtual bool use_lexicographical_order() const override { return use_lexicographical_order_; }
  void set_use_lexicographical_order(const bool v) { use_lexicographical_order_ = v; }

  // serialize(encode) & deserialize(decode)
  int encode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int decode(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const override;

  INHERIT_TO_STRING_KV("ObSemiSchemaAbstract", ObSemiSchemaAbstract, KP(this), K_(has_key_dict),
                K_(use_lexicographical_order), K_(flags), K_(key_dict), K_(columns), K_(spare_columns));

private:
  // not serialize & deserialize
  union {
    struct {
      uint32_t has_key_dict_ : 1;
      uint32_t use_lexicographical_order_ : 1;
      uint32_t reserved_ : 30;
    };
    uint32_t flags_;
  };
  ObSubSchemaKeyDict key_dict_;
  ObSEArray<ObSemiStructSubColumn, 10> columns_;
  ObSEArray<ObSemiStructSubColumn, 10> spare_columns_;
  // not serialize & deserialize
  ObSemiStructSubColumn spare_col_;

};

class ObJsonBinVisitor
{
public:
  ObJsonBinVisitor(ObIAllocator *allocator, int64_t min_data_version):
    allocator_(allocator),
    ptr_(nullptr),
    len_(0),
    pos_(0),
    meta_(),
    json_type_(ObJsonNodeType::J_ERROR),
    total_cnt_(0),
    null_cnt_(0),
    visited_bytes_(0),
    use_lexicographical_order_(false),
    json_key_cmp_(use_lexicographical_order_),
    row_index_(0),
    freq_column_count_(0),
    min_data_version_(min_data_version)
  {}

  int init(const ObString& data);
  void reset();
  void reuse();

  virtual int visit(const ObString& data) = 0;
  virtual int do_visit(const ObString& data) = 0;
  int visit_value(ObSemiJsonNode *&json_node);
  virtual int visit_object(ObSemiSchemaObject *&json_node) = 0;
  virtual int visit_array(ObSemiSchemaArray *&json_node) = 0;
  virtual int visit_scalar(ObSemiSchemaScalar *&json_node) = 0;
  // virtual int handle(ObFlatJson &flat_json) = 0;
  int to_bin(ObJsonBin &bin);
  OB_INLINE bool get_boolean() const { return static_cast<bool>(uint_val_); }
  OB_INLINE double get_double() const { return double_val_; }
  OB_INLINE uint16_t get_freq_column_count() const { return freq_column_count_; }
  OB_INLINE void set_freq_column_count(uint16_t freq_column_count) { freq_column_count_ = freq_column_count; }
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
  bool get_use_lexicographical_order() const { return use_lexicographical_order_; }
  OB_INLINE ObPrecision get_decimal_precision() const { return prec_; }
  OB_INLINE ObScale get_decimal_scale() const { return scale_; }
  OB_INLINE uint64_t element_count() const { return meta_.element_count(); }
  void set_semi_json_node(ObSemiJsonNode *node) { base_node_ = node; }
  OB_INLINE ObJsonNodeType json_type() const { return meta_.json_type(); }

  int flat_datums(const ObColDatums &datums);
  int flat_datum(const ObDatum &datum);

protected:
  virtual int handle_null() = 0;
  int read_type();
  int deserialize_bin_header();
  int deserialize_doc_header();
  int deserialize();
  int deserialize_decimal();
  int deserialize_int();
  int deserialize_uint();
  int deserialize_string();
  int deserialize_opaque();
  int deserialize_boolean();
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
  ObStorageDatum datum_;
  ObSemiJsonNode *base_node_;
  ObJsonKeyCompare json_key_cmp_;
  int64_t row_index_;
  uint16_t freq_column_count_;
  int64_t min_data_version_;
};

class ObJsonDataFlatter : public ObJsonBinVisitor
{
public:
  static ObStorageDatum NOP_DATUM;

public:
  ObJsonDataFlatter(ObIAllocator *allocator, int64_t min_data_version):
    ObJsonBinVisitor(allocator, min_data_version),
    sub_schema_(nullptr),
    sub_col_datums_(nullptr),
    spare_col_(nullptr),
    spare_data_allocator_("SemiEncTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}
  void reset();
  int init(const ObSemiSchemaAbstract *sub_schema, ObArray<ObColDatums *> &sub_col_datums);
  virtual int visit(const ObString& data) override;
  virtual int do_visit(const ObString& data) override;
  virtual int visit_object(ObSemiSchemaObject *&json_node) override;
  virtual int visit_array(ObSemiSchemaArray *&json_node) override;
  virtual int visit_scalar(ObSemiSchemaScalar *&json_node) override;
  int handle_stop_container(uint16_t col_id, ObJsonNodeType json_type);

  TO_STRING_KV(KP(this), KP_(allocator), KPC_(sub_col_datums));

protected:
  // virtual int add(const ObFlatJson &flat_json);
  virtual int handle_null();

private:
  int copy_datum(ObDatum &src, ObDatum &dest);
  int gen_spare_key(const uint16_t col_id, ObString &key);
  int add_spare_col(ObSemiSchemaScalar *&scalar_node);
private:
  const ObSemiSchemaAbstract *sub_schema_;
  ObArray<ObColDatums *> *sub_col_datums_;
  ObJsonObject *spare_col_;
  ObArenaAllocator spare_data_allocator_;
};

class ObJsonSchemaFlatter : public ObJsonBinVisitor
{
public:
  ObJsonSchemaFlatter(ObIAllocator *allocator, ObIAllocator &schema_alloc, int64_t min_data_version):
    ObJsonBinVisitor(allocator, min_data_version),
    schema_alloc_(schema_alloc),
    row_cnt_(1),
    spare_col_idx_(0),
    schema_info_arr_(),
    freq_col_idx_(0)
  {}

  int init(const uint8_t freq_threshold);
  int visit(const ObString& data) override;
  int do_visit(const ObString& data) override;
  int alloc_node(ObIAllocator &allocator, const uint8_t obj_type, ObSemiSchemaNode *&node);
  virtual int visit_object(ObSemiSchemaObject *&json_node) override;
  virtual int visit_array(ObSemiSchemaArray *&json_node) override;
  virtual int visit_scalar(ObSemiSchemaScalar *&json_node) override;
  int build_sub_schema(ObSemiSchemaArray *array_node, ObSemiStructSubSchema &result);
  int build_sub_schema(ObSemiSchemaScalar *scalar_node, ObSemiStructSubSchema &result);
  int build_sub_schema(ObSemiSchemaObject *object_node, ObSemiStructSubSchema &result);
  int build_sub_schema(ObSemiJsonNode *json_node, ObSemiStructSubSchema &result);

// ---------------------------- for new schema --------------------------------
  int build_new_sub_schema(ObSemiNewSchema &new_schema, ObSemiSchemaArray *array_node, ObJsonNode &parent);
  int build_new_sub_schema(ObSemiNewSchema &new_schema, ObSemiSchemaObject *object_node, ObJsonNode &parent);
  int handle_hete_col(ObSemiNewSchema &new_schema, ObSemiJsonNode *semi_node, ObJsonHeteCol &parent);
  int build_schema_child_node(ObSemiNewSchema &new_schema, ObSemiJsonNode *json_node, ObJsonNode *&child_node);
  int build_sub_schema(ObSemiNewSchema &schema_str_buf);
  int add_schema_info(ObSemiSchemaNode &schema_node, ObSemiNewSchema &new_schema);
  const share::ObSubColumnPath get_path() const { return path_; }
  TO_STRING_KV(KP(this), KP_(allocator));
  void set_row_cnt(int64_t row_cnt) { row_cnt_ = row_cnt; }
  uint16_t get_store_column_count() { return freq_column_count_ + (spare_col_idx_ == freq_column_count_ ? 0 : 1); }

protected:
  // virtual int add(const ObFlatJson &flat_json);
  virtual int handle_null() { return OB_SUCCESS; };
  bool need_store_as_freq_column(int32_t cur_cnt) { return cur_cnt * 100 / (row_cnt_ - null_cnt_) >= freq_col_threshold_; }


private:
  ObIAllocator &schema_alloc_;
  uint8_t freq_col_threshold_;
  int64_t row_cnt_;
  uint16_t spare_col_idx_;
  share::ObSubColumnPath path_;
  ObSEArray<ObSemiSchemaInfo, 16> schema_info_arr_;
  uint16_t freq_col_idx_;
};

class ObSemiHetCol : public ObJsonNode
{
public:
  ObSemiHetCol(ObIAllocator *allocator)
    : ObJsonNode(allocator)
  {}

private:
  ObJsonScalar *scalar_node_;
  bool has_object_nop_;
  ObJsonObject *object_node_;
  bool has_array_nop_;
  ObJsonArray *array_node_;
};

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
  int init(ObPrecision prec, ObScale scale);
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
  ObJsonReassembler(ObSemiSchemaAbstract *sub_schema, ObIAllocator* decode_allocator):
    allocator_("SemiJson", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    tmp_allocator_("SemiJsonTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    decode_allocator_(decode_allocator),
    sub_schema_(sub_schema),
    json_(nullptr),
    is_inited_(false)
  {}
  void reset();
  int init();
  inline bool is_inited() const { return is_inited_; }
  int serialize(const ObDatumRow &row, ObString &result);

public:
  static int prepare_lob_common(ObJsonBuffer &result);

private:
  int fill_freq_column(const ObDatumRow &row);
  int fill_spare_column(const ObDatumRow &row);
  int alloc_container_node(const share::ObSubColumnPathItem& item, const int child_cnt, ObIJsonBase *&node);
  int alloc_container_node(int real_child_cnt, int child_cnt, ObJsonNodeType type, ObSemiNewSchema &schema,
            ObIJsonBase **child_schemas, ObString *keys,  ObJsonNode *&current);
  int alloc_scalar_json_node(const ObSemiStructSubColumn& sub_column, ObIJsonBase *&node);
  int alloc_scalar_json_node(uint16_t col_id, ObSemiNewSchema &schema, ObJsonNode *&node);
  int add_child(ObIJsonBase *parent, ObIJsonBase *child, const share::ObSubColumnPathItem &item);
  bool is_heterigeneous_column(const share::ObSubColumnPathItem &item, ObIJsonBase *&current);
  int reassemble(const int start, const int end, const int depth, ObIJsonBase *&current, int &real_end);
  int merge_sub_cols();
  int deserialize_new_schema();
  int json_to_schema(ObIJsonBase &json_base, ObSemiNewSchema &semi_schema, ObJsonNode **&current);
  int build_schema_tree(ObIJsonBase &json_base, ObSemiNewSchema &semi_schema, ObIJsonBase *&parent);
  int reshape(ObIJsonBase *node);
  bool has_value(ObIJsonBase *node) const;

private:
  ObArenaAllocator allocator_;
  ObArenaAllocator tmp_allocator_;
  ObIAllocator* decode_allocator_;
  ObSemiSchemaAbstract *sub_schema_;
  ObIJsonBase *json_;
  ObSEArray<ObSemiStructScalar*, 10> leaves_;
  ObSEArray<ObSemiStructScalar*, 10> spare_leaves_;
  ObSEArray<const ObSemiStructSubColumn*, 10> sub_cols_;
  bool is_inited_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_SEMISTRUCT_JSON_H_