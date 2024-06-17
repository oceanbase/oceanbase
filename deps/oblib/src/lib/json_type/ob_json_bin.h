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

#ifndef OCEANBASE_SQL_OB_JSON_BIN
#define OCEANBASE_SQL_OB_JSON_BIN

#include "ob_json_common.h"
#include "ob_json_base.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_obj_type.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/number/ob_number_v2.h" // for number::ObNumber
#include "lib/utility/ob_fast_convert.h"

namespace oceanbase {
namespace common {

class ObJsonNode;
class ObJsonObject;
class ObJsonArray;
class ObJsonDecimal;
class ObJsonPath;

#define OB_JSON_TYPE_INLINE_REV_MASK  (0x7F)
#define OB_JSON_TYPE_INLINE_MASK      (0x80)
#define OB_JSON_TYPE_IS_INLINE(origin_type)  ((OB_JSON_TYPE_INLINE_MASK & (origin_type)) != 0)
#define OB_JSON_TYPE_GET_INLINE(origin_type) (OB_JSON_TYPE_INLINE_REV_MASK & (origin_type))

#define OB_JSON_BIN_MAX_SERIALIZE_TIME 2
enum ObJsonBinLenSize:uint8_t {
  JBLS_UINT8 = 0,
  JBLS_UINT16 = 1,
  JBLS_UINT32 = 2,
  JBLS_UINT64 = 3,
  JBLS_MAX = 4,
};

enum ObJBVerType:uint8_t {
  J_NULL_V0, // 0
  J_DECIMAL_V0,
  J_INT_V0,
  J_UINT_V0,
  J_DOUBLE_V0,
  J_STRING_V0, // 5
  J_OBJECT_V0,
  J_ARRAY_V0,
  J_BOOLEAN_V0,
  J_DATE_V0,
  J_TIME_V0, // 10
  J_DATETIME_V0,
  J_TIMESTAMP_V0,
  J_OPAQUE_V0, // 13

  J_OFLOAT_V0 = 15,
  J_ODOUBLE_V0 = 16,
  J_ODECIMAL_V0 = 17,
  J_OINT_V0 = 18,
  J_OLONG_V0 = 19,
  J_OBINARY_V0 = 20,
  J_OOID_V0 = 21,
  J_ORAWHEX_V0 = 22,
  J_ORAWID_V0 = 23,
  J_ORACLEDATE_V0 = 24,
  J_ODATE_V0 = 25,
  J_OTIMESTAMP_V0 = 26,
  J_OTIMESTAMPTZ_V0 = 27,
  J_ODAYSECOND_V0 = 28,
  J_OYEARMONTH_V0 = 29,
  J_DOC_HEADER_V0 = 30,
  J_FORWARD_V0 = 31,

  J_ERROR_V0 = 200
};

typedef struct ObJsonBinKeyDict {
  uint64_t dict_size_; // varsize
  uint64_t next_offset_;
  uint64_t key_count_;
  char data[]; // {keyhash_array:uint32[]}{key_offset_array:keyEntry[]}{keys}
} ObJsonBinKeyDict;

typedef struct ObJsonBinHeader {
  ObJsonBinHeader()
      : type_(0),
        entry_size_(0),
        count_size_(0),
        obj_size_size_(0),
        is_continuous_(0),
        reserved_(0)
  {
  }
  uint8_t type_;			 // node type for current node
  uint8_t entry_size_   : 2; // the size describe var size of key_entry，val_entry
  uint8_t count_size_   : 2; // the size describe var size of element count
  uint8_t obj_size_size_ : 2; // the size describe var size of key_entry，val_entry
  uint8_t is_continuous_  : 1; // memory of current node and subtree is continous 
  uint8_t reserved_   : 1; // reserved bit
  char used_size_[]; // var size

public:
  TO_STRING_KV(
      K(type_),
      K(entry_size_),
      K(count_size_),
      K(obj_size_size_),
      K(is_continuous_),
      K(reserved_));

} ObJsonBinHeader;


struct ObJsonBinDocHeader
{
  uint64_t type_ : 8;
  uint64_t reserved_ : 14;
  uint64_t extend_seg_offset_ : 42;

  TO_STRING_KV(
    K(type_),
    K(reserved_),
    K(extend_seg_offset_));

  ObJsonBinDocHeader() :
      type_(J_DOC_HEADER_V0),
      reserved_(0),
      extend_seg_offset_(0)
  {}
};

typedef ObJsonBinHeader ObJsonBinObjHeader;
typedef ObJsonBinHeader ObJsonBinArrHeader;

static const int OB_JSON_BIN_HEADER_LEN = 2; // actual size of ObJsonBinHeader
static const int OB_JSON_BIN_OBJ_HEADER_LEN = 2; // actual size of ObJsonBinObjHeader
static const int OB_JSON_BIN_ARR_HEADER_LEN = 2; // actual size of ObJsonBinArrHeader

static const int OB_JSON_BIN_VALUE_TYPE_LEN = sizeof(uint8_t);

class ObJsonVerType {
public:
  static ObJsonNodeType get_json_type(ObJBVerType type);
  static bool is_opaque_or_string(ObJsonNodeType type);
  static ObJBVerType get_json_vertype(ObJsonNodeType type);
  static bool is_array(ObJBVerType type);
  static bool is_object(ObJBVerType type);
  static bool is_custom(ObJBVerType type);
  static bool is_scalar(ObJBVerType type);
  static bool is_opaque_or_string(ObJBVerType type);
  static bool is_signed_online_integer(uint8_t type);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonVerType);
};

class ObJsonBinUpdateCtx;
struct ObJsonBinCtx
{
public:
  ObJsonBinCtx():
    extend_seg_offset_(0),
    update_ctx_(nullptr),
    is_update_ctx_alloc_(false)
  {}

  ~ObJsonBinCtx();

  TO_STRING_KV(
    K(extend_seg_offset_), K(is_update_ctx_alloc_));

  int64_t extend_seg_offset_;
  ObJsonBinUpdateCtx *update_ctx_;
  bool is_update_ctx_alloc_;
};

struct ObJsonBinMeta
{
public:
  ObJsonBinMeta()
    :
      type_(0),
      entry_size_(0),
      count_size_(0),
      obj_size_size_(0),
      is_continuous_(0),
      element_count_(0),
      bytes_(0),
      obj_size_(0),
      key_offset_start_(0),
      value_offset_start_(0),
      field_type_(ObObjType::ObNullType),
      str_data_offset_(0)
    {}

  void reset()
  {
    type_ = 0;
    entry_size_ = 0;
    count_size_ = 0;
    obj_size_size_ = 0;
    is_continuous_ = 0;
    element_count_ = 0;
    bytes_ = 0;
    obj_size_ = 0;
    key_offset_start_ = 0;
    value_offset_start_ = 0;
    field_type_ = ObObjType::ObNullType;
    str_data_offset_ = 0;
  }

  void set_type(ObJBVerType vertype, bool is_inline)
  {
    uint8_t type = static_cast<uint8_t>(vertype);
    if (is_inline) {
      type |= OB_JSON_TYPE_INLINE_MASK;
    }
    type_ = type;
  }

  void set_type(uint8_t type)
  {
    type_ = type;
  }

  uint8_t get_type() const
  {
    return type_;
  }

  ObJsonNodeType json_type() const
  {
    return ObJsonVerType::get_json_type(vertype());
  }

  ObJBVerType vertype() const
  {
    return static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type_));
  }

  bool is_inline_vertype() const
  {
    return OB_JSON_TYPE_IS_INLINE(type_);
  }

  void set_is_continuous(bool is_continuous)
  {
    is_continuous_ = is_continuous;
  }
  bool is_continuous() const
  {
    return is_continuous_;
  }


  void set_entry_var_type(uint8_t var_type)
  {
    entry_size_ = var_type;
  }

  uint8_t entry_var_type() const
  {
    return entry_size_;
  }

  uint64_t entry_var_size() const;


  void set_obj_size_var_type(uint8_t var_type)
  {
    obj_size_size_ = var_type;
  }

  uint8_t obj_size_var_type() const
  {
    return obj_size_size_;
  }

  uint64_t obj_size_var_size() const;
  void set_obj_size(uint64_t size)
  {
    obj_size_ = size;
  }

  uint64_t obj_size() const
  {
    return obj_size_;
  }

  uint64_t get_obj_size_offset() const;

  uint64_t element_count() const { return element_count_; }
  void set_element_count(uint64_t count)
  {
    element_count_ = count;
  }

  void set_element_count_var_type(uint8_t var_type)
  {
    count_size_ = var_type;
  }

  uint8_t element_count_var_type() const
  {
    return count_size_;
  }

  uint64_t element_count_var_size() const;
  uint64_t get_element_count_offset() const;

  int to_header(ObJsonBinHeader &buffer);
  int to_header(ObJsonBuffer &buffer);

  int calc_entry_array();

  uint64_t get_value_entry_offset(int index) const;
  uint64_t get_key_entry_offset(int index) const;

public:
  TO_STRING_KV(
    K(type_),
    K(entry_size_),
    K(count_size_),
    K(obj_size_size_),
    K(is_continuous_),
    K(element_count_),
    K(bytes_),
    K(obj_size_),
    K(key_offset_start_),
    K(value_offset_start_),
    K(field_type_),
    K(str_data_offset_));

public:
  // ObJsonBinHeader header_;
  // use ObJsonBinHeader as filed will report error :
  //          'field 'header_' with variable sized type 'oceanbase::common::ObJsonBinHeader'
  //           not at the end of a struct or class is a GNU extension
  // so not used as field.
  // below field copy from ObJsonBinHeader. not directly use it, use method instead.
  uint8_t type_;
  uint8_t entry_size_   : 2;
  uint8_t count_size_   : 2;
  uint8_t obj_size_size_ : 2;
  uint8_t is_continuous_  : 1;

  // elem count for obj or array, length for string or opaque
  uint64_t element_count_;
  uint64_t bytes_; // acutal used bytes for curr iter node, inlined node will set 0

  uint64_t obj_size_;
  uint64_t key_offset_start_;
  uint64_t value_offset_start_;
  ObObjType field_type_; // field type for opaque

  uint64_t str_data_offset_;
};

class ObJsonBinMetaParser {

public:
  ObJsonBinMetaParser(const ObILobCursor *cursor, int64_t offset, ObJsonBinMeta &meta)
    : cursor_(cursor),
      offset_(offset),
      meta_(meta) {}
  int parse();

private:
  int parse_type_();

  int parse_header_();

  int parse_header_v0_();

public:
  TO_STRING_KV(
      KP(cursor_),
      K(offset_));

private:
  const ObILobCursor *cursor_;
  int64_t offset_;
  ObJsonBinMeta &meta_;
};

class ObJsonBinSerializer;

class ObJsonBin : public ObIJsonBase
{
public:
  friend class ObJsonBinSerializer;
  ObJsonBin()
      : ObIJsonBase(nullptr),
        allocator_(nullptr),
        meta_(),
        cursor_(nullptr),
        local_cursor_(),
        pos_(0),
        node_stack_(nullptr),
        data_(nullptr),
        int_val_(0),
        ctx_(nullptr),
        is_alloc_ctx_(false),
        is_seek_only_(true),
        is_schema_(false)
  {
    cursor_ = &local_cursor_;
  }
  explicit ObJsonBin(ObIAllocator *allocator)
      : ObIJsonBase(allocator),
        allocator_(allocator),
        meta_(),
        cursor_(nullptr),
        local_cursor_(),
        pos_(0),
        node_stack_(allocator),
        data_(nullptr),
        int_val_(0),
        ctx_(nullptr),
        is_alloc_ctx_(false),
        is_seek_only_(true),
        is_schema_(false)
  {
    cursor_ = &local_cursor_;
  }

  explicit ObJsonBin(ObIAllocator *allocator, ObJsonBinCtx *ctx, bool is_alloc_ctx = false)
      : ObIJsonBase(allocator),
        allocator_(allocator),
        meta_(),
        cursor_(nullptr),
        local_cursor_(),
        pos_(0),
        node_stack_(allocator),
        data_(nullptr),
        int_val_(0),
        ctx_(ctx),
        is_alloc_ctx_(is_alloc_ctx),
        is_seek_only_(true),
        is_schema_(false)
  {
    cursor_ = &local_cursor_;
  }

  explicit ObJsonBin(const char *data, const int64_t length, ObIAllocator *allocator = nullptr)
      : ObIJsonBase(allocator),
        allocator_(allocator),
        meta_(),
        cursor_(nullptr),
        local_cursor_(data, length),
        pos_(0),
        node_stack_(allocator),
        data_(nullptr),
        int_val_(0),
        ctx_(0),
        is_alloc_ctx_(false),
        is_seek_only_(true),
        is_schema_(false)
  {
    cursor_ = &local_cursor_;
  }

  explicit ObJsonBin(const char *data, const int64_t length, ObJsonBinCtx *ctx)
      : ObIJsonBase(nullptr),
        allocator_(nullptr),
        meta_(),
        cursor_(nullptr),
        local_cursor_(data, length),
        pos_(0),
        node_stack_(nullptr),
        data_(nullptr),
        int_val_(0),
        ctx_(ctx),
        is_alloc_ctx_(false),
        is_seek_only_(true),
        is_schema_(false)
  {
    cursor_ = &local_cursor_;
  }

  virtual ~ObJsonBin() {
    destroy();
  }

  ObJsonBin(const ObJsonBin& other)
      : ObJsonBin()
  {
    assign(other);
  }

  void assign(const ObJsonBin& other) {
    meta_ = other.meta_;
    cursor_ = other.cursor_;
    local_cursor_ = other.local_cursor_;
    pos_ = other.pos_;
    // ToDo: check again, why need assign ?
    // node_stack_ = other.node_stack_;
    data_ = other.data_;
    int_val_ = other.int_val_;
    number_ = other.number_;
    prec_ = other.prec_;
    scale_ = other.scale_;
    ctx_ = other.ctx_;
    is_alloc_ctx_ = other.is_alloc_ctx_;
    is_seek_only_ = other.is_seek_only_;
    is_schema_ = other.is_schema_;
  }

  ObJsonBin& operator=(const ObJsonBin& other)
  {
    new (this) ObJsonBin();
    assign(other);
    return *this;
  }

  OB_INLINE bool get_boolean() const override { return static_cast<bool>(uint_val_); }
  OB_INLINE double get_double() const override { return double_val_; }
  OB_INLINE float get_float() const override { return float_val_; };
  OB_INLINE int64_t get_int() const override { return int_val_; }
  OB_INLINE uint64_t get_uint() const override { return uint_val_; }
  OB_INLINE uint64_t get_inline_value() const { return inline_value_; }
  //OB_INLINE const char *get_data() const override { return data_; }

  OB_INLINE uint64_t get_data_length() const override
  {
    uint64_t data_length = get_element_count();
    ObJsonNodeType type = json_type();
    if (type == ObJsonNodeType::J_ARRAY || type == ObJsonNodeType::J_OBJECT) {
      data_length = meta_.bytes_;
    }
    return data_length;
  }
  OB_INLINE number::ObNumber get_decimal_data() const override
  {
    number::ObNumber nmb;
    nmb.shadow_copy(number_);
    return nmb;
  }
  OB_INLINE ObPrecision get_decimal_precision() const override { return prec_; }
  OB_INLINE ObScale get_decimal_scale() const override { return scale_; }
  int get_obtime(ObTime &t) const override;
  OB_INLINE ObJBVerType get_vertype() const
  {
    return static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(get_type()));
  }

  OB_INLINE uint8_t get_type() const { return meta_.get_type(); }

  OB_INLINE bool is_inline_vertype() const
  {
    return OB_JSON_TYPE_IS_INLINE(get_type());
  }

  OB_INLINE ObJsonInType get_internal_type() const override { return ObJsonInType::JSON_BIN; }
  OB_INLINE uint64_t element_count() const override { return meta_.element_count(); }
  OB_INLINE uint64_t get_element_count() const { return meta_.element_count(); }
  int get_used_bytes(uint64_t &size) const;
  OB_INLINE ObJsonNodeType json_type() const override
  {
    return static_cast<ObJsonNodeType>(ObJsonVerType::get_json_type(get_vertype()));
  }
  OB_INLINE ObObjType field_type() const override
  {
    return meta_.field_type_;
  }
  int get_total_value(ObStringBuffer &res) const;

  virtual uint64_t member_count() const override
  {
    return (meta_.get_type() == static_cast<uint8_t>(ObJsonNodeType::J_ARRAY) || meta_.get_type() == static_cast<uint8_t>(ObJsonNodeType::J_OBJECT)) ?
      element_count() : 1;
  }

  int get_array_element(uint64_t index, ObIJsonBase *&value) const override;
  int get_object_value(uint64_t index, ObIJsonBase *&value) const override;
  int get_object_value(const ObString &key, ObIJsonBase *&value) const override;
  int get_object_value(uint64_t index, ObString &key, ObIJsonBase *&value) const override;
  int get_key(uint64_t index, common::ObString &key_out) const override;
  int raw_binary(common::ObString &out, ObIAllocator *allocator) const;
  int get_raw_binary(common::ObString &out, ObIAllocator *allocator) const;
  int get_raw_binary_v0(common::ObString &out, ObIAllocator *allocator) const;
  int get_value_binary(ObString &out) const;
  int get_area_size(uint64_t& size) const;
  int get_serialize_size(uint64_t &size) const;
  int array_remove(uint64_t index) override;
  int object_remove(const common::ObString &key) override;
  int replace(const ObIJsonBase *old_node, ObIJsonBase *new_node) override;
  int array_append(ObIJsonBase *value) override;
  int array_insert(uint64_t index, ObIJsonBase *value) override;
  int object_add(const common::ObString &key, ObIJsonBase *value) override;
public:
  static OB_INLINE ObJBVerType get_null_vertype() { return J_NULL_V0; }
  static OB_INLINE ObJBVerType get_decimal_vertype() { return J_DECIMAL_V0; } 
  static OB_INLINE ObJBVerType get_int_vertype() { return J_INT_V0; }
  static OB_INLINE ObJBVerType get_uint_vertype() { return J_UINT_V0; }
  static OB_INLINE ObJBVerType get_double_vertype() { return J_DOUBLE_V0; }
  static OB_INLINE ObJBVerType get_string_vertype() { return J_STRING_V0; }
  static OB_INLINE ObJBVerType get_object_vertype() { return J_OBJECT_V0; }
  static OB_INLINE ObJBVerType get_array_vertype() { return J_ARRAY_V0; }
  static OB_INLINE ObJBVerType get_boolean_vertype() { return J_BOOLEAN_V0; }
  static OB_INLINE ObJBVerType get_date_vertype() { return J_DATE_V0; }
  static OB_INLINE ObJBVerType get_time_vertype() { return J_TIME_V0; }
  static OB_INLINE ObJBVerType get_datetime_vertype() { return J_DATETIME_V0; }
  static OB_INLINE ObJBVerType get_timestamp_vertype() { return J_TIMESTAMP_V0; }
  static OB_INLINE ObJBVerType get_opaque_vertype() { return J_OPAQUE_V0; }

  static OB_INLINE ObJBVerType get_ofloat_vertype() { return J_OFLOAT_V0; }
  static OB_INLINE ObJBVerType get_odouble_vertype() { return J_ODOUBLE_V0; }
  static OB_INLINE ObJBVerType get_odecimal_vertype() { return J_ODECIMAL_V0; }
  static OB_INLINE ObJBVerType get_oint_vertype() { return J_OINT_V0; }
  static OB_INLINE ObJBVerType get_olong_vertype() { return J_OLONG_V0; }
  static OB_INLINE ObJBVerType get_obinary_vertype() { return J_OBINARY_V0; }
  static OB_INLINE ObJBVerType get_ooid_vertype() { return J_OOID_V0; }
  static OB_INLINE ObJBVerType get_orawhex_vertype() { return J_ORAWHEX_V0; }
  static OB_INLINE ObJBVerType get_orawid_vertype() { return J_ORAWID_V0; }
  static OB_INLINE ObJBVerType get_oracledate_vertype() { return J_ORACLEDATE_V0; }
  static OB_INLINE ObJBVerType get_odate_vertype() { return J_ODATE_V0; }
  static OB_INLINE ObJBVerType get_otimestamp_vertype() { return J_OTIMESTAMP_V0; }
  static OB_INLINE ObJBVerType get_otimestamptz_vertype() { return J_OTIMESTAMPTZ_V0; }
  static OB_INLINE ObJBVerType get_ointervalDS_vertype() { return J_ODAYSECOND_V0; }
  static OB_INLINE ObJBVerType get_ointervalYM_vertype() { return J_OYEARMONTH_V0; }
  static OB_INLINE bool is_doc_header(uint8_t type) { return is_doc_header_v0(type); }
private:
  static OB_INLINE bool is_forward_v0(uint8_t type) { return J_FORWARD_V0 == type; }
  static OB_INLINE bool is_doc_header_v0(uint8_t type) { return J_DOC_HEADER_V0 == type; }
  static int add_doc_header_v0(ObJsonBuffer &buffer);
  static int set_doc_header_v0(ObJsonBuffer &buffer,int64_t extend_seg_offset);
  static int set_doc_header_v0(ObString &buffer, int64_t extend_seg_offset);
public:
  TO_STRING_KV(
    K(meta_),
    K(pos_),
    K(int_val_),
    K(uint_val_),
    K(double_val_),
    K(ctx_)
  );

  /*
  parse json tree to json bin
  @param[in] Json_tree 
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int parse_tree(ObJsonNode *json_tree);

  /*
  parse json bin to json tree
  @param[out] Json_tree
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int to_tree(ObJsonNode *&json_tree);

  /*
  get json binary raw data base on iter without copy
  @param[out] buf Json binary raw data
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int raw_binary_at_iter(ObString &buf) const;

  /* look up */
  /*
  Reset iter to root, root's type should be JsonObject or JsonArray or has root common header
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int reset_iter();

  /**
   * init current bin with type and data
   * type is vertype, but may inline
   * buffer is the data area
   * pos is start parsing point for current node
   * value_entry_var_type is used for inline value, indicate the data length need be read
  */
  int reset(
      const uint8_t type,
      const ObString &buffer,
      const int64_t pos,
      const uint8_t value_entry_var_type,
      ObJsonBinCtx *ctx);

  /**
   * reset current bin to specify type and pos
   * type and value_entry_var_type is used for inline value
  */
  int reset(const uint8_t type, const int64_t pos, const uint8_t value_entry_var_type);

  /**
   * reset current bin to specify pos
  */
  int reset(const int64_t pos);

  /**
   * init current bin with type and data
   * buf and len is the data area
   * pos is start parsing point for current node
   * must ensure the first byte is vertype
  */
  int reset(const ObString &buffer, int64_t pos, ObJsonBinCtx *ctx);
  int reset(ObJsonBinCtx *ctx, int64_t pos);

  /**
   * equal to reset , but buf will set curr_
  */
  int reset_child(
      ObJsonBin &child,
      const uint8_t child_type,
      const int64_t child_pos,
      const uint8_t value_entry_var_type) const;
  int reset_child(ObJsonBin &child, const int64_t child_pos) const;

  /*
  move iter to parent
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int move_parent_iter(); // move iter to parent

  int get_parent(ObIJsonBase *& parent) const override;

  /*
  move iter to child node by index
  @param[in] index   The index.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int element(size_t index);

  /*
  Get index by key
  @param[in] key   The key
  @param[out] idx  The index
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int lookup_index(const ObString &key, size_t *idx) const;

  /*
  Move iter to child node by key
  @param[in] key  The key.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int lookup(const ObString &key);

  /*
  Update child node by key, first try inplace update.
  @param[in] key       The key.
  @param[in] new_value New child node.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int update(const ObString &key, ObJsonBin *new_value);

  /*
  append array node
  @param[in] pos Insert position, iff pos = OB_JSON_TYPE_INSERT_LAST do append.
  @param[in] new_value New child node.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int append(ObJsonBin *new_value);

  /*
  add child node by key, first try update.
  @param[in] key       The key.
  @param[in] new_value New child node.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int add(const ObString &key, ObJsonBin *new_value);

  /*
  add array node
  @param[in] pos Insert position, iff pos = OB_JSON_TYPE_INSERT_LAST do append.
  @param[in] new_value New child node.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int insert(ObJsonBin *new_value, int64_t pos);

  /*
  append array node
  @param[in] pos Insert position, iff pos = OB_JSON_TYPE_INSERT_LAST do append.
  @param[in] new_value New child node.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int insert(const ObString &key, ObJsonBin *new_value, int64_t pos);

  /*
  Update child node by index, first try inplace update.
  @param[in] index     The index.
  @param[in] new_value New child node.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int update(int index, ObJsonBin *new_value);

  /*
  According to iter's location, remove child node by index
  @param[in] index The index.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int remove(size_t index);

  /*
  According to iter's location, remove child node by key
  @param[in] key  The key.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int remove(const ObString &key);

    /*
  Rebuild the whole json binary for reduce free space. After rebuild, iter will be reset to root
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int rebuild();

  /**
   * Rebuild the whole json binary to buffer
  */
  int rebuild(ObJsonBuffer &buffer) const;

  /*
  Rebuild the json binary at iter position, and copy to string
  This function won't change the data itself.
  @param[out] string   Output json binary data after rebuild.
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int rebuild_at_iter(ObJsonBuffer &string);

  /*
  get json binary free space
  @param[out] space Free space
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int get_free_space(size_t &space) const;

  // release resource
  void destroy();
  void set_is_schema(bool is_schema) {is_schema_ = is_schema;}
  virtual int reset();
  OB_INLINE const ObILobCursor* get_cursor() const { return cursor_; }
  OB_INLINE ObILobCursor* get_cursor() { return cursor_; }
  OB_INLINE void set_cursor(ObILobCursor *cursor) { cursor_ = cursor; }
  OB_INLINE ObJsonBinCtx* get_ctx() const { return ctx_; }
  OB_INLINE ObJsonBinUpdateCtx* get_update_ctx() const { return nullptr == ctx_ ? nullptr : ctx_->update_ctx_; }


  // get flag for json doc used only for search
  OB_INLINE bool get_seek_flag() const { return is_seek_only_; }
  // set flag for json doc
  OB_INLINE void set_seek_flag(bool is_seek_only) { is_seek_only_ = is_seek_only; }
  int clone_new_node(ObJsonBin*& res, common::ObIAllocator *allocator) const;
private:
  // used as stack
  struct ObJBNodeMeta {
    uint8_t ver_type_;   // version type, the value is ObJBVerType::members
    uint8_t size_type_;  // the obj_size_type of obj_size
    uint8_t entry_type_; // tht obj_offset_type of key_offset or value_offset, may bigger than size_type_
    uint8_t reserve;     // to align uint64_t
    uint32_t idx_;       // the index of array or object array
    uint64_t offset_;    // cur node offset from 
    uint64_t obj_size_;  // cur node total size
    ObJBNodeMeta(uint8_t ver_type, uint8_t size_type, uint8_t entry_type, uint32_t idx, uint64_t offset, uint64_t obj_size) :
                ver_type_(ver_type), size_type_(size_type), entry_type_(entry_type), reserve(0), idx_(idx), offset_(offset), obj_size_(obj_size) {}
    ObJBNodeMeta() : ver_type_(0), size_type_(0), entry_type_(0), reserve(0), idx_(0), offset_(0), obj_size_(0) {}
    ObJBNodeMeta(const ObJBNodeMeta& src): ver_type_(src.ver_type_), size_type_(src.size_type_), entry_type_(src.entry_type_), reserve(0),
                idx_(src.idx_), offset_(src.offset_), obj_size_(src.obj_size_) {}

    TO_STRING_KV(
        K(ver_type_),
        K(size_type_),
        K(entry_type_),
        K(reserve),
        K(idx_),
        K(offset_),
        K(obj_size_));
  };

  typedef struct ObJBNodeMeta ObJBNodeMeta;
  static const int64_t JB_PATH_NODE_LEN = sizeof(ObJBNodeMeta);
  static const int64_t OB_JSON_INSERT_LAST = -1;
  class ObJBNodeMetaStack {
  public:
    ObJBNodeMetaStack(ObIAllocator *allocator) : buf_(allocator) {}
    ~ObJBNodeMetaStack() {}

    void update(uint32_t idx, const ObJBNodeMeta& new_value);
    int copy(const ObJBNodeMetaStack& dst);
    int pop();
    int push(const ObJBNodeMeta& node);
    int at(uint32_t idx, ObJBNodeMeta& node) const;
    int32_t size() const;
    void reset();
    int back(ObJBNodeMeta& node, bool is_pop = false);
    int back(ObJBNodeMeta& node) const;
  private:
    ObJsonBuffer buf_;
  };

private:
  int add_v0(const ObString &key, ObJsonBin *new_value);
  int insert_internal_v0(ObJBNodeMeta& meta, int64_t pos, const ObString &key, ObJsonBin *new_value, ObJsonBuffer& result);
  int insert_v0(int64_t pos, const ObString& key, ObJsonBin *new_value);
  int object_remove_v0(size_t index);
  int array_remove_v0(size_t index);

  int update_v0(int index, ObJsonBin *new_value);
  int update_append_v0(int index, ObJsonBin *new_value, bool &is_update_append);
  int update_recursion(int index, ObJsonBin *new_value);
  int insert_recursion(int index, const ObString &new_key, ObJsonBin *new_value);
  int lookup_insert_postion(const ObString &key, size_t &idx) const;

  int deserialize_json_value(ObJsonNode *&json_tree);
  int deserialize_json_object_v0(ObJsonObject *object);
  int deserialize_json_object(ObJsonObject *object);
  int deserialize_json_array_v0(ObJsonArray *array);
  int deserialize_json_array(ObJsonArray *array);

  int serialize_number_to_json_decimal(number::ObNumber number, ObJsonBuffer &result);

  int calc_size_with_insert_new_value(const ObString &new_key, const ObJsonBin *new_value, ObJsonBinMeta &new_meta) const;
  int calc_size_with_new_value(const ObJsonBin *old_value, const ObJsonBin *new_value, ObJsonBinMeta &new_meta) const;

  int reset_root(const ObString &data);
  int rebuild_json_value(ObJsonBuffer &result) const;
  int rebuild_json_array_v0(ObJsonBuffer &result) const;
  int rebuild_json_array(ObJsonBuffer &result) const;
  int rebuild_json_object_v0(ObJsonBuffer &result) const;
  int rebuild_json_object(ObJsonBuffer &result) const;
  int rebuild_with_new_insert_value(int64_t index, const ObString &new_key, ObJsonBin *new_value, ObStringBuffer &result) const;
  int rebuild_with_new_value(int64_t index, ObJsonBin *new_value, ObStringBuffer &result) const;
  int extend_entry_var_type(
    const bool is_obj_type,
    const uint64_t element_count,
    const uint64_t old_size,
    uint8_t old_entry_var_type,
    uint8_t &new_entry_var_type,
    uint64_t &new_size) const;

  int try_update_inline(
      const int index,
      const ObJsonNode *value,
      bool &is_update_inline);
  int try_update_inline(
      const int index,
      const ObJsonBin *value,
      bool &is_update_inline);
  int try_update_inplace(
      int index,
      ObJsonBin *new_value,
      bool &is_update_inplace);
  int try_update_inplace_in_extend(
      int index,
      ObJsonBin *new_value,
      bool &is_update_inplace);
  int replace_value(const ObString &new_data);

  // use reset functions instrea, this is innner logic.
  int init_bin_data();
  int init_meta();
  int init_string_node();
  int init_string_node_v0();
  int init_opaque_node();
  int init_opaque_node_v0();

  int parse_doc_header_v0();
  int init_ctx();

  int get_element_v0(size_t index, uint64_t *get_addr_only);
  int get_element_in_array(size_t index, uint64_t *get_addr_only = NULL);
  int get_element_in_object(size_t index, uint64_t *get_addr_only = NULL);

  int get_key_in_object_v0(size_t i, ObString &key) const;
  int get_key_in_object(size_t i, ObString &key) const;

  int check_valid_object_op(ObIJsonBase *value) const;
  int check_valid_array_op(ObIJsonBase *value) const;
  int check_valid_object_op(uint64_t index) const;
  int check_valid_array_op(uint64_t index) const;
  int create_new_binary(ObIJsonBase *value, ObJsonBin *&new_bin) const;

  // data access layer function
  // used for key_entry and value_entry
  int get_key_entry(int index, uint64_t &key_offset, uint64_t &key_len) const;
  int get_value_entry(int index, uint64_t &value_offset, uint8_t &value_type) const;
  int64_t get_value_entry_size() const;
  int get_value(int index, ObJsonBin &value) const;
  int set_key_entry(int index, uint64_t key_offset, uint64_t key_len, bool check=true);
  int set_value_entry(int index, uint64_t value_offset, uint8_t value_type, bool check=true);
  OB_INLINE uint64_t get_value_entry_offset(int index) const { return meta_.get_value_entry_offset(index); }
  OB_INLINE uint64_t get_key_entry_offset(int index) const { return meta_.get_key_entry_offset(index); }
  OB_INLINE uint8_t entry_var_type() const { return meta_.entry_var_type(); }
  OB_INLINE uint64_t entry_var_size() const { return meta_.entry_var_size(); }

  OB_INLINE uint64_t obj_size() const { return meta_.obj_size(); }
  int set_obj_size(uint64_t obj_size);
  OB_INLINE uint64_t obj_size_var_size() const { return meta_.obj_size_var_size(); }
  OB_INLINE uint64_t get_obj_size_offset() const { return meta_.get_obj_size_offset(); }
  OB_INLINE uint8_t obj_size_var_type() const { return meta_.obj_size_var_type(); }
  OB_INLINE uint8_t element_count_var_type() const { return meta_.element_count_var_type(); }
  OB_INLINE uint64_t element_count_var_size() const { return meta_.element_count_var_size(); }
  OB_INLINE uint64_t get_element_count_offset() const { return meta_.get_element_count_offset(); }
  int set_element_count(uint64_t count);
  OB_INLINE uint64_t get_extend_seg_offset() const { return nullptr == ctx_ ? 0 : ctx_->extend_seg_offset_; }
  OB_INLINE void set_extend_seg_offset(uint64_t offset) { if (nullptr != ctx_) ctx_->extend_seg_offset_  = offset; }
  OB_INLINE uint64_t get_extend_value_offset(uint64_t offset) const { return get_extend_seg_offset() + offset; }
  static OB_INLINE bool need_type_prefix(const uint8_t value_type)
  {
    return value_type == ObJBVerType::J_ARRAY_V0 ||
        value_type == ObJBVerType::J_OBJECT_V0 ||
        ObJsonVerType::is_opaque_or_string(static_cast<ObJBVerType>(value_type));
  }

  int get_extend_value_type(uint64_t offset, uint8_t &value_type) const;

  // for json diff record
  int record_inline_update_offset(int index);
  int record_inplace_update_offset(int index, ObJsonBin *new_value, bool is_record_header_binary);
  int record_extend_inplace_update_offset(int index, int64_t value_offset, int64_t value_len, uint8_t value_type);
  int record_append_update_offset(int index, int64_t value_offset, int64_t value_len, uint8_t value_type);
  int record_remove_offset(int index);
  int record_insert_offset(int index, int64_t value_offset, int64_t value_len, uint8_t value_type);
  int get_json_path_at_iter(int index, ObString &path) const;

  int set_current(const ObString &data, int64_t offset);

  int parse_type_();
  int skip_type_byte_();
  int parse_doc_header_();
  bool is_empty_data() const;

  int rebuild_child_key(
      const int64_t index,
      const ObString& child_key,
      const int64_t key_offset,
      ObJsonBuffer& result);
  int rebuild_child(
      const int64_t index,
      const ObJsonBin& child_value,
      const int64_t value_offset,
      ObJsonBuffer& result);
  bool is_at_root() const
  {
    bool res = false;
    if (OB_ISNULL(ctx_) || ctx_->extend_seg_offset_ == 0) {
      res = (pos_ == 0 || pos_ == OB_JSON_BIN_VALUE_TYPE_LEN) && node_stack_.size() == 0;
    } else {
      res = pos_ == sizeof(ObJsonBinDocHeader) && node_stack_.size() == 0;
    }
    return res;
  }
  int init_cursor(const ObString &data);

public:
  int should_pack_diff(bool &is_should_pack) const;
  //TODO replace this with ObString
  const char *get_data() const override;
  int get_data(ObString &data);

/* data */
private:
  common::ObIAllocator *allocator_;

  ObJsonBinMeta meta_;
  ObILobCursor *cursor_;
  ObLobInRowCursor local_cursor_;
  int64_t pos_;
  // path node stack used
  ObJBNodeMetaStack node_stack_;

  // aux data field
  char *data_;
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
  ObJsonBinCtx* ctx_;
  bool is_alloc_ctx_;
  // json doc used only for search
  bool is_seek_only_;

  bool is_schema_;
  // ToDo:refine
  // DISALLOW_COPY_AND_ASSIGN(ObJsonBin);
};

class ObJsonVar {
public:
  static int read_var(const ObILobCursor *cursor, int64_t offset, uint8_t type, uint64_t *var);
  static int read_var(const ObILobCursor *cursor, int64_t offset, uint8_t type, int64_t *var);
  static int read_var(const char *data, uint8_t type, uint64_t *var);
  static int read_var(const ObString& buffer, uint8_t type, uint64_t *var);
  static int append_var(uint64_t var, uint8_t type, ObJsonBuffer &result);
  static int reserve_var(uint8_t type, ObJsonBuffer &result);
  static int set_var(uint64_t var, uint8_t type, char *pos); // fill var at pos
  static int set_var(ObILobCursor *cursor, int64_t offset, uint64_t var, uint8_t type);
  static uint64_t get_var_size(uint8_t type);
  static uint8_t get_var_type(uint64_t var);
  static int read_var(const char *data, uint8_t type, int64_t *var);
  static uint64_t var_int2uint(int64_t var);
  static int64_t var_uint2int(uint64_t var, uint8_t entry_size);
  static uint8_t get_var_type(int64_t var);

  static bool is_fit_var_type(uint64_t var, uint8_t type);
};


class ObJsonBinSerializer
{
public:
  ObJsonBinSerializer(ObIAllocator *allocator):
    allocator_(allocator),
    bin_ctx_()
  {}
  int serialize(ObJsonNode *json_tree, ObString &result);
  int serialize_json_object(ObJsonObject* object, ObJsonBuffer &result, uint32_t depth = 0);
  int serialize_json_array(ObJsonArray *array, ObJsonBuffer &result, uint32_t depth = 0);
  int serialize_json_value(ObJsonNode *json_tree, ObJsonBuffer &result);

public:
  static int serialize_json_integer(int64_t value, ObJsonBuffer &result);
  static int serialize_json_decimal(ObJsonDecimal *json_dec, ObJsonBuffer &result);

private:
  ObIAllocator *allocator_;
  ObJsonBinCtx bin_ctx_;

};

struct ObJsonBinCompare {
  int operator()(const ObJsonBin& left, const ObJsonBin& right)
  {
    int result = 0;
    left.compare(right, result);
    return result > 0 ? 1 : 0;
  }
};

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_JSON_BIN
