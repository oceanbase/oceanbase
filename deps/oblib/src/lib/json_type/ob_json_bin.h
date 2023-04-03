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

  J_ERROR_V0 = 200
};

typedef struct ObJsonBinKeyDict {
  uint64_t dict_size_; // varsize
  uint64_t next_offset_;
  uint64_t key_count_;
  char data[]; // {keyhash_array:uint32[]}{key_offset_array:keyEntry[]}{keys}
} ObJsonBinKeyDict;

typedef struct ObJsonBinHeader {
  uint8_t type_;			 // node type for current node
  uint8_t entry_size_   : 2; // the size describe var size of key_entry，val_entry
  uint8_t count_size_   : 2; // the size describe var size of element count
  uint8_t obj_size_size_ : 2; // the size describe var size of key_entry，val_entry
  uint8_t is_continuous_  : 1; // memory of current node and subtree is continous 
  uint8_t reserved_   : 1; // reserved bit
  char used_size_[]; // var size
} ObJsonBinHeader;


typedef ObJsonBinHeader ObJsonBinObjHeader;
typedef ObJsonBinHeader ObJsonBinArrHeader;

static const int OB_JSON_BIN_HEADER_LEN = 2; // actual size of ObJsonBinHeader
static const int OB_JSON_BIN_OBJ_HEADER_LEN = 2; // actual size of ObJsonBinObjHeader
static const int OB_JSON_BIN_ARR_HEADER_LEN = 2; // actual size of ObJsonBinArrHeader

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
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonVerType);
};

class ObJsonBin : public ObIJsonBase
{
public:
  ObJsonBin()
      : ObIJsonBase(NULL),
        allocator_(NULL),
        result_(NULL),
        stack_buf_(NULL)
  {
  }
  explicit ObJsonBin(common::ObIAllocator *allocator)
      : ObIJsonBase(allocator),
        allocator_(allocator),
        result_(allocator_),
        stack_buf_(allocator_)
  {
  }
  explicit ObJsonBin(const char *data, const int64_t length, common::ObIAllocator *allocator = NULL)
      : ObIJsonBase(allocator),
        allocator_(allocator),
        result_(allocator),
        curr_(length, data),
        is_alloc_(false),
        stack_buf_(allocator)
  {
  }
  virtual ~ObJsonBin() { result_.reset(); stack_buf_.reset(); }
  OB_INLINE bool get_boolean() const override { return static_cast<bool>(uint_val_); }
  OB_INLINE double get_double() const override { return double_val_; }
  OB_INLINE float get_float() const override { return float_val_; };
  OB_INLINE int64_t get_int() const override { return int_val_; }
  OB_INLINE uint64_t get_uint() const override { return uint_val_; }
  OB_INLINE const char *get_data() const override { return data_; }
  OB_INLINE uint64_t get_data_length() const override
  {
    uint64_t data_length = element_count_;
    ObJsonNodeType type = json_type();
    if (type == ObJsonNodeType::J_ARRAY || type == ObJsonNodeType::J_OBJECT) {
      data_length = bytes_;
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
    return static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type_));
  }
  OB_INLINE ObJsonInType get_internal_type() const override { return ObJsonInType::JSON_BIN; }
  OB_INLINE uint64_t element_count() const override { return element_count_; }
  OB_INLINE uint64_t get_used_bytes() const { return bytes_; } // return acutal used bytes for curr iter
  OB_INLINE ObJsonNodeType json_type() const override
  {
    return static_cast<ObJsonNodeType>(ObJsonVerType::get_json_type(get_vertype()));
  }
  OB_INLINE ObObjType field_type() const override
  {
    return field_type_;
  }
  int get_array_element(uint64_t index, ObIJsonBase *&value) const override;
  int get_object_value(uint64_t index, ObIJsonBase *&value) const override;
  int get_object_value(const ObString &key, ObIJsonBase *&value) const override;
  int get_key(uint64_t index, common::ObString &key_out) const override;
  int get_raw_binary(common::ObString &out, ObIAllocator *allocator = NULL) const;
  int get_use_size(uint64_t& used_size) const;
  int get_max_offset(const char* data, ObJsonNodeType cur_node, uint64_t& max_offset) const ;
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
public:
  int64_t to_string(char *buf, int64_t len) const;
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
  get json binary raw data with copy
  always return raw data with common header
  @param[out] buf       Json binary raw data
  @param[in]  alloctor  memory alloctor
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int raw_binary(ObString &buf, ObIAllocator *allocator) const;

  /*
  get json binary raw data without copy
  @param[out] buf Json binary raw data
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int raw_binary(ObString &buf) const;

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

  /*
  move iter to parent
  @return Returns OB_SUCCESS on success, error code otherwise.
  */
  int move_parent_iter(); // move iter to parent

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
  };

  typedef struct ObJBNodeMeta ObJBNodeMeta;
  static const int64_t JB_PATH_NODE_LEN = sizeof(ObJBNodeMeta);
  static const int64_t OB_JSON_INSERT_LAST = -1;

  int add_v0(const ObString &key, ObJsonBin *new_value);
  int insert_internal_v0(ObJBNodeMeta& meta, int64_t pos, const ObString &key, ObJsonBin *new_value, ObJsonBuffer& result);
  int insert_v0(int64_t pos, const ObString& key, ObJsonBin *new_value);
  int remove_v0(size_t index);
  int update_v0(int index, ObJsonBin *new_value);
  int estimate_need_rebuild(ObJsonBuffer& update_stack, int64_t size_change,
                            int32_t pos, uint32_t& top_pos, bool& need_rebuild);

  int move_iter(ObJsonBuffer& stack, uint32_t start = 0);
  // build at tail, the offset_size type grow largger, need rebuild
  int estimate_need_rebuild_kv_entry(ObJsonBuffer &result, ObJsonBuffer& origin_stack, 
                            ObJsonBuffer& update_stack, uint32_t& top_pos, bool& rebuild);
  int serialize_json_object(ObJsonObject* object, ObJsonBuffer &result, uint32_t depth = 0);
  int serialize_json_array(ObJsonArray *array, ObJsonBuffer &result, uint32_t depth = 0);
  int serialize_json_value(ObJsonNode *json_tree, ObJsonBuffer &result);
  int serialize_json_integer(int64_t value, ObJsonBuffer &result) const;
  int serialize_json_decimal(ObJsonDecimal *json_dec, ObJsonBuffer &result) const;
  bool try_update_inline(const ObJsonNode *value,
                        uint8_t var_type,
                        int64_t *value_entry_offset,
                        ObJsonBuffer &result);
  bool try_update_inline(const ObJsonBin *value,
                        uint8_t var_type,
                        int64_t *value_entry_offset,
                        ObJsonBuffer &result);

  int deserialize_json_value(const char *data,
                             uint64_t length,
                             uint8_t type,
                             uint64_t value_offset,
                             ObJsonNode *&json_tree,
                             uint64_t type_size);

  int deserialize_json_object_v0(const char *data, uint64_t length, ObJsonObject *object);
  inline int deserialize_json_object(const char *data, uint64_t length, ObJsonObject *object, ObJBVerType vertype);

  int deserialize_json_array_v0(const char *data, uint64_t length, ObJsonArray *array);
  inline int deserialize_json_array(const char *data, uint64_t length, ObJsonArray *array, ObJBVerType vertype);

  int set_curr_by_type(int64_t new_pos, uint64_t val_offset, uint8_t type, uint8_t entry_size = 0);
  void parse_obj_header(const char *data, uint64_t &offset, uint8_t &node_type,
                        uint8_t &type, uint8_t& obj_size_type, uint64_t &count, uint64_t &obj_size) const;
  
  int get_element_in_array_v0(size_t index, char **get_addr_only);
  inline int get_element_in_array(size_t index, char **get_addr_only = NULL);

  int get_element_in_object_v0(size_t index, char **get_addr_only = NULL);
  inline int get_element_in_object(size_t index, char **get_addr_only = NULL);

  int get_key_in_object_v0(size_t i, ObString &key) const;
  inline int get_key_in_object(size_t i, ObString &key) const;
  
  int update_parents(int64_t size_change, bool is_continous);

  int update_offset(uint64_t parent_offset, uint64_t idx, uint64_t value_offset);
  /*
  Get current iter node whether continuous or not.
  */
  bool is_discontinuous() const;
  int get_update_val_ptr(ObJsonBin *new_value_bin, char *&val, uint64_t &len, ObJsonBuffer &str);

  int rebuild(ObJsonBuffer &result);

  int rebuild_with_meta(const char *data, uint64_t length, ObJsonBuffer& old_stack, ObJsonBuffer& new_meta, 
                        uint32_t min, uint32_t max, ObJsonBuffer &result, uint32_t depth = 0);

  int rebuild_json_value_v0(const char *data, uint64_t length, uint8_t type,
                            uint8_t dst_type, uint64_t inline_data, ObJsonBuffer &result) const;
  inline int rebuild_json_value(const char *data, uint64_t length, uint8_t type, uint8_t dst_type,
                                uint64_t inline_data, ObJsonBuffer &result) const;


  int rebuild_json_array_v0(const char *data, uint64_t length, ObJsonBuffer &result) const;
  inline int rebuild_json_array(const char *data, uint64_t length, ObJsonBuffer &result,
                                ObJBVerType cur_vertype, ObJBVerType dest_vertype) const;

  int rebuild_json_obj_v0(const char *data, uint64_t length, ObJsonBuffer &result) const;
  inline int rebuild_json_obj(const char *data, uint64_t length, ObJsonBuffer &result,
                              ObJBVerType cur_vertype, ObJBVerType dest_vertype) const;

  int rebuild_json_process_value_v0(const char *data, uint64_t length, const char *old_val_entry, uint64_t new_val_entry_offset,
                                    uint64_t count, uint8_t var_type, int64_t st_pos, ObJsonBuffer &result) const;
    
  inline int rebuild_json_process_value(const char *data, uint64_t length, const char *old_val_entry, 
                                        uint64_t new_val_entry_offset, uint64_t count, uint8_t var_type, int64_t st_pos,
                                        ObJsonBuffer &result, ObJBVerType cur_vertype, ObJBVerType dest_vertype) const;

  void stack_update(ObJsonBuffer& stack, uint32_t idx, const ObJBNodeMeta& new_value);
  int stack_copy(ObJsonBuffer& src, ObJsonBuffer& dst);
  int stack_pop(ObJsonBuffer& stack);
  int stack_push(ObJsonBuffer& stack, const ObJBNodeMeta& node);
  int stack_at(ObJsonBuffer& stack, uint32_t idx, ObJBNodeMeta& node);
  int32_t stack_size(const ObJsonBuffer& stack) const;
  void stack_reset(ObJsonBuffer& stack);
  int stack_back(ObJsonBuffer& stack, ObJBNodeMeta& node, bool is_pop = false);
  int check_valid_object_op(ObIJsonBase *value) const;
  int check_valid_array_op(ObIJsonBase *value) const;
  int check_valid_object_op(uint64_t index) const;
  int check_valid_array_op(uint64_t index) const;
  int create_new_binary(ObIJsonBase *&value, ObJsonBin *&new_bin) const;
/* data */
private:
  common::ObIAllocator *allocator_;
  ObJsonBuffer result_;
  ObString curr_;
  bool is_alloc_;
  // path node stack used
  ObJsonBuffer stack_buf_;
  
  // curr iter info
  uint8_t type_;
  int64_t pos_;
  uint64_t element_count_; // elem count for obj or array, length for string or opaque
  uint64_t bytes_; // acutal used bytes for curr iter node, inlined node will set 0
  ObObjType field_type_; // field type for opaque
  char *data_;
  union {
    int64_t int_val_;
    uint64_t uint_val_;
    double double_val_;
    float float_val_;
  };
  number::ObNumber number_;
  ObPrecision prec_;
  ObScale scale_;

  DISALLOW_COPY_AND_ASSIGN(ObJsonBin);
};

class ObJsonVar {
public:
  static int read_var(const char *data, uint8_t type, uint64_t *var);
  static int append_var(uint64_t var, uint8_t type, ObJsonBuffer &result);
  static int reserve_var(uint8_t type, ObJsonBuffer &result);
  static int set_var(uint64_t var, uint8_t type, char *pos); // fill var at pos
  static uint64_t get_var_size(uint8_t type);
  static uint8_t get_var_type(uint64_t var);
  static int read_var(const char *data, uint8_t type, int64_t *var);
  static uint64_t var_int2uint(int64_t var);
  static int64_t var_uint2int(uint64_t var, uint8_t entry_size);
  static uint8_t get_var_type(int64_t var);
};

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_JSON_BIN
