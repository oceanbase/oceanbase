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
 * This file contains interface support for the json tree abstraction.
 */

#ifndef OCEANBASE_SQL_OB_JSON_TREE
#define OCEANBASE_SQL_OB_JSON_TREE

#include "ob_json_base.h"
#include "ob_json_path.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/number/ob_number_v2.h" // for number::ObNumber
#include "lib/timezone/ob_time_convert.h" // for ObTime
#include "lib/timezone/ob_timezone_info.h"
#include <rapidjson/error/en.h>
#include <rapidjson/error/error.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/reader.h>

namespace oceanbase {
namespace common {

class ObJsonNode;

class ObJsonTreeUtil final
{
public:
  ObJsonTreeUtil() {}
  virtual ~ObJsonTreeUtil() {}
  template <typename T, typename... Args>
  static ObJsonNode *clone_new_node(ObIAllocator* allocator, Args &&... args);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonTreeUtil);
};

class ObJsonNode : public ObIJsonBase
{
public:
  explicit ObJsonNode(ObIAllocator *allocator)
      : ObIJsonBase(allocator),
        parent_(NULL),
        serialize_size_(0)
  {
  }
  virtual ~ObJsonNode() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json_type = %d", json_type());
    return pos;
  }
  bool get_boolean() const override;
  double get_double() const override;
  float get_float() const override;
  int64_t get_int() const override;
  uint64_t get_uint() const override;
  const char *get_data() const override;
  uint64_t get_data_length() const override;
  number::ObNumber get_decimal_data() const override;
  ObPrecision get_decimal_precision() const override;
  ObScale get_decimal_scale() const override;
  int get_obtime(ObTime &t) const override;
  OB_INLINE ObJsonInType get_internal_type() const override { return ObJsonInType::JSON_TREE; }
  OB_INLINE uint64_t element_count() const override { return 1; }
  virtual uint64_t member_count() const override { return element_count(); }
  OB_INLINE int get_parent(ObIJsonBase *&parent) const override { parent = parent_; return OB_SUCCESS; }
  OB_INLINE ObObjType field_type() const override
  {
    return ObMaxType; // ObJsonOpaque override
  }
  OB_INLINE void set_parent(ObJsonNode *parent) { parent_ = parent; }
  OB_INLINE ObJsonNode *get_parent() { return parent_; }
  virtual bool is_scalar() const { return false; }
  virtual bool is_number() const { return false; }
  virtual uint32_t depth() const = 0;
  virtual uint64_t get_serialize_size() = 0;
  virtual ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const = 0;
  OB_INLINE void set_serialize_size(uint64_t size) { serialize_size_ = size; }
  OB_INLINE void set_serialize_delta_size(int64_t size)
  {
    serialize_size_ += size;
    update_serialize_size_cascade(size);
  }
  virtual void update_serialize_size(int64_t change_size = 0) {UNUSED(change_size);}
  OB_INLINE void update_serialize_size_cascade(int64_t change_size = 0)
  {
    ObJsonNode *node = this->parent_;
    while (OB_NOT_NULL(node)) {
      if (!node->is_scalar()) {
        node->update_serialize_size(change_size);
      }
      node = node->parent_;
    }
  }
  int get_array_element(uint64_t index, ObIJsonBase *&value) const override;
  int get_object_value(uint64_t index, ObIJsonBase *&value) const override;
  int get_object_value(const ObString &key, ObIJsonBase *&value) const override;
  int get_object_value(uint64_t index, ObString &key, ObIJsonBase *&value) const override;
  int get_key(uint64_t index, common::ObString &key_out) const override;
  int array_remove(uint64_t index) override;
  int object_remove(const common::ObString &key) override;
  int replace(const ObIJsonBase *old_node, ObIJsonBase *new_node) override;
  int get_location(ObJsonBuffer &path) const;
  int merge_tree(ObIAllocator *allocator, ObIJsonBase *other, ObIJsonBase *&result);
  int array_append(ObIJsonBase *value) override;
  int array_insert(uint64_t index, ObIJsonBase *value) override;
  int object_add(const common::ObString &key, ObIJsonBase *value) override;
private:
  int check_valid_object_op(ObIJsonBase *value) const;
  int check_valid_array_op(ObIJsonBase *value) const;
  int check_valid_object_op(uint64_t index) const;
  int check_valid_array_op(uint64_t index) const;
private:
  ObJsonNode *parent_;
  uint64_t serialize_size_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonNode);
};

// json container include(object, array)
class ObJsonContainer : public ObJsonNode
{
public:
  explicit ObJsonContainer(ObIAllocator *allocator)
      : ObJsonNode(allocator)
  {
  }
  virtual ~ObJsonContainer() {}

  // Replace the old node in the Container with the new node.
  //
  // @param [in] old_node The old node to be replaced.
  // @param [in] new_node The new node.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  virtual int replace(const ObJsonNode *old_node, ObJsonNode *new_node) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonContainer);
};

// json object pairs(key, value)
class ObJsonObjectPair final
{
public:
  ObJsonObjectPair() {}
  explicit ObJsonObjectPair(const ObString &key, ObJsonNode *value)
      : key_(key),
        value_(value)
  {
  }
  ~ObJsonObjectPair() {}
  OB_INLINE common::ObString get_key() const { return key_; }
  OB_INLINE void set_key(const common::ObString &new_key)
  {
    key_.assign_ptr(new_key.ptr(), new_key.length());
  }
  OB_INLINE void set_value(ObJsonNode *value) { value_ = value; }
  OB_INLINE ObJsonNode *get_value() const { return value_; }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "key = %s", key_.ptr());
    return pos;
  }
private:
  common::ObString key_;
  ObJsonNode *value_;
};

typedef common::ObArray<ObJsonObjectPair> ObJsonObjectArray;
class ObJsonObject : public ObJsonContainer
{
private:
  static const int64_t DEFAULT_PAGE_SIZE = 512L; // 8kb -> 512
public:
  explicit ObJsonObject(ObIAllocator *allocator)
      : ObJsonContainer(allocator),
        serialize_size_(0),
        page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        object_array_(DEFAULT_PAGE_SIZE, page_allocator_)
  {
    set_parent(NULL);
  }
  virtual ~ObJsonObject() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json object element_count = %lu", element_count());
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_OBJECT; }
  OB_INLINE uint64_t element_count() const override { return object_array_.size(); }
  OB_INLINE void set_serialize_size(uint64_t size) { serialize_size_ = size; }
  OB_INLINE uint32_t depth() const override
  {
    uint32_t max_child = 0;
    uint64_t count = element_count();
    ObJsonNode *child_node = NULL;
    for (uint64_t i = 0; i < count; i++) {
      child_node = get_value(i);
      if (OB_NOT_NULL(child_node)) {
        max_child = max(max_child, child_node->depth());
      }
    }
    return max_child + 1;
  }

  void unique();
  void stable_sort();
  OB_INLINE uint64_t get_serialize_size()
  {
    if (serialize_size_ == 0) {
      update_serialize_size();
    }
    return serialize_size_;
  }
  OB_INLINE void set_serialize_delta_size(int64_t size)
  {
    serialize_size_ += size;
    update_serialize_size_cascade(size);
  }
  void update_serialize_size(int64_t change_size = 0);
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
  
  // Get json node by key.
  //
  // @param [in] key The key.
  // @return Returns ObJsonNode on success, NULL otherwise.
  ObJsonNode *get_value(const common::ObString &key) const;

  // Get json node by index.
  //
  // @param [in] index The index.
  // @return Returns ObJsonNode on success, NULL otherwise.
  ObJsonNode *get_value(uint64_t index) const;

  // Get object pair by index.
  //
  // @param [in] index The index.
  // @param [out] key The key.
  // @param [out] vale The value.
  int get_value_by_idx(uint64_t index, ObString& key, ObJsonNode*& value) const;
  // Get object pair by index.
  //
  // @param [in] index The index.
  // @param [out] key The key.
  int get_key_by_idx(uint64_t index, ObString& key) const;

  // Get json node by index.
  //
  // @param [in] index The index.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int get_key(uint64_t index, common::ObString &key_out) const;

  // Remove json node by key
  //
  // @param [in] key The key.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int remove(const common::ObString &key);

  // Replace the old node with the new node.
  //
  // @param [in] old_node   The old node to be replaced.
  // @param [in] new_node   The new node.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int replace(const ObJsonNode *old_node, ObJsonNode *new_node) override;

  // Add a key-value pair to the current object (push_back if not found).
  //
  // @param [in] key    The key.
  // @param [in] value  The Json node.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int add(const common::ObString &key, ObJsonNode *value, bool with_unique_key = false, bool is_lazy_sort = false, bool need_overwrite = true, bool is_schema = false);

  // Rename key in current object if exist.
  //
  // @param [in] old_key  The old key name.
  // @param [in] new_key  The new key name.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int rename_key(const common::ObString &old_key, const common::ObString &new_key);

  // Merges all elements on the other to the end of the current object
  //
  // @param [in] allocator  The memory allocator.
  // @param [in] other      The ObJsonObject need to merge.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int consume(ObIAllocator *allocator, ObJsonObject *other);

  // Patch the second json object to current json object.
  //
  // @param[in] patch_obj The json obect to be patch
  // return Returns OB_SUCCESS on success, error code otherwise.
  int merge_patch(ObIAllocator* allocator, ObJsonObject *patch_obj);

  // Sort the nodes of this layer
  //
  // @return void
  void sort();

  // Remove all nodes.
  //
  // @return void
  void clear();

  void get_obj_array(ObJsonObjectArray*& obj_array) { obj_array = &object_array_; }

private:
  uint64_t serialize_size_;
  ModulePageAllocator page_allocator_;
  ObJsonObjectArray object_array_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonObject);
};

typedef PageArena<ObJsonNode *, ModulePageAllocator> JsonNodeModuleArena;
typedef common::ObVector<ObJsonNode *, JsonNodeModuleArena> ObJsonNodeVector;
class ObJsonArray : public ObJsonContainer
{
private:
  static const int64_t DEFAULT_PAGE_SIZE = 512L; // 8kb -> 512
public:
  explicit ObJsonArray(ObIAllocator *allocator)
      : ObJsonContainer(allocator),
        serialize_size_(0),
        page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        mode_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
        node_vector_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR)
  {
    set_parent(NULL);
  }
  virtual ~ObJsonArray() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json array element_count = %lu", element_count());
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_ARRAY; }
  OB_INLINE uint64_t element_count() const override { return node_vector_.size(); }
  OB_INLINE uint32_t depth() const override
  {
    uint32_t max_child = 0;
    uint64_t size = element_count();
    ObJsonNode *child_node = NULL;
    for (uint64_t i = 0; i < size; i++) {
      child_node = node_vector_[i];
      if (OB_NOT_NULL(child_node)) {
        max_child = max(max_child, child_node->depth());
      }
    }
    return max_child + 1;
  }
  OB_INLINE uint64_t get_serialize_size()
  {
    if (serialize_size_ == 0) {
      update_serialize_size();
    }
    return serialize_size_;
  }
  OB_INLINE void set_serialize_size(uint64_t size) { serialize_size_ = size; }
  OB_INLINE void set_serialize_delta_size(int64_t size)
  {
    serialize_size_ += size;
    update_serialize_size_cascade(size);
  }
  void update_serialize_size(int64_t change_size = 0);
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;

  // Removes the array element by index.
  //
  // @param [in] index  Index of the array to be removed.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int remove(uint64_t index);
  
  // Overloaded operator[]
  //
  // @return ObJsonNode pointer.
  ObJsonNode *operator[](uint64_t index) const;

  // Replace the old node with the new node.
  //
  // @param [in] old_node   The old node to be replaced.
  // @param [in] new_node   The new node.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int replace(const ObJsonNode *old_node, ObJsonNode *new_node) override;

  // Append value to the json array.
  //
  // @param [in] value The json node to be appended.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int append(ObJsonNode *value);

  // Inserts an element at the specified position.
  //
  // @param [in] index  Index of the array to be inserted.
  // @param [in] value  The array element to be inserted.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int insert(uint64_t index, ObJsonNode *value);

  // Merges all elements on the other to the end of the current ObJsonArray
  //
  // @param [in] allocator  The memory allocator.
  // @param [in] other      The ObJsonArray need to merge.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int consume(ObIAllocator *allocator, ObJsonArray *other);

  // Clears all array elements.
  //
  // @return void
  void clear();

private:
  uint64_t serialize_size_;
  ModulePageAllocator page_allocator_;
  JsonNodeModuleArena mode_arena_;
  ObJsonNodeVector node_vector_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonArray);
};

// json scalar include(number(decimal, double, int, uint), string, null, datetime, opaque, boolean)
class ObJsonScalar : public ObJsonNode
{
public:
  explicit ObJsonScalar(ObIAllocator *allocator = NULL)
        : ObJsonNode(allocator)
  {
  }
  virtual ~ObJsonScalar() {}
  OB_INLINE bool is_scalar() const { return true; }
  OB_INLINE uint32_t depth() const override { return 1; }
  OB_INLINE uint64_t element_count() const { return 1; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonScalar);
};

// json number include(decimal, double, int, uint)
class ObJsonNumber : public ObJsonScalar
{
public:
  ObJsonNumber() {}
  virtual ~ObJsonNumber() {}
  OB_INLINE bool is_number() const { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonNumber);
};

class ObJsonDecimal : public ObJsonNumber
{
public:
  explicit ObJsonDecimal(const number::ObNumber &value, ObPrecision prec = -1, ObScale scale = -1)
      : ObJsonNumber(),
        value_(value),
        prec_(prec),
        scale_(scale)
  {
  }
  virtual ~ObJsonDecimal() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = value_.to_string(buf, buf_len);
    databuff_printf(buf, buf_len, pos,
        "json decimal, prec_ = %d, scale_ = %d", prec_, scale_);
    return pos;
  }
  OB_INLINE virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_DECIMAL; }
  OB_INLINE void set_value(number::ObNumber value) { value_ = value; }
  OB_INLINE number::ObNumber value() const { return value_; }
  OB_INLINE void set_precision(ObPrecision prec) { prec_ = prec; }
  OB_INLINE ObPrecision get_precision() const { return prec_; }
  OB_INLINE void set_scale(ObScale scale) { scale_ = scale; }
  OB_INLINE ObScale get_scale() const { return scale_; }
  OB_INLINE uint64_t get_serialize_size()
  {
    return value_.get_serialize_size() + serialization::encoded_length_i16(prec_)
    + serialization::encoded_length_i16(scale_); 
  }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  number::ObNumber value_;
  ObPrecision prec_;
  ObScale scale_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonDecimal);
};

class ObJsonDouble : public ObJsonNumber
{
public:
  explicit ObJsonDouble(double value)
      : ObJsonNumber(),
        value_(value)
  {
  }
  virtual ~ObJsonDouble() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json double value = %lf", value_);
    return pos;
  }
  OB_INLINE virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_DOUBLE; }
  OB_INLINE void set_value(double value) { value_ = value; }
  OB_INLINE double value() const { return value_; }
  OB_INLINE uint64_t get_serialize_size() { return sizeof(double); }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  double value_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonDouble);
};

class ObJsonInt : public ObJsonNumber
{
public:
  explicit ObJsonInt(int64_t value)
      : ObJsonNumber(),
        value_(value)
  {
  }
  virtual ~ObJsonInt() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json int value = %ld", value_);
    return pos;
  }
  OB_INLINE virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_INT; }
  OB_INLINE void set_value(int64_t value) { value_ = value; }
  OB_INLINE int64_t value() const { return value_; }
  OB_INLINE uint64_t get_serialize_size()
  {
    return serialization::encoded_length_vi64(value_);
  }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  int64_t value_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonInt);
};

class ObJsonUint : public ObJsonNumber
{
public:
  explicit ObJsonUint(uint64_t value)
      : ObJsonNumber(),
        value_(value),
        is_string_length_(false)
  {
  }
  virtual ~ObJsonUint() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json uint value = %lu", value_);
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_UINT; }
  OB_INLINE void set_value(uint64_t value) { value_ = value; }
  OB_INLINE uint64_t value() const { return value_; }
  OB_INLINE void set_is_string_length(bool value) { is_string_length_ = value; }
  OB_INLINE bool get_is_string_length() const { return is_string_length_; }
  OB_INLINE uint64_t get_serialize_size()
  {
    return serialization::encoded_length_vi64(value_);
  }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  uint64_t value_;
  bool is_string_length_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonUint);
};

class ObJsonString : public ObJsonScalar
{
public:
  explicit ObJsonString(const char *str, uint64_t length)
      : ObJsonScalar(),
        str_(length, str),
        ext_(0),
        is_null_to_str_(false)
  {
  }
  explicit ObJsonString(ObString str)
      : ObJsonScalar(),
        str_(str),
        ext_(0),
        is_null_to_str_(false)
  {
  }
  virtual ~ObJsonString() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json string ");
    pos += str_.to_string(buf + pos, buf_len - pos);
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_STRING; }
  OB_INLINE void set_value(const char *str, uint64_t length) { str_.assign_ptr(str, length); }
  OB_INLINE const common::ObString &value() const { return str_; }
  OB_INLINE void set_is_null_to_str(bool value) { is_null_to_str_ = value; }
  OB_INLINE bool get_is_null_to_str() const { return is_null_to_str_; }
  OB_INLINE uint64_t length() const { return str_.length(); }
  OB_INLINE ObString get_str() const { return str_; }
  OB_INLINE uint64_t get_serialize_size()
  {
    return serialization::encoded_length_vi64(length()) + length();
  }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
  void set_ext(uint64_t type) { ext_ = type; }
  uint64_t get_ext() { return ext_; }
private:
  common::ObString str_;
  uint64_t ext_;
  bool is_null_to_str_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonString);
};

class ObJsonNull : public ObJsonScalar
{
public:
  ObJsonNull()
      : ObJsonScalar(),
      is_not_null_(false)
  {
  }
  ObJsonNull(bool is_not_null)
      : ObJsonScalar(),
      is_not_null_(is_not_null)
  {
  }
  virtual ~ObJsonNull() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json null");
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_NULL; }
  OB_INLINE uint64_t get_serialize_size() { return sizeof(char); }
  OB_INLINE bool is_not_null() { return is_not_null_;}
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonNull);
  bool is_not_null_;
};

class ObJsonDatetime : public ObJsonScalar
{
public:
  explicit ObJsonDatetime(const ObTime &time, ObObjType field_type);
  explicit ObJsonDatetime(ObJsonNodeType type, const ObTime &time);
  virtual ~ObJsonDatetime() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json datetime ");
    pos += value_.to_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, "filed type:%d", field_type_);
    return pos;
  }
  ObJsonNodeType json_type() const { return json_type_; };
  OB_INLINE ObObjType field_type() const { return field_type_; }
  OB_INLINE void set_field_type(ObObjType field_type) { field_type_ = field_type; }
  OB_INLINE void set_json_type(ObJsonNodeType json_type);
  OB_INLINE ObTime value() const { return value_; }
  OB_INLINE void set_value(ObTime value) { value_ = value; }
  OB_INLINE uint64_t get_serialize_size()
  {
    uint64_t size = 0;
    if (json_type() == ObJsonNodeType::J_DATE || json_type() == ObJsonNodeType::J_ORACLEDATE) {
      size = sizeof(int32_t);
    } else {
      size = sizeof(int64_t);
    }
    return size;
  }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  ObTime value_;
  ObObjType field_type_;
  ObJsonNodeType json_type_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonDatetime);
};

class ObJsonOpaque : public ObJsonScalar
{
public:
  explicit ObJsonOpaque(common::ObString value, ObObjType field_type)
      : ObJsonScalar(),
        field_type_(field_type),
        value_(value)
  {
  }
  virtual ~ObJsonOpaque() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json opaque ");
    pos += value_.to_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, "filed type:%d", field_type_);
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_OPAQUE; }
  OB_INLINE const char *value() const { return value_.ptr(); }
  OB_INLINE void set_value(common::ObString value) { value_ = value; }
  OB_INLINE ObObjType field_type() const override { return field_type_; }
  OB_INLINE void set_field_type(ObObjType field_type) { field_type_ = field_type; }
  OB_INLINE uint64_t size() const { return value_.length(); }
  OB_INLINE uint64_t get_serialize_size()
  {
    return sizeof(uint16_t) + sizeof(uint64_t) + size(); // [field_type][length][value]; 
  }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  ObObjType field_type_;
  common::ObString value_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonOpaque);
};

class ObJsonBoolean : public ObJsonScalar
{
public:
  explicit ObJsonBoolean(bool value)
      : ObJsonScalar(),
        value_(value)
  {
  }
  virtual ~ObJsonBoolean() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json bool value = %d", value_);
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_BOOLEAN; }
  OB_INLINE void set_value(bool value) { value_ = value; }
  OB_INLINE bool value() const
  {
    bool bool_ret = (value_ != 0);
    return bool_ret;
  }
  OB_INLINE uint64_t get_serialize_size() { return sizeof(char); }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  bool value_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonBoolean);
};

class ObJsonODecimal : public ObJsonDecimal
{
public:
  explicit ObJsonODecimal(const number::ObNumber &value, ObPrecision prec = -1, ObScale scale = -1)
    :ObJsonDecimal(value, prec, scale) {}
  OB_INLINE virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_ODECIMAL; }
};

class ObJsonOFloat : public ObJsonNumber
{
public:
  explicit ObJsonOFloat(float value)
          : ObJsonNumber(),
            value_(value)
  {
  }
  virtual ~ObJsonOFloat() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "json float value = %f", value_);
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_OFLOAT; }
  OB_INLINE void set_value(float value) { value_ = value; }
  OB_INLINE float value() const { return value_; }
  OB_INLINE uint64_t get_serialize_size() { return sizeof(float); }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  float value_;
  DISALLOW_COPY_AND_ASSIGN(ObJsonOFloat);
};

class ObJsonOInt : public ObJsonInt
{
public:
  explicit ObJsonOInt(int64_t value)
      : ObJsonInt(value)
  {
  }
  OB_INLINE virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_OINT; }
};

class ObJsonOLong : public ObJsonUint
{
public:
  explicit ObJsonOLong(uint64_t value)
      : ObJsonUint(value)
  {
  }
  OB_INLINE ObJsonNodeType json_type() const override { return ObJsonNodeType::J_OLONG; }
};

class ObJsonODouble : public ObJsonDouble
{
public:
  explicit ObJsonODouble(double value)
      : ObJsonDouble(value)
  {
  }
  OB_INLINE virtual ObJsonNodeType json_type() const override { return ObJsonNodeType::J_ODOUBLE; }
};

class ObJsonOInterval : public ObJsonScalar
{
public:
  explicit ObJsonOInterval(const char *str, uint64_t length, ObObjType field_type)
      : ObJsonScalar(),
        str_val_(length, str),
        field_type_(field_type),
        val_()
  {
  }
  virtual ~ObJsonOInterval() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (field_type_ == ObIntervalYMType) {
      databuff_printf(buf, buf_len, pos, "json intervalymtype");
    } else {
      databuff_printf(buf, buf_len, pos, "json intervaldstype");
    }
    pos += str_val_.to_string(buf + pos, buf_len - pos);
    return pos;
  }
  int parse();

  OB_INLINE ObJsonNodeType json_type() const override
  {
    return field_type_ == ObIntervalYMType ? ObJsonNodeType::J_OYEARMONTH : ObJsonNodeType::J_ODAYSECOND;
  }
  OB_INLINE void set_value(const char *str, uint64_t length) { str_val_.assign_ptr(str, length); }
  OB_INLINE const common::ObString &value() const { return str_val_; }
  OB_INLINE uint64_t length() const { return str_val_.length(); }
  OB_INLINE uint64_t get_serialize_size()
  {
    return serialization::encoded_length_vi64(length()) + length();
  }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;

private:
  ObString str_val_;
  ObObjType field_type_;
  union IntervalValue {
    ObIntervalYMValue ym_;
    ObIntervalDSValue ds_;
    IntervalValue() {}
  } val_;
};

class ObJsonORawString : public ObJsonScalar
{
public:
  explicit ObJsonORawString(const char *str, uint64_t length, ObJsonNodeType node_type)
      : ObJsonScalar(),
        str_val_(length, str),
        json_type_(node_type)
  {
    if (node_type == ObJsonNodeType::J_OBINARY) {
      field_type_ = ObHexStringType;
    } else if (node_type == ObJsonNodeType::J_OOID) {
      field_type_ = ObHexStringType;
    } else if (node_type == ObJsonNodeType::J_ORAWHEX) {
      field_type_ = ObRawType;
    } else { // J_ORAWID
      field_type_ = ObURowIDType;
    }
  }
  virtual ~ObJsonORawString() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (json_type_ == ObJsonNodeType::J_OBINARY) {
      databuff_printf(buf, buf_len, pos, "json binary ");
    } else if (json_type_ == ObJsonNodeType::J_OOID) {
      databuff_printf(buf, buf_len, pos, "json oid ");
    } else if (json_type_ == ObJsonNodeType::J_ORAWHEX) {
      databuff_printf(buf, buf_len, pos, "json rawhex ");
    } else {
      databuff_printf(buf, buf_len, pos, "json rawid ");
    }

    pos += str_val_.to_string(buf + pos, buf_len - pos);
    return pos;
  }
  OB_INLINE ObJsonNodeType json_type() const override { return json_type_; }
  OB_INLINE void set_value(const char *str, uint64_t length) { str_val_.assign_ptr(str, length); }
  OB_INLINE const common::ObString &value() const { return str_val_; }
  OB_INLINE uint64_t length() const { return str_val_.length(); }
  OB_INLINE uint64_t get_serialize_size()
  {
    return serialization::encoded_length_vi64(length()) + length();
  }
  ObJsonNode *clone(ObIAllocator* allocator, bool is_deep_copy = false) const;
private:
  ObString str_val_;
  ObJsonNodeType json_type_;
  ObObjType field_type_;
};

// Object element comparison function.
struct ObJsonKeyCompare {
  int operator()(const ObJsonObjectPair &left, const ObJsonObjectPair &right)
  {
    INIT_SUCC(ret);

    common::ObString left_key = left.get_key();
    common::ObString right_key = right.get_key();

    // first compare length
    if (left_key.length() != right_key.length()) {
      ret = (left_key.length() < right_key.length());
    } else { // do Lexicographic order when length equals
      ret = (left_key.compare(right_key) < 0);
    }

    return ret;
  }

  int compare(const ObString &left, const ObString &right)
  {
    int result = 0;

    // first compare length
    if (left.length() != right.length()) {
      result = left.length() - right.length();
    } else { // do Lexicographic order when length equals
      result = left.compare(right);
    }

    return result;
  }
};

} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_JSON_TREE
