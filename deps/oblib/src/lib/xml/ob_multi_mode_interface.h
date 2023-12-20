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
 * This file contains interface define for the multi mode type data abstraction.
 */

#ifndef OCEANBASE_SQL_OB_MULTI_MODE_INTERFACE
#define OCEANBASE_SQL_OB_MULTI_MODE_INTERFACE

#include "lib/json_type/ob_json_common.h"
#include "lib/number/ob_number_v2.h" // for number::ObNumber
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_vector.h"

namespace oceanbase {
namespace common {

const uint16_t MUL_MODE_SINGLE_LELVEL_COUNT = 256;

enum ObMulModeNodeType
{
  /**
   *  attention !!!
   *  inline data type must added at(0 ~ 127) !!!
   *  often used data type added at(0 ~ 127)
   *  rarely used data new type added at(129，unlimited)
   *  At the same time increase the type in g_mul_mode_tc
   * */

  // the following M_NULL ~ M_OPAQUE is for json type
  M_NULL,
  M_DECIMAL,
  M_INT,
  M_UINT,
  M_DOUBLE,
  M_STRING, // 5
  M_OBJECT, //
  M_ARRAY, //
  M_BOOLEAN,
  M_DATE,
  M_TIME, // 10
  M_DATETIME,
  M_TIMESTAMP,
  M_OPAQUE,
  // reserve some for json
  // the following is for xml type
  M_UNPARESED_DOC, //14
  M_UNPARSED, // 15
  M_DOCUMENT, //
  M_CONTENT, //
  M_ELEMENT, // 18
  M_ATTRIBUTE, // 19
  M_NAMESPACE,
  M_INSTRUCT,
  M_TEXT, // 22
  M_COMMENT,
  M_CDATA, // 24
  M_ENTITY,
  M_ENTITY_REF, // 26
  M_DTD, // 27
  M_MAX_TYPE,

  /**
   * attention !!!
   * often used data type added at(0 ~ 127)
  */

  M_EXTENT_LEVEL2 = 125,
  M_EXTENT_LEVEL1 = 126,
  M_EXTENT_LEVEL0 = 127,

  M_EXTENT_BEGIN0 = 128,
  // attention !!!
  // rarely used data new type add here !!!

  M_EXTENT_BEGIN1 = M_EXTENT_BEGIN0 + MUL_MODE_SINGLE_LELVEL_COUNT,
  M_EXTENT_BEGIN2 = M_EXTENT_BEGIN1 + MUL_MODE_SINGLE_LELVEL_COUNT,
};

enum scan_type {
  PRE_ORDER,
  POST_ORDER
};

static constexpr int OB_XML_PARSER_MAX_DEPTH_ = 1000;

enum ObMulModeNodeFlag: uint64_t {
  XML_DECL_FLAG = 0x1,
  XML_ENCODING_EMPTY_FLAG = 0x2
};

typedef struct ObParameterPrint
{
  ObString encode;
  ObString version;
  int indent;
  ObParameterPrint() : encode(), version(), indent(0) {}
} ParamPrint;

struct ObXmlConstants {
  static constexpr const char* XML_STRING = "xml";
  static constexpr const char* XMLNS_STRING = "xmlns";
  static constexpr const char* XML_NAMESPACE_SPECIFICATION_URI = "http://www.w3.org/XML/1998/namespace";
};

enum ObXmlFormatType: uint32_t {
  NO_FORMAT = 0,
  MERGE_EMPTY_TAG = 1 << 0,
  NEWLINE = 1 << 1,
  INDENT = 1 << 2,
  HIDE_PROLOG = 1 << 3,
  HIDE_PI = 1 << 4,
  PRINT_CDATA_AS_TEXT = 1 << 5,
  NO_ENTITY_ESCAPE = 1 << 6,
  NEWLINE_AND_INDENT = NEWLINE | INDENT,
  WITH_FORMAT = MERGE_EMPTY_TAG | NEWLINE_AND_INDENT,
};

enum ObNodeMemType : int8_t {
  BINARY_TYPE = 1,
  TREE_TYPE,
};

enum ObNodeDataType: int8_t {
  OB_XML_TYPE = 0,
  OB_JSON_TYPE,
  OB_PATH_TYPE
};

class ObIMulModeBase;
struct ObNodeMetaData {
  ObNodeMetaData(ObNodeMemType m_type, ObNodeDataType data_type)
    : m_type_(m_type),
      data_type_(data_type)
  {}

  ObNodeMetaData(const ObNodeMetaData& meta)
    : m_type_(meta.m_type_),
      data_type_(meta.data_type_)
  {}

  ObNodeMemType m_type_;
  ObNodeDataType data_type_;
} ;

struct ObLibTreeNodeBase;
class ObNsPair;
typedef PageArena<ObLibTreeNodeBase *, ModulePageAllocator> LibTreeModuleArena;
typedef common::ObVector<ObLibTreeNodeBase *, LibTreeModuleArena> ObLibTreeNodeVector;
typedef common::ObSortedVector<ObNsPair*> ObNsSortedVector;

struct ObMulModeMemCtx {
  ObIAllocator *allocator_;
  ModulePageAllocator page_allocator_;
  LibTreeModuleArena mode_arena_;
};

class ObMulModeFilter {
public:
  ObMulModeFilter() {}
  ~ObMulModeFilter() {}
  virtual int operator()(ObIMulModeBase* cur, bool& filtered) = 0;
};


template<typename T>
class ObStack {
public:
  ObStack(common::ObIAllocator *allocator, int stack_size = 2, int extend_step = 2)
    : buffer_(allocator),
      total_(0),
      pos_(0),
      init_size_(stack_size),
      extend_step_(extend_step)
  {
    if (OB_NOT_NULL(allocator)) {
      int64_t length = stack_size * sizeof(T);
      int ret = buffer_.reserve(length);
      if (OB_SUCC(ret)) {
        total_ = stack_size;
        buffer_.set_length(length);
      } else {
        OB_LOG(WARN, "failed to construct ObStack", K(ret), K(length));
      }
    }
  }

  ObStack(const ObStack& from)
    : buffer_(from.buffer_.get_allocator()),
      total_(0),
      pos_(0),
      init_size_(from.init_size_),
      extend_step_(from.extend_step_)
  {
    int64_t length = from.total_ * sizeof(T);
    int ret = buffer_.reserve(length);
    if (OB_SUCC(ret)) {
      buffer_.set_length(length);
      total_ = from.total_;
      pos_ = from.pos_;
      for (int16_t i = 0; i < pos_; ++i) {
        char* src_buf = const_cast<char*> (from.buffer_.ptr()) + i * sizeof(T);
        char* dst_buf = const_cast<char*> (buffer_.ptr()) + i * sizeof(T);
        new (dst_buf) T(*reinterpret_cast<T*>(src_buf));
      }
    } else {
      OB_LOG(WARN, "failed to construct ObStack", K(ret), K(length));;
    }
  }

  ObStack& operator=(const ObStack& from)
  {
    new (this) ObStack(from);
    return *this;
  }

  int push(const T& iter)
  {
    INIT_SUCC(ret);
    int32_t node_size = sizeof(T);

    if (total_ <= pos_) {
      if (OB_FAIL(extend())) {
        OB_LOG(WARN, "failed to extend", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      char* write_buf = buffer_.ptr() + pos_* node_size;
      new (write_buf) T(iter);
      pos_++;
    }

    return ret;
  }

  int extend()
  {
    INIT_SUCC(ret);
    int64_t length = buffer_.length();
    int64_t extend_size = extend_step_ * sizeof(T);
    if (OB_FAIL(buffer_.reserve(extend_size))) {
      OB_LOG(WARN, "failed to reserve.", K(ret), K(extend_size));
    } else {
      total_ += extend_step_;
      length += extend_size;
      buffer_.set_length(length);
    }
    return ret;
  }

  T& top() {
    int64_t size = count();
    int64_t offset = (size - 1) * sizeof(T);
    return *reinterpret_cast<T*>(buffer_.ptr() + offset);
  }

  void pop()
  {
    if (!empty()) {
      int64_t size = count();
      int64_t offset = (size - 1) * sizeof(T);
      T* tmp = reinterpret_cast<T*>(buffer_.ptr() + offset);
      tmp->~T();

      --pos_;
    }
  }

  T& at(int64_t pos)
  {
    int32_t size = pos_;
    OB_ASSERT(pos < size);

    int64_t offset = pos * sizeof(T);
    T* tmp = reinterpret_cast<T*>(buffer_.ptr() + offset);
    return *tmp;
  }

  int set(int64_t pos, const T& iter)
  {
    INIT_SUCC(ret);
    int32_t node_size = sizeof(T);
    int32_t size = pos_;
    OB_ASSERT(pos < size);
    int64_t offset = pos * node_size;

    if (OB_SUCC(ret)) {
      char* write_buf = buffer_.ptr() + offset;
      new (write_buf) T(iter);
    }

    return ret;
  }

  bool empty() { return pos_ == 0; }
  int64_t size() { return pos_; }
  int64_t count() { return size(); }
  void reset() { pos_ = 0; }
private:
  ObStringBuffer buffer_;
  int32_t total_;
  int32_t pos_;
  int32_t init_size_;
  int32_t extend_step_;
};

class ObPathPool {
public:
  ObPathPool() : freelist_(nullptr), is_inited_(false) {}
  ~ObPathPool() {reset();}
  int init(int64_t obj_size, ObIAllocator *alloc);
  void *alloc();
  void free(void *obj);
  void reset();
  uint64_t get_free_count() {return free_count_;}
  uint64_t get_in_use_count() {return in_use_count_;}
  uint64_t get_total_count() {return total_count_;}
private:
  void *freelist_pop();
  void freelist_push(void *obj);
  void alloc_new_node();
private:
  struct FreeNode
  {
    FreeNode *next_;
  };
private:
  // data members
  int64_t obj_size_;
  uint64_t in_use_count_;
  uint64_t free_count_;
  uint64_t total_count_;
  ObIAllocator *alloc_;
  FreeNode *freelist_;
  bool is_inited_;
};

class ObIMulModeBase {
public:
  ObIMulModeBase(ObNodeMemType mem_type,
                 ObNodeDataType data_type,
                 ObIAllocator *allocator)
  : meta_(mem_type, data_type),
    allocator_(allocator)
  {}

  ObIMulModeBase(ObNodeMemType mem_type,
                 ObNodeDataType data_type)
  : meta_(mem_type, data_type),
    allocator_(nullptr)
  {}

  virtual ~ObIMulModeBase()
  {}

  ObIAllocator *get_allocator() { return allocator_; }

  virtual void set_allocator(ObIAllocator* allocator) { allocator_ = allocator; }

  ObNodeDataType data_type() { return meta_.data_type_; }

  /**
   * node type, json xml together enum
  */
  virtual ObMulModeNodeType type() const = 0;

  bool is_binary() const { return meta_.m_type_ == BINARY_TYPE; }

  bool is_tree() const { return meta_.m_type_ == TREE_TYPE; }

  /**
   * children number under current node
  */
  virtual int64_t size() = 0;

  /**
   * children number under current node
   * the same as size()
  */
  virtual int64_t count() = 0;

  /**
   * children number under current node
  */
  virtual int64_t attribute_size() = 0;

  /**
   * children number under current node
   * the same as size()
  */
  virtual int64_t attribute_count() = 0;

  /**
   * curent is node is tree, or is storage type for example json type , xml type
  */
  ObNodeMemType get_internal_type() const { return meta_.m_type_; };

  /**
   * for search
   * current node key name is consistent with input key string
  */
  virtual int compare(const ObString& key, int& res) = 0;

  /**
   * under current node
   * get specified element's key string
  */
  virtual int get_key(ObString& res, int64_t index = -1) = 0;

  /**
   * under current node
   * get specified element's value string
  */
  virtual int get_value(ObString& value, int64_t index = -1) = 0;

  /**
   * get speicified position element pointer under current node
  */
  virtual int get_value(ObIMulModeBase*& value, int64_t index = -1) = 0;

  /**
   * get namespace for element
  */
  virtual int get_ns_value(ObStack<ObIMulModeBase*>& stk, ObString &ns_value, ObIMulModeBase* extend) = 0;

  virtual int get_ns_value(const ObString& prefix, ObString& ns_value, int& ans_idx) = 0;

  /**
   * get speicified element pointer under current node
  */
  virtual ObIMulModeBase* at(int64_t pos, ObIMulModeBase* buffer = nullptr) = 0;

  virtual ObIMulModeBase* attribute_at(int64_t pos, ObIMulModeBase* buffer = nullptr) = 0;

  /**
   * get all children member under current node
  */
  virtual int get_children(ObIArray<ObIMulModeBase*> &res, ObMulModeFilter* filter = nullptr) = 0;

  /**
   * get all childrent member under current node, whose key string is equal with input key stirng
  */
  virtual int get_children(const ObString& key, ObIArray<ObIMulModeBase*>& res, ObMulModeFilter* filter = nullptr) = 0;


  /**
   * get ObNodeMemType=node_type count
  */
  virtual int get_node_count(ObMulModeNodeType node_type, int &count) = 0;

  virtual int get_attribute(ObIArray<ObIMulModeBase*>& res, ObMulModeNodeType filter_type, int32_t flags = 0) = 0;

  virtual int get_attribute(ObIMulModeBase*& res, ObMulModeNodeType filter_type, const ObString& key1, const ObString &key2 = ObString()) = 0;

  virtual int get_raw_binary(common::ObString &out, ObIAllocator *allocator) = 0;
  /**
   * just append new member children under current node
   * the same as insert at last
  */
  virtual int append(ObIMulModeBase* node) = 0;

  /**
   * add new member at specified postition element
   * if pos is larger than size, do append at last
   * if smaller than 0, insert at 0 position
  */
  virtual int insert(int64_t pos, ObIMulModeBase* node) = 0;

  /**
   * remove specified member at specified postion, under current node
   *  iff pos larger than size(), or smaller than 0, return error code
  */
  virtual int remove(int64_t pos) = 0;

  /**
   * remove speicfied node from current node
   * return error code if node not exists
  */
  virtual int remove(ObIMulModeBase* node) = 0;

  /**
   * update specified postion member under current node
   * if pos is too large or smaller, return error
  */
  virtual int update(int64_t pos, ObIMulModeBase* new_node) = 0;

  /**
   * update specified postion member under current node
   * return error code iff old_node not found
  */
  virtual int update(ObIMulModeBase* old_node, ObIMulModeBase* new_node) = 0;

  // serialize as text string，json xml both require
  // format_flag: use 32 bit to indicate the print format, the specific meaning of each bit decided by each data type
  virtual int print(ObStringBuffer& x_buf, uint32_t format_flag, uint64_t depth = 0, uint64_t size = 0, ObCollationType charset = CS_TYPE_INVALID);

  /**
   * for node compare
   */
  // virtual int compare(const ObIMulModeBase &other, int &res) = 0;

  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "data_type = %d, m_type_=%d", meta_.data_type_, meta_.m_type_);
    return pos;
  }

  virtual ObMulModeMemCtx* get_mem_ctx() { return nullptr; }

  virtual int print_xml(ObStringBuffer& x_buf, uint32_t format_flag, uint64_t depth, uint64_t size, ObNsSortedVector* ns_vec = nullptr, ObCollationType charset = CS_TYPE_INVALID);
  virtual int print_attr(ObStringBuffer& x_buf, uint32_t format_flag);
  virtual int print_ns(ObStringBuffer& x_buf, uint32_t format_flag);
  virtual int print_pi(ObStringBuffer& x_buf, uint32_t format_flag);
  virtual int print_cdata(ObStringBuffer& x_buf, uint32_t format_flag);
  virtual int print_comment(ObStringBuffer& x_buf, uint32_t format_flag);
  virtual int print_text(ObStringBuffer& x_buf, uint32_t format_flag);
  virtual int print_document(ObStringBuffer& x_buf, ObCollationType charset, uint32_t format_flag, uint64_t size = 2, ObNsSortedVector* ns_vec = nullptr);
  virtual int print_unparsed(ObStringBuffer& x_buf, ObCollationType charset, uint32_t format_flag, uint64_t size = 2);
  virtual int print_content(ObStringBuffer& x_buf, bool with_encoding, bool with_version, uint32_t format_flag, ParamPrint &param_list, ObNsSortedVector* ns_vec = nullptr);
  virtual int print_element(ObStringBuffer& x_buf, uint64_t depth, uint32_t format_flag, uint64_t size = 2, ObNsSortedVector* ns_vec = nullptr);

  virtual ObString get_version() = 0;
  virtual ObString get_encoding() = 0;
  virtual ObString get_prefix() = 0;
  virtual uint16_t get_standalone() = 0;
  virtual void set_standalone(uint16_t) = 0;
  virtual uint16_t get_encoding_flag() = 0;
  virtual uint16_t has_xml_decl() = 0;
  virtual uint16_t is_unparse() = 0;
  virtual ObIMulModeBase* get_attribute_handle() = 0;

  virtual bool get_is_empty() = 0;
  virtual bool has_flags(ObMulModeNodeFlag flag) = 0;
  virtual bool get_unparse() = 0;
  virtual bool is_equal_node(const ObIMulModeBase* other) = 0;
  virtual bool is_node_before(const ObIMulModeBase* other) = 0;
  virtual bool check_extend() = 0;
  virtual bool check_if_defined_ns() = 0;

  virtual int64_t get_serialize_size();
  // eval interface
  /**
   * get boolean
   */
  virtual bool get_boolean();
  /**
   * get double
  */
  virtual double get_double();
  /**
   *  get float
   */
  virtual float get_float();
  /**
   * get int
   */
  virtual int64_t get_int();
  /**
   * get uint
   */
  virtual uint64_t get_uint();
  /**
   * get data
   */
  virtual const char *get_data();
  /***
   * get data length
   */
  virtual uint64_t get_data_length();
  /**
   *  get number
   */
  virtual number::ObNumber get_decimal_data();
  /**
   * get decimal precision
   */
  virtual ObPrecision get_decimal_precision();
  /**
   * get decimal scale
   */
  virtual ObScale get_decimal_scale();

  virtual ObTime get_time();

  /*  // for cast
  *  virtual int to_int(int64_t &value, bool check_range = false, bool force_convert = false) const = 0;
  *  virtual int to_uint(uint64_t &value, bool fail_on_negative = false, bool check_range = false) const = 0;
  *  virtual int to_double(double &value) const = 0;
  *  virtual int to_number(ObIAllocator *allocator, number::ObNumber &number) const = 0;
  *  virtual int to_datetime(int64_t &value, ObTimeConvertCtx *cvrt_ctx_t = nullptr) const = 0;
  *  virtual int to_date(int32_t &value) const = 0;
  *  virtual int to_otimestamp(common::ObOTimestampData &value, ObTimeConvertCtx *cvrt_ctx = nullptr) const = 0;
  *  virtual int to_time(int64_t &value) const = 0;
  *  virtual int to_bit(uint64_t &value) const = 0;
  **/

protected:
  ObNodeMetaData meta_;
  ObIAllocator *allocator_;

};

} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_MULTI_MODE_INTERFACE