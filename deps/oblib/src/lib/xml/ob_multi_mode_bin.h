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
 * This file contains interface support for the xml & json basic interface abstraction.
 */

#ifndef OCEANBASE_MUL_MODE_BIN_BASE
#define OCEANBASE_MUL_MODE_BIN_BASE

#include "lib/string/ob_string_buffer.h"
#include "lib/xml/ob_multi_mode_interface.h"

namespace oceanbase {
namespace common {

class ObIMulModeBase;

enum ObMulModeBinType {
  MulModeNull = 0,
  MulModeBoolean,
  MulModeDecimal,
  MulModeUint,
  MulModeInt,
  MulModeTime,
  MulModeDouble,
  MulModeString,
  /**
   * container is not a basic type, while in multi mode binary is a basic data structure
   */
  MulModePair,
  MulModeContainer,
  MulModeMaxType
};

static ObMulModeBinType g_mul_mode_tc[] = {
  MulModeNull, //  M_NULL, // 0 oracle & mysql
  MulModeDecimal, //  M_DECIMAL,
  MulModeInt, //  M_INT,
  MulModeInt, //  M_UINT,
  MulModeDouble, //  M_DOUBLE,
  MulModeString, //  M_STRING, // 5 oracle & mysql
  MulModeContainer, //  M_OBJECT, // oracle & mysql
  MulModeContainer, //  M_ARRAY, // oracle & mysql
  MulModeBoolean, //  M_BOOLEAN,
  MulModeTime, //  M_DATE,
  MulModeTime, //  M_TIME, // 10
  MulModeTime, //  M_DATETIME,
  MulModeTime, //  M_TIMESTAMP,
  MulModeString, //  M_OPAQUE, 13
  MulModeContainer, //  M_UNPARSED, 14
  MulModeContainer, // M_UNPARESED_DOC, 15
  MulModeContainer, //  M_DOCUMENT, 16
  MulModeContainer, //  M_CONTENT, 17
  MulModeContainer, //  M_ELEMENT, 18
  MulModePair, //  M_ATTRIBUTE, 19
  MulModePair, //  M_NAMESPACE, 20
  MulModePair, //  M_PI, 21
  MulModeString, //  M_TEXT, 22
  MulModeString, //  M_COMMENT, 23
  MulModeString, //  M_CDATA, 24
  MulModePair, //  M_ENTITY, 25
  MulModePair, //  M_ENTITY_REF, 26
  MulModeContainer, //  M_DTD 27
};

#define OB_MUL_MODE_INLINE_REV_MASK  (0x7F)
#define OB_MUL_MODE_INLINE_MASK      (0x80)
#define OB_MUL_MODE_IS_INLINE(origin_type)  ((OB_MUL_MODE_INLINE_MASK & (origin_type)) != 0)
#define OB_MUL_MODE_GET_INLINE(origin_type) (OB_MUL_MODE_INLINE_REV_MASK & (origin_type))

#define OB_MUL_MODE_BIN_MAX_SERIALIZE_TIME 2
enum ObMulModeBinLenSize:uint8_t {
  MBL_UINT8 = 0,
  MBL_UINT16 = 1,
  MBL_UINT32 = 2,
  MBL_UINT64 = 3,
  MBL_MAX = 4,
};

static const uint8_t MUL_MODE_BIN_HEADER_LEN = 2;
static const uint8_t MUL_MODE_BIN_BASE_HEADER_LEN = 1;

typedef struct ObMulModeBinHeader {
  ObMulModeBinHeader() {
    (&type_)[1] = 0;
  }
  ObMulModeBinHeader(uint8_t type,
    uint8_t kv_entry_type,
    uint8_t count_type,
    uint8_t obj_type,
    uint8_t is_continous)
    : type_(type),
      kv_entry_size_type_(kv_entry_type),
      count_size_type_(count_type),
      obj_size_type_(obj_type),
      is_continuous_(is_continous),
      reserved_(0) {}

  uint8_t type_;			 // node type for current node
  uint8_t kv_entry_size_type_   : 2; // the size describe var size of key_entry，val_entry
  uint8_t count_size_type_   : 2; // the size describe var size of element count
  uint8_t obj_size_type_ : 2; // the size describe var size of key_entry，val_entry
  uint8_t is_continuous_  : 1; // memory of current node and subtree is continous
  uint8_t reserved_   : 1; // reserved bit
  char used_size_[]; // var size

} ObMulModeBinHeader;

typedef ObMulModeBinHeader ObXmlBinHeader;

typedef std::pair<uint8_t, uint8_t> ObMulModeExtendStorageType;

class ObMulBinHeaderSerializer {
public:
  ObMulBinHeaderSerializer(ObStringBuffer* buffer,
                           ObMulModeNodeType type,
                           uint64_t total_size,
                           uint64_t count);

  ObMulBinHeaderSerializer(const char* data, uint64_t length);
  ObMulBinHeaderSerializer() {};

  int serialize();
  int deserialize();
  uint8_t get_obj_var_size() { return obj_var_size_; }
  uint8_t get_entry_var_size() { return entry_var_size_; }
  uint8_t get_count_var_size() { return count_var_size_; }

  void set_obj_size(uint64_t size);
  void set_count(uint64_t size);

  uint64_t get_obj_size() { return total_; }
  uint64_t count() { return count_; }

  ObStringBuffer* buffer() { return buffer_; }

  uint64_t start() { return begin_; }
  uint64_t finish() { return begin_ + obj_var_offset_ + obj_var_size_; }
  uint64_t header_size() { return obj_var_offset_ + obj_var_size_; }
  uint8_t get_obj_var_size_type() { return obj_var_size_type_; }
  uint8_t get_entry_var_size_type() { return entry_var_size_type_; }
  uint8_t get_count_var_size_type() { return count_var_size_type_; }
  ObMulModeNodeType type() { return type_; }


  TO_STRING_KV(K_(obj_var_size_type),
               K_(entry_var_size_type),
               K_(count_var_size_type),
               K_(obj_var_size),
               K_(entry_var_size),
               K_(count_var_size),
               K_(obj_var_offset),
               K_(count_var_offset),
               K_(type),
               K_(total),
               K_(count));

  void set_var_value(uint8_t var_size, uint8_t offset, uint64_t value);

  // meta info
  uint8_t obj_var_size_type_;
  uint8_t entry_var_size_type_;
  uint8_t count_var_size_type_;

  // meta info
  uint8_t obj_var_size_;
  uint8_t entry_var_size_;
  uint8_t count_var_size_;

  uint8_t obj_var_offset_;
  uint8_t count_var_offset_;

  ObMulModeNodeType type_;

  // input for serialize
  ObStringBuffer* buffer_;
  uint64_t begin_;

  // serialize & deserialize both use
  int64_t total_;
  int64_t count_;

  // input for deserialize
  const char* data_;
  uint64_t data_len_;
};

class ObMulModeContainerSerializer {
public:
  ObMulModeContainerSerializer(ObIMulModeBase* root, ObStringBuffer* buffer);
  ObMulModeContainerSerializer(const char* data, int64_t length);
  ObMulModeContainerSerializer(ObIMulModeBase* root, ObStringBuffer* buffer, int64_t children_count);

  bool need_serialize_key() {
    return root_->data_type() == OB_XML_TYPE || root_->type() == M_OBJECT;
  }

  bool is_kv_seperate() { return root_->data_type() == OB_XML_TYPE; }

protected:

  ObIMulModeBase* root_;
  ObMulModeNodeType type_;


  int64_t value_entry_start_;
  int64_t value_entry_size_;


  ObMulBinHeaderSerializer header_;
  // for deseialize
  const char* data_;
  int64_t length_;

};


inline ObMulModeBinType get_mul_mode_tc(ObMulModeNodeType type)
{
  ObMulModeBinType res = MulModeMaxType;
  if (type >= M_MAX_TYPE) {
    // do nothing,current not used
  } else if (type >= M_NULL && type < M_MAX_TYPE) {
     res = g_mul_mode_tc[type];
  }
  return res;
}

inline bool is_valid_xml_type(uint8_t type)
{
  return (type >= M_UNPARESED_DOC && type <= M_DTD);
}

inline bool is_extend_type(ObMulModeNodeType type)
{
  return (type >= M_EXTENT_LEVEL2 && type <= M_EXTENT_LEVEL0);
}

inline ObMulModeNodeType eval_data_type(ObMulModeNodeType part1, uint8_t part2)
{
  return static_cast<ObMulModeNodeType>(M_EXTENT_BEGIN0 + 256 * (M_EXTENT_LEVEL0 - part1));
}


inline bool is_scalar_data_type(ObMulModeNodeType type)
{
  ObMulModeBinType tc_type =  get_mul_mode_tc(type);
  return (tc_type == MulModeNull
          || tc_type == MulModeBoolean
          || tc_type == MulModeDecimal
          || tc_type == MulModeInt
          || tc_type == MulModeUint
          || tc_type == MulModeTime
          || tc_type == MulModeDouble
          || tc_type == MulModeString);
}

inline bool is_complex_data_type(ObMulModeNodeType type)
{
  ObMulModeBinType tc_type =  get_mul_mode_tc(type);
  return (tc_type == MulModePair
          || tc_type == MulModeContainer);
}

inline bool is_int_type(ObMulModeNodeType type)
{
  return get_mul_mode_tc(type) == MulModeInt;
}

inline bool is_uint_type(ObMulModeNodeType type)
{
  return get_mul_mode_tc(type) == MulModeUint;
}

inline ObMulModeExtendStorageType get_extend_storage_type(ObMulModeNodeType type)
{
  ObMulModeExtendStorageType res;
  if (is_scalar_data_type(type)) {
    res.first = ObMulModeNodeType::M_EXTENT_LEVEL0 - ((type - 0x7f) >> 8);
  } else {
    res.first = ObMulModeNodeType::M_EXTENT_LEVEL0 - ((type - 0x7f) >> 8);
  }

  res.second = (type & 0xff) - 0x7f;
  return res;
}

inline ObMulModeNodeType get_extend_data_type(ObMulModeExtendStorageType& type)
{
  ObMulModeNodeType res;
  res = static_cast<ObMulModeNodeType>(type.second + (ObMulModeNodeType::M_EXTENT_LEVEL0 - type.first) * 256);
  return res;
}

inline bool is_xml_type(ObIMulModeBase* node) { return node->data_type() == ObNodeDataType::OB_XML_TYPE; }
inline bool is_json_type(ObIMulModeBase* node) { return node->data_type() == ObNodeDataType::OB_XML_TYPE; }

class ObMulModeScalarSerializer {
public:
  // serialize use
  ObMulModeScalarSerializer(ObStringBuffer* buffer)
    : buffer_(buffer) {}

  int serialize_integer(ObIMulModeBase* node, int32_t depth);
  int serialize_string(ObIMulModeBase* node, int32_t depth);
  int serialize_decimal(ObIMulModeBase* node, int32_t depth);
  int serialize_null(ObIMulModeBase* node, int32_t depth);
  int serialize_boolean(ObIMulModeBase* node, int32_t depth);
  int serialize_time(ObIMulModeBase* node, int32_t depth);
  int serialize_double(ObIMulModeBase* node, int32_t depth);

  static int serialize_scalar_header(ObMulModeNodeType type, ObStringBuffer& buffer);
protected:
  ObStringBuffer* buffer_;
};


class ObMulModeVar {
public:
  static int read_size_var(const char *data, uint8_t var_size, int64_t *var);
  static int read_var(const char *data, uint8_t type, uint64_t *var);
  static int append_var(uint64_t var, uint8_t type, ObStringBuffer &result);
  static int reserve_var(uint8_t type, ObStringBuffer &result);
  static int set_var(uint64_t var, uint8_t type, char *pos); // fill var at pos
  static uint64_t get_var_size(uint8_t type);
  static uint8_t get_var_type(uint64_t var);
  static int read_var(const char *data, uint8_t type, int64_t *var);
  static uint64_t var_int2uint(int64_t var);
  static int64_t var_uint2int(uint64_t var, uint8_t entry_size);
  static uint8_t get_var_type(int64_t var);
};

class ObBinMergeCtx {
public:
  ObBinMergeCtx(ObIAllocator* allocator)
    : allocator_(allocator),
      del_map_(allocator),
      defined_ns_idx_(allocator) {}
  ~ObBinMergeCtx() {}
  bool is_all_deleted();
  int get_valid_key_count();
  common::ObIAllocator *allocator_;
  ObStringBuffer* buffer_;
  uint64_t retry_len_;
  uint64_t retry_count_;
  uint8_t reuse_del_map_ : 1;
  uint8_t only_merge_ns_ : 1;
  uint8_t reserve_ : 6;
  // for xml, defined ns or duplicate ns should be delete
  // for json, dulipcate key should be delete
  // deleted key do not need merge
  ObStack<bool> del_map_;
  ObStack<int> defined_ns_idx_;
};
class ObMulModeBinMerge {
public:
// use for merge binary, make sure base_node is binary
  virtual int merge(ObIMulModeBase& origin, ObIMulModeBase& patch, ObIMulModeBase& res);
protected:
  virtual int inner_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                          ObIMulModeBase& patch, ObIMulModeBase& res, bool retry = false);
  virtual int init_merge_info(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                              ObIMulModeBase& patch, ObIMulModeBase& res) = 0;
  virtual int if_need_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                            ObIMulModeBase& patch, ObIMulModeBase& res, bool& need_merge) = 0;
  virtual bool if_need_append_key(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                  ObIMulModeBase& patch, ObIMulModeBase& res) = 0;
  virtual int append_res_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                      ObIMulModeBase& patch, ObIMulModeBase& res) = 0;
  virtual int append_value_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& value, ObIMulModeBase& res) = 0;
  virtual int append_key_without_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                      ObMulBinHeaderSerializer& header, ObIMulModeBase& res) = 0;
  virtual int append_header_to_res(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch,
                                  ObMulBinHeaderSerializer& header, ObIMulModeBase& res);
  virtual int append_merge_key(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch,
                              ObMulBinHeaderSerializer& header, ObIMulModeBase& res) = 0;
  virtual int append_value_by_idx(bool is_origin, int idx, ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                  ObIMulModeBase& patch, ObMulBinHeaderSerializer& header, ObIMulModeBase& res) = 0;
  virtual int set_value_offset(int idx, uint64_t offset, ObBinMergeCtx& ctx, ObIMulModeBase& res) = 0;
  virtual uint64_t estimated_length(bool retry, ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch) = 0;
  virtual uint64_t estimated_count(bool retry, ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch);
  virtual ObMulModeNodeType get_res_type(const ObMulModeNodeType &origin_type, const ObMulModeNodeType &res_type) = 0;
};

} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_MUL_MODE_BIN_BASE