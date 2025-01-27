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

#ifndef OCEANBASE_OB_SQL_COLLECTION_TYPE_
#define OCEANBASE_OB_SQL_COLLECTION_TYPE_
#include <stdint.h>
#include <string.h>
#include "common/object/ob_object.h"
#include "lib/string/ob_string.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_field.h"

namespace oceanbase {
namespace common {

enum ObNestedType {
  OB_BASIC_TYPE = 0,
  OB_ARRAY_TYPE = 1,
  OB_VECTOR_TYPE = 2,
};

class ObCollectionTypeBase {
public:
  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(type_id));

  virtual int deep_copy(ObIAllocator &allocator, ObCollectionTypeBase *&dst) const = 0;
  virtual const ObDataType &get_basic_meta(uint32_t &depth) const = 0;
  uint16_t get_compatiable_type_id() const { return type_id_ == OB_VECTOR_TYPE ? OB_ARRAY_TYPE : type_id_; }
  bool is_vector_type() { return type_id_ == OB_VECTOR_TYPE; }
  uint16_t type_id_; // array/vector/map
};

class ObCollectionArrayType : public ObCollectionTypeBase
{
public:
  ObCollectionArrayType(ObIAllocator &allocator) : allocator_(allocator) , dim_cnt_(0), element_type_(nullptr) {}
  virtual ~ObCollectionArrayType() {}
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int deep_copy(ObIAllocator &allocator, ObCollectionTypeBase *&dst) const;
  bool has_same_super_type(const ObCollectionArrayType &other) const;
  const ObDataType &get_basic_meta(uint32_t &depth) const { depth++; return element_type_->get_basic_meta(depth);}
  int generate_spec_type_info(const ObString &type, ObString &type_info);
  bool check_is_valid_vector() const;
  TO_STRING_KV(K_(dim_cnt), KPC_(element_type));

  ObIAllocator &allocator_;
  uint32_t dim_cnt_; // vector dimension
  ObCollectionTypeBase *element_type_;
};

class ObCollectionBasicType : public ObCollectionTypeBase
{
public:
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int deep_copy(ObIAllocator &allocator, ObCollectionTypeBase *&dst) const;
  bool has_same_super_type(const ObCollectionBasicType &other) const;
  const ObDataType &get_basic_meta(uint32_t &depth) const { UNUSED(depth); return basic_meta_; }
  ObDataType basic_meta_;
};

typedef struct ObSqlCollectionInfo
{
  OB_UNIS_VERSION(1);
public:

  ObSqlCollectionInfo(ObIAllocator &allocator)
    : allocator_(allocator), name_len_(0),
      name_def_(nullptr), collection_meta_(nullptr) {}
  ObSqlCollectionInfo(common::ObIAllocator *allocator)
    : ObSqlCollectionInfo(*allocator) {}
   virtual ~ObSqlCollectionInfo() {}
  void set_name(const ObString &name)
  {
    name_def_ = name.ptr();
    name_len_ = name.length();
  }

  bool is_same(const ObSqlCollectionInfo &other) // test only remove later
  {
    bool is_same = (strncmp(name_def_, other.name_def_, name_len_) == 0);
    return is_same;
  }

  ObString get_def_string() const {return ObString(name_len_, name_def_);}
  int64_t get_signature() const { return get_def_string().hash(); }
  int get_child_def_string(ObString &child_def) const;
  int deep_copy(ObIAllocator &allocator, ObSqlCollectionInfo *&dst) const;
  int create_meta_info_by_name(const std::string &name, ObCollectionTypeBase *&meta_info, uint8_t &arr_depth);
  int set_element_meta_info(const std::string &name, uint8_t meta_attr_idx, ObCollectionBasicType *meta_info);
  int set_element_meta_unsigned(ObCollectionBasicType *meta_info);
  static int collection_type_deserialize(ObIAllocator &allocator, const char* buf, const int64_t data_len, int64_t& pos,
                                         ObCollectionTypeBase *&collection_meta);
  bool has_same_super_type(const ObSqlCollectionInfo &other) const;
  const ObDataType &get_basic_meta(uint32_t &depth) const { depth = 0; return collection_meta_->get_basic_meta(depth); }
  int parse_type_info();
  TO_STRING_KV(K(ObString(name_len_, name_def_)));

private:
  inline bool isNumber(std::string &str) {
    for (int i = 0; i < str.length(); i++) {
      if (!std::isdigit(str[i])) {
        return false;
      }
    }
    return !str.empty();
  }

public:
  ObIAllocator &allocator_;
  size_t name_len_;
  const char *name_def_;
  ObCollectionTypeBase *collection_meta_;
} ObSqlCollectionInfo;

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_SQL_COLLECTION_TYPE_
