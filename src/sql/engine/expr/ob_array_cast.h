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

#ifndef OCEANBASE_OB_ARRAY_CAST_
#define OCEANBASE_OB_ARRAY_CAST_
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "lib/udt/ob_array_fixed_size.h"
#include "lib/udt/ob_array_binary.h"
#include "lib/udt/ob_array_nested.h"
#include "lib/udt/ob_vector_type.h"
#include "share/object/ob_obj_cast.h"
#include "deps/oblib/src/lib/json_type/ob_json_tree.h"

namespace oceanbase {
namespace common {
class ObJsonNode;
}
namespace sql {

enum ARRAY_CAST_TYPE {
  FIXED_SIZE_FIXED_SIZE = 0,
  CAST_TYPE_MAX,
};

class ObArrayTypeCast
{
public:
  ObArrayTypeCast() {};
  virtual ~ObArrayTypeCast() {};
  virtual int cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
                   ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode = 0) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObArrayTypeCast);
};

class ObArrayFixedSizeCast : public ObArrayTypeCast
{
public:
  int cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
           ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode = 0);
};

class ObVectorDataCast : public ObArrayTypeCast
{
public:
  int cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
           ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode = 0);
  uint32_t dim_cnt_;
};

class ObArrayBinaryCast : public ObArrayTypeCast
{
public:
  int cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
           ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode = 0);
};

class ObArrayNestedCast : public ObArrayTypeCast
{
public :
int cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
         ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode = 0);
}
;



class ObArrayCastUtils
{
public:
  static int string_cast_vector(common::ObIAllocator &alloc, ObString &arr_text, ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type);
  static int string_cast(common::ObIAllocator &alloc, ObString &arr_text, ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type);
  static int cast_get_element(ObIArrayType *src, const ObCollectionBasicType *elem_type, uint32_t idx, ObObj &src_elem);
  static int cast_add_element(common::ObIAllocator &alloc, ObObj &src_elem,  ObIArrayType *dst, const ObCollectionBasicType *dst_elem_type, ObCastMode mode);
  static int add_json_node_to_array(common::ObIAllocator &alloc, ObJsonNode &j_node, const ObCollectionTypeBase *elem_type, ObIArrayType *dst);
  static int add_vector_element(const double value, const ObCollectionTypeBase *elem_type, ObIArrayType *dst);
public:
  static const int32_t NULL_STR_LEN = 4;
};

class ObArrayTypeCastFactory
{
public:
  ObArrayTypeCastFactory() {};
  virtual ~ObArrayTypeCastFactory() {};
  static int alloc(common::ObIAllocator &alloc, const ObCollectionTypeBase &src_array_meta,
                   const ObCollectionTypeBase &dst_array_meta, ObArrayTypeCast *&arr_cast);
private:
  DISALLOW_COPY_AND_ASSIGN(ObArrayTypeCastFactory);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_OB_ARRAY_CAST_
