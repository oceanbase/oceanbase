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

#ifndef OCEANBASE_OB_ENUM_SET_EMTA_TYPE_
#define OCEANBASE_OB_ENUM_SET_EMTA_TYPE_

#include <stdint.h>
#include <string.h>
#include "common/object/ob_object.h"
#include "lib/container/ob_array_helper.h"
#include "lib/string/ob_string.h"
#include "share/ob_cluster_version.h"

namespace oceanbase {
namespace common {

/**
 * The struct ObEnumSetMeta is primarily used to store complex meta-information about
 * `enum` and `set` types. It contains `obj_meta` to save `cs_type` and array `str_values` that
 * encapsulates the set of string representations associated with the enumerated type values.
 * It is intended to be stored within `ObSubSchemaCtx` and is utilized during the execution phase
 * to retrieve the extended type info about enum/set type.
 */
struct ObEnumSetMeta
{
  OB_UNIS_VERSION(1);

private:
  typedef ObIArray<common::ObString> ObStrValues;

public:
  /**
   * Since the `scale` field within the `ObExprResType` of the enum/set type is not used, we use
   * this field to save the meta state.
   */
  enum MetaState
  {
    UNINITIALIZED = -1, // meta has not been saved, -1(SCALE_UNKNOWN_YET) as the default value
    SKIP = 0, // used in pl, compilation and running are separated, so skip this scenario
    READY = 1, // meta has been saved in `ObSubSchemaCtx`
    MAX_STATE
  };

public:
  ObEnumSetMeta(common::ObIAllocator *alloc = NULL) : obj_meta_(), str_values_(NULL),
    allocator_(alloc) {}
  ObEnumSetMeta(ObObjMeta obj_meta, const ObStrValues *str_value) :
      obj_meta_(obj_meta), str_values_(str_value), allocator_(NULL) {}
  inline bool is_valid() const
  {
    return obj_meta_.is_enum_or_set() && NULL != str_values_ && !str_values_->empty();
  }
  bool is_same(const ObObjMeta &obj_meta, const ObStrValues &str_value) const;
  bool is_same(const ObEnumSetMeta &other) const;
  uint64_t hash() const;
  int hash(uint64_t &res) const
  {
    res = hash();
    return common::OB_SUCCESS;
  }
  inline bool operator ==(const ObEnumSetMeta &other) const { return is_same(other); }
  inline bool operator !=(const ObEnumSetMeta &other) const { return !this->operator==(other); }
  int deep_copy(ObIAllocator &allocator, ObEnumSetMeta *&dst) const;
  void destroy()
  {
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(str_values_)) {
      allocator_->free(const_cast<ObStrValues *>(str_values_));
      allocator_ = NULL;
    }
  }
  inline uint64_t get_signature() const { return udt_id_; }
  inline const common::ObObjMeta &get_obj_meta() const { return obj_meta_; }
  inline ObObjType get_type() const { return obj_meta_.get_type(); }
  inline ObCollationType get_collation_type() const { return obj_meta_.get_collation_type(); }
  inline ObCollationLevel get_collation_level() const { return obj_meta_.get_collation_level(); }
  inline const ObStrValues *get_str_values() const { return str_values_; }

  TO_STRING_KV(K_(obj_meta), KP_(str_values), KP_(allocator));

private:
  common::ObObjMeta obj_meta_;  // original type obj meta
  union
  {
    const ObStrValues *str_values_; // pointer to extended type info
    uint64_t udt_id_;
  };
  common::ObIAllocator *allocator_; // used for deserialize only
};


} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_ENUM_SET_EMTA_TYPE_
