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

#ifndef OCEANBASE_COMMON_FIELD_
#define OCEANBASE_COMMON_FIELD_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_fast_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_bit_set.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "common/ob_accuracy.h"

namespace oceanbase
{
namespace common
{

struct ObParamedSelectItemCtx
{
public:
  // paramed column for display
  ObString paramed_cname_;
  // Record every parameterized? Offset relative to field name
  common::ObSEArray<int64_t, 4, ModulePageAllocator, true> param_str_offsets_;
  // Record the subscript of each parameterized parameter in the param store
  common::ObSEArray<int64_t, 4, ModulePageAllocator, true> param_idxs_;
  // Record the subscript of the negative constant in the param store
  common::ObBitSet<OB_DEFAULT_BITSET_SIZE, ModulePageAllocator, true> neg_param_idxs_;
  bool esc_str_flag_; // Whether to copy the escaped column name
  bool need_check_dup_name_; // Whether the mark needs to check raw_param, which is used to parameterize the select item constant
  bool is_column_field_;

  ObParamedSelectItemCtx():
    paramed_cname_(), param_str_offsets_(), param_idxs_(),
    neg_param_idxs_(), esc_str_flag_(false), need_check_dup_name_(false),
    is_column_field_(false) {}

  int deep_copy(const ObParamedSelectItemCtx &other, ObIAllocator *allocator);
  int64_t to_string(char *buffer, int64_t length) const;
  int64_t get_convert_size() const;

  void reset()
  {
    paramed_cname_.assign(NULL, 0);
    param_str_offsets_.reset();
    param_idxs_.reset();
    neg_param_idxs_.reset();
    esc_str_flag_ = false;
    need_check_dup_name_ = false;
    is_column_field_ = false;
  }
};

// NOTE: ~ObFileld may not called, all memory should be auto free.
class ObField
{
  OB_UNIS_VERSION(1);
public:
  ObString dname_; //database name for display
  ObString tname_; // table name for display
  ObString org_tname_; // original table name
  ObString cname_;     // column name for display
  ObString org_cname_; // original column name
  ObObj type_; // value type
  ObString type_owner_; // udt relation name, only valid when type is ObExtended
  ObString type_name_; // udt relation name, only valid when type is ObExtended
  ObObj default_value_; //default value, only effective when command was COM_FIELD_LIST
  ObAccuracy accuracy_;
  uint16_t charsetnr_;    //collation of table, this is to correspond with charset.
  uint16_t flags_; // binary, not_null flag etc
  int32_t length_;//in bytes not characters. used for client
  ObParamedSelectItemCtx *paramed_ctx_; // context for paramed select item
  bool is_paramed_select_item_;
  bool is_hidden_rowid_;
  uint8_t inout_mode_; // using for routine/anonymous resultset

  ObField()
    : dname_(), tname_(), org_tname_(), cname_(), org_cname_(), type_(),
      type_owner_(), type_name_(),
      default_value_(ObObj::make_nop_obj()), accuracy_(),
      charsetnr_(CS_TYPE_UTF8MB4_GENERAL_CI),
      flags_(0), length_(0), paramed_ctx_(NULL), is_paramed_select_item_(false),
      is_hidden_rowid_(false), inout_mode_(0)
  {
  }

  int64_t get_convert_size() const; //deep copy size
  int64_t to_string(char *buffer, int64_t length) const;
  //why deep_copy do not full deep copy
  int full_deep_copy(const ObField &other, ObIAllocator *allocator);
  int deep_copy(const ObField &other, ObIAllocator *allocator);

  int update_field_mb_length();

  static int get_field_mb_length(const ObObjType type,
                                 const ObAccuracy &accuracy,
                                 const ObCollationType charsetnr,
                                 int32_t &length);

private:
  static int32_t my_decimal_precision_to_length_no_truncation(int16_t precision,
                                                              int16_t scale,
                                                              bool unsigned_flag)
  {
    /*
     * When precision is 0 it means that original length was also 0. Thus
     * unsigned_flag is ignored in this case.
     */
    return (int32_t)(precision + (scale > 0 ? 1 : 0) +
                     ((unsigned_flag || !precision) ? 0 : 1));
  }
};

static const int64_t COMMON_PARAM_NUM = 32;
typedef common::ObFixedArray<common::ObField, common::ObIAllocator> ParamsFieldArray;
typedef common::ObFixedArray<common::ObField, common::ObIAllocator> ColumnsFieldArray;
typedef common::ObIArray<common::ObField> ParamsFieldIArray;
typedef common::ObIArray<common::ObField> ColumnsFieldIArray;
}
}
#endif
