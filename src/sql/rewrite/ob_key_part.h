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

#ifndef OCEANBASE_SQL_REWRITE_OB_KEY_PART_
#define OCEANBASE_SQL_REWRITE_OB_KEY_PART_

#include "share/ob_define.h"
#include "lib/objectpool/ob_tc_factory.h"
#include "lib/container/ob_array_serialization.h"
#include "common/object/ob_obj_compare.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/geo/ob_geo_common.h"

namespace oceanbase
{
namespace sql
{
class ObQueryRange;

enum ObKeyPartType
{
  T_NORMAL_KEY = 0,
  T_LIKE_KEY,
  T_IN_KEY,
  T_GEO_KEY
};

enum InType
{
  T_IN_KEY_PART,
  T_NOT_IN_KEY_PART
};
class ObKeyPart;
const int64_t MAX_EXTRACT_IN_COLUMN_NUMBER = 6;

class ObKeyPartId
{
  OB_UNIS_VERSION(1);
public:
  ObKeyPartId(uint64_t data_table_id = common::OB_INVALID_ID, uint64_t column_id = common::OB_INVALID_ID)
      : table_id_(data_table_id),
        column_id_(column_id)
  {
  }

  inline uint64_t hash() const
  {
    uint64_t hash_code = 0;
    hash_code = common::murmurhash(&table_id_, sizeof(table_id_), hash_code);
    hash_code = common::murmurhash(&column_id_, sizeof(column_id_), hash_code);
    return hash_code;
  }

  inline bool operator==(const ObKeyPartId &other) const
  {
    return (table_id_ == other.table_id_) && (column_id_ == other.column_id_);
  }

  inline bool operator!=(const ObKeyPartId &other) const
  {
    return !(*this == other);
  }

  TO_STRING_KV(N_TID, table_id_,
               N_CID, column_id_);
  uint64_t table_id_;
  uint64_t column_id_;
};

class ObKeyPartPos
{
  OB_UNIS_VERSION(1);
public:
  ObKeyPartPos()
      : offset_(-1),
      column_type_(),
      enum_set_values_()
  {
  }

  ObKeyPartPos(int64_t offset, ObExprResType type)
      : offset_(offset),
      column_type_(type),
      enum_set_values_()
  {
  }

  inline bool operator==(const ObKeyPartPos &other) const
  {
    bool is_equal = true;
    is_equal = ((offset_ == other.offset_)
                && (column_type_ == other.column_type_)
                && (enum_set_values_.count() == other.enum_set_values_.count()));
    for (int64_t i = 0; is_equal && i < enum_set_values_.count(); ++i) {
      is_equal = enum_set_values_.at(i) == other.enum_set_values_.at(i);
    }
    return is_equal;
  }

  inline bool operator!=(const ObKeyPartPos &other) const
  {
    return !(*this == other);
  }
  int set_enum_set_values(common::ObIAllocator &allocator,
                          const common::ObIArray<common::ObString> &enum_set_values);
  inline const common::ObIArray<common::ObString> &get_enum_set_values() const { return enum_set_values_; }
  int assign(const ObKeyPartPos &other);
  TO_STRING_KV(N_OFFSET, offset_,
               N_COLUMN_TYPE, column_type_,
               N_ENUM_SET_VALUES, enum_set_values_);

  int64_t offset_;
  ObExprResType column_type_;
  common::ObSArray<common::ObString> enum_set_values_;
};

struct ObNormalKeyPart
{
  ObNormalKeyPart()
    : start_(),
      include_start_(false),
      end_(),
      include_end_(false),
      always_true_(false),
      always_false_(false) {}
  bool operator ==(const ObNormalKeyPart &other) const
  {
    return (start_ == other.start_)
        && (end_ == other.end_)
        && (include_start_ == other.include_start_)
        && (include_end_ == other.include_end_)
        && (always_false_ == other.always_false_)
        && (always_true_ == other.always_true_);
  }
  //normal key type
  common::ObObj start_;
  bool include_start_;
  common::ObObj end_;
  bool include_end_;
  bool always_true_;
  bool always_false_;
};

struct ObLikeKeyPart
{
  common::ObObj pattern_;
  common::ObObj escape_;
};

struct InParamMeta
{
  OB_UNIS_VERSION(1);
  public:
    InParamMeta()
      : pos_(),
        vals_() {
    }
    ~InParamMeta() { reset(); }
    inline void reset() { vals_.reset(); }
    ObKeyPartPos pos_;
    ObArray<ObObj> vals_;
    int assign(const InParamMeta &other, ObIAllocator &alloc);
    TO_STRING_KV(K_(pos), K_(vals));
};

struct InParamValsWrapper
{
  InParamValsWrapper():
    param_vals_(),
    cmp_funcs_()
    { }
  int assign(const InParamValsWrapper &other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(param_vals_.assign(other.param_vals_))) {
      SQL_REWRITE_LOG(WARN, "failed to assign param vals", K(ret));
    } else if (OB_FAIL(cmp_funcs_.assign(other.cmp_funcs_))) {
      SQL_REWRITE_LOG(WARN, "failed to assign cmp funcs", K(ret));
    }
    return ret;
  }
  inline bool operator==(const InParamValsWrapper &other) const
  {
    int ret = OB_SUCCESS;
    int64_t param_cnt = param_vals_.count();
    int64_t other_param_cnt = other.param_vals_.count();
    int64_t cmp_funcs_cnt = cmp_funcs_.count();
    int64_t other_cmp_funcs_cnt = other.cmp_funcs_.count();
    bool bret = param_cnt == other_param_cnt && cmp_funcs_cnt == other_cmp_funcs_cnt &&
                param_cnt == cmp_funcs_cnt;
    ObCompareCtx cmp_ctx(ObMaxType, CS_TYPE_INVALID, true, INVALID_TZ_OFF, default_null_pos());
    for (int64_t i = 0; bret && i < param_cnt; ++i) {
      obj_cmp_func cmp_op_func = cmp_funcs_.at(i);
      OB_ASSERT(NULL != cmp_op_func);
      bret = (ObObjCmpFuncs::CR_TRUE == cmp_op_func(param_vals_.at(i), other.param_vals_.at(i), cmp_ctx));
    }
    return bret;
  }
  inline bool operator!=(const InParamValsWrapper &other) const
  {
    return !(*this == other);
  }
  inline int hash(uint64_t &hash_code) const
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_vals_.count(); ++i) {
      // no need to calculate hash for cmp_funcs_
      ret = param_vals_.at(i).hash(hash_code, hash_code);
    }
    return ret;
  }
  ObSEArray<ObObj, MAX_EXTRACT_IN_COLUMN_NUMBER> param_vals_;
  ObSEArray<obj_cmp_func, MAX_EXTRACT_IN_COLUMN_NUMBER> cmp_funcs_;
  TO_STRING_KV(K_(param_vals));
};

typedef ObSEArray<int64_t, MAX_EXTRACT_IN_COLUMN_NUMBER, ModulePageAllocator> OffsetsArr;
typedef ObSEArray<InParamMeta *, MAX_EXTRACT_IN_COLUMN_NUMBER, ModulePageAllocator> InParamsArr;
struct ObInKeyPart
{
  ObInKeyPart()
    : table_id_(common::OB_INVALID_ID),
      in_params_(),
      offsets_(),
      missing_offsets_(),
      in_type_(T_IN_KEY_PART),
      is_strict_in_(true),
      contain_questionmark_(false) { }
  ~ObInKeyPart() { reset(); }
  void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    in_params_.reset();
    offsets_.reset();
    missing_offsets_.reset();
    in_type_ = T_IN_KEY_PART;
    is_strict_in_ = true;
    contain_questionmark_ = false;
  }
  bool is_inited() const { return !offsets_.empty() && !in_params_.empty(); }
  int64_t get_min_offset() const { return !is_inited() ? -1 : offsets_.at(0); }
  int64_t get_max_offset() const { return !is_inited() ? -1 : offsets_.at(offsets_.count() - 1); }
  int64_t get_param_val_cnt() const { return !is_inited() ? 0 : in_params_.at(0)->vals_.count(); }
  int64_t get_valid_offset_cnt(int64_t max_valid_off) const;
  bool is_in_precise_get() const { return get_param_val_cnt() == 1; }
  bool is_single_in() const { return offsets_.count() == 1; }
  // only can be called when no questionmark existed
  bool find_param(const int64_t offset, InParamMeta *&param_meta);
  bool offsets_same_to(const ObInKeyPart *other) const;
  int union_in_key(ObInKeyPart *other);
  int get_dup_vals(int64_t offset, const common::ObObj &val, common::ObIArray<int64_t> &dup_val_idx);
  int remove_in_dup_vals();
  InParamMeta* create_param_meta(common::ObIAllocator &alloc);
  int get_obj_cmp_funcs(ObIArray<obj_cmp_func> &cmp_funcs);

  uint64_t table_id_;
  InParamsArr in_params_;
  OffsetsArr offsets_;
  // if key is is not strict in, need to store the missing offsets
  OffsetsArr missing_offsets_;
  InType in_type_;
  bool is_strict_in_;
  bool contain_questionmark_;
};

struct ObGeoKeyPart
{
  common::ObObj wkb_;
  common::ObGeoRelationType geo_type_;
  common::ObObj distance_;
};

class ObKeyPart : public common::ObDLinkBase<ObKeyPart>
{
  OB_UNIS_VERSION_V(1);
public:
  ObKeyPart(common::ObIAllocator &allocator,
            uint64_t column_id = common::OB_INVALID_ID,
            int32_t offset = -1)
      : allocator_(allocator),
        pos_(),
        null_safe_(false),
        key_type_(T_NORMAL_KEY),
        normal_keypart_(),
        item_next_(NULL),
        or_next_(NULL),
        and_next_(NULL),
        rowid_column_idx_(OB_INVALID_ID),
        is_phy_rowid_key_part_(false)
  { }
  virtual ~ObKeyPart() { reset(); }
  virtual void reset();
  void reset_key();
  typedef common::hash::ObHashMap<int64_t, ObSEArray<int64_t, 16>> SameValIdxMap;
  static int try_cast_value(const ObDataTypeCastParams &dtc_params, ObIAllocator &alloc,
                            const ObKeyPartPos &pos, ObObj &value, int64_t &cmp);
  inline bool operator <=(const ObKeyPart &other) const { return pos_.offset_ <= other.pos_.offset_; }

  inline void set_normal_start(ObKeyPart *other)
  {
    if (NULL != other && other->is_normal_key() && is_normal_key()) {
      this->id_ = other->id_;
      this->pos_ = other->pos_;
      this->normal_keypart_->start_ = other->normal_keypart_->start_;
      this->normal_keypart_->include_start_ = other->normal_keypart_->include_start_;
    }
  }
  inline void set_normal_end(ObKeyPart *other)
  {
    if (NULL != other && other->is_normal_key() && is_normal_key()) {
      this->id_ = other->id_;
      this->pos_ = other->pos_;
      this->normal_keypart_->end_ = other->normal_keypart_->end_;
      this->normal_keypart_->include_end_ = other->normal_keypart_->include_end_;
    }
  }

  void link_gt(ObKeyPart *and_next);
  ObKeyPart *general_or_next();
  ObKeyPart *cut_general_or_next();

  bool equal_to(const ObKeyPart *other);
  bool key_node_is_equal(const ObKeyPart *other);
  bool is_equal_condition() const;
  bool is_range_condition() const;
  bool is_question_mark() const;

  inline bool is_rowid_key_part() const { return rowid_column_idx_ != OB_INVALID_ID; }
  inline bool is_always_true() const { return is_normal_key() && normal_keypart_->always_true_; }
  inline bool is_always_false() const { return is_normal_key() && normal_keypart_->always_false_; }
  inline bool is_normal_key() const { return T_NORMAL_KEY == key_type_ && normal_keypart_ != NULL; }
  inline bool is_like_key() const { return T_LIKE_KEY == key_type_ && like_keypart_ != NULL; }
  inline bool is_in_key() const {return T_IN_KEY == key_type_ && in_keypart_ != NULL && in_keypart_->in_type_ == T_IN_KEY_PART; }
  inline bool is_not_in_key() const {return T_IN_KEY == key_type_ && in_keypart_ != NULL && in_keypart_->in_type_ == T_NOT_IN_KEY_PART; }
  inline bool is_geo_key() const { return T_GEO_KEY == key_type_ && geo_keypart_ != NULL; }

  int create_normal_key();
  int create_like_key();
  int create_in_key();
  int create_not_in_key();
  int create_geo_key();

  inline ObNormalKeyPart *get_normal_key()
  {
    ObNormalKeyPart *normal_key = NULL;
    if (T_NORMAL_KEY == key_type_) {
      normal_key = normal_keypart_;
    }
    return normal_key;
  }
  inline ObLikeKeyPart *get_like_key()
  {
    ObLikeKeyPart *like_key = NULL;
    if (T_LIKE_KEY == key_type_) {
      like_key = like_keypart_;
    }
    return like_key;
  }
  inline ObInKeyPart *get_in_key()
  {
    ObInKeyPart *in_key = NULL;
    if (T_IN_KEY == key_type_) {
      in_key = in_keypart_;
    }
    return in_key;
  }

  ///////// intersect /////////
  bool has_intersect(const ObKeyPart *other) const;
  int intersect(ObKeyPart *other, bool contain_row);
  int intersect_in(ObKeyPart *other);
  int intersect_two_in_keys(ObKeyPart *other,
                            const ObIArray<int64_t> &common_offsets);
  int collect_same_val_idxs(const bool is_first_offset,
                            const InParamMeta *left_param,
                            const InParamMeta *right_param,
                            SameValIdxMap &lr_idx);
  int merge_two_in_keys(ObKeyPart *other, const SameValIdxMap &lr_idx);

  ///////// union /////////
  bool union_key(const ObKeyPart *other);
  int union_in_dup_vals(ObKeyPart *other, bool &is_unioned);

  ///////// formalize key /////////
  // for normal keypart, check key part can be always true of false
  // for in keypart, adjust params according to the invalid_offsets
  // and check it can be always true
  int formalize_keypart(bool contain_row);
  int get_dup_param_and_vals(common::ObIArray<int64_t> &dup_param_idx,
                             common::ObIArray<int64_t> &invalid_val_idx);
  int remove_in_params(const common::ObIArray<int64_t> &invalid_param_idx, bool always_true);
  int remove_in_params_vals(const common::ObIArray<int64_t> &val_idx);
  int remove_in_dup_vals();
  int convert_to_true_or_false(bool is_always_true);

  int cast_value_type(const common::ObDataTypeCastParams &dtc_params, bool contain_row, bool &is_bound_modified);

  // copy all except next_ pointer
  int deep_node_copy(const ObKeyPart &other);
  int shallow_node_copy(const ObKeyPart &other);
  bool is_phy_rowid_key_part() const { return is_phy_rowid_key_part_; }
  bool is_logical_rowid_key_part() const {
    return !is_phy_rowid_key_part_ && rowid_column_idx_ != OB_INVALID_ID; }
  DECLARE_TO_STRING;
private:
  DISALLOW_COPY_AND_ASSIGN(ObKeyPart);
public:
  common::ObIAllocator &allocator_;
  ObKeyPartId id_;
  ObKeyPartPos pos_;
  bool null_safe_;
  ObKeyPartType key_type_;
  union {
    //normal key type
    ObNormalKeyPart *normal_keypart_;
    //like expr type
    ObLikeKeyPart *like_keypart_;
    // in expr type
    ObInKeyPart *in_keypart_;
    //geo expr type
    ObGeoKeyPart *geo_keypart_;
  };
  //list member
  ObKeyPart *item_next_;
  ObKeyPart *or_next_;
  ObKeyPart *and_next_;
  int64_t rowid_column_idx_;//used for rowid to extract query range, mark nth column in rowid.
  bool is_phy_rowid_key_part_;//mark the rowid key part is physical rowid or not.

};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_REWRITE_OB_KEY_PART_
