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

#ifndef _OB_EXPR_RES_TYPE_H
#define _OB_EXPR_RES_TYPE_H 1

#include "common/object/ob_object.h"
#include "common/ob_field.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/utility.h"
#include "common/ob_accuracy.h"
#include "common/object/ob_obj_type.h"
#include "lib/enumset/ob_enum_set_meta.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
namespace sql
{

typedef common::ObObjMeta ObExprCalcType;

/* 说明：为什么从ObObjMeta继承？
 *  这是为了一个特殊的需求新增的：在calc_result_type阶段，
 *  推导过程可能跟常量值有关系。
 *  对于一个常量，有可能不光需要知道它的type是什么，
 *  还需要知道它的值是什么，才能推出跟MySQL兼容的行为。
 */
class ObRawExprResType : public common::ObObjMeta
{
public:
  ObRawExprResType() : ObObjMeta(),
    res_flags_(0),
    accuracy_()
  {
  }
  OB_INLINE int assign(const ObRawExprResType &other)
  {
    int ret = common::OB_SUCCESS;
    if (OB_LIKELY(this != &other)) {
      // assign func in ObFixedArray is not used for deep copy
      common::ObObjMeta::operator=(other);//default assignment operator is enough
      this->res_flags_ = other.res_flags_;
      this->accuracy_ = other.accuracy_;
    }
    return ret;
  }

  OB_INLINE bool operator ==(const ObRawExprResType &other) const
  {
    return (ObObjMeta::operator==(other) && accuracy_ == other.accuracy_);
  }
  OB_INLINE bool operator !=(const ObRawExprResType &other) const { return !this->operator ==(other); }
public:
  OB_INLINE void reset()
  {
    ObObjMeta::reset();
    res_flags_ = 0;
    accuracy_.reset();
  }
  OB_INLINE void set_accuracy(int64_t accuracy) { accuracy_.set_accuracy(accuracy); }
  // accuracy.
  OB_INLINE void set_accuracy(const common::ObAccuracy &accuracy)
  {
    accuracy_.set_accuracy(accuracy);
  }
  OB_INLINE void set_length(const common::ObLength length) { accuracy_.set_length(length); }
  OB_INLINE void set_length_within_max_length(common::ObLength length, bool is_from_pl)
  {
    common::ObLength max_length = length;
    if (lib::is_oracle_mode()) {
      if (is_varchar() || is_nvarchar2()) {
        max_length = common::OB_MAX_ORACLE_VARCHAR_LENGTH;
      } else if (is_char() || is_nchar()) {
        max_length = is_from_pl ? common::OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE
                                : common::OB_MAX_ORACLE_CHAR_LENGTH_BYTE;
      }
    } else {
      if (is_char()) {
        max_length = common::OB_MAX_CHAR_LENGTH;
      } else if (is_varchar()) {
        max_length = common::OB_MAX_VARCHAR_LENGTH;
      }
    }
    set_length(MIN(length, max_length));
  }
  //set both length and length_semantics in case of someone forget it
  OB_INLINE void set_length_semantics(const common::ObLengthSemantics value)
  {
    if (lib::is_oracle_mode()) {
      accuracy_.set_length_semantics(value);
    }
  }
  OB_INLINE void set_full_length(const common::ObLength length, const common::ObLengthSemantics length_semantics)
  {
    set_length(length);
    set_length_semantics(length_semantics);
  }
  OB_INLINE void set_udt_id(uint64_t id)
  {
    accuracy_.set_accuracy(id);
  }
  OB_INLINE void set_precision(const common::ObPrecision precision) { accuracy_.set_precision(precision);}
  OB_INLINE void set_scale(const common::ObScale scale) { accuracy_.set_scale(scale); }
  OB_INLINE const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  /* character count*/
  OB_INLINE common::ObLength get_length() const
  {
    int ret = common::OB_SUCCESS;
    common::ObLength length = accuracy_.get_length();
    if (!is_string_type() && !is_enum_or_set() && !is_enumset_inner_type()
        && !is_ext() && !is_lob_locator() && !is_user_defined_sql_type()
        && !is_collection_sql_type()) {
      if (OB_FAIL(common::ObField::get_field_mb_length(get_type(),
                                                       get_accuracy(),
                                                       common::CS_TYPE_INVALID,
                                                       length))) {
        SQL_RESV_LOG(WARN, "failed to get length", K(ret), K(common::lbt()), N_TYPE, get_type());
      }
    }
    return length;
  }
  OB_INLINE common::ObLengthSemantics get_length_semantics() const
  {
    return accuracy_.get_length_semantics();
  }
  OB_INLINE uint64_t get_expr_udt_id() const
  {
    uint64_t udt_id = OB_INVALID_ID;
    if (is_user_defined_sql_type()) {
      if (is_xml_sql_type()) {
        udt_id = T_OBJ_XML;
      } else {
        // NOTICE: process new sql type id in here.
      }
    } else {
      udt_id = get_udt_id();
    }
    return udt_id;
  }
  OB_INLINE uint64_t get_udt_id() const
  {
    return accuracy_.get_accuracy();
  }

  OB_INLINE int get_length_for_meta_in_bytes(common::ObLength &length) const
  {
    int ret = common::OB_SUCCESS;
    length = -1;
    if (is_string_or_lob_locator_type() || is_enum_or_set() || is_enumset_inner_type() || is_json() || is_geometry()) {
      if (OB_FAIL(common::ObField::get_field_mb_length(get_type(),
                                                       get_accuracy(),
                                                       get_collation_type(),
                                                       length))) {
        SQL_RESV_LOG(WARN, "failed to get length of varchar", K(ret));
      }
    } else {
      if (OB_FAIL(common::ObField::get_field_mb_length(get_type(),
                                                       get_accuracy(),
                                                       common::CS_TYPE_INVALID,
                                                       length))) {
        SQL_RESV_LOG(WARN, "failed to get length of non-varchar", K(ret), K(common::lbt()), N_TYPE, get_type());
      }
    }
    return ret;
  }
  /* meta info for client */
  OB_INLINE int get_length_for_meta_in_bytes(common::ObLength &length, ObCollationType collation_type) const
  {
    int ret = common::OB_SUCCESS;
    length = -1;
    if (is_string_or_lob_locator_type() || is_enum_or_set() || is_enumset_inner_type() || is_json() || is_geometry()) {
      if (OB_FAIL(common::ObField::get_field_mb_length(get_type(),
                                                       get_accuracy(),
                                                       collation_type,
                                                       length))) {
        SQL_RESV_LOG(WARN, "failed to get length of varchar", K(ret));
      }
    } else {
      if (OB_FAIL(common::ObField::get_field_mb_length(get_type(),
                                                       get_accuracy(),
                                                       common::CS_TYPE_INVALID,
                                                       length))) {
        SQL_RESV_LOG(WARN, "failed to get length of non-varchar", K(ret), K(common::lbt()), N_TYPE, get_type());
      }
    }
    return ret;
  }

  OB_INLINE common::ObPrecision get_precision() const { return accuracy_.get_precision(); }
  OB_INLINE common::ObScale get_scale() const
  {
    common::ObScale scale = accuracy_.get_scale();
    if (ob_is_integer_type(get_type())) {
      scale = common::DEFAULT_SCALE_FOR_INTEGER;
    }
    return scale;
  }
  OB_INLINE common::ObScale get_mysql_compatible_scale() const
  {
    return static_cast<common::ObScale>(accuracy_.get_scale() == -1
        ? (lib::is_oracle_mode() ? ORACLE_NOT_FIXED_DEC : NOT_FIXED_DEC)
        : accuracy_.get_scale());
  }

  OB_INLINE bool is_null() const { return common::ObNullType == get_type(); }
  OB_INLINE bool is_mysql_question_mark_type() const
  { return is_varbinary() && 0 == get_length(); }
  OB_INLINE bool is_oracle_question_mark_type() const
  { return is_char() && common::ObAccuracy::PS_QUESTION_MARK_DEDUCE_LEN == get_length(); }

  OB_INLINE bool is_not_null_for_read() const { return has_result_flag(NOT_NULL_FLAG); }
  OB_INLINE bool is_not_null_for_write() const { return has_result_flag(NOT_NULL_WRITE_FLAG); }

  OB_INLINE void set_result_flag(uint32_t flag) { res_flags_ |= flag; }
  OB_INLINE void unset_result_flag(uint32_t flag) { res_flags_ &= (~flag); }
  OB_INLINE bool has_result_flag(uint32_t flag) const { return res_flags_ & flag; }
  OB_INLINE uint32_t get_result_flag() const { return res_flags_; }
  OB_INLINE bool is_oracle_integer() const { return lib::is_oracle_mode() && is_number() 
                                                    && -1 == get_accuracy().get_precision() 
                                                    && 0 == get_accuracy().get_scale(); }
  OB_INLINE void mark_enum_set_with_subschema(const ObEnumSetMeta::MetaState state)
  {
    if (is_enum_or_set()) {
      set_scale(state);
    }
  }
  OB_INLINE void mark_sql_enum_set_with_subschema()
  {
    if (is_enum_or_set()) {
      set_scale(ObEnumSetMeta::MetaState::SQL);
    }
  }
  OB_INLINE void mark_pl_enum_set_with_subschema()
  {
    if (is_enum_or_set()) {
      set_scale(ObEnumSetMeta::MetaState::PL);
    }
  }
  OB_INLINE ObEnumSetMeta::MetaState get_enum_set_subschema_state() const
  { return is_enum_or_set() ? static_cast<ObEnumSetMeta::MetaState>(get_scale()) :
                              ObEnumSetMeta::MetaState::UNINITIALIZED; }
  OB_INLINE bool is_sql_enum_set_with_subschema() const
  { return is_enum_or_set() && get_scale() == ObEnumSetMeta::MetaState::SQL; }
  OB_INLINE bool is_pl_enum_set_with_subschema() const
  { return is_enum_or_set() && get_scale() == ObEnumSetMeta::MetaState::PL; }
  OB_INLINE bool is_enum_set_with_subschema() const
  { return is_enum_or_set() && (ObEnumSetMeta::MetaState::SQL == get_scale() || ObEnumSetMeta::MetaState::PL == get_scale()); }
  OB_INLINE void reset_enum_set_meta_state() { set_scale(ObEnumSetMeta::MetaState::UNINITIALIZED); }

  uint64_t hash(uint64_t seed) const
  {
    seed = common::do_hash(type_, seed);
    seed = common::do_hash(cs_level_, seed);
    seed = common::do_hash(cs_type_, seed);
    seed = common::do_hash(scale_, seed);
    seed = common::do_hash(accuracy_, seed);
    seed = common::do_hash(res_flags_, seed);
    return seed;
  }
  int hash(uint64_t &hash_val, uint64_t seed) const
  {
    hash_val = hash(seed);
    return OB_SUCCESS;
  }
  // others.
  INHERIT_TO_STRING_KV(N_META,
                       ObObjMeta,
                       N_ACCURACY,
                       accuracy_,
                       N_FLAG,
                       res_flags_);
protected:
  uint32_t res_flags_; // BINARY, NUM, NOT_NULL, TIMESTAMP, etc
                       // reference: src/lib/regex/include/mysql_com.h
  common::ObAccuracy accuracy_; //当是Extend类型时，用来表示复杂数据类型的id
};

class ObExprResType : public ObRawExprResType
{
  OB_UNIS_VERSION(1);
public:
  ObExprResType() : ObRawExprResType(),
    calc_type_(),
    calc_accuracy_(),
    param_(),
    cast_mode_(0)
  {
  }
  ObExprResType(const ObRawExprResType &base) : ObRawExprResType(base),
    calc_type_(),
    calc_accuracy_(),
    param_(),
    cast_mode_(0)
  {
  }
  OB_INLINE int assign(const ObExprResType &other)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObRawExprResType::assign(other))) {
    } else {
      this->calc_type_ = other.calc_type_;
      this->calc_accuracy_ = other.calc_accuracy_;
      this->param_ = other.param_;
      this->cast_mode_ = other.cast_mode_;
    }
    return ret;
  }
  OB_INLINE void reset()
  {
    ObRawExprResType::reset();
    calc_type_.reset();
    calc_accuracy_.reset();
    param_.reset();
    cast_mode_ = 0;
  }


  // calc_accuracy.
  OB_INLINE void set_calc_accuracy(const common::ObAccuracy &accuracy)
  {
    calc_accuracy_.set_accuracy(accuracy);
  }
  OB_INLINE void set_calc_precision(ObPrecision precision) { calc_accuracy_.set_precision(precision); }
  OB_INLINE void set_calc_scale(common::ObScale scale) { calc_accuracy_.set_scale(scale); }
  OB_INLINE void set_calc_length_semantics(const common::ObLengthSemantics value)
  {
    if (lib::is_oracle_mode()) {
      calc_accuracy_.set_length_semantics(value);
    }
  }
  OB_INLINE void set_calc_length(common::ObLength length) { calc_accuracy_.set_length(length); }
  OB_INLINE const common::ObAccuracy &get_calc_accuracy() const { return calc_accuracy_; }
  OB_INLINE common::ObScale get_calc_scale() const { return calc_accuracy_.get_scale(); }
  OB_INLINE common::ObLength get_calc_length() const { return calc_accuracy_.get_length(); }
  // compare type
  OB_INLINE ObExprCalcType &get_calc_meta() { return calc_type_; }
  OB_INLINE const ObExprCalcType &get_calc_meta() const { return calc_type_; }
  OB_INLINE void set_calc_meta(const ObExprCalcType &meta) { calc_type_ = meta; }

  // calc_type: 表示表达式计算时，表达式将转换成calc_type后再计算
  OB_INLINE void set_calc_type(const common::ObObjType &type) { calc_type_.set_type(type); }
  OB_INLINE void set_calc_subschema_id(const uint16_t subschema_id) { calc_type_.set_subschema_id(subschema_id); }
  OB_INLINE void set_calc_collation_utf8()
  {
    set_calc_collation_by_charset(common::CHARSET_UTF8MB4);
  }
  OB_INLINE void set_calc_type_default_varchar()
  {
    set_calc_type(common::ObVarcharType);
    set_calc_collation_utf8();
  }
  OB_INLINE void set_calc_collation_ascii_compatible()
  {
    if (ObCharset::is_cs_nonascii(get_collation_type())) {
      set_calc_type_default_varchar();
    }
  }
  OB_INLINE void set_calc_collation_by_charset(common::ObCharsetType charset_type)
  {
    set_calc_collation_type(
          common::ObCharset::get_default_collation_by_mode(charset_type, lib::is_oracle_mode()));
  }
  OB_INLINE common::ObObjType get_calc_type() const { return calc_type_.get_type(); }
  OB_INLINE common::ObObjTypeClass get_calc_type_class() const
  {
    return calc_type_.get_type_class();
  }

  OB_INLINE void set_calc_collation_level(common::ObCollationLevel cs_level)
  {
    calc_type_.set_collation_level(cs_level);
  }
  OB_INLINE void set_calc_collation_type(common::ObCollationType cs_type)
  {
    calc_type_.set_collation_type(cs_type);
  }
  OB_INLINE void set_calc_collation(const ObExprResType &type)
  {
    calc_type_.set_collation_type(type.get_calc_collation_type());
    calc_type_.set_collation_level(type.get_calc_collation_level());
  }
  OB_INLINE common::ObCollationType get_calc_collation_type() const
  {
    return calc_type_.get_collation_type();
  }
  OB_INLINE common::ObCollationLevel get_calc_collation_level() const
  {
    return calc_type_.get_collation_level();
  }

  OB_INLINE void set_param(const common::ObObj &param) { param_ = param; }
  OB_INLINE const common::ObObj &get_param() const { return param_; }
  OB_INLINE bool is_column() const { return !is_literal(); }
  OB_INLINE bool is_literal() const { return get_param().get_type() == get_type()
                                             && get_param().get_collation_type() == get_collation_type(); }
  void add_decimal_int_cast_mode(uint64_t cm)
  {
    cast_mode_ = cast_mode_ | (cm & (CM_CONST_TO_DECIMAL_INT_DOWN |
                                     CM_CONST_TO_DECIMAL_INT_EQ |
                                     CM_CONST_TO_DECIMAL_INT_UP));
  }
  uint64_t get_cast_mode() const { return cast_mode_; }
  INHERIT_TO_STRING_KV("RawExprResType", ObRawExprResType,
                       N_CALC_TYPE,
                       calc_type_,
                       K_(calc_accuracy),
                       K_(param),
                       K_(cast_mode));
protected:
  ObExprCalcType calc_type_;
  common::ObAccuracy calc_accuracy_; //当是Extend类型时，length字段用来表示复杂数据类型的size
  common::ObObj param_;
  uint64_t cast_mode_; // store cast mode for decimal int single side cast
};

typedef common::ObSEArray<ObExprResType, 5, common::ModulePageAllocator, true> ObExprResTypes;
typedef common::ObIArray<ObExprResType> ObIExprResTypes;

enum ObSubQueryKey : int8_t
{
  T_WITH_NONE,
  T_WITH_ANY,
  T_WITH_ALL
};

}
}

#endif /* _OB_EXPR_RES_TYPE_H */
