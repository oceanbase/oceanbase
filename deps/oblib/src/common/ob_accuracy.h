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

#ifndef OCEANBASE_COMMON_OB_ACCURACY_
#define OCEANBASE_COMMON_OB_ACCURACY_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase
{
namespace common
{

typedef int32_t ObLength;
typedef int16_t ObPrecision;
typedef int16_t ObScale;

//ObLengthSemantics: used for oracle char/varchar length semantics: byte length, or char length
//In the byte length semantics, size is the maximum number of bytes that can be stored in the column.
//In the character length semantics, size is the maximum number of code points in the database
//character set that can be stored in the column. A code point may have from 1 to 4 bytes depending on
//the database character set and the particular character encoded by the code point.
typedef int16_t ObLengthSemantics;

//If you do not specify a qualifier, the value of the NLS_LENGTH_SEMANTICS parameter of the session
//creating the column defines the length semantics, unless the table belongs to the schema SYS,
//in which case the default semantics is BYTE.
const static int16_t LS_DEFAULT   = 0;//only used for print column type
const static int16_t LS_CHAR      = 1;//char length semantics
const static int16_t LS_BYTE      = 2;//byte length semantics
const static int16_t LS_INVALIED  = -1;//invalid

const char *get_length_semantics_str(const ObLengthSemantics type);
ObLengthSemantics get_length_semantics(const ObString &str);
bool is_oracle_byte_length(const bool is_oracle_mode, const ObLengthSemantics type);


inline const char *get_length_semantics_str(const ObLengthSemantics value)
{
  const char *ret_str = NULL;
  const static char *const_value_str[] = {
      "",
      "CHAR",
      "BYTE"
  };

  if (value < LS_DEFAULT || value > LS_BYTE) {
    ret_str = const_value_str[LS_DEFAULT];
  } else {
    ret_str = const_value_str[value];
  }
  return ret_str;
}

inline ObLengthSemantics get_length_semantics(const ObString &str)
{
  ObLengthSemantics ret_ls = LS_INVALIED;
  if (0 == str.case_compare("BYTE")) {
    ret_ls = LS_BYTE;
  } else if (0 == str.case_compare("CHAR")) {
    ret_ls = LS_CHAR;
  }
  return ret_ls;
}

inline bool is_oracle_byte_length(const bool is_oracle_mode, const ObLengthSemantics type)
{
  return is_oracle_mode && LS_BYTE == type;
}

class ObAccuracy
{
public:
  ObAccuracy() { reset(); }
  ~ObAccuracy() {}
  explicit ObAccuracy(ObLength length) { set_length(length); set_precision(0); set_scale(0);}
  ObAccuracy(ObPrecision precision, ObScale scale) { set_length(0); set_precision(precision); set_scale(scale); }
  ObAccuracy(const ObAccuracy &other) { accuracy_ = other.accuracy_; }
  OB_INLINE void set_accuracy(const ObAccuracy &accuracy) { accuracy_ = accuracy.accuracy_; }
  OB_INLINE void set_accuracy(const int64_t &accuracy) { accuracy_ = accuracy; }
  OB_INLINE void set_length(const ObLength length) { length_ = length; }

  //set both length and length_semantics in case of someone forget it
  OB_INLINE void set_full_length(const ObLength length, const ObLengthSemantics length_semantics, const bool is_oracle_mode)
  {
    length_ = length;
    if (is_oracle_mode) {
      length_semantics_ = length_semantics;
    }
  }
  OB_INLINE void set_precision(const ObPrecision precision) { precision_ = precision; }
  OB_INLINE void set_length_semantics(const ObLengthSemantics length_semantics) { length_semantics_ = length_semantics; }
  OB_INLINE void set_scale(const ObScale scale) { scale_ = scale; }
  // get union data
  OB_INLINE int64_t get_accuracy() const { return accuracy_; }
  // get detail data
  OB_INLINE ObLength get_length() const { return length_; }
  OB_INLINE ObPrecision get_precision() const { return precision_; }
  OB_INLINE ObLengthSemantics get_length_semantics() const { return length_semantics_; }
  OB_INLINE ObScale get_scale() const { return scale_; }
  OB_INLINE void reset() { accuracy_ = -1; }
  static bool is_default_number_or_int(const ObAccuracy &accuracy)
  { return -1 == accuracy.get_precision(); }
  static bool is_default_number(const ObAccuracy &accuracy)
  { return -1 == accuracy.get_precision() && 0 != accuracy.get_scale(); }
  static bool is_default_number_int(const ObAccuracy &accuracy)
  { return -1 == accuracy.get_precision() && 0 == accuracy.get_scale(); }

  // for ObNumberType/ObUNumberType and !is_default_number()
  int64_t get_fixed_number_precision() const {
    int64_t precision = get_precision();
    if (is_default_number_int(*this)) {
      precision = DEFAULT_NUMBER_PRECISION_FOR_INTEGER;
    }
    return precision;
  }

  // for ObNumberType/ObUNumberType and !is_default_number()
  int64_t get_fixed_number_scale() const {
    int64_t scale = get_scale();
    if (is_default_number_int(*this)) {
      scale = DEFAULT_NUMBER_SCALE_FOR_INTEGER;
    }
    return scale;
  }

public:
  OB_INLINE ObAccuracy &operator =(const ObAccuracy &other)
  {
    if (this != &other) {
      accuracy_ = other.accuracy_;
    }
    return *this;
  }
  OB_INLINE bool operator ==(const ObAccuracy &other) const { return accuracy_ == other.accuracy_; }
  OB_INLINE bool operator !=(const ObAccuracy &other) const { return accuracy_ != other.accuracy_; }
public:
  static const int64_t PS_QUESTION_MARK_DEDUCE_LEN = 2000; // used for ps prepare in oracle mode
  // why we expose this 3 arrays directly?
  // imagine that we add 'if ... else' statements in ddl_default_accuracy() first,
  // and 'int ret = OB_SUCCESS' and 'return ret' statements too.
  // then the caller must add some 'if (OB_FAIL(...)) ... else LOG_WARN()'.
  // at last we get much more codes which are very, very, very ugly.
  // so I think this is a better way: expose this 3 static const arrays directly.
  static const ObAccuracy DDL_DEFAULT_ACCURACY[ObMaxType];
  static const ObAccuracy DDL_DEFAULT_ACCURACY2[ORACLE_MODE + 1][ObMaxType];
  static const ObAccuracy MAX_ACCURACY[ObMaxType];
  static const ObAccuracy MAX_ACCURACY2[ORACLE_MODE + 1][ObMaxType];
  static const ObAccuracy DML_DEFAULT_ACCURACY[ObMaxType];
  static const ObAccuracy MAX_ACCURACY_OLD[ObMaxType];
public:
  TO_STRING_KV(N_LENGTH, length_,
               N_PRECISION, precision_,
               N_SCALE, scale_);
  NEED_SERIALIZE_AND_DESERIALIZE;

  union
  {
    int64_t accuracy_;
    struct {
      ObLength length_;//count in charater. NOT byte
      union {
        ObPrecision precision_;
        ObLengthSemantics length_semantics_;
      };
      ObScale scale_;
    };
  };
};

}
}

#endif /* OCEANBASE_COMMON_OB_ACCURACY_ */
