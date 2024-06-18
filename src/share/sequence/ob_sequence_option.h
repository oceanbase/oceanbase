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

#ifndef __OB_SHARE_SEQUENCE_OB_SEQUENCE_OPTION_H__
#define __OB_SHARE_SEQUENCE_OB_SEQUENCE_OPTION_H__

#include "lib/container/ob_bit_set.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
namespace share
{

enum ObSequenceArg
{
  INCREMENT_BY = 0,
  START_WITH,
  MAXVALUE,
  NOMAXVALUE,
  MINVALUE,
  NOMINVALUE,
  CACHE,
  NOCACHE,
  CYCLE,
  NOCYCLE,
  ORDER,
  NOORDER,
  RESTART
};

struct ObSequenceValueAllocator : public common::ObDataBuffer
{
public:
    ObSequenceValueAllocator()
        : ObDataBuffer(static_cast<char *>(buf_), common::number::ObNumber::MAX_BYTE_LEN)
    {}
    ~ObSequenceValueAllocator() = default;
private:
  char buf_[common::number::ObNumber::MAX_BYTE_LEN];
};

struct ObSequenceValue
{
  OB_UNIS_VERSION(1);
public:
  ObSequenceValue();
  ObSequenceValue(int64_t init_val);
  int assign(const ObSequenceValue &other);
  int set(const common::number::ObNumber &val);
  int set(const char *val);
  int set(int64_t val);
  common::number::ObNumber &val() { return val_; }
  const common::number::ObNumber &val() const { return val_; }
  TO_STRING_KV("val", val_.format());
private:
  char buf_[common::number::ObNumber::MAX_BYTE_LEN];
  common::number::ObNumber val_;
};

struct ObSequenceMaxMinInitializer
{
public:
  ObSequenceMaxMinInitializer();

  // Oracle defaults to 28 integers of 9s and 27 negatives of 9s
  // https://docs.oracle.com/database/121/SQLRF/statements_6017.htm
  // If the SQL exceeds this range, it will be automatically truncated
  //
  // MIN_VALUE = -999999999999999999999999999
  // MAX_VALUE = 9999999999999999999999999999
  //
  // For programming convenience, the default value saved in Option is NO_MIN_VALUE, NO_MAX_VALUE
  // 1. In the resolve phase, if it is found that the user has not set MIN_VALUE or used NOMINVALUE,
  // Then keep the NO_MIN_VALUE in Option unchanged,
  // 2. During the execution phase, if the value in Option is found to be NO_MIN_VALUE, then according to the semantics
  // Help fill in the correct value. For example, when INCREMENT_BY> 0, minvalue will be automatically set to 1
  // If other values are filled in Option, the executor will check the validity of this value, and an error will be reported if it is illegal
  //
  static ObSequenceValue NO_MAX_VALUE; // NO_MAX_VALUE = MAX_VALUE + 1
  static ObSequenceValue NO_MIN_VALUE; // NO_MIN_VALUE = MINVALUE - 1
  static ObSequenceValue MAX_VALUE;
  static ObSequenceValue MIN_VALUE;

  // MySQL support int64_t sequence value
  //
  // MYSQL_MIN_VALUE = -9223372036854775808  -(2^63+1)
  // MYSQL_MAX_VALUE = +9223372036854775807  +(2^63)
  static ObSequenceValue MYSQL_NO_MAX_VALUE; // MYSQL_NO_MAX_VALUE = MYSQL_MAX_VALUE + 1
  static ObSequenceValue MYSQL_NO_MIN_VALUE; // MYSQL_NO_MIN_VALUE = MYSQL_MINVALUE - 1
  static ObSequenceValue MYSQL_MAX_VALUE;
  static ObSequenceValue MYSQL_MIN_VALUE;

public:
  static common::number::ObNumber &no_max_value()
  {
    if (lib::is_oracle_mode()) {
      return NO_MAX_VALUE.val();
    } else {
      return MYSQL_NO_MAX_VALUE.val();
    }
  }

  static common::number::ObNumber &no_min_value()
  {
    if (lib::is_oracle_mode()) {
      return NO_MIN_VALUE.val();
    } else {
      return MYSQL_NO_MIN_VALUE.val();
    }
  }

  static common::number::ObNumber &max_value()
  {
    if (lib::is_oracle_mode()) {
      return MAX_VALUE.val();
    } else {
      return MYSQL_MAX_VALUE.val();
    }
  }

  static common::number::ObNumber &min_value()
  {
    if (lib::is_oracle_mode()) {
      return MIN_VALUE.val();
    } else {
      return MYSQL_MIN_VALUE.val();
    }
  }
};

enum ObSequenceCacheOrderMode
{
  OLD_ACTION = 0,
  NEW_ACTION = 1,
};

class ObSequenceOption
{
  OB_UNIS_VERSION(2);
public:
  static const int64_t NO_CACHE = 1;
public:
  ObSequenceOption() :
      increment_by_(static_cast<int64_t>(1)),
      start_with_(static_cast<int64_t>(0)),
      maxvalue_(),
      minvalue_(),
      cache_(static_cast<int64_t>(20)),
      cycle_(false),
      order_(false),
      flag_(0)
  {
    set_nomaxvalue();
    set_nominvalue();
  }
  // Used for internal initialization
  OB_INLINE int set_increment_by(int64_t value) { return increment_by_.set(value); }
  OB_INLINE int set_start_with(int64_t value)   { return start_with_.set(value); }
  OB_INLINE int set_max_value(int64_t value)    { return maxvalue_.set(value); }
  OB_INLINE int set_min_value(int64_t value)    { return minvalue_.set(value); }
  OB_INLINE int set_cache_size(int64_t value)   { return cache_.set(value); }
  // Used for user SQL specified values
  OB_INLINE int set_increment_by(const common::number::ObNumber & value) { return increment_by_.set(value); }
  OB_INLINE int set_start_with(const common::number::ObNumber & value)   { return start_with_.set(value); }
  OB_INLINE int set_max_value(const common::number::ObNumber & value)    { return maxvalue_.set(value); }
  OB_INLINE int set_min_value(const common::number::ObNumber & value)    { return minvalue_.set(value); }
  OB_INLINE int set_cache_size(const common::number::ObNumber & value)   { return cache_.set(value); }
  OB_INLINE void set_cycle_flag(bool flag) { cycle_ = flag; }
  OB_INLINE void set_order_flag(bool flag) { order_ = flag; }
  OB_INLINE void set_flag(int64_t flag) { flag_ = flag; }
  OB_INLINE void set_cache_order_mode(ObSequenceCacheOrderMode mode) { cache_order_mode_ = mode; }

  OB_INLINE const common::number::ObNumber &get_increment_by() const { return increment_by_.val(); }
  OB_INLINE const common::number::ObNumber &get_start_with()   const { return start_with_.val(); }
  OB_INLINE const common::number::ObNumber &get_max_value()    const { return maxvalue_.val(); }
  OB_INLINE const common::number::ObNumber &get_min_value()    const { return minvalue_.val(); }
  OB_INLINE const common::number::ObNumber &get_cache_size()   const { return cache_.val(); }
  OB_INLINE bool get_cycle_flag() const { return cycle_; }
  OB_INLINE bool get_order_flag() const { return order_; }
  OB_INLINE int64_t get_flag() const { return flag_; }
  OB_INLINE ObSequenceCacheOrderMode get_cache_order_mode() const { return cache_order_mode_; }


  // helper func
  OB_INLINE const common::number::ObNumber &increment_by() const { return get_increment_by(); }
  OB_INLINE const common::number::ObNumber &start_with() const { return get_start_with(); }
  OB_INLINE const common::number::ObNumber &maxvalue() const { return get_max_value(); }
  OB_INLINE const common::number::ObNumber &minvalue() const { return get_min_value(); }
  OB_INLINE const common::number::ObNumber &cache() const { return get_cache_size(); }


  OB_INLINE bool has_set_maxvalue() const { return ObSequenceMaxMinInitializer::no_max_value() != maxvalue_.val(); }
  OB_INLINE bool has_set_minvalue() const { return ObSequenceMaxMinInitializer::no_min_value() != minvalue_.val(); }
  OB_INLINE bool has_cache() const { return cache_.val() > static_cast<int64_t>(1); }
  OB_INLINE bool has_cycle() const { return cycle_; }
  OB_INLINE bool has_order() const { return order_; }

  // helper func
  void set_nomaxvalue() { (void)maxvalue_.set(ObSequenceMaxMinInitializer::no_max_value()); }
  void set_nominvalue() { (void)minvalue_.set(ObSequenceMaxMinInitializer::no_min_value()); }
  int set_maxvalue() { return maxvalue_.set(ObSequenceMaxMinInitializer::max_value()); }
  int set_minvalue() { return minvalue_.set(ObSequenceMaxMinInitializer::min_value()); }


  void reset()
  {
    increment_by_.set(static_cast<int64_t>(1));
    start_with_.set(static_cast<int64_t>(0));
    set_nomaxvalue();
    set_nominvalue();
    cache_.set(static_cast<int64_t>(20));
    cycle_ = false;
    order_ = false;
    flag_ = 0;
  }

  /* 如果 bitset 中未设置，则使用 from 中的值，
   * 否则保持 this 中的值不变
   */
  int merge(const common::ObBitSet<> &opt_bitset,
             const share::ObSequenceOption &from)
  {
    int ret = common::OB_SUCCESS;
    if (OB_SUCC(ret) &&
        !opt_bitset.has_member(ObSequenceArg::INCREMENT_BY)) {
      ret = increment_by_.assign(from.increment_by_);
    }
    if (OB_SUCC(ret) &&
        !opt_bitset.has_member(ObSequenceArg::START_WITH)) {
      ret = start_with_.assign(from.start_with_);
    }
    if (OB_SUCC(ret) &&
        !opt_bitset.has_member(ObSequenceArg::MAXVALUE) &&
        !opt_bitset.has_member(ObSequenceArg::NOMAXVALUE)) {
      ret = maxvalue_.assign(from.maxvalue_);
    }
    if (OB_SUCC(ret) &&
        !opt_bitset.has_member(ObSequenceArg::MINVALUE) &&
        !opt_bitset.has_member(ObSequenceArg::NOMINVALUE)) {
      ret = minvalue_.assign(from.minvalue_);
    }
    if (OB_SUCC(ret) &&
        !opt_bitset.has_member(ObSequenceArg::CACHE) &&
        !opt_bitset.has_member(ObSequenceArg::NOCACHE)) {
      ret = cache_.assign(from.cache_);
    }
    if (OB_SUCC(ret) &&
        !opt_bitset.has_member(ObSequenceArg::CYCLE) &&
        !opt_bitset.has_member(ObSequenceArg::NOCYCLE)) {
      cycle_ = from.cycle_;
    }
    if (OB_SUCC(ret) &&
        !opt_bitset.has_member(ObSequenceArg::ORDER) &&
        !opt_bitset.has_member(ObSequenceArg::NOORDER)) {
      order_ = from.order_;
    }
    return ret;
  }

  int assign(const share::ObSequenceOption &from);

  TO_STRING_KV(K_(increment_by),
               K_(start_with),
               K_(maxvalue),
               K_(minvalue),
               K_(cache),
               K_(cycle),
               K_(order),
               K_(flag));

private:
  ObSequenceValue increment_by_;
  ObSequenceValue start_with_;
  ObSequenceValue maxvalue_;
  ObSequenceValue minvalue_;
  ObSequenceValue cache_;
  bool cycle_;
  bool order_;
  union {
    int64_t flag_;
    struct {
      ObSequenceCacheOrderMode cache_order_mode_:1;
    };
  };
};



}
}
#endif /* __OB_SHARE_SEQUENCE_OB_SEQUENCE_OPTION_H__ */
//// end of header file




