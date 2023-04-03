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

#define USING_LOG_PREFIX SHARE

#include "share/ob_errno.h"
#include "ob_sequence_option_builder.h"
#include "share/sequence/ob_sequence_option.h"


using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::share;

int ObSequenceOptionBuilder::build_create_sequence_option(
    const common::ObBitSet<> &opt_bitset,
    share::ObSequenceOption &opt_new)
{
  int ret = OB_SUCCESS;
  // fill missing default value, to compatible with Oracle behavior
  ObSequenceOption &option = opt_new;

  if (OB_FAIL(pre_check_sequence_option(opt_new))) {
    LOG_WARN("fail pre check sequence option for create sequence", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (option.get_increment_by() > static_cast<int64_t>(0)) {
      if (!option.has_set_minvalue() && OB_FAIL(option.set_min_value(static_cast<int64_t>(1)))) {
        LOG_WARN("fail set default min value", K(ret));
      } else if (!option.has_set_maxvalue() && OB_FAIL(option.set_maxvalue())) {
        LOG_WARN("fail set default max value", K(ret));
      }
    } else if (option.get_increment_by() < static_cast<int64_t>(0)) {
      if (!option.has_set_maxvalue() && OB_FAIL(option.set_max_value(static_cast<int64_t>(-1)))) {
        LOG_WARN("fail set default max value", K(ret));
      } else if (!option.has_set_minvalue() && OB_FAIL(option.set_minvalue())) {
        LOG_WARN("fail set default min value", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !opt_bitset.has_member(ObSequenceArg::START_WITH)) {
    if (option.has_set_minvalue() &&
        option.get_increment_by() > static_cast<int64_t>(0)) {
      ret = option.set_start_with(option.minvalue());
    } else if (option.has_set_maxvalue() &&
               option.get_increment_by() < static_cast<int64_t>(0)) {
      ret = option.set_start_with(option.maxvalue());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_sequence_option(opt_bitset, option))) {
      LOG_WARN("sequence option not valid", K(option), K(ret));
    }
  }
  return ret;
}

int ObSequenceOptionBuilder::build_alter_sequence_option(
    const common::ObBitSet<> &opt_bitset,
    const share::ObSequenceOption &opt_old,
    share::ObSequenceOption &opt_new,
    bool can_alter_start_with)
{
  int ret = OB_SUCCESS;
  // If there is no setting in alter, the old value is used, that is, the option in schema (opt_old)
  if (OB_FAIL(opt_new.merge(opt_bitset, opt_old))) {
    LOG_WARN("fail merge options", K(opt_new), K(opt_old), K(ret));
  } else if (!can_alter_start_with && opt_bitset.has_member(ObSequenceArg::START_WITH)) {
    // cannot alter starting sequence number
    ret = OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED;
    LOG_WARN("cannot alter starting sequence number", K(ret));
  } else if (can_alter_start_with && !opt_bitset.has_member(ObSequenceArg::START_WITH)) {
    if (opt_new.get_increment_by() >  static_cast<int64_t>(0)) {
      if (OB_FAIL(opt_new.set_start_with(opt_new.minvalue()))) {
        LOG_WARN("fail assign min value as start with", K(opt_new));
      }
    } else {
      if (OB_FAIL(opt_new.set_start_with(opt_new.maxvalue()))) {
        LOG_WARN("fail assign max value as start with", K(opt_new));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_sequence_option(opt_bitset, opt_new))) {
      LOG_WARN("sequence option not valid", K(opt_new), K(ret));
    }
  }
  return ret;
}

// Oracle's behavior is not rigorous, here is just to be consistent with Oracle's behavior:
// Oracle allows: create sequence s1; alter sequence s1 cycle;
// Oracle prohibits: create sequence s1 cycle; (Report ascending sequences that CYCLE must specify MAXVALUE)
int ObSequenceOptionBuilder::pre_check_sequence_option(const ObSequenceOption &option)
{
  int ret = OB_SUCCESS;
  if (option.has_cycle() &&
      option.get_increment_by() > static_cast<int64_t>(0) &&
      !option.has_set_maxvalue()) {
    // ascending sequences that CYCLE must specify MAXVALUE
    ret = OB_ERR_SEQ_REQUIRE_MAXVALUE;

  } else if (option.has_cycle() &&
             option.get_increment_by() < static_cast<int64_t>(0) &&
             !option.has_set_minvalue()) {
    // descending sequences that CYCLE must specify MINVALUE
    ret = OB_ERR_SEQ_REQUIRE_MINVALUE;

  }
  return ret;
}

int ObSequenceOptionBuilder::check_sequence_option(
    const common::ObBitSet<> &opt_bitset,
    const ObSequenceOption &option)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObNumber cache_range;
  ObNumber value_range;
  ObNumberCalc max(option.get_max_value(), allocator);
  ObNumberCalc cache(option.get_cache_size(), allocator);
  // value_range = option.get_max_value() - option.get_min_value()
  // cache_range = option.get_increment_by().abs() *  option.get_cache_size() - 1
  if (OB_FAIL(check_sequence_option_integer(opt_bitset, option))) {
    LOG_WARN("fail check sequence options integer", K(option), K(ret));
  } else if (OB_FAIL(max.sub(option.get_min_value()).get_result(value_range))) {
    LOG_WARN("fail calc number", K(ret));
  } else if (OB_FAIL(cache.mul(option.get_increment_by().abs()).sub(static_cast<int64_t>(1)).get_result(cache_range))) {
    LOG_WARN("fail calc number", K(ret));
  } else if (option.get_increment_by() == static_cast<int64_t>(0)) {
    // INCREMENT must be a nonzero integer
    ret = OB_ERR_SEQ_INCREMENT_CAN_NOT_BE_ZERO;

  } else if (option.get_cache_size() <= static_cast<int64_t>(1)
             && opt_bitset.has_member(ObSequenceArg::CACHE)) {
    // the number of values to CACHE must be greater than 1 if speficied
    ret = OB_ERR_SEQ_CACHE_TOO_SMALL;

  } else if (option.has_set_minvalue() &&
      option.has_set_maxvalue() &&
      option.minvalue() >= option.maxvalue()) {
    // MINVALUE must be less than MAXVALUE
    ret = OB_ERR_MINVALUE_LARGER_THAN_MAXVALUE;

  } else if (option.has_set_maxvalue() &&
             option.get_start_with() > option.maxvalue()) {
    // START WITH cannot be more than MAXVALUE
    ret = OB_ERR_START_WITH_EXCEED_MAXVALUE;

  } else if (option.has_set_minvalue() &&
             option.get_start_with() < option.minvalue()) {
    // START WITH cannot be less than MINVALUE
    ret = OB_ERR_START_WITH_LESS_THAN_MINVALUE;

  } else if (value_range < option.get_increment_by().abs()) {
    // INCREMENT must be less than MAXVALUE minus MINVALUE
    ret = OB_ERR_SEQ_INCREMENT_TOO_LARGE;

  } else if (option.has_cycle() &&
             option.get_increment_by() > static_cast<int64_t>(0) &&
             !option.has_set_maxvalue()) {
    // ascending sequences that CYCLE must specify MAXVALUE
    ret = OB_ERR_SEQ_REQUIRE_MAXVALUE;

  } else if (option.has_cycle() &&
             option.get_increment_by() < static_cast<int64_t>(0) &&
             !option.has_set_minvalue()) {
    // descending sequences that CYCLE must specify MINVALUE
    ret = OB_ERR_SEQ_REQUIRE_MINVALUE;

  } else if (option.has_cycle() && value_range < cache_range) {
    // number to CACHE must be less than one cycle
    ret = OB_ERR_SEQ_CACHE_TOO_LARGE;

  }
  LOG_INFO("sequence option", K(ret), K(option));
  return ret;
}

int ObSequenceOptionBuilder::check_sequence_option_integer(
    const common::ObBitSet<> &opt_bitset,
    const ObSequenceOption &option)
{
  int ret = OB_SUCCESS;
  if (opt_bitset.has_member(ObSequenceArg::MINVALUE) &&
      !option.get_min_value().is_integer()) {
    ret = OB_ERR_SEQ_OPTION_MUST_BE_INTEGER;
    LOG_USER_ERROR(OB_ERR_SEQ_OPTION_MUST_BE_INTEGER, "MINVALUE");
  } else if (opt_bitset.has_member(ObSequenceArg::MAXVALUE) &&
             !option.get_max_value().is_integer()) {
    ret = OB_ERR_SEQ_OPTION_MUST_BE_INTEGER;
    LOG_USER_ERROR(OB_ERR_SEQ_OPTION_MUST_BE_INTEGER, "MAXVALUE");
  } else if (opt_bitset.has_member(ObSequenceArg::INCREMENT_BY) &&
             !option.get_increment_by().is_integer()) {
    ret = OB_ERR_SEQ_OPTION_MUST_BE_INTEGER;
    LOG_USER_ERROR(OB_ERR_SEQ_OPTION_MUST_BE_INTEGER, "INCREMENT");
  } else if (opt_bitset.has_member(ObSequenceArg::START_WITH) &&
             !option.get_start_with().is_integer()) {
    ret = OB_ERR_SEQ_OPTION_MUST_BE_INTEGER;
    LOG_USER_ERROR(OB_ERR_SEQ_OPTION_MUST_BE_INTEGER, "START WITH");
  } else if (opt_bitset.has_member(ObSequenceArg::CACHE) &&
             !option.get_cache_size().is_integer()) {
    ret = OB_ERR_SEQ_OPTION_MUST_BE_INTEGER;
    LOG_USER_ERROR(OB_ERR_SEQ_OPTION_MUST_BE_INTEGER, "CACHE");
  }
  return ret;
}
