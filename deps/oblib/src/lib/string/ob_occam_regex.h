/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef LIB_STRING_OB_OCCAM_REGEX_H
#define LIB_STRING_OB_OCCAM_REGEX_H
#include "lib/ob_errno.h"
#include "ob_string_holder.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
// regex_match/regex_search/regex_replace interfaces is defined in CPP11 ISO standard.
// ObOccamRegex has similar interfaces with CPP11 ISO standard regex interface definition,
// but implemented as a wrapper of POSIX regex APIs
// (not std::regex because it's poor implementation - not fixed bugs(see GNU Bug 86164) and performance issues).
//
// The regex rule version used is [POSIX Extended Regular Expression](specifed 'REG_EXTENDED' when call regcomp())
class ObOccamRegex {
public:
  // POSIX API must specified group size
  // Theoretically, group size of regex pattern can be calculated before call POSIX APIs, but it's too complicated to process with all
  // coner-cases, if I successfully implement this, I will have successfully implemented part of a regex engine.
  // So MAX_GROUP_SIZE is defined here, groups in your regex pattern should less then this limitation.
  // (The entire regular expression's match string are stored in group[0], group (.*) described in regex pattern stored from group[1]).
  static const constexpr int64_t MAX_GROUP_SIZE = 64;
  // Flag can be specified to control regex engine process behavior.(if not specified, default is None)
  // for now, there is just : IgnoreCase to tell regex engine process with case-insensitive.
  enum Flag : int64_t {
    None = 0,
    IgnoreCase = 1LL << 0,
  };
  // check if regex_pattern can fully matched input string.
  // fully matched means regex_pattern matches stared from input[0], ended at input[size].
  // for example:
  //   regex_match("foo.txt, bar.txt, baz.dat", "([a-z]+).txt", result), result will be empty cause not fully matched
  //   regex_match("foo.txt, bar.txt, baz.dat", ".*([a-z]+).dat", result), result[0] will be "foo.txt, bar.txt, baz.dat", result[1] will be "baz"
  //
  // @param [in] input regex: matched string, should not be empty
  // @param [in] regex_pattern: regex pattern, should not be empty
  // @param [out] searched_group: result of search action, searched_group[0] is the entire regular expression's match string
  // @param [in] flag: falg to control regex engine bahaviour
  // @return the error code, typically one of below:
  //  OB_INVALID_ARGUMENT: input or regex_pattern is empty, or searched_group not empty
  //  OB_ALLOCATE_MEMORY_FAILED: no memory in this tenant
  //  OB_ERR_SYS: error happened when call POSIX regex API, see log to get more detail infoes
  static int regex_match(const ObString &input,
                         const ObString &regex_pattern,
                         ObIArray<ObStringHolder> &searched_group,
                         Flag flag = None);
  // bahave just like function above, but convert !searched_group.empty() to is_matched
  static int regex_match(const ObString &input,
                         const ObString &regex_pattern,
                         bool &is_matched,
                         Flag flag = None);
  // check if regex_pattern can matched input string or any sub-string of it.
  // sub-string matched means regex_pattern matches no need stared from input[0], no need ended at input[size].
  // for example:
  //   regex_search("foo.txt, bar.txt, baz.dat", "([a-z]+).txt", result), result[0] will be ""foo.txt", result[1] will be "foo",
  //   (NOTICE that bar.txt is ignored, if you wants regex_patter works more than one times, use regex_search_all())
  //   regex_search("foo.txt, bar.txt, baz.dat", ".*([a-z]+).dat", result), result[0] will be "foo.txt, bar.txt, baz.dat", result[1] will be "baz"
  //
  // all args has similar meanings as regex_match() API
  static int regex_search(const ObString &input,
                          const ObString &regex_pattern,
                          ObIArray<ObStringHolder> &searched_group,
                          Flag flag = None);
  static int regex_search(const ObString &input,
                          const ObString &regex_pattern,
                          bool &is_searched,
                          Flag flag = None);

  // no time to implement for now(maybe later if needed)
  // static int regex_search_all(const ObString &input,
  //                             const ObString &regex_pattern,
  //                             ObStringHolder &replace_result/*will consider of part match*/);
  // static int regex_replace(const ObString &input,
  //                          const ObString &regex_pattern,
  //                          ObStringHolder &replace_result/*will consider of part match*/);
  // static int regex_replace_all(const ObString &input,
  //                              const ObString &regex_pattern,
  //                              ObStringHolder &replace_result/*will consider of part match*/);
};

}
}
#endif