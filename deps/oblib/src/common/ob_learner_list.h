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

#ifndef OCEANBASE_COMMON_OB_LEARNER_LIST_H_
#define OCEANBASE_COMMON_OB_LEARNER_LIST_H_

#include "lib/container/ob_se_array.h"        // SEArray
#include "lib/container/ob_se_array_iterator.h"        // SEArrayIterator
#include "lib/string/ob_sql_string.h"         // ObSqlString
#include "lib/utility/ob_unify_serialize.h"   // serialize
#include "common/ob_member.h"
#include <algorithm>

namespace oceanbase
{
namespace common
{

template <int64_t MAX_SIZE, typename T = common::ObMember>
class BaseLearnerList
{
  OB_UNIS_VERSION(1);
public:
  BaseLearnerList(): learner_array_() { }
  ~BaseLearnerList() { reset(); }

public:
  bool is_valid() const;
  bool is_full() const;
  void reset();
  int get_learner(const int64_t idx, T &learner) const;
  // dangerous
  T &get_learner(const int64_t idx);
  int get_server_by_index(const int64_t idx, common::ObAddr &server) const;
  int get_member_by_index(const int64_t idx, common::ObMember &member) const;
  int add_server(const common::ObAddr &server);
  int add_learner(const T &learner);
  int remove_learner(const T &learner);
  int remove_learner(const common::ObAddr &server);
  bool contains(const T &learner) const;
  bool contains(const common::ObAddr &server) const;
  int get_learner_by_addr(const common::ObAddr server, T &learner) const;
  int64_t get_member_number() const;
  bool learner_addr_equal(const BaseLearnerList<MAX_SIZE, T> &learner_list) const;
  BaseLearnerList &operator=(const BaseLearnerList<MAX_SIZE, T> &learner_list);
  int append(const BaseLearnerList<MAX_SIZE, T> &learner_list);
  int deep_copy(const BaseLearnerList<MAX_SIZE, T> &learner_list);
  template <int64_t ARG_MAX_SIZE>
  int deep_copy_to(BaseLearnerList<ARG_MAX_SIZE, common::ObMember> &learner_list) const;
  int transform_to_string(common::ObSqlString &output_string) const;
  TO_STRING_KV("learner_num", learner_array_.count(), K_(learner_array));
  // by operator ==
  int64_t get_index_by_learner(const T &learner) const;
  // by addr
  int64_t get_index_by_addr(const common::ObAddr &server) const;
private:
  typedef common::ObSEArray<T, OB_MAX_MEMBER_NUMBER> LogLearnerArray;
  LogLearnerArray learner_array_;
};

typedef BaseLearnerList<common::OB_MAX_GLOBAL_LEARNER_NUMBER, common::ObMember> GlobalLearnerList;
typedef BaseLearnerList<OB_MAX_MEMBER_NUMBER + OB_MAX_GLOBAL_LEARNER_NUMBER, common::ObMember> ResendConfigLogList;

} // namespace common end
} // namespace oceanbase end
#include "ob_learner_list.ipp"
#endif
