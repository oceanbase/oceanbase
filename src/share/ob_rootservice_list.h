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

#ifndef OCEANBASE_SHARE_OB_SERVER_LIST_H_
#define OCEANBASE_SHARE_OB_SERVER_LIST_H_

#include "lib/net/ob_addr.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace share
{

typedef common::ObIArray<common::ObAddr> ObIServerList;

class ObRootServiceList
{
  OB_UNIS_VERSION(1);
public:
  ObRootServiceList() : rootservice_list_() {}
  ~ObRootServiceList() {}
  int assign(const ObRootServiceList &other);
  int assign(const common::ObIArray<common::ObAddr> &other);
  bool is_valid() const;
  int parse_from_string(ObString &server_list_str);
  int rootservice_list_to_str(ObSqlString &server_list_str) const;
  void reset()
  {
    rootservice_list_.reset();
  }

  // Getter
  ObIServerList &get_rs_list_arr() { return rootservice_list_; }
  const ObIServerList &get_rs_list_arr() const { return rootservice_list_; }

  TO_STRING_KV(K_(rootservice_list));

private:
  common::ObSArray<common::ObAddr> rootservice_list_;
};



} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SERVER_LIST_H_
