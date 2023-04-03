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

#ifndef OCEABASE_STORAGE_LS_ROLE_HANDLER_
#define OCEABASE_STORAGE_LS_ROLE_HANDLER_

#include "lib/net/ob_addr.h" // ObAddr
namespace oceanbase
{
namespace storage
{
class ObLS;

class ObLSRoleHandler
{
public:
  ObLSRoleHandler() : is_inited_(false), ls_(nullptr) {}
  ~ObLSRoleHandler() {}
  int init(ObLS *ls);
public:
  // coordinate log stream leader change
  // @param [in] leader, new leader
  int change_leader(const common::ObAddr &leader);

  // for change leader callback.
  int leader_revoke();
  int leader_takeover();
  int leader_active();

private:
  bool is_inited_;
  ObLS *ls_;
};

} // storage
} // oceanbase
#endif
