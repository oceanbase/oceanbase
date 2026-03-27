/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
