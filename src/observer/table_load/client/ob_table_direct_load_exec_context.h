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

#pragma once

#include "lib/allocator/ob_allocator.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace observer
{

class ObTableDirectLoadExecContext
{
public:
  explicit ObTableDirectLoadExecContext(common::ObIAllocator &allocator)
    : allocator_(allocator), tenant_id_(0), user_id_(0), database_id_(0)
  {
  }
  ~ObTableDirectLoadExecContext() = default;
  common::ObIAllocator &get_allocator() { return allocator_; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_user_id(uint64_t user_id) { user_id_ = user_id; }
  uint64_t get_user_id() const { return user_id_; }
  void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  uint64_t get_database_id() const { return database_id_; }
  void set_user_client_addr(const ObAddr &user_client_addr)
  {
    user_client_addr_ = user_client_addr;
  }
  const ObAddr get_user_client_addr() const { return user_client_addr_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDirectLoadExecContext);
private:
  common::ObIAllocator &allocator_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  ObAddr user_client_addr_;
};

} // namespace observer
} // namespace oceanbase
