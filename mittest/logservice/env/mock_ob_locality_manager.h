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

#include "storage/ob_locality_manager.h"

namespace oceanbase
{
using namespace storage;
namespace unittest
{
class MockObLocalityManager : public ObLocalityManager
{
public:
  MockObLocalityManager(): self_(), is_inited_(false) { }
  ~MockObLocalityManager() { destroy(); }
  int init(const common::ObAddr &self)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (self.is_valid() == false) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid argument", K(self));
    } else {
      self_ = self;
      is_inited_ = true;
    }
    return ret;
  }
  void destroy()
  {
    is_inited_ = false;
    self_.reset();
  }
  int is_same_zone(const common::ObAddr &server, bool &is_same_zone)
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else {
      is_same_zone = false;
    }
    return ret;
  }
private:
  common::ObAddr self_;
  bool is_inited_;
};
}// storage
}// oceanbase
