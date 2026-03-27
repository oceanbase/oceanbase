/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_memtable_interface.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

int ObIMemtable::get_ls_id(ObLSID &ls_id) {
  int ret = OB_SUCCESS;
  ls_id = get_ls_id();
  return ret;
}
ObLSID ObIMemtable::get_ls_id() const { return ls_id_; }

} // namespace storage
} // namespace oceanbase
