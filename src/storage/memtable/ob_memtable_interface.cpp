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
