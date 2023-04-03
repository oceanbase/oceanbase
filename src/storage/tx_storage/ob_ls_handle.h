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

#ifndef OCEANBASE_STORAGE_TX_STORAGE_OB_LS_HANDLE
#define OCEANBASE_STORAGE_TX_STORAGE_OB_LS_HANDLE

#include "lib/utility/ob_print_utils.h"
#include "share/leak_checker/obj_leak_checker.h"
#include "storage/ls/ob_ls_get_mod.h"

namespace oceanbase
{
namespace storage
{
class ObLSMap;
class ObLS;

class ObLSHandle final
{
public:
  ObLSHandle();
  ObLSHandle(const ObLSHandle &other);
  ObLSHandle &operator=(const ObLSHandle &other);
  ~ObLSHandle();
  int set_ls(const ObLSMap &ls_map, ObLS &ls, const ObLSGetMod &mod);
  void reset();
  bool is_valid() const;
  ObLS *get_ls() { return ls_; }
  ObLS *get_ls() const { return ls_; }
  TO_STRING_KV(KP(ls_map_), KP(ls_), K(mod_));
private:
  const ObLSMap *ls_map_;
  ObLS *ls_;
  ObLSGetMod mod_;
  DEFINE_OBJ_LEAK_DEBUG_NODE(node_);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TX_STORAGE_OB_LS_HANDLE
