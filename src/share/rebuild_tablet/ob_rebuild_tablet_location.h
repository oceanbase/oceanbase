/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCENABASE_SHARE_OB_REBUILD_TABLET_LOCATION_TYPE_H
#define OCENABASE_SHARE_OB_REBUILD_TABLET_LOCATION_TYPE_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_sql_string.h" // ObSqlString
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_ls_id.h" // ObLSID
#include "share/ob_define.h"
#include "common/ob_tablet_id.h" // ObTabletID
#include "share/scn.h" // SCN

namespace oceanbase
{
namespace share
{

struct ObRebuildTabletLocationType final
{
  enum TYPE
  {
    NONE = 0,
    SERVER_ADDR = 1,
    REGION = 2,
    SERVER_ID = 3,
    MAX
  };

  static const char *get_str(const TYPE &type);
  static TYPE get_type(const char *type_str);
  static OB_INLINE bool is_valid(const TYPE &type) { return type >= 0 && type < MAX; }
};

struct ObRebuildTabletLocation final
{
  OB_UNIS_VERSION(1);
public:
  ObRebuildTabletLocation();
  ~ObRebuildTabletLocation() = default;
  void reset();
  bool is_valid() const;
  int get_location_addr(common::ObAddr &addr) const;
  int resolve_location(const ObString &src);
  int64_t to_string(char *buffer, const int64_t size) const;
private:
  int get_type_from_src_(const ObString &src);

private:
  ObRebuildTabletLocationType::TYPE type_;
  char location_[MAX_REGION_LENGTH];
};

} // end namespace share
} // end namespace oceanbase
#endif
