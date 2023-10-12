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

#ifndef __OCENABASE_SHARE_RESTORE_URI_PARSER_H__
#define __OCENABASE_SHARE_RESTORE_URI_PARSER_H__

#include "share/restore/ob_physical_restore_info.h"
#include "share/ob_kv_parser.h"
#include "share/ob_list_parser.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_zone.h"

namespace oceanbase
{
namespace share
{

class ObPhysicalRestoreOptionParser
{
public:
  static int parse(const common::ObString &uri, ObPhysicalRestoreJob &job);
private:
  static int parse(const char *uri, ObPhysicalRestoreJob &job);
public:
  class ExtraArgsCb : public share::ObKVMatchCb
  {
  public:
    ExtraArgsCb(ObPhysicalRestoreJob &job);
    int match(const char *key, const char *value);
    bool check() const;
  private:
    /* functions */
    typedef int (*Setter)(ObPhysicalRestoreJob &job, const char *val);
    static int set_pool_list(ObPhysicalRestoreJob &job, const char *val);
    static int set_locality(ObPhysicalRestoreJob &job, const char *val);
    static int set_primary_zone(ObPhysicalRestoreJob &job, const char *val);
    static int set_kms_encrypt(ObPhysicalRestoreJob &job, const char *val);
    static int set_concurrency(ObPhysicalRestoreJob &job, const char *val);
  private:
    ObPhysicalRestoreJob &job_;
    struct Action {
      const char *key;
      Setter setter;
      bool required;
    };
    const static int ACTION_CNT = 5;
    static Action actions_[ACTION_CNT];
    bool is_set_[ACTION_CNT];
  };
private:

  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreOptionParser);
};


class ObPhysicalRestoreUriParser
{
public:
  ObPhysicalRestoreUriParser() = default;
  ~ObPhysicalRestoreUriParser() = default;
  static int parse(
    const common::ObString &multi_uri,
    common::ObArenaAllocator &allocator,
    common::ObIArray<common::ObString> &uri_list);

private:
  static int find_repeat_(const common::ObIArray<common::ObString> &uri_list, 
      const common::ObString &uri, bool &is_repeat);

  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreUriParser);
};
}
}
#endif /* __OCENABASE_SHARE_RESTORE_URI_PARSER_H__ */
//// end of header file


