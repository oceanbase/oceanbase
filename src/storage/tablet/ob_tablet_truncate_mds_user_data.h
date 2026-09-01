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

#ifndef OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_USER_DATA_H
#define OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_USER_DATA_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

class ObTabletTruncateMdsUserData final
{
  OB_UNIS_VERSION(1);
public:
  ObTabletTruncateMdsUserData();
  ~ObTabletTruncateMdsUserData() = default;
  ObTabletTruncateMdsUserData(const ObTabletTruncateMdsUserData &) = delete;
  ObTabletTruncateMdsUserData &operator=(const ObTabletTruncateMdsUserData &) = delete;

  void reset();
  bool is_valid() const;
  bool is_default() const;
  void set_default_value();
  int assign(const ObTabletTruncateMdsUserData &other);
  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn);

  TO_STRING_KV(K_(truncate_commit_scn),
               K_(truncate_commit_version),
               K_(schema_version));

public:
  share::SCN truncate_commit_scn_;
  int64_t truncate_commit_version_;
  int64_t schema_version_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_USER_DATA_H
