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

#ifndef OCEANBASE_STORAGE_OB_TABLET_BINDING_MDS_USER_DATA
#define OCEANBASE_STORAGE_OB_TABLET_BINDING_MDS_USER_DATA

#include <stdint.h>
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
class ObTabletBindingInfo;

class ObTabletBindingMdsUserData
{
public:
  OB_UNIS_VERSION(1);

public:
  ObTabletBindingMdsUserData();
  ~ObTabletBindingMdsUserData() = default;
  ObTabletBindingMdsUserData(const ObTabletBindingMdsUserData &) = delete;
  ObTabletBindingMdsUserData &operator=(const ObTabletBindingMdsUserData &) = delete;

public:
  bool is_valid() const;
  int set_allocator(common::ObIAllocator &allocator);
  int assign(const ObTabletBindingMdsUserData &other);
  int assign_from_tablet_meta(const ObTabletBindingInfo &other);
  int dump_to_tablet_meta(ObTabletBindingInfo &other);
  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn);

  void reset();
  void set_default_value();

  TO_STRING_KV(K_(redefined),
                K_(snapshot_version),
                K_(schema_version),
                K_(data_tablet_id),
                K_(hidden_tablet_id),
                K_(lob_meta_tablet_id),
                K_(lob_piece_tablet_id),
                K_(is_old_mds));

public:
  int64_t snapshot_version_; // if redefined it is max readable snapshot, else it is min readable snapshot.
  int64_t schema_version_;
  common::ObTabletID data_tablet_id_;
  common::ObTabletID hidden_tablet_id_;
  common::ObTabletID lob_meta_tablet_id_;
  common::ObTabletID lob_piece_tablet_id_;
  bool redefined_;
  bool is_old_mds_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_BINDING_MDS_USER_DATA
