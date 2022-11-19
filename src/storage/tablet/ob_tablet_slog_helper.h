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

#ifndef OCEANBASE_STORAGE_OB_TABLET_SLOG_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_SLOG_HELPER

#include "lib/container/ob_array.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace storage
{
class ObTabletSlogHelper
{
public:
  static int write_create_tablet_slog(
      const ObTabletHandle &tablet_handle,
      ObMetaDiskAddr &disk_addr);
  static int write_create_tablet_slog(
      const common::ObIArray<ObTabletHandle> &tablet_handle_array,
      common::ObIArray<ObMetaDiskAddr> &disk_addr_array);
  static int write_remove_tablet_slog(
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids);
private:
  static int safe_batch_write_remove_tablet_slog(
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_SLOG_HELPER
