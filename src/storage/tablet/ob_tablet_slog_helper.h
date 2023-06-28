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
  static int write_update_tablet_slog(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObMetaDiskAddr &disk_addr);
  static int write_empty_shell_tablet_slog(
      ObTablet *empty_shell_tablet,
      ObMetaDiskAddr &disk_addr);
  static int write_remove_tablet_slog(
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids);
  static int write_remove_tablet_slog(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_ids);
private:
  static int safe_batch_write_remove_tablet_slog(
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_SLOG_HELPER
