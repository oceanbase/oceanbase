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

#ifndef OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_
#define OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_

#include "storage/lob/ob_lob_util.h"

namespace oceanbase
{
namespace storage
{
class ObDMLRunningCtx;
class ObLobTabletDmlHelper
{
public:
  static int handle_valid_old_outrow_lob_value(
      const bool is_total_quantity_log,
      ObLobCommon* old_lob_common,
      ObLobCommon* new_lob_common);
  static int is_support_ext_info_log(ObDMLRunningCtx &run_ctx, bool &is_support);
  static int set_lob_data_outrow_ctx_op(ObLobCommon* lob_common, ObLobDataOutRowCtx::OpType ty);

private:
  static int is_disable_record_outrow_lob_in_clog_(bool &disable_record_outrow_lob_in_clog);
  static int is_below_4_2_5_version_(bool &is_below);
  static int copy_seq_no_(ObLobCommon* old_lob_common, ObLobCommon* new_lob_common);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_
