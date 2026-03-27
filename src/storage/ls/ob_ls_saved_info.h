/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_LS_SAVED_INFO_
#define OCEABASE_STORAGE_LS_SAVED_INFO_

#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

struct ObLSSavedInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObLSSavedInfo();
  ~ObLSSavedInfo() = default;
  bool is_valid() const;
  void reset();
  bool is_empty() const;

  TO_STRING_KV(K_(clog_checkpoint_scn), K_(clog_base_lsn), K_(replayable_point), K_(tablet_change_checkpoint_scn));

  share::SCN clog_checkpoint_scn_;
  palf::LSN clog_base_lsn_;
  int64_t replayable_point_;
  share::SCN tablet_change_checkpoint_scn_;
};


}
}

#endif
