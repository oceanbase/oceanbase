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

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_service_clog_replay_executor.h"
#include "logservice/ob_log_base_header.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::logservice;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
ObTabletServiceClogReplayExecutor::ObTabletServiceClogReplayExecutor()
  : ObTabletReplayExecutor(), buf_(nullptr), buf_size_(0), pos_(0), scn_()
{
}

int ObTabletServiceClogReplayExecutor::init(
    const char *buf,
    const int64_t buf_size,
    const int64_t pos,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
          || OB_UNLIKELY(buf_size <= 0)
          || OB_UNLIKELY(pos < 0)
          || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",  KP(buf), K(buf_size), K(pos), K(scn), K(ret));
  } else {
    buf_ = buf;
    buf_size_ = buf_size;
    pos_ = pos;
    scn_ = scn;
    is_inited_ = true;
  }

  return ret;
}

int ObTabletServiceClogReplayExecutor::do_replay_(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(handle.get_obj()->replay_update_storage_schema(scn_, buf_, buf_size_, pos_))) {
    LOG_WARN("update tablet storage schema fail", K(ret), K(handle), K_(scn), KP_(buf), K_(buf_size), K_(pos));
  }

  return ret;
}

}
}
