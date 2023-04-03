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

#include "storage/tx/ob_one_phase_committer.h"

namespace oceanbase
{
using namespace common;
namespace transaction
{
int ObTxOnePhaseCommitter::one_phase_commit(ObICommitCallback &cb)
{
  UNUSED(cb);
  return OB_SUCCESS;
}

int ObTxOnePhaseCommitter::apply_log()
{
  return OB_SUCCESS;
}

int ObTxOnePhaseCommitter::handle_timeout()
{
  return OB_SUCCESS;
}

int ObTxOnePhaseCommitter::handle_reboot()
{
  return OB_SUCCESS;
}

int ObTxOnePhaseCommitter::leader_takeover()
{
  return OB_SUCCESS;
}

int ObTxOnePhaseCommitter::leader_revoke()
{
  return OB_SUCCESS;
}

int ObTxOnePhaseCommitter::apply_commit_log()
{
  return OB_SUCCESS;
}

int ObTxOnePhaseCommitter::apply_abort_log()
{
  return OB_SUCCESS;
}

} // end
} // end namespace oceanbase

