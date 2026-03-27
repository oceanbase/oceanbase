/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

