/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TRANSACTION_OB_TX_HOTSPOT_HELPER_
#define OCEANBASE_TRANSACTION_OB_TX_HOTSPOT_HELPER_

#include "storage/tx/ob_tx_hotspot_define.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"

namespace oceanbase
{

namespace transaction
{

// ==================================================================
// Helper functions for TxRedoFlushStatus
// All functions are static inline (header-only)
//
// State alignment (logical, not numeric):
//   PRIMARY_PREPARING (110)    <-> SECONDARY_PREPARING (510)
//   PRIMARY_COLLECTING (120)   <-> SECONDARY_MIGRATING (520)
//   PRIMARY_AGGR_SUCCEEDED (150) <-> SECONDARY_MIGRATE_SYNCED (530)
//   PRIMARY_AGGR_FAILED (160)  <-> SECONDARY_MIGRATE_FAILED (540)
//   (no primary equivalent)   <-> SECONDARY_MIGRATE_SUCCEEDED (550) - scheduler response sent
//
// Key rule: Once secondary enters aggregation (>= SECONDARY_PREPARING),
// it must NEVER write any log (commit, abort, etc.). All cleanup is no-log.
// ==================================================================

/**
 * @brief Checks if a status value is a valid defined enum member.
 */
static inline bool is_valid_status(TxRedoFlushStatus s)
{
  bool is_valid = false;
  if (s == TxRedoFlushStatus::NORMAL_START           // 10
      || s == TxRedoFlushStatus::NORMAL_FLUSHED      // 80
      || s == TxRedoFlushStatus::PRIMARY_PREPARING   // 110
      || s == TxRedoFlushStatus::PRIMARY_COLLECTING  // 120
      || s == TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED // 150
      || s == TxRedoFlushStatus::PRIMARY_AGGR_FAILED    // 160
      || s == TxRedoFlushStatus::PRIMARY_COMPLETED      // 180
      || s == TxRedoFlushStatus::SECONDARY_PREPARING    // 510
      || s == TxRedoFlushStatus::SECONDARY_MIGRATING    // 520
      || s == TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED // 530
      || s == TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED // 540
      || s == TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED) { // 550
    is_valid = true;
  }
  return is_valid;
}

/**
 * @brief Checks if status belongs to Primary role group.
 */
static inline bool is_primary_status(TxRedoFlushStatus s)
{
  bool is_primary = false;
  if (s == TxRedoFlushStatus::PRIMARY_PREPARING      // 110
      || s == TxRedoFlushStatus::PRIMARY_COLLECTING   // 120
      || s == TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED // 150
      || s == TxRedoFlushStatus::PRIMARY_AGGR_FAILED    // 160
      || s == TxRedoFlushStatus::PRIMARY_COMPLETED) {   // 180
    is_primary = true;
  }
  return is_primary;
}

/**
 * @brief Checks if status belongs to Secondary role group.
 */
static inline bool is_secondary_status(TxRedoFlushStatus s)
{
  bool is_secondary = false;
  if (s == TxRedoFlushStatus::SECONDARY_PREPARING      // 510
      || s == TxRedoFlushStatus::SECONDARY_MIGRATING   // 520
      || s == TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED // 530
      || s == TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED // 540
      || s == TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED) { // 550
    is_secondary = true;
  }
  return is_secondary;
}

/**
 * @brief Checks if status belongs to Normal role group.
 */
static inline bool is_normal_status(TxRedoFlushStatus s)
{
  bool is_normal = false;
  if (s == TxRedoFlushStatus::NORMAL_START
      || s == TxRedoFlushStatus::NORMAL_FLUSHED) {
    is_normal = true;
  }
  return is_normal;
}

/**
 * @brief Checks if status represents a completed lifecycle (all transitions exhausted).
 * Note: SECONDARY_MIGRATE_SYNCED is NOT terminal - it's an intermediate state waiting
 * for scheduler response. SECONDARY_MIGRATE_SUCCEEDED is the terminal state after
 * successful response.
 */
static inline bool has_completed_lifecycle(TxRedoFlushStatus s)
{
  return s == TxRedoFlushStatus::NORMAL_FLUSHED
      || s == TxRedoFlushStatus::PRIMARY_AGGR_FAILED
      || s == TxRedoFlushStatus::PRIMARY_COMPLETED
      || s == TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED
      || s == TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED;
}

/**
 * @brief Checks if status allows entering aggregation flow.
 * Only NORMAL_START is eligible — any other state means aggregation
 * is already in progress, completed, or the tx has a different role.
 */
static inline bool is_eligible_for_aggregation_entry(TxRedoFlushStatus s)
{
  return s == TxRedoFlushStatus::NORMAL_START;
}

/**
 * @brief Checks if primary transaction is in an active aggregation intermediate state.
 * Returns true ONLY for PRIMARY_PREPARING and PRIMARY_COLLECTING.
 */
static inline bool is_primary_tx_aggregating(TxRedoFlushStatus s)
{
  bool is_aggregating = false;
  if (s == TxRedoFlushStatus::PRIMARY_PREPARING    // 110
      || s == TxRedoFlushStatus::PRIMARY_COLLECTING) { // 120
    is_aggregating = true;
  }
  return is_aggregating;
}

/**
 * @brief Validates state transition from one status to another.
 */
static inline int is_valid_transition(TxRedoFlushStatus from, TxRedoFlushStatus to)
{
  int ret = OB_SUCCESS;

  // Defensive check: both values must be valid enum members
  if (!is_valid_status(from) || !is_valid_status(to)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (from == to) {
    // Idempotent transition is always allowed
    ret = OB_SUCCESS;
  } else {
    // -------------------------------------------------------
    // Normal transaction transitions
    // -------------------------------------------------------
    if (from == TxRedoFlushStatus::NORMAL_START) {
      if (to == TxRedoFlushStatus::NORMAL_FLUSHED
          || to == TxRedoFlushStatus::PRIMARY_PREPARING
          || to == TxRedoFlushStatus::SECONDARY_PREPARING) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    } else if (from == TxRedoFlushStatus::NORMAL_FLUSHED) {
      ret = OB_STATE_NOT_MATCH;
    }
    // -------------------------------------------------------
    // Primary transactions
    // -------------------------------------------------------
    else if (from == TxRedoFlushStatus::PRIMARY_PREPARING) {
      if (to == TxRedoFlushStatus::PRIMARY_COLLECTING
          || to == TxRedoFlushStatus::PRIMARY_AGGR_FAILED) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    } else if (from == TxRedoFlushStatus::PRIMARY_COLLECTING) {
      if (to == TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED
          || to == TxRedoFlushStatus::PRIMARY_AGGR_FAILED) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    } else if (from == TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED) {
      if (to == TxRedoFlushStatus::PRIMARY_COMPLETED) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    } else if (from == TxRedoFlushStatus::PRIMARY_AGGR_FAILED
               || from == TxRedoFlushStatus::PRIMARY_COMPLETED) {
      ret = OB_STATE_NOT_MATCH;
    }
    // -------------------------------------------------------
    // Secondary transactions
    // -------------------------------------------------------
    else if (from == TxRedoFlushStatus::SECONDARY_PREPARING) {
      if (to == TxRedoFlushStatus::SECONDARY_MIGRATING
          || to == TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    } else if (from == TxRedoFlushStatus::SECONDARY_MIGRATING) {
      if (to == TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED
          || to == TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    } else if (from == TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED) {
      // SYNCED is intermediate state - can transition to SUCCEEDED or FAILED
      if (to == TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED
          || to == TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    } else if (from == TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED
               || from == TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED) {
      // Terminal states: no outgoing transitions
      ret = OB_STATE_NOT_MATCH;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

} // namespace transaction

} // namespace oceanbase

#endif