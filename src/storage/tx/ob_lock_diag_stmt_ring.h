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

#ifndef OCEANBASE_STORAGE_TX_OB_LOCK_DIAG_STMT_RING_H_
#define OCEANBASE_STORAGE_TX_OB_LOCK_DIAG_STMT_RING_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/config/ob_server_config.h"
#include "sql/resolver/ob_stmt_type.h"

namespace oceanbase
{
namespace transaction
{

inline constexpr int64_t LOCK_DIAG_RING_SIZE = 16;
inline constexpr int64_t LOCK_DIAG_QUERY_SQL_LIMIT = 200;

inline bool need_push_lock_diag_stmt(const sql::stmt::StmtType stmt_type, const bool is_select_for_update)
{
  switch (stmt_type) {
    case sql::stmt::T_INSERT:
    case sql::stmt::T_INSERT_ALL:
    case sql::stmt::T_REPLACE:
    case sql::stmt::T_UPDATE:
    case sql::stmt::T_DELETE:
    case sql::stmt::T_MERGE:
      return true;
    case sql::stmt::T_SELECT:
      return is_select_for_update;
    default:
      return false;
  }
}

inline int64_t truncate_lock_diag_query_sql_len(const char *sql, const int64_t len)
{
  return (OB_NOT_NULL(sql) && len > 0) ? MIN(len, LOCK_DIAG_QUERY_SQL_LIMIT) : 0;
}

struct ObLockDiagStmtSlot
{
  int64_t seq_start_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
};

struct ObLockDiagQuerySqlSlot
{
  char query_sql_[LOCK_DIAG_QUERY_SQL_LIMIT + 1];
};

class ObTxStmtRing
{
public:
  ObTxStmtRing()
    : seqlock_version_(0),
      head_(0),
      collect_query_sql_(false),
      snapshot_inited_(false),
      query_sql_ring_(nullptr)
  {
    MEMSET(ring_, 0, sizeof(ring_));
  }
  ~ObTxStmtRing()
  {
    if (OB_NOT_NULL(query_sql_ring_)) {
      ob_free(query_sql_ring_);
      query_sql_ring_ = nullptr;
    }
  }

  // Logical reset for tx reuse (switch_to_idle / ObTxDesc::reset).
  // query_sql_ring_ is NOT freed here: ASH may still hold ObTxDesc ref and read
  // the heap buffer after switch_to_idle; defer ob_free to ~ObTxStmtRing when
  // ObTxDesc ref count reaches zero and the object is destroyed.
  void reset()
  {
    ATOMIC_AAF(&seqlock_version_, 1);
    ATOMIC_STORE_RLX(&head_, 0);
    collect_query_sql_ = false;
    snapshot_inited_ = false;
    MEMSET(ring_, 0, sizeof(ring_));
    ATOMIC_AAF(&seqlock_version_, 1);
  }

  void push(const int64_t seq_start,
            const char *sql_id,
            const char *query_sql,
            const int64_t query_sql_len,
            const uint64_t tenant_id)
  {
    if (OB_UNLIKELY(!snapshot_inited_)) {
      snapshot_inited_ = true;
      collect_query_sql_ = GCONF._ob_enable_lock_diagnose_collect_query_sql;
      if (collect_query_sql_ && OB_ISNULL(query_sql_ring_)) {
        void *buf = ob_malloc(sizeof(ObLockDiagQuerySqlSlot) * LOCK_DIAG_RING_SIZE,
                              lib::ObMemAttr(tenant_id, "AshDiagQuerySql"));
        if (OB_NOT_NULL(buf)) {
          query_sql_ring_ = reinterpret_cast<ObLockDiagQuerySqlSlot *>(buf);
          MEMSET(query_sql_ring_, 0, sizeof(ObLockDiagQuerySqlSlot) * LOCK_DIAG_RING_SIZE);
        } else {
          collect_query_sql_ = false;
        }
      }
    }
    const uint64_t ver = ATOMIC_AAF(&seqlock_version_, 1);
    OB_ASSERT(ver & 1);
    const int64_t pos = head_ % LOCK_DIAG_RING_SIZE;
    ring_[pos].seq_start_ = seq_start;
    if (OB_NOT_NULL(sql_id)) {
      MEMCPY(ring_[pos].sql_id_, sql_id, common::OB_MAX_SQL_ID_LENGTH);
      ring_[pos].sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
    } else {
      ring_[pos].sql_id_[0] = '\0';
    }
    if (OB_NOT_NULL(query_sql_ring_)) {
      query_sql_ring_[pos].query_sql_[0] = '\0';
    }
    if (collect_query_sql_ && OB_NOT_NULL(query_sql_ring_) && OB_NOT_NULL(query_sql)) {
      const int64_t copy_len = truncate_lock_diag_query_sql_len(query_sql, query_sql_len);
      if (copy_len > 0) {
        MEMCPY(query_sql_ring_[pos].query_sql_, query_sql, copy_len);
      }
      query_sql_ring_[pos].query_sql_[copy_len] = '\0';
    }
    // read of head_ races with no other writer (single-threaded writer).
    ATOMIC_STORE_RLX(&head_, head_ + 1);
    ATOMIC_AAF(&seqlock_version_, 1);
  }

  int lookup_stmt_info(const int64_t holder_seq,
                       ObLockDiagStmtSlot &stmt_info,
                       ObLockDiagQuerySqlSlot &query_sql_info,
                       bool &has_query_sql) const
  {
    int ret = OB_ENTRY_NOT_EXIST;
    int best_pos = -1;
    int64_t best_seq = -1;
    has_query_sql = false;
    query_sql_info.query_sql_[0] = '\0';
    for (int64_t retry = 0; retry < 2; ++retry) {
      const uint64_t ver_before = ATOMIC_LOAD(&seqlock_version_);
      if (ver_before & 1) {
        ret = OB_EAGAIN;
        continue;
      }
      best_pos = -1;
      best_seq = -1;
      const int64_t cur_head = ATOMIC_LOAD_RLX(&head_);
      const int64_t count = MIN(cur_head, LOCK_DIAG_RING_SIZE);
      for (int64_t i = 0; i < count; ++i) {
        const int64_t idx = (cur_head - 1 - i + LOCK_DIAG_RING_SIZE * 2) % LOCK_DIAG_RING_SIZE;
        const int64_t seq_start = ring_[idx].seq_start_;
        if (seq_start > 0 && seq_start <= holder_seq && seq_start >= best_seq) {
          best_seq = seq_start;
          best_pos = static_cast<int>(idx);
        }
      }
      if (best_pos >= 0) {
        stmt_info = ring_[best_pos];
        if (collect_query_sql_ && OB_NOT_NULL(query_sql_ring_)) {
          query_sql_info = query_sql_ring_[best_pos];
          has_query_sql = (query_sql_info.query_sql_[0] != '\0');
        }
        ret = OB_SUCCESS;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
      const uint64_t ver_after = ATOMIC_LOAD(&seqlock_version_);
      if (ver_before == ver_after && (ver_after & 1) == 0) {
        // Defensive: seqlock_version_ already enforces seq_start <= holder_seq in this seqlock window.
        if (OB_SUCCESS == ret
            && (stmt_info.seq_start_ <= 0 || stmt_info.seq_start_ > holder_seq)) {
          ret = OB_ENTRY_NOT_EXIST;
          has_query_sql = false;
        }
        break;
      } else {
        ret = OB_EAGAIN;
        has_query_sql = false;
        query_sql_info.query_sql_[0] = '\0';
      }
    }
    return ret;
  }

private:
  uint64_t seqlock_version_;
  int64_t head_;
  bool collect_query_sql_;
  bool snapshot_inited_;
  ObLockDiagStmtSlot ring_[LOCK_DIAG_RING_SIZE];
  ObLockDiagQuerySqlSlot *query_sql_ring_;
  DISALLOW_COPY_AND_ASSIGN(ObTxStmtRing);
};

} // namespace transaction
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TX_OB_LOCK_DIAG_STMT_RING_H_
