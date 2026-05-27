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

#define USING_LOG_PREFIX OBLOG_SORTER

#include "ob_cdc_update_split_merge_cache.h"
#include "ob_cdc_update_split_merge_payload.h"
#include "ob_cdc_update_split_merge_storager.h"
#include "ob_log_resource_collector.h"
#include "ob_log_instance.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

namespace
{

static const int64_t MAX_UNMATCHED_DELETE_LOG_COUNT = 5;
static const int64_t UNMATCHED_TRACE_ID_BUF_LEN = 256;

int build_delete_old_cols_payload_(
    ObLogBR &del_br,
    ObIAllocator &allocator,
    MergeOldColsPayload &payload)
{
  int ret = OB_SUCCESS;
  IBinlogRecord *del_data = del_br.get_data();

  if (OB_ISNULL(del_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("delete br data is null", KR(ret), K(del_br));
  } else if (OB_FAIL(payload.init_from_delete(*del_data, allocator))) {
    LOG_ERROR("init merge old cols payload from delete failed", KR(ret), K(del_br));
  }

  return ret;
}

int serialize_merge_old_cols_payload_(
    const MergeOldColsPayload &payload,
    ObIAllocator &allocator,
    const char *&data,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  data = nullptr;
  data_len = 0;

  const int64_t payload_len = payload.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(payload_len));
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc merge old cols payload buffer failed", KR(ret), K(payload_len));
  } else if (OB_FAIL(payload.serialize(buf, payload_len, pos))) {
    LOG_ERROR("serialize merge old cols payload failed", KR(ret), K(payload_len), K(pos), K(payload));
  } else {
    data = buf;
    data_len = pos;
  }

  return ret;
}

int deserialize_merge_old_cols_payload_(
    const char *data,
    const int64_t data_len,
    MergeOldColsPayload &payload)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_ISNULL(data) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid merge old cols payload", KR(ret), KP(data), K(data_len));
  } else if (OB_FAIL(payload.deserialize(data, data_len, pos))) {
    LOG_ERROR("deserialize merge old cols payload failed", KR(ret), K(data_len), K(pos));
  }

  return ret;
}

void build_unmatched_trace_id_summary_(
    UpdateSplitMergeMap &merge_map,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t logged_count = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "["))) {
    // do nothing
  } else {
    for (UpdateSplitMergeMap::iterator iter = merge_map.begin();
        iter != merge_map.end() && logged_count < MAX_UNMATCHED_DELETE_LOG_COUNT;
        ++iter, ++logged_count) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld",
              (0 == logged_count ? "" : ","),
              iter->first))) {
        break;
      }
    }

    if ((OB_SUCCESS == ret || OB_BUF_NOT_ENOUGH == ret)
        && logged_count < merge_map.size()) {
      ret = databuff_printf(buf, buf_len, pos, ",...");
    }

    if (OB_SUCCESS == ret || OB_BUF_NOT_ENOUGH == ret) {
      ret = databuff_printf(buf, buf_len, pos, "]");
    }
  }

  if (OB_SUCCESS != ret && OB_BUF_NOT_ENOUGH != ret && pos < buf_len) {
    buf[pos] = '\0';
  }
}

void log_unmatched_delete_details_(
    UpdateSplitMergeMap &merge_map,
    const transaction::ObTransID &trans_id)
{
  int64_t logged_count = 0;

  for (UpdateSplitMergeMap::iterator iter = merge_map.begin();
      iter != merge_map.end() && logged_count < MAX_UNMATCHED_DELETE_LOG_COUNT;
      ++iter, ++logged_count) {
    const int64_t trace_id = iter->first;
    const MergeCacheEntry &entry = iter->second;
    DmlStmtTask *delete_stmt = entry.delete_stmt_;

    if (OB_NOT_NULL(delete_stmt)) {
      PartTransTask &part_trans = delete_stmt->get_host();
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "[MERGE] unmatched DELETE detail",
          K(trans_id),
          K(trace_id),
          "state", entry.state_,
          "commit_version", part_trans.get_trans_commit_version(),
          "table_id", delete_stmt->get_table_id(),
          "seq_no", delete_stmt->get_row_seq_no(),
          KP(delete_stmt));
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "[MERGE] unmatched DELETE detail",
          K(trans_id),
          K(trace_id),
          "state", entry.state_,
          KP(delete_stmt));
    }
  }
}

} // namespace

ObCDCUpdateSplitMergeCache::ObCDCUpdateSplitMergeCache()
  : is_inited_(false),
    trans_id_(),
    merge_map_(),
    storager_(nullptr),
    resource_collector_(nullptr),
    stat_(nullptr)
{
}

ObCDCUpdateSplitMergeCache::~ObCDCUpdateSplitMergeCache()
{
  destroy();
}

int ObCDCUpdateSplitMergeCache::init(
    const transaction::ObTransID &trans_id,
    ObCDCUpdateSplitMergeStorager &storager,
    IObLogResourceCollector &resource_collector,
    UpdateSplitMergeStatInfo &stat)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("merge cache already inited", KR(ret));
  } else if (OB_FAIL(merge_map_.create(64, "CDCMerge"))) {
    LOG_ERROR("merge_map create failed", KR(ret));
  } else {
    trans_id_ = trans_id;
    storager_ = &storager;
    resource_collector_ = &resource_collector;
    stat_ = &stat;
    is_inited_ = true;
  }

  return ret;
}

void ObCDCUpdateSplitMergeCache::destroy()
{
  if (is_inited_) {
    trans_id_.reset();
    merge_map_.destroy();
    storager_ = nullptr;
    resource_collector_ = nullptr;
    stat_ = nullptr;
    is_inited_ = false;
  }
}

int ObCDCUpdateSplitMergeCache::put(const int64_t trace_id, DmlStmtTask *del_stmt)
{
  int ret = OB_SUCCESS;
  MergeCacheEntry entry;

  if (should_offload_to_storager_(trace_id)) {
    ObLogBR *del_br = del_stmt->get_binlog_record();
    const char *data = nullptr;
    int64_t data_len = 0;
    ObIAllocator &allocator = del_stmt->get_host().get_allocator();
    MergeOldColsPayload payload;

    if (OB_ISNULL(del_br)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("delete br is null", KR(ret), K(trace_id), KPC(del_stmt));
    } else if (OB_FAIL(build_delete_old_cols_payload_(*del_br, allocator, payload))) {
      LOG_ERROR("build delete old cols payload failed", KR(ret), K(trace_id), KPC(del_stmt));
    } else if (OB_FAIL(serialize_merge_old_cols_payload_(payload, allocator, data, data_len))) {
      LOG_ERROR("serialize merge old cols payload failed", KR(ret), K(trace_id), KPC(del_stmt));
    } else {
      PartTransTask &host = del_stmt->get_host();
      UpdateSplitMergeKey key(
          host.get_tenant_id(),
          host.get_trans_commit_version(),
          host.get_trans_id(),
          trace_id);
      if (OB_FAIL(storager_->put(key, data, static_cast<int64_t>(data_len)))) {
        LOG_ERROR("storager put failed", KR(ret), K(key));
      } else if (OB_FAIL(resource_collector_->revert(EDELETE, del_br))) {
        LOG_ERROR("revert offloaded delete br failed", KR(ret), K(trace_id));
      } else {
        entry.state_ = MergeCacheEntry::OFFLOADED;
        entry.delete_stmt_ = nullptr;
        entry.offload_key_ = key;
        // T2 put_count / bytes / current are bumped inside storager_->put().
      }
    }
  } else {
    entry.state_ = MergeCacheEntry::IN_STMT;
    entry.delete_stmt_ = del_stmt;
    stat_->add_kept_in_stmt();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(merge_map_.set_refactored(trace_id, entry))) {
      if (OB_HASH_EXIST == ret) {
        PartTransTask &part_trans = del_stmt->get_host();
        LOG_ERROR("[MERGE] duplicate DELETE trace_id inserted into merge_map",
            KR(ret),
            K(trace_id),
            K_(trans_id),
            "commit_version", part_trans.get_trans_commit_version(),
            "table_id", del_stmt->get_table_id(),
            "seq_no", del_stmt->get_row_seq_no(),
            KP(del_stmt));
      } else {
        LOG_ERROR("merge_map set failed", KR(ret), K(trace_id));
      }
    }
  }

  return ret;
}

int ObCDCUpdateSplitMergeCache::get_and_merge(
    const int64_t trace_id,
    DmlStmtTask *ins_stmt,
    ObLogBR *&output_br)
{
  int ret = OB_SUCCESS;
  output_br = nullptr;
  MergeCacheEntry entry;

  if (OB_FAIL(merge_map_.get_refactored(trace_id, entry))) {
    if (OB_HASH_NOT_EXIST == ret) {
      PartTransTask &part_trans = ins_stmt->get_host();
      ret = OB_SUCCESS;
      output_br = ins_stmt->get_binlog_record();
      LOG_WARN("[MERGE] INSERT with trace_id has no matching DELETE, output as-is",
          K(trace_id),
          "trans_id", part_trans.get_trans_id(),
          "commit_version", part_trans.get_trans_commit_version(),
          "table_id", ins_stmt->get_table_id(),
          "seq_no", ins_stmt->get_row_seq_no(),
          KP(ins_stmt));
    } else {
      LOG_ERROR("merge_map get failed", KR(ret), K(trace_id));
    }
  } else if (MergeCacheEntry::IN_STMT == entry.state_) {
    if (OB_FAIL(merge_in_stmt_(entry, ins_stmt, output_br))) {
      LOG_ERROR("merge_in_stmt_ failed", KR(ret), K(trace_id));
    } else if (OB_FAIL(merge_map_.erase_refactored(trace_id))) {
      LOG_ERROR("merge_map erase failed", KR(ret), K(trace_id));
    } else {
      stat_->dec_kept_in_stmt();
    }
  } else {
    if (OB_FAIL(merge_offloaded_(entry, trace_id, ins_stmt, output_br))) {
      LOG_ERROR("merge_offloaded_ failed", KR(ret), K(trace_id));
    } else if (OB_FAIL(merge_map_.erase_refactored(trace_id))) {
      LOG_ERROR("merge_map erase failed", KR(ret), K(trace_id));
    } else {
      stat_->dec_storager_disk_current();
    }
  }

  return ret;
}

int ObCDCUpdateSplitMergeCache::merge_in_stmt_(
    MergeCacheEntry &entry,
    DmlStmtTask *ins_stmt,
    ObLogBR *&output_br)
{
  int ret = OB_SUCCESS;
  DmlStmtTask *del_stmt = entry.delete_stmt_;
  ObLogBR *ins_br = ins_stmt->get_binlog_record();
  IBinlogRecord *ins_data = ins_br->get_data();
  ObLogBR *del_br = del_stmt->get_binlog_record();
  IBinlogRecord *del_data = nullptr;
  unsigned int del_old_count = 0;

  if (OB_ISNULL(del_br)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("delete br is null for in-stmt merge", KR(ret),
        "trace_id", ins_stmt->get_update_split_trace_id(), KPC(del_stmt));
  } else if (OB_ISNULL(del_data = del_br->get_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("delete br data is null for in-stmt merge", KR(ret), KPC(del_stmt));
  } else {
    binlogBuf *del_old_cols = del_data->oldCols(del_old_count);
    for (unsigned int i = 0; OB_SUCC(ret) && i < del_old_count; ++i) {
      if (OB_FAIL(ins_data->putOld(del_old_cols[i].buf,
              static_cast<int>(del_old_cols[i].buf_used_size),
              del_old_cols[i].m_origin))) {
        LOG_ERROR("put old column from delete br failed for in-stmt merge",
            KR(ret), K(i), K(del_old_cols[i].buf_used_size), K(del_old_cols[i].m_origin), KPC(ins_stmt));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ins_br->set_merged_delete_stmt(static_cast<void*>(del_stmt));
    ins_data->setRecordType(EUPDATE);
    output_br = ins_br;
    stat_->inc_success();

    LOG_DEBUG("[MERGE] in-stmt merge succ",
        K(ins_stmt->get_update_split_trace_id()), K(del_old_count));
  }

  return ret;
}

int ObCDCUpdateSplitMergeCache::merge_offloaded_(
    const MergeCacheEntry &entry,
    const int64_t trace_id,
    DmlStmtTask *ins_stmt,
    ObLogBR *&output_br)
{
  int ret = OB_SUCCESS;
  ObLogBR *ins_br = ins_stmt->get_binlog_record();
  IBinlogRecord *ins_data = ins_br->get_data();
  PartTransTask &part_trans = ins_stmt->get_host();

  // Reuse the exact key stamped at put time — avoids any drift between
  // fields used at put vs. get (e.g. if INSERT's commit_version metadata ever
  // diverges from DELETE's due to upstream changes).
  const UpdateSplitMergeKey &key = entry.offload_key_;
  UNUSED(trace_id);
  const char *data = nullptr;
  int64_t data_len = 0;
  ObIAllocator &allocator = part_trans.get_allocator();

  if (OB_FAIL(storager_->get(allocator, key, data, data_len))) {
    LOG_ERROR("storager get failed", KR(ret), K(key));
  } else {
    MergeOldColsPayload payload;
    if (OB_FAIL(deserialize_merge_old_cols_payload_(data, data_len, payload))) {
      LOG_ERROR("deserialize merge old cols payload failed", KR(ret), K(key), K(data_len));
    } else if (OB_FAIL(payload.apply_to_insert(*ins_data))) {
      LOG_ERROR("apply merge old cols payload to insert failed", KR(ret), K(key), K(payload));
    } else {
      LOG_DEBUG("[MERGE] offloaded merge succ", K(key), K(data_len));
    }

    if (OB_SUCC(ret)) {
      int del_ret = storager_->del(key);
      if (OB_SUCCESS != del_ret) {
        LOG_WARN("storager del failed, ignoring", K(del_ret), K(key));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ins_data->setRecordType(EUPDATE);
    output_br = ins_br;
    stat_->inc_success();
  }

  return ret;
}

int ObCDCUpdateSplitMergeCache::handle_unmatched()
{
  int ret = OB_SUCCESS;
  const int64_t remaining = merge_map_.size();
  char unmatched_trace_ids[UNMATCHED_TRACE_ID_BUF_LEN] = "";

  if (remaining > 0) {
    stat_->inc_contract_violation(remaining);
    build_unmatched_trace_id_summary_(merge_map_, unmatched_trace_ids, sizeof(unmatched_trace_ids));
    log_unmatched_delete_details_(merge_map_, trans_id_);

    // Reclaim T1/T2 current gauges for the leftover entries, and for any
    // OFFLOADED entry also best-effort delete the underlying RocksDB row so
    // stale payloads don't linger beyond the trans that put them.
    int64_t t1_leftover = 0;
    int64_t t2_leftover = 0;
    for (UpdateSplitMergeMap::iterator iter = merge_map_.begin();
        iter != merge_map_.end();
        ++iter) {
      if (MergeCacheEntry::IN_STMT == iter->second.state_) {
        ++t1_leftover;
      } else {
        ++t2_leftover;
        const int del_ret = storager_->del(iter->second.offload_key_);
        if (OB_SUCCESS != del_ret) {
          LOG_WARN_RET(del_ret, "[MERGE] storager del for unmatched OFFLOADED failed, ignoring",
              "trace_id", iter->first, "key", iter->second.offload_key_);
        }
      }
    }
    for (int64_t i = 0; i < t1_leftover; ++i) {
      stat_->dec_kept_in_stmt();
    }
    for (int64_t i = 0; i < t2_leftover; ++i) {
      stat_->dec_storager_disk_current();
    }

    if (TCONF.update_split_merge_abort_on_data_loss) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("[MERGE] contract violation: unmatched DELETE stmts remain in merge_map, "
          "set update_split_merge_abort_on_data_loss=0 to continue with data loss",
          KR(ret), K(remaining), K_(trans_id), K(t1_leftover), K(t2_leftover),
          K(unmatched_trace_ids),
          "logged_unmatched_count",
          MIN(remaining, MAX_UNMATCHED_DELETE_LOG_COUNT));
    } else {
      stat_->inc_data_loss(remaining);
      LOG_WARN("[MERGE] contract violation: discarding unmatched DELETE stmts (data loss mode)",
          K(remaining), K_(trans_id), K(t1_leftover), K(t2_leftover),
          K(unmatched_trace_ids),
          "logged_unmatched_count",
          MIN(remaining, MAX_UNMATCHED_DELETE_LOG_COUNT));
    }
  }

  return ret;
}

bool ObCDCUpdateSplitMergeCache::should_offload_to_storager_(const int64_t trace_id) const
{
  // Naming note: the config key is still `update_split_merge_persist_mode` for
  // historical reasons — its semantics are "offload to storager", not "persist".
  const char *persist_mode = TCONF.update_split_merge_persist_mode.str();
  bool persist = false;

  if (OB_ISNULL(persist_mode) || 0 == strcmp("auto", persist_mode)) {
  const int64_t mem_hold = lib::get_memory_hold();
  const int64_t mem_limit = TCONF.memory_limit;
  const double mem_warn_threshold = mem_limit * TCONF.memory_usage_warn_threshold / 100.0;
    persist = static_cast<double>(mem_hold) > mem_warn_threshold;
  } else if (0 == strcmp("always", persist_mode)) {
    persist = true;
  } else if (0 == strcmp("hash50", persist_mode)) {
    // Fibonacci hashing: trace_id is generated by the SQL layer and may have
    // low-order regularity (per-partition / per-trans monotonic), so we can't
    // just read its LSB. Multiplying by floor(2^64 / golden_ratio) mixes the
    // input across the whole 64-bit word; the top bit of the product is then
    // a statistically balanced 1-bit hash — deterministic, dependency-free,
    // ~1 ns. Its only purpose is to deterministically pick ~50% of trace_ids
    // for the offload path in tests (update_split_merge_persist_mode=hash50),
    // so both T1 kept_in_stmt and T2 storager_disk paths get exercised.
    const uint64_t mixed = static_cast<uint64_t>(trace_id) * 0x9e3779b97f4a7c15ULL;
    persist = (0 == (mixed >> 63));
  } else {
    const int64_t mem_hold = lib::get_memory_hold();
    const int64_t mem_limit = TCONF.memory_limit;
    const double mem_warn_threshold = mem_limit * TCONF.memory_usage_warn_threshold / 100.0;
    persist = static_cast<double>(mem_hold) > mem_warn_threshold;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid update_split_merge_persist_mode, fallback to auto",
        K(persist_mode), K(trace_id), K(persist));
  }

  return persist;
}

} // namespace libobcdc
} // namespace oceanbase
