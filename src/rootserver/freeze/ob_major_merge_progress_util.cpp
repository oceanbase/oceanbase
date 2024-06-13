//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX RS_COMPACTION
#include "rootserver/freeze/ob_major_merge_progress_util.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "observer/ob_server_struct.h"
#include "share/transfer/ob_transfer_task_operator.h"
#include "share/compaction/ob_schedule_batch_size_mgr.h"

namespace oceanbase
{
using namespace share;
using namespace common;
namespace compaction
{

ObTableCompactionInfo &ObTableCompactionInfo::operator=(const ObTableCompactionInfo &other)
{
  table_id_ = other.table_id_;
  tablet_cnt_ = other.tablet_cnt_;
  status_ = other.status_;
  unfinish_index_cnt_ = other.unfinish_index_cnt_;
  need_check_fts_ = other.need_check_fts_;
  return *this;
}

const char *ObTableCompactionInfo::TableStatusStr[] = {
  "INITIAL",
  "COMPACTED",
  "CAN_SKIP_VERIFYING",
  "INDEX_CKM_VERIFIED",
  "VERIFIED"
};

const char *ObTableCompactionInfo::status_to_str(const Status &status)
{
  STATIC_ASSERT(static_cast<int64_t>(TB_STATUS_MAX) == ARRAYSIZEOF(TableStatusStr), "table status str len is mismatch");
  const char *str = "";
  if (status < INITIAL || status >= TB_STATUS_MAX) {
    str = "invalid_status";
  } else {
    str = TableStatusStr[status];
  }
  return str;
}

ObTableCompactionInfo::ObTableCompactionInfo()
  : table_id_(OB_INVALID_ID),
    tablet_cnt_(0),
    unfinish_index_cnt_(INVALID_INDEX_CNT),
    status_(Status::INITIAL),
    need_check_fts_(false)
{
}
/**
 * -------------------------------------------------------------------ObMergeProgress-------------------------------------------------------------------
 */
int64_t ObMergeProgress::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    if (merge_finish_) {
      J_KV(K_(merge_finish), K_(total_table_cnt));
    } else {
      J_KV(KP(this), K_(merge_finish), K_(unmerged_tablet_cnt), K_(merged_tablet_cnt), K_(total_table_cnt));
      for (int64_t i = 0; i < RECORD_TABLE_TYPE_CNT; ++i) {
        J_COMMA();
        J_KV(ObTableCompactionInfo::TableStatusStr[i], table_cnt_[i]);
      }
    }
    J_OBJ_END();
  }
  return pos;
}

/**
 * -------------------------------------------------------------------ObTabletLSPairCache-------------------------------------------------------------------
 */
const int64_t ObTabletLSPairCache::TABLET_LS_MAP_BUCKET_CNT;
const int64_t ObTabletLSPairCache::RANGE_SIZE;
ObTabletLSPairCache::ObTabletLSPairCache()
  : tenant_id_(0),
    last_refresh_ts_(0),
    max_task_id_()
{
}

ObTabletLSPairCache::~ObTabletLSPairCache()
{
  destroy();
}

void ObTabletLSPairCache::reuse()
{
  last_refresh_ts_ = 0;
  max_task_id_.reset();
  map_.reuse();
}

void ObTabletLSPairCache::destroy()
{
  tenant_id_ = 0;
  last_refresh_ts_ = 0;
  max_task_id_.reset();
  if (map_.created()) {
    map_.destroy();
  }
}

int ObTabletLSPairCache::refresh()
{
  int ret = OB_SUCCESS;
  ObTabletID start_tablet_id;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is unexpected null", KR(ret));
  } else {
    map_.reuse();
  }
  SMART_VAR(ObArray<ObTabletLSPair>, tablet_ls_pair_array) {
    tablet_ls_pair_array.set_attr(ObMemAttr(tenant_id_, "RSCompPairCache"));
    if (OB_FAIL(tablet_ls_pair_array.reserve(RANGE_SIZE))) {
      LOG_WARN("failed to reserve array", KR(ret));
    }
    while (OB_SUCC(ret)) {
      tablet_ls_pair_array.reuse();
      if (OB_FAIL(ObTabletToLSTableOperator::range_get_tablet(
                                                *GCTX.sql_proxy_,
                                                tenant_id_,
                                                start_tablet_id,
                                                RANGE_SIZE,
                                                tablet_ls_pair_array))) {
        LOG_WARN("fail to get a range of tablet through tablet_to_ls_table_operator",
                KR(ret), K_(tenant_id), K(start_tablet_id), K(RANGE_SIZE),
                K(tablet_ls_pair_array));
      } else if (tablet_ls_pair_array.empty()) {
        break;
      } else {
        for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_ls_pair_array.count(); ++idx) {
          ObTabletLSPair &pair = tablet_ls_pair_array.at(idx);
          if (OB_FAIL(map_.set_refactored(pair.get_tablet_id(), pair.get_ls_id()))) {
            LOG_WARN("fail set tablet ls map", KR(ret));
          }
        } // end of for
        if (OB_SUCC(ret)) {
          start_tablet_id = tablet_ls_pair_array.at(tablet_ls_pair_array.count() - 1).get_tablet_id();
        }
      }
    } // end of while
  }
  if (OB_SUCC(ret)) {
    last_refresh_ts_ = ObTimeUtility::fast_current_time();
    cost_ts = last_refresh_ts_ - cost_ts;
    LOG_INFO("success to refresh tablet ls pair cache", KR(ret), K(cost_ts), "map_item_cnt", map_.size(), K(map_.bucket_count()));
  }
  return ret;
}

int ObTabletLSPairCache::rebuild_map_by_tablet_cnt()
{
  int ret = OB_SUCCESS;
  int64_t tablet_cnt = 0;
  if (map_.empty()) {
    if (OB_FAIL(ObTabletToLSTableOperator::get_tablet_ls_pairs_cnt(*GCTX.sql_proxy_, tenant_id_, tablet_cnt))) {
      LOG_WARN("failed to get tablet_ls pair cnt", KR(ret));
    }
  } else {
    tablet_cnt = map_.size();
  }
  if (OB_FAIL(ret) || tablet_cnt == 0) {
  } else {
    int64_t recommend_map_bucked_cnt = 0;
    const int64_t cur_bucket_cnt = map_.created() ? map_.bucket_count() : 0;
    bool rebuild_map_flag = ObScheduleBatchSizeMgr::need_rebuild_map(
      TABLET_LS_MAP_BUCKET_CNT, tablet_cnt, cur_bucket_cnt, recommend_map_bucked_cnt);
    if (rebuild_map_flag) {
      if (map_.created()) {
        map_.destroy();
      }
      if (OB_FAIL(map_.create(recommend_map_bucked_cnt, "RSCompPairCache", "RSCompPairCache", tenant_id_))) {
        LOG_WARN("fail to create tablet ls pair map", KR(ret), K_(tenant_id), K(recommend_map_bucked_cnt));
      } else {
        LOG_INFO("success to rebuild or create map", KR(ret), K(tablet_cnt), K(map_.bucket_count()));
      }
    }
  }
  return ret;
}

int ObTabletLSPairCache::check_exist_new_transfer_task(
  bool &exist,
  share::ObTransferTaskID &tmp_max_task_id)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (OB_FAIL(ObTransferTaskOperator::get_max_task_id_from_history(
      *GCTX.sql_proxy_,
      tenant_id_,
      tmp_max_task_id))) {
    LOG_WARN("get max transfer task id from history failed", KR(ret), K_(tenant_id),
      K(tmp_max_task_id));
  } else if (tmp_max_task_id.is_valid() && tmp_max_task_id > max_task_id_) {
    exist = true;
  }
  return ret;
}

int ObTabletLSPairCache::try_refresh(const bool force_refresh/* = false*/)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  share::ObTransferTaskID tmp_max_task_id;
  if (OB_FAIL(check_exist_new_transfer_task(exist, tmp_max_task_id))) {
    LOG_WARN("failed to check transfer task", KR(ret));
  } else if (force_refresh && OB_FAIL(rebuild_map_by_tablet_cnt())) {
    LOG_WARN("failed to rebuild map by tablet cnt", KR(ret), K(force_refresh));
  } else if (force_refresh
      || (exist && (ObTimeUtility::fast_current_time() - last_refresh_ts_ >= REFRESH_CACHE_TIME_INTERVAL))) {
    if (OB_FAIL(refresh())) {
      LOG_WARN("failed to refresh", KR(ret));
    } else {
      max_task_id_ = tmp_max_task_id;
    }
  }
  return ret;
}

int ObTabletLSPairCache::get_tablet_ls_pairs(
    const uint64_t table_id,
    const ObIArray<ObTabletID> &tablet_ids,
    ObIArray<share::ObTabletLSPair> &pairs) const
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id_) || is_sys_table(table_id)) {
    ObLSID tmp_ls_id(ObLSID::SYS_LS_ID);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_ids.count(); ++idx) {
      if (OB_FAIL(pairs.push_back(ObTabletLSPair(tablet_ids.at(idx), tmp_ls_id)))) {
        LOG_WARN("fail to push back pair", KR(ret), K(table_id));
      }
    } // end of for
  } else {
    ObLSID tmp_ls_id;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_ids.count(); ++idx) {
      if (OB_FAIL(map_.get_refactored(tablet_ids.at(idx), tmp_ls_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_ITEM_NOT_MATCH;
        } else {
          LOG_WARN("failed to get ls id", KR(ret), K(idx), K(tablet_ids));
        }
      } else if (OB_FAIL(pairs.push_back(ObTabletLSPair(tablet_ids.at(idx), tmp_ls_id)))) {
        LOG_WARN("fail to push back pair", KR(ret), K(table_id));
      }
    } // end of for
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObUncompactInfo-------------------------------------------------------------------
 */
ObUncompactInfo::ObUncompactInfo()
  : diagnose_rw_lock_(ObLatchIds::MAJOR_FREEZE_DIAGNOSE_LOCK),
    tablets_(),
    table_ids_()
{}

ObUncompactInfo::~ObUncompactInfo()
{
  reset();
}

void ObUncompactInfo::reset()
{
  SpinWLockGuard w_guard(diagnose_rw_lock_);
  tablets_.reuse();
  table_ids_.reuse();
}

void ObUncompactInfo::add_table(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard w_guard(diagnose_rw_lock_);
  if (table_ids_.count() < DEBUG_INFO_CNT
      && OB_FAIL(table_ids_.push_back(table_id))) {
    LOG_WARN("fail to push_back", KR(ret), K(table_id));
  }
}

void ObUncompactInfo::add_tablet(const share::ObTabletReplica &replica)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard w_guard(diagnose_rw_lock_);
  if (tablets_.count() < DEBUG_INFO_CNT
      && OB_FAIL(tablets_.push_back(replica))) {
    LOG_WARN("fail to push_back", KR(ret), K(replica));
  }
}

void ObUncompactInfo::add_tablet(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletReplica fake_replica;
  fake_replica.fake_for_diagnose(tenant_id, ls_id, tablet_id);
  SpinWLockGuard w_guard(diagnose_rw_lock_);
  if (tablets_.count() < DEBUG_INFO_CNT
      && OB_FAIL(tablets_.push_back(fake_replica))) {
    LOG_WARN("fail to push_back", KR(ret), K(fake_replica));
  }
}

int ObUncompactInfo::get_uncompact_info(
    ObIArray<ObTabletReplica> &input_tablets,
    ObIArray<uint64_t> &input_table_ids) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard r_guard(diagnose_rw_lock_);
  if (OB_FAIL(input_tablets.assign(tablets_))) {
    LOG_WARN("fail to assign uncompacted_tablets", KR(ret), K_(tablets));
  } else if (OB_FAIL(input_table_ids.assign(table_ids_))) {
    LOG_WARN("fail to assign uncompacted_tablets", KR(ret), K_(table_ids));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
