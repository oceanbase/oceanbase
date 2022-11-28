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

#include "ob_defensive_check_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace transaction
{

void SingleRowDefensiveRecord::reset()
{
  generate_ts_ = 0;
  rowkey_.reset();
  row_.reset();
  allocator_.reset();
}

int SingleRowDefensiveRecord::deep_copy(const blocksstable::ObDatumRow &row,
                                        const blocksstable::ObDatumRowkey &rowkey,
                                        const ObDefensiveCheckRecordExtend &extend_info)
{
  int ret = OB_SUCCESS;
  
  if (OB_FAIL(rowkey.deep_copy(rowkey_, allocator_))) {
    TRANS_LOG(WARN, "rowkey deep copy error", K(ret), K(rowkey));
  } else if (OB_FAIL(row_.init(row.count_))) {
    TRANS_LOG(WARN, "datum row init error", K(ret), K(row));
  } else if (OB_FAIL(row_.deep_copy(row, allocator_))) {
    TRANS_LOG(WARN, "datum row deep copy error", K(ret), K(row));
  } else {
    extend_info_ = extend_info;
    generate_ts_ = ObTimeUtility::current_time();
  }

  return ret;
}

void ObSingleTabletDefensiveCheckInfo::reset()
{
  tablet_id_.reset();
  for (int64_t i = 0; i < record_arr_.count(); ++i) {
    if (NULL != record_arr_.at(i)) {
      op_free(record_arr_.at(i));
      record_arr_.at(i) = NULL;
    }
  }
}

int ObSingleTabletDefensiveCheckInfo::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
  }

  return ret;
}

int ObSingleTabletDefensiveCheckInfo::add_record(SingleRowDefensiveRecord *record)
{
  int ret = OB_SUCCESS;

  if (NULL == record) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(record));
  } else if (OB_FAIL(record_arr_.push_back(record))) {
    TRANS_LOG(WARN, "record arr push back error", K(ret), K(*record));
  } else {
    // do nothing
  }

  return ret;
}

int64_t ObDefensiveCheckMgr::max_record_cnt_ = 128;

int ObDefensiveCheckMgr::init()
{
  int ret = OB_SUCCESS;
  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObDefensiveCheckMgr init twince", K(ret));
  } else if (OB_FAIL(map_.init())) {
    TRANS_LOG(WARN, "ObDefensiveCheckMgr map init error", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObDefensiveCheckMgr::reset()
{
  if (is_inited_) {
    map_.reset();
    is_inited_ = false;
  }
}

int ObDefensiveCheckMgr::put(const ObTabletID &tablet_id,
                             const blocksstable::ObDatumRow &row,
                             const blocksstable::ObDatumRowkey &rowkey,
                             const ObDefensiveCheckRecordExtend &extend_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObDefensiveCheckMgr not init ", K(ret));
  } else {
    SingleRowDefensiveRecord *r = NULL;
    if (NULL == (r = op_alloc(SingleRowDefensiveRecord))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc memory fail", K(ret), K(tablet_id));
    } else if (OB_FAIL(r->deep_copy(row, rowkey, extend_info))) {
      TRANS_LOG(WARN, "Defensive reocrd deep copy error", K(ret), K(row), K(rowkey));
    } else {

      Guard spin_guard(lock_);

      ObSingleTabletDefensiveCheckInfo *info = NULL;
      if (OB_FAIL(map_.get(tablet_id, info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "map get error", K(ret), K(tablet_id));
        } else if (NULL == (info = op_alloc(ObSingleTabletDefensiveCheckInfo))
            || OB_FAIL(info->init(tablet_id))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "Defensive info alloc or init error", K(tablet_id));
        } else if (OB_FAIL(info->add_record(r))) {
          TRANS_LOG(WARN, "add defensive record error", K(ret));
        } else if (OB_FAIL(map_.insert_and_get(tablet_id, info, NULL))) {
          TRANS_LOG(WARN, "defensive check info insert map error", K(ret), K(tablet_id), KP(info));
        } else {
          TRANS_LOG(DEBUG, "add record", K(tablet_id), K(row), K(rowkey), K(extend_info));
          // do nothing
        }
        if (OB_FAIL(ret)) {
          if (NULL != info) {
            op_free(info);
            info = NULL;
          }
        }
      } else if (info->get_record_arr().count() >= max_record_cnt_) {
        op_free(r);
        r = NULL;
      } else if (OB_FAIL(info->add_record(r))) {
        TRANS_LOG(WARN, "add defensive record error", K(ret));
      } else {
        TRANS_LOG(DEBUG, "add record", K(tablet_id), K(row), K(rowkey), K(extend_info));
        // do nothing
      }
      if (OB_SUCC(ret) && NULL != info) {
        map_.revert(info);
      }
    }
    if (OB_FAIL(ret) && NULL != r) {
      op_free(r);
      r = NULL;
    }
  }

  return ret;
}

void ObDefensiveCheckMgr::dump(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObDefensiveCheckMgr not init ", K(ret));
  } else {

    Guard spin_guard(lock_);

    ObSingleTabletDefensiveCheckInfo *info = NULL;
    if (OB_FAIL(map_.get(tablet_id, info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "map get error", K(ret), K(tablet_id));
      }
    } else {
      const int64_t max_count = std::min(max_record_cnt_, info->get_record_arr().count());
      TRANS_LOG(INFO, "###################### start to print defensice check info##########################",
          K(max_count), "tablet_id", info->get_tablet_id());
      for (int64_t i = 0; i < max_count; ++i) {
        TRANS_LOG(INFO, "print single row defensive check info",
            "rowkey", info->get_record_arr().at(i)->rowkey_,
            "row", info->get_record_arr().at(i)->row_,
            "extend_info", info->get_record_arr().at(i)->extend_info_);
      }
      TRANS_LOG(INFO, "##################### print defensice check info finished #########################",
            K(max_count),
            "tablet_id", info->get_tablet_id());

      map_.revert(info);
    }
  }
  UNUSED(ret);
}


} /* namespace transaction*/
} /* namespace oceanbase */
