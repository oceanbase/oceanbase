/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/tablet/ob_session_tablet_info_map.h"
#include "storage/tablet/ob_session_tablet_helper.h"
#include "storage/tablet/ob_tablet_to_global_temporary_table_operator.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

int ObSessionTabletInfo::init(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id, const uint64_t table_id,
  const int64_t sequence, const uint32_t session_id, const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  ls_id_ = ls_id;
  table_id_ = table_id;
  sequence_ = sequence;
  session_id_ = session_id;
  transfer_seq_ = transfer_seq;
  if (is_valid() == false) {
    reset();
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(ls_id), K(table_id),
      K(sequence), K(session_id), K(transfer_seq));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSessionTabletInfo,
                    table_id_,
                    sequence_,
                    session_id_,
                    ls_id_,
                    tablet_id_,
                    transfer_seq_);

ObSessionTabletInfoMap::ObSessionTabletInfoMap()
  : tablet_infos_(),
    mutex_()
{
  tablet_infos_.set_attr(lib::ObMemAttr(MTL_ID(), "SessTblInfoM"));
}

OB_SERIALIZE_MEMBER(ObSessionTabletInfoMap,
                    tablet_infos_);

int ObSessionTabletInfoMap::get_session_tablet_if_not_exist_add(
    const ObSessionTabletInfoKey &key,
    ObSessionTabletInfo &session_tablet_info)
{
  int ret = OB_SUCCESS;
  session_tablet_info.reset();
  if (OB_UNLIKELY(OB_INVALID_ID == key.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(inner_get_session_tablet(key.table_id_,
                                         key.sequence_,
                                         key.session_id_,
                                         session_tablet_info))
            && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to inner get session tablet", KR(ret), K(key));
    }
  }
  if (OB_SUCC(ret)) { // already exist
    LOG_INFO("session tablet already exists", KR(ret), K(key), K(session_tablet_info));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ObSessionTabletCreateHelper create_helper(MTL_ID(), key.table_id_, key.sequence_, key.session_id_, *this);
    if (OB_FAIL(create_helper.do_work())) {
      LOG_WARN("failed to create session tablet", KR(ret), K(key));
    } else if (OB_FAIL(session_tablet_info.init(create_helper.get_tablet_ids().at(0), create_helper.get_ls_id(),
        key.table_id_, key.sequence_, key.session_id_, 0/*transfer_seq*/))) {
      LOG_WARN("failed to init session tablet info", KR(ret), K(key), K(session_tablet_info));
    } else {
      session_tablet_info.is_creator_ = true;
      if (OB_UNLIKELY(!session_tablet_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(session_tablet_info));
      } else {
        lib::ObMutexGuard guard(mutex_);
        if (OB_FAIL(tablet_infos_.push_back(session_tablet_info))) {
          LOG_WARN("failed to push back", KR(ret), K(session_tablet_info));
        } else {
          FLOG_INFO("session tablet added", KR(ret), K(key), K(session_tablet_info), K(common::lbt()));
        }
      }
    }
  } else {
    LOG_WARN("failed to get session tablet", KR(ret), K(key));
  }
  return ret;
}

int ObSessionTabletInfoMap::add_session_tablet(
    const common::ObIArray<uint64_t> &table_ids,
    const int64_t sequence,
    const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  ObSessionTabletInfo tablet_info;
  ObSessionTabletCreateHelper create_helper(MTL_ID(), sequence, session_id, *this);
  if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_ids));
  } else if (OB_FAIL(check_session_tablet_by_table_id_from_inner_table(table_ids, session_id, sequence))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("session tablet already exists", KR(ret), K(table_ids), K(session_id), K(sequence));
    } else {
      LOG_WARN("failed to check session tablet by table id from inner table or some exist", KR(ret), K(table_ids), K(session_id), K(sequence));
    }
  } else if (OB_FAIL(create_helper.set_table_ids(table_ids))) {
    LOG_WARN("failed to set table ids", KR(ret), K(table_ids));
  } else if (OB_FAIL(create_helper.do_work())) {
    LOG_WARN("failed to create session tablet", KR(ret), K(table_ids));
  } else if (OB_UNLIKELY(create_helper.get_tablet_ids().count() < table_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(create_helper.get_tablet_ids().count()), K(table_ids.count()));
  } else {
    const common::ObIArray<common::ObTabletID> &tablet_ids = create_helper.get_tablet_ids();
    const common::ObIArray<uint64_t> &create_table_ids = create_helper.get_table_ids();
    const share::ObLSID &ls_id = create_helper.get_ls_id();
    lib::ObMutexGuard guard(mutex_);
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      const uint64_t table_id = create_table_ids.at(i);
      const common::ObTabletID &tablet_id = tablet_ids.at(i);
      tablet_info.reset();
      // The tablet has been newly created, so it must be inserted into tablet_infos_.
      if (OB_FAIL(tablet_info.init(tablet_id, ls_id, table_id, sequence, session_id, 0/*transfer_seq*/))) {
        LOG_WARN("failed to init session tablet info", KR(ret), K(create_table_ids.at(i)));
      } else if (FALSE_IT(tablet_info.is_creator_ = true)) {
      } else if (OB_FAIL(tablet_infos_.push_back(tablet_info))) {
        LOG_WARN("failed to push back", KR(ret), K(tablet_info));
      }
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("session tablet added", KR(ret), K(table_ids), K(create_table_ids), K(sequence), K(session_id), K(tablet_ids), K(tablet_infos_));
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::get_session_tablet(
    const ObSessionTabletInfoKey &key,
    ObSessionTabletInfo &session_tablet_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == key.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(inner_get_session_tablet(key.table_id_, key.sequence_, key.session_id_, session_tablet_info))) {
      LOG_WARN("failed to inner get session tablet", KR(ret), K(key));
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::inner_get_session_tablet(
    const uint64_t table_id,
    const int64_t sequence,
    const int32_t session_id,
    ObSessionTabletInfo &session_tablet_info)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  for (; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
    if (tablet_infos_.at(i).table_id_ == table_id &&
        tablet_infos_.at(i).sequence_ == sequence &&
        tablet_infos_.at(i).session_id_ == session_id) {
      session_tablet_info = tablet_infos_.at(i);
      break;
    }
  }
  if (OB_SUCC(ret) && i >= tablet_infos_.count()) {
    // try get from inner table
    if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::point_get(*GCTX.sql_proxy_, MTL_ID(), table_id, sequence, session_tablet_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get session tablet from inner table", KR(ret), K(table_id), K(sequence));
      }
    } else if (OB_UNLIKELY(!session_tablet_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(session_tablet_info));
    } else if (OB_FAIL(tablet_infos_.push_back(session_tablet_info))) {
      LOG_WARN("failed to push back", KR(ret), K(session_tablet_info));
    } else {
      FLOG_INFO("session tablet get from inner table", KR(ret), K(table_id), K(sequence), K(session_tablet_info), K(tablet_infos_));
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::remove_session_tablet(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else {
    lib::ObMutexGuard guard(mutex_);
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
      if (tablet_infos_.at(i).table_id_ == table_id) {
        break;
      }
    }
    if (OB_SUCC(ret) && i >= tablet_infos_.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("session tablet not found", KR(ret), K(table_id));
    } else if (OB_FAIL(tablet_infos_.remove(i))) {
      LOG_WARN("failed to remove", KR(ret), K(table_id));
    }
    FLOG_INFO("session tablet removed", KR(ret), K(table_id), K(i), K(tablet_infos_));
  }
  return ret;
}

int ObSessionTabletInfoMap::get_table_ids_by_session_id_and_sequence(
    const uint32_t session_id,
    const int64_t sequence,
    common::ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  if (!tablet_infos_.empty()) {
    lib::ObMutexGuard guard(mutex_);
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
      if (tablet_infos_.at(i).session_id_ == session_id &&
          tablet_infos_.at(i).sequence_ == sequence) {
        if (OB_FAIL(table_ids.push_back(tablet_infos_.at(i).table_id_))) {
          LOG_WARN("failed to push back", KR(ret), K(tablet_infos_.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObSessionTabletInfoMap::check_session_tablet_by_table_id_from_inner_table(
  const common::ObIArray<uint64_t> &table_ids,
  const uint32_t session_id,
  const int64_t sequence)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObSessionTabletInfo, MAX_SESSION_TABLET_COUNT> tablet_infos;
  common::ObArray<uint64_t> exist_table_ids;
  if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_ids));
  } else if (OB_FAIL(share::ObTabletToGlobalTmpTableOperator::batch_get_by_table_ids(*GCTX.sql_proxy_, MTL_ID(), table_ids, tablet_infos))) {
    LOG_WARN("failed to get session tablet infos from inner table", KR(ret), K(table_ids));
  } else if (0 == tablet_infos.count()) { // fast path for tablet infos not exist
    ret = OB_SUCCESS;
    LOG_INFO("session tablet not exist", KR(ret), K(table_ids), K(tablet_infos));
  } else if (OB_FAIL(exist_table_ids.reserve(table_ids.count()))) {
    LOG_WARN("failed to reserve", KR(ret), K(table_ids));
  } else {
    int64_t exist_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_infos.count(); ++i) {
      if (tablet_infos.at(i).sequence_ == sequence &&
          tablet_infos.at(i).session_id_ == session_id &&
          !is_contain(exist_table_ids, tablet_infos.at(i).table_id_) && // table id in one session should be unique
          is_contain(table_ids, tablet_infos.at(i).table_id_)) {
        exist_count++;
        if (OB_FAIL(exist_table_ids.push_back(tablet_infos.at(i).table_id_))) {
          LOG_WARN("failed to push back", KR(ret), K(tablet_infos.at(i).table_id_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (exist_count == table_ids.count()) {
        ret = OB_ENTRY_EXIST;
        LOG_INFO("session tablet already exists", KR(ret), K(table_ids), K(tablet_infos));
      } else if (0 == exist_count) {
        ret = OB_SUCCESS;
        LOG_INFO("session tablet not exist", KR(ret), K(table_ids), K(tablet_infos));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session tablet exist but not all", KR(ret), K(table_ids), K(tablet_infos));
      }
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
