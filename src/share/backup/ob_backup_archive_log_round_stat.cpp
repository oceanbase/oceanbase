// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_backup_archive_log_round_stat.h"
#include "share/backup/ob_backup_path.h"
#include "lib/lock/ob_lock_guard.h"
#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::lib;

namespace oceanbase {
namespace share {

ObPGBackupBackupsetStatTree::ObPGBackupBackupsetStatTree()
    : is_inited_(false), interrupted_(false), current_round_(0), current_round_stat_(NULL), allocator_()
{}

ObPGBackupBackupsetStatTree::~ObPGBackupBackupsetStatTree()
{
  reset();
}

void ObPGBackupBackupsetStatTree::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(current_round_stat_)) {
    TenantPGStatMap::iterator iter;
    if (OB_NOT_NULL(current_round_stat_->tenant_pg_map_)) {
      iter = current_round_stat_->tenant_pg_map_->begin();
      for (; iter != current_round_stat_->tenant_pg_map_->end(); ++iter) {
        if (OB_NOT_NULL(iter->second)) {
          iter->second->~ObHashMap();
          iter->second = NULL;
        }
      }
      current_round_stat_->tenant_pg_map_->~ObHashMap();
      current_round_stat_->tenant_pg_map_ = NULL;
    }
    current_round_stat_->allocator_.clear();
    current_round_stat_->~RoundStat();
    current_round_stat_ = NULL;
    current_round_ = 0;
  }
  allocator_.clear();
}

void ObPGBackupBackupsetStatTree::reuse()
{
  reset();
  is_inited_ = true;
  interrupted_ = false;
  current_round_ = 1;
  current_round_stat_ = NULL;
  LOG_INFO("backup archive log stat tree reuse");
}

int ObPGBackupBackupsetStatTree::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pg backup backupset stat tree init twice", K(ret));
  } else {
    is_inited_ = true;
    current_round_ = 0;
    current_round_stat_ = NULL;
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::set_scheduled(const int64_t round)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(round));
  } else if (OB_ISNULL(current_round_stat_)) {
  } else {
    current_round_stat_->scheduled_ = true;
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::get_next_round(const ObArray<int64_t>& round_list, int64_t& next_round)
{
  int ret = OB_SUCCESS;
  next_round = round_list.empty() ? current_round_ : round_list.at(0);
  if (current_round_ == next_round) {
    // do nothing
  } else if (OB_FAIL(free_round_stat(current_round_stat_))) {
    LOG_WARN("failed to free round stat", K(ret), K(current_round_));
  } else if (OB_FAIL(alloc_round_stat(next_round))) {
    LOG_WARN("failed to alloc round stat", K(ret), K(current_round_));
  } else {
    current_round_ = next_round;
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::check_round_exist(
    const common::ObArray<int64_t>& round_list, const int64_t round, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round_list.empty() || round <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(round));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < round_list.count(); ++i) {
      const int64_t tmp_round = round_list.at(i);
      if (tmp_round == round) {
        exist = true;
        break;
      }
    }
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::set_current_round_in_history()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (OB_ISNULL(current_round_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("round stat should not be null", K(ret));
  } else {
    current_round_stat_->in_history_ = true;
    LOG_INFO("set current round in history", K(*current_round_stat_));
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::add_pg_list(
    const int64_t round, const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  RoundStat* stat = current_round_stat_;
  PGStatMap* pg_map = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round != current_round_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round should match", K(ret), K(current_round_), K(round));
  } else if (pg_list.empty()) {
    LOG_INFO("pg key list is empty", K(pg_list));
  } else if (OB_ISNULL(stat) && OB_FAIL(alloc_round_stat(current_round_))) {
    LOG_WARN("failed to allocate round stat", K(round));
  } else if (OB_ISNULL(current_round_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc round stat failed", K(ret));
  } else if (FALSE_IT(stat = current_round_stat_)) {
    // assign
  } else {
    hash_ret = stat->tenant_pg_map_->get_refactored(tenant_id, pg_map);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // does not exist
      if (OB_FAIL(alloc_pg_stat_map(round, tenant_id, allocator_, pg_list, pg_map))) {
        LOG_WARN("failed to allocate pg stat map", K(ret), K(tenant_id), K(round));
      } else {
        hash_ret = stat->tenant_pg_map_->set_refactored(tenant_id, pg_map);
        if (OB_FAIL(hash_ret)) {
          ret = hash_ret;
          LOG_WARN("failed to set pg key", K(ret), K(tenant_id));
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(pg_map)) {
        pg_map->~ObHashMap();
      }
    } else if (OB_SUCCESS == hash_ret) {
      if (OB_FAIL(inner_add_pg_list(pg_map, round, tenant_id, pg_list))) {
        LOG_WARN("failed to inner add pg list", K(ret), K(round), K(tenant_id));
      } else {
        LOG_INFO("inner add pg list success", K(round), K(tenant_id));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get pg map", K(ret));
    }
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::mark_pg_list_finished(
    const int64_t round, const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  PGStatMap* pg_map_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round <= 0 || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark pg list finished get invalid argument", K(ret));
  } else if (OB_FAIL(get_pg_stat_map(round, tenant_id, pg_map_ptr))) {
    LOG_WARN("failed to get pg stat map", K(ret), K(round), K(tenant_id));
  } else if (OB_ISNULL(pg_map_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg map ptr is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      const common::ObPGKey& pg_key = pg_list.at(i);
      PGStat pg_stat;
      hash_ret = pg_map_ptr->get_refactored(pg_key, pg_stat);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pg key should exist", K(ret), K(pg_key), K(pg_map_ptr->size()));
      } else if (OB_SUCCESS == hash_ret) {
        pg_stat.finished_ = true;
        int flag = 1;  // overwrite
        if (OB_FAIL(pg_map_ptr->set_refactored(pg_key, pg_stat, flag))) {
          LOG_WARN("failed to overwrite pg stat", K(ret));
        }
      } else {
        ret = hash_ret;
        LOG_WARN("failed to get from map", K(ret), K(pg_key));
      }
    }
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::free_pg_map(const int64_t round, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  PGStatMap* pg_map_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round <= 0 || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark pg list finished get invalid argument", K(ret));
  } else if (OB_FAIL(get_pg_stat_map(round, tenant_id, pg_map_ptr))) {
    LOG_WARN("failed to get pg stat map", K(ret), K(round), K(tenant_id));
  } else if (OB_ISNULL(pg_map_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg map ptr is null", K(ret));
  } else if (OB_FAIL(pg_map_ptr->clear())) {
    LOG_WARN("failed to clear pg map", KR(ret), KP(pg_map_ptr));
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::free_tenant_stat(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  PGStatMap* map_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tenant_id));
  } else if (OB_ISNULL(current_round_stat_) || OB_ISNULL(current_round_stat_->tenant_pg_map_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("current round stat should not be null", KR(ret), K(tenant_id));
  } else {
    hash_ret = current_round_stat_->tenant_pg_map_->get_refactored(tenant_id, map_ptr);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // do nothing
    } else if (OB_SUCCESS == hash_ret) {
      map_ptr->~ObHashMap();
      map_ptr = NULL;
      hash_ret = current_round_stat_->tenant_pg_map_->erase_refactored(tenant_id);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        // do nothing
      } else if (OB_SUCCESS == hash_ret) {
        LOG_DEBUG("free tenant stat success", K(tenant_id));
      } else {
        ret = hash_ret;
        LOG_WARN("failed to erase map element", KR(ret), K(tenant_id));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get map element", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::set_interrupted()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else {
    interrupted_ = true;
  }
  return ret;
}

bool ObPGBackupBackupsetStatTree::is_mark_finished() const
{
  bool bret = false;
  if (IS_NOT_INIT) {
    bret = false;
    LOG_WARN("stat tree do not init");
  } else if (OB_ISNULL(current_round_stat_)) {
    bret = false;
  } else {
    bret = current_round_stat_->mark_finished_;
  }
  return bret;
}

int ObPGBackupBackupsetStatTree::set_mark_finished()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (OB_ISNULL(current_round_stat_)) {
    ret = OB_NOT_INIT;
  } else {
    current_round_stat_->mark_finished_ = true;
  }
  return ret;
}

// current round : 1
// round list    :   3 4 (because round 1, 2 has been cleaned)
// fast forward round will set current round to 3
int ObPGBackupBackupsetStatTree::fast_forward_round_if_needed(
    const common::ObArray<int64_t>& round_list, bool& fast_forwarded)
{
  int ret = OB_SUCCESS;
  fast_forwarded = true;
  bool in_round_list = false;
  int64_t min_next_round = INT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round list should not be empty", KR(ret), K(round_list));
  } else if (OB_FAIL(check_current_round_in_round_list(round_list, in_round_list))) {
    LOG_WARN("failed to check current round in round list", KR(ret), K(round_list));
  } else if (in_round_list) {
    fast_forwarded = false;
  } else if (OB_FAIL(get_min_next_round(round_list, min_next_round))) {
    LOG_WARN("failed to get min next round", KR(ret), K(round_list));
  } else {
    if (OB_NOT_NULL(current_round_stat_)) {
      if (OB_FAIL(free_round_stat(current_round_stat_))) {
        LOG_WARN("failed to free round stat", KR(ret));
      } else {
        current_round_stat_ = NULL;
        LOG_INFO("set current round stat to null");
      }
    }
    current_round_ = min_next_round;
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::check_current_round_in_round_list(
    const common::ObArray<int64_t>& round_list,  // sorted
    bool& in_round_list)
{
  int ret = OB_SUCCESS;
  in_round_list = false;
  const int64_t current_round = current_round_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round list should not be empty", KR(ret), K(round_list));
  } else {
    RoundListIter iter = std::lower_bound(round_list.begin(), round_list.end(), current_round);
    if (iter == round_list.end()) {
      --iter;
    } else if (iter != round_list.begin() && *iter > current_round) {
      --iter;
    }
    if (*iter == current_round) {
      in_round_list = true;
    }
  }
  return ret;
}

// current round 1
// round list 3, 4, 5
// return -> 3
int ObPGBackupBackupsetStatTree::get_min_next_round(const common::ObArray<int64_t>& round_list, int64_t& min_next_round)
{
  int ret = OB_SUCCESS;
  min_next_round = INT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round list should not be empty", KR(ret), K(round_list));
  } else {
    min_next_round = round_list.at(0);
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::alloc_round_stat(const int64_t round)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  RoundStat* round_stat = NULL;
  TenantPGStatMap* map_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(RoundStat))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (FALSE_IT(round_stat = new (buf) RoundStat)) {
    // placement new
  } else {
    round_stat->round_ = round;
    round_stat->mark_finished_ = false;
    round_stat->in_history_ = false;
    round_stat->tenant_pg_map_ = NULL;

    if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(TenantPGStatMap))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate map", K(ret));
    } else if (FALSE_IT(map_ptr = new (buf) TenantPGStatMap)) {
      // placement new
    } else if (OB_FAIL(map_ptr->create(MAX_TENANT_MAP_BUCKET_SIZE, ObModIds::BACKUP))) {
      LOG_WARN("failed to create tenant pg stat map", K(ret));
    } else {
      round_stat->tenant_pg_map_ = map_ptr;
    }

    current_round_stat_ = round_stat;
    round_stat = NULL;
    LOG_INFO("alloc round stat", KR(ret), K(current_round_stat_));
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::free_round_stat(RoundStat*& round_stat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (OB_ISNULL(round_stat)) {
    // do nothing
  } else if (!round_stat->in_history_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("round has not finished, can not free now", K(ret));
  } else {
    LOG_INFO("free round stat", KR(ret), K(*round_stat));
    if (OB_NOT_NULL(round_stat)) {
      TenantPGStatMap::iterator iter;
      if (OB_NOT_NULL(round_stat->tenant_pg_map_)) {
        iter = round_stat->tenant_pg_map_->begin();
        for (; OB_SUCC(ret) && iter != current_round_stat_->tenant_pg_map_->end(); ++iter) {
          if (OB_NOT_NULL(iter->second)) {
            iter->second->~ObHashMap();
            iter->second = NULL;
          }
        }
        round_stat->tenant_pg_map_->~ObHashMap();
        round_stat->tenant_pg_map_ = NULL;
      }
      round_stat->allocator_.clear();
      round_stat->~RoundStat();
      round_stat = NULL;
    }
    allocator_.clear();
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::check_round_finished(const int64_t round, bool& finished, bool& interrupted)
{
  int ret = OB_SUCCESS;
  finished = true;
  interrupted = false;
  RoundStat* stat = current_round_stat_;
  TenantPGStatMap* tenant_pg_map = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (OB_ISNULL(stat)) {
    finished = false;
    LOG_INFO("stat is null, not finished", K(round));
  } else if (!stat->scheduled_) {
    finished = false;
    LOG_INFO("round has not been scheduled before", K(round));
  } else if (!stat->in_history_) {
    finished = false;
    LOG_INFO("still in progress, not finished", K(round), K(stat));
  } else if (!stat->mark_finished_) {
    finished = false;
    LOG_INFO("not mark finished, not finished", K(round));
  } else if (interrupted_) {
    finished = true;
    interrupted = true;
  } else if (FALSE_IT(tenant_pg_map = stat->tenant_pg_map_)) {
    // assign
  } else if (OB_ISNULL(tenant_pg_map)) {
    // not finished if null
  } else {
    TenantPGStatMap::const_iterator iter;
    for (iter = tenant_pg_map->begin(); OB_SUCC(ret) && iter != tenant_pg_map->end(); ++iter) {
      bool tenant_finished = true;
      const uint64_t tenant_id = iter->first;
      if (OB_FAIL(check_all_pg_finished(round, tenant_id, tenant_finished))) {
        LOG_WARN("failed to check all pg finished", K(ret), K(round), K(tenant_id));
      } else if (!tenant_finished) {
        finished = false;
        LOG_INFO("tenant do not finish", K(ret));
        break;
      }
    }
    LOG_INFO("round finish status", K(ret), K(round), K(finished));
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::alloc_pg_stat_map(const int64_t round, const uint64_t tenant_id,
    common::ObArenaAllocator& allocator, const common::ObIArray<common::ObPGKey>& pg_list, PGStatMap*& pg_map)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  pg_map = NULL;
  char* buf = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(sizeof(PGStatMap))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate map", K(ret));
  } else if (FALSE_IT(pg_map = new (buf) PGStatMap)) {
    // in place new
  } else if (OB_FAIL(pg_map->create(MAX_PG_MAP_BUCKET_SIZE, ObModIds::BACKUP))) {
    LOG_WARN("failed to create pg map", K(ret), K(round), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      const common::ObPGKey& pg_key = pg_list.at(i);
      PGStat pg_stat;
      pg_stat.round_ = round;
      pg_stat.finished_ = false;
      pg_stat.tenant_id_ = tenant_id;
      hash_ret = pg_map->set_refactored(pg_key, pg_stat);
      if (OB_HASH_EXIST == hash_ret) {
        LOG_INFO("pg key already exist", K(pg_key));
      } else if (OB_SUCCESS == hash_ret) {
        LOG_INFO("set pg key success", K(ret), K(pg_key));
      } else {
        ret = hash_ret;
        LOG_WARN("map set refactored error", K(hash_ret));
      }
    }
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::get_pg_stat_map(const int64_t round, const uint64_t tenant_id, PGStatMap*& pg_map)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  pg_map = NULL;
  RoundStat* round_stat = current_round_stat_;
  TenantPGStatMap* tenant_pg_map = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (OB_ISNULL(round_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("round stat should not be null", K(ret));
  } else if (FALSE_IT(tenant_pg_map = round_stat->tenant_pg_map_)) {
    // assign
  } else if (OB_ISNULL(tenant_pg_map)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("tenant pg map do not exist", K(ret), K(round), K(tenant_id));
  } else {
    hash_ret = tenant_pg_map->get_refactored(tenant_id, pg_map);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("do not exist", K(ret), K(round), K(tenant_id));
    } else if (OB_SUCCESS == hash_ret) {
      LOG_INFO("get pg map ptr success", K(ret), K(round), K(tenant_id), K(pg_map->size()));
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get pg map", K(ret));
    }
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::check_all_pg_finished(
    const int64_t round, const uint64_t tenant_id, bool& all_finished)
{
  int ret = OB_SUCCESS;
  all_finished = true;
  PGStatMap* pg_map_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round <= 0 || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check all pg finished get invalid argument", K(ret));
  } else if (OB_FAIL(get_pg_stat_map(round, tenant_id, pg_map_ptr))) {
    LOG_WARN("failed to get pg stat map", K(ret), K(round), K(tenant_id));
  } else if (OB_ISNULL(pg_map_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg map ptr is null", K(ret), K(round), K(tenant_id));
  } else {
    PGStatMap::const_iterator iter;
    for (iter = pg_map_ptr->begin(); OB_SUCC(ret) && iter != pg_map_ptr->end(); ++iter) {
      const PGStat& pg_stat = iter->second;
      if (!pg_stat.finished_) {
        all_finished = false;
        break;
      }
    }
  }
  return ret;
}

int ObPGBackupBackupsetStatTree::inner_add_pg_list(PGStatMap*& pg_stat_map, const int64_t round,
    const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat tree do not init", K(ret));
  } else if (round <= 0 || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check all pg finished get invalid argument", K(ret));
  } else {
    PGStat pg_stat;
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      const common::ObPGKey& pg_key = pg_list.at(i);
      hash_ret = pg_stat_map->get_refactored(pg_key, pg_stat);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        pg_stat.finished_ = false;
        pg_stat.round_ = round;
        pg_stat.tenant_id_ = tenant_id;
        if (OB_FAIL(pg_stat_map->set_refactored(pg_key, pg_stat))) {
          LOG_WARN("failed to set pg key", K(ret));
        }
      } else if (OB_SUCCESS == hash_ret) {
        LOG_INFO("pg key already exist", K(pg_stat));
      } else {
        ret = hash_ret;
        LOG_WARN("error unexpected", K(ret));
      }
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
