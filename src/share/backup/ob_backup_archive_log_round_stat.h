// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVE_LOG_ROUND_STAT_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVE_LOG_ROUND_STAT_H_

#include "lib/lock/ob_mutex.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_partition_key.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace share {

/**
 *                 stat
 *                /  |  \
 *   round       1   2   3
 *              / \ / \  / \
 *   tenant   1001        1002
 *           /  |  \
 *   pg    11  22  33
 **/

class ObPGBackupBackupsetStatTree {
  struct PGStat {
    PGStat()
        : round_(-1), tenant_id_(common::OB_INVALID_ID), finished_(false), total_file_size_(-1), catch_file_size_(-1)
    {}
    ~PGStat()
    {}

    bool is_valid() const
    {
      return round_ > 0 && common::OB_INVALID_ID != tenant_id_;
    }

    TO_STRING_KV(K_(round), K_(tenant_id), K_(finished));

    int64_t round_;
    uint64_t tenant_id_;
    bool finished_;
    int64_t total_file_size_;
    int64_t catch_file_size_;
  };

  typedef common::hash::ObHashMap<common::ObPGKey /* pg key */, PGStat> PGStatMap;
  typedef common::hash::ObHashMap<uint64_t /*tenant_id*/, PGStatMap*> TenantPGStatMap;

  struct RoundStat {
    RoundStat()
        : round_(-1), scheduled_(false), mark_finished_(false), in_history_(false), tenant_pg_map_(NULL), allocator_()
    {}
    ~RoundStat()
    {}
    TO_STRING_KV(K_(round), K_(scheduled), K_(mark_finished), K_(in_history));
    int64_t round_;
    bool scheduled_;
    bool mark_finished_;
    bool in_history_;
    TenantPGStatMap* tenant_pg_map_;
    common::ObArenaAllocator allocator_;
  };

  typedef common::ObArray<int64_t>::const_iterator RoundListIter;

  const int64_t MAX_TENANT_MAP_BUCKET_SIZE = 1024;
  const int64_t MAX_ROUND_MAP_BUCKET_SIZE = 1024;
  const int64_t MAX_PG_MAP_BUCKET_SIZE = 10240;

public:
  ObPGBackupBackupsetStatTree();
  ~ObPGBackupBackupsetStatTree();

  void reset();
  void reuse();
  int init();
  int set_scheduled(const int64_t round_id);
  int get_next_round(const common::ObArray<int64_t>& round_list, int64_t& next_round);
  int check_round_exist(const common::ObArray<int64_t>& round_list, const int64_t round, bool& exist);
  int set_current_round_in_history();
  int add_pg_list(const int64_t round, const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list);
  int mark_pg_list_finished(
      const int64_t round, const uint64_t tenant_id, const common::ObIArray<common::ObPGKey>& pg_list);
  int free_pg_map(const int64_t round, const uint64_t tenant_id);
  int free_tenant_stat(const int64_t tenant_id);
  int64_t get_current_round() const
  {
    return current_round_;
  }
  void set_current_round(const int64_t round)
  {
    current_round_ = round;
  }
  bool is_interrupted() const
  {
    return interrupted_;
  }
  int set_interrupted();
  bool is_mark_finished() const;
  int set_mark_finished();
  int check_round_finished(const int64_t round, bool& finished, bool& interrupted);

private:
  int fast_forward_round_if_needed(const common::ObArray<int64_t>& round_list,  // require round list is sorted
      bool& fast_forwarded);
  int check_current_round_in_round_list(const common::ObArray<int64_t>& round_list,  // require round list is sorted
      bool& in_round_list);
  int get_min_next_round(const common::ObArray<int64_t>& round_list,  // require round list is sorted
      int64_t& min_next_round);
  int alloc_round_stat(const int64_t round);
  int free_round_stat(RoundStat*& round_stat);
  int alloc_pg_stat_map(const int64_t round, const uint64_t tenant_id, common::ObArenaAllocator& allocator,
      const common::ObIArray<common::ObPGKey>& pg_list, PGStatMap*& pg_map);
  int get_pg_stat_map(const int64_t round, const uint64_t tenant_id, PGStatMap*& pg_map);
  int check_all_pg_finished(const int64_t round, const uint64_t tenant_id, bool& finished);
  int inner_add_pg_list(PGStatMap*& pg_stat_map, const int64_t round, const uint64_t tenant_id,
      const common::ObIArray<common::ObPGKey>& pg_list);

private:
  bool is_inited_;
  bool interrupted_;
  int64_t current_round_;
  RoundStat* current_round_stat_;
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPGBackupBackupsetStatTree);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
