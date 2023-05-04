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
 *
 * Transaction statistics
 * 1. tps: number of transactions per second
 * 2. rps: number of statements per second
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_TRANS_STAT_INFO_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_TRANS_STAT_INFO_H__

#include "lib/utility/ob_print_utils.h"         // TO_STRING_KV
#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool
#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap

namespace oceanbase
{
namespace libobcdc
{
// Transaction tps statistics
struct TransTpsStatInfo
{
  int64_t created_trans_count_;                    // Number of created transactions counts
  int64_t last_created_trans_count_;               // Number of created transactions counts last time

  TransTpsStatInfo() { reset(); }
  ~TransTpsStatInfo() { reset(); }

  void reset()
  {
    created_trans_count_ = 0;
    last_created_trans_count_ = 0;
  }

  void do_tps_stat()
  {
    ATOMIC_INC(&created_trans_count_);
  }

  double calc_tps(const int64_t delta_time);

  TO_STRING_KV(K_(created_trans_count),
               K_(last_created_trans_count));
};

// Transaction rps statistics
struct TransRpsStatInfo
{
  int64_t created_records_count_;                  // Number of created statements count
  int64_t last_created_records_count_;             // Number of created statements count last time

  TransRpsStatInfo() { reset(); }
  ~TransRpsStatInfo() { reset(); }

  void reset()
  {
    created_records_count_ = 0;
    last_created_records_count_ = 0;
  }

  void do_rps_stat(int64_t record_count)
  {
    ATOMIC_AAF(&created_records_count_, record_count);
  }

  double calc_rps(const int64_t delta_time);

  TO_STRING_KV(K_(created_records_count),
               K_(last_created_records_count));
};

// Transaction tps/rps statistics
struct TransTpsRpsStatInfo
{
  TransTpsStatInfo tps_stat_info_;
  TransRpsStatInfo rps_stat_info_;

  TransTpsRpsStatInfo() { reset(); }
  ~TransTpsRpsStatInfo() { reset(); }
  void reset();

  void do_tps_stat()
  {
    tps_stat_info_.do_tps_stat();
  }

  void do_rps_stat(int64_t record_count)
  {
    rps_stat_info_.do_rps_stat(record_count);
  }

  double calc_tps(const int64_t delta_time);

  double calc_rps(const int64_t delta_time);

  TO_STRING_KV(K_(tps_stat_info),
               K_(rps_stat_info));
};


struct DispatcherStatInfo
{
  int64_t dispatched_trans_count_;
  int64_t dispatched_redo_count_;

  void reset()
  {
    ATOMIC_SET(&dispatched_trans_count_, 0);
    ATOMIC_SET(&dispatched_redo_count_, 0);
  }

  void inc_dispatched_trans_count()
  {
    ATOMIC_INC(&dispatched_trans_count_);
  }

  void inc_dispatched_redo_count()
  {
    ATOMIC_INC(&dispatched_redo_count_);
  }

  void get_and_reset_dispatcher_stat(int64_t &trans_count, int64_t &redo_count)
  {
    trans_count = ATOMIC_TAS(&dispatched_trans_count_, 0);
    redo_count = ATOMIC_TAS(&dispatched_redo_count_, 0);
  }

  void calc_and_print_stat(int64_t delta_time);
};

struct SorterStatInfo
{
  int64_t sorted_trans_count_;
  int64_t sorted_br_count_;

  void reset()
  {
    ATOMIC_SET(&sorted_trans_count_, 0);
    ATOMIC_SET(&sorted_br_count_, 0);
  }

  void inc_sorted_trans_count()
  {
    ATOMIC_INC(&sorted_trans_count_);
  }

  void inc_sorted_br_count()
  {
    ATOMIC_INC(&sorted_br_count_);
  }

  void get_and_reset_sorter_stat(int64_t &trans_count, int64_t &br_count)
  {
    trans_count = ATOMIC_TAS(&sorted_trans_count_, 0);
    br_count = ATOMIC_TAS(&sorted_br_count_, 0);
  }

  void calc_and_print_stat(int64_t delta_time);
};

class IObLogTransStatMgr
{
public:
  IObLogTransStatMgr() {}
  virtual ~IObLogTransStatMgr() {}

public:
  // trans stat
  virtual void do_tps_stat() = 0;
  virtual void do_rps_stat_before_filter(const int64_t record_count) = 0;
  virtual void do_rps_stat_after_filter(const int64_t record_count) = 0;

  // tenant stat
  virtual int add_served_tenant(const char *tenant_name, const uint64_t tenant_id) = 0;
  virtual int drop_served_tenant(const uint64_t tenant_id) = 0;
  // tenant tps and rps(before filter) stat
  virtual int do_tenant_tps_rps_stat(const uint64_t tenant_id, int64_t record_count) = 0;
  virtual int do_tenant_rps_stat_after_filter(const uint64_t tenant_id, int64_t record_count) = 0;

  // drc stat
  // next record
  virtual void do_drc_consume_tps_stat() = 0;
  virtual void do_drc_consume_rps_stat() = 0;
  // release record
  virtual void do_drc_release_tps_stat() = 0;
  virtual void do_drc_release_rps_stat() = 0;
  // dispatch_redo
  virtual void do_dispatch_trans_stat() = 0;
  virtual void do_dispatch_redo_stat() = 0;
  // sorter
  virtual void do_sort_trans_stat() = 0;
  virtual void do_sort_br_stat() = 0;

  // print stat info
  virtual void print_stat_info() = 0;
};

class ObLogTransStatMgr : public IObLogTransStatMgr
{
public:
  ObLogTransStatMgr();
  ~ObLogTransStatMgr();

public:
  int init();
  void destroy();

public:
  void do_tps_stat();
  void do_rps_stat_before_filter(const int64_t record_count);
  void do_rps_stat_after_filter(const int64_t record_count);

  int add_served_tenant(const char *tenant_name, const uint64_t tenant_id);
  int drop_served_tenant(const uint64_t tenant_id);
  int do_tenant_tps_rps_stat(const uint64_t tenant_id, int64_t record_count);
  int do_tenant_rps_stat_after_filter(const uint64_t tenant_id, int64_t record_count);

  void do_drc_consume_tps_stat();
  void do_drc_consume_rps_stat();
  void do_drc_release_tps_stat();
  void do_drc_release_rps_stat();
  void do_dispatch_trans_stat();
  void do_dispatch_redo_stat();
  void do_sort_trans_stat();
  void do_sort_br_stat();

  void print_stat_info();

private:
  void clear_tenant_stat_info_();

private:
  static const int64_t CACHED_TENANT_STAT_INFO_COUNT = 1 << 10;

private:
  struct TenantID;
  struct TenantStatInfo;
  typedef common::ObLinearHashMap<TenantID, TenantStatInfo *> TenantStatInfoMap;
  typedef common::ObSmallObjPool<TenantStatInfo> TenantStatInfoPool;

private:
  struct TenantID
  {
    uint64_t tenant_id_;

    TenantID(const uint64_t tenant_id) :
        tenant_id_(tenant_id)
    {}

    int64_t hash() const
    {
      return static_cast<int64_t>(tenant_id_);
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }

    bool operator== (const TenantID &other) const
    {
      return tenant_id_ == other.tenant_id_;
    }

    void reset()
    {
      tenant_id_ = common::OB_INVALID_ID;
    }

    TO_STRING_KV(K_(tenant_id));
  };

  struct TenantStatInfo
  {
    // storage format: TENANT_NAME
    char name_[common::OB_MAX_TENANT_NAME_LENGTH + 1];
    // tps
    TransTpsStatInfo tps_stat_info_;
    // RPS( before filtered by Formatter)
    TransRpsStatInfo rps_stat_info_before_filter_;
    // RPS( after filtered by Formatter)
    TransRpsStatInfo rps_stat_info_after_filter_;

    TenantStatInfo() { reset(); }
    ~TenantStatInfo() { reset(); }

    void reset()
    {
      name_[0] = '\0';
      tps_stat_info_.reset();
      rps_stat_info_before_filter_.reset();
      rps_stat_info_after_filter_.reset();
    }

    TO_STRING_KV(K_(name), K_(tps_stat_info),
                 K_(rps_stat_info_before_filter),
                 K_(rps_stat_info_after_filter));
  };

  // Update rps information before filtering for a given tenant
  struct TenantRpsBeforeFilterUpdater
  {
    uint64_t tenant_id_;
    int64_t record_count_;

    TenantRpsBeforeFilterUpdater(const uint64_t tenant_id, const int64_t record_count) :
      tenant_id_(tenant_id),
      record_count_(record_count) {}

    bool operator()(const TenantID &tid, TenantStatInfo *ts_info);
  };

  // Update filtered rps information for a given tenant
  struct TenantRpsAfterFilterUpdater
  {
    uint64_t tenant_id_;
    int64_t record_count_;

    TenantRpsAfterFilterUpdater(const uint64_t tenant_id, const int64_t record_count) :
      tenant_id_(tenant_id),
      record_count_(record_count) {}

    bool operator()(const TenantID &tid, TenantStatInfo *ts_info);
  };

  struct TenantStatInfoPrinter
  {
    int64_t delta_time_;
    TenantStatInfoPrinter(const int64_t delta_time) : delta_time_(delta_time) {}

    bool operator()(const TenantID &tid, TenantStatInfo *ts_info);
  };

  struct TenantStatInfoErase
  {
    uint64_t tenant_id_;
    TenantStatInfoPool &pool_;

    TenantStatInfoErase(const uint64_t tenant_id, TenantStatInfoPool &pool) :
      tenant_id_(tenant_id), pool_(pool) {}

    bool operator()(const TenantID &tid, TenantStatInfo *ts_info);
  };

  struct TenantStatInfoClear
  {
    TenantStatInfoPool &pool_;

    TenantStatInfoClear(TenantStatInfoPool &pool) : pool_(pool) {}

    bool operator()(const TenantID &tid, TenantStatInfo *ts_info);
  };

private:
  bool                  inited_;
  // tps
  TransTpsStatInfo      tps_stat_info_ CACHE_ALIGNED;
  // rps before filter
  TransRpsStatInfo      rps_stat_info_before_filter_ CACHE_ALIGNED;
  // rps after filter
  TransRpsStatInfo      rps_stat_info_after_filter_ CACHE_ALIGNED;
  // 租户统计信息
  TenantStatInfoMap     tenant_stat_info_map_;
  TenantStatInfoPool    tenant_stat_info_pool_;
  // drc 消费统计信息
  TransTpsRpsStatInfo   next_record_stat_ CACHE_ALIGNED;     // Statistics next_record: tps and rps information
  TransTpsRpsStatInfo   release_record_stat_ CACHE_ALIGNED;  // Statistics release_record: tps and rps information
  DispatcherStatInfo    dispatcher_stat_ CACHE_ALIGNED;
  SorterStatInfo        sorter_stat_ CACHE_ALIGNED;

  // 记录统计时间
  int64_t               last_stat_time_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTransStatMgr);
};

}
}

#endif
