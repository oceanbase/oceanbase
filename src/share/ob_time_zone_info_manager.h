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

#ifndef OCEANBASE_SHARE_OB_TIME_ZONE_INFO_MGR_H
#define OCEANBASE_SHARE_OB_TIME_ZONE_INFO_MGR_H

#include "lib/hash/ob_hashmap.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/net/ob_addr.h"
#include "lib/timezone/ob_timezone_info.h"
namespace oceanbase {
namespace rootserver {
class ObRootService;
}
namespace common {
class ObISQLClient;
namespace sqlclient {
class ObMySQLResult;
}
class ObMySQLProxy;
}  // namespace common
namespace obrpc {
class ObCommonRpcProxy;
}
namespace common {

class ObRequestTZInfoArg {
  OB_UNIS_VERSION(1);

public:
  explicit ObRequestTZInfoArg(const common::ObAddr& addr, uint64_t tenant_id) : obs_addr_(addr), tenant_id_(tenant_id)
  {}
  ObRequestTZInfoArg() : obs_addr_()
  {}
  ~ObRequestTZInfoArg()
  {}

public:
  common::ObAddr obs_addr_;
  uint64_t tenant_id_;
};

class ObRequestTZInfoResult {
  OB_UNIS_VERSION(1);

public:
  ObRequestTZInfoResult() : last_version_(-1), tz_array_()
  {}
  ~ObRequestTZInfoResult()
  {}

public:
  int64_t last_version_;
  common::ObSArray<ObTimeZoneInfoPos> tz_array_;
};

class ObTZAbbrIDStruct;
class ObTZAbbrNameStruct;
class ObTimeZoneInfoManager {
  const int64_t TZ_INFO_BUCKET_NUM = 600;
  const int64_t TASK_THREAD_NUM = 1;
  const int64_t TASK_NUM_LIMIT = 512;
  static const char* UPDATE_TZ_INFO_VERSION_SQL;

private:
  class TaskProcessThread : public common::ObSimpleThreadPool {
  public:
    virtual void handle(void* task);
  };

  class TZInfoTask {
  public:
    explicit TZInfoTask(ObTimeZoneInfoManager& tz_mgr) : tz_mgr_(tz_mgr)
    {}
    virtual ~TZInfoTask()
    {}
    virtual int run_task() = 0;

  protected:
    ObTimeZoneInfoManager& tz_mgr_;

  private:
    DISALLOW_COPY_AND_ASSIGN(TZInfoTask);
  };

  class FillRequestTZInfoResult {
  public:
    FillRequestTZInfoResult(ObRequestTZInfoResult& tz_result) : tz_result_(tz_result)
    {}
    bool operator()(ObTZIDKey key, ObTimeZoneInfoPos* tz_info);

  private:
    ObRequestTZInfoResult& tz_result_;
  };

public:
  ObTimeZoneInfoManager(obrpc::ObCommonRpcProxy& rs_rpc_proxy, common::ObMySQLProxy& sql_proxy,
      rootserver::ObRootService& root_service, ObTZInfoMap& tz_info_map, int64_t tenant_id)
      : rs_rpc_proxy_(rs_rpc_proxy),
        sql_proxy_(sql_proxy),
        root_service_(root_service),
        tz_info_map_(tz_info_map),
        inited_(false),
        is_usable_(false),
        last_version_(-1),
        tenant_id_(tenant_id)
  {}
  ~ObTimeZoneInfoManager()
  {}
  int init();
  int is_usable() const
  {
    return is_usable_;
  }
  void set_usable()
  {
    is_usable_ = true;
  }
  // rs fetch tz_info from time_zone tables
  int fetch_time_zone_info();
  int response_time_zone_info(ObRequestTZInfoResult& tz_result);
  int update_sys_time_zone_info_version();
  int update_time_zone_info(int64_t tz_info_version);
  int get_time_zone();
  int find_time_zone_info(const common::ObString& tz_name, ObTimeZoneInfoPos& tz_info);
  void free_tz_info_pos(ObTimeZoneInfoPos*& tz_info)
  {
    tz_info_map_.free_tz_info_pos(tz_info);
  }
  int64_t get_version() const
  {
    return last_version_;
  }
  const ObTZInfoMap* get_tz_info_map() const
  {
    return &tz_info_map_;
  }
  const ObTZInfoNameIDMap* get_tz_info_name_map() const
  {
    return &tz_info_map_.name_map_;
  }

  static const char* FETCH_TZ_INFO_SQL;
  static const char* FETCH_TENANT_TZ_INFO_SQL;
  static const char* FETCH_LATEST_TZ_VERSION_SQL;
  static int fill_tz_info_map(common::sqlclient::ObMySQLResult& result, ObTZInfoMap& tz_info_map);

private:
  int fetch_time_zone_info_from_tenant_table(const int64_t current_tz_version);
  int fetch_time_zone_info_from_sys_table();
  static int calc_default_tran_type(
      const common::ObIArray<ObTZTransitionTypeInfo>& types_with_null, ObTimeZoneInfoPos& type_info);
  static int prepare_tz_info(
      const common::ObIArray<ObTZTransitionTypeInfo>& types_with_null, ObTimeZoneInfoPos& type_info);
  static int set_tz_info_map(
      ObTimeZoneInfoPos*& stored_tz_info, ObTimeZoneInfoPos& new_tz_info, ObTZInfoMap& tz_info_map);
  int fill_tz_info_map(ObRequestTZInfoResult& tz_result);
  int print_tz_info_map();

private:
  obrpc::ObCommonRpcProxy& rs_rpc_proxy_;
  common::ObMySQLProxy& sql_proxy_;
  rootserver::ObRootService& root_service_;
  ObTZInfoMap& tz_info_map_;
  bool inited_;

  // server start to provide service when is_usable_ == true. is_usable_ is set true when:
  // 1. time_zone_info_version == 0 in __all_zone, when get heartbeat first time.
  // 2. time_zone_info_version > 0 __all_zone, when refresh timezone_info.
  volatile bool is_usable_;
  int64_t last_version_;
  // record tenant_id_ for getting tz_info_version.
  int64_t tenant_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTimeZoneInfoManager);
};

}  // namespace common
}  // namespace oceanbase
#endif
