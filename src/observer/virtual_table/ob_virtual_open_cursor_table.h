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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_OPEN_CURSOR
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_OPEN_CURSOR
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase {
namespace common {
class ObObj;
class ObNewRow;

namespace hash {}
}  // namespace common
namespace share {
namespace schema {
class ObTableSchema;
class ObDatabaseSchema;
}  // namespace schema
}  // namespace share

namespace sql {
class ObPlanCacheManager;
class ObCacheObject;
class ObPlanCache;
class ObCacheObjectFactory;
}  // namespace sql

namespace observer {
class ObSessionOpenCursor {
public:
  explicit ObSessionOpenCursor()
  {}
  virtual ~ObSessionOpenCursor()
  {}
  bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo* sess_info);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSessionOpenCursor);
};

class ObVirtualOpenCursorTable : public common::ObVirtualTableScannerIterator {
public:
  struct SessionInfo {
    uint32_t version;  // session version
    uint32_t id;       // session id
    uint64_t addr;     // session addr
    ObString user_name;
    ObString sql_id;  // ob_max_sql_id_length;
    bool is_valid()
    {
      return 0 != addr && 0 != id;
    }
    TO_STRING_KV(K(version), K(id), K(addr), K(user_name), K(sql_id));
  };
  typedef common::ObSEArray<SessionInfo, 8> SessionInfoArray;
  typedef common::hash::ObHashMap<ObString, SessionInfoArray> SidMap;
  class ObEachSessionId {
  public:
    explicit ObEachSessionId(ObIAllocator* alloc) : is_success_(false), allocator_(alloc)
    {}
    virtual ~ObEachSessionId()
    {}
    bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo* sess_info);
    int init(int64_t sess_cnt, ObIAllocator* allocator);
    void reset();
    inline SidMap& get_sid_maps()
    {
      return sids_map_;
    }

  private:
    SidMap sids_map_;
    bool is_success_;
    ObIAllocator* allocator_;
    DISALLOW_COPY_AND_ASSIGN(ObEachSessionId);
  };

  ObVirtualOpenCursorTable();
  virtual ~ObVirtualOpenCursorTable();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  virtual int inner_open();
  void set_session_mgr(sql::ObSQLSessionMgr* sess_mgr)
  {
    sess_mgr_ = sess_mgr;
  }
  void set_plan_cache_manager(sql::ObPlanCacheManager* pcm)
  {
    pcm_ = pcm;
  }
  inline void set_tenant_id(uint64_t tid)
  {
    tenant_id_ = tid;
  }
  int set_addr(const common::ObAddr& addr);

protected:
  int fill_cells(ObNewRow*& row, bool& is_filled);
  int fill_cells_impl(const SessionInfo& sess_info, const sql::ObPhysicalPlan* plan);

private:
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    SADDR,
    SID,
    USER_NAME,
    ADDRESS,
    HASH_VALUE,
    SQL_ID,
    SQL_TEXT,
    LAST_SQL_ACTIVE_TIME,
    SQL_EXEC_ID,
  };

private:
  sql::ObSQLSessionMgr* sess_mgr_;
  sql::ObPlanCacheManager* pcm_;
  common::ObSEArray<uint64_t, 1024> plan_id_array_;
  common::ObSEArray<SessionInfo, 1024> session_info_array_;
  int64_t plan_id_array_idx_;
  sql::ObPlanCache* plan_cache_;
  const SessionInfoArray* sess_arr_;
  int64_t sess_id_array_idx_;
  ObEachSessionId oesid_;
  uint64_t tenant_id_;
  common::ObString ipstr_;
  uint32_t port_;
  bool is_travs_sess_;
  sql::ObCacheObject* cache_obj_;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualOpenCursorTable);

};  // end ObVirtualOpenCursorTable
}  // namespace observer
}  // namespace oceanbase

#endif