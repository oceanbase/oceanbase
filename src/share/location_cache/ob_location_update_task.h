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

#ifndef OCEANBASE_SHARE_OB_LS_LOCATION_UPDATE_TASK
#define OCEANBASE_SHARE_OB_LS_LOCATION_UPDATE_TASK

#include "share/location_cache/ob_location_struct.h"
#include "observer/ob_uniq_task_queue.h"

namespace oceanbase
{
namespace share
{
class ObLSLocationService;
class ObTabletLSService;

class ObLSLocationUpdateTask
    : public observer::ObIUniqTaskQueueTask<ObLSLocationUpdateTask>
{
public:
  ObLSLocationUpdateTask()
      : cluster_id_(OB_INVALID_CLUSTER_ID),
        tenant_id_(OB_INVALID_TENANT_ID),
        ls_id_(),
        renew_for_tenant_(false),
        add_timestamp_(OB_INVALID_TIMESTAMP) {}
  explicit ObLSLocationUpdateTask(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const bool renew_for_tenant,
      const int64_t add_timestamp)
      : cluster_id_(cluster_id),
        tenant_id_(tenant_id),
        ls_id_(ls_id),
        renew_for_tenant_(renew_for_tenant),
        add_timestamp_(add_timestamp) {}
  virtual ~ObLSLocationUpdateTask() {}
  int init(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const bool renew_for_tenant,
      const int64_t add_timestamp);
  int assign(const ObLSLocationUpdateTask &other);
  virtual void reset();
  virtual bool is_barrier() const { return false; }
  virtual bool need_process_alone() const { return true; }
  virtual bool need_assign_when_equal() const { return false; }
  virtual bool is_valid() const;
  virtual int64_t hash() const;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual bool operator==(const ObLSLocationUpdateTask &other) const;
  virtual bool operator!=(const ObLSLocationUpdateTask &other) const;
  virtual bool compare_without_version(const ObLSLocationUpdateTask &other) const;
  virtual uint64_t get_group_id() const { return tenant_id_; }
  virtual int assign_when_equal(const ObLSLocationUpdateTask &other);

  inline int64_t get_cluster_id() const { return cluster_id_; }
  inline int64_t get_tenant_id() const { return tenant_id_; }
  inline ObLSID get_ls_id() const { return ls_id_; }
  inline int64_t get_add_timestamp() const { return add_timestamp_; }
  inline bool is_renew_for_tenant() const { return renew_for_tenant_; }

  TO_STRING_KV(K_(cluster_id), K_(tenant_id), K_(ls_id), K_(renew_for_tenant), K_(add_timestamp));
private:
  int64_t cluster_id_;
  uint64_t tenant_id_;
  ObLSID ls_id_;
  bool renew_for_tenant_; // renew all ls location caches for tenant
  int64_t add_timestamp_;
};

class ObTabletLSUpdateTask
    : public observer::ObIUniqTaskQueueTask<ObTabletLSUpdateTask>
{
public:
  ObTabletLSUpdateTask()
      : tenant_id_(OB_INVALID_TENANT_ID),
        tablet_id_(),
        add_timestamp_(OB_INVALID_TIMESTAMP) {}
  explicit ObTabletLSUpdateTask(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const int64_t add_timestamp)
      : tenant_id_(tenant_id),
        tablet_id_(tablet_id),
        add_timestamp_(add_timestamp) {}
  virtual ~ObTabletLSUpdateTask() {}
  int init(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const int64_t add_timestamp);
  int assign(const ObTabletLSUpdateTask &other);
  virtual void reset();
  virtual bool is_barrier() const { return false; }
  virtual bool need_process_alone() const { return false; }
  virtual bool need_assign_when_equal() const { return false; }
  virtual bool is_valid() const;
  virtual int64_t hash() const;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual bool operator==(const ObTabletLSUpdateTask &other) const;
  virtual bool operator!=(const ObTabletLSUpdateTask &other) const;
  virtual bool compare_without_version(const ObTabletLSUpdateTask &other) const;
  virtual uint64_t get_group_id() const { return tenant_id_; }
  virtual int assign_when_equal(const ObTabletLSUpdateTask &other);

  inline int64_t get_tenant_id() const { return tenant_id_; }
  inline ObTabletID get_tablet_id() const { return tablet_id_; }
  inline int64_t get_add_timestamp() const { return add_timestamp_; }

  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(add_timestamp));
private:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
  int64_t add_timestamp_;
};

class ObLSLocationTimerTask : public common::ObTimerTask
{
public:
  explicit ObLSLocationTimerTask(ObLSLocationService &ls_loc_service);
  virtual ~ObLSLocationTimerTask() {}
  virtual void runTimerTask() override;
private:
  ObLSLocationService &ls_loc_service_;
};

class ObLSLocationByRpcTimerTask : public common::ObTimerTask
{
public:
  explicit ObLSLocationByRpcTimerTask(ObLSLocationService &ls_loc_service);
  virtual ~ObLSLocationByRpcTimerTask() {}
  virtual void runTimerTask() override;
private:
  ObLSLocationService &ls_loc_service_;
};

class ObDumpLSLocationCacheTimerTask : public common::ObTimerTask
{
public:
  explicit ObDumpLSLocationCacheTimerTask(ObLSLocationService &ls_loc_service);
  virtual ~ObDumpLSLocationCacheTimerTask() {}
  virtual void runTimerTask() override;
private:
  ObLSLocationService &ls_loc_service_;
};

class ObVTableLocUpdateTask
    : public observer::ObIUniqTaskQueueTask<ObVTableLocUpdateTask>
{
public:
  ObVTableLocUpdateTask()
      : tenant_id_(OB_INVALID_TENANT_ID),
        table_id_(OB_INVALID_ID),
        add_timestamp_(OB_INVALID_TIMESTAMP) {}
  explicit ObVTableLocUpdateTask(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t add_timestamp)
      : tenant_id_(tenant_id),
        table_id_(table_id),
        add_timestamp_(add_timestamp) {}
  virtual ~ObVTableLocUpdateTask() {}
  int init(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t add_timestamp);
  int assign(const ObVTableLocUpdateTask &other);
  virtual void reset();
  virtual bool is_barrier() const { return false; }
  virtual bool need_process_alone() const { return true; }
  virtual bool need_assign_when_equal() const { return false; }
  virtual bool is_valid() const;
  virtual int64_t hash() const;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual bool operator==(const ObVTableLocUpdateTask &other) const;
  virtual bool operator!=(const ObVTableLocUpdateTask &other) const;
  virtual bool compare_without_version(const ObVTableLocUpdateTask &other) const;
  virtual uint64_t get_group_id() const { return tenant_id_; }
  virtual int assign_when_equal(const ObVTableLocUpdateTask &other);

  inline int64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline int64_t get_add_timestamp() const { return add_timestamp_; }

  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(add_timestamp));
private:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t add_timestamp_;
};

class ObClearTabletLSCacheTimerTask : public common::ObTimerTask
{
public:
  explicit ObClearTabletLSCacheTimerTask(ObTabletLSService &tablet_ls_service);
  virtual ~ObClearTabletLSCacheTimerTask() {}
  virtual void runTimerTask() override;
private:
  ObTabletLSService &tablet_ls_service_;
};

} // end namespace share
} // end namespace oceanbase
#endif
