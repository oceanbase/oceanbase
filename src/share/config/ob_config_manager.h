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

#ifndef OCEANBASE_SHARE_CONFIG_OB_CONFIG_MANAGER_H_
#define OCEANBASE_SHARE_CONFIG_OB_CONFIG_MANAGER_H_

#include "lib/thread/thread_mgr_interface.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_system_config.h"
#include "share/config/ob_reload_config.h"

// 去掉代码改动较大, 先保留

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;

class ObConfigManager
{
  friend class UpdateTask;
public:
  static const int64_t DEFAULT_VERSION = 1;

  ObConfigManager(ObServerConfig &server_config, ObReloadConfig &reload_config);
  virtual ~ObConfigManager();

  // get newest version received
  const int64_t &get_version() const;
  // config version current used
  int64_t get_current_version() const;

  int base_init();

  int init(const ObAddr &server);
  int init(ObMySQLProxy &sql_proxy, const ObAddr &server);
  void stop();
  void wait();
  void destroy();

  int check_header_change(const char* path, const char* buf) const;
  // manual dump to file named by path
  int dump2file(const char *path = NULL) const;

  // set dump path (filename) for autodump
  void set_dump_path(const char *path);

  // This function should been invoked after @base_init
  int load_config(const char *path = NULL);

  // Reload config really
  int reload_config();

  ObServerConfig &get_config(void);

  int config_backup();
  int update_local(int64_t expected_version);
  virtual int got_version(int64_t version, const bool remove_repeat = false);

private:
  class UpdateTask
    : public ObTimerTask
  {
  public:
    UpdateTask()
        : config_mgr_(NULL), update_local_(false), version_(0), scheduled_time_(0)
    {}
    virtual ~UpdateTask() {}
    // main routine
    void runTimerTask(void);
    ObConfigManager *config_mgr_;
    bool update_local_;
    volatile int64_t version_;
    volatile int64_t scheduled_time_;
  private:
    DISALLOW_COPY_AND_ASSIGN(UpdateTask);
  };

private:
  bool inited_;
  bool init_config_load_; //
  UpdateTask update_task_; // Update local config from internal table
  ObAddr self_;
  ObMySQLProxy *sql_proxy_;
  ObSystemConfig system_config_;
  ObServerConfig &server_config_;
  int64_t current_version_;
  char dump_path_[OB_MAX_FILE_NAME_LENGTH];
  ObReloadConfig &reload_config_func_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigManager);
};

inline ObConfigManager::ObConfigManager(ObServerConfig &server_config,
                                        ObReloadConfig &reload_config)
    : inited_(false),
      init_config_load_(false),
      update_task_(),
      self_(),
      sql_proxy_(NULL),
      system_config_(),
      server_config_(server_config),
      current_version_(1),
      reload_config_func_(reload_config)
{
  dump_path_[0] = '\0';
}

inline int64_t ObConfigManager::get_current_version() const
{
  return current_version_;
}

inline ObServerConfig &ObConfigManager::get_config(void)
{
  return server_config_;
}

inline void ObConfigManager::set_dump_path(const char *path)
{
  snprintf(dump_path_, OB_MAX_FILE_NAME_LENGTH, "%s", path);
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_CONFIG_MANAGER_H_
