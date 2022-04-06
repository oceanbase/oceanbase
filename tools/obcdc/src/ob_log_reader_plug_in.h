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

#ifndef OCEANBASE_LIBOBLOG_READER_H_
#define OCEANBASE_LIBOBLOG_READER_H_

#include "ob_log_row_data_index.h"                  // ObLogRowDataIndex
#include "ob_log_store_service_stat.h"              // StoreServiceStatInfo

namespace oceanbase
{
namespace liboblog
{
class IObStoreService;

class ObLogReader
{
public:
  ObLogReader();
  virtual ~ObLogReader();
  int init(IObStoreService &store_service);
  void destroy();

public:
  int read(ObLogRowDataIndex &row_data_index);
  StoreServiceStatInfo& get_store_stat_info() { return store_service_stat_; }

private:
  int read_store_service_(void *column_family_handle,
      ObLogRowDataIndex &row_data_index,
      std::string &key,
      std::string &value);

  // for test
  int print_serilized_br_value_(const std::string &key,
      ObLogBR &br);

private:
  bool                      inited_;
  StoreServiceStatInfo      store_service_stat_;
  IObStoreService           *store_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReader);
};

} // namespace liboblog
} // namespace oceanbase
#endif
