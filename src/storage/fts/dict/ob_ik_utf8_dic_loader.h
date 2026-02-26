/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_DICT_OB_IK_UTF8_DICT_H_
#define OCEANBASE_STORAGE_DICT_OB_IK_UTF8_DICT_H_
#include "storage/fts/dict/ob_dic_loader.h"

namespace oceanbase
{
namespace storage
{
class ObTenantIKUTF8DicLoader final : public ObTenantDicLoader
{
public:
  ObTenantIKUTF8DicLoader()  = default;
  virtual ~ObTenantIKUTF8DicLoader() = default;
  virtual int init() override;
private:
  virtual int get_dic_item(const uint64_t i, const uint64_t pos, ObDicItem& item) override;
  virtual int fill_dic_item(const ObDicItem &item, share::ObDMLSqlSplicer &dml) override;
  virtual ObDicTableInfo get_main_dic_info() override;
  virtual ObDicTableInfo get_stop_dic_info() override;
  virtual ObDicTableInfo get_quantifier_dic_info() override;
  DISALLOW_COPY_AND_ASSIGN(ObTenantIKUTF8DicLoader);
};
} //end storage
} // end oceanbase
#endif //OCEANBASE_STORAGE_DICT_OB_IK_UTF8_DICT_H_