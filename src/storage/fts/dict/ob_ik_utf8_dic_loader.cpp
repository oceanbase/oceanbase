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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ik_utf8_dic_loader.h"

#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/fts/dict/ob_ik_dic.h"

namespace oceanbase
{
namespace storage
{
int ObTenantIKUTF8DicLoader::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the dic loader initialize twice", K(ret));
  } else if (OB_FAIL(dic_tables_info_.push_back(get_main_dic_info()))
             || OB_FAIL(dic_tables_info_.push_back(get_stop_dic_info()))
             || OB_FAIL(dic_tables_info_.push_back(get_quantifier_dic_info()))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTenantIKUTF8DicLoader::get_dic_item(const uint64_t table_info_pos,
                                          const uint64_t data_pos,
                                          ObDicItem &item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_info_pos >= get_dic_tables_info().count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table info pos is too large",
             K(ret),
             K(table_info_pos),
             K(get_dic_tables_info().count()));
  } else {
    const uint64_t array_size = get_dic_tables_info().at(table_info_pos).array_size_;
    const char **raw_data = get_dic_tables_info().at(table_info_pos).raw_data_;
    if (data_pos >= array_size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("data pos is too large", K(ret), K(data_pos), K(array_size));
    } else {
      item.word_ = raw_data[data_pos];
    }
  }
  return ret;
}

int ObTenantIKUTF8DicLoader::fill_dic_item(const ObDicItem &item, share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_column("word", item.word_))) {
    LOG_WARN("fail to add column", K(ret));
  }
  return ret;
}

ObTenantDicLoader::ObDicTableInfo ObTenantIKUTF8DicLoader::get_main_dic_info()
{
  static const ObIKDictLoader::RawDict main_dict = ObIKDictLoader::dict_text();

  return ObDicTableInfo(main_dict.data_,
                        main_dict.array_size_,
                        share::OB_FT_DICT_IK_UTF8_TNAME,
                        share::OB_FT_DICT_IK_UTF8_TID);
}

ObTenantDicLoader::ObDicTableInfo ObTenantIKUTF8DicLoader::get_stop_dic_info()
{
  static const ObIKDictLoader::RawDict stop_dict = ObIKDictLoader::dict_stop();
  return ObDicTableInfo(stop_dict.data_,
                        stop_dict.array_size_,
                        share::OB_FT_STOPWORD_IK_UTF8_TNAME,
                        share::OB_FT_STOPWORD_IK_UTF8_TID);
}

ObTenantDicLoader::ObDicTableInfo ObTenantIKUTF8DicLoader::get_quantifier_dic_info()
{
  static const ObIKDictLoader::RawDict quan_dict = ObIKDictLoader::dict_quen_text();
  return ObDicTableInfo(quan_dict.data_,
                        quan_dict.array_size_,
                        share::OB_FT_QUANTIFIER_IK_UTF8_TNAME,
                        share::OB_FT_QUANTIFIER_IK_UTF8_TID);
}
} // namespace storage
} // namespace oceanbase