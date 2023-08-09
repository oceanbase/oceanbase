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

#ifndef OB_LABEL_SECURITY_H_
#define OB_LABEL_SECURITY_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{

struct ObLabelSeLabelCompNums
{
  ObLabelSeLabelCompNums()
  {
    reset();
  }
  void reset()
  {
    level_num_ = -1;
    compartments_nums_.reset();
    groups_nums_.reset();
  }

  bool is_valid()
  {
    return level_num_ >= 0;
  }
  TO_STRING_KV(K_(level_num), K_(compartments_nums), K_(groups_nums));

  bool operator <= (const ObLabelSeLabelCompNums& other)
  {
    return level_num_ <= other.level_num_;
  }

  int64_t level_num_;
  common::ObSEArray<int64_t, 10> compartments_nums_;
  common::ObSEArray<int64_t, 10> groups_nums_;
};

struct ObLabelSeDecomposedLabel
{
  ObLabelSeDecomposedLabel()
  {
    reset();
  }

  void reset()
  {
    level_.reset();
    compartments_.reset();
    groups_.reset();
  }

  int32_t calc_length() const
  {
    int32_t length = 0;

    length = level_.length();

    for (int64_t i = 0; i < compartments_.count(); ++i) {
      length += 1; //separator
      length += compartments_[i].length();
    }

    for (int64_t i = 0; i < groups_.count(); ++i) {
      length += 1; //separator
      length += groups_[i].length();
    }
    return length;
  }



  TO_STRING_KV(K_(level), K_(compartments), K_(groups));

  common::ObString level_;
  common::ObSEArray<common::ObString, 10> compartments_;
  common::ObSEArray<common::ObString, 10> groups_;
};

class ObLabelSeLabelTag {
  OB_UNIS_VERSION(1);
public:
  static const int64_t OB_INVALID_LABEL_TAG = -1;
  static const int64_t OB_UNAUTH_LABEL_TAG = INT64_MAX;
  ObLabelSeLabelTag() : label_tag_(OB_INVALID_LABEL_TAG) {}
  ObLabelSeLabelTag(int64_t label_tag) : label_tag_(label_tag) {}
  bool is_valid() const { return label_tag_ >= 0; }
  bool is_unauth() const { return label_tag_ == OB_UNAUTH_LABEL_TAG; }
  void set_unauth_value() { label_tag_ = OB_UNAUTH_LABEL_TAG; }
  void reset() { label_tag_ = OB_INVALID_LABEL_TAG; }
  int64_t get_value() const { return label_tag_; }
  void set_value(int64_t label_tag) { label_tag_ = label_tag; }


  TO_STRING_KV(K_(label_tag));
private:
  int64_t label_tag_;
};

class ObLabelSeSessionLabel
{
  OB_UNIS_VERSION(1);
public:

  ObLabelSeSessionLabel() : policy_id_(common::OB_INVALID_ID), read_label_tag_(), write_label_tag_() {}
  ObLabelSeSessionLabel(uint64_t policy_id) : policy_id_(policy_id), read_label_tag_(), write_label_tag_() {}
  ~ObLabelSeSessionLabel() {}

  void set_policy_id(uint64_t policy_id) { policy_id_ = policy_id; }
  void set_read_label_tag(ObLabelSeLabelTag new_label_tag) { read_label_tag_ = new_label_tag; }
  void set_write_label_tag(ObLabelSeLabelTag new_label_tag) { write_label_tag_ = new_label_tag; }

  uint64_t get_policy_id() const { return policy_id_; }
  ObLabelSeLabelTag &get_read_label_tag() { return read_label_tag_; }
  ObLabelSeLabelTag &get_write_label_tag() {  return write_label_tag_; }

  ObLabelSeSessionLabel &operator=(const ObLabelSeSessionLabel &other)
  {
    policy_id_ = other.policy_id_;
    read_label_tag_ = other.read_label_tag_;
    write_label_tag_ = other.write_label_tag_;
    return *this;
  }

  void reset()
  {
    policy_id_ = common::OB_INVALID_ID;
    read_label_tag_.reset();
    write_label_tag_.reset();
  }

  TO_STRING_KV(K_(policy_id), K_(read_label_tag), K_(write_label_tag));

private:
  uint64_t policy_id_;
  ObLabelSeLabelTag read_label_tag_;
  ObLabelSeLabelTag write_label_tag_;
};

class ObLabelSeUtil
{
public:
  static int convert_label_comps_name_to_num(
      uint64_t tenant_id,
      uint64_t policy_id,
      schema::ObSchemaGetterGuard &schema_guard,
      const ObLabelSeDecomposedLabel &label_comps,
      ObLabelSeLabelCompNums &label_comp_nums);
  static int convert_label_comps_num_to_name(
      uint64_t tenant_id,
      uint64_t policy_id,
      schema::ObSchemaGetterGuard &schema_guard,
      const ObLabelSeLabelCompNums &label_nums,
      ObLabelSeDecomposedLabel &label_comps);
  static int validate_user_auth(
      uint64_t tenant_id,
      uint64_t policy_id,
      uint64_t user_id,
      schema::ObSchemaGetterGuard &schema_guard,
      const ObLabelSeLabelCompNums &label_comp_nums,
      bool check_lower_bound = false);

  //static int convert_obj_to_label_tag(const common::ObObj &obj,
  //                                    schema::ObSchemaGetterGuard &schema_guard,
  //                                    ObLabelSeLabelTag &label_tag);
  static int convert_label_tag_to_column_obj(const ObLabelSeLabelTag &label_tag, common::ObObj &obj);

  static int convert_component_name_to_num(
      uint64_t tenant_id,
      uint64_t policy_id,
      schema::ObLabelSeComponentSchema::CompType comp_type,
      const common::ObString &component_name,
      schema::ObSchemaGetterGuard &schema_guard,
      int64_t &component_num);
  static int load_default_session_label(
      uint64_t tenant_id,
      uint64_t policy_id,
      uint64_t user_id,
      schema::ObSchemaGetterGuard &schema_guard,
      ObLabelSeSessionLabel &session_label);
  static int check_policy_column(uint64_t tenant_id,
                                 const common::ObString &schema_name,
                                 const common::ObString &table_name,
                                 const common::ObString &column_name,
                                 schema::ObSchemaGetterGuard &schema_guard,
                                 bool &is_policy_column_exist,
                                 bool &is_policy_already_applied_to_column);
  static int adjust_table_scan_filter(common::ObIArray<sql::ObRawExpr*> &filter_exprs);
};



class ObLabelSeResolver
{
public:
  static const char SEPARATOR_IN_LABEL_TEXT = ':';
  static const char SEPARATOR_IN_COMPONENTS = ',';

  static int resolve_label_text(const common::ObString &label_text, ObLabelSeDecomposedLabel &label_comps);
  static int construct_label_text(const ObLabelSeDecomposedLabel &label_comps,
                                  common::ObIAllocator *allocator,
                                  common::ObString &label_text);

  static int resolve_enforcement_options(schema::ObSchemaGetterGuard &schema_guard,
                                         const common::ObString &table_options,
                                         int64_t &table_option_flags_);

  static int resolve_policy_name(uint64_t tenant_id,
                                 const common::ObString &policy_name,
                                 schema::ObSchemaGetterGuard &schema_guard,
                                 uint64_t &policy_id);

  static int serialize_session_labels(const common::ObIArray<ObLabelSeSessionLabel> &labels,
                                      common::ObIAllocator &allocator,
                                      common::ObString &labels_str);
  static int deserialize_session_labels(const common::ObString &labels_str,
                                        common::ObIArray<share::ObLabelSeSessionLabel> &labels);

private:
  static ObString cut_label_text(common::ObString &label_text);
  static int resolve_components(common::ObString components, common::ObIArray<ObString> &component_array);

};



} // end namespace share
} // end namespace oceanbase

#endif // OB_LABEL_SECURITY_H_
