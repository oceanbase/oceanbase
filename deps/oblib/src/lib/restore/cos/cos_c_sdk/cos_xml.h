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

#ifndef LIBCOS_XML_H
#define LIBCOS_XML_H

#include <mxml.h>
#include "cos_string.h"
#include "cos_transport.h"
#include "cos_status.h"
#include "cos_define.h"
#include "cos_resumable.h"

COS_CPP_START

/**
  * @brief  functions for xml body parse
**/
int get_xmldoc(cos_list_t *bc, mxml_node_t **root);
char *get_xmlnode_value(cos_pool_t *p, mxml_node_t * root, const char *xml_path);

/**
  * @brief  build xml body for complete_multipart_upload
**/
char *build_complete_multipart_upload_xml(cos_pool_t *p, cos_list_t *bc);

/**
  * @brief  build body for complete multipart upload
**/
void build_complete_multipart_upload_body(cos_pool_t *p, cos_list_t *part_list, cos_list_t *body);

/**
  * @brief  build xml body for put lifecycle
**/
char *build_lifecycle_xml(cos_pool_t *p, cos_list_t *lifecycle_rule_list);

/**
  * @brief  build body for put lifecycle
**/
void build_lifecycle_body(cos_pool_t *p, cos_list_t *lifecycle_rule_list, cos_list_t *body);

/**
  * @brief  build xml body for put cors
**/
char *build_cors_xml(cos_pool_t *p, cos_list_t *cors_rule_list);

/**
  * @brief  build body for put cors
**/
void build_cors_body(cos_pool_t *p, cos_list_t *cors_rule_list, cos_list_t *body);

/**
  * @brief  build xml body for put bucket replication
**/
char *build_replication_xml(cos_pool_t *p, cos_replication_params_t *replication_param);

/**
  * @brief  build body for put bucket replication
**/
void build_replication_body(cos_pool_t *p, cos_replication_params_t *replication_param, cos_list_t *body);

/**
  * @brief  build body for put bucket versioning
**/
void build_versioning_body(cos_pool_t *p, cos_versioning_content_t *versioning, cos_list_t *body);

/**
  * @brief  build xml body for put bucket versioning
**/
char *build_versioning_xml(cos_pool_t *p, cos_versioning_content_t *versioning);

/**
  * @brief  build a xml node
  * eg: <xml>param->data</xml>
**/
void build_xml_node(mxml_node_t *pnode, const char *xml, cos_string_t *param);

/** @brief  build a xml node with parent.
  * eg: <pxml><cxml>pamam->data</cxml></pxml>
**/
void build_xml_node_with_parent(mxml_node_t *root, const char *pxml, const char *cxml, cos_string_t *param);

/**
  * @brief  build body for put bucket website
**/
void build_website_body(cos_pool_t *p, cos_website_params_t *website_params, cos_list_t *body);

/**
  * @brief  build xml body for put bucket website
**/
char *build_website_xml(cos_pool_t *p, cos_website_params_t *website_params);

/**
  * @brief  build body for put bucket domain
**/
void build_domain_body(cos_pool_t *p, cos_domain_params_t *domain_params, cos_list_t *body);

/**
  * @brief  build xml body for put bucket domain
**/
char *build_domain_xml(cos_pool_t *p, cos_domain_params_t *domain_params);

/**
 *  @brief  build body for put bucket logging
 */
void build_logging_body(cos_pool_t *p, cos_logging_params_t *params, cos_list_t *body);
char *build_logging_xml(cos_pool_t *p, cos_logging_params_t *params);

/**
 *  @brief  build body for put bucket logging
 */
void build_inventory_body(cos_pool_t *p, cos_inventory_params_t *params, cos_list_t *body);
char *build_inventory_xml(cos_pool_t *p, cos_inventory_params_t *params);

void build_tagging_body(cos_pool_t *p, cos_tagging_params_t *params, cos_list_t *body);
char *build_tagging_xml(cos_pool_t *p, cos_tagging_params_t *params);


void build_intelligenttiering_body(cos_pool_t *p, cos_intelligenttiering_params_t *params, cos_list_t *body);
char *build_intelligenttiering_xml(cos_pool_t *p, cos_intelligenttiering_params_t *params);

void build_object_restore_body(cos_pool_t *p, cos_object_restore_params_t *params, cos_list_t *body);

/**
  * @brief  build xml body for delete objects
**/
char *build_objects_xml(cos_pool_t *p, cos_list_t *object_list, const char *quiet);

/**
  * @brief  build body for delete objects
**/
void build_delete_objects_body(cos_pool_t *p, cos_list_t *object_list, int is_quiet,
            cos_list_t *body);

mxml_node_t *set_xmlnode_value_str(mxml_node_t *parent, const char *name, const cos_string_t *value);
mxml_node_t *set_xmlnode_value_int(mxml_node_t *parent, const char *name, int value);
mxml_node_t *set_xmlnode_value_int64(mxml_node_t *parent, const char *name, int64_t value);

int get_xmlnode_value_str(cos_pool_t *p, mxml_node_t *xml_node, const char *xml_path, cos_string_t *value);
int get_xmlnode_value_int(cos_pool_t *p, mxml_node_t *xml_node, const char *xml_path, int *value);
int get_xmlnode_value_int64(cos_pool_t *p, mxml_node_t *xml_node, const char *xml_path, int64_t *value);

/**
  * @brief  build xml for checkpoint
**/
char *cos_build_checkpoint_xml(cos_pool_t *p, const cos_checkpoint_t *checkpoint);

/**
  * @bried  parse checkpoint from xml
**/
int cos_checkpoint_parse_from_body(cos_pool_t *p, const char *xml_body, cos_checkpoint_t *checkpoint);

/**
  * @bried  parse acl from xml body for get_bucket_acl
**/
int cos_acl_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_acl_params_t *content);

/**
  * @bried  parse acl from xml body for get_bucket_acl
**/
void cos_acl_grantee_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_acl_grantee_content_t *content);

/**
  * @bried  parse acl from xml body for get_bucket_acl
**/
void cos_acl_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path, cos_list_t *acl_list);

/**
  * @bried  parse acl from xml body for get_bucket_acl
**/
void cos_acl_owner_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_acl_params_t *content);

/**
  * @bried  parse result from xml body for copy_object
**/
int cos_copy_object_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_copy_object_params_t *content);

/**
  * @brief parse upload_id from xml body for init multipart upload
**/
int cos_upload_id_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_string_t *upload_id);

/**
 *  @brief parse buckets list from xml body for get services
**/

int cos_get_service_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_get_service_params_t *params);

/**
  * @brief parse objects from xml body for list objects
**/
void cos_list_objects_owner_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_list_object_content_t *content);
void cos_list_objects_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_list_object_content_t *content);
void cos_list_objects_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
            cos_list_t *object_list);
void cos_list_objects_prefix_parse(cos_pool_t *p, mxml_node_t *root,
            cos_list_object_common_prefix_t *common_prefix);
void cos_list_objects_common_prefix_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
            cos_list_t *common_prefix_list);
int cos_list_objects_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *object_list,
            cos_list_t *common_prefix_list, cos_string_t *marker, int *truncated);

/**
  * @brief parse parts from xml body for list upload part
**/
void cos_list_parts_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
            cos_list_t *part_list);
void cos_list_parts_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_list_part_content_t *content);
int cos_list_parts_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *part_list,
            cos_string_t *part_number_marker, int *truncated);

/**
  * @brief  parse uploads from xml body for list multipart upload
**/
void cos_list_multipart_uploads_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
            cos_list_t *upload_list);
void cos_list_multipart_uploads_content_parse(cos_pool_t *p, mxml_node_t *xml_node,
            cos_list_multipart_upload_content_t *content);
int cos_list_multipart_uploads_parse_from_body(cos_pool_t *p, cos_list_t *bc,
            cos_list_t *upload_list, cos_string_t *key_marker,
            cos_string_t *upload_id_marker, int *truncated);

/**
  * @brief parse cors rules from xml body
**/
int cos_cors_rules_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *cors_rule_list);
void cos_cors_rule_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path, cos_list_t *cors_rule_list);
void cos_cors_rule_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_cors_rule_content_t *content);

/**
  * @brief parse bucket replication rules from xml body
**/
void cos_replication_rule_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_replication_rule_content_t *content);
void cos_replication_rules_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path, cos_list_t *rule_list);
int cos_replication_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_replication_params_t *content);

/**
  * @brief parse versioning from body
**/
int cos_versioning_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_versioning_content_t *versioning);

/**
  * @brief parse lifecycle rules from xml body
**/
void cos_lifecycle_rule_expire_parse(cos_pool_t *p, mxml_node_t *xml_node,
    cos_lifecycle_rule_content_t *content);
void cos_lifecycle_rule_transition_parse(cos_pool_t *p, mxml_node_t *xml_node,
    cos_lifecycle_rule_content_t *content);
void cos_lifecycle_rule_abort_parse(cos_pool_t *p, mxml_node_t *xml_node,
    cos_lifecycle_rule_content_t *content);
void cos_lifecycle_rule_content_parse(cos_pool_t *p, mxml_node_t *xml_node,
    cos_lifecycle_rule_content_t *content);
void cos_lifecycle_rule_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *lifecycle_rule_list);
int cos_lifecycle_rules_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *lifecycle_rule_list);

/**
  * @brief parse from a xml node.
  * eg: <xml>text</xml>
**/
void cos_common_parse_from_xml_node(cos_pool_t *p, mxml_node_t *pnode, mxml_node_t *root, const char *xml, cos_string_t *param);

/**
  * @brief parse from a parent xml node.
  * eg: <pxml><cxml>test<cxml></pxml>
**/
void cos_common_parse_from_parent_node(cos_pool_t *p, mxml_node_t *root, const char *pxml, const char *cxml, cos_string_t *param);

/**
  * @brief parse website from body
**/
int cos_get_website_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_website_params_t *website);

/**
  * @brief parse domain form body
**/
int cos_get_domain_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_domain_params_t *domain);

/**
 *  @brief parse logging from body
 */
int cos_get_logging_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_logging_params_t *logging);

//  @brief parse inventory from body
int cos_get_inventory_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_inventory_params_t *params);
int cos_list_inventory_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_inventory_params_t *params);

int cos_get_tagging_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_tagging_params_t *params);

int cos_get_intelligenttiering_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_intelligenttiering_params_t *params);

/**
  * @brief parse delete objects contents from xml body
**/
void cos_delete_objects_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *object_list);
void cos_object_key_parse(cos_pool_t *p, mxml_node_t * xml_node, cos_object_key_t *content);
int cos_delete_objects_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *object_list);

/**
  * @brief  build body for create live channel
**/
char *build_create_live_channel_xml(cos_pool_t *p, cos_live_channel_configuration_t *config);
void build_create_live_channel_body(cos_pool_t *p, cos_live_channel_configuration_t *config, cos_list_t *body);

/**
  * @brief parse create live channel contents from xml body
**/
void cos_publish_url_parse(cos_pool_t *p, mxml_node_t *node, cos_live_channel_publish_url_t *content);
void cos_play_url_parse(cos_pool_t *p, mxml_node_t *node, cos_live_channel_play_url_t *content);
void cos_publish_urls_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
	cos_list_t *publish_xml_list);
void cos_play_urls_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
	cos_list_t *play_xml_list);
void cos_create_live_channel_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *publish_xml_path,
	cos_list_t *publish_url_list, const char *play_xml_path, cos_list_t *play_url_list);
int cos_create_live_channel_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *publish_url_list,
	cos_list_t *play_url_list);

/**
  * @brief parse live channel info content from xml body
**/
void cos_live_channel_info_target_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_live_channel_target_t *target);
void cos_live_channel_info_content_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_live_channel_configuration_t *info);
int cos_live_channel_info_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_live_channel_configuration_t *info);

/**
  * @brief parse live channel stat content from xml body
**/
void cos_live_channel_stat_video_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_video_stat_t *video_stat);
void cos_live_channel_stat_audio_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_audio_stat_t *audio_stat);
void cos_live_channel_stat_content_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path, cos_live_channel_stat_t *stat);
int cos_live_channel_stat_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_live_channel_stat_t *stat);

/**
  * @brief parse live channel from xml body for list list channel
**/
void cos_list_live_channel_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_live_channel_content_t *content);
void cos_list_live_channel_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *live_channel_list);
int cos_list_live_channel_parse_from_body(cos_pool_t *p, cos_list_t *bc,
    cos_list_t *live_channel_list, cos_string_t *next_marker, int *truncated);

/**
  * @brief parse live channel history content from xml body
**/
void cos_live_channel_history_content_parse(cos_pool_t *p, mxml_node_t * xml_node, cos_live_record_content_t *content);
void cos_live_channel_history_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *live_record_list);
int cos_live_channel_history_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *live_record_list);

COS_CPP_END

#endif
