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

#include "cos_string.h"
#include "cos_list.h"
#include "cos_buf.h"
#include "cos_sys_util.h"
#include "cos_log.h"
#include "cos_status.h"
#include "cos_utility.h"
#include "cos_auth.h"
#include "cos_xml.h"
#include "cos_define.h"

static int get_truncated_from_xml(cos_pool_t *p, mxml_node_t *xml_node, const char *truncated_xml_path);

int get_truncated_from_xml(cos_pool_t *p, mxml_node_t *xml_node, const char *truncated_xml_path)
{
    char *is_truncated;
    int truncated = 0;
    is_truncated = get_xmlnode_value(p, xml_node, truncated_xml_path);
    if (is_truncated) {
        truncated = strcasecmp(is_truncated, "false") == 0 ? 0 : 1;
    }
    return truncated;
}

static char* new_xml_buff(mxml_node_t *doc);

char* new_xml_buff(mxml_node_t *doc)
{
    int	bytes;
    char buffer[8192];
    char *s;

    bytes = mxmlSaveString(doc, buffer, sizeof(buffer), MXML_NO_CALLBACK);
    if (bytes <= 0) {
        return (NULL);
    }

    if (bytes < (int)(sizeof(buffer) - 1)) {
        return (strdup(buffer));
    }

    if ((s = malloc(bytes + 1)) == NULL) {
        return (NULL);
    }
    mxmlSaveString(doc, s, bytes + 1, MXML_NO_CALLBACK);

    return (s);
}

int get_xmldoc(cos_list_t *bc, mxml_node_t **root)
{
    int res;

    if (cos_list_empty(bc)) {
        return COSE_XML_PARSE_ERROR;
    }

    if ((res = cos_parse_xml_body(bc, root)) != COSE_OK) {
        return COSE_XML_PARSE_ERROR;
    }

    return COSE_OK;
}

char *get_xmlnode_value(cos_pool_t *p, mxml_node_t *xml_node, const char *xml_path)
{
    char *value = NULL;
    mxml_node_t *node;
    const char *node_content;

    node = mxmlFindElement(xml_node, xml_node, xml_path, NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        value = apr_pstrdup(p, node_content);
    }

    return value;
}

void cos_get_service_owner_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_get_service_params_t *params)
{
    const char *content;
    char *owner_id;
    char *owner_display_name;
    mxml_node_t *node = NULL;

    node = mxmlFindElement(xml_node, xml_node, "ID", NULL, NULL, MXML_DESCEND);
    content = mxmlGetOpaque(node);
    if (content != NULL) {
        owner_id = apr_pstrdup(p, content);
        cos_str_set(&params->owner_id, owner_id);
    }

    node = mxmlFindElement(xml_node, xml_node, "DisplayName", NULL, NULL, MXML_DESCEND);
    content = mxmlGetOpaque(node);
    if (content != NULL) {
        owner_display_name = apr_pstrdup(p, content);
        cos_str_set(&params->owner_display_name, owner_display_name);
    }
}

void cos_get_service_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_get_service_content_t *bucket_content)
{
    const char *content = NULL;
    char *tmp_point = NULL;
    mxml_node_t *node = NULL;

    node = mxmlFindElement(xml_node, xml_node, "Name", NULL, NULL, MXML_DESCEND);
    content = mxmlGetOpaque(node);
    if (content != NULL) {
        tmp_point = apr_pstrdup(p, content);
        cos_str_set(&bucket_content->bucket_name, tmp_point);
    }

    node = mxmlFindElement(xml_node, xml_node, "Location", NULL, NULL, MXML_DESCEND);
    content = mxmlGetOpaque(node);
    if (content != NULL) {
        tmp_point = apr_pstrdup(p, content);
        cos_str_set(&bucket_content->location, tmp_point);
    }

    node = mxmlFindElement(xml_node, xml_node, "CreationDate", NULL, NULL, MXML_DESCEND);
    content = mxmlGetOpaque(node);
    if (content != NULL) {
        tmp_point = apr_pstrdup(p, content);
        cos_str_set(&bucket_content->creation_date, tmp_point);
    }
}

void cos_get_service_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *path, cos_list_t *bucket_list)
{
    cos_get_service_content_t *content = NULL;
    mxml_node_t *xml_node = NULL;
    mxml_node_t *node = NULL;
    const char bucket_content_path[] = "Bucket";

    //查找Buckets节点
    xml_node = mxmlFindElement(root, root, path, NULL, NULL, MXML_DESCEND);
    if (xml_node == NULL) {
        return;
    }

    //查找Bucket节点
    node = mxmlFindElement(xml_node, xml_node, bucket_content_path, NULL, NULL, MXML_DESCEND);
    while (node != NULL) {
        content = cos_create_get_service_content(p);
        cos_get_service_content_parse(p, node, content);
        cos_list_add_tail(&content->node, bucket_list);
        node = mxmlFindElement(node, xml_node, bucket_content_path, NULL, NULL, MXML_DESCEND);
    }
}

int cos_get_service_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_get_service_params_t *params)
{
    int res;
    mxml_node_t *root;
    mxml_node_t *node;
    static const char owner_path[] = "Owner";
    static const char buckets_xml_path[] = "Buckets";

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {

        node = mxmlFindElement(root, root, owner_path, NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            cos_get_service_owner_parse(p, node, params);
        }

        cos_get_service_contents_parse(p, root, buckets_xml_path, &params->bucket_list);

        mxmlDelete(root);
    }

    return res;
}

void cos_acl_grantee_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_acl_grantee_content_t *content)
{
    char *id;
    char *name;
    char *permission;
    const char *node_content;
    char *type = NULL;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Grantee", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        const char *attr = mxmlElementGetAttr(node, "xsi:type");
        type = apr_pstrdup(p, (char *)attr);
        cos_str_set(&content->type, type);
    }

    node = mxmlFindElement(xml_node, xml_node, "ID", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        id = apr_pstrdup(p, node_content);
        cos_str_set(&content->id, id);
    } else {
        node = mxmlFindElement(xml_node, xml_node, "URI", NULL, NULL, MXML_DESCEND);
        node_content = mxmlGetOpaque(node);
        if (node_content != NULL) {
            id = apr_pstrdup(p, node_content);
            cos_str_set(&content->id, id);
        }
    }

    node = mxmlFindElement(xml_node, xml_node, "DisplayName", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        name = apr_pstrdup(p, node_content);
        cos_str_set(&content->name, name);
    }

    node = mxmlFindElement(xml_node, xml_node, "Permission", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        permission = apr_pstrdup(p, node_content);
        cos_str_set(&content->permission, permission);
    }

}

void cos_acl_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path, cos_list_t *acl_list)
{
    mxml_node_t *content_node;
    cos_acl_grantee_content_t *content;

    content_node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; content_node != NULL; ) {
        content = cos_create_acl_list_content(p);
        cos_acl_grantee_content_parse(p, content_node, content);
        cos_list_add_tail(&content->node, acl_list);
        content_node = mxmlFindElement(content_node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_acl_owner_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_acl_params_t *content)
{
    mxml_node_t *node;
    const char *node_content;
    char *owner_id;
    char *owner_name;

    node = mxmlFindElement(xml_node, xml_node, "ID",NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        owner_id = apr_pstrdup(p, node_content);
        cos_str_set(&content->owner_id, owner_id);
    }

    node = mxmlFindElement(xml_node, xml_node, "DisplayName", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        owner_name = apr_pstrdup(p, node_content);
        cos_str_set(&content->owner_name, owner_name);
    }
}

int cos_acl_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_acl_params_t *content)
{
    int res;
    mxml_node_t *root;
    mxml_node_t *node;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        node = mxmlFindElement(root, root, "Owner", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            cos_acl_owner_parse(p, node, content);
        }

        node = mxmlFindElement(root, root, "AccessControlList", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            cos_acl_contents_parse(p, node, "Grant", &content->grantee_list);
        }

        mxmlDelete(root);
    }

    return res;
}

void cos_replication_rule_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_replication_rule_content_t *content)
{
    char *status;
    char *id;
    char *prefix;
    char *bucket;
    char *storage_class;
    const char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Status", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        status = apr_pstrdup(p, node_content);
        cos_str_set(&content->status, status);
    }

    node = mxmlFindElement(xml_node, xml_node, "ID", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        id = apr_pstrdup(p, node_content);
        cos_str_set(&content->id, id);
    }

    node = mxmlFindElement(xml_node, xml_node, "Prefix", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        prefix = apr_pstrdup(p, node_content);
        cos_str_set(&content->prefix, prefix);
    }

    node = mxmlFindElement(xml_node, xml_node, "Bucket", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        bucket = apr_pstrdup(p, node_content);
        cos_str_set(&content->dst_bucket, bucket);
    }

    node = mxmlFindElement(xml_node, xml_node, "StorageClass", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        storage_class = apr_pstrdup(p, node_content);
        cos_str_set(&content->storage_class, storage_class);
    }

}

void cos_replication_rules_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path, cos_list_t *rule_list)
{
    mxml_node_t *content_node;
    cos_replication_rule_content_t *content;

    content_node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; content_node != NULL; ) {
        content = cos_create_replication_rule_content(p);
        cos_replication_rule_parse(p, content_node, content);
        cos_list_add_tail(&content->node, rule_list);
        content_node = mxmlFindElement(content_node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

int cos_replication_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_replication_params_t *content)
{
    int res;
    mxml_node_t *root;
    mxml_node_t *node;
    const char *node_content;
    char *role;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        node = mxmlFindElement(root, root, "Role", NULL, NULL, MXML_DESCEND);
        node_content = mxmlGetOpaque(node);
        if (node_content != NULL) {
            role = apr_pstrdup(p, node_content);
            cos_str_set(&content->role, role);
        }

        cos_replication_rules_parse(p, root, "Rule", &content->rule_list);

        mxmlDelete(root);
    }

    return res;
}

int cos_copy_object_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_copy_object_params_t *content)
{
    int res;
    mxml_node_t *root;
    mxml_node_t *node;
    const char *node_content;
    char *etag;
    char *last_modify;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        node = mxmlFindElement(root, root, "ETag", NULL, NULL, MXML_DESCEND);
        node_content = mxmlGetOpaque(node);
        if (node_content != NULL) {
            etag = apr_pstrdup(p, node_content);
            cos_str_set(&content->etag, etag);
        }

        node = mxmlFindElement(root, root, "LastModified", NULL, NULL, MXML_DESCEND);
        node_content = mxmlGetOpaque(node);
        if (node_content != NULL) {
            last_modify = apr_pstrdup(p, node_content);
            cos_str_set(&content->last_modify, last_modify);
        }

        mxmlDelete(root);
    }

    return res;
}

void cos_list_objects_owner_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_list_object_content_t *content)
{
    mxml_node_t *node;
    const char *node_content;
    char *owner_id;
    char *owner_display_name;

    node = mxmlFindElement(xml_node, xml_node, "ID",NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        owner_id = apr_pstrdup(p, node_content);
        cos_str_set(&content->owner_id, owner_id);
    }

    node = mxmlFindElement(xml_node, xml_node, "DisplayName", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        owner_display_name = apr_pstrdup(p, node_content);
        cos_str_set(&content->owner_display_name, owner_display_name);
    }
}

void cos_list_objects_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_list_object_content_t *content)
{
    char *key;
    char *last_modified;
    char *etag;
    char *size;
    const char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Key", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        key = apr_pstrdup(p, node_content);
        cos_str_set(&content->key, key);
    }

    node = mxmlFindElement(xml_node, xml_node, "LastModified", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        last_modified = apr_pstrdup(p, node_content);
        cos_str_set(&content->last_modified, last_modified);
    }

    node = mxmlFindElement(xml_node, xml_node, "ETag", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        etag = apr_pstrdup(p, node_content);
        cos_str_set(&content->etag, etag);
    }

    node = mxmlFindElement(xml_node, xml_node, "Size", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        size = apr_pstrdup(p, node_content);
        cos_str_set(&content->size, size);
    }

    node = mxmlFindElement(xml_node, xml_node, "Owner", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        cos_list_objects_owner_parse(p, node, content);
    }

    node = mxmlFindElement(xml_node, xml_node, "StorageClass", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        etag = apr_pstrdup(p, node_content);
        cos_str_set(&content->storage_class, etag);
    }
}

void cos_list_objects_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *object_list)
{
    mxml_node_t *content_node;
    cos_list_object_content_t *content;

    content_node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; content_node != NULL; ) {
        content = cos_create_list_object_content(p);
        cos_list_objects_content_parse(p, content_node, content);
        cos_list_add_tail(&content->node, object_list);
        content_node = mxmlFindElement(content_node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_list_objects_prefix_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_list_object_common_prefix_t *common_prefix)
{
    char *prefix;
    mxml_node_t *node;
    const char *node_content;

    node = mxmlFindElement(xml_node, xml_node, "Prefix", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        prefix = apr_pstrdup(p, node_content);
        cos_str_set(&common_prefix->prefix, prefix);
    }
}

void cos_list_objects_common_prefix_parse(cos_pool_t *p, mxml_node_t *xml_node, const char *xml_path,
            cos_list_t *common_prefix_list)
{
    mxml_node_t *node;
    cos_list_object_common_prefix_t *common_prefix;

    node = mxmlFindElement(xml_node, xml_node, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; node != NULL; ) {
        common_prefix = cos_create_list_object_common_prefix(p);
        cos_list_objects_prefix_parse(p, node, common_prefix);
        cos_list_add_tail(&common_prefix->node, common_prefix_list);
        node = mxmlFindElement(node, xml_node, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

int cos_list_objects_parse_from_body(cos_pool_t *p, cos_list_t *bc,
    cos_list_t *object_list, cos_list_t *common_prefix_list, cos_string_t *marker, int *truncated)
{
    int res;
    mxml_node_t *root;
    const char next_marker_xml_path[] = "NextMarker";
    const char truncated_xml_path[] = "IsTruncated";
    const char buckets_xml_path[] = "Contents";
    const char common_prefix_xml_path[] = "CommonPrefixes";
    char* next_marker;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        next_marker = get_xmlnode_value(p, root, next_marker_xml_path);
        if (next_marker) {
            cos_str_set(marker, next_marker);
        }

        *truncated = get_truncated_from_xml(p, root, truncated_xml_path);

        cos_list_objects_contents_parse(p, root, buckets_xml_path, object_list);
        cos_list_objects_common_prefix_parse(p, root, common_prefix_xml_path, common_prefix_list);

        mxmlDelete(root);
    }

    return res;
}

int cos_upload_id_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_string_t *upload_id)
{
    int res;
    mxml_node_t *root;
    const char xml_path[] = "UploadId";
    char *id;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        id = get_xmlnode_value(p, root, xml_path);
        if (id) {
            cos_str_set(upload_id, id);
        }
        mxmlDelete(root);
    }

    return res;
}

void cos_list_parts_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *part_list)
{
    mxml_node_t *content_node;
    cos_list_part_content_t *content;

    content_node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; content_node != NULL; ) {
        content = cos_create_list_part_content(p);
        cos_list_parts_content_parse(p, content_node, content);
        cos_list_add_tail(&content->node, part_list);
        content_node = mxmlFindElement(content_node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_list_parts_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_list_part_content_t *content)
{
    char *part_number;
    char *last_modified;
    char *etag;
    char *size;
    const char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "PartNumber", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        part_number = apr_pstrdup(p, node_content);
        cos_str_set(&content->part_number, part_number);
    }

    node = mxmlFindElement(xml_node, xml_node, "LastModified", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        last_modified = apr_pstrdup(p, node_content);
        cos_str_set(&content->last_modified, last_modified);
    }

    node = mxmlFindElement(xml_node, xml_node, "ETag", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        etag = apr_pstrdup(p, node_content);
        cos_str_set(&content->etag, etag);
    }

    node = mxmlFindElement(xml_node, xml_node, "Size", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        size = apr_pstrdup(p, node_content);
        cos_str_set(&content->size, size);
    }
}

int cos_list_parts_parse_from_body(cos_pool_t *p, cos_list_t *bc,
    cos_list_t *part_list, cos_string_t *partnumber_marker, int *truncated)
{
    int res;
    mxml_node_t *root;
    const char next_partnumber_marker_xml_path[] = "NextPartNumberMarker";
    const char truncated_xml_path[] = "IsTruncated";
    const char parts_xml_path[] = "Part";
    char *next_partnumber_marker;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        next_partnumber_marker = get_xmlnode_value(p, root,
                next_partnumber_marker_xml_path);
        if (next_partnumber_marker) {
            cos_str_set(partnumber_marker, next_partnumber_marker);
        }

        *truncated = get_truncated_from_xml(p, root, truncated_xml_path);

        cos_list_parts_contents_parse(p, root, parts_xml_path, part_list);

        mxmlDelete(root);
    }

    return res;
}

void cos_list_multipart_uploads_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *upload_list)
{
    mxml_node_t *content_node;
    cos_list_multipart_upload_content_t *content;

    content_node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; content_node != NULL; ) {
        content = cos_create_list_multipart_upload_content(p);
        cos_list_multipart_uploads_content_parse(p, content_node, content);
        cos_list_add_tail(&content->node, upload_list);
        content_node = mxmlFindElement(content_node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_list_multipart_uploads_content_parse(cos_pool_t *p, mxml_node_t *xml_node,
    cos_list_multipart_upload_content_t *content)
{
    char *key;
    char *upload_id;
    char *initiated;
    const char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Key",NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        key = apr_pstrdup(p, node_content);
        cos_str_set(&content->key, key);
    }

    node = mxmlFindElement(xml_node, xml_node, "UploadID",NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        upload_id = apr_pstrdup(p, node_content);
        cos_str_set(&content->upload_id, upload_id);
    }

    node = mxmlFindElement(xml_node, xml_node, "Initiated",NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        initiated = apr_pstrdup(p, node_content);
        cos_str_set(&content->initiated, initiated);
    }
}

int cos_list_multipart_uploads_parse_from_body(cos_pool_t *p, cos_list_t *bc,
    cos_list_t *upload_list, cos_string_t *key_marker,
    cos_string_t *upload_id_marker, int *truncated)
{
    int res;
    mxml_node_t *root;
    const char next_key_marker_xml_path[] = "NextKeyMarker";
    const char next_upload_id_marker_xml_path[] = "NextUploadIdMarker";
    const char truncated_xml_path[] = "IsTruncated";
    const char uploads_xml_path[] = "Upload";
    char *next_key_marker;
    char *next_upload_id_marker;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        next_key_marker = get_xmlnode_value(p, root, next_key_marker_xml_path);
        if (next_key_marker) {
            cos_str_set(key_marker, next_key_marker);
        }

        next_upload_id_marker = get_xmlnode_value(p, root, next_upload_id_marker_xml_path);
        if (next_upload_id_marker) {
            cos_str_set(upload_id_marker, next_upload_id_marker);
        }

        *truncated = get_truncated_from_xml(p, root, truncated_xml_path);

        cos_list_multipart_uploads_contents_parse(p, root, uploads_xml_path, upload_list);

        mxmlDelete(root);
    }

    return res;
}

char *build_complete_multipart_upload_xml(cos_pool_t *p, cos_list_t *bc)
{
    char *xml_buff;
    char *complete_part_xml;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    cos_complete_part_content_t *content;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "CompleteMultipartUpload");
    cos_list_for_each_entry(cos_complete_part_content_t, content, bc, node) {
        mxml_node_t *part_node = mxmlNewElement(root_node, "Part");
        mxml_node_t *part_number_node = mxmlNewElement(part_node, "PartNumber");
        mxml_node_t *etag_node = mxmlNewElement(part_node, "ETag");
        mxmlNewText(part_number_node, 0, content->part_number.data);
        mxmlNewText(etag_node, 0, content->etag.data);
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    complete_part_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return complete_part_xml;
}

void build_complete_multipart_upload_body(cos_pool_t *p, cos_list_t *part_list, cos_list_t *body)
{
    char *complete_multipart_upload_xml;
    cos_buf_t *b;

    complete_multipart_upload_xml = build_complete_multipart_upload_xml(p, part_list);
    cos_list_init(body);
    b = cos_buf_pack(p, complete_multipart_upload_xml, strlen(complete_multipart_upload_xml));
    cos_list_add_tail(&b->node, body);
}

char *build_lifecycle_xml(cos_pool_t *p, cos_list_t *lifecycle_rule_list)
{
    char *lifecycle_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    cos_lifecycle_rule_content_t *content;
    mxml_node_t *transition_node = NULL;
    mxml_node_t *expire_node = NULL;
    mxml_node_t *abort_node = NULL;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "LifecycleConfiguration");
    cos_list_for_each_entry(cos_lifecycle_rule_content_t, content, lifecycle_rule_list, node) {
        mxml_node_t *rule_node = mxmlNewElement(root_node, "Rule");
        mxml_node_t *id_node = mxmlNewElement(rule_node, "ID");
        mxml_node_t *filter_node = mxmlNewElement(rule_node, "Filter");
        mxml_node_t *prefix_node = mxmlNewElement(filter_node, "Prefix");
        mxml_node_t *status_node = mxmlNewElement(rule_node, "Status");
        mxmlNewText(id_node, 0, content->id.data);
        mxmlNewText(prefix_node, 0, content->prefix.data);
        mxmlNewText(status_node, 0, content->status.data);
        if (content->expire.days != INT_MAX) {
            char value_str[64];
            expire_node = mxmlNewElement(rule_node, "Expiration");
            mxml_node_t *days_node = mxmlNewElement(expire_node, "Days");
            apr_snprintf(value_str, sizeof(value_str), "%d", content->expire.days);
            mxmlNewText(days_node, 0, value_str);
        } else if (content->expire.date.len != 0 && strcmp(content->expire.date.data, "") != 0) {
            mxml_node_t *expire_node = mxmlNewElement(rule_node, "Expiration");
            mxml_node_t *date_node = mxmlNewElement(expire_node, "Date");
            mxmlNewText(date_node, 0, content->expire.date.data);
        }
        if (content->transition.days != INT_MAX) {
            char value_str[64];
            transition_node = mxmlNewElement(rule_node, "Transition");
            mxml_node_t *days_node = mxmlNewElement(transition_node, "Days");
            apr_snprintf(value_str, sizeof(value_str), "%d", content->transition.days);
            mxmlNewText(days_node, 0, value_str);
        } else if (content->transition.date.len != 0 && strcmp(content->transition.date.data, "") != 0) {
            transition_node = mxmlNewElement(rule_node, "Transition");
            mxml_node_t *date_node = mxmlNewElement(transition_node, "Date");
            mxmlNewText(date_node, 0, content->transition.date.data);
        }
        if (transition_node && content->transition.storage_class.len != 0 && strcmp(content->transition.storage_class.data, "") != 0) {
            mxml_node_t *date_node = mxmlNewElement(transition_node, "StorageClass");
            mxmlNewText(date_node, 0, content->transition.storage_class.data);
        }
        if (content->abort.days != INT_MAX) {
            char value_str[64];
            abort_node = mxmlNewElement(rule_node, "AbortIncompleteMultipartUpload");
            mxml_node_t *days_node = mxmlNewElement(abort_node, "DaysAfterInitiation");
            apr_snprintf(value_str, sizeof(value_str), "%d", content->abort.days);
            mxmlNewText(days_node, 0, value_str);
        }
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    lifecycle_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return lifecycle_xml;
}

void build_lifecycle_body(cos_pool_t *p, cos_list_t *lifecycle_rule_list, cos_list_t *body)
{
    char *lifecycle_xml;
    cos_buf_t *b;
    lifecycle_xml = build_lifecycle_xml(p, lifecycle_rule_list);
    cos_list_init(body);
    b = cos_buf_pack(p, lifecycle_xml, strlen(lifecycle_xml));
    cos_list_add_tail(&b->node, body);
}

void build_versioning_body(cos_pool_t *p, cos_versioning_content_t *versioning, cos_list_t *body)
{
    char *versioning_xml;
    cos_buf_t *b;
    versioning_xml = build_versioning_xml(p, versioning);
    cos_list_init(body);
    b = cos_buf_pack(p, versioning_xml, strlen(versioning_xml));
    cos_list_add_tail(&b->node, body);
}

char *build_versioning_xml(cos_pool_t *p, cos_versioning_content_t *versioning)
{
    char *cors_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "VersioningConfiguration");
    mxml_node_t *status_node = mxmlNewElement(root_node, "Status");
    mxmlNewText(status_node, 0, versioning->status.data);

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    cors_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return cors_xml;
}

char *build_cors_xml(cos_pool_t *p, cos_list_t *cors_rule_list)
{
    char *cors_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    cos_cors_rule_content_t *content;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "CORSConfiguration");
    cos_list_for_each_entry(cos_cors_rule_content_t, content, cors_rule_list, node) {
        mxml_node_t *rule_node = mxmlNewElement(root_node, "CORSRule");
        if (content->id.len !=0 && strcmp(content->id.data, "") != 0) {
            mxml_node_t *id_node = mxmlNewElement(rule_node, "ID");
            mxmlNewText(id_node, 0, content->id.data);
        }
        mxml_node_t *allowed_origin_node = mxmlNewElement(rule_node, "AllowedOrigin");
        mxmlNewText(allowed_origin_node, 0, content->allowed_origin.data);
        mxml_node_t *allowed_method_node = mxmlNewElement(rule_node, "AllowedMethod");
        mxmlNewText(allowed_method_node, 0, content->allowed_method.data);
        if (content->allowed_header.len !=0 && strcmp(content->allowed_header.data, "") != 0) {
            mxml_node_t *allowed_header_node = mxmlNewElement(rule_node, "AllowedHeader");
            mxmlNewText(allowed_header_node, 0, content->allowed_header.data);
        }
        if (content->max_age_seconds != INT_MAX) {
            char value_str[64];
            mxml_node_t *max_age_node = mxmlNewElement(rule_node, "MaxAgeSeconds");
            apr_snprintf(value_str, sizeof(value_str), "%d", content->max_age_seconds);
            mxmlNewText(max_age_node, 0, value_str);
        }
        if (content->expose_header.len !=0 && strcmp(content->expose_header.data, "") != 0) {
            mxml_node_t *expose_header_node = mxmlNewElement(rule_node, "ExposeHeader");
            mxmlNewText(expose_header_node, 0, content->expose_header.data);
        }
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    cors_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return cors_xml;
}

void build_cors_body(cos_pool_t *p, cos_list_t *cors_rule_list, cos_list_t *body)
{
    char *cors_xml;
    cos_buf_t *b;
    cors_xml = build_cors_xml(p, cors_rule_list);
    cos_list_init(body);
    b = cos_buf_pack(p, cors_xml, strlen(cors_xml));
    cos_list_add_tail(&b->node, body);
}

char *build_replication_xml(cos_pool_t *p, cos_replication_params_t *replication_param)
{
    char *replication_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    cos_replication_rule_content_t *content = NULL;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "ReplicationConfiguration");
    if (replication_param->role.len !=0 && strcmp(replication_param->role.data, "") != 0) {
        mxml_node_t *role_node = mxmlNewElement(root_node, "Role");
        mxmlNewText(role_node, 0, replication_param->role.data);
    }
    cos_list_for_each_entry(cos_replication_rule_content_t, content, &replication_param->rule_list, node) {
        mxml_node_t *rule_node = mxmlNewElement(root_node, "Rule");
        if (content->id.len !=0 && strcmp(content->id.data, "") != 0) {
            mxml_node_t *id_node = mxmlNewElement(rule_node, "ID");
            mxmlNewText(id_node, 0, content->id.data);
        }
        if (content->status.len !=0 && strcmp(content->status.data, "") != 0) {
            mxml_node_t *status_node = mxmlNewElement(rule_node, "Status");
            mxmlNewText(status_node, 0, content->status.data);
        }
        if (content->prefix.len !=0 && strcmp(content->prefix.data, "") != 0) {
            mxml_node_t *prefix_node = mxmlNewElement(rule_node, "Prefix");
            mxmlNewText(prefix_node, 0, content->prefix.data);
        }
        mxml_node_t *dst_node = mxmlNewElement(rule_node, "Destination");
        if (content->dst_bucket.len !=0 && strcmp(content->dst_bucket.data, "") != 0) {
            mxml_node_t *bucket_node = mxmlNewElement(dst_node, "Bucket");
            mxmlNewText(bucket_node, 0, content->dst_bucket.data);
        }
        if (content->storage_class.len !=0 && strcmp(content->storage_class.data, "") != 0) {
            mxml_node_t *bucket_node = mxmlNewElement(dst_node, "StorageClass");
            mxmlNewText(bucket_node, 0, content->storage_class.data);
        }
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    replication_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return replication_xml;
}

void build_replication_body(cos_pool_t *p, cos_replication_params_t *replication_param, cos_list_t *body)
{
    char *cors_xml;
    cos_buf_t *b;
    cors_xml = build_replication_xml(p, replication_param);
    cos_list_init(body);
    b = cos_buf_pack(p, cors_xml, strlen(cors_xml));
    cos_list_add_tail(&b->node, body);
}

void build_xml_node(mxml_node_t *pnode, const char *xml, cos_string_t *param)
{
    if (!cos_is_null_string(param)) {
        mxml_node_t *node = mxmlNewElement(pnode, xml);
        mxmlNewText(node, 0, param->data);
    }
}

void build_xml_node_with_parent(mxml_node_t *root, const char *pxml, const char *cxml, cos_string_t *param)
{
    if (!cos_is_null_string(param)) {
        mxml_node_t *pnode = mxmlNewElement(root, pxml);
        mxml_node_t *cnode = mxmlNewElement(pnode, cxml);
        mxmlNewText(cnode, 0, param->data);
    }
}

mxml_node_t *build_empty_node_with_parent(mxml_node_t *root, const char *pxml, const char *cxml)
{
    mxml_node_t *pnode = mxmlNewElement(root, pxml);
    mxml_node_t *cnode = mxmlNewElement(pnode, cxml);
    return cnode;
}

void build_website_body(cos_pool_t *p, cos_website_params_t *website_params, cos_list_t *body)
{
    char *website_xml;
    cos_buf_t *b;

    website_xml = build_website_xml(p, website_params);
    cos_list_init(body);
    b = cos_buf_pack(p, website_xml, strlen(website_xml));
    cos_list_add_tail(&b->node, body);
}

char* build_website_xml(cos_pool_t *p, cos_website_params_t *website_params)
{
    char *website_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    cos_website_rule_content_t *content = NULL;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "WebsiteConfiguration");

    build_xml_node_with_parent(root_node, "IndexDocument", "Suffix", &website_params->index);
    build_xml_node_with_parent(root_node, "RedirectAllRequestsTo", "Protocol", &website_params->redirect_protocol);
    build_xml_node_with_parent(root_node, "ErrorDocument", "Key", &website_params->error_document);

    if (!cos_list_empty(&website_params->rule_list)) {
        mxml_node_t *rules_node = mxmlNewElement(root_node, "RoutingRules");

        cos_list_for_each_entry(cos_website_rule_content_t, content, &website_params->rule_list, node) {
            mxml_node_t *rule_node = mxmlNewElement(rules_node, "RoutingRule");

            if (!cos_is_null_string(&content->condition_errcode)
                    || !cos_is_null_string(&content->condition_prefix)) {

                mxml_node_t *condition_node = mxmlNewElement(rule_node, "Condition");
                build_xml_node(condition_node, "HttpErrorCodeReturnedEquals", &content->condition_errcode);
                build_xml_node(condition_node, "KeyPrefixEquals", &content->condition_prefix);
            }

            if (!cos_is_null_string(&content->redirect_protocol)
                    || !cos_is_null_string(&content->redirect_replace_key)
                    || !cos_is_null_string(&content->redirect_replace_key_prefix)) {

                mxml_node_t *redirect_node = mxmlNewElement(rule_node, "Redirect");
                build_xml_node(redirect_node, "Protocol", &content->redirect_protocol);
                build_xml_node(redirect_node, "ReplaceKeyWith", &content->redirect_replace_key);
                build_xml_node(redirect_node, "ReplaceKeyPrefixWith", &content->redirect_replace_key_prefix);
            }
        }
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    website_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return website_xml;
}

void build_domain_body(cos_pool_t *p, cos_domain_params_t *domain_params, cos_list_t *body)
{
    char *domain_xml;
    cos_buf_t *b;

    domain_xml = build_domain_xml(p, domain_params);
    cos_list_init(body);
    b = cos_buf_pack(p, domain_xml, strlen(domain_xml));
    cos_list_add_tail(&b->node, body);
}

char *build_domain_xml(cos_pool_t *p, cos_domain_params_t *domain_params)
{
    char *domain_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    mxml_node_t *rule_node;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "DomainConfiguration");
    rule_node = mxmlNewElement(root_node, "DomainRule");
    build_xml_node(rule_node, "Status", &domain_params->status);
    build_xml_node(rule_node, "Name", &domain_params->name);
    build_xml_node(rule_node, "Type", &domain_params->type);
    build_xml_node(rule_node, "ForedReplacement", &domain_params->forced_replacement);

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    domain_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return domain_xml;
}

void build_logging_body(cos_pool_t *p, cos_logging_params_t *params, cos_list_t *body)
{
    char *logging_xml;
    cos_buf_t *b;

    logging_xml = build_logging_xml(p, params);
    cos_list_init(body);
    b = cos_buf_pack(p, logging_xml, strlen(logging_xml));
    cos_list_add_tail(&b->node, body);
}

char *build_logging_xml(cos_pool_t *p, cos_logging_params_t *params)
{
    char *logging_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    mxml_node_t *logging_node;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "BucketLoggingStatus");
    logging_node = mxmlNewElement(root_node, "LoggingEnabled");
    build_xml_node(logging_node, "TargetBucket", &params->target_bucket);
    build_xml_node(logging_node, "TargetPrefix", &params->target_prefix);

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    logging_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return logging_xml;
}

void build_inventory_body(cos_pool_t *p, cos_inventory_params_t *params, cos_list_t *body)
{
    char *inventory_xml;
    cos_buf_t *b;

    inventory_xml = build_inventory_xml(p, params);
    cos_list_init(body);
    b = cos_buf_pack(p, inventory_xml, strlen(inventory_xml));
    cos_list_add_tail(&b->node, body);
}

char *build_inventory_xml(cos_pool_t *p, cos_inventory_params_t *params)
{
    char *inventory_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    mxml_node_t *dest_node;
    mxml_node_t *optional_node;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "InventoryConfiguration");
    build_xml_node(root_node, "Id", &params->id);
    build_xml_node(root_node, "IsEnabled", &params->is_enabled);
    build_xml_node_with_parent(root_node, "Schedule", "Frequency", &params->frequency);
    build_xml_node_with_parent(root_node, "Filter", "Prefix", &params->filter_prefix);
    build_xml_node(root_node, "IncludedObjectVersions", &params->included_object_versions);
    dest_node = build_empty_node_with_parent(root_node, "Destination", "COSBucketDestination");
    build_xml_node(dest_node, "Format", &params->destination.format);
    build_xml_node(dest_node, "AccountId", &params->destination.account_id);
    build_xml_node(dest_node, "Bucket", &params->destination.bucket);
    build_xml_node(dest_node, "Prefix", &params->destination.prefix);
    if (params->destination.encryption) {
        build_empty_node_with_parent(dest_node, "Encryption", "SSE-COS");
    }
    if (!cos_list_empty(&params->fields)) {
        cos_inventory_optional_t *field;
        optional_node = mxmlNewElement(root_node, "OptionalFields");
        cos_list_for_each_entry(cos_inventory_optional_t, field, &params->fields, node) {
            build_xml_node(optional_node, "Field", &field->field);
        }
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    inventory_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return inventory_xml;
}

void build_tagging_body(cos_pool_t *p, cos_tagging_params_t *params, cos_list_t *body)
{
    char *tagging_xml;
    cos_buf_t *b;

    tagging_xml = build_tagging_xml(p, params);
    cos_list_init(body);
    b = cos_buf_pack(p, tagging_xml, strlen(tagging_xml));
    cos_list_add_tail(&b->node, body);
}

char *build_tagging_xml(cos_pool_t *p, cos_tagging_params_t *params)
{
    char *tagging_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    mxml_node_t *set_node;
    cos_tagging_tag_t *content = NULL;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "Tagging");
    set_node = mxmlNewElement(root_node, "TagSet");
    cos_list_for_each_entry(cos_tagging_tag_t, content, &params->node, node) {
        mxml_node_t *tag_node = mxmlNewElement(set_node, "Tag");
        build_xml_node(tag_node, "Key", &content->key);
        build_xml_node(tag_node, "Value", &content->value);
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    tagging_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return tagging_xml;
}

void build_intelligenttiering_body(cos_pool_t *p, cos_intelligenttiering_params_t *params, cos_list_t *body)
{
    char *xml;
    cos_buf_t *b;
    xml = build_intelligenttiering_xml(p, params);
    cos_list_init(body);
    b = cos_buf_pack(p, xml, strlen(xml));
    cos_list_add_tail(&b->node, body);
}

char *build_intelligenttiering_xml(cos_pool_t *p, cos_intelligenttiering_params_t *params)
{
    char *xml;
    char *xml_buff;
    char value_str[64];
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    mxml_node_t *trans_node;
    mxml_node_t *node;
    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "IntelligentTieringConfiguration");
    build_xml_node(root_node, "Status", &params->status);
    trans_node = mxmlNewElement(root_node, "Transition");
    if (params->days != 0) {
        apr_snprintf(value_str, sizeof(value_str), "%d", params->days);
        node = mxmlNewElement(trans_node, "Days");
        mxmlNewText(node, 0, value_str);
    }
    node = mxmlNewElement(trans_node, "RequestFrequent");
    mxmlNewText(node, 0, "1");      // RequestFrequent 当前固定是 1

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);
    return xml;
}

void build_object_restore_body(cos_pool_t *p, cos_object_restore_params_t *params, cos_list_t *body)
{
    cos_buf_t *b;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    char *xml_buff;
    cos_string_t xml_doc;
    char *restore_xml;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "RestoreRequest");

    if (params->days != INT_MAX) {
        char value_str[64];
        mxml_node_t *days_node = mxmlNewElement(root_node, "Days");
        apr_snprintf(value_str, sizeof(value_str), "%d", params->days);
        mxmlNewText(days_node, 0, value_str);
    }
    if (params->tier.len != 0 && strcmp(params->tier.data, "") != 0) {
        mxml_node_t *cas_job_params_node = mxmlNewElement(root_node, "CASJobParameters");
        mxml_node_t *tier_node = mxmlNewElement(cas_job_params_node, "Tier");
        mxmlNewText(tier_node, 0, params->tier.data);
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        mxmlDelete(doc);
        return;
    }
    cos_str_set(&xml_doc, xml_buff);
    restore_xml = cos_pstrdup(p, &xml_doc);

    cos_list_init(body);
    b = cos_buf_pack(p, restore_xml, strlen(restore_xml));
    cos_list_add_tail(&b->node, body);

    free(xml_buff);
    mxmlDelete(doc);
}

int cos_versioning_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_versioning_content_t *versioning)
{
    int res;
    mxml_node_t *root = NULL;
    mxml_node_t *node;
    const char *node_content;
    char *status;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        node = mxmlFindElement(root, root, "Status", NULL, NULL, MXML_DESCEND);
        node_content = mxmlGetOpaque(node);
        if (node_content != NULL) {
            status = apr_pstrdup(p, node_content);
            cos_str_set(&versioning->status, status);
        }
        mxmlDelete(root);
    }

    return res;
}

int cos_cors_rules_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *cors_rule_list)
{
    int res;
    mxml_node_t *root = NULL;
    const char rule_xml_path[] = "CORSRule";

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_cors_rule_contents_parse(p, root, rule_xml_path, cors_rule_list);
        mxmlDelete(root);
    }

    return res;
}

void cos_cors_rule_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path, cos_list_t *cors_rule_list)
{
    mxml_node_t *node;
    cos_cors_rule_content_t *content;

    node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; node != NULL; ) {
        content = cos_create_cors_rule_content(p);
        cos_cors_rule_content_parse(p, node, content);
        cos_list_add_tail(&content->node, cors_rule_list);
        node = mxmlFindElement(node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_cors_rule_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_cors_rule_content_t *content)
{
    char *id;
    char *allowed_origin;
    char *allowed_method;
    char *allowed_header;
    char *expose_header;
    char *max_age_seconds;
    mxml_node_t *node;
    const char *node_content;

    node = mxmlFindElement(xml_node, xml_node, "ID", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        id = apr_pstrdup(p, node_content);
        cos_str_set(&content->id, id);
    }

    node = mxmlFindElement(xml_node, xml_node, "AllowedOrigin", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        allowed_origin = apr_pstrdup(p, node_content);
        cos_str_set(&content->allowed_origin, allowed_origin);
    }

    node = mxmlFindElement(xml_node, xml_node, "AllowedMethod", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        allowed_method = apr_pstrdup(p, node_content);
        cos_str_set(&content->allowed_method, allowed_method);
    }

    node = mxmlFindElement(xml_node, xml_node, "AllowedHeader", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        allowed_header = apr_pstrdup(p, node_content);
        cos_str_set(&content->allowed_header, allowed_header);
    }

    node = mxmlFindElement(xml_node, xml_node, "ExposeHeader", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        expose_header = apr_pstrdup(p, node_content);
        cos_str_set(&content->expose_header, expose_header);
    }

    node = mxmlFindElement(xml_node, xml_node, "MaxAgeSeconds", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        max_age_seconds = apr_pstrdup(p, node_content);
        content->max_age_seconds = atoi(max_age_seconds);
    }
}

int cos_lifecycle_rules_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *lifecycle_rule_list)
{
    int res;
    mxml_node_t *root = NULL;
    const char rule_xml_path[] = "Rule";

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_lifecycle_rule_contents_parse(p, root, rule_xml_path, lifecycle_rule_list);
        mxmlDelete(root);
    }

    return res;
}

void cos_lifecycle_rule_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *lifecycle_rule_list)
{
    mxml_node_t *node;
    cos_lifecycle_rule_content_t *content;

    node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; node != NULL; ) {
        content = cos_create_lifecycle_rule_content(p);
        cos_lifecycle_rule_content_parse(p, node, content);
        cos_list_add_tail(&content->node, lifecycle_rule_list);
        node = mxmlFindElement(node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_lifecycle_rule_content_parse(cos_pool_t *p, mxml_node_t * xml_node,
    cos_lifecycle_rule_content_t *content)
{
    char *id;
    char *prefix;
    char *status;
    const char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "ID",NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        id = apr_pstrdup(p, node_content);
        cos_str_set(&content->id, id);
    }

    node = mxmlFindElement(xml_node, xml_node, "Prefix",NULL, NULL,MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        prefix = apr_pstrdup(p, node_content);
        cos_str_set(&content->prefix, prefix);
    }

    node = mxmlFindElement(xml_node, xml_node, "Status",NULL, NULL,MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        status = apr_pstrdup(p, node_content);
        cos_str_set(&content->status, status);
    }

    node = mxmlFindElement(xml_node, xml_node, "Expiration",NULL, NULL,MXML_DESCEND);
    if (NULL != node) {
        cos_lifecycle_rule_expire_parse(p, node, content);
    }

    node = mxmlFindElement(xml_node, xml_node, "Transition",NULL, NULL,MXML_DESCEND);
    if (NULL != node) {
        cos_lifecycle_rule_transition_parse(p, node, content);
    }

    node = mxmlFindElement(xml_node, xml_node, "AbortIncompleteMultipartUpload",NULL, NULL,MXML_DESCEND);
    if (NULL != node) {
        cos_lifecycle_rule_abort_parse(p, node, content);
    }
}

void cos_lifecycle_rule_expire_parse(cos_pool_t *p, mxml_node_t * xml_node,
    cos_lifecycle_rule_content_t *content)
{
    char* days;
    char *date;
    mxml_node_t *node;
    const char *node_content;

    node = mxmlFindElement(xml_node, xml_node, "Days", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        days = apr_pstrdup(p, node_content);
        content->expire.days = atoi(days);
    }

    node = mxmlFindElement(xml_node, xml_node, "Date", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        date = apr_pstrdup(p, node_content);
        cos_str_set(&content->expire.date, date);
    }
}

void cos_lifecycle_rule_transition_parse(cos_pool_t *p, mxml_node_t * xml_node,
    cos_lifecycle_rule_content_t *content)
{
    char* days;
    char *date;
    mxml_node_t *node;
    const char *node_content;

    node = mxmlFindElement(xml_node, xml_node, "Days", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        days = apr_pstrdup(p, node_content);
        content->transition.days = atoi(days);
    }

    node = mxmlFindElement(xml_node, xml_node, "Date", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        date = apr_pstrdup(p, node_content);
        cos_str_set(&content->transition.date, date);
    }

    node = mxmlFindElement(xml_node, xml_node, "StorageClass", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        date = apr_pstrdup(p, node_content);
        cos_str_set(&content->transition.storage_class, date);
    }
}

void cos_lifecycle_rule_abort_parse(cos_pool_t *p, mxml_node_t * xml_node,
    cos_lifecycle_rule_content_t *content)
{
    char* days;
    mxml_node_t *node;
    const char *node_content;

    node = mxmlFindElement(xml_node, xml_node, "Days", NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        days = apr_pstrdup(p, node_content);
        content->abort.days = atoi(days);
    }
}

void cos_common_parse_from_xml_node(cos_pool_t *p, mxml_node_t *pnode, mxml_node_t *root, const char *xml, cos_string_t *param)
{
    const char *content;
    char *content_in_pool;
    mxml_node_t *node;

    node = mxmlFindElement(pnode, root, xml, NULL, NULL, MXML_DESCEND_FIRST);
    content = mxmlGetOpaque(node);
    if (content != NULL) {
        content_in_pool = apr_pstrdup(p, content);
        cos_str_set(param, content_in_pool);
    }
}

void cos_common_parse_from_parent_node(cos_pool_t *p, mxml_node_t *root, const char *pxml, const char *cxml, cos_string_t *param)
{
    mxml_node_t *pnode;

    pnode = mxmlFindElement(root, root, pxml, NULL, NULL, MXML_DESCEND_FIRST);
    if (pnode != NULL) {
        cos_common_parse_from_xml_node(p, pnode, root, cxml, param);
    }
}

void cos_get_website_parse_rules(cos_pool_t *p, mxml_node_t *root, cos_website_params_t *website)
{
    static const char *kRoutingRule = "RoutingRule";
    static const char *kCondition = "Condition";
    static const char *kCondition_ErrCode = "HttpErrorCodeReturnedEquals";
    static const char *kCondition_Prefix = "KeyPrefixEquals";
    static const char *kRedirect = "Redirect";
    static const char *kRedirect_Protocol = "Protocol";
    static const char *kRedirect_ReplaceKey = "ReplaceKeyWith";
    static const char *kRedirect_ReplaceKeyPrefix = "ReplaceKeyPrefixWith";

    mxml_node_t *node;
    mxml_node_t *condition_node;
    mxml_node_t *redirect_node;
    cos_website_rule_content_t *content;

    node = mxmlFindElement(root, root, kRoutingRule, NULL, NULL, MXML_DESCEND);
    while (node != NULL) {
        content = cos_create_website_rule_content(p);

        condition_node = mxmlFindElement(node, node, kCondition, NULL, NULL, MXML_DESCEND_FIRST);
        if (condition_node != NULL) {
            cos_common_parse_from_xml_node(p, condition_node, node, kCondition_ErrCode, &content->condition_errcode);
            cos_common_parse_from_xml_node(p, condition_node, node, kCondition_Prefix, &content->condition_prefix);
        }

        redirect_node = mxmlFindElement(node, node, kRedirect, NULL, NULL, MXML_DESCEND_FIRST);
        if (redirect_node != NULL) {
            cos_common_parse_from_xml_node(p, redirect_node, node, kRedirect_Protocol, &content->redirect_protocol);
            cos_common_parse_from_xml_node(p, redirect_node, node, kRedirect_ReplaceKey, &content->redirect_replace_key);
            cos_common_parse_from_xml_node(p, redirect_node, node, kRedirect_ReplaceKeyPrefix, &content->redirect_replace_key_prefix);
        }

        cos_list_add_tail(&content->node, &website->rule_list);

        node = mxmlFindElement(node, root, kRoutingRule, NULL, NULL, MXML_DESCEND);
    }
}

int cos_get_website_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_website_params_t *website)
{
    static const char *kIndexDocument = "IndexDocument";
    static const char *kIndexDocument_Suffix = "Suffix";
    static const char *kRedirectRequest = "RedirectAllRequestsTo";
    static const char *kRedirectRequest_Protocol = "Protocol";
    static const char *kErrorDocument = "ErrorDocument";
    static const char *kErrorDocument_Key = "Key";
    static const char *kRoutingRules = "RoutingRules";

    int res;
    mxml_node_t *root;
    mxml_node_t *rules_node;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_common_parse_from_parent_node(p, root, kIndexDocument, kIndexDocument_Suffix, &website->index);
        cos_common_parse_from_parent_node(p, root, kRedirectRequest, kRedirectRequest_Protocol, &website->redirect_protocol);
        cos_common_parse_from_parent_node(p, root, kErrorDocument, kErrorDocument_Key, &website->error_document);

        rules_node = mxmlFindElement(root, root, kRoutingRules, NULL, NULL, MXML_DESCEND_FIRST);
        if (rules_node != NULL) {
            cos_get_website_parse_rules(p, rules_node, website);
        }

        mxmlDelete(root);
    }
    return res;
}

int cos_get_domain_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_domain_params_t *domain)
{
    static const char *kDomainRule = "DomainRule";
    static const char *kStatus = "Status";
    static const char *kName = "Name";
    static const char *kType = "Type";
    static const char *kForcedReplacement = "ForcedReplacement";

    int res = 0;
    mxml_node_t *root;
    mxml_node_t *rule_node;
    if (cos_list_empty(bc)) {
        return res;
    }
    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        rule_node = mxmlFindElement(root, root, kDomainRule, NULL, NULL, MXML_DESCEND);
        if (rule_node != NULL) {
            cos_common_parse_from_xml_node(p, rule_node, rule_node, kStatus, &domain->status);
            cos_common_parse_from_xml_node(p, rule_node, rule_node, kName, &domain->name);
            cos_common_parse_from_xml_node(p, rule_node, rule_node, kType, &domain->type);
            cos_common_parse_from_xml_node(p, rule_node, rule_node, kForcedReplacement, &domain->forced_replacement);
        }

        mxmlDelete(root);
    }
    return res;
}


int cos_get_logging_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_logging_params_t *logging)
{
    int res = 0;
    mxml_node_t *root;
    mxml_node_t *logging_node;
    if (cos_list_empty(bc)) {
        return res;
    }
    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        logging_node = mxmlFindElement(root, root, "LoggingEnabled", NULL, NULL, MXML_DESCEND);
        if (logging_node != NULL) {
            cos_common_parse_from_xml_node(p, logging_node, logging_node, "TargetBucket", &logging->target_bucket);
            cos_common_parse_from_xml_node(p, logging_node, logging_node, "TargetPrefix", &logging->target_prefix);
        }

        mxmlDelete(root);
    }
    return res;
}

void cos_inventory_parse_from_node(cos_pool_t *p, mxml_node_t *root, cos_inventory_params_t *params)
{
    mxml_node_t *dest_node;
    mxml_node_t *optional_node;

    cos_common_parse_from_xml_node(p, root, root, "Id", &params->id);
    cos_common_parse_from_xml_node(p, root, root, "IsEnabled", &params->is_enabled);
    cos_common_parse_from_xml_node(p, root, root, "IncludedObjectVersions", &params->included_object_versions);
    cos_common_parse_from_parent_node(p, root, "Schedule", "Frequency", &params->frequency);
    cos_common_parse_from_parent_node(p, root, "Filter", "Prefix", &params->filter_prefix);

    dest_node = mxmlFindElement(root, root, "COSBucketDestination", NULL, NULL, MXML_DESCEND);
    if (dest_node != NULL) {
        cos_common_parse_from_xml_node(p, dest_node, root, "Format", &params->destination.format);
        cos_common_parse_from_xml_node(p, dest_node, root, "AccountId", &params->destination.account_id);
        cos_common_parse_from_xml_node(p, dest_node, root, "Bucket", &params->destination.bucket);
        cos_common_parse_from_xml_node(p, dest_node, root, "Prefix", &params->destination.prefix);
        optional_node = mxmlFindElement(dest_node, root, "SSE-COS", NULL, NULL, MXML_DESCEND);
        if (optional_node != NULL) {
            params->destination.encryption = 1;
        }
    }

    optional_node = mxmlFindElement(root, root, "OptionalFields", NULL, NULL, MXML_DESCEND);
    if (optional_node != NULL) {
        char *content;
        mxml_node_t *field_node = mxmlFindElement(optional_node, optional_node, "Field", NULL, NULL, MXML_DESCEND);
        while (field_node != NULL) {
            cos_inventory_optional_t *optional = cos_create_inventory_optional(p);
            const char *opaque = mxmlGetOpaque(field_node);
            if (opaque != NULL) {
                content = apr_pstrdup(p, opaque);
                cos_str_set(&optional->field, content);
            }
            cos_list_add_tail(&optional->node, &params->fields);
            field_node = mxmlFindElement(field_node, optional_node, "Field", NULL, NULL, MXML_DESCEND);
        }
    }
}

int cos_get_inventory_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_inventory_params_t *params)
{
    int res = 0;
    mxml_node_t *root;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_inventory_parse_from_node(p, root, params);
        mxmlDelete(root);
    }
    return res;
}

int cos_list_inventory_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_inventory_params_t *params)
{
    int res = 0;
    mxml_node_t *root;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_string_t is_truncated;
        cos_common_parse_from_xml_node(p, root, root, "IsTruncated", &is_truncated);
        if (strncmp(is_truncated.data, "true", is_truncated.len)) {
            params->is_truncated = 1;
        }
        cos_common_parse_from_xml_node(p, root, root, "ContinuationToken", &params->continuation_token);
        cos_common_parse_from_xml_node(p, root, root, "NextContinuationToken", &params->next_continuation_token);
        mxml_node_t *node = mxmlFindElement(root, root, "InventoryConfiguration", NULL, NULL, MXML_DESCEND);
        while (node != NULL) {
            cos_inventory_params_t *inventory = cos_create_inventory_params(p);
            cos_inventory_parse_from_node(p, node, inventory);
            cos_list_add_tail(&inventory->node, &params->inventorys);
            node = mxmlFindElement(node, root, "InventoryConfiguration", NULL, NULL, MXML_DESCEND);
        }
    }
    return res;
}

int cos_get_tagging_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_tagging_params_t *params)
{
    int res = 0;
    mxml_node_t *root;
    mxml_node_t *tags_node;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        tags_node = mxmlFindElement(root, root, "TagSet", NULL, NULL, MXML_DESCEND);
        if (tags_node != NULL) {
            mxml_node_t *tag_node = mxmlFindElement(tags_node, tags_node, "Tag", NULL, NULL, MXML_DESCEND);
            while (tag_node != NULL) {
                cos_tagging_tag_t *tag = cos_create_tagging_tag(p);
                cos_common_parse_from_xml_node(p, tag_node, tag_node, "Key", &tag->key);
                cos_common_parse_from_xml_node(p, tag_node, tag_node, "Value", &tag->value);
                cos_list_add_tail(&tag->node, &params->node);
                tag_node = mxmlFindElement(tag_node, tags_node, "Tag", NULL, NULL, MXML_DESCEND);
            }
        }
        mxmlDelete(root);
    }

    return res;
}

int cos_get_intelligenttiering_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_intelligenttiering_params_t *params)
{
    int res = 0;
    mxml_node_t *root;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_string_t val;
        cos_common_parse_from_xml_node(p, root, root, "Status", &params->status);
        cos_common_parse_from_parent_node(p, root, "Transition", "Days", &val);
        params->days = atoi(val.data);
        mxmlDelete(root);
    }
    return res;
}

void cos_delete_objects_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *object_list)
{
    mxml_node_t *node;

    node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; node != NULL; ) {
        cos_object_key_t *content = cos_create_cos_object_key(p);
        cos_object_key_parse(p, node, content);
        cos_list_add_tail(&content->node, object_list);
        node = mxmlFindElement(node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_object_key_parse(cos_pool_t *p, mxml_node_t * xml_node,
    cos_object_key_t *content)
{
    char *key;
    char *encoded_key;
    const char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Key",NULL, NULL, MXML_DESCEND);
    node_content = mxmlGetOpaque(node);
    if (node_content != NULL) {
        encoded_key = (char*)node_content;
        key = (char *) cos_palloc(p, strlen(encoded_key));
        cos_url_decode(encoded_key, key);
        cos_str_set(&content->key, key);
    }
}

int cos_delete_objects_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *object_list)
{
    int res;
    mxml_node_t *root = NULL;
    const char deleted_xml_path[] = "Deleted";

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_delete_objects_contents_parse(p, root, deleted_xml_path, object_list);
        mxmlDelete(root);
    }

    return res;
}

#if 0
void cos_publish_url_parse(cos_pool_t *p, mxml_node_t *node, cos_live_channel_publish_url_t *content)
{
    char *url;
    char *node_content;

    if (NULL != node) {
        node_content = node->child->value.opaque;
        url = apr_pstrdup(p, node_content);
        cos_str_set(&content->publish_url, url);
    }
}

void cos_play_url_parse(cos_pool_t *p, mxml_node_t *node, cos_live_channel_play_url_t *content)
{
    char *url;
    char *node_content;

    if (NULL != node) {
        node_content = node->child->value.opaque;
        url = apr_pstrdup(p, node_content);
        cos_str_set(&content->play_url, url);
    }
}

void cos_publish_urls_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *publish_xml_list)
{
    mxml_node_t *node;

    node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; node != NULL; ) {
        cos_live_channel_publish_url_t *content = cos_create_live_channel_publish_url(p);
        cos_publish_url_parse(p, node, content);
        cos_list_add_tail(&content->node, publish_xml_list);
        node = mxmlFindElement(node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_play_urls_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *play_xml_list)
{
    mxml_node_t *node;

    node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; node != NULL; ) {
        cos_live_channel_play_url_t *content = cos_create_live_channel_play_url(p);
        cos_play_url_parse(p, node, content);
        cos_list_add_tail(&content->node, play_xml_list);
        node = mxmlFindElement(node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

void cos_create_live_channel_content_parse(cos_pool_t *p, mxml_node_t *root,
    const char *publish_xml_path, cos_list_t *publish_url_list,
    const char *play_xml_path, cos_list_t *play_url_list)
{
    mxml_node_t *node;
    const char url_xml_path[] = "Url";

    node = mxmlFindElement(root, root, publish_xml_path, NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        cos_publish_urls_contents_parse(p, node, url_xml_path, publish_url_list);
    }

    node = mxmlFindElement(root, root, play_xml_path, NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        cos_play_urls_contents_parse(p, node, url_xml_path, play_url_list);
    }
}

int cos_create_live_channel_parse_from_body(cos_pool_t *p, cos_list_t *bc,
    cos_list_t *publish_url_list, cos_list_t *play_url_list)
{
    int res;
    mxml_node_t *root = NULL;
    const char publish_urls_xml_path[] = "PublishUrls";
    const char play_urls_xml_path[] = "PlayUrls";

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_create_live_channel_content_parse(p, root, publish_urls_xml_path, publish_url_list,
            play_urls_xml_path, play_url_list);
        mxmlDelete(root);
    }

    return res;
}

char *build_create_live_channel_xml(cos_pool_t *p, cos_live_channel_configuration_t *config)
{
    char *xml_buff;
    char *complete_part_xml;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    char value_str[64];
    mxml_node_t *description_node;
    mxml_node_t *status_node;
    mxml_node_t *target_node;
    mxml_node_t *type_node;
    mxml_node_t *frag_duration_node;
    mxml_node_t *frag_count_node;
    mxml_node_t *play_list_node;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "LiveChannelConfiguration");

    description_node = mxmlNewElement(root_node, "Description");
    mxmlNewText(description_node, 0, config->description.data);

    status_node = mxmlNewElement(root_node, "Status");
    mxmlNewText(status_node, 0, config->status.data);

    // target
    target_node = mxmlNewElement(root_node, "Target");
    type_node = mxmlNewElement(target_node, "Type");
    mxmlNewText(type_node, 0, config->target.type.data);

    apr_snprintf(value_str, sizeof(value_str), "%d", config->target.frag_duration);
    frag_duration_node = mxmlNewElement(target_node, "FragDuration");
    mxmlNewText(frag_duration_node, 0, value_str);

    apr_snprintf(value_str, sizeof(value_str), "%d", config->target.frag_count);
    frag_count_node = mxmlNewElement(target_node, "FragCount");
    mxmlNewText(frag_count_node, 0, value_str);

    play_list_node = mxmlNewElement(target_node, "PlaylistName");
    mxmlNewText(play_list_node, 0, config->target.play_list_name.data);

    // dump
	xml_buff = new_xml_buff(doc);
	if (xml_buff == NULL) {
		return NULL;
	}
	cos_str_set(&xml_doc, xml_buff);
	complete_part_xml = cos_pstrdup(p, &xml_doc);

	free(xml_buff);
	mxmlDelete(doc);

    return complete_part_xml;
}

void build_create_live_channel_body(cos_pool_t *p, cos_live_channel_configuration_t *config, cos_list_t *body)
{
    char *live_channel_xml;
    cos_buf_t *b;

    live_channel_xml = build_create_live_channel_xml(p, config);
    cos_list_init(body);
    b = cos_buf_pack(p, live_channel_xml, strlen(live_channel_xml));
    cos_list_add_tail(&b->node, body);
}


void cos_live_channel_info_target_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_live_channel_target_t *target)
{
    char *type;
    char *frag_duration;
    char *frag_count;
    char *play_list;
    char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Type", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        type = apr_pstrdup(p, node_content);
        cos_str_set(&target->type, type);
    }

    node = mxmlFindElement(xml_node, xml_node, "FragDuration", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        frag_duration = apr_pstrdup(p, node_content);
        target->frag_duration = atoi(frag_duration);
    }

    node = mxmlFindElement(xml_node, xml_node, "FragCount", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        frag_count = apr_pstrdup(p, node_content);
        target->frag_count = atoi(frag_count);
    }

    node = mxmlFindElement(xml_node, xml_node, "PlaylistName",NULL, NULL,MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        play_list = apr_pstrdup(p, node_content);
        cos_str_set(&target->play_list_name, play_list);
    }
}

void cos_live_channel_info_content_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_live_channel_configuration_t *info)
{
    mxml_node_t *cofig_node;
    mxml_node_t *target_node;

    cofig_node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    if (NULL != cofig_node) {
        char *description;
        char *status;
        char *node_content;
        mxml_node_t *node;

        node = mxmlFindElement(cofig_node, cofig_node, "Description", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            node_content = node->child->value.opaque;
            description = apr_pstrdup(p, node_content);
            cos_str_set(&info->description, description);
        }

        node = mxmlFindElement(cofig_node, cofig_node, "Status", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            node_content = node->child->value.opaque;
            status = apr_pstrdup(p, node_content);
            cos_str_set(&info->status, status);
        }

        target_node = mxmlFindElement(cofig_node, cofig_node, "Target", NULL, NULL, MXML_DESCEND);
        if (NULL != target_node) {
            cos_live_channel_info_target_content_parse(p, target_node, &info->target);
        }
    }
}

int cos_live_channel_info_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_live_channel_configuration_t *info)
{
    int res;
    mxml_node_t *root;
    const char xml_path[] = "LiveChannelConfiguration";

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_live_channel_info_content_parse(p, root, xml_path, info);
        mxmlDelete(root);
    }

    return res;
}

void cos_live_channel_stat_video_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_video_stat_t *video_stat)
{
    char *width;
    char *height;
    char *frame_rate;
    char *band_width;
    char *codec;
    char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Width", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        width = apr_pstrdup(p, node_content);
        video_stat->width = atoi(width);
    }

    node = mxmlFindElement(xml_node, xml_node, "Height", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        height = apr_pstrdup(p, node_content);
        video_stat->height = atoi(height);
    }

    node = mxmlFindElement(xml_node, xml_node, "FrameRate", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        frame_rate = apr_pstrdup(p, node_content);
        video_stat->frame_rate = atoi(frame_rate);
    }

    node = mxmlFindElement(xml_node, xml_node, "Bandwidth", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        band_width = apr_pstrdup(p, node_content);
        video_stat->band_width = atoi(band_width);
    }

    node = mxmlFindElement(xml_node, xml_node, "Codec", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        codec = apr_pstrdup(p, node_content);
        cos_str_set(&video_stat->codec, codec);
    }
}

void cos_live_channel_stat_audio_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_audio_stat_t *audio_stat)
{
    char *band_width;
    char *sample_rate;
    char *codec;
    char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Bandwidth", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        band_width = apr_pstrdup(p, node_content);
        audio_stat->band_width = atoi(band_width);
    }

    node = mxmlFindElement(xml_node, xml_node, "SampleRate", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        sample_rate = apr_pstrdup(p, node_content);
        audio_stat->sample_rate = atoi(sample_rate);
    }

    node = mxmlFindElement(xml_node, xml_node, "Codec", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        codec = apr_pstrdup(p, node_content);
        cos_str_set(&audio_stat->codec, codec);
    }
}

void cos_live_channel_stat_content_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path, cos_live_channel_stat_t *stat)
{
    mxml_node_t *stat_node;

    stat_node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    if (NULL != stat_node) {
        char *status;
        char *connected_time;
        char *remote_addr;
        char *node_content;
        mxml_node_t *node;

        node = mxmlFindElement(stat_node, stat_node, "Status", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            node_content = node->child->value.opaque;
            status = apr_pstrdup(p, (char *)node_content);
            cos_str_set(&stat->pushflow_status, status);
        }

        node = mxmlFindElement(stat_node, stat_node, "ConnectedTime", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            node_content = node->child->value.opaque;
            connected_time = apr_pstrdup(p, (char *)node_content);
            cos_str_set(&stat->connected_time, connected_time);
        }

        node = mxmlFindElement(stat_node, stat_node, "RemoteAddr", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            node_content = node->child->value.opaque;
            remote_addr = apr_pstrdup(p, (char *)node_content);
            cos_str_set(&stat->remote_addr, remote_addr);
        }

        node = mxmlFindElement(stat_node, stat_node, "Video", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            cos_live_channel_stat_video_content_parse(p, node, &stat->video_stat);
        }

        node = mxmlFindElement(stat_node, stat_node, "Audio", NULL, NULL, MXML_DESCEND);
        if (NULL != node) {
            cos_live_channel_stat_audio_content_parse(p, node, &stat->audio_stat);
        }
    }
}

int cos_live_channel_stat_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_live_channel_stat_t *stat)
{
    int res;
    mxml_node_t *root;
    const char xml_path[] = "LiveChannelStat";

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_live_channel_stat_content_parse(p, root, xml_path, stat);
        mxmlDelete(root);
    }

    return res;
}

void cos_list_live_channel_content_parse(cos_pool_t *p, mxml_node_t *xml_node, cos_live_channel_content_t *content)
{
    char *name;
    char *description;
    char *status;
    char *last_modified;
    char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "Name", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        name = apr_pstrdup(p, (char *)node_content);
        cos_str_set(&content->name, name);
    }

    node = mxmlFindElement(xml_node, xml_node, "Description", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        if (NULL != node->child) {
            node_content = node->child->value.opaque;
            description = apr_pstrdup(p, (char *)node_content);
            cos_str_set(&content->description, description);
        } else {
            cos_str_set(&content->description, "");
        }
    }

    node = mxmlFindElement(xml_node, xml_node, "Status", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        status = apr_pstrdup(p, (char *)node_content);
        cos_str_set(&content->status, status);
    }

    node = mxmlFindElement(xml_node, xml_node, "LastModified", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        last_modified = apr_pstrdup(p, (char *)node_content);
        cos_str_set(&content->last_modified, last_modified);
    }

    node = mxmlFindElement(xml_node, xml_node, "PublishUrls", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        cos_publish_urls_contents_parse(p, node, "Url", &content->publish_url_list);
    }

    node = mxmlFindElement(xml_node, xml_node, "PlayUrls", NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        cos_play_urls_contents_parse(p, node, "Url", &content->play_url_list);
    }
}

void cos_list_live_channel_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *live_channel_list)
{
    mxml_node_t *content_node;
    cos_live_channel_content_t *content;

    content_node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; content_node != NULL; ) {
        content = cos_create_list_live_channel_content(p);
        cos_list_live_channel_content_parse(p, content_node, content);
        cos_list_add_tail(&content->node, live_channel_list);
        content_node = mxmlFindElement(content_node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

int cos_list_live_channel_parse_from_body(cos_pool_t *p, cos_list_t *bc,
    cos_list_t *live_channel_list, cos_string_t *next_marker, int *truncated)
{
    int res;
    mxml_node_t *root;
    const char next_marker_xml_path[] = "NextMarker";
    const char truncated_xml_path[] = "IsTruncated";
    const char live_channel_xml_path[] = "LiveChannel";
    char *next_partnumber_marker;

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        next_partnumber_marker = get_xmlnode_value(p, root, next_marker_xml_path);
        if (next_partnumber_marker) {
            cos_str_set(next_marker, next_partnumber_marker);
        }

        *truncated = get_truncated_from_xml(p, root, truncated_xml_path);

        cos_list_live_channel_contents_parse(p, root, live_channel_xml_path, live_channel_list);

        mxmlDelete(root);
    }

    return res;
}

void cos_live_channel_history_content_parse(cos_pool_t *p, mxml_node_t * xml_node,
    cos_live_record_content_t *content)
{
    char *start_time;
    char *end_time;
    char *remote_addr;
    char *node_content;
    mxml_node_t *node;

    node = mxmlFindElement(xml_node, xml_node, "StartTime",NULL, NULL, MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        start_time = apr_pstrdup(p, (char *)node_content);
        cos_str_set(&content->start_time, start_time);
    }

    node = mxmlFindElement(xml_node, xml_node, "EndTime",NULL, NULL,MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        end_time = apr_pstrdup(p, (char *)node_content);
        cos_str_set(&content->end_time, end_time);
    }

    node = mxmlFindElement(xml_node, xml_node, "RemoteAddr",NULL, NULL,MXML_DESCEND);
    if (NULL != node) {
        node_content = node->child->value.opaque;
        remote_addr = apr_pstrdup(p, (char *)node_content);
        cos_str_set(&content->remote_addr, remote_addr);
    }
}

void cos_live_channel_history_contents_parse(cos_pool_t *p, mxml_node_t *root, const char *xml_path,
    cos_list_t *live_record_list)
{
    mxml_node_t *node;
    cos_live_record_content_t *content;

    node = mxmlFindElement(root, root, xml_path, NULL, NULL, MXML_DESCEND);
    for ( ; node != NULL; ) {
        content = cos_create_live_record_content(p);
        cos_live_channel_history_content_parse(p, node, content);
        cos_list_add_tail(&content->node, live_record_list);
        node = mxmlFindElement(node, root, xml_path, NULL, NULL, MXML_DESCEND);
    }
}

int cos_live_channel_history_parse_from_body(cos_pool_t *p, cos_list_t *bc, cos_list_t *live_record_list)
{
    int res;
    mxml_node_t *root = NULL;
    const char rule_xml_path[] = "LiveRecord";

    res = get_xmldoc(bc, &root);
    if (res == COSE_OK) {
        cos_live_channel_history_contents_parse(p, root, rule_xml_path, live_record_list);
        mxmlDelete(root);
    }

    return res;
}
#endif

char *build_objects_xml(cos_pool_t *p, cos_list_t *object_list, const char *quiet)
{
    char *object_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    cos_object_key_t *content;
    mxml_node_t *quiet_node;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "Delete");
    quiet_node = mxmlNewElement(root_node, "Quiet");
    mxmlNewText(quiet_node, 0, quiet);
    cos_list_for_each_entry(cos_object_key_t, content, object_list, node) {
        mxml_node_t *object_node = mxmlNewElement(root_node, "Object");
        mxml_node_t *key_node = mxmlNewElement(object_node, "Key");
        mxmlNewText(key_node, 0, content->key.data);
    }

    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    object_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return object_xml;
}

void build_delete_objects_body(cos_pool_t *p, cos_list_t *object_list, int is_quiet, cos_list_t *body)
{
    char *objects_xml;
    cos_buf_t *b;
    char *quiet;
    quiet = is_quiet > 0 ? "true": "false";
    objects_xml = build_objects_xml(p, object_list, quiet);
    cos_list_init(body);
    b = cos_buf_pack(p, objects_xml, strlen(objects_xml));
    cos_list_add_tail(&b->node, body);
}

mxml_node_t	*set_xmlnode_value_str(mxml_node_t *parent, const char *name, const cos_string_t *value)
{
    mxml_node_t *node;
    char buff[COS_MAX_XML_NODE_VALUE_LEN];
    node = mxmlNewElement(parent, name);
    apr_snprintf(buff, COS_MAX_XML_NODE_VALUE_LEN, "%.*s", value->len, value->data);
    return mxmlNewText(node, 0, buff);
}

mxml_node_t	*set_xmlnode_value_int(mxml_node_t *parent, const char *name, int value)
{
    mxml_node_t *node;
    char buff[COS_MAX_INT64_STRING_LEN];
    node = mxmlNewElement(parent, name);
    apr_snprintf(buff, COS_MAX_INT64_STRING_LEN, "%d", value);
    return mxmlNewText(node, 0, buff);
}

mxml_node_t	*set_xmlnode_value_int64(mxml_node_t *parent, const char *name, int64_t value)
{
    mxml_node_t *node;
    char buff[COS_MAX_INT64_STRING_LEN];
    node = mxmlNewElement(parent, name);
    apr_snprintf(buff, COS_MAX_INT64_STRING_LEN, "%" APR_INT64_T_FMT, value);
    return mxmlNewText(node, 0, buff);
}

int get_xmlnode_value_str(cos_pool_t *p, mxml_node_t *xml_node, const char *xml_path, cos_string_t *value)
{
    char *node_content;
    node_content = get_xmlnode_value(p, xml_node, xml_path);
    if (NULL == node_content) {
        return COS_FALSE;
    }
    cos_str_set(value, node_content);
    return COS_TRUE;
}

int get_xmlnode_value_int(cos_pool_t *p, mxml_node_t *xml_node, const char *xml_path, int *value)
{
    char *node_content;
    node_content = get_xmlnode_value(p, xml_node, xml_path);
    if (NULL == node_content) {
        return COS_FALSE;
    }
    *value = atoi(node_content);
    return COS_TRUE;
}

int get_xmlnode_value_int64(cos_pool_t *p, mxml_node_t *xml_node, const char *xml_path, int64_t *value)
{
    char *node_content;
    node_content = get_xmlnode_value(p, xml_node, xml_path);
    if (NULL == node_content) {
        return COS_FALSE;
    }
    *value = cos_atoi64(node_content);
    return COS_TRUE;
}

char *cos_build_checkpoint_xml(cos_pool_t *p, const cos_checkpoint_t *checkpoint)
{
    char *checkpoint_xml;
    char *xml_buff;
    cos_string_t xml_doc;
    mxml_node_t *doc;
    mxml_node_t *root_node;
    mxml_node_t *local_node;
    mxml_node_t *object_node;
    mxml_node_t *cpparts_node;
    mxml_node_t *parts_node;
    int i = 0;

    doc = mxmlNewXML("1.0");
    root_node = mxmlNewElement(doc, "Checkpoint");

    // MD5
    set_xmlnode_value_str(root_node, "MD5", &checkpoint->md5);

    // Type
    set_xmlnode_value_int(root_node, "Type", checkpoint->cp_type);

    // LocalFile
    local_node = mxmlNewElement(root_node, "LocalFile");
    // LocalFile.Path
    set_xmlnode_value_str(local_node, "Path", &checkpoint->file_path);
    // LocalFile.Size
    set_xmlnode_value_int64(local_node, "Size", checkpoint->file_size);
    // LocalFile.LastModified
    set_xmlnode_value_int64(local_node, "LastModified", checkpoint->file_last_modified);
    // LocalFile.MD5
    set_xmlnode_value_str(local_node, "MD5", &checkpoint->file_md5);

    // Object
    object_node = mxmlNewElement(root_node, "Object");
    // Object.Key
    set_xmlnode_value_str(object_node, "Key", &checkpoint->object_name);
    // Object.Size
    set_xmlnode_value_int64(object_node, "Size", checkpoint->object_size);
    // Object.LastModified
    set_xmlnode_value_str(object_node, "LastModified", &checkpoint->object_last_modified);
    // Object.ETag
    set_xmlnode_value_str(object_node, "ETag", &checkpoint->object_etag);

    // UploadId
    set_xmlnode_value_str(root_node, "UploadId", &checkpoint->upload_id);

    // CpParts
    cpparts_node = mxmlNewElement(root_node, "CPParts");
    // CpParts.Number
    set_xmlnode_value_int(cpparts_node, "Number", checkpoint->part_num);
    // CpParts.Size
    set_xmlnode_value_int64(cpparts_node, "Size", checkpoint->part_size);
    // CpParts.Parts
    parts_node = mxmlNewElement(cpparts_node, "Parts");
    for (i = 0; i < checkpoint->part_num; i++) {
        mxml_node_t *part_node = mxmlNewElement(parts_node, "Part");
        set_xmlnode_value_int(part_node, "Index", checkpoint->parts[i].index);
        set_xmlnode_value_int64(part_node, "Offset", checkpoint->parts[i].offset);
        set_xmlnode_value_int64(part_node, "Size", checkpoint->parts[i].size);
        set_xmlnode_value_int(part_node, "Completed", checkpoint->parts[i].completed);
        set_xmlnode_value_str(part_node, "ETag", &checkpoint->parts[i].etag);
    }

    // dump
    xml_buff = new_xml_buff(doc);
    if (xml_buff == NULL) {
        return NULL;
    }
    cos_str_set(&xml_doc, xml_buff);
    checkpoint_xml = cos_pstrdup(p, &xml_doc);

    free(xml_buff);
    mxmlDelete(doc);

    return checkpoint_xml;
}

int cos_checkpoint_parse_from_body(cos_pool_t *p, const char *xml_body, cos_checkpoint_t *checkpoint)
{
    mxml_node_t *root;
    mxml_node_t *local_node;
    mxml_node_t *object_node;
    mxml_node_t *cpparts_node;
    mxml_node_t *parts_node;
    mxml_node_t *node;
    int index = 0;

    root = mxmlLoadString(NULL, xml_body, MXML_OPAQUE_CALLBACK);
    if (NULL == root) {
        return COSE_XML_PARSE_ERROR;
    }

    // MD5
    get_xmlnode_value_str(p, root, "MD5", &checkpoint->md5);

    // Type
    get_xmlnode_value_int(p, root, "Type", &checkpoint->cp_type);

    // LocalFile
    local_node = mxmlFindElement(root, root, "LocalFile", NULL, NULL, MXML_DESCEND);
    // LocalFile.Path
    get_xmlnode_value_str(p, local_node, "Path", &checkpoint->file_path);
    // LocalFile.Size
    get_xmlnode_value_int64(p, local_node, "Size", &checkpoint->file_size);
    // LocalFile.LastModified
    get_xmlnode_value_int64(p, local_node, "LastModified", &checkpoint->file_last_modified);
    // LocalFile.MD5
    get_xmlnode_value_str(p, local_node, "MD5", &checkpoint->file_md5);

    // Object
    object_node = mxmlFindElement(root, root, "Object", NULL, NULL, MXML_DESCEND);
    // Object.Key
    get_xmlnode_value_str(p, object_node, "Key", &checkpoint->object_name);
    // Object.Size
    get_xmlnode_value_int64(p, object_node, "Size", &checkpoint->object_size);
    // Object.LastModified
    get_xmlnode_value_str(p, object_node, "LastModified", &checkpoint->object_last_modified);
    // Object.ETag
    get_xmlnode_value_str(p, object_node, "ETag", &checkpoint->object_etag);

    // UploadId
    get_xmlnode_value_str(p, root, "UploadId", &checkpoint->upload_id);

    // CpParts
    cpparts_node = mxmlFindElement(root, root, "CPParts", NULL, NULL, MXML_DESCEND);
    // CpParts.Number
    get_xmlnode_value_int(p, cpparts_node, "Number", &checkpoint->part_num);
    // CpParts.Size
    get_xmlnode_value_int64(p, cpparts_node, "Size", &checkpoint->part_size);
    // CpParts.Parts
    parts_node = mxmlFindElement(cpparts_node, cpparts_node, "Parts", NULL, NULL, MXML_DESCEND);
    node = mxmlFindElement(parts_node, parts_node, "Part", NULL, NULL, MXML_DESCEND);
    for ( ; node != NULL; ) {
        get_xmlnode_value_int(p, node, "Index", &index);
        checkpoint->parts[index].index = index;
        get_xmlnode_value_int64(p, node, "Offset", &checkpoint->parts[index].offset);
        get_xmlnode_value_int64(p, node, "Size", &checkpoint->parts[index].size);
        get_xmlnode_value_int(p, node, "Completed", &checkpoint->parts[index].completed);
        get_xmlnode_value_str(p, node, "ETag", &checkpoint->parts[index].etag);
        node = mxmlFindElement(node, parts_node, "Part", NULL, NULL, MXML_DESCEND);
    }

    mxmlDelete(root);

    return COSE_OK;
}
