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

#include "cos_log.h"
#include "cos_sys_util.h"
#include "cos_status.h"

const char COS_XML_PARSE_ERROR_CODE[] = "ParseXmlError";
const char COS_OPEN_FILE_ERROR_CODE[] = "OpenFileFail";
const char COS_WRITE_FILE_ERROR_CODE[] = "WriteFileFail";
const char COS_HTTP_IO_ERROR_CODE[] = "HttpIoError";
const char COS_UNKNOWN_ERROR_CODE[] = "UnknownError";
const char COS_CLIENT_ERROR_CODE[] = "ClientError";
const char COS_UTF8_ENCODE_ERROR_CODE[] = "Utf8EncodeFail";
const char COS_URL_ENCODE_ERROR_CODE[] = "UrlEncodeFail";
const char COS_INCONSISTENT_ERROR_CODE[] = "InconsistentError";
const char COS_CREATE_QUEUE_ERROR_CODE[] = "CreateQueueFail";
const char COS_CREATE_THREAD_POOL_ERROR_CODE[] = "CreateThreadPoolFail";
const char COS_LACK_OF_CONTENT_LEN_ERROR_CODE[] = "LackOfContentLength";


cos_status_t *cos_status_create(cos_pool_t *p)
{
    return (cos_status_t *)cos_pcalloc(p, sizeof(cos_status_t));
}

cos_status_t *cos_status_dup(cos_pool_t *p, cos_status_t *src)
{
    cos_status_t *dst = cos_status_create(p);
    dst->code = src->code;
    dst->error_code = apr_pstrdup(p, src->error_code);
    dst->error_msg = apr_pstrdup(p, src->error_msg);
    return dst;
}

int cos_should_retry(cos_status_t *s) {
    int cos_error_code = 0;

    if (s == NULL || s->code / 100 == 2) {
        return COS_FALSE;
    }

    if (s->code / 100 == 5) {
        return COS_TRUE;
    }

    if (s->error_code != NULL) {
        cos_error_code = atoi(s->error_code);
        if (cos_error_code == COSE_CONNECTION_FAILED || cos_error_code == COSE_REQUEST_TIMEOUT ||
            cos_error_code == COSE_FAILED_CONNECT || cos_error_code == COSE_SERVICE_ERROR) {
            return COS_TRUE;
        }
    }

    return COS_FALSE;
}

cos_status_t *cos_status_parse_from_body(cos_pool_t *p, cos_list_t *bc, int code, cos_status_t *s)
{
    int res;
    mxml_node_t *root, *node;
    mxml_node_t *code_node, *message_node;
    const char *node_content;

    if (s == NULL) {
        s = cos_status_create(p);
    }
    s->code = code;

    if (cos_http_is_ok(code)) {
        return s;
    }

    if (cos_list_empty(bc)) {
        s->error_code = (char *)COS_UNKNOWN_ERROR_CODE;
        return s;
    }

    if ((res = cos_parse_xml_body(bc, &root)) != COSE_OK) {
        s->error_code = (char *)COS_UNKNOWN_ERROR_CODE;
        return s;
    }

    node = mxmlFindElement(root, root, "Error",NULL, NULL,MXML_DESCEND);
    if (NULL == node) {
        char *xml_content = cos_buf_list_content(p, bc);
        cos_error_log("Xml format invalid, root node name is not Error.\n");
        cos_error_log("Xml Content:%s\n", xml_content);
        s->error_code = (char *)COS_UNKNOWN_ERROR_CODE;
        mxmlDelete(root);
        return s;
    }

    code_node = mxmlFindElement(node, root, "Code",NULL, NULL,MXML_DESCEND);
    node_content = mxmlGetOpaque(code_node);
    if (node_content != NULL) {
        s->error_code = apr_pstrdup(p, node_content);
    }

    message_node = mxmlFindElement(node, root, "Message",NULL, NULL,MXML_DESCEND);
    node_content = mxmlGetOpaque(message_node);
    if (node_content != NULL) {
        s->error_msg = apr_pstrdup(p, node_content);
    }

    mxmlDelete(root);

    return s;
}
