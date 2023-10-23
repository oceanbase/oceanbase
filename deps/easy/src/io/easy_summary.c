#include "io/easy_summary.h"
#include "packet/http/easy_http_handler.h"
#include "util/easy_time.h"
#include "util/easy_string.h"

//http content
#define EASY_SUMMARY_NODE_DIFF(a,b,c,field) a->field = ((b->field >= c->field) ? (b->field - c->field) : 0)
#define EASY_SUMMARY_HTTP_CONTENT                                                   \
    "<html><title>SUMMARY</title><body><br><table border=0"                         \
    "cellspacing=0 cellpadding=3 width=\"100%\"><tr bgcolor=8899AA><td>"            \
    "<center><b><font color=black>Original Summary Data</font></b></center>"        \
    "</td></tr></table><table bgcolor=blue border=0 cellspacing=1 cellpadding=3 "   \
    "width=\"100%\"><tr bgcolor=99cc99>"                                            \
    "<TH>FD</TH><TH>ADDRESS</TH><TH>IN_BYTE"                                        \
    "</TH><TH>OUT_BYTE</TH><TH>DOING_REQ_CNT</TH><TH>DONE_REQ_CNT</TH>"             \
    "<TH>QPS</TH><TH>PROCESS_TIME(ms)</TH></TR>"
#define EASY_SUMMARY_HTTP_LIST                                                      \
    "<TR bgcolor=white><TH>%d</TH><TH>%s</TH><TH>%s /s</TH><TH>%s /s</TH><TH>%d"    \
    "</TH><TH>%" PRId64 "</TH><TH>%.2f</TH><TH>%.3f</TH></TR>"
#define EASY_SUMMARY_HTTP_FOOT                                                      \
    "<TR bgcolor=cccccc><TH>TOTAL: %d</TH><TH>&nbsp;</TH><TH>%s /s</TH>"            \
    "<TH>%s /s</TH><TH>%d</TH><TH>%" PRId64 "</TH><TH>%.2f</TH><TH>%.3f</TH></TR>"

static easy_summary_node_t *easy_summary_insert_node(int fd, easy_summary_t *sum);
easy_summary_node_t     easy_summary_node_null = {0};

/**
 * create easy_summary_t
 */
easy_summary_t *easy_summary_create()
{
    easy_summary_t          *sum;
    easy_pool_t             *pool;

    if ((pool = easy_pool_create(sizeof(easy_summary_t))) == NULL)
        return NULL;

    sum = (easy_summary_t *)easy_pool_calloc(pool, sizeof(easy_summary_t));
    sum->pool = pool;

    return sum;
}

/**
 * easy_summary_destroy
 */
void easy_summary_destroy(easy_summary_t *sum)
{
    if (sum && sum->pool) {
        easy_pool_destroy(sum->pool);
    }
}

/**
 *locate the place
 */
easy_summary_node_t *easy_summary_locate_node(int fd, easy_summary_t *sum, int hidden)
{
    easy_summary_node_t     *sum_node, *node;
    int                     idx;

    if (fd > 65535 || fd < 0) {
        return &easy_summary_node_null;
    }

    if (sum == NULL || sum->pool == NULL) {
        easy_error_log("sum or sum->pool is  NULL \n");
        return &easy_summary_node_null;
    }

    //find node and add fd_total
    idx = fd >> EASY_SUMMARY_LENGTH_BIT;

    if ((sum_node = sum->bucket[idx]) == NULL) {
        // lock
        easy_spin_lock(&sum->lock);

        if ((sum_node = sum->bucket[idx]) == NULL) {
            if ((sum_node = easy_summary_insert_node(idx, sum)) == &(easy_summary_node_null)) {
                easy_spin_unlock(&sum->lock);
                return &easy_summary_node_null;
            }
            memset(sum_node, 0, sizeof(easy_summary_node_t) * EASY_SUMMARY_LENGTH);
        }

        // unlock
        easy_spin_unlock(&sum->lock);
    }

    node = sum_node + (fd & EASY_SUMMARY_LENGTH_MASK);
    node->doing_request_count = 0;

    // max fd, FIXME: ֻ?�?
    if (fd > sum->max_fd) sum->max_fd = fd;

    node->fd = (hidden ? -fd : fd);

    return node;
}

/**
 *clean and destroy node
 */
void easy_summary_destroy_node(int fd, easy_summary_t *sum)
{
    easy_summary_node_t     *sum_node, *node;

    //for ueser ,be selfsafe
    if (fd > 65535 || fd < 0) {
        return;
    }

    if (sum == NULL || sum->pool == NULL) {
        easy_error_log("sum or pool is NULL \n");
        return;
    }

    //find node and clean
    sum_node = sum->bucket[fd >> EASY_SUMMARY_LENGTH_BIT];

    if (sum_node == NULL) {
        return;
    }

    node = sum_node + (fd & EASY_SUMMARY_LENGTH_MASK);
    node->fd = 0;
}

/**
 * copy to dest
 */
void easy_summary_copy(easy_summary_t *src, easy_summary_t *dest)
{
    int                     i, size, max;
    easy_summary_node_t     *node, *newnode;

    // size
    size = sizeof(easy_summary_node_t) * EASY_SUMMARY_LENGTH;
    max = (src->max_fd >> EASY_SUMMARY_LENGTH_BIT);

    for(i = 0; i <= max; i++) {
        if ((node = src->bucket[i]) == NULL)
            continue;

        if ((newnode = dest->bucket[i]) == NULL)
            newnode = easy_summary_insert_node(i, dest);

        memcpy(newnode, node, size);
    }

    dest->max_fd = src->max_fd;
}

/**
 * diff easy_summary_t
 */
easy_summary_t *easy_summary_diff(easy_summary_t *ns, easy_summary_t *os)
{
    int                     i, j, max;
    easy_summary_t          *diff;
    easy_summary_node_t     *onode, *nnode, *dnode;

    // size
    diff = easy_summary_create();
    diff->max_fd = easy_max(os->max_fd, ns->max_fd);
    max = (diff->max_fd >> EASY_SUMMARY_LENGTH_BIT);

    for(i = 0; i <= max; i++) {
        onode = os->bucket[i];
        nnode = ns->bucket[i];

        if (nnode == NULL)
            continue;

        // new diff
        dnode = easy_summary_insert_node(i, diff);

        // diff
        if (onode == NULL) {
            memcpy(dnode, nnode, EASY_SUMMARY_LENGTH * sizeof(easy_summary_node_t));
        } else {
            for(j = 0; j < EASY_SUMMARY_LENGTH; j++) {
                dnode->fd = nnode->fd;
                EASY_SUMMARY_NODE_DIFF(dnode, nnode, onode, rt_total);
                EASY_SUMMARY_NODE_DIFF(dnode, nnode, onode, doing_request_count);
                EASY_SUMMARY_NODE_DIFF(dnode, nnode, onode, done_request_count);
                EASY_SUMMARY_NODE_DIFF(dnode, nnode, onode, in_byte);
                EASY_SUMMARY_NODE_DIFF(dnode, nnode, onode, out_byte);

                dnode ++;
                nnode ++;
                onode ++;
            }
        }
    }

    diff->time = ns->time - os->time;

    return diff;
}

/**
 *http request html output
 */
void easy_summary_html_output(easy_pool_t *pool, easy_list_t *bc, easy_summary_t *sum, easy_summary_t *last)
{
    easy_buf_t              *b;
    easy_summary_node_t     *node, *lnode;
    int                     m, k, max, l, doing_rc;
    int64_t                 done_rc, done_request_count;
    easy_addr_t             addr;
    easy_summary_node_t     ts;
    char                    buffer[32], inbytes[32], outbytes[32];

    // header
    memset(&ts, 0, sizeof(easy_summary_node_t));
    b = easy_buf_check_write_space(pool, bc, sizeof(EASY_SUMMARY_HTTP_CONTENT));
    b->last += lnprintf(b->last, b->end - b->last, "%s\n", EASY_SUMMARY_HTTP_CONTENT);
    done_request_count = 0;

    max = (sum->max_fd >> EASY_SUMMARY_LENGTH_BIT);

    for (k = 0; k <= max; k++) {
        if ((node = sum->bucket[k]) == NULL)
            continue;

        lnode = last->bucket[k];

        for (m = 0; m < EASY_SUMMARY_LENGTH; m++, node++) {
            if (node->fd <= 0)
                continue;

            addr = easy_inet_getpeername(node->fd);

            // total
            ts.fd ++;
            ts.rt_total += node->rt_total;
            doing_rc = done_rc = 0;

            if (lnode) {
                doing_rc = lnode[m].doing_request_count;
                done_rc = lnode[m].done_request_count;
            }

            ts.doing_request_count += doing_rc;
            ts.done_request_count += done_rc;
            ts.in_byte += node->in_byte;
            ts.out_byte += node->out_byte;
            done_request_count += node->done_request_count;

            //html output node
            b = easy_buf_check_write_space(pool, bc, sizeof(EASY_SUMMARY_HTTP_LIST) * 2);
            easy_string_format_size(easy_div(node->in_byte, sum->time), inbytes, 32);
            easy_string_format_size(easy_div(node->out_byte, sum->time), outbytes, 32);
            l = lnprintf(b->last, b->end - b->last,
                         EASY_SUMMARY_HTTP_LIST,
                         node->fd, easy_inet_addr_to_str(&addr, buffer, 32),
                         inbytes, outbytes,
                         doing_rc, done_rc,
                         easy_div(node->done_request_count, sum->time),
                         easy_div(node->rt_total * 1000, node->done_request_count));
            b->last += l;
        }
    }

    // total
    b = easy_buf_check_write_space(pool, bc, sizeof(EASY_SUMMARY_HTTP_LIST) * 2);
    easy_string_format_size(easy_div(ts.in_byte, sum->time), inbytes, 32);
    easy_string_format_size(easy_div(ts.out_byte, sum->time), outbytes, 32);
    l = lnprintf(b->last, b->end - b->last,
                 EASY_SUMMARY_HTTP_FOOT,
                 ts.fd, inbytes, outbytes,
                 ts.doing_request_count, ts.done_request_count,
                 easy_div(done_request_count, sum->time),
                 easy_div(ts.rt_total * 1000, done_request_count));
    b->last += l;

    b->last += sprintf(b->last, "</table><br></body></html>");
}

// print raw
void easy_summary_raw_output(easy_pool_t *pool, easy_list_t *bc, easy_summary_t *sum, const char *desc)
{
    easy_buf_t              *b;
    easy_summary_node_t     *node;
    int                     m, k, max, l;
    easy_addr_t             addr;
    char                    buffer[32];

    // header
    max = (sum->max_fd >> EASY_SUMMARY_LENGTH_BIT);
    b = easy_buf_check_write_space(pool, bc, 256);
    b->last += lnprintf(b->last, b->end - b->last, "<pre>\n%s\ntime=%.3f\n", desc, sum->time);

    for (k = 0; k <= max; k++) {
        if ((node = sum->bucket[k]) == NULL)
            continue;

        for (m = 0; m < EASY_SUMMARY_LENGTH; m++, node++) {
            if (node->fd == 0)
                continue;

            addr = easy_inet_getpeername((node->fd < 0 ? -node->fd : node->fd));

            //raw output node
            b = easy_buf_check_write_space(pool, bc, 256);
            l = lnprintf(b->last, b->end - b->last,
                         "fd=%d,addr=%s,inb=%" PRId64 ",outb=%" PRId64 ",doing=%d,done=%" PRId64 ",rt=%.3f\n",
                         node->fd, easy_inet_addr_to_str(&addr, buffer, 32),
                         node->in_byte, node->out_byte,
                         node->doing_request_count, node->done_request_count,
                         node->rt_total);
            b->last += l;
        }
    }

    b = easy_buf_check_write_space(pool, bc, 256);
    b->last += lnprintf(b->last, b->end - b->last, "</pre>\n");
}


///////////////////////////////////////////////////////////////////////////////////////////////////
/**
 *insert node
 */
static easy_summary_node_t *easy_summary_insert_node(int idx, easy_summary_t *sum)
{
    easy_summary_node_t         *node;

    // for locate use
    int                     size = sizeof(easy_summary_node_t) * EASY_SUMMARY_LENGTH;
    if ((node = (easy_summary_node_t *) easy_pool_alloc(sum->pool, size)) == NULL) {
        return &easy_summary_node_null;
    }
    sum->bucket[idx] = node;

    return node;
}
