
目录
    1. 数据包的处理过程
        见图 http://baike.corp.taobao.com/index.php/File:Easy%E6%95%B0%E6%8D%AE%E6%B5%81%E7%A8%8B%E5%9B%BE.png

    2. 主要数据类型（src/io/easy_io_struct.h中的数据结构）

    3. 服务器端和客户端程序使用api的步骤

    4. 主要io接口（src/io/文件夹）

    5. include包（src/include/文件夹）

    6. util包（src/util/文件夹）

1. 数据包的处理过程

    1.1 服务器端调用过程
        /**
         * accept 事件处理
         * 1. 为新连接创建一个easy_connection_t对象
         *    初始化c->read_watcher = easy_connection_on_readable
         *    初始化c->write_watcher = easy_connection_on_writable
         *    初始化c->timeout_watcher = easy_connection_on_timeout_conn
         * 2. accept请求
         * 3. 调用easy_connection_on_readable
         **/
        void easy_connection_on_accept(struct ev_loop *loop, ev_io *w, int revents)

        /**
         * read事件处理
         * 1. 第一次读或者上次读完整了, 新建一个easy_message_t
         * 2. 从conn里读入数据
         * 3. 调用easy_connection_do_request处理请求
         */
        easy_connection_on_readable(struct ev_loop *loop, ev_io *w, int revents)

        /**
         * 处理请求
         * 1. 对请求decode，返回packet
         * 2. 创建easy_request_t，加入到m->request_list列表里
         * 3. batch_process操作
         * 4. 对request_list列表里的请求调用process进行处理
         * 5. 1）对处理返回EASY_OK的，调用encode返回响应
         *    2）处理返回EASY_AGAIN等待process中的回调函数，在回调函数中调用easy_request_do_reply
         */
        int easy_connection_do_request(easy_message_t *m)

        /**
         * 在process中异步处理通过回调函数返回
         * 1. 通过encode把 easy_request_t->packet里面的回复信息添加到easy_connection_t->output链里面
         * 2. 调用easy_connection_write_socket把easy_connection_t->output链里面的数据写到socket里面
         */
        easy_request_do_reply(easy_request_t *r)

                                图1 easy数据流程图
    1.2 客户端调用过程

    1.2.1 客户端发送请求步骤
        /**
         * 连接。
         * 1. connect到服务器
         * 2. 创建一个easy_connection_t对象，初始化easy_connection_t对象。
         *    初始化c->read_watcher = easy_connection_on_readable
         *    初始化c->write_watcher = easy_connection_on_writable
         *    初始化c->timeout_watcher = easy_connection_on_timeout_conn
         */
        easy_connection_t *easy_connection_connect_addr(easy_io_t *eio, easy_addr_t addrv, easy_io_handler_pt *handler)

        /**
         * write事件处理
         * 1. 调用new_packet创建packet
         * 2. 创建easy_session_t和packet
         * 3. 调用easy_connection_send_session发送数据
         */
        static void easy_connection_on_writable(struct ev_loop *loop, ev_io *w, int revents)

        /**
         * 1. 对easy_session_t进行easy_connection_sesson_build
         *    在easy_connection_session_build里面对请求进行encode
         * 2. 调用easy_connection_write_socket把encode好的数据写到socket里面
         */
        int easy_connection_send_session(easy_session_t *s)

        /**
         * 1. 在easy_connection_sesson_build里面对请求进行encode
         * 2. 以packet_id为key，把easy_session_t加入到c->send_queue哈希表里面
         */
        void easy_connection_session_build(easy_session_t *s)

    1.2.2 客户端接收响应步骤
        /**
         * read事件处理
         * 1. 第一次读或者上次读完整了, 新建一个easy_message_t
         * 2. 从conn里读入数据
         * 3. 调用easy_connection_do_response处理响应
         */
        easy_connection_on_readable(struct ev_loop *loop, ev_io *w, int revents)

        /**
         * 处理响应
         * 1. 对服务器返回的响应进行decode操作
         * 2. 通过获取的packet_id从c->send_queue哈希表中得到easy_session_t
         * 2. 调用easy_session_process处理返回的响应
         * 3. 如果客户端没有发送完请求，创建新的packet继续请求服务器
         */
        static int easy_connection_do_response(easy_message_t *m)

        /**
         * 1. 对响应进行处理
         */
        int easy_session_process(easy_session_t *s)

2 主要操作的类型
    2.1
    struct easy_io_handler_pt {
        // 1.在服务器端，用于解析客户端请求。
        // 2.在客户端端，用于解析服务器返回的响应
        void*               (*decode)(easy_message_t *m);

        // 1.服务器端，packet是decode的输出，把数据封装后添加到output里
        // 2.客户端端，用于封装new_packet创建的packet，然后填加到output里
        int                 (*encode)(easy_buf_chain_t *output, void *packet);
        easy_io_process_pt   *process;
        int                 (*batch_process)(easy_message_t *m);
        int                 (*cleanup)(void *packet);
        // 获取packet_id
        uint64_t            (*get_packet_id)(easy_connection_t *c, void *packet);//
        int                 (*on_connect) (easy_connection_t *c);      //

        // 在销毁easy_connection_t的时候，释放掉easy_io_handler_pt包含的数据
        int              (*on_disconnect) (easy_connection_t *c);

        // 创建用于请求的packet和easy_session_t
        // easy_connection_send_session发送请求
        int                 (*new_packet) (easy_connection_t *c);                //
        void                *user_data, *user_data2;                             //
        int                 is_uthread;
    };

    // 对应一个SOCKET连接
    struct easy_connection_t {
        struct ev_loop          *loop;
        easy_pool_t             *pool;            // 用于分配在connect存活周期对象的空间，easy_connection_t本身也由pool分配的
        easy_pool_t             *pbuf;            // 用于创建packet easy_buf_t
        easy_atomic_t           ref;              //
        easy_io_thread_t        *ioth;            // 每个connect某一时刻在一个线程执行，connect满足一定要求是可以切换到其他线程执行，ioth用于保存线程的指针
        easy_connection_t       *next;            // 没次accept会创建一个connetect，next指向下一个connect
        easy_list_t             conn_list_node;   // 加入到ioth中connect list的节点
        easy_hash_list_t        client_list_node; // 用于client端

        // file description
        uint32_t                default_message_len;
        int                     fd;
        easy_addr_t             addr;

        ev_io                   read_watcher;     // 在client端是easy_connection_on_readable，server端是easy_connection_on_accept/easy_connection_on_readable
        ev_io                   write_watcher;    //
        ev_timer                timeout_watcher;  //
        easy_list_t             message_list;     // 用于保存easy_message_t的链表。接收请求/响应时会用到easy_message_t，保存一个或者多easy_request_t
        easy_list_t             server_session_list;     // 用于保存easy_session_t的链表。easy_session_t是用于发送请求/响应的，每个easy_session_t只带一个easy_request_t

        easy_buf_chain_t        output;           // 输出缓存链， output.pool = pbuf。
        easy_io_handler_pt      *handler;         // 操作类型， 包括decode,encode, process, batch_process, on_connect等等
        void                    *user_data;       //
        easy_hash_t             *send_queue;      // 作为client端执行easy_connection_do_response时，用于查找packet_id对应的easy_session_t

        uint32_t                status : 3;           // connect()链接状态
        uint32_t                event_status : 3;     //
        uint32_t                type : 2;             // EASY_TYPE_CLIENT、EASY_TYPE_SERVER
        uint32_t                conn_has_error : 1;   //
        uint32_t                sendfile_flag : 1;    // 标记connect是否已经添加异步sendfile()操作
        uint32_t                tcp_cork_flag : 1;    // set TCP_CORK
        uint32_t                tcp_nodelay_flag : 1; // set TCP_NODELAY
        uint32_t                wait_close : 1;       // 标记关闭链接
        uint32_t                need_redispatch : 1;  // 标记connect需要切换到其他线程执行

        easy_atomic32_t         doing_request_count;  // server端：正在处理的请求的数量。client端：正在等待请求的数量
        easy_atomic_t           done_request_count;   // 处理完请求的数量
        ev_tstamp               start_time;           // 开始时间，double类型
        easy_uthread_t          *uthread;             //user thread
    };

    // easy_message_t和easy_session_t共同的部分，类似于基类
    struct easy_message_session_t {
        easy_connection_t       *c;               //
        easy_pool_t             *pool;            // 在easy_message_t存活期间的内存池
        int8_t                  type；            // 转换成easy_message_session_t时用来判断是EASY_TYPE_MESSAGE或EASY_TYPE_SESSION
        int8_t                  async;            //
    };

    // easy_message_t用来接收请求/响应，由一个或者多easy_request_t组成。
    // easy_message_t是decode的单位，每个easy_message_t可以decode一个或多个请求。
    struct easy_message_t {
        easy_connection_t       *c;               //
        easy_pool_t             *pool;            // 在easy_message_t存活期间的内存池
        int8_t                  type；            // 转换成easy_message_session_t时用来判断是EASY_TYPE_MESSAGE，区别EASY_TYPE_SESSION
        int8_t                  async;            //
        int8_t                  status;           // EASY_MESG_READ_AGAIN、EASY_MESG_WRITE_AGAIN、EASY_ERROR
        easy_atomic32_t         ref;              //

        easy_list_t             message_list_node;  // 用于把easy_message_t添加到c->message_list中的节点
        easy_buf_t              *input;             // 接收数据的缓存区，input->pos是当前decode的位置，input->last接收数据的末尾的下一个位置
        easy_list_t             request_list;       // decode出来的easy_request_t链表（在server端接收请求时用）
        int                     request_list_count; // request_list中easy_request_t的数量
        int                     next_read_len;      // 下一次读取数据的长度，

        void                    *user_data;         //
    };

    // 用于发送, 只带一个easy_request_t
    struct easy_session_t {
        easy_connection_t       *c;               //
        easy_pool_t             *pool;            // 在easy_session_t存活期间的内存池
        int8_t                  type；            // 转换成easy_message_session_t时用来判断是EASY_TYPE_SESSION，区别EASY_TYPE_MESSAGE
        int8_t                  async;            //
        int                     timeout;          //
        ev_timer                timeout_watcher;  //

        easy_list_t             session_list_node;//
        easy_hash_list_t        send_queue_node;  // c->send_queue发送队列hash表里的节点，用于查找packet_id对应的easy_session_t

        uint64_t                packet_id;        // 每个easy_session_t有一个packet_id来标示不同的packet，返回的响应中也要获得packet_id，来去发送的c->send_queue查找对应的packet
        void                    *thread_ptr;      //
        easy_request_t          r;                // easy_session_t对应的请求
        char                    data[0];          //
    };

    // ipacket放进来的包, opacket出去的包
    // easy_request_t在处理请求的时候创建 ms = easy_message_t，在new_packet创建，ms = easy_session_t
    struct easy_request_t {
        easy_message_session_t  *ms;              // 对应的easy_message_t/easy_session_t

        easy_list_t             request_list_node;//
        void                    *ipacket;         // ipacket放进来的包
        void                    *opacket;         // opacket出去的包
        void                    *args;            //
    };


    struct easy_thread_pool_t {
        int                     thread_count;  // 线程数量
        int                     member_size;   // 线程池所包含的线程的size
        easy_atomic32_t         last_number;   // 用于顺序返回线程的计数器
        easy_list_t             list_node;     // 把easy_thread_pool_t加入到eio->thread_pool_list
        easy_thread_pool_t      *next;         // file tp 和 request tp可能有多个，存放在eio->thread_pool链表中
        char                    *last;         // 线程数组的结尾 last = &data[0] + member_size * thread_count;
        char                    data[0];       // 线程对象数组
    };

    // 处理IO的线程，easy中处理链接的工作线程叫easy_io_thread_t
    struct easy_io_thread_t {
        easy_baseth_on_start_pt         *on_start;       // 线程开始函数
        pthread_t                       tid;             // 线程id
        int                             idx;             // 线程在线程池中的位置索引
        struct ev_loop                  *loop;           //
        ev_async                        thread_watcher;  //
        easy_atomic_t                   thread_lock;     //
        easy_io_t                       *eio;            // 线程所在的eio

        // file
        int                      task_count;             // 线程正在执行异步io任务数量
        easy_pool_t              *task_pool;             // 用于异步操作的内存池

        // queue
        easy_list_t              conn_list, server_session_list, request_list, filet_list;
        // listen watcher
        ev_timer                 listen_watcher;
        easy_io_uthread_start_pt *on_utstart;
        void                     *ut_args;

        // client list
        easy_client_t            *client_list;

        // connected list
        easy_list_t              connected_list;
        easy_atomic32_t          doing_request_count;
        easy_atomic_t            done_request_count;
    };


    // 处理FILE读写线程叫easy_file_thread_t
    struct easy_file_thread_t {
        easy_baseth_on_start_pt         *on_start;       // 线程开始函数
        pthread_t                       tid;             // 线程id
        int                             idx;             // 线程在线程池中的位置索引
        struct ev_loop                  *loop;           //
        ev_async                        thread_watcher;  //
        easy_atomic_t                   thread_lock;     //
        easy_io_t                       *eio;            // 线程所在的eio

        // queue
        int                     task_list_count;         // 异步文件线程task_list中任务的数量
        easy_list_t             task_list;               // io线程加进来的任务列表
    };

    // easy_io对象，维护全局变量，包括io线程池，file线程池，处理request的线程池
    struct easy_io_t {
        easy_pool_t             *pool;
        easy_list_t             eio_list_node;    // 再有多个easy_io_t时，把easy_io_t加入到easy_io_list_var全局链表里
        easy_atomic_t           lock;             // 用于对easy_io_t的使用加锁

        easy_listen_t           *listen;          // 添加的监听端口，一个eio可以有多个监听
        easy_client_t           *client, *default_client;//
        int                     io_thread_count, file_thread_count;// io线程和file线程数
        easy_thread_pool_t      *io_thread_pool;  // io线程池，每个io线程有一个ev_loop
        easy_thread_pool_t      *file_thread_pool;// 文件线程池，用处理异步文件操作
        easy_thread_pool_t      *thread_pool;     // file线程池和request线程池的链表
        void                    *user_data;       //
        easy_list_t             thread_pool_list; // 用于存放作用于easy_io_t的所有线程池

        // flags
        uint32_t                stoped : 1;       // 调用过easy_eio_stop(eio)让eio停止标志
        uint32_t                started : 1;      // 调用过easy_eio_start(eio)，线程已经开始执行了
        uint32_t                tcp_cork : 1;     // 默认eio链接的tcp_cork表示设成1
        uint32_t                tcp_nodelay : 1;  // 默认eio链接的tcp_nodelay表示设成0
        uint32_t                listen_all : 1;   //
        uint32_t                uthread_enable : 1;//

        ev_tstamp               start_time;       // 创建eio的时间
        easy_atomic_t           send_byte;        // writev发送的字节
        easy_atomic_t           recv_byte;        // 接收的字节
    };

3 服务器端和客户端程序使用api的步骤

    3.1 服务器端步骤
    1) 使用easy_io_create初始化线程。对easy_io初始化, 设置io的线程数, file的线程数。

       /**
        * io_thread_count 用于处理socket请求的线程数
        * file_thread_count 文件io的线程数，多用于磁盘操作
        * 返回值 EASY_ERROR(-1) 创建失败，EASY_OK(0)创建成功
        */
        easy_io_t *easy_io_create(int io_thread_count, int file_thread_count)
        例：
        if (easy_io_create(1, 0)) {
            fprintf(stderr, "easy_io_create error.\n");
            return EASY_ERROR;
        }

    2) 实例化easy_io_handler_pt对象，为监听端口设置处理函数，并增加一个监听端口。

        easy_io_handler_pt io_handler; // 详见2.1 struct easy_io_handler_pt
        /**
         * 增加监听端口, 要在easy_io_start开始前调用
         *
         * @param host  机器名或IP, 或NULL
         * @param port  端口号
         *
         * @return      如果成功返回easy_connection_t对象, 否则返回NULL
         */
         easy_listen_t *easy_io_add_listen(const char *host, int port, easy_io_handler_pt *handler)
        例：
        memset(io_handler, 0, sizeof(io_handler));
        io_handler.decode = echo_decode;
        io_handler.encode = echo_encode;
        io_handler.process = echo_process;
        if ((listen = easy_io_add_listen(NULL, 8080, io_handler)) == NULL) {
            fprintf(stderr, "easy_connection_add_listen error, port: 8080, %s\n", strerror(errno));
            return EASY_ERROR;
         } else {
            fprintf(stderr, "listen start, port = 8080\n");
        }

    3) 开始执行线程
        /**
         * 开始easy_io, 第一个线程用于listen, 后面的线程用于处理
         */
        int easy_io_start()
        例：
        if (easy_io_start()) {
            fprintf(stderr, "easy_io_start error.\n");
            return EASY_ERROR;
        }

    4)此步骤可选。起处理速度定时器
        /**
         * 起处理速度定时器
         */
        void easy_io_stat_watcher_start(ev_timer *stat_watcher,
                                        double interval,
                                        easy_io_stat_t *iostat,
                                        easy_io_stat_process_pt *process)
        例：
        ev_timer stat_watcher;
        easy_io_stat_t iostat;
        easy_io_stat_watcher_start(stat_watcher, 5.0, iostat, NULL);

    5) 等待线程退出
        /**
         * 等待easy_io
         */
        int easy_io_wait()
        例：
        if (easy_io_wait() != EASY_OK) {
            fprintf(stderr, "easy io is aborted!!!\n");
        }

    3.2 客户端步骤

    1）初始化线程。对easy_io初始化, 设置io的线程数, file的线程数。
        /**
         * io_thread_count 用于处理socket请求的线程数
         * file_thread_count 文件io的线程数，多用于磁盘操作
         * 返回值 EASY_ERROR(-1) 创建失败，EASY_OK(0)创建成功
         */
        easy_io_t *easy_io_create(int io_thread_count, int file_thread_count)
        例：
        if (easy_io_create(1, 0)) {
            fprintf(stderr, "easy_io_init error.\n");
            return EASY_ERROR;
        }

    2）创建连接。
        /**
         * 创建easy_connection_t，与服务器建立连接
         * addrv 64地址。
         */
        easy_connection_t *easy_io_connect_addr(easy_addr_t addrv, easy_io_handler_pt *handler);
        例：
        /* 客户程序填充服务端的资料 */
        struct sockaddr_in server_addr;
        bzero(server_addr, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(5000);
        server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        easy_addr_t address = *((easy_addr_t*)server_addr);
        // 为监听端口设置处理函数，并增加一个监听端口
        easy_io_handler_pt io_handler;
        memset(io_handler, 0, sizeof(io_handler));
        io_handler.decode = echo_decode;
        io_handler.encode = echo_encode;
        io_handler.process = echo_process;
        io_handler.new_packet = echo_new_packet;
        io_handler.on_disconnect = echo_disconnect;
        io_handler.user_data = (void *)(long)cp.request_size;
        // 创建连接
        if (easy_io_connect_addr(address, io_handler) == NULL) {
            fprintf(stderr, "failure: %s\n", easy_socket_addr_to_str(cp.address, 0, 0));
            break;
        }

    3) 开始执行线程
        /**
         * 开始easy_io, 第一个线程用于listen, 后面的线程用于处理
         */
        int easy_io_start()
        例：
        if (easy_io_start()) {
            fprintf(stderr, "easy_io_start error.\n");
            return EASY_ERROR;
        }

    4)此步骤可选。起处理速度定时器
        /**
         * 起处理速度定时器
         */
        void easy_io_stat_watcher_start(ev_timer *stat_watcher, double interval,
                                easy_io_stat_t *iostat, easy_io_stat_process_pt *process)
        例：
        ev_timer stat_watcher;
        easy_io_stat_t iostat;
        easy_io_stat_watcher_start(stat_watcher, 5.0, iostat, NULL);

    5) 等待线程退出
        /**
         * 等待easy_io
         */
        int easy_io_wait()
        例：
        if (easy_io_wait() != EASY_OK) {
            fprintf(stderr, "easy io is aborted!!!\n");
        }

4. 主要io接口
    4.1 easy_io.h
    /**
     * 初始化线程。对easy_io初始化。
     * io_thread_count 用于处理socket请求的线程数
     * file_thread_count 文件io的线程数，多用于磁盘操作
     * 返回值 EASY_ERROR(-1) 创建失败，EASY_OK(0)创建成功
     */
    easy_io_t *easy_eio_create(easy_io_t *eio, int io_thread_count, int file_thread_count);

    /**
     * 开始easy_io, 第一个线程用于listen, 后面的线程用于处理
     */
    int easy_eio_start(easy_io_t *eio);

    /**
     * 等待eio->thread_pool_list中所有线程退出
     */
    int easy_eio_wait(easy_io_t *eio);

    /**
     * 标记eio->stoped = 1，唤醒所有线程
     */
    int easy_eio_stop(easy_io_t *eio);

    /**
     * 释放资源，销毁所有线程
     */
    void easy_eio_destroy(easy_io_t *eio);

    void easy_eio_set_uthread_start(easy_io_t *eio, easy_io_uthread_start_pt *on_utstart, void *args);
    struct ev_loop *easy_eio_thread_loop(easy_io_t *eio, int index);
    void easy_eio_stat_watcher_start(easy_io_t *eio, ev_timer *stat_watcher,
                         double interval, easy_io_stat_t *iostat, easy_io_stat_process_pt *process);

    /**
     * 增加监听端口到eio->listen监听链表中, 要在easy_io_start开始调用
     *
     * @param host  机器名或IP, 或NULL
     * @param port  端口号
     *
     * @return      如果成功返回easy_connection_t对象, 否则返回NULL
     */
    easy_listen_t *easy_connection_add_listen(easy_io_t *eio,
        const char *host, int port, easy_io_handler_pt *handler)

    /**
     * 链接host服务器，同easy_connection_connect_addr
     */
    easy_connection_t *easy_connection_connect(easy_io_t *eio,
        const char *host, int port, easy_io_handler_pt *handler)

    /**
     * 连接。
     * 1. connect到服务器
     * 2. 创建一个easy_connection_t对象，初始化easy_connection_t对象。
     *    初始化c->read_watcher = easy_connection_on_readable
     *    初始化c->write_watcher = easy_connection_on_writable
     *    初始化c->timeout_watcher = easy_connection_on_timeout_conn
     */
    easy_connection_t *easy_connection_connect_addr(easy_io_t *eio,
        easy_addr_t addrv, easy_io_handler_pt *handler)

    /**
     * 创建处理request线程
     */
    easy_thread_pool_t *easy_thread_pool_create(easy_io_t *eio, int cnt, easy_io_process_pt *cb)

    /**
     * 断开连接
     */
    void easy_connection_disconnect(easy_connection_t *c)

    4.2 异步线程读取文件的使用：

    首先在easy_io_create第二个参数大于等于1，创建异步文件线程数
    例对easy_io初始化, 设置io的线程数, file的线程数
    if (!easy_io_create(1, 1)) {
        easy_error_log("easy_io_init error.\n");
        return EASY_ERROR;
    }

    在异步文件线程创建好之后就可以执行文件读写请求了：
    easy_http_request_on_process中，对文件处理步骤：

    文件处理步骤1：打开文件
    int fd = open(filename, O_RDONLY);

    1）如果使用sendfile把数据直接发送出去：
        文件处理步骤2：创建easy_file_buf_t，并对easy_buf_t初始化
        easy_file_buf_t *fb = easy_file_buf_create(p->output.pool); // 创建
        fb->fd = fd;                        // 初始化发送的文件描述符
        fb->offset = 0;                     // 初始化sendfile的偏移量
        fb->count = read_len;               // 初始化发送的数据长度
        fb->close = easy_file_buf_on_close; // 发送完的对文件的处理操作，这里选择关闭文件描述符
        fb->args = NULL;                    // 设置用于(fb->close)(fd->fd, fb->args)的参数

        文件处理步骤3：把easy_file_buf_t加到packet输出缓存链表里面
        easy_buf_chain_push_fbuf(&p->output, fb);

        文件处理步骤4：返回EASY_OK
        return EASY_OK;

    2）如果使用异步文件线程读取数据
        文件处理步骤2：如果使用异步文件线程读取数据，经过处理之后再发出去
        b = easy_buf_create(p->output.pool, fs.st_size);           // 创建easy_buf_t

        文件处理步骤3：编写异步线程处理完文件任务之后的回调函数，回调函数是easy_file_process_pt类型
        typedef int (easy_file_process_pt)(easy_file_task_t *ft);
        static int read_done(easy_file_task_t *ft)
        {
            easy_request_t          *r = (easy_request_t *) ft->args;

            if (ft->ret < 0) {
                easy_warn_log("async read done: read len: %d\n", ft->ret);
            }

            // your code ...

            close(ft->fd);
            return easy_request_do_reply(r);
        }

        文件处理步骤4：把读取文件请求加入到file线程任务队列里，read_done是异步线程读取完数据的回调函数
        easy_file_async_read(r->ms->c, fd, b->pos, read_len, 0, read_done, r, b); // 把读取文件请求加入到file线程任务队列里

        文件处理步骤5：使用异步文件线程读取数据，process的返回值要返回EASY_AGAIN，暂停对这个请求的处理，等候异步线程的处理请求完成，调用read_done继续处理没完成的工作。
        return EASY_AGAIN;

5. include包

    5.1 easy_define.h

    #define likely(x)                   __builtin_expect(!!(x), 1)
    #define unlikely(x)                 __builtin_expect(!!(x), 0)
    // 返回p按照a的长度对齐的指针
    #define easy_align_ptr(p, a)        (uint8_t*)(((uintptr_t)(p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))
    // 返回d按照a对齐的长度
    #define easy_align(d, a)            (((d) + (a - 1)) & ~(a - 1))
    #define easy_max(a,b)               (a > b ? a : b)
    #define easy_min(a,b)               (a < b ? a : b)

    #define EASY_OK                     0
    #define EASY_ERROR                  (-1)
    #define EASY_ABORT                  (-2)
    #define EASY_ASYNC                  (-3)
    #define EASY_BREAK                  (-4)
    #define EASY_AGAIN                  (-EAGAIN)

    5.2 easy_list.h参考kernel list.h
        /**
         * 双向循环链表，链表第一个不作为链表的node，只作为head，数据从head->next开始
         */
        struct easy_list_t {
            easy_list_t *next, *prev;
        };
        /**
         * 用EASY_LIST_HEAD_INIT或者easy_list_init初始化链表head，prev和next都指向自己
         */
        #define EASY_LIST_HEAD_INIT(name) {(name), (name)}
        #define easy_list_init(ptr) do {                \
                (ptr)->next = (ptr);                    \
                (ptr)->prev = (ptr);                    \
            } while (0)

        /**
         * 向head链表头中添加list节点，list只能是一个easy_list_t节点
         * 注：在head和head->next中间添加list，head是头节点，list是链表中
         * 第一个entry
         */
        static inline void easy_list_add_head(easy_list_t *list, easy_list_t *head)

        /**
         * 向head链表尾中添加list节点，list只能是一个easy_list_t节点
         * 注：在head和head->prev中间添加list，head是头节点，list是尾节点
         */
        static inline void easy_list_add_tail(easy_list_t *list, easy_list_t *head)

        /**
         * 把entry从所在链表中删除
         */
        static inline void easy_list_del(easy_list_t *entry)

        /**
         * 判断head是否只有一个节点，只有一个head节点，链表为空
         */
        static inline int easy_list_empty(const easy_list_t *head)

        /**
         * 把list赋值给new_list
         */
        static inline void easy_list_movelist(easy_list_t *list, easy_list_t *new_list)

        /**
         * 当list不为空（即链表中大于一个节点）时，把list链表加入到head链表前面
         * list可以包含多个元素
         */
        static inline void easy_list_join(easy_list_t *list, easy_list_t *head)

        /**
         * 通过member链表节点的地址获取链表所在对象的地址
         * ptr为type对象的member的指针地址（即ptr=type.member）
         * type为想要获取的对象类型，member为type的成员
         */
        #define easy_list_entry(ptr, type, member) ({                        \
                const typeof( ((type *)0)->member ) *__mptr = (ptr);         \
                (type *)( (char *)__mptr - offsetof(type,member) );})

        /**
         * 遍历head链表中的对象，从head->next开始遍历（head节点本身不做遍历）
         * pos为链表所在对象的临时指针，head为要遍历的链表，member是pos的
         * 成员对象链表节点
         */
        #define easy_list_for_each_entry(pos, head, member)                         \
            for (pos = easy_list_entry((head)->next, typeof(*pos), member);         \
                    pos->member != (head);          \
                    pos = easy_list_entry(pos->member.next, typeof(*pos), member))

    5.3 easy_atomic.h
    #define easy_atomic_set(v,i)        ((v) = (i))

    // 32bit
    typedef volatile int32_t            easy_atomic32_t;
    /**
     * 32bit原子操作 v = v + i
     */
    static __inline__ void easy_atomic32_add(easy_atomic32_t *v, int i)

    /**
     * 32bit原子操作 v = v + i， 返回v的值
     */
    static __inline__ int32_t easy_atomic32_add_return(easy_atomic32_t *value, int32_t diff)

    /**
     * 32bit原子操作 v ++
     */
    static __inline__ void easy_atomic32_inc(easy_atomic32_t *v)

    /**
     * 32bit原子操作 v --
     */
    static __inline__ void easy_atomic32_dec(easy_atomic32_t *v)

    // 64bit
    typedef volatile int64_t easy_atomic_t;
    /**
     * 64bit原子操作 v = v + i
     */
    static __inline__ void easy_atomic_add(easy_atomic_t *v, int64_t i)
    /**
     * 64bit原子操作 v = v + i， 返回v的值
     */
    static __inline__ int64_t easy_atomic_add_return(easy_atomic_t *value, int64_t diff)
    /**
     * 64bit原子操作 lock值由原来的old赋值成set值，设置成功返回1，失败返回0
     * lock原来的值不等于old，会设置不成功，返回0
     */
    static __inline__ int64_t easy_atomic_cmp_set(easy_atomic_t *lock, int64_t old, int64_t set)

    /**
     * 64bit原子操作 v ++
     */
    static __inline__ void easy_atomic_inc(easy_atomic_t *v)
    /**
     * 64bit原子操作 v --
     */
    static __inline__ void easy_atomic_dec(easy_atomic_t *v)

    /**
     * 对lock加锁，如果加锁不成功线程继续执行其它任务
     */
    #define easy_trylock(lock)  (*(lock) == 0 && easy_atomic_cmp_set(lock, 0, 1))
    // 对lock解锁
    #define easy_unlock(lock)   {__asm__ ("" ::: "memory"); *(lock) = 0;}
    #define easy_spin_unlock easy_unlock

    /**
     * 对lock加锁，如果lock被加过锁，线程会自旋一段时间，在这段时间自己还不能拥有锁，把cpu交给其他线程
     * 把线程放到调度队列的队尾等待调度。
     */
    static __inline__ void easy_spin_lock(easy_atomic_t *lock)
    // 对地址addr所指向的内存的第nr位设成0
    static __inline__ void easy_clear_bit(unsigned long nr, volatile void *addr)
    // 对地址addr所指向的内存的第nr位设成1
    static __inline__ void easy_set_bit(unsigned long nr, volatile void *addr)

6. util包
    6.1 easy_hash.h和easy_hash.c
        struct easy_hash_t {  // hash table
            easy_hash_list_t **buckets; // hash桶
            uint32_t         size;      // hash桶的数量  桶的数量为2 ^ n
            uint32_t         mask;      // 桶数量的掩码，size - 1
            uint32_t         count;     // 对象的数量
            int              offset;    // easy_hash_list_t在对象中的偏移量offsetof(type obj_type, easy_hash_list_t hash_list)
            uint64_t         seqno;     // 向hash table中增加数据的序列标示，从1开始计数
        };

        struct easy_hash_list_t {
            uint64_t key;
            easy_hash_list_t *next, **pprev;
        };

        /**
         * 创建并初始化hash table
         * offset为easy_hash_list_t在对象中的偏移量 = offsetof(type obj_type, easy_hash_list_t hash_list)
         * size hash桶的数量，pool用于分配easy_hash_t的内存池
         * 返回创建的easy_hash_t *
         */
        easy_hash_t *easy_hash_create(easy_pool_t *pool, uint32_t size, int offset)

        /**
         * 向hash表table中增加节点，返回EASY_OK
         */
        int easy_hash_add(easy_hash_t *table, uint64_t key, easy_hash_list_t *list)

        /**
         * 通过key查找对象，返回的是easy_hash_list_t所在的对象的指针
         */
        void *easy_hash_find(easy_hash_t *table, uint64_t key)

        typedef int (easy_hash_cmp_pt)(const void *a, const void *b);
        /**
         * 找到相同key值的对象obj，返回满足cmp(a, obj) == 0的第一个对象
         */
        void *easy_hash_find_ex(easy_hash_t *table, uint64_t key, easy_hash_cmp_pt cmp, const void *a)

        /**
         * 删除key对应的easy_hash_list_t，如果删除成功返回删除easy_hash_list_t对应的对象
         * 没有删除成功，返回NULL
         */
        void *easy_hash_del(easy_hash_t *table, uint64_t key)

        /**
         * 删除节点node，成功返回1，失败返回0
         */
        int easy_hash_del_node(easy_hash_list_t *node)

        /**
         * hash 64 bit
         * key为64bit时封装 easy_hash_code
         */
        uint64_t easy_hash_key(volatile uint64_t key)

        /**
         * 生成hash值，key：字符串，len：key的长度，seed：随机种子，一般为一个质数
         * 同 MurmurHash64A
         */
        uint64_t easy_hash_code(const void *key, int len, unsigned int seed)

    6.2 easy_buf.c和easy_buf.h
        /**
         * 用于存放数据的buffer
         */
        struct easy_buf_t {
            easy_buf_t      *next;    // 用于在easy_buf_chain_t中使用的下一个buf
            int             flags;    // 判断是默认的buf、EASY_BUF_FILE三种情况
            char            *pos;     // buf当前指向的位置
            char            *last;    // buf包含有效数据最后一个字节的下一个位置
            char            *start;   // buf开始位置
            char            *end;     // buf缓存区最后一个位置，end - last表示剩余可用的空间
        };

        /**
         * 文件buffer，用于sendfile等
         */
        struct easy_file_buf_t {
            easy_buf_t          *next;    // 用于在easy_buf_chain_t中使用的下一个buf
            int                 flags;    // 判断是默认的buf、EASY_BUF_MALLOC、EASY_BUF_FILE三种情况
            int                 fd;       // 文件描述符
            int64_t             offset;   // 文件偏移量
            int64_t             count;    // 文件的大小
            easy_fbuf_close_pt  *close;   // 释放buffer时，调用的关闭文件的函数
            void                *args;    // close参数
        };

        /**
         * buffer链表
         */
        struct easy_buf_chain_t {
            easy_buf_t  *head;
            easy_buf_t  *tail;
            easy_pool_t *pool;    // 用于创建easy_buf_t
        };

        /**
         * easy string
         */
        struct easy_buf_string_t {
            char        *data;
            int         len;
        };

        /**
         * 创建一个新的easy_buf_t
         */
        easy_buf_t *easy_buf_create(easy_pool_t *pool, uint32_t size)

         /**
         * 把data包成easy_buf_t
         */
        easy_buf_t *easy_buf_pack(easy_pool_t *pool, const char *data, uint32_t size)

        /**
         * 创建一个easy_file_buf_t, 用于sendfile等
         */
         easy_file_buf_t *easy_file_buf_create(easy_pool_t *pool)

        /**
         * 销毁buffer
         */
        void easy_buf_destroy(easy_pool_t *pool, easy_buf_t *b)

        /**
         * 空间不够,分配出一块来,保留之前的空间
         */
        int easy_buf_check_read_space(easy_pool_t *pool, easy_buf_t *b, uint32_t size)

        /**
         * 空间不够,分配出一块来,保留之前的空间
         */
        int easy_buf_check_write_space(easy_buf_chain_t *bc, easy_buf_t *b, uint32_t size)

        /**
         * 弹出easy_buf_chain_t中顶端的easy_buf_t并返回弹出的值
         */
        easy_buf_t *easy_buf_chain_pop(easy_buf_chain_t *l)

        /**
         * 清除easy_buf_chain_t，释放easy_buf_t
         */
        void easy_buf_chain_clear(easy_buf_chain_t *l)

        /**
         * 向easy_buf_chain_t结尾处添加easy_buf_t
         */
        void easy_buf_chain_push(easy_buf_chain_t *l, easy_buf_t *b)

        /**
         * 向easy_buf_chain_t结尾处添加easy_file_buf_t
         */
        void easy_buf_chain_push_fbuf(easy_buf_chain_t *l, easy_file_buf_t *b)

        /**
         * 把l合并到d的结尾处
         */
        void easy_buf_chain_merge(easy_buf_chain_t *l, easy_buf_chain_t *d)

        /**
         * 把s复制到d上
         */
        int easy_buf_string_copy(easy_pool_t *pool, easy_buf_string_t *d, easy_buf_string_t *s)

        /**
         * 格式字符串打印到d上
         */
        int easy_buf_string_printf(easy_pool_t *pool, easy_buf_string_t *d, const char *fmt, ...)

    6.3 easy_pool.c和easy_pool.h
    struct easy_pool_cleanup_t {
        easy_pool_cleanup_pt    handler;  // 在释放pool的时候的清理函数
        const void              *data;    // 要清理的数据
        easy_pool_cleanup_t     *next;    // 清理函数可以有多个，用next连起来
    };

    struct easy_pool_large_t {
        easy_pool_large_t       *next;
        uint8_t                 data[0];
    };

    struct easy_pool_t {
        uint8_t                 *last;    // 指向easy_pool_t未分配最后一个字节的下一个字节
        uint8_t                 *end;     // 指向easy_pool_t内存区域的结束字节的下一个字节
        easy_pool_t             *next;    // 此easy_pool_t内存区域不够分了，有多个easy_pool_t连起来
        uint32_t                failed;   // 从current指向的easy_pool_t分配失败（分配一个新的easy_pool_t）的次数，
        uint32_t                max;      // 从easy_pool_t内存区域分配的最大值，超过max的用easy_pool_large_t分配

        // pool header
        // 下面只在pool中的第一个easy_pool_t（也就是没有加入到easy_pool_t->next的）用到
        // 加入到easy_pool_t->next的easy_pool_t下面变量占用的空间会被分给用户
        easy_pool_t             *current; // 当前指向的easy_pool_t开始分配空间给用户，直到next中的最后一个easy_pool_t
        easy_pool_large_t       *large;   // 要分配的size大于max，用easy_pool_large_t分配，并把分配好的内存添加到large链表后面
        easy_pool_cleanup_t     *cleanup; // 在释放pool的时候的清理操作
        easy_atomic_t           lock;     // 多线程分配的自旋锁，使用easy_pool_t分配内存是线程安全的
        uint32_t                ref;      // ref是用到这个easy_pool_t的多线程引用的数量，为了判断是否加锁
        easy_atomic32_t         bufcnt;   // 用easy_pool_t分配的easy_buf_t或easy_file_buf_t的数量
    };

    /**
     * 创建内存池，当size等于0的时，内存池大小为EASY_POOL_ALIGNMENT(512)字节
     * p->max = EASY_POOL_ALIGNMENT - sizeof(easy_pool_t)
     * 分配的内存从p + sizeof(easy_pool_t)开始
     */
    easy_pool_t *easy_pool_create(uint32_t size)

    /**
     * 把pool清空
     */
    void easy_pool_clear(easy_pool_t *pool)

    /**
     * 释放pool
     */
    void easy_pool_destroy(easy_pool_t *pool)

    /**
     * 从pool中分配size大小的空间
     * 分配的空间起始位置按sizeof(unsigned long)对齐
     */
    void *easy_pool_alloc(easy_pool_t *pool, uint32_t size)

    /**
     * 从pool中分配size大小的空间，起始位置不一定对齐
     */
    void *easy_pool_nalloc(easy_pool_t *pool, uint32_t size)

    /**
     * 注册 在释放pool的时候对data的清理操作
     */
    void easy_pool_cleanup_register(easy_pool_t *pool, const void *data, easy_pool_cleanup_pt handler)

    /**
     * 从pool中分配size大小的空间 并清零
     * 分配的空间起始位置按sizeof(unsigned long)对齐
     */
    void *easy_pool_calloc(easy_pool_t *pool, uint32_t size)

    /**
     * 设置分配函数代替easy_pool_default_realloc
     */
    void easy_pool_set_allocator(easy_pool_realloc_pt alloc)

    6.4 easy_array_queue.h和easy_array_queue.c

    /**
     * 固定长度的FIFO队列
     */
    struct easy_array_queue_t {
        int                             count;       // 队列里对象数量
        int                             object_size; // 先入先出队列每个对象的size
        int                             max_size;    // 能存放最大对象的数量
        char                            *rpos;       // 队列头读的位置
        char                            *wpos;       // 队列尾push进的位置
        char                            *last;       // 队列实际内存中队列最后的位置
        char                            data[0];     // 队列内存中开始的位置
    };

    /**
     * 创建队列
     */
    easy_array_queue_t *easy_array_queue_create(easy_pool_t *pool, int max_size, int object_size)

    void *easy_array_queue_push(easy_array_queue_t *queue, void *item)

    void *easy_array_queue_pop(easy_array_queue_t *queue)

    6.5 easy_inet.h和easy_inet.c

    char *easy_inet_addr_to_str(easy_addr_t *addr, char *buffer, int len);

    easy_addr_t easy_inet_str_to_addr(const char *host, int port);

    int easy_inet_parse_host(easy_addr_t *address, const char *host, int port);

    int easy_inet_is_ipaddr(const char *host);

    6.6 easy_string.h和easy_string.c

    char *easy_strncpy(char *dst, const char *src, size_t n);

    char *easy_string_tohex(const char *str, int n, char *result, int size);

    6.7 easy_time.h和easy_time.c

    int easy_localtime(const time_t *t, struct tm *tp);

