#include "stdafx.h"

namespace couchbasepp
{
    using boost::asio::ip::tcp;

    class couchbase_error_category : public boost::system::error_category
    {
    public:
        couchbase_error_category(){}
        const char * name() const;
        std::string message( int ev ) const;
    };

    const char * couchbase_error_category::name() const
    {
        return "couchbase";
    }

    std::string couchbase_error_category::message(int ev) const
    {
        return lcb_strerror(0, (lcb_error_t)ev);
    }

    inline const boost::system::error_category& get_couchbase_category()
    {
        static const couchbase_error_category  couchbase_category_const;
        return couchbase_category_const;
    }

    inline boost::system::error_code make_error_code(lcb_error_t e)
    {
        return boost::system::error_code(
            static_cast<int>(e), get_couchbase_category());
    }

    namespace error
    {
        enum lcb_error
        {
            auth_continue = 0x01,
            auth_error = 0x02,
            bad_value = 0x03,
            too_big = 0x04,
            too_busy = 0x05,
            critical = 0x06,
            invalid_args = 0x07,
            out_of_memory = 0x08,
            invalid_range = 0x09,
            generic_error = 0x0a,
            temporary_failure = 0x0b,
            key_already_exists = 0x0c,
            key_not_exist = 0x0d,
            open_shared_object = 0x0e,
            locate_symbol = 0x0f,
            network_error = 0x10,
            not_my_vbucket = 0x11,
            not_stored = 0x12,
            not_supported = 0x13,
            unknown_command = 0x14,
            unknown_host = 0x15,
            protocol_error = 0x16,
            timed_out = 0x17,
            connect_error = 0x18,
            bucket_not_exist = 0x19,
            client_out_of_memory = 0x1a, 
            client_temporary_failure = 0x1b,
            bad_handle = 0x1c,
            server_bug = 0x1d,
            version_mismatch = 0x1e,
            invalid_host_format = 0x1f,
            invalid_char = 0x20
        };
    };

    class instance
    {
    private:
        struct io_timer
        {
            io_timer(instance &owner_)
                : owner(owner_), _timer(owner_._io_service)
            {
            }

            static void _timer_expired(boost::system::error_code ec, std::function<void()> handler) {
                if(!ec) handler();
            }

            int update(uint32_t usec, std::function<void()> handler) {
                _timer.expires_from_now(boost::posix_time::microseconds(usec));
                _timer.async_wait(owner._strand.wrap(boost::bind(_timer_expired, boost::asio::placeholders::error, handler)));
                return 0;
            }

            void cancel()
            {
                _timer.cancel();
            }

            instance &owner;
            boost::asio::deadline_timer _timer;
        };

        struct io_event;
        struct io_sock
        {
            io_sock(instance& owner_, int sock_idx)
                : owner(owner_), _socket(owner_._io_service), _sock_idx(sock_idx),
                    _connect_in_progress(false), _read_in_progress(false), 
                    _write_in_progress(false), _connected(false)
            {
            }

            void add_event(io_event *evnt) {
                for(auto i = _events.begin(); i != _events.end(); ++i) {
                    if( *i == evnt ) {
                        return;
                    }
                }
                _events.push_back(evnt);
            }
            void remove_event(io_event *evnt) {
                for(auto i = _events.begin(); i != _events.end(); ++i) {
                    _events.erase(i);
                    break;
                }
            }

            void handle_read(boost::system::error_code ec)
            {
                _read_in_progress = false;

                if (!ec) {
                    for(auto i = _events.begin(); i != _events.end(); ++i) {
                        if( (*i)->_flags & LCB_READ_EVENT ) {
                            if( (*i)->_handler ) (*i)->_handler(LCB_READ_EVENT);
                        }
                    }

                    start_operations();
                } else {
                    _socket.close();
                }
            }

            void handle_write(boost::system::error_code ec)
            {
                _write_in_progress = false;

                if (!ec) {
                    for(auto i = _events.begin(); i != _events.end(); ++i) {
                        if( (*i)->_flags & LCB_WRITE_EVENT ) {
                            if( (*i)->_handler ) (*i)->_handler(LCB_WRITE_EVENT);
                        }
                    }

                    start_operations();
                } else {
                    _socket.close();
                }
            }

            void start_operations()
            {
                if(!_connected) {
                    return;
                }

                bool read_wanted = false;
                bool write_wanted = false;

                for(auto i = _events.begin(); i != _events.end(); ++i) {
                    if( (*i)->_flags & LCB_READ_EVENT ) {
                        read_wanted = true;
                    }
                    if( (*i)->_flags & LCB_WRITE_EVENT ) {
                        write_wanted = true;
                    }
                    if( read_wanted && write_wanted ) {
                        break;
                    }
                }

                if(read_wanted && !_read_in_progress) {
                    _read_in_progress = true;
                    _socket.async_read_some(
                        boost::asio::null_buffers(),
                        owner._strand.wrap(boost::bind(&io_sock::handle_read,
                            this, boost::asio::placeholders::error)));
                }
                if(write_wanted && !_write_in_progress) {
                    _write_in_progress = true;
                    _socket.async_write_some(
                        boost::asio::null_buffers(),
                        owner._strand.wrap(boost::bind(&io_sock::handle_write,
                            this, boost::asio::placeholders::error)));
                }
            }

            void _handle_connected(boost::system::error_code ec)
            {
                _connect_in_progress = false;
                if(!ec){
                    _connected = true;
                    start_operations();
                } else {
                
                }
            }

            boost::system::error_code connect(const boost::asio::ip::tcp::endpoint& endpoint)
            {
                if(_connected) {
                    return boost::asio::error::already_connected;
                } else if(_connect_in_progress) {
                    return boost::asio::error::already_started;
                } else {
                    _socket.async_connect(endpoint, 
                        boost::bind(&io_sock::_handle_connected, this, boost::asio::placeholders::error));
                    return boost::asio::error::would_block;
                }
            }

            int _sock_idx;
            instance &owner;
            tcp::socket _socket;
            std::vector<io_event*> _events;
            bool _connected;
            bool _connect_in_progress;
            bool _read_in_progress;
            bool _write_in_progress;
        };

        struct io_event
        {
            io_event(instance &owner)
                : _socket(nullptr), _flags(0)
            {
            }

            int update(io_sock *socket, short flags, std::function<void(short)> handler)
            {
                _flags = flags;
                _handler = handler;
                if(_socket != socket) {
                    if( _socket ) {
                        _socket->remove_event(this);
                        _socket = nullptr;
                    }

                    if(socket) {
                        _socket = socket;
                        _socket->add_event(this);
                    }
                }
                if(_socket) {
                    _socket->start_operations();
                }
                return 0;
            }

            void cancel()
            {
                _socket->remove_event(this);
                _socket = nullptr;
                _flags = 0;
                _handler = nullptr;
            }

            io_sock *_socket;
            short _flags;
            std::function<void(short)> _handler;
        };

        static int _io_translate_error(boost::system::error_code ec)
        {
            if(ec == boost::asio::error::connection_aborted
                || ec == boost::asio::error::connection_reset) {
                return ECONNRESET;
            } else if(ec == boost::asio::error::would_block) {
                return EWOULDBLOCK;
            } else if(ec == boost::asio::error::invalid_argument) {
                return EINVAL;
            } else if(ec == boost::asio::error::in_progress) {
                return EINPROGRESS;
            } else if(ec == boost::asio::error::already_started) {
                return EALREADY;
            } else if(ec == boost::asio::error::already_connected) {
                return EISCONN;
            } else if(ec == boost::asio::error::not_connected) {
                return ENOTCONN;
            } else if(ec == boost::asio::error::connection_refused) {
                return ECONNREFUSED;
            } else {
                std::cerr << "Unknown error: " << ec << std::endl;
                return EINVAL;
            }
        }

        lcb_io_opt_st* create_opt_st()
        {
            lcb_io_opt_st *io = new lcb_io_opt_st();

            io->version = 0;
            io->destructor = destroy_opt_st;
            io->v.v0.cookie = this;
            io->v.v0.create_event = io_create_event;
            io->v.v0.update_event = io_update_event;
            io->v.v0.delete_event = io_delete_event;
            io->v.v0.destroy_event = io_destroy_event;
            io->v.v0.create_timer = io_create_timer;
            io->v.v0.update_timer = io_update_timer;
            io->v.v0.delete_timer = io_delete_timer;
            io->v.v0.destroy_timer = io_destroy_timer;
            io->v.v0.socket = io_socket;
            io->v.v0.connect = io_connect;
            io->v.v0.send = io_send;
            io->v.v0.recv = io_recv;
            io->v.v0.sendv = io_sendv;
            io->v.v0.recvv = io_recvv;
            io->v.v0.close = io_close;
            io->v.v0.run_event_loop = io_run_event_loop;
            io->v.v0.stop_event_loop = io_stop_event_loop;

            return io;
        }

        static void destroy_opt_st(lcb_io_opt_st *iops)
        {
        }

        static void io_run_event_loop(lcb_io_opt_st *iops)
        {
            instance* cb = (instance*)iops->v.v0.cookie;
            cb->_io_service.reset();
            cb->_io_service.run();
        }

        static void io_stop_event_loop(lcb_io_opt_st *iops)
        {
            instance* cb = (instance*)iops->v.v0.cookie;
            cb->_io_service.stop();
        }

        static void* io_create_timer(lcb_io_opt_st *iops)
        {
            instance* cb = (instance*)iops->v.v0.cookie;
            return new io_timer(*cb);
        }

        static int io_update_timer(lcb_io_opt_st *iops, void *timer, lcb_uint32_t usec, void *cb_data, void (*handler)(lcb_socket_t sock,short which,void *cb_data) )
        {
            io_timer *tmr = (io_timer*)timer;
            return tmr->update(usec, boost::bind(handler, -1, 0, cb_data));
        }

        static void io_delete_timer(lcb_io_opt_st *iops, void *timer)
        {
            io_timer *tmr = (io_timer*)timer;
            tmr->cancel();
        }

        static void io_destroy_timer(lcb_io_opt_st *iops, void *timer)
        {
            delete (io_timer*)timer;
        }

        static void* io_create_event(lcb_io_opt_st *iops)
        {
            instance* cb = (instance*)iops->v.v0.cookie;
            return new io_event(*cb);
        }

        static int io_update_event(lcb_io_opt_st *iops, lcb_socket_t socket, void *evnt, short flags, void *cb_data, void (*handler)(lcb_socket_t sock,short which,void *cb_data))
        {
            io_event *evt = (io_event*)evnt;
            io_sock *sock = connections[socket];
            return evt->update(sock, flags, std::bind(handler, socket, std::placeholders::_1, cb_data));
        }

        static void io_delete_event(lcb_io_opt_st *iops, lcb_socket_t socket, void *evnt)
        {
            io_event *evt = (io_event*)evnt;
            evt->cancel();
        }

        static void io_destroy_event(lcb_io_opt_st *iops, void *evnt)
        {
            delete (io_event*)evnt;
        }

        static lcb_socket_t io_socket(lcb_io_opt_st *iops, int domain, int type, int protocol)
        {
            if( domain != 2 || type != 1 ) {
                return -1;
            }

            int sock_idx = -1;
            for( size_t i = 0; i < connections.size(); ++i ) {
                if( !connections[i] ) {
                    sock_idx = i;
                }
            }
            if( sock_idx < 0 ) {
                sock_idx = connections.size();
                connections.push_back(nullptr);
            }

            instance* cb = (instance*)iops->v.v0.cookie;
            io_sock *conn = new io_sock(*cb, sock_idx);
            connections[sock_idx] = conn;

            return sock_idx;
        }

        static void io_close(lcb_io_opt_st *iops, lcb_socket_t socket)
        {
            if( connections[socket] ) {
                delete connections[socket];
                connections[socket] = nullptr;
            }
        }

        static int io_connect(lcb_io_opt_st *iops, lcb_socket_t socket, const struct sockaddr *name, unsigned int namelen)
        {
            io_sock *sock = connections[socket];

            sockaddr_in *addr = (sockaddr_in*)name;
            uint32_t ip_addr = ntohl(addr->sin_addr.S_un.S_addr);
            short ip_port = ntohs(addr->sin_port);

            boost::system::error_code ec;
            ec = sock->connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::address_v4(ip_addr), ip_port));
            if(ec == boost::asio::error::would_block) {
                iops->v.v0.error = EWOULDBLOCK;
                return -1;
            } else if(ec == boost::asio::error::already_started) {
                iops->v.v0.error = EALREADY;
                return -1;
            } else if(ec == boost::asio::error::already_connected) {
                iops->v.v0.error = EISCONN;
                return -1;
            } else {
                iops->v.v0.error = EINVAL;
                return -1;
            }
        }

        static lcb_ssize_t io_recv(lcb_io_opt_st *iops, lcb_socket_t socket, void *buffer, lcb_size_t len, int flags)
        {
            io_sock *sock = connections[socket];
            boost::system::error_code ec;
            size_t readlen = sock->_socket.read_some(boost::asio::buffer(buffer, len), ec);
            if(ec) {
                iops->v.v0.error = _io_translate_error(ec);
                if(iops->v.v0.error == ECONNRESET) {
                    return 0;
                }
                return -1;
            }
            return readlen;
        }

        static lcb_ssize_t io_send(lcb_io_opt_st *iops, lcb_socket_t socket, const void *buffer, lcb_size_t len, int flags)
        {
            io_sock *sock = connections[socket];
            boost::system::error_code ec;
            size_t readlen = sock->_socket.write_some(boost::asio::buffer(buffer, len), ec);
            if(ec) {
                iops->v.v0.error = _io_translate_error(ec);
                return -1;
            }
            return readlen;
        }

        static lcb_ssize_t io_recvv(lcb_io_opt_st *iops, lcb_socket_t socket, lcb_iovec_st *iov, lcb_size_t niov) 
        {
            io_sock *sock = connections[socket];

            std::vector<boost::asio::mutable_buffer> buffers;
            for(lcb_size_t i = 0; i < niov; ++i) {
                buffers.push_back(boost::asio::buffer(iov[i].iov_base, iov[i].iov_len));
            }

            boost::system::error_code ec;
            size_t readlen = sock->_socket.read_some(buffers, ec);
            if(ec) {
                iops->v.v0.error = _io_translate_error(ec);
                return -1;
            }
            return readlen;
        }

        static lcb_ssize_t io_sendv(lcb_io_opt_st *iops, lcb_socket_t socket, lcb_iovec_st *iov, lcb_size_t niov) 
        {
            io_sock *sock = connections[socket];

            std::vector<boost::asio::const_buffer> buffers;
            for(lcb_size_t i = 0; i < niov; ++i) {
                buffers.push_back(boost::asio::buffer(iov[i].iov_base, iov[i].iov_len));
            }

            boost::system::error_code ec;
            size_t readlen = sock->_socket.write_some(buffers, ec);
            if(ec) {
                iops->v.v0.error = _io_translate_error(ec);
                if(iops->v.v0.error == ECONNRESET) {
                    return 0;
                }
                return -1;
            }
            return readlen;
        }

        static std::vector<io_sock*> connections;
        lcb_t _couchbase;
        boost::asio::io_service& _io_service;
        boost::asio::io_service::strand _strand;

    private:
        static void _error_handler(lcb_t lcb, lcb_error_t err, const char *errinfo)
        {
        }

        static void _configuration_handler(lcb_t lcb, lcb_configuration_t config)
        {
            instance *cb = (instance*)lcb_get_cookie(lcb);
            if(config == LCB_CONFIGURATION_NEW) {
                cb->_connected = true;
                for(auto i = cb->_connect_callbacks.begin(); i != cb->_connect_callbacks.end(); ++i) {
                    (*i)();
                }
                cb->_connect_callbacks.clear();
            }
        }

        template<class T>
        struct _operation {
            _operation(T callback_) : callback(callback_) {}
            T callback;
        };

        typedef std::function<void(const boost::system::error_code&, const std::string&, const std::string&, int64_t cas)> _get_callback;

        typedef _operation<_get_callback> _get_operation;

        static void _get_handler(lcb_t lcb, const void *cookie, lcb_error_t error, const lcb_get_resp_t *resp)
        {
            _get_operation *op = (_get_operation*)cookie;
            if (error != LCB_SUCCESS) {
                op->callback(make_error_code(error), std::string(), std::string(), 0);
            } else {
                op->callback(boost::system::error_code(), std::string((char*)resp->v.v0.key, resp->v.v0.nkey), std::string((char*)resp->v.v0.bytes, resp->v.v0.nbytes), resp->v.v0.cas);
            }
            delete op;
        }

        bool _connected;
        std::vector<std::function<void()>> _connect_callbacks;

    public:
        instance(const std::string &hosts, const std::string &user, const std::string &passwd, const std::string &bucket, boost::asio::io_service& io_service)
            : _io_service(io_service), _strand(io_service), _connected(false)
        {
            lcb_error_t err;

            lcb_create_st create_options;
            create_options.v.v0.io = create_opt_st();
            create_options.v.v0.host = hosts.c_str();
            create_options.v.v0.user = user.c_str();
            create_options.v.v0.passwd = passwd.c_str();
            create_options.v.v0.bucket = bucket.c_str();
        
            err = lcb_create(&_couchbase, &create_options);
            if (err != LCB_SUCCESS) {
                throw std::exception(lcb_strerror(NULL, err));
            }
            lcb_set_cookie(_couchbase, this);

            lcb_set_error_callback(_couchbase, _error_handler);
            lcb_set_configuration_callback(_couchbase, _configuration_handler);
            lcb_set_get_callback(_couchbase, _get_handler);
        }

        ~instance()
        {
            lcb_destroy(_couchbase);
        }

        void connect(std::function<void()> callback)
        {
            if(_connected) {
                callback();
            } else {
                _connect_callbacks.push_back(callback);

                lcb_error_t err;
                if ((err = lcb_connect(_couchbase)) != LCB_SUCCESS) {
                    std::cerr << "Failed to connect" << std::endl;
                    return;
                }
            }
        }

        void wait()
        {
            lcb_wait(_couchbase);
        }

	    void get(const std::string& key, const _get_callback& callback) {
            _strand.dispatch([this,key,callback](){
                lcb_get_cmd_t cmd;
                const lcb_get_cmd_t * const commands[1] = { &cmd };
                memset(&cmd, 0, sizeof(cmd));
                cmd.v.v0.key = key.c_str();
                cmd.v.v0.nkey = key.size();

                _get_operation *op = new _get_operation(callback);
                lcb_error_t err = lcb_get(_couchbase, op, 1, commands);
                if (err != LCB_SUCCESS) {
                    std::cerr << "Failed to get: " << key << std::endl;
                    return;
                }
            });
	    };

    };
    std::vector<instance::io_sock*> instance::connections;
};