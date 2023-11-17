#ifndef __RAFT_PROTOCOL_OPERATION_HH__
#define __RAFT_PROTOCOL_OPERATION_HH__

#include <algorithm>
#include <chrono>
#include "boost/intrusive/list.hpp"

namespace raft {
  namespace util {
    // Non-polymorphic base class that "inlines" a vtable for a method
    template<typename _Protocol>
    class protocol_operation
    {
    public:
      typedef boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::normal_link> > link_type;
      link_type list_hook_;

      typedef boost::intrusive::member_hook<protocol_operation, 
                                            link_type, 
                                            &protocol_operation::list_hook_> operation_queue_option;
      typedef boost::intrusive::list<protocol_operation, operation_queue_option, boost::intrusive::constant_time_size<true> > queue_type;
      
    protected:
      typedef void (*func_type)(_Protocol *, protocol_operation*);
      protocol_operation(func_type func)
        :
        func_(func)
      {
      }

      ~protocol_operation()
      {
      }
    private:
      func_type func_;
    public:
      void complete(_Protocol * owner)
      {
        func_(owner, this);
      }

      void destroy()
      {
        func_(nullptr, this);
      }
    };

    template<typename _Messages, typename _Protocol>
    class request_vote_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef typename messages_type::request_vote_traits_type::arg_type request_vote_arg_type;      
    private:
      request_vote_arg_type message_;
    public:
      request_vote_operation(request_vote_arg_type && msg)
        :
        protocol_operation_type(&do_complete),
        message_(std::move(msg))
      {
      }
      ~request_vote_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        request_vote_operation * op(static_cast<request_vote_operation *>(base));
        request_vote_arg_type msg = std::move(op->message_);
        delete op;
        if (nullptr != owner) {
          owner->on_request_vote(std::move(msg));
        }
      }
    };

    template<typename _Messages, typename _Protocol>
    class vote_response_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef typename messages_type::vote_response_traits_type::arg_type vote_response_arg_type;      
    private:
      vote_response_arg_type message_;
    public:
      vote_response_operation(vote_response_arg_type && msg)
        :
        protocol_operation_type(&do_complete),
        message_(std::move(msg))
      {
      }
      ~vote_response_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        vote_response_operation * op(static_cast<vote_response_operation *>(base));
        vote_response_arg_type msg = std::move(op->message_);
        delete op;
        if (nullptr != owner) {
          owner->on_vote_response(std::move(msg));
        }
      }
    };

    template<typename _Messages, typename _Protocol>
    class append_entry_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef typename messages_type::append_entry_traits_type::arg_type append_entry_arg_type;      
    private:
      append_entry_arg_type message_;
    public:
      append_entry_operation(append_entry_arg_type && msg)
        :
        protocol_operation_type(&do_complete),
        message_(std::move(msg))
      {
      }
      ~append_entry_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        append_entry_operation * op(static_cast<append_entry_operation *>(base));
        append_entry_arg_type msg = std::move(op->message_);
        delete op;
        if (nullptr != owner) {
          owner->on_append_entry(std::move(msg));
        }
      }
    };

    template<typename _Messages, typename _Protocol>
    class append_entry_response_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef typename messages_type::append_entry_response_traits_type::arg_type append_entry_response_arg_type;      
    private:
      append_entry_response_arg_type message_;
    public:
      append_entry_response_operation(append_entry_response_arg_type && msg)
        :
        protocol_operation_type(&do_complete),
        message_(std::move(msg))
      {
      }
      ~append_entry_response_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        append_entry_response_operation * op(static_cast<append_entry_response_operation *>(base));
        append_entry_response_arg_type msg = std::move(op->message_);
        delete op;
        if (nullptr != owner) {
          owner->on_append_response(std::move(msg));
        }
      }
    };    

    template<typename _Messages, typename _Protocol>
    class append_checkpoint_chunk_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef typename messages_type::append_checkpoint_chunk_traits_type::arg_type append_checkpoint_chunk_arg_type;      
    private:
      append_checkpoint_chunk_arg_type message_;
    public:
      append_checkpoint_chunk_operation(append_checkpoint_chunk_arg_type && msg)
        :
        protocol_operation_type(&do_complete),
        message_(std::move(msg))
      {
      }
      ~append_checkpoint_chunk_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        append_checkpoint_chunk_operation * op(static_cast<append_checkpoint_chunk_operation *>(base));
        append_checkpoint_chunk_arg_type msg = std::move(op->message_);
        delete op;
        if (nullptr != owner) {
          owner->on_append_checkpoint_chunk(std::move(msg));
        }
      }
    };

    template<typename _Messages, typename _Protocol>
    class append_checkpoint_chunk_response_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef typename messages_type::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;      
    private:
      append_checkpoint_chunk_response_arg_type message_;
    public:
      append_checkpoint_chunk_response_operation(append_checkpoint_chunk_response_arg_type && msg)
        :
        protocol_operation_type(&do_complete),
        message_(std::move(msg))
      {
      }
      ~append_checkpoint_chunk_response_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        append_checkpoint_chunk_response_operation * op(static_cast<append_checkpoint_chunk_response_operation *>(base));
        append_checkpoint_chunk_response_arg_type msg = std::move(op->message_);
        delete op;
        if (nullptr != owner) {
          owner->on_append_checkpoint_chunk_response(std::move(msg));
        }
      }
    };    

    template<typename _Messages, typename _Protocol, typename _Client>
    class set_configuration_request_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef _Client client_type;
      typedef typename messages_type::set_configuration_request_traits_type::arg_type set_configuration_request_arg_type;      
    private:
      client_type & client_;
      set_configuration_request_arg_type message_;
    public:
      set_configuration_request_operation(client_type & client, set_configuration_request_arg_type && msg)
        :
        protocol_operation_type(&do_complete),
        client_(client),
        message_(std::move(msg))
      {
      }
      ~set_configuration_request_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        set_configuration_request_operation * op(static_cast<set_configuration_request_operation *>(base));
        set_configuration_request_arg_type msg = std::move(op->message_);
        client_type & client(op->client_);
        delete op;
        if (nullptr != owner) {
          owner->on_set_configuration(client, std::move(msg));
        }
      }
    };

    template<typename _Messages, typename _Protocol, typename _ClientEndpoint>
    class open_session_request_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef _ClientEndpoint client_endpoint_type;
      typedef typename messages_type::open_session_request_traits_type::arg_type open_session_request_arg_type;      
    private:
      const client_endpoint_type & client_endpoint_;
      open_session_request_arg_type message_;
      std::chrono::time_point<std::chrono::steady_clock> now_;
    public:
      open_session_request_operation(const client_endpoint_type & client_endpoint, open_session_request_arg_type && msg, std::chrono::time_point<std::chrono::steady_clock> now)
        :
        protocol_operation_type(&do_complete),
        client_endpoint_(client_endpoint),
        message_(std::move(msg)),
        now_(now)
      {
      }
      ~open_session_request_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        open_session_request_operation * op(static_cast<open_session_request_operation *>(base));
        open_session_request_arg_type msg = std::move(op->message_);
        const client_endpoint_type & client_endpoint(op->client_endpoint_);
        auto now = op->now_;
        delete op;
        if (nullptr != owner) {
          owner->on_open_session(client_endpoint, std::move(msg), now);
        }
      }
    };

    template<typename _Messages, typename _Protocol, typename _ClientEndpoint>
    class close_session_request_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef _ClientEndpoint client_endpoint_type;
      typedef typename messages_type::close_session_request_traits_type::arg_type close_session_request_arg_type;      
    private:
      const client_endpoint_type & client_endpoint_;
      close_session_request_arg_type message_;
      std::chrono::time_point<std::chrono::steady_clock> now_;
    public:
      close_session_request_operation(const client_endpoint_type & client_endpoint, close_session_request_arg_type && msg, std::chrono::time_point<std::chrono::steady_clock> now)
        :
        protocol_operation_type(&do_complete),
        client_endpoint_(client_endpoint),
        message_(std::move(msg)),
        now_(now)
      {
      }
      ~close_session_request_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        close_session_request_operation * op(static_cast<close_session_request_operation *>(base));
        close_session_request_arg_type msg = std::move(op->message_);
        const client_endpoint_type & client_endpoint(op->client_endpoint_);
        auto now = op->now_;
        delete op;
        if (nullptr != owner) {
          owner->on_close_session(client_endpoint, std::move(msg), now);
        }
      }
    };

    template<typename _Messages, typename _Protocol, typename _ClientEndpoint>
    class linearizable_command_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
      typedef _Messages messages_type;
      typedef _ClientEndpoint client_endpoint_type;
      typedef typename messages_type::linearizable_command_traits_type::arg_type linearizable_command_arg_type;      
    private:
      const client_endpoint_type & client_endpoint_;
      linearizable_command_arg_type message_;
      std::chrono::time_point<std::chrono::steady_clock> now_;
    public:
      linearizable_command_operation(const client_endpoint_type & client_endpoint, linearizable_command_arg_type && msg, std::chrono::time_point<std::chrono::steady_clock> now)
        :
        protocol_operation_type(&do_complete),
        client_endpoint_(client_endpoint),
        message_(std::move(msg)),
        now_(now)
      {
      }
      ~linearizable_command_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        linearizable_command_operation * op(static_cast<linearizable_command_operation *>(base));
        linearizable_command_arg_type msg = std::move(op->message_);
        const client_endpoint_type & client_endpoint(op->client_endpoint_);
        auto now = op->now_;
        delete op;
        if (nullptr != owner) {
          owner->on_linearizable_command(client_endpoint, std::move(msg), now);
        }
      }
    };

    template<typename _Protocol>
    class log_sync_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
    private:
      uint64_t index_;
      std::chrono::time_point<std::chrono::steady_clock> now_;
    public:
      log_sync_operation(uint64_t index, std::chrono::time_point<std::chrono::steady_clock> now)
        :
        protocol_operation_type(&do_complete),
        index_(index),
        now_(now)
      {
      }
      ~log_sync_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        log_sync_operation * op(static_cast<log_sync_operation *>(base));
        auto index = op->index_;
        auto now = op->now_;
        delete op;
        if (nullptr != owner) {
          owner->on_log_sync(index, now);
        }
      }
    };

    template<typename _Protocol>
    class log_header_sync_operation : public protocol_operation<_Protocol>
    {
    public:
      typedef protocol_operation<_Protocol> protocol_operation_type;
    private:
      std::chrono::time_point<std::chrono::steady_clock> now_;
    public:
      log_header_sync_operation(std::chrono::time_point<std::chrono::steady_clock> now)
        :
        protocol_operation_type(&do_complete),
        now_(now)
      {
      }
      ~log_header_sync_operation()
      {
      }
      static void do_complete(_Protocol * owner, protocol_operation_type * base)
      {
        log_header_sync_operation * op(static_cast<log_header_sync_operation *>(base));
        auto now = op->now_;
        delete op;
        if (nullptr != owner) {
          owner->on_log_header_sync(now);
        }
      }
    };
  }
}
#endif
