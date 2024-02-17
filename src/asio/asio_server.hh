#ifndef __RAFT_ASIO_SERVER_HH__
#define __RAFT_ASIO_SERVER_HH__

#include <condition_variable>
#include <deque>
#include <mutex>
#include <set>
#include <thread>

#include "boost/asio.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/log/trivial.hpp"

#include "asio/asio_block_device.hh"
#include "asio/asio_messages.hh"
#include "asio/basic_file_object.hh"
#include "asio/disk_io_service.hh"
#include "leveldb_log.hh"
#include "protocol.hh"
#include "slice.hh"
#include "state_machine/session.hh"
#include "util/builder_communicator.hh"
#include "util/protocol_operation.hh"

namespace raft {
  namespace asio {

    struct communicator_peer
    {
      enum connection_state { DISCONNECTED, CONNECTING, CONNECTED };
      boost::asio::ip::tcp::endpoint peer_endpoint;
      boost::asio::ip::tcp::socket peer_socket;
      std::deque<std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>> write_queue;
      connection_state state;
      bool write_outstanding;

      communicator_peer(boost::asio::io_service & ios)
	:
	peer_socket(ios),
        state(DISCONNECTED),
        write_outstanding(false)
      {
      }

      ~communicator_peer()
      {
        if (peer_socket.is_open()) {
          boost::system::error_code ec;
          peer_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
          peer_socket.close(ec);
        }
      }
    };

    inline const char * to_string(communicator_peer::connection_state s)
    {
      switch(s) {
      case communicator_peer::DISCONNECTED:
        return "DISCONNECTED";
      case communicator_peer::CONNECTING:
        return "CONNECTING";
      case communicator_peer::CONNECTED:
        return "CONNECTED";
      default:
        return "UNKNOWN";
      }
    }

    template<typename T>
    class asio_tcp_base_communicator_single_thread_policy
    {
    public:
      typedef size_t endpoint;
    private:
      T & base_communicator()
      {
        return *static_cast<T *>(this);
      }

    protected:
      void internal_send(endpoint ep, const std::string & addr, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> && buf)
      {
        base_communicator().send_buffer(ep, addr, std::move(buf));
      }

      void dispatch()
      {
      }
    };

    template<typename T>
    class asio_tcp_base_communicator_multi_thread_policy
    {
    public:
      typedef size_t endpoint;
      struct message_and_endpoint
      {
        endpoint ep;
        std::string addr;
        std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> buf;
      };
    private:
      std::deque<message_and_endpoint> thread_queue_;
      std::mutex mutex_;

      T & base_communicator()
      {
        return *static_cast<T *>(this);
      }

    protected:
      void internal_send(endpoint ep, const std::string & addr, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> && buf)
      {
        std::lock_guard<std::mutex> lk(mutex_);
        thread_queue_.push_front( { ep, addr, std::move(buf) } );
      }
      void dispatch()
      {
        std::deque<message_and_endpoint> tmp;
        {
          std::lock_guard<std::mutex> lk(mutex_);
          std::swap(tmp, thread_queue_);
        }
        while(!tmp.empty()) {
          base_communicator().send_buffer(tmp.back().ep, tmp.back().addr, std::move(tmp.back().buf));
          tmp.pop_back();
        }
      }
    };

    template<typename _Messages, typename _Serialization>
    class asio_tcp_base_communicator : public asio_tcp_base_communicator_multi_thread_policy<asio_tcp_base_communicator<_Messages, _Serialization>>
    {
    public:
      typedef size_t endpoint;    
      typedef serialization<_Messages, _Serialization> serialization_type;
      friend asio_tcp_base_communicator_multi_thread_policy<asio_tcp_base_communicator<_Messages, _Serialization>>;
    private:
      
      boost::asio::io_service & io_service_;
      boost::asio::ip::tcp::endpoint endpoint_;
      // This contains all sockets regardless of state
      std::map<std::string, std::shared_ptr<communicator_peer> > sockets_;
      // As a perf optimization (avoiding scan through all sockets in handle_timer), put disconnected sockets here as well
      std::map<std::string, std::shared_ptr<communicator_peer> > disconnected_;

      struct connect_and_send_handler
      {
        asio_tcp_base_communicator * this_;
        std::string addr;
        std::shared_ptr<communicator_peer> socket_;

        connect_and_send_handler(asio_tcp_base_communicator * _this, std::string && _addr, std::shared_ptr<communicator_peer> socket)
          :
          this_(_this),
          addr(std::move(_addr)),
          socket_(socket)
        {
        }

        void operator()(boost::system::error_code ec)
        {
          BOOST_ASSERT(socket_->state = communicator_peer::CONNECTING);
          if (ec) {
            BOOST_LOG_TRIVIAL(warning) << "asio_tcp_base_communicator " << this_->endpoint_ << " failed to connect to " << addr <<
              ": " << ec;            
            socket_->state = communicator_peer::DISCONNECTED;
            this_->disconnected_.emplace(addr, socket_);
            return;
          }
          socket_->state = communicator_peer::CONNECTED;
          BOOST_LOG_TRIVIAL(info) << "asio_tcp_base_communicator " << this_->endpoint_ << " connected to " << addr;
          this_->send_next(addr, socket_);
          // BOOST_LOG_TRIVIAL(trace) << "asio_tcp_base_communicator " << this_->endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
          //   " bytes for operation " << reinterpret_cast<const rpc_header *>(buf[0].data())->operation <<
          //   " to " << ocket_[addr]->peer_endpoint;
        }
      };

      void send_next(const std::string & addr, std::shared_ptr<communicator_peer> socket)
      {
        // Perform any policy dispatch prior to checking socket queues
        this->dispatch();

        if(socket->write_queue.empty()) {
          return;
        }
	if (socket->state == communicator_peer::CONNECTED && !socket->write_outstanding) {
          auto buf = socket->write_queue.back().first;
          BOOST_ASSERT(nullptr != buf[0].data() && nullptr != buf[1].data());
	  BOOST_LOG_TRIVIAL(trace) << "asio_tcp_base_communicator " << endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
	      " bytes for operation " << reinterpret_cast<const rpc_header *>(buf[0].data())->operation <<
	      " to " << socket->peer_endpoint;
          socket->write_outstanding = true;
	  boost::asio::async_write(socket->peer_socket,
				   buf,
				   [this, sock_addr = std::string(addr), sock = socket](boost::system::error_code ec, std::size_t bytes_transferred) {
                                     BOOST_ASSERT(sock->write_outstanding);
                                     BOOST_ASSERT(!sock->write_queue.empty());
                                     sock->write_outstanding = false;
                                     if (ec) {
                                       BOOST_LOG_TRIVIAL(warning) << "asio_tcp_base_communicator " << this->endpoint_ << " error writing to " << sock_addr
                                                                  << ": " << ec << ". Closing socket and reconnecting.";
                                       sock->peer_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
                                       if (ec) {
                                         BOOST_LOG_TRIVIAL(warning) << "asio_tcp_base_communicator " << this->endpoint_ << " error shutting down socket to " << sock_addr
                                                                    << ": " << ec;
                                       }
                                       sock->peer_socket.close(ec);
                                       if (ec) {
                                         BOOST_LOG_TRIVIAL(warning) << "asio_tcp_base_communicator " << this->endpoint_ << " error closing socket to " << sock_addr
                                                                    << ": " << ec;
                                       }
                                       sock->state = communicator_peer::DISCONNECTED;
                                       this->disconnected_.emplace(sock_addr, sock);
                                     } else {
                                       BOOST_ASSERT(bytes_transferred == boost::asio::buffer_size(sock->write_queue.back().first));
                                       sock->write_queue.pop_back();                                       
                                       this->send_next(sock_addr, sock);
                                       BOOST_LOG_TRIVIAL(trace) << "asio_tcp_base_communicator " << this->endpoint_ << " completed write to " << sock_addr
                                                                << ", write_queue.size()=" << sock->write_queue.size();
                                     }
				   });
	}
      }

      void send_buffer(endpoint ep, const std::string & addr, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> && buf)
      {
	BOOST_LOG_TRIVIAL(trace) << "Entering asio_tcp_base_communicator::send_buffer on " << endpoint_;
	// TODO: Make sure at most one send outstanding on each socket?
	auto sit = sockets_.find(addr);
 	if (sit == sockets_.end()) {
	  // Only handling v4 addresses
	  auto pos = addr.find_last_of(':');
	  auto v4address = boost::asio::ip::address_v4::from_string(addr.substr(0, pos));
	  auto port = boost::lexical_cast<unsigned short>(addr.substr(pos+1));
	  auto ins_it = sockets_.emplace(addr, std::shared_ptr<communicator_peer>(new communicator_peer(io_service_)));
	  ins_it.first->second->peer_endpoint.address(v4address);
	  ins_it.first->second->peer_endpoint.port(port);
          ins_it.first->second->state = communicator_peer::CONNECTING;
          BOOST_ASSERT(nullptr != buf.first[0].data() && nullptr != buf.first[1].data());
	  ins_it.first->second->write_queue.push_front(std::move(buf));
          BOOST_LOG_TRIVIAL(info) << "asio_tcp_base_communicator " << endpoint_ << " connecting to " << addr;
          if (0 == ins_it.first->second->write_queue.size() % 100) {
            BOOST_LOG_TRIVIAL(warning) << "asio_tcp_base_communicator " << endpoint_ << " write queue for " << addr
                                       << " has " << ins_it.first->second->write_queue.size() << " elements"
                                       << ", write_outstanding=" << (ins_it.first->second->write_outstanding ? "true" : "false")
                                       << ", state=" << to_string(ins_it.first->second->state);
          }
          BOOST_LOG_TRIVIAL(trace) << "asio_tcp_base_communicator " << endpoint_ << " write queue for " << addr
                                   << " has " << ins_it.first->second->write_queue.size() << " elements"
                                   << ", write_outstanding=" << (ins_it.first->second->write_outstanding ? "true" : "false")
                                   << ", state=" << to_string(ins_it.first->second->state);
          ins_it.first->second->peer_socket.async_connect(ins_it.first->second->peer_endpoint, connect_and_send_handler(this, std::string(addr), ins_it.first->second));
	} else {
          BOOST_ASSERT(nullptr != buf.first[0].data() && nullptr != buf.first[1].data());
          sit->second->write_queue.push_front(std::move(buf));
          if (0 == sit->second->write_queue.size() % 100) {
            BOOST_LOG_TRIVIAL(warning) << "asio_tcp_base_communicator " << endpoint_ << " write queue for " << addr
                                       << " has " << sit->second->write_queue.size() << " elements"
                                       << ", write_outstanding=" << (sit->second->write_outstanding ? "true" : "false")
                                       << ", state=" << to_string(sit->second->state);
          }
          BOOST_LOG_TRIVIAL(trace) << "asio_tcp_base_communicator " << endpoint_ << " write queue for " << addr
                                   << " has " << sit->second->write_queue.size() << " elements"
                                   << ", write_outstanding=" << (sit->second->write_outstanding ? "true" : "false")
                                   << ", state=" << to_string(sit->second->state);
          if (sit->second->state == communicator_peer::CONNECTED) {
            send_next(addr, sit->second);
          } else if (sit->second->state == communicator_peer::DISCONNECTED) {
            BOOST_ASSERT(disconnected_.end() != disconnected_.find(addr));
            disconnected_.erase(addr);
            sit->second->state = communicator_peer::CONNECTING;
            BOOST_LOG_TRIVIAL(info) << "asio_tcp_base_communicator " << endpoint_ << " connecting to " << sit->first;
            sit->second->peer_socket.async_connect(sit->second->peer_endpoint, connect_and_send_handler(this, std::string(addr), sit->second));
          }
        }
      }

    protected:
      asio_tcp_base_communicator(boost::asio::io_service & ios,
                             const boost::asio::ip::tcp::endpoint & endpoint)
	:
	io_service_(ios),
	endpoint_(endpoint)
      {
      }

      template<typename _T>
      void send(endpoint ep, const std::string & addr, _T && msg)
      {
        // TODO: Fix this!!!!!!
        std::size_t sz = 128*1024;
	  uint8_t * mem = new uint8_t [sz];
	  auto buf = serialization_type::serialize(boost::asio::buffer(mem, sz), std::move(msg));
          for(const auto & b : buf.first) {
            BOOST_LOG_TRIVIAL(trace) << "asio_tcp_base_commmunicator at " << endpoint_ << " sending to Server(" << ep << ") at address " << addr << " : " << raft::slice(reinterpret_cast<const uint8_t *>(b.data()), b.size());
          }
	  this->internal_send(ep, addr, std::move(buf));
      }

    public:
      void handle_timer()
      {
        // Initiate connections for any disconnected sockets.
        if (!disconnected_.empty()) {
          for(auto & s : disconnected_) {
            BOOST_ASSERT(s.second->state == communicator_peer::DISCONNECTED);
            s.second->state = communicator_peer::CONNECTING;
            BOOST_LOG_TRIVIAL(info) << "asio_tcp_base_communicator " << endpoint_ << " connecting to " << s.first;
            s.second->peer_socket.async_connect(s.second->peer_endpoint, connect_and_send_handler(this, std::string(s.first), s.second));
          }
          disconnected_.clear();
        }
        // Perform any policy dispatch
        this->dispatch();
      }
    };

    template<typename _Builders, typename _Serialization>
    struct asio_tcp_communicator_metafunction
    {
      template <typename _Messages>
      struct apply
      {
        typedef raft::util::builder_communicator<_Messages, _Builders, asio_tcp_base_communicator<_Messages, _Serialization>> type;
      };
    };
    
    template<typename _Dispatcher>
    class raft_receiver
    {
    public:
      typedef _Dispatcher dispatcher_type;
    protected:
      boost::asio::ip::tcp::socket socket_;
      boost::asio::ip::tcp::endpoint endpoint_;
      rpc_header header_;
      uint8_t * buffer_;
      std::size_t buffer_size_;
      raft::util::call_on_delete deleter_;
      dispatcher_type dispatcher_;
      uint64_t my_cluster_id_;
      
      void dispatch(boost::asio::const_buffer buf)
      {
	uint16_t op = header_.operation;
	dispatcher_(op, buf, std::move(deleter_));
	if (!deleter_) {
          buffer_ = nullptr;
          buffer_size_ = 0;
	}
      }

      void shutdown_socket()
      {
        boost::system::error_code ec;
        socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
        if (ec) {
          BOOST_LOG_TRIVIAL(warning) << "[raft_reciever::shutdown_socket] Server(" << my_cluster_id_ << ") at " << endpoint_ << " Error shutting down socket: " << ec;
        }
        socket_.close(ec);
        if (ec) {
          BOOST_LOG_TRIVIAL(warning) << "[raft_reciever::shutdown_socket] Server(" << my_cluster_id_ << ") at " << endpoint_ << " Error closing socket: " << ec;
        }
      }

      void handle_body_read(boost::system::error_code ec, std::size_t bytes_transferred)
      {
	if (ec) {
          BOOST_LOG_TRIVIAL(warning) << "[raft_reciever::handle_body_read] Server(" << my_cluster_id_ << ") at " << endpoint_ << " Error reading socket: " << ec
                                     << ".  Shutting down socket.";
          shutdown_socket();
          return;
	}
        BOOST_LOG_TRIVIAL(trace) << "[raft_receiver::handle_body_read] Server(" << my_cluster_id_ << ") at " << endpoint_ << " handle_body_read : " << raft::slice(&buffer_[0], bytes_transferred);
        dispatch(boost::asio::const_buffer(&buffer_[0], bytes_transferred));
	header_.magic = rpc_header::POISON;
	header_.payload_length = 0;
	boost::asio::async_read(socket_,
				boost::asio::buffer(&header_, sizeof(rpc_header)),
				std::bind(&raft_receiver::handle_header_read, this, std::placeholders::_1, std::placeholders::_2));
      }

      void handle_header_read(boost::system::error_code ec, std::size_t bytes_transferred)
      {
	if (ec) {
          BOOST_LOG_TRIVIAL(warning) << "[raft_reciever::handle_header_read] Server(" << my_cluster_id_ << ") at " << endpoint_ << " Error reading socket: " << ec << ".  Read " << bytes_transferred << " of " << sizeof(rpc_header)
                                   << " header bytes";
          if (bytes_transferred > 0) {
            BOOST_LOG_TRIVIAL(warning) << "[raft_reciever::handle_header_read] Server(" << my_cluster_id_ << ") at " << endpoint_ << " header read = { \"magic\" = " << header_.magic << ", \"payload_length\" = " << header_.payload_length << " }";
          }
          shutdown_socket();
	  return;
	}
	BOOST_ASSERT(bytes_transferred == sizeof(rpc_header));
	BOOST_ASSERT(header_.magic == rpc_header::MAGIC());
	BOOST_ASSERT(header_.payload_length > 0);
        BOOST_LOG_TRIVIAL(trace) << "[raft_reciever::handle_header_read] Server(" << my_cluster_id_
                                 << ") at " << endpoint_
                                 << " header read = { \"magic\" = " << (uint16_t) header_.magic
                                 << ", \"payload_length\" = " << (uint32_t) header_.payload_length
                                 << ", \"service\" = " << (uint16_t) header_.service
                                 << ", \"operation\" = " << (uint16_t) header_.operation
                                 << " }";
        if (buffer_size_ < header_.payload_length) {
          // Don't worry about overwriting buffer_ since deleter_ has a copy of it
          buffer_ = new uint8_t[header_.payload_length];
          buffer_size_ = header_.payload_length;
          deleter_ = raft::util::call_on_delete([ptr = buffer_](){ delete [] ptr; });
        }
        BOOST_ASSERT(buffer_ != nullptr);
        BOOST_ASSERT(deleter_);
        
	boost::asio::async_read(this->socket_,
				boost::asio::buffer(&buffer_[0], header_.payload_length),
				std::bind(&raft_receiver::handle_body_read, this, std::placeholders::_1, std::placeholders::_2));
      }
      
    public:

      virtual ~raft_receiver()
      {
        if (socket_.is_open()) {
          boost::system::error_code ec;
          socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
          socket_.close(ec);
        }
      }
      
      template<typename _Callback>
      raft_receiver(uint64_t server_id, boost::asio::ip::tcp::socket && s, boost::asio::ip::tcp::endpoint && e, _Callback && dispatcher)
	:
	socket_(std::move(s)),
	endpoint_(std::move(e)),
	buffer_(nullptr),
        buffer_size_(0),
	dispatcher_(std::move(dispatcher)),
        my_cluster_id_(server_id)
      {
	header_.magic = rpc_header::POISON;
	header_.payload_length = 0;
	boost::asio::async_read(socket_,
				boost::asio::buffer(&header_, sizeof(rpc_header)),
				std::bind(&raft_receiver::handle_header_read, this, std::placeholders::_1, std::placeholders::_2));
      }
    };

    template<typename _Messages, typename _Builders, typename _Serialization, typename _Protocol, typename _Client, typename _ClientSessionManager>
    class client_dispatcher
    {
    public:
      typedef serialization<_Messages, _Serialization> serialization_type;
      typedef _Messages messages_type;
      typedef _Builders builders_type;
      typedef _Protocol protocol_type;
      typedef _Client client_type;
      typedef _ClientSessionManager session_manager_type;
      typedef typename messages_type::client_result_type client_result_type;
    private:
      protocol_type & protocol_;
      session_manager_type & session_manager_;
      client_type * client_;
    public:
      client_dispatcher(protocol_type & protocol, session_manager_type & session_manager)
    	:
    	protocol_(protocol),
        session_manager_(session_manager),
    	client_(nullptr)
      {
      }
      void set_client(client_type * client)
      {
	client_ = client;
      }
      void operator()(uint16_t op, boost::asio::const_buffer buf, raft::util::call_on_delete && deleter)
      {
    	switch(op) {
    	case serialization_type::SET_CONFIGURATION_REQUEST:
    	  {
    	    protocol_.on_set_configuration(*client_, serialization_type::deserialize_set_configuration_request(buf, std::move(deleter)), std::chrono::steady_clock::now());
    	    break;
    	  }
    	case serialization_type::GET_CONFIGURATION_REQUEST:
    	  {
            auto callback = [this](client_result_type result, uint64_t id, std::vector<std::pair<uint64_t, std::string>> && cfg) {
                              this->client_->on_get_configuration_response(result, id, std::move(cfg));
                            };
    	    protocol_.on_get_configuration(std::move(callback), serialization_type::deserialize_get_configuration_request(buf, std::move(deleter)), std::chrono::steady_clock::now());
    	    break;
    	  }
        case serialization_type::OPEN_SESSION_REQUEST:
          {
    	    session_manager_.on_open_session(client_, serialization_type::deserialize_open_session_request(buf, std::move(deleter)), std::chrono::steady_clock::now());
            break;
          }
        case serialization_type::CLOSE_SESSION_REQUEST:
          {
    	    session_manager_.on_close_session(client_, serialization_type::deserialize_close_session_request(buf, std::move(deleter)), std::chrono::steady_clock::now());
            break;
          }
        case serialization_type::LINEARIZABLE_COMMAND_REQUEST:
          {
    	    session_manager_.on_linearizable_command(client_, serialization_type::deserialize_linearizable_command_request(buf, std::move(deleter)), std::chrono::steady_clock::now());
            break;
          }
    	default:
    	  BOOST_LOG_TRIVIAL(error) << "client_dispatcher received unsupported operation " << op;
    	  break;
    	}
      }
    };

    template<typename T>
    class raft_client_single_thread_policy
    {
    public:
      typedef size_t endpoint;
    private:
      T & base_client()
      {
        return *static_cast<T *>(this);
      }

    protected:
      void internal_send(std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> && buf)
      {
        base_client().send_buffer(std::move(buf));
      }
    public:
      static void handle_timer()
      {
      }
    };

    template<typename T>
    class raft_client_multi_thread_policy
    {
    private:
      static std::deque<std::pair<T *, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>> & thread_queue()
      {
        static std::deque<std::pair<T *, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>> thread_queue_;
        return thread_queue_;
      }
      
      static std::mutex & mutex()
      {
        static std::mutex mutex_;
        return mutex_;
      }

      T & base_client()
      {
        return *static_cast<T *>(this);
      }

    protected:
      void internal_send(std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> && buf)
      {
        std::lock_guard<std::mutex> lk(mutex());
        thread_queue().push_front(std::make_pair(&base_client(), std::move(buf)));
      }
    public:
      static void handle_timer()
      {
        std::deque<std::pair<T *, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>> tmp;
        {
          std::lock_guard<std::mutex> lk(mutex());
          std::swap(tmp, thread_queue());
        }
        while(!tmp.empty()) {
          tmp.back().first->send_buffer(std::move(tmp.back().second));
          tmp.pop_back();
        }
      }
    };

    // This is both a client communicator and receiver
    template<typename _Messages, typename _Builders, typename _Serialization, typename _Protocol, typename _ClientSessionManager>
    class raft_client : public raft_receiver<client_dispatcher<_Messages, _Builders, _Serialization, _Protocol, raft_client<_Messages, _Builders, _Serialization, _Protocol, _ClientSessionManager>, _ClientSessionManager>>, public raft_client_multi_thread_policy<raft_client<_Messages, _Builders, _Serialization, _Protocol, _ClientSessionManager>>, public raft::client_completion_operation<_Messages>
    {
    private:
      std::deque<std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>> write_queue_;
      bool write_outstanding_;

      void handle_send(boost::system::error_code ec, std::size_t bytes_transferred)
      {
        BOOST_ASSERT(write_outstanding_);
        BOOST_ASSERT(!write_queue_.empty());
        write_outstanding_ = false;
        if(ec) {
          BOOST_LOG_TRIVIAL(warning) << "[raft_client::handle_send] error writing to client " << ec;
          this->shutdown_socket();
          // We can't recover this client socket so just clear the write queue.   If the client
          // picks with a different peer (or perhaps even us again) the linearizability protocol
          // will handle resending
          write_queue_.clear();
        } else {
          write_queue_.pop_back();
        }
        send_next();
      }

      void send_next()
      {
        if (!write_outstanding_ && !write_queue_.empty()) {
          write_outstanding_ = true;
	  boost::asio::async_write(this->socket_,
				   write_queue_.back().first,
				   [this](boost::system::error_code ec, std::size_t bytes_transferred) {
                                     this->handle_send(ec, bytes_transferred);
				   });
        }
      }
      
    public:
      typedef client_dispatcher<_Messages, _Builders, _Serialization, _Protocol, raft_client<_Messages, _Builders, _Serialization, _Protocol, _ClientSessionManager>, _ClientSessionManager> dispatcher_type;
      typedef typename dispatcher_type::messages_type::client_result_type client_result_type;
      typedef typename dispatcher_type::serialization_type serialization_type;
      typedef typename dispatcher_type::builders_type::set_configuration_response_builder_type set_configuration_response_builder;
      typedef typename dispatcher_type::messages_type::set_configuration_response_traits_type::arg_type set_configuration_response_arg_type;
      typedef typename dispatcher_type::builders_type::get_configuration_response_builder_type get_configuration_response_builder;
      typedef typename dispatcher_type::messages_type::get_configuration_response_traits_type::arg_type get_configuration_response_arg_type;
      typedef typename dispatcher_type::messages_type::open_session_response_traits_type::arg_type open_session_response_arg_type;
      typedef typename dispatcher_type::messages_type::close_session_response_traits_type::arg_type close_session_response_arg_type;
      typedef typename dispatcher_type::messages_type::client_response_traits_type::arg_type client_response_arg_type;

      raft_client(uint64_t server_id, boost::asio::ip::tcp::socket && s, boost::asio::ip::tcp::endpoint && e, dispatcher_type && dispatcher)
	:
	raft_receiver<dispatcher_type>(server_id, std::move(s), std::move(e), std::move(dispatcher)),
        client_completion_operation<_Messages>(&do_client_complete),
        write_outstanding_(false)
      {
	// TODO: This is grotesque; set the dispatcher to know that this is the client
	this->dispatcher_.set_client(this);
      }

      void send_buffer(std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> && buf)
      {
        write_queue_.push_front(std::move(buf));
        BOOST_LOG_TRIVIAL(trace) << "raft_client sending " << boost::asio::buffer_size(write_queue_.front().first) <<
          " bytes for response to operation " << reinterpret_cast<const rpc_header *>(write_queue_.front().first[0].data())->operation <<
          " to " << this->endpoint_;
        send_next();
      }
      
      template<typename _T>
      void send(_T && msg)
      {
        uint8_t * mem = new uint8_t [128*1024];
        this->internal_send(serialization_type::serialize(boost::asio::buffer(mem, 128*1024), std::move(msg)));
      }

      void send_set_configuration_response(set_configuration_response_arg_type && resp)
      {
        send(std::move(resp));
      }
      void send_get_configuration_response(get_configuration_response_arg_type && resp)
      {
        send(std::move(resp));
      }
      void send_open_session_response(open_session_response_arg_type && resp)
      {
        send(std::move(resp));
      }
      void send_close_session_response(close_session_response_arg_type && resp)
      {
        send(std::move(resp));
      }
      void send_client_response(client_response_arg_type && resp)
      {
        send(std::move(resp));
      }

      // Set configuration client completion
      static void do_client_complete(client_completion_operation<_Messages> * base,
                                     client_result_type result,
                                     std::vector<std::pair<uint64_t, std::string>> && bad_servers)
      {
        raft_client * cli(static_cast<raft_client *>(base));
        cli->on_configuration_response(result, std::move(bad_servers));
      }

      void on_configuration_response(client_result_type result, std::vector<std::pair<uint64_t, std::string>> && bad_servers)
      {
        set_configuration_response_builder bld;
	bld.result(result);
	{
	  auto scdb = bld.bad_servers();
	  for(const auto & bs : bad_servers) {
	    scdb.server().id(bs.first).address(bs.second.c_str());
	  }
	}
	auto resp = bld.finish();
	send_set_configuration_response(std::move(resp));	
      }

      void on_get_configuration_response(client_result_type result, uint64_t id, std::vector<std::pair<uint64_t, std::string>> && cfg)
      {
        get_configuration_response_builder bld;
	bld.result(result);
        bld.id(id);
	{
	  auto scdb = bld.configuration();
	  for(const auto & server : cfg) {
	    scdb.server().id(server.first).address(server.second.c_str());
	  }
	}
	auto resp = bld.finish();
	send_get_configuration_response(std::move(resp));	
      }
    };

    template<typename _Messages, typename _Builders, typename _Serialization>
    struct raft_protocol_type_builder
    {
      typedef raft::protocol<asio_tcp_communicator_metafunction<_Builders, _Serialization>, _Messages> type;
    };

    template<typename _Messages, typename _Builders, typename _Serialization, typename _StateMachine, typename _ClientCommunicator, typename _LogWriter>
    class protocol_box
    {
    public:
      typedef protocol_box<_Messages, _Builders, _Serialization, _StateMachine, _ClientCommunicator, _LogWriter> this_type;
      typedef _Messages messages_type;
      typedef typename messages_type::log_entry_type log_entry_type;
      typedef typename messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
      typedef typename messages_type::client_result_type client_result_type;
      typedef typename messages_type::vote_request_traits_type::arg_type vote_request_arg_type;
      typedef typename messages_type::vote_response_traits_type::arg_type vote_response_arg_type;
      typedef typename messages_type::append_checkpoint_chunk_request_traits_type::arg_type append_checkpoint_chunk_request_arg_type;
      typedef typename messages_type::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;
      typedef typename messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
      typedef typename messages_type::append_entry_response_traits_type::arg_type append_entry_response_arg_type;
      typedef typename messages_type::set_configuration_request_traits_type::arg_type set_configuration_request_arg_type;
      typedef typename messages_type::get_configuration_request_traits_type::arg_type get_configuration_request_arg_type;
      typedef typename messages_type::open_session_request_traits_type::arg_type open_session_request_arg_type;
      typedef typename messages_type::close_session_request_traits_type::arg_type close_session_request_arg_type;
      typedef typename messages_type::linearizable_command_request_traits_type::arg_type linearizable_command_request_arg_type;
      typedef serialization<messages_type, _Serialization> serialization_type;
      typedef typename raft_protocol_type_builder<messages_type, _Builders, _Serialization>::type raft_protocol_type;
      typedef typename raft_protocol_type::communicator_type communicator_type;
      typedef _StateMachine state_machine_type;
      typedef _ClientCommunicator client_communicator_type;
      typedef ::raft::state_machine::client_session_manager<_Messages, _Builders, _Serialization, raft_protocol_type, client_communicator_type, state_machine_type> session_manager_type;
      typedef _LogWriter log_writer_type;
      typedef typename raft_protocol_type::communicator_type::endpoint endpoint_type;
    private:
      typename raft_protocol_type::log_type l_;
      typename raft_protocol_type::checkpoint_data_store_type store_;
      typename raft_protocol_type::configuration_manager_type config_manager_;
      std::shared_ptr<raft_protocol_type> protocol_;
      std::shared_ptr<session_manager_type> session_manager_;
      state_machine_type state_machine_;
      log_writer_type & log_writer_;
      uint64_t last_log_index_written_;

      // Checkpoint when the log has this many entries
      uint32_t checkpoint_interval_ = 100000;

    public:
      protocol_box(uint64_t server_id,
                   std::pair<const log_entry_type *, raft::util::call_on_delete> && config,
                   typename raft_protocol_type::communicator_type & protocol_comm,
                   client_communicator_type & client_comm,
                   log_writer_type & log_writer)
        :
	config_manager_(server_id),
        log_writer_(log_writer),
        last_log_index_written_(0)
      {
	// Set sync callback in the log (TODO: Fix this is dumb way of doing things)
	l_.set_log_header_writer(&log_writer_);

        if (nullptr != config.first) {
          l_.append(std::move(config));
        }
	l_.update_header(0, raft_protocol_type::INVALID_PEER_ID());
	protocol_.reset(new raft_protocol_type(protocol_comm, l_, store_, config_manager_));

        // Create the session manager and connect it up to the protocol box
        session_manager_.reset(new session_manager_type(*protocol_.get(), client_comm, state_machine_));
        protocol_->set_state_machine([this](log_entry_const_arg_type le, uint64_t idx, size_t leader_id) { this->session_manager_->apply(le, idx, leader_id); });
        protocol_->set_state_change_listener([this](typename raft_protocol_type::state s, uint64_t t) { this->session_manager_->on_protocol_state_change(s, t); });
      }

      void on_vote_request(vote_request_arg_type && req)
      {
        protocol_->on_vote_request(std::move(req), std::chrono::steady_clock::now());
      }

      void on_vote_response(vote_response_arg_type && resp)
      {
        protocol_->on_vote_response(std::move(resp), std::chrono::steady_clock::now());
      }
      void on_append_entry_request(append_entry_request_arg_type && req)
      {
        protocol_->on_append_entry_request(std::move(req), std::chrono::steady_clock::now());
      }
      void on_append_entry_response(append_entry_response_arg_type && resp)
      {
        protocol_->on_append_entry_response(std::move(resp), std::chrono::steady_clock::now());
      }
    
      void on_append_checkpoint_chunk_request(append_checkpoint_chunk_request_arg_type && req)
      {
        protocol_->on_append_checkpoint_chunk_request(std::move(req), std::chrono::steady_clock::now());
      }
      void on_append_checkpoint_chunk_response(append_checkpoint_chunk_response_arg_type && resp)
      {
        protocol_->on_append_checkpoint_chunk_response(std::move(resp), std::chrono::steady_clock::now());
      }
    
      template<typename _Client>
      void on_set_configuration(_Client & client, set_configuration_request_arg_type && req, std::chrono::time_point<std::chrono::steady_clock> now)
      {
        protocol_->on_set_configuration(client, std::move(req), now);
      }
      
      template<typename _Client>
      void on_get_configuration(_Client && client, get_configuration_request_arg_type && req, std::chrono::time_point<std::chrono::steady_clock> now)
      {
        protocol_->on_get_configuration(std::move(client), std::move(req), now);
      }
      
      void on_log_sync(uint64_t index)
      {
        on_log_sync(index, std::chrono::steady_clock::now());
      }
      
      void on_log_sync(uint64_t index, std::chrono::time_point<std::chrono::steady_clock> now)
      {
        protocol_->on_log_sync(index, now);
      }
      
      void on_log_header_sync()
      {
        on_log_header_sync(std::chrono::steady_clock::now());
      }
    
      void on_log_header_sync(std::chrono::time_point<std::chrono::steady_clock> now)
      {
        protocol_->on_log_header_sync(now);
      }
    
      template<typename _ClientEndpoint>
      void on_open_session(const _ClientEndpoint & ep,
                           open_session_request_arg_type && req,
                           std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        session_manager_->on_open_session(ep, std::move(req), clock_now);
      }
      template<typename _ClientEndpoint>
      void on_close_session(const _ClientEndpoint & ep,
                            close_session_request_arg_type && req,
                            std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        session_manager_->on_close_session(ep, std::move(req), clock_now);
      }
      template<typename _ClientEndpoint>
      void on_linearizable_command(const _ClientEndpoint & ep,
                                   linearizable_command_request_arg_type && req,
                                   std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        session_manager_->on_linearizable_command(ep, std::move(req), clock_now);
      }
    
      void handle_timer()
      {
        if (!!protocol_) {
          protocol_->on_timer();

          boost::array<uint8_t, 128*1024> tmp_buf;
          if (!l_.empty()) {
            // TODO: a checkpoint could cause a problem here because we could truncate a log entry that
            // didn't get persisted?  Is this logic sufficient?
            if (last_log_index_written_ < l_.index_begin()) {
              last_log_index_written_ = l_.index_begin();
            }
            if (last_log_index_written_ < l_.index_end()) {
              BOOST_LOG_TRIVIAL(info) << "[raft::asio::protocol_box::handle_timer] Server(" << config_manager_.configuration().my_cluster_id()
                                      << ") writing log records [" << last_log_index_written_ << ", " << l_.index_end() << ")";
            }
            for(;last_log_index_written_ < l_.index_end(); ++last_log_index_written_) {
              log_writer_.append_record(serialization_type::serialize(boost::asio::buffer(tmp_buf), l_.entry(last_log_index_written_)), last_log_index_written_);
            }
          }

          // All of our checkpointing is to in-memory right now so immediately complete any required checkpoint
          // sync
          if (protocol_->checkpoint_sync_required()) {
            protocol_->on_checkpoint_sync();
          }

          // Time to take a periodic checkpoint?
          if (l_.index_begin() + checkpoint_interval_ <= l_.index_end()) {
            BOOST_LOG_TRIVIAL(info) << "[raft::asio::protocol_box::handle_timer] Server(" << config_manager_.configuration().my_cluster_id()
                                    << ") initiating checkpoint";
            session_manager_->on_checkpoint_request(std::chrono::steady_clock::now());
            BOOST_LOG_TRIVIAL(info) << "[raft::asio::protocol_box::handle_timer] Server(" << config_manager_.configuration().my_cluster_id()
                                    << ") completed checkpoint";
          }
        }
      }

      void checkpoint_interval(uint32_t val)
      {
        checkpoint_interval_ = val;
      }

      // ONLY FOR TESTING!!!! Not safe to use when things are running!
      const state_machine_type & state_machine() const
      {
        return state_machine_;
      }
    };

    template<typename _Messages, typename _Builders, typename _Serialization, typename _StateMachine, typename _ClientCommunicator, typename _LogWriter>
    class protocol_box_thread
    {
    public:
      typedef protocol_box_thread<_Messages, _Builders, _Serialization, _StateMachine, _ClientCommunicator, _LogWriter> this_type;
      typedef protocol_box<_Messages, _Builders, _Serialization, _StateMachine, _ClientCommunicator, _LogWriter> protocol_box_type;
      typedef _Messages messages_type;
      typedef typename messages_type::log_entry_type log_entry_type;
      typedef typename messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
      typedef typename messages_type::client_result_type client_result_type;
      typedef typename messages_type::vote_request_traits_type::arg_type vote_request_arg_type;
      typedef typename messages_type::vote_response_traits_type::arg_type vote_response_arg_type;
      typedef typename messages_type::append_checkpoint_chunk_request_traits_type::arg_type append_checkpoint_chunk_request_arg_type;
      typedef typename messages_type::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;
      typedef typename messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
      typedef typename messages_type::append_entry_response_traits_type::arg_type append_entry_response_arg_type;
      typedef typename messages_type::set_configuration_request_traits_type::arg_type set_configuration_request_arg_type;
      typedef typename messages_type::get_configuration_request_traits_type::arg_type get_configuration_request_arg_type;
      typedef typename messages_type::open_session_request_traits_type::arg_type open_session_request_arg_type;
      typedef typename messages_type::close_session_request_traits_type::arg_type close_session_request_arg_type;
      typedef typename messages_type::linearizable_command_request_traits_type::arg_type linearizable_command_request_arg_type;
      typedef serialization<messages_type, _Serialization> serialization_type;
      typedef typename protocol_box_type::raft_protocol_type raft_protocol_type;
      typedef typename raft_protocol_type::communicator_type communicator_type;
      typedef typename protocol_box_type::state_machine_type state_machine_type;
      typedef typename protocol_box_type::client_communicator_type client_communicator_type;
      typedef typename protocol_box_type::session_manager_type session_manager_type;
      typedef typename protocol_box_type::log_writer_type log_writer_type;
      typedef typename protocol_box_type::endpoint_type endpoint_type;

      // Protocol operations
      typedef raft::util::protocol_operation<protocol_box_type> protocol_operation_type;
      typedef typename raft::util::protocol_operation<protocol_box_type>::queue_type protocol_operation_queue_type;
      typedef raft::util::vote_request_operation<messages_type, protocol_box_type> vote_request_operation_type;
      typedef raft::util::vote_response_operation<messages_type, protocol_box_type> vote_response_operation_type;
      typedef raft::util::append_entry_request_operation<messages_type, protocol_box_type> append_entry_request_operation_type;
      typedef raft::util::append_entry_response_operation<messages_type, protocol_box_type> append_entry_response_operation_type;
      typedef raft::util::append_checkpoint_chunk_request_operation<messages_type, protocol_box_type> append_checkpoint_chunk_request_operation_type;
      typedef raft::util::append_checkpoint_chunk_response_operation<messages_type, protocol_box_type> append_checkpoint_chunk_response_operation_type;

      typedef raft::util::log_sync_operation<protocol_box_type> log_sync_operation_type;
      typedef raft::util::log_header_sync_operation<protocol_box_type> log_header_sync_operation_type;
    private:
      protocol_box_type protocol_box_;
      protocol_operation_queue_type op_queue_;
      std::mutex op_queue_mutex_;
      std::condition_variable condvar_;
      std::atomic<bool> shutdown_;
      std::unique_ptr<std::thread> protocol_thread_;
      std::chrono::time_point<std::chrono::steady_clock> timer_;
      uint64_t my_cluster_id_;

      void enqueue(protocol_operation_type * op)
      {
        std::size_t sz = 0;
        {
          std::unique_lock<std::mutex> lk(op_queue_mutex_);
          op_queue_.push_back(*op);
          sz = op_queue_.size();
        }
        BOOST_LOG_TRIVIAL(trace) << "[raft::asio::protocol_box_thread::enqueue] Server(" << my_cluster_id_
                                 << ") op_queue_.size()=" << sz;
      }

      void thread_proc()
      {
        BOOST_LOG_TRIVIAL(info) << "[raft::asio::protocol_box_thread::thread_proc] Server(" << my_cluster_id_
                                << ") protocol thread proc starting";
        while(!this->shutdown_.load()) {
          protocol_operation_queue_type tmp;
          {
            std::unique_lock<std::mutex> lk(this->op_queue_mutex_);
            if (!this->op_queue_.empty()) {
              this->op_queue_.swap(tmp);
            } else {
              // BOOST_LOG_TRIVIAL(trace) << "[raft::asio::protocol_box_thread::thread_proc] Server(" << my_cluster_id_
              //                          << ") beginning condition variable wait";
              this->condvar_.wait_for(lk, std::chrono::milliseconds(1));
              // BOOST_LOG_TRIVIAL(trace) << "[raft::asio::protocol_box_thread::thread_proc] Server(" << my_cluster_id_
              //                          << ") ending condition variable wait";
            }
          }
          auto now = std::chrono::steady_clock::now();
          if (now > this->timer_) {
            this->protocol_box_.handle_timer();
            this->timer_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(1);
          }
          if (!tmp.empty()) {
            auto sz = tmp.size();
            BOOST_LOG_TRIVIAL(trace) << "[raft::asio::protocol_box_thread::thread_proc] Server(" << my_cluster_id_
                                     << ") beginning " << sz << " operation completions";
            while(!tmp.empty()) {
              protocol_operation_type * op = &tmp.front();
              tmp.pop_front();
              op->complete(&this->protocol_box_);
            }
            BOOST_LOG_TRIVIAL(trace) << "[raft::asio::protocol_box_thread::thread_proc] Server(" << my_cluster_id_
                                     << ") ending operation " << sz << " completions";
          }
        }
        BOOST_LOG_TRIVIAL(info) << "[raft::asio::protocol_box_thread::thread_proc] Server(" << my_cluster_id_
                                << ") protocol thread proc exiting";
      }

    public:
      protocol_box_thread(uint64_t server_id,
                          std::pair<const log_entry_type *, raft::util::call_on_delete> && config,
                          communicator_type & protocol_comm,
                          client_communicator_type & client_comm,
                          log_writer_type & log_writer)
        :
        protocol_box_(server_id, std::move(config), protocol_comm, client_comm, log_writer),
        shutdown_(false),
        timer_(std::chrono::steady_clock::now() + std::chrono::milliseconds(server_id*100)),
        my_cluster_id_(server_id)
      {
        protocol_thread_.reset(new std::thread([this]() { this->thread_proc(); }));
      }

      ~protocol_box_thread()
      {
        shutdown();
        // No need to lock as protocol thread is shutdown
        while(!op_queue_.empty()) {
          auto & front = op_queue_.front();
          op_queue_.pop_front();
          front.destroy();          
        }
      }

      void shutdown()
      {
        if (!!protocol_thread_) {
          shutdown_.store(true);
          condvar_.notify_all();
          protocol_thread_->join();
          protocol_thread_.reset();
        }
      }

      void on_vote_request(vote_request_arg_type && req)
      {
        enqueue(new vote_request_operation_type(std::move(req)));
      }

      void on_vote_response(vote_response_arg_type && resp)
      {
        enqueue(new vote_response_operation_type(std::move(resp)));
      }
      void on_append_entry_request(append_entry_request_arg_type && req)
      {
        enqueue(new append_entry_request_operation_type(std::move(req)));
      }
      void on_append_entry_response(append_entry_response_arg_type && resp)
      {
        enqueue(new append_entry_response_operation_type(std::move(resp)));
      }
    
      void on_append_checkpoint_chunk_request(append_checkpoint_chunk_request_arg_type && req)
      {
        enqueue(new append_checkpoint_chunk_request_operation_type(std::move(req)));
      }
      void on_append_checkpoint_chunk_response(append_checkpoint_chunk_response_arg_type && resp)
      {
        enqueue(new append_checkpoint_chunk_response_operation_type(std::move(resp)));
      }
    
      template<typename _Client>
      void on_set_configuration(_Client & client, set_configuration_request_arg_type && req, std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        typedef raft::util::set_configuration_request_operation<messages_type, protocol_box_type, _Client> set_configuration_request_operation_type;
        enqueue(new set_configuration_request_operation_type(client, std::move(req), clock_now));
      }
      
      template<typename _Client>
      void on_get_configuration(_Client && client, get_configuration_request_arg_type && req, std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        typedef raft::util::get_configuration_request_operation<messages_type, protocol_box_type, _Client> get_configuration_request_operation_type;
        enqueue(new get_configuration_request_operation_type(std::move(client), std::move(req), clock_now));
      }
      
      void on_log_sync(uint64_t index)
      {
        enqueue(new log_sync_operation_type(index, std::chrono::steady_clock::now()));
      }
      
      void on_log_header_sync()
      {
        enqueue(new log_header_sync_operation_type(std::chrono::steady_clock::now()));
      }

      template<typename _ClientEndpoint>
      void on_open_session(const _ClientEndpoint & ep,
                           open_session_request_arg_type && req,
                           std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        typedef raft::util::open_session_request_operation<messages_type, protocol_box_type, _ClientEndpoint> open_session_request_operation_type;
        enqueue(new open_session_request_operation_type(ep, std::move(req), clock_now));
      }
      template<typename _ClientEndpoint>
      void on_close_session(const _ClientEndpoint & ep,
                            close_session_request_arg_type && req,
                            std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        typedef raft::util::close_session_request_operation<messages_type, protocol_box_type, _ClientEndpoint> close_session_request_operation_type;
        enqueue(new close_session_request_operation_type(ep, std::move(req), clock_now));
      }
      template<typename _ClientEndpoint>
      void on_linearizable_command(const _ClientEndpoint & ep,
                                   linearizable_command_request_arg_type && req,
                                   std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        typedef raft::util::linearizable_command_request_operation<messages_type, protocol_box_type, _ClientEndpoint> linearizable_command_request_operation_type;
        enqueue(new linearizable_command_request_operation_type(ep, std::move(req), clock_now));
      }
    
      void handle_timer()
      {
      }


      void checkpoint_interval(uint32_t val)
      {
        protocol_box_.checkpoint_interval(val);
      }
      
      // ONLY FOR TESTING!!!! Not safe to use when things are running!
      const state_machine_type & state_machine() const
      {
        return protocol_box_.state_machine();
      }
    };

    template<typename _Protocol, typename _Serialization>
    class peer_dispatcher
    {
    public:
      typedef _Protocol raft_protocol_type;
      typedef typename raft_protocol_type::messages_type messages_type;
      typedef serialization<messages_type, _Serialization> serialization_type;
    private:
      raft_protocol_type & protocol_;      
    public:
      peer_dispatcher(raft_protocol_type & protocol)
	:
	protocol_(protocol)
      {
      }
      void operator()(uint16_t op, boost::asio::const_buffer buf, raft::util::call_on_delete && deleter)
      {
	switch(op) {
	case serialization_type::VOTE_REQUEST:
	  {
	    protocol_.on_vote_request(serialization_type::deserialize_vote_request(buf, std::move(deleter)));
	    break;
	  }
	case serialization_type::VOTE_RESPONSE:
	  {
	    protocol_.on_vote_response(serialization_type::deserialize_vote_response(buf, std::move(deleter)));
	    break;
	  }
	case serialization_type::APPEND_ENTRY_REQUEST:
	  {
	    protocol_.on_append_entry_request(serialization_type::deserialize_append_entry_request(buf, std::move(deleter)));
	    break;
	  }
	case serialization_type::APPEND_ENTRY_RESPONSE:
	  {
	    protocol_.on_append_entry_response(serialization_type::deserialize_append_entry_response(buf, std::move(deleter)));
	    break;
	  }
	case serialization_type::APPEND_CHECKPOINT_CHUNK_REQUEST:
	  {
	    protocol_.on_append_checkpoint_chunk_request(serialization_type::deserialize_append_checkpoint_chunk_request(buf, std::move(deleter)));
	    break;
	  }
	case serialization_type::APPEND_CHECKPOINT_CHUNK_RESPONSE:
	  {
	    protocol_.on_append_checkpoint_chunk_response(serialization_type::deserialize_append_checkpoint_chunk_response(buf, std::move(deleter)));
	    break;
	  }
	default:
	  BOOST_LOG_TRIVIAL(error) << "peer_dispatcher received unsupported operation " << op;
	  break;
	}
      }
    };

    template<typename T>
    class log_files_single_thread_policy
    {
    private:
      T & base()
      {
        return *static_cast<T *>(this);
      }

    protected:
      log_files_single_thread_policy()
      {
      }

      ~log_files_single_thread_policy()
      {
      }

    public:
      void async_write_log_header(uint64_t current_term, uint64_t voted_for)
      {
        base().process_header(current_term, voted_for);
      }

      void append_record(std::pair<boost::asio::const_buffer, raft::util::call_on_delete> && buf, uint64_t log_index)
      {
        base().process_record(std::move(buf), log_index);
      }
      
      void thread_policy_dispatch()
      {
      }
    };

    template<typename T>
    class log_files_multi_thread_policy
    {
    private:
      std::deque<std::pair<uint64_t, uint64_t>> header_thread_queue_;
      std::deque<std::pair<std::pair<boost::asio::const_buffer, raft::util::call_on_delete>, uint64_t>> record_thread_queue_;
      std::mutex mutex_;

      T & base()
      {
        return *static_cast<T *>(this);
      }

      log_files_multi_thread_policy()
      {
      }

      ~log_files_multi_thread_policy()
      {
      }

      friend T;

    public:
      void async_write_log_header(uint64_t current_term, uint64_t voted_for)
      {
        std::lock_guard<std::mutex> lk(mutex_);
        header_thread_queue_.push_front(std::make_pair(current_term, voted_for));
      }
      void append_record(std::pair<boost::asio::const_buffer, raft::util::call_on_delete> && buf, uint64_t log_index)
      {
        std::lock_guard<std::mutex> lk(mutex_);
        record_thread_queue_.push_front(std::make_pair(std::move(buf), log_index));
      }
      void thread_policy_dispatch()
      {
        std::deque<std::pair<uint64_t, uint64_t>> header_tmp;
        std::deque<std::pair<std::pair<boost::asio::const_buffer, raft::util::call_on_delete>, uint64_t>> record_tmp;
        {
          std::lock_guard<std::mutex> lk(mutex_);
          std::swap(header_tmp, header_thread_queue_);
          std::swap(record_tmp, record_thread_queue_);
        }
        while(!header_tmp.empty()) {
          base().process_header(header_tmp.back().first, header_tmp.back().second);
          header_tmp.pop_back();
        }
        while(!record_tmp.empty()) {
          base().process_record(std::move(record_tmp.back().first), record_tmp.back().second);
          record_tmp.pop_back();
        }
      }
    };

    template<typename _Protocol>
    class log_files : public log_files_multi_thread_policy<log_files<_Protocol>>
    {
    public:
      typedef _Protocol protocol_type;
    private:
      protocol_type * protocol_;
      // Log Disk Infrastructure
      int log_header_fd_;
      std::unique_ptr<basic_file_object<disk_io_service> > log_header_file_;
      log_header header_;
      int log_fd_;
      std::unique_ptr<raft::asio::writable_file> log_file_;
      std::unique_ptr<raft::leveldb::log_writer<raft::asio::writable_file>> log_writer_;
      // One past last log index written to the writer
      uint64_t last_log_index_written_;
      // One past last log index synced to disk
      uint64_t last_log_index_synced_;
      uint64_t my_cluster_id_;
      bool log_sync_in_progress_;
    public:
      log_files(boost::asio::io_service & ios,
                uint64_t server_id,
                const std::string & log_directory)
        :
        protocol_(nullptr),
	last_log_index_written_(0),
	last_log_index_synced_(0),
        my_cluster_id_(server_id),
        log_sync_in_progress_(false)
      {
	// Open the log file
	std::string log_header_file(log_directory);
	log_header_file += std::string("/log_header.bin");
	log_header_fd_ = ::open(log_header_file.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	if (-1 == log_header_fd_) {
	  BOOST_LOG_TRIVIAL(error) << "Server(" << my_cluster_id_ << ") Failed to open log header file " << log_header_file << ": " << ::strerror(errno);
	}
	log_header_file_.reset(new basic_file_object<disk_io_service>(ios, log_header_fd_));
	std::string log_file(log_directory);
	log_file += std::string("/log.bin");
	log_fd_ = ::open(log_file.c_str(), O_CREAT | O_WRONLY | O_APPEND, S_IRUSR | S_IWUSR);
	if (-1 == log_fd_) {
	  BOOST_LOG_TRIVIAL(error) << "Server(" << my_cluster_id_ << ") Failed to open log header file " << log_file << ": " << ::strerror(errno);
	}
	
	log_file_.reset(new raft::asio::writable_file(ios, log_fd_));
	log_writer_.reset(new raft::leveldb::log_writer<raft::asio::writable_file>(*log_file_.get()));
      }

      ~log_files()
      {
	::close(log_header_fd_);
	::close(log_fd_);
      }

      void set_protocol(protocol_type & protocol)
      {
        protocol_ = &protocol;
      }
      
      void handle_timer()
      {
        this->thread_policy_dispatch();
        // Sync to disk and let the protocol know when it's done
        if (!log_sync_in_progress_ && last_log_index_synced_ < last_log_index_written_) {
          BOOST_LOG_TRIVIAL(info) << "[raft::asio::log_files::handle_timer] Server(" << my_cluster_id_
                                  << ") syncing log to disk";
          log_sync_in_progress_ = true;
          uint64_t idx = last_log_index_written_;
          log_file_->flush_and_sync([this,idx](boost::system::error_code) {
                                      BOOST_LOG_TRIVIAL(info) << "[raft::asio::log_files::handle_timer] Server(" << my_cluster_id_
                                                              << ") log sync completed up to index " << idx;
                                      this->protocol_->on_log_sync(idx);
                                      this->last_log_index_synced_ = idx;
                                      this->log_sync_in_progress_ = false;
                                    });
        }
      }

      void process_header(uint64_t current_term, uint64_t voted_for)
      {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id_ <<
	  ") raft::asio::tcp_server request to sync log header current_term=" << current_term <<
	  " voted_for=" << voted_for;
	header_.current_term = current_term;
	header_.voted_for = voted_for;
	log_header_file_->async_write(boost::asio::buffer(&header_, sizeof(header_)),
				      [this](boost::system::error_code ec, std::size_t bytes_transferred) mutable {
					BOOST_LOG_TRIVIAL(info) << "[raft::asio::log_files::process_header] Server(" << this->my_cluster_id_ <<
					  ") completed sync log header sync";
					this->protocol_->on_log_header_sync();
				      });
      }

      void process_record(std::pair<boost::asio::const_buffer, raft::util::call_on_delete> && serialize_bufs, uint64_t log_index)
      {
        for(auto it = boost::asio::buffer_sequence_begin(serialize_bufs.first), e = boost::asio::buffer_sequence_end(serialize_bufs.first);
            it != e; ++it) {
          log_writer_->append_record(boost::asio::buffer_cast<const uint8_t *>(*it),
                                     boost::asio::buffer_size(*it));
        }
        BOOST_ASSERT(log_index >= last_log_index_written_);
        last_log_index_written_ = log_index+1;
      }
    };

    template<typename _Messages, typename _Builders, typename _Serialization, typename _StateMachine>
    class tcp_server : public raft::log_header_write
    {
    public:
      typedef tcp_server<_Messages, _Builders, _Serialization, _StateMachine> this_type;
      typedef typename _Messages::log_entry_type log_entry_type;
      typedef typename _Messages::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
      typedef serialization<_Messages, _Serialization> serialization_type;
      typedef protocol_box_thread<_Messages, _Builders, _Serialization, _StateMachine, this_type, this_type> raft_protocol_type;
      typedef typename raft_protocol_type::state_machine_type state_machine_type;
      typedef raft_client<_Messages, _Builders, _Serialization, raft_protocol_type, raft_protocol_type> raft_client_type;
      typedef typename raft_client_type::dispatcher_type client_dispatcher_type;
      typedef peer_dispatcher<raft_protocol_type, _Serialization> peer_dispatcher_type;
      typedef raft_receiver<peer_dispatcher_type> peer_receiver_type;
      typedef log_files<raft_protocol_type> log_files_type;
    private:
      boost::asio::io_service & io_service_;
      boost::asio::deadline_timer timer_;

      // Network Infrastructure
      boost::asio::ip::tcp::acceptor peer_listener_;
      boost::asio::ip::tcp::socket peer_listen_socket_;
      boost::asio::ip::tcp::endpoint peer_listen_endpoint_;
      std::vector<std::shared_ptr<peer_receiver_type>> receivers_;
      boost::asio::ip::tcp::acceptor client_listener_;
      boost::asio::ip::tcp::socket client_listen_socket_;
      boost::asio::ip::tcp::endpoint client_listen_endpoint_;
      std::set<std::unique_ptr<raft_client_type>> clients_;

      // // Log Disk Infrastructure
      std::unique_ptr<log_files_type> log_files_;
      uint64_t my_cluster_id_;
      std::unique_ptr<raft_protocol_type> protocol_;

      typename raft_protocol_type::communicator_type comm_;

      void handle_timer(boost::system::error_code ec)
      {
        // Periodic work for timer such as attempting reconnects
        comm_.handle_timer();

        raft_client_type::handle_timer();
        
	if (!!protocol_) {
          protocol_->handle_timer();
          log_files_->handle_timer();
	}
        
        timer_.expires_from_now(boost::posix_time::milliseconds(1));
        timer_.async_wait(std::bind(&tcp_server::handle_timer, this, std::placeholders::_1));
      }
      
      void peer_handle_accept(boost::system::error_code ec)
      {
	if (ec) {
	  BOOST_LOG_TRIVIAL(warning) << "raft::asio::tcp_server failed accepting connection: " << ec;
	  return;
	}
	
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id_ << ") raft::asio::tcp_server got connection on " << peer_listen_endpoint_;
	receivers_.push_back(std::make_shared<peer_receiver_type>(my_cluster_id_, std::move(peer_listen_socket_), std::move(peer_listen_endpoint_), peer_dispatcher_type(*protocol_.get())));
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id_ << ") raft::asio::tcp_server listening on " << peer_listener_.local_endpoint();
	peer_listener_.async_accept(peer_listen_socket_, peer_listen_endpoint_,
			       std::bind(&tcp_server::peer_handle_accept, this, std::placeholders::_1));
      }

      void client_handle_accept(boost::system::error_code ec)
      {
	if (ec) {
	  BOOST_LOG_TRIVIAL(warning) << "raft::asio::tcp_server failed accepting connection: " << ec;
	  return;
	}
	
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id_ << ") raft::asio::tcp_server got connection on " << client_listen_endpoint_;
	clients_.insert(std::make_unique<raft_client_type>(my_cluster_id_, std::move(client_listen_socket_), std::move(client_listen_endpoint_), client_dispatcher_type(*protocol_.get(), *protocol_.get())));
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id_ << ") raft::asio::tcp_server listening on " << client_listener_.local_endpoint();
	client_listener_.async_accept(client_listen_socket_, client_listen_endpoint_,
			       std::bind(&tcp_server::client_handle_accept, this, std::placeholders::_1));
      }

    public:
      tcp_server(boost::asio::io_service & ios,
		 uint64_t server_id,
		 const boost::asio::ip::tcp::endpoint & e,
		 const boost::asio::ip::tcp::endpoint & client_endpoint,
		 std::pair<const log_entry_type *, raft::util::call_on_delete> && config,
		 const std::string & log_directory)
	:
	io_service_(ios),
	timer_(io_service_),
	peer_listener_(io_service_, e),
	peer_listen_socket_(io_service_),
	client_listener_(io_service_, client_endpoint),
	client_listen_socket_(io_service_),
	comm_(io_service_, e),
        my_cluster_id_(server_id)
      {
        log_files_.reset(new log_files_type(ios, server_id, log_directory));
        protocol_.reset(new raft_protocol_type(server_id, std::move(config), comm_, *this, *this));

        log_files_->set_protocol(*protocol_);

	// Setup listener for interprocess communication
	// if (protocol_->includes_self()) {
	  // TODO: Handle v6
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id_ << ") raft::asio::tcp_server listening for peers on " << peer_listener_.local_endpoint();
	  peer_listener_.async_accept(peer_listen_socket_, peer_listen_endpoint_,
				  std::bind(&tcp_server::peer_handle_accept, this, std::placeholders::_1));
	  // TODO: Handle v6
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id_ << ") raft::asio::tcp_server listening for clients on " << client_listener_.local_endpoint();
	  client_listener_.async_accept(client_listen_socket_, client_listen_endpoint_,
				  std::bind(&tcp_server::client_handle_accept, this, std::placeholders::_1));
	// }
        
	// Setup periodic timer callbacks from ASIO
	timer_.expires_from_now(boost::posix_time::milliseconds(server_id*100));
	timer_.async_wait(std::bind(&tcp_server::handle_timer, this, std::placeholders::_1));
      }

      ~tcp_server()
      {
	// Use error code to prevent exception
	boost::system::error_code ec;
	peer_listener_.close(ec);
      }

      void async_write_log_header(uint64_t current_term, uint64_t voted_for) override
      {
        log_files_->async_write_log_header(current_term, voted_for);
      }

      void append_record(std::pair<boost::asio::const_buffer, raft::util::call_on_delete> && serialize_bufs, uint64_t log_index)
      {
        log_files_->append_record(std::move(serialize_bufs), log_index);
      }

      void checkpoint_interval(uint32_t val)
      {
        protocol_->checkpoint_interval(val);
      }

      ///
      /// Client Session Communicator
      ///
      typedef _Messages messages_type;
      typedef typename messages_type::open_session_response_traits_type::arg_type open_session_response_arg_type;
      typedef typename messages_type::close_session_response_traits_type::arg_type close_session_response_arg_type;
      typedef typename messages_type::client_response_traits_type::arg_type client_response_arg_type;
      // These make tcp_server a valid client session communicator
      typedef raft_client_type * endpoint_type;

      void send_open_session_response(open_session_response_arg_type && resp, endpoint_type ep)
      {
        ep->send_open_session_response(std::move(resp));
      }
      void send_close_session_response(close_session_response_arg_type && resp, endpoint_type ep)
      {
        ep->send_close_session_response(std::move(resp));
      }
      void send_client_response(client_response_arg_type && resp, endpoint_type ep)
      {
        ep->send_client_response(std::move(resp));
      }

      // For testing
      const state_machine_type & state_machine() const
      {
        return protocol_->state_machine();
      }
    };
  }
}

#endif
