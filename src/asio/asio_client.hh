#ifndef __RAFT_ASIO_CLIENT_HH__
#define __RAFT_ASIO_CLIENT_HH__

#include <chrono>
#include <thread>

#include "boost/asio.hpp"
#include "boost/log/trivial.hpp"
#include "boost/timer/timer.hpp"

#include "asio/asio_messages.hh"

namespace raft {
  namespace asio {
    template<typename _Client>
    class synchronous_client_session
    {
    public:
      typedef _Client client_type;
      typedef typename client_type::messages_type messages_type;
      typedef typename client_type::client_result_type client_result_type;
    private:
      client_type * client_;
      uint64_t session_id_;
      uint64_t first_unacknowledged_sequence_number_;
      uint64_t sequence_number_;
    public:
      synchronous_client_session(client_type & client, uint64_t session_id);
      ~synchronous_client_session();

      synchronous_client_session(synchronous_client_session && other)
        :
        client_(other.client_),
        session_id_(other.session_id),
        first_unacknowledged_sequence_number_(other.first_unacknowledged_sequence_number_),
        sequence_number_(other.sequence_number_)
      {
        other.client_ = nullptr;
      }
      
      synchronous_client_session & operator=(synchronous_client_session && other)
      {
        close();
        client_ = other.client_;
        session_id_ = other.session_id;
        first_unacknowledged_sequence_number_ = other.first_unacknowledged_sequence_number_;
        sequence_number_ = other.sequence_number_;
        other.client_ = nullptr;
      }
      
      synchronous_client_session(const synchronous_client_session & ) = delete;
      synchronous_client_session & operator=(const synchronous_client_session & ) = delete;

      void close()
      {
        if (client_ != nullptr) {
          client_->close_session(session_id_);
        }
      }
      
      void send_command(raft::slice && cmd);
    };

    template<typename _Client>
    synchronous_client_session<_Client>::synchronous_client_session(_Client & client,
                                                                    uint64_t session_id)
      :
      client_(&client),
      session_id_(session_id),
      first_unacknowledged_sequence_number_(0),
      sequence_number_(0)
    {
    }

    template<typename _Client>
    synchronous_client_session<_Client>::~synchronous_client_session()
    {
      close();
    }

    template<typename _Client>
    void synchronous_client_session<_Client>::send_command(raft::slice && cmd)
    {
      auto buf = reinterpret_cast<const uint8_t *>(cmd.data());
      auto bufsz = cmd.size();
      auto ret = client_->send_command(session_id_, first_unacknowledged_sequence_number_, sequence_number_, raft::slice(buf, bufsz));
      if (ret.first == messages_type::client_result_success()) {
        first_unacknowledged_sequence_number_++;
        sequence_number_++;
        return;
      } else {
        throw std::runtime_error("send_command failed");
      }
    }

    template<typename _Messages, typename _Builders, typename _Serialization>
    class synchronous_client
    {
    public:
      typedef _Messages messages_type;
      typedef _Builders builders_type;
      typedef typename builders_type::log_entry_builder_type log_entry_builder;
      typedef typename builders_type::open_session_request_builder_type open_session_request_builder;
      typedef typename messages_type::open_session_response_traits_type open_session_response_traits;
      typedef typename builders_type::close_session_request_builder_type close_session_request_builder;
      typedef typename messages_type::close_session_response_traits_type close_session_response_traits;
      typedef typename builders_type::linearizable_command_builder_type linearizable_command_builder;
      typedef typename messages_type::client_response_traits_type client_response_traits;
      typedef typename messages_type::client_result_type client_result_type;
      typedef raft::asio::serialization<messages_type, _Serialization> serialization_type;
      typedef synchronous_client_session<synchronous_client<_Messages, _Builders, _Serialization>> client_session_type;
    private:
      std::vector<boost::asio::ip::tcp::endpoint> servers_;
      std::size_t leader_id_;
      boost::asio::io_service ios_;
      std::unique_ptr<boost::asio::ip::tcp::socket> client_socket_;
      bool leader_confirmed_ = false;

      void try_new_leader()
      {
        leader_confirmed_ = false;
        if (client_socket_) {
          BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::try_new_leader] Server(" << leader_id_ << ") is not LEADER";
          client_socket_.reset();
          leader_id_ = (leader_id_ + 1) % servers_.size();
        }
        for(std::size_t i=0; i<servers_.size(); ++i) {
          std::array<boost::asio::ip::tcp::endpoint, 1> eps = { servers_[leader_id_] };
          client_socket_.reset(new boost::asio::ip::tcp::socket(ios_));
          boost::system::error_code ec;
          boost::asio::connect(*client_socket_, eps, ec);
          if (ec) {
            BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::try_new_leader] Server(" << leader_id_ << ") failed trying to connect";
            client_socket_.reset();
            leader_id_ = (leader_id_ + 1) % servers_.size();
          } else {
            BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::try_new_leader] Server(" << leader_id_ << ") is new candidate as LEADER";
            return;
          }
        }
        throw std::runtime_error("Cluster is down");
      }

      uint64_t internal_open_session()
      {
        // Try each server if there is a failure
        // TODO: Should add a status to the response which tells us if this is NOT_LEADER and provides a hint who is
        for(std::size_t i=0; i<servers_.size(); ++i) {
          if (!leader_confirmed_) {
            BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::internal_open_session] Server(" << leader_id_ << ") is non-confirmed leader is being sent open_session";
          }
          open_session_request_builder bld;
          auto msg = bld.finish();
          auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
          boost::system::error_code ec;
          auto bytes_transferred = boost::asio::write(*client_socket_, result.first, ec);
          if (ec) {
            continue;
          }
          BOOST_ASSERT(bytes_transferred == boost::asio::buffer_size(result.first));
          raft::asio::rpc_header header;
          bytes_transferred = boost::asio::read(*client_socket_, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)), ec);
          if (ec) {
            try_new_leader();
            continue;
          }
          BOOST_ASSERT(bytes_transferred == sizeof(raft::asio::rpc_header));
          BOOST_ASSERT(header.magic == raft::asio::rpc_header::MAGIC());
          BOOST_ASSERT(header.payload_length > 0);
          BOOST_ASSERT(header.operation == serialization_type::OPEN_SESSION_RESPONSE);
          uint8_t *  buf = new uint8_t [header.payload_length];
          raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
          bytes_transferred = boost::asio::read(*client_socket_, boost::asio::buffer(&buf[0], header.payload_length), ec);
          if (ec) {
            try_new_leader();
            continue;
          }
          BOOST_ASSERT(bytes_transferred == header.payload_length);
          boost::asio::const_buffer asio_buf(buf, header.payload_length);
          auto resp = serialization_type::deserialize_open_session_response(asio_buf, std::move(deleter));
          auto session_id = open_session_response_traits::session_id(resp);
          if (session_id > 0) {
            if (!leader_confirmed_) {
              BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::internal_open_session] Server(" << leader_id_ << ") confirmed as leader with successful open_session";
              leader_confirmed_ = true;
            }
            return session_id;
          } else {
            // 0 == session_id means NOT_LEADER
            try_new_leader();
          }
        }
        throw std::runtime_error("Could not find cluster leader");
      }

    public:
      synchronous_client(const std::vector<boost::asio::ip::tcp::endpoint> && servers)
        :
        servers_(std::move(servers)),
        leader_id_(0)
      {
        try_new_leader();
      }

      client_session_type open_session()
      {
        return client_session_type(*this, internal_open_session());
      }

      void close_session(uint64_t session_id)
      {
        // Only try on the current leader.  It if doesn't succeed the session will time out.
        if (!client_socket_) {
          return;
        }
        if (!leader_confirmed_) {
          BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::close_session] Server(" << leader_id_ << ") is non-confirmed leader is being sent close_session";
        }
        close_session_request_builder bld;
        auto msg = bld.session_id(session_id).finish();
        auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
        boost::system::error_code ec;
        auto bytes_transferred = boost::asio::write(*client_socket_, result.first, ec);
        if (ec) {
          return;
        }
        BOOST_ASSERT(bytes_transferred == boost::asio::buffer_size(result.first));
        raft::asio::rpc_header header;
        bytes_transferred = boost::asio::read(*client_socket_, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)), ec);
        if (ec) {
          return;
        }
        BOOST_ASSERT(bytes_transferred == sizeof(raft::asio::rpc_header));
        BOOST_ASSERT(header.magic == raft::asio::rpc_header::MAGIC());
        BOOST_ASSERT(header.payload_length > 0);
        BOOST_ASSERT(header.operation == serialization_type::CLOSE_SESSION_RESPONSE);
        uint8_t *  buf = new uint8_t [header.payload_length];
        raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
        bytes_transferred = boost::asio::read(*client_socket_, boost::asio::buffer(&buf[0], header.payload_length), ec);
        if (ec) {
          return;
        }
        BOOST_ASSERT(bytes_transferred == header.payload_length);
        boost::asio::const_buffer asio_buf(buf, header.payload_length);
        auto resp = serialization_type::deserialize_close_session_response(asio_buf, std::move(deleter));
        // Don't worry about failure since timeouts will take care of this.
      }

      std::pair<client_result_type, uint64_t> send_command(uint64_t session_id, uint64_t first_unacked_seq_no, uint64_t seq_no, raft::slice && cmd)
      {
        if (!client_socket_) {
          BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::send_command] failed could not establish connection to cluster";
          return std::pair<client_result_type, uint64_t>(messages_type::client_result_fail(), std::numeric_limits<uint64_t>::max());
        }
        for(std::size_t j=0; j<100; ++j) {
          for(std::size_t i=0; i<servers_.size(); ++i) {
            if (!leader_confirmed_) {
              BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::send_command] Server(" << leader_id_ << ") is non-confirmed leader is being sent send_command";
            }
            {
              boost::timer::cpu_timer timer;
              timer.start();
              linearizable_command_builder bld;
              auto msg = bld.session_id(session_id).first_unacknowledged_sequence_number(first_unacked_seq_no).sequence_number(seq_no).command(std::move(cmd)).finish();
              auto result = serialization_type::serialize(boost::asio::buffer(new uint8_t [1024], 1024), std::move(msg));
              boost::system::error_code ec;
              auto bytes_transferred = boost::asio::write(*client_socket_, result.first, ec);
              timer.stop();
              if (ec) {
                try_new_leader();
                continue;
              }
              BOOST_ASSERT(bytes_transferred == boost::asio::buffer_size(result.first));
              BOOST_LOG_TRIVIAL(trace) << "[raft::asio::synchronous_client::send_command] write_request time: " << timer.format();
            }
            {
              boost::timer::cpu_timer timer;
              raft::asio::rpc_header header;
              boost::system::error_code ec;
              timer.start();
              auto bytes_transferred = boost::asio::read(*client_socket_, boost::asio::buffer(&header, sizeof(raft::asio::rpc_header)), ec);
              timer.stop();
              if (ec) {
                try_new_leader();
                continue;
              }
              BOOST_LOG_TRIVIAL(trace) << "[raft::asio::synchronous_client::send_command] read_response_header time: " << timer.format();
              BOOST_ASSERT(bytes_transferred == sizeof(raft::asio::rpc_header));
              BOOST_ASSERT(header.magic == raft::asio::rpc_header::MAGIC());
              BOOST_ASSERT(header.payload_length > 0);
              BOOST_ASSERT(header.operation == serialization_type::CLIENT_RESPONSE);
              uint8_t *  buf = new uint8_t [header.payload_length];
              raft::util::call_on_delete deleter([ptr = buf](){ delete [] ptr; });    
              timer.start();
              bytes_transferred = boost::asio::read(*client_socket_, boost::asio::buffer(&buf[0], header.payload_length), ec);
              timer.stop();
              if (ec) {
                try_new_leader();
                continue;
              }
              BOOST_LOG_TRIVIAL(trace) << "[raft::asio::synchronous_client::send_command] read_response_body time: " << timer.format();
              BOOST_ASSERT(bytes_transferred == header.payload_length);
              boost::asio::const_buffer asio_buf(buf, header.payload_length);
              auto resp = serialization_type::deserialize_client_response(asio_buf, std::move(deleter));
              if (messages_type::client_result_success() == client_response_traits::result(resp)) {
                if (!leader_confirmed_) {
                  BOOST_LOG_TRIVIAL(info) << "Server(" << leader_id_ << ") confirmed as leader with successful send_command";
                  leader_confirmed_ = true;
                }
                return std::make_pair(client_response_traits::result(resp), client_response_traits::leader_id(resp));
              } else if (messages_type::client_result_not_leader() == client_response_traits::result(resp)) {
                try_new_leader();
              } else {
                BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::send_command] Server(" << leader_id_ << ") linearizable command failed : " << (int) client_response_traits::result(resp);
                return std::make_pair(client_response_traits::result(resp), client_response_traits::leader_id(resp));
              }
            }
          }
          BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::send_command] failed to find new leader.   Waiting and will try again.";
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        BOOST_LOG_TRIVIAL(info) << "[raft::asio::synchronous_client::send_command] linearizable command failed could not determine leader in cluster.";
        return std::make_pair(messages_type::client_result_fail(), std::numeric_limits<uint64_t>::max());
      }
    };
  }
}

#endif
