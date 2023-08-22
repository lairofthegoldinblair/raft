#ifndef __RAFT_ASIO_SERVER_HH__
#define __RAFT_ASIO_SERVER_HH__

#include <set>

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

namespace raft {
  namespace asio {

    struct communicator_peer
    {
      boost::asio::ip::tcp::endpoint peer_endpoint;
      boost::asio::ip::tcp::socket peer_socket;

      communicator_peer(boost::asio::io_service & ios)
	:
	peer_socket(ios)
      {
      }
    };

    // Possible abstractions: A communicator built from : types, serialization, transport
    // vs.
    // A communicator built from : pre-serialized types (e.g. flatbuffers), transport
    //
    // This assumes that we are using type serialization
    //
    // Another nasty issue is that message builders may want to consume data in different
    // orders (e.g flatbuffers is very strict about building data structures bottom up).  I'm
    // trying to avoid baking in such an assumption therefore I am passing all of the data in a
    // somewhat functional manner to avoid control flow issues.  Presumably there are high brow ways of handling
    // this such as expression templates.
    template<typename _Messages, typename _Builders, typename _Serialization>
    class asio_tcp_communicator
    {
    public:
      typedef size_t endpoint;    
      typedef serialization<_Messages, _Serialization> serialization_type;
      typedef _Builders builders_type;
      typedef typename builders_type::request_vote_builder_type request_vote_builder;
      typedef typename builders_type::vote_response_builder_type vote_response_builder;
      typedef typename builders_type::append_entry_builder_type append_entry_builder;
      typedef typename builders_type::append_response_builder_type append_response_builder;
      typedef typename builders_type::append_checkpoint_chunk_builder_type append_checkpoint_chunk_builder;
      typedef typename builders_type::append_checkpoint_chunk_response_builder_type append_checkpoint_chunk_response_builder;
      typedef typename _Messages::checkpoint_header_traits_type checkpoint_header_traits;

      boost::asio::io_service & io_service_;
      boost::asio::ip::tcp::endpoint endpoint_;
      std::map<std::string, std::shared_ptr<communicator_peer> > sockets_;

      asio_tcp_communicator(boost::asio::io_service & ios,
			    const boost::asio::ip::tcp::endpoint & endpoint)
	:
	io_service_(ios),
	endpoint_(endpoint)
      {
      }

      template<typename _T>
      void send(endpoint ep, const std::string & addr, _T && msg)
      {
	  uint8_t * mem = new uint8_t [128*1024];
	  auto buf = serialization_type::serialize(boost::asio::buffer(mem, 128*1024), std::move(msg));
          for(const auto & b : buf.first) {
            BOOST_LOG_TRIVIAL(trace) << "asio_tcp_commmunicator at " << endpoint_ << " sending to Server(" << ep << ") at address " << addr << " : " << raft::slice(reinterpret_cast<const uint8_t *>(b.data()), b.size());
          }
	  send_buffer(ep, addr, buf.first, std::move(buf.second));
      }

      void vote_request(endpoint ep, const std::string & address,
			uint64_t recipient_id,
			uint64_t term_number,
			uint64_t candidate_id,
			uint64_t last_log_index,
			uint64_t last_log_term)
      {
	auto msg = request_vote_builder().recipient_id(recipient_id).term_number(term_number).candidate_id(candidate_id).last_log_index(last_log_index).last_log_term(last_log_term).finish();
	send(ep, address, std::move(msg));	
      }

      template<typename EntryProvider>
      void append_entry(endpoint ep, const std::string& address,
			uint64_t recipient_id,
			uint64_t term_number,
			uint64_t leader_id,
			uint64_t previous_log_index,
			uint64_t previous_log_term,
			uint64_t leader_commit_index,
			uint64_t num_entries,
			EntryProvider entries)
      {
	append_entry_builder bld;
	bld.recipient_id(recipient_id).term_number(term_number).leader_id(leader_id).previous_log_index(previous_log_index).previous_log_term(previous_log_term).leader_commit_index(leader_commit_index);
	for(uint64_t i=0; i<num_entries; ++i) {
	  bld.entry(entries(i));
	}
	auto msg = bld.finish();
	send(ep, address, std::move(msg));	
      }
	
      void append_entry_response(endpoint ep, const std::string& address,
				 uint64_t recipient_id,
				 uint64_t term_number,
				 uint64_t request_term_number,
				 uint64_t begin_index,
				 uint64_t last_index,
				 bool success)
      {
	auto msg = append_response_builder().recipient_id(recipient_id).term_number(term_number).request_term_number(request_term_number).begin_index(begin_index).last_index(last_index).success(success).finish();
	send(ep, address, std::move(msg));	
      }

      void vote_response(endpoint ep, const std::string& address,
			 uint64_t peer_id,
			 uint64_t term_number,
			 uint64_t request_term_number,
			 bool granted)
      {
	auto msg = vote_response_builder().peer_id(peer_id).term_number(term_number).request_term_number(request_term_number).granted(granted).finish();
	send(ep, address, std::move(msg));	
      }

      void append_checkpoint_chunk(endpoint ep, const std::string& address,
				   uint64_t recipient_id,
				   uint64_t term_number,
				   uint64_t leader_id,
				   const typename _Messages::checkpoint_header_type & last_checkpoint_header,
				   uint64_t checkpoint_begin,
				   uint64_t checkpoint_end,
				   bool checkpoint_done,
				   raft::slice && data)
      {
	append_checkpoint_chunk_builder bld;
	bld.recipient_id(recipient_id).term_number(term_number).leader_id(leader_id).checkpoint_begin(checkpoint_begin).checkpoint_end(checkpoint_end).checkpoint_done(checkpoint_done).data(std::move(data));
	{
	  auto chb = bld.last_checkpoint_header();
	  chb.last_log_entry_index(checkpoint_header_traits::last_log_entry_index(&last_checkpoint_header));
	  chb.last_log_entry_term(checkpoint_header_traits::last_log_entry_index(&last_checkpoint_header));
	  chb.index(checkpoint_header_traits::index(&last_checkpoint_header));
	  chb.configuration(checkpoint_header_traits::configuration(&last_checkpoint_header));
	}
	auto msg = bld.finish();
	send(ep, address, std::move(msg));	
      }		       
  
      void append_checkpoint_chunk_response(endpoint ep, const std::string& address,
					    uint64_t recipient_id,
					    uint64_t term_number,
					    uint64_t request_term_number,
					    uint64_t bytes_stored)
      {
	auto msg = append_checkpoint_chunk_response_builder().recipient_id(recipient_id).term_number(term_number).request_term_number(request_term_number).bytes_stored(bytes_stored).finish();
	send(ep, address, std::move(msg));	
      }

      template<typename ConstBufferSequence>
      void send_buffer(endpoint ep, const std::string & addr, ConstBufferSequence buf, raft::util::call_on_delete && deleter)
      {
	BOOST_LOG_TRIVIAL(trace) << "Entering asio_tcp_communicator::send on " << endpoint_;
	// TODO: Make sure at most one send outstanding on each socket?
	auto sit = sockets_.find(addr);
	if (sit == sockets_.end()) {
	  // Only handling v4 addresses
	  auto pos = addr.find_last_of(':');
	  auto v4address = boost::asio::ip::address_v4::from_string(addr.substr(0, pos));
	  auto port = boost::lexical_cast<unsigned short>(addr.substr(pos+1));
	  auto ins_it = sockets_.emplace(addr, std::shared_ptr<communicator_peer>(new communicator_peer(io_service_)));
	  const std::string & addr_copy(ins_it.first->first);
	  ins_it.first->second->peer_endpoint.address(v4address);
	  ins_it.first->second->peer_endpoint.port(port);	  
	  ins_it.first->second->peer_socket.async_connect(sockets_[addr]->peer_endpoint,
							  [this, buf, addr_copy, deleter = std::move(deleter)](boost::system::error_code ec) mutable {
							    if (ec) {
							      BOOST_LOG_TRIVIAL(warning) << "asio_tcp_communicator " << endpoint_ << " failed to connect to " << addr_copy <<
								": " << ec;
							      return;
							    }
							    BOOST_LOG_TRIVIAL(info) << "asio_tcp_communicator " << endpoint_ << " connected to " << addr_copy;
							    BOOST_LOG_TRIVIAL(trace) << "asio_tcp_communicator " << endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
							      " bytes for operation " << reinterpret_cast<const rpc_header *>(buf[0].data())->operation <<
							      " to " << sockets_[addr_copy]->peer_endpoint;
							    boost::asio::async_write(this->sockets_[addr_copy]->peer_socket,
										     buf,
										     [buf, deleter = std::move(deleter)](boost::system::error_code ec, std::size_t bytes_transferred) {
										     });
						    });
	  
	} else {
	  BOOST_LOG_TRIVIAL(trace) << "asio_tcp_communicator " << endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
	      " bytes for operation " << reinterpret_cast<const rpc_header *>(buf[0].data())->operation <<
	      " to " << sit->second->peer_endpoint;
	  boost::asio::async_write(sit->second->peer_socket,
				   buf,
				   [buf, deleter = std::move(deleter)](boost::system::error_code ec, std::size_t bytes_transferred) {
				   });
	}
	BOOST_LOG_TRIVIAL(trace) << "Exiting asio_tcp_communicator::send on " << endpoint_;
      }      
    };

    template<typename _Builders, typename _Serialization>
    struct asio_tcp_communicator_metafunction
    {
      template <typename _Messages>
      struct apply
      {
	typedef asio_tcp_communicator<_Messages, _Builders, _Serialization> type;
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
      raft::util::call_on_delete deleter_;
      dispatcher_type dispatcher_;

      
      void dispatch(boost::asio::const_buffer buf)
      {
	uint16_t op = header_.operation;
	dispatcher_(op, buf, std::move(deleter_));
	if (!deleter_) {
	  buffer_ = new uint8_t [1024*128];
	  deleter_ = [ptr = buffer_](){ delete [] ptr; };
	}
      }

      void handle_body_read(boost::system::error_code ec, std::size_t bytes_transferred)
      {
	if (ec) {
	  // TODO: handle error
	  return;
	}
        
	BOOST_LOG_TRIVIAL(trace) << "raft_receiver at " << endpoint_ << " handle_body_read : " << raft::slice(&buffer_[0], bytes_transferred);
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
	  // TODO: handle error
	  return;
	}
	BOOST_LOG_TRIVIAL(trace) << "raft_receiver at " << endpoint_ << " handle_header_read : "  << raft::slice(reinterpret_cast<const uint8_t *>(&header_), bytes_transferred);
	BOOST_ASSERT(bytes_transferred == sizeof(rpc_header));
	BOOST_ASSERT(header_.magic == rpc_header::MAGIC());
	BOOST_ASSERT(header_.payload_length > 0);
	boost::asio::async_read(this->socket_,
				boost::asio::buffer(&buffer_[0], header_.payload_length),
				std::bind(&raft_receiver::handle_body_read, this, std::placeholders::_1, std::placeholders::_2));
      }
      
    public:
      template<typename _Callback>
      raft_receiver(boost::asio::ip::tcp::socket && s, boost::asio::ip::tcp::endpoint && e, _Callback && dispatcher)
	:
	socket_(std::move(s)),
	endpoint_(std::move(e)),
	buffer_(new uint8_t [1024*128]),
	deleter_([ptr = buffer_](){ delete [] ptr; }),
	dispatcher_(std::move(dispatcher))
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
    	case 4:
    	  {
    	    protocol_.on_client_request(*client_, serialization_type::deserialize_client_request(buf, std::move(deleter)));
    	    break;
    	  }
    	case 6:
    	  {
    	    protocol_.on_set_configuration(*client_, serialization_type::deserialize_set_configuration_request(buf, std::move(deleter)));
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
        case serialization_type::LINEARIZABLE_COMMAND:
          {
    	    session_manager_.on_linearizable_command(client_, serialization_type::deserialize_linearizable_command(buf, std::move(deleter)), std::chrono::steady_clock::now());
            break;
          }
    	default:
    	  BOOST_LOG_TRIVIAL(warning) << "client_dispatcher received unsupported operation " << op;
    	  break;
    	}
      }
    };

    // Ugh since client depends on the protocol type and protocol depends on the client type
    // I have to break the cycle.   There are some pure template solutions out there but they
    // seem to rely on partial specializaiton which entails copying a ton of code so I am using
    // inheritence here.   TODO: think about this some more
    template<typename _Messages>
    class raft_client_base
    {
    public:
      typedef typename _Messages::client_result_type client_result_type;
      virtual void on_client_response(client_result_type result,
				      uint64_t index,
				      std::size_t leader_id) = 0;
      virtual void on_configuration_response(client_result_type result) = 0;
      virtual void on_configuration_response(client_result_type result, const std::vector<std::pair<uint64_t, std::string>> & bad_servers) = 0;
    };
    
    struct raft_client_metafunction
    {
      template <typename _Messages>
      struct apply
      {
	typedef raft_client_base<_Messages> type;
      };
    };

    // This is both a client communicator and receiver
    template<typename _Messages, typename _Builders, typename _Serialization, typename _Protocol, typename _ClientSessionManager>
    class raft_client : public raft_receiver<client_dispatcher<_Messages, _Builders, _Serialization, _Protocol, raft_client<_Messages, _Builders, _Serialization, _Protocol, _ClientSessionManager>, _ClientSessionManager>>, public raft_client_base<_Messages>
    {
    public:
      typedef client_dispatcher<_Messages, _Builders, _Serialization, _Protocol, raft_client<_Messages, _Builders, _Serialization, _Protocol, _ClientSessionManager>, _ClientSessionManager> dispatcher_type;
      typedef typename dispatcher_type::messages_type::client_result_type client_result_type;
      typedef typename dispatcher_type::serialization_type serialization_type;
      typedef typename dispatcher_type::builders_type::client_response_builder_type client_response_builder;
      typedef typename dispatcher_type::builders_type::set_configuration_response_builder_type set_configuration_response_builder;
      typedef typename dispatcher_type::messages_type::open_session_response_traits_type::arg_type open_session_response_arg_type;
      typedef typename dispatcher_type::messages_type::close_session_response_traits_type::arg_type close_session_response_arg_type;
      typedef typename dispatcher_type::messages_type::client_response_traits_type::arg_type client_response_arg_type;

      raft_client(boost::asio::ip::tcp::socket && s, boost::asio::ip::tcp::endpoint && e, dispatcher_type && dispatcher)
	:
	raft_receiver<dispatcher_type>(std::move(s), std::move(e), std::move(dispatcher))
      {
	// This is grotesque; set the dispatcher to know that this is the client
	this->dispatcher_.set_client(this);
      }

      template<typename _T>
      void send(_T && msg)
      {
	  uint8_t * mem = new uint8_t [128*1024];
	  auto buf = serialization_type::serialize(boost::asio::buffer(mem, 128*1024), std::move(msg));
	  BOOST_LOG_TRIVIAL(trace) << "raft_client sending " << boost::asio::buffer_size(buf.first) <<
	      " bytes for response to operation " << reinterpret_cast<const rpc_header *>(buf.first[0].data())->operation <<
	      " to " << this->endpoint_;
	  boost::asio::async_write(this->socket_,
				   buf.first,
				   [deleter = std::move(buf.second)](boost::system::error_code ec, std::size_t bytes_transferred) {
				   });
      }

      void on_client_response(client_result_type result,
			      uint64_t index,
			      std::size_t leader_id) override
      {
	auto resp = client_response_builder().result(result).index(index).leader_id(leader_id).finish();
	send(std::move(resp));
      }

      void on_configuration_response(client_result_type result) override
      {
	auto resp = set_configuration_response_builder().result(result).finish();
	send(std::move(resp));
      }
    
      void on_configuration_response(client_result_type result, const std::vector<std::pair<uint64_t, std::string>> & bad_servers) override
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
    };

    template<typename _Messages, typename _Builders, typename _Serialization>
    struct raft_protocol_type_builder
    {
      typedef raft::protocol<asio_tcp_communicator_metafunction<_Builders, _Serialization>, raft_client_metafunction, _Messages> type;
    };

    template<typename _Messages, typename _Builders, typename _Serialization>
    class peer_dispatcher
    {
    public:
      typedef serialization<_Messages, _Serialization> serialization_type;
      typedef typename raft_protocol_type_builder<_Messages, _Builders, _Serialization>::type raft_protocol_type;
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
	case 0:
	  {
	    protocol_.on_request_vote(serialization_type::deserialize_request_vote(buf, std::move(deleter)));
	    break;
	  }
	case 1:
	  {
	    protocol_.on_vote_response(serialization_type::deserialize_vote_response(buf, std::move(deleter)));
	    break;
	  }
	case 2:
	  {
	    protocol_.on_append_entry(serialization_type::deserialize_append_entry(buf, std::move(deleter)));
	    break;
	  }
	case 3:
	  {
	    protocol_.on_append_response(serialization_type::deserialize_append_entry_response(buf, std::move(deleter)));
	    break;
	  }
	case 9:
	  {
	    protocol_.on_append_checkpoint_chunk(serialization_type::deserialize_append_checkpoint_chunk(buf, std::move(deleter)));
	    break;
	  }
	case 10:
	  {
	    protocol_.on_append_checkpoint_chunk_response(serialization_type::deserialize_append_checkpoint_chunk_response(buf, std::move(deleter)));
	    break;
	  }
	default:
	  BOOST_LOG_TRIVIAL(warning) << "peer_dispatcher received unsupported operation " << op;
	  break;
	}
      }
    };    

    template<typename _Messages, typename _Builders, typename _Serialization, typename _StateMachine>
    class tcp_server : public raft::log_header_write
    {
    public:
      typedef typename _Messages::log_entry_type log_entry_type;
      typedef typename _Messages::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
      typedef serialization<_Messages, _Serialization> serialization_type;
      typedef peer_dispatcher<_Messages, _Builders, _Serialization> peer_dispatcher_type;
      typedef raft_receiver<peer_dispatcher_type> peer_receiver_type;
      typedef typename raft_protocol_type_builder<_Messages, _Builders, _Serialization>::type raft_protocol_type;
      typedef _StateMachine state_machine_type;
      typedef ::raft::state_machine::client_session_manager<_Messages, _Builders, _Serialization, raft_protocol_type, tcp_server<_Messages, _Builders, _Serialization, state_machine_type>, state_machine_type> session_manager_type;
      typedef raft_client<_Messages, _Builders, _Serialization, raft_protocol_type, session_manager_type> raft_client_type;
      typedef typename raft_client_type::dispatcher_type client_dispatcher_type;
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

      // Log Disk Infrastructure
      int log_header_fd_;
      std::unique_ptr<basic_file_object<disk_io_service> > log_header_file_;
      log_header header_;
      int log_fd_;
      std::unique_ptr<raft::asio::writable_file> log_file_;
      std::unique_ptr<raft::leveldb::log_writer<raft::asio::writable_file>> log_writer_;
      // TEMPORARY HACK! Pretend we are syncing to log
      uint64_t last_log_index_synced_;
      uint64_t last_log_index_written_;
      bool log_sync_in_progress_;

      typename raft_protocol_type::communicator_type comm_;
      typename raft_protocol_type::log_type l_;
      typename raft_protocol_type::checkpoint_data_store_type store_;
      typename raft_protocol_type::configuration_manager_type config_manager_;
      std::shared_ptr<raft_protocol_type> protocol_;
      std::shared_ptr<session_manager_type> session_manager_;
      state_machine_type state_machine_;
      int32_t protocol_timer_downsample_ = 1;
      

      void handle_timer(boost::system::error_code ec)
      {
	if (!!protocol_) {
          if (--protocol_timer_downsample_ <= 0) {
            // Want the logs to run much faster than the protocol since
            // I haven't implemented append entry pacing.
            protocol_->on_timer();
            protocol_timer_downsample_ = 10;
          }

	  boost::array<uint8_t, 128*1024> tmp_buf;
	  if (!l_.empty()) {
	    // TODO: a checkpoint could cause a problem here because we could truncate a log entry that
	    // didn't get persisted?  Is this logic sufficient?
	    if (last_log_index_written_ < l_.start_index()) {
	      last_log_index_written_ = l_.start_index();
	    }
            if (last_log_index_written_ < l_.last_index()) {
              BOOST_LOG_TRIVIAL(info) << "[raft::asio::tcp_server::handle_timer] Server(" << config_manager_.configuration().my_cluster_id()
                                      << ") writing log records [" << last_log_index_written_ << ", " << l_.last_index() << ")";
            }
	    for(;last_log_index_written_ < l_.last_index(); ++last_log_index_written_) {
	      auto serialize_bufs = serialization_type::serialize(boost::asio::buffer(tmp_buf), l_.entry(last_log_index_written_));
	      for(auto it = boost::asio::buffer_sequence_begin(serialize_bufs.first), e = boost::asio::buffer_sequence_end(serialize_bufs.first);
		  it != e; ++it) {
		log_writer_->append_record(boost::asio::buffer_cast<const uint8_t *>(*it),
					   boost::asio::buffer_size(*it));
	      }
	    }

	    // Sync to disk and let the protocol know when it's done
	    if (!log_sync_in_progress_ && last_log_index_synced_ < last_log_index_written_) {
              BOOST_LOG_TRIVIAL(info) << "[raft::asio::tcp_server::handle_timer] Server(" << config_manager_.configuration().my_cluster_id()
                                      << ") syncing log to disk";
	      log_sync_in_progress_ = true;
	      uint64_t idx = last_log_index_written_;
	      log_file_->flush_and_sync([this,idx](boost::system::error_code) {
                                          BOOST_LOG_TRIVIAL(info) << "[raft::asio::tcp_server::handle_timer] Server(" << config_manager_.configuration().my_cluster_id()
                                                                  << ") log sync completed up to index " << idx;
                                          this->protocol_->on_log_sync(idx);
                                          this->last_log_index_synced_ = idx;
                                          this->log_sync_in_progress_ = false;
                                        });
	    }
	  }	  
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
	
	BOOST_LOG_TRIVIAL(info) << "Server(" << config_manager_.configuration().my_cluster_id() << ") raft::asio::tcp_server got connection on " << peer_listen_endpoint_;
	receivers_.push_back(std::make_shared<peer_receiver_type>(std::move(peer_listen_socket_), std::move(peer_listen_endpoint_), peer_dispatcher_type(*protocol_.get())));
	BOOST_LOG_TRIVIAL(info) << "Server(" << config_manager_.configuration().my_cluster_id() << ") raft::asio::tcp_server listening on " << peer_listener_.local_endpoint();
	peer_listener_.async_accept(peer_listen_socket_, peer_listen_endpoint_,
			       std::bind(&tcp_server::peer_handle_accept, this, std::placeholders::_1));
      }

      void client_handle_accept(boost::system::error_code ec)
      {
	if (ec) {
	  BOOST_LOG_TRIVIAL(warning) << "raft::asio::tcp_server failed accepting connection: " << ec;
	  return;
	}
	
	BOOST_LOG_TRIVIAL(info) << "Server(" << config_manager_.configuration().my_cluster_id() << ") raft::asio::tcp_server got connection on " << client_listen_endpoint_;
	clients_.insert(std::make_unique<raft_client_type>(std::move(client_listen_socket_), std::move(client_listen_endpoint_), client_dispatcher_type(*protocol_.get(), *session_manager_.get())));
	BOOST_LOG_TRIVIAL(info) << "Server(" << config_manager_.configuration().my_cluster_id() << ") raft::asio::tcp_server listening on " << client_listener_.local_endpoint();
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
	config_manager_(server_id),
	last_log_index_synced_(0),
	last_log_index_written_(0),
	log_sync_in_progress_(false)
      {
	// Open the log file
	std::string log_header_file(log_directory);
	log_header_file += std::string("/log_header.bin");
	log_header_fd_ = ::open(log_header_file.c_str(), O_CREAT | O_WRONLY);
	if (-1 == log_header_fd_) {
	  BOOST_LOG_TRIVIAL(error) << "Server(" << config_manager_.configuration().my_cluster_id() << ") Failed to open log header file " << log_header_file << ": " << ::strerror(errno);
	}
	log_header_file_.reset(new basic_file_object<disk_io_service>(io_service_, log_header_fd_));
	std::string log_file(log_directory);
	log_file += std::string("/log.bin");
	log_fd_ = ::open(log_file.c_str(), O_CREAT | O_WRONLY | O_APPEND);
	if (-1 == log_fd_) {
	  BOOST_LOG_TRIVIAL(error) << "Server(" << config_manager_.configuration().my_cluster_id() << ") Failed to open log header file " << log_file << ": " << ::strerror(errno);
	}
	
	log_file_.reset(new raft::asio::writable_file(io_service_, log_fd_));
	log_writer_.reset(new raft::leveldb::log_writer<raft::asio::writable_file>(*log_file_.get()));

	// Set sync callback in the log (TODO: Fix this is dumb way of doing things)
	l_.set_log_header_writer(this);

	l_.append(std::move(config));
	l_.update_header(0, raft_protocol_type::INVALID_PEER_ID);
	protocol_.reset(new raft_protocol_type(comm_, l_, store_, config_manager_));

        // Create the session manager and connect it up to the protocol box
        session_manager_.reset(new session_manager_type(*protocol_.get(), *this, state_machine_));
        protocol_->set_state_machine([this](log_entry_const_arg_type le, uint64_t idx, size_t leader_id) { this->session_manager_->apply(le, idx, leader_id); });
        
	// Setup periodic timer callbacks from ASIO
	timer_.expires_from_now(boost::posix_time::milliseconds(server_id==0 ? 1 : server_id*100));
	timer_.async_wait(std::bind(&tcp_server::handle_timer, this, std::placeholders::_1));

	// Setup listener for interprocess communication
	if (config_manager_.configuration().includes_self()) {
	  // TODO: Handle v6
	  BOOST_LOG_TRIVIAL(info) << "Server(" << config_manager_.configuration().my_cluster_id() << ") raft::asio::tcp_server listening for peers on " << peer_listener_.local_endpoint();
	  peer_listener_.async_accept(peer_listen_socket_, peer_listen_endpoint_,
				  std::bind(&tcp_server::peer_handle_accept, this, std::placeholders::_1));
	  // TODO: Handle v6
	  BOOST_LOG_TRIVIAL(info) << "Server(" << config_manager_.configuration().my_cluster_id() << ") raft::asio::tcp_server listening for clients on " << client_listener_.local_endpoint();
	  client_listener_.async_accept(client_listen_socket_, client_listen_endpoint_,
				  std::bind(&tcp_server::client_handle_accept, this, std::placeholders::_1));
	}
      }

      ~tcp_server()
      {
	// Use error code to prevent exception
	boost::system::error_code ec;
	peer_listener_.close(ec);

	::close(log_header_fd_);
	::close(log_fd_);
      }

      void async_write_log_header(uint64_t current_term, uint64_t voted_for) override
      {
	BOOST_LOG_TRIVIAL(info) << "Server(" << config_manager_.configuration().my_cluster_id() <<
	  ") raft::asio::tcp_server request to sync log header current_term=" << current_term <<
	  " voted_for=" << voted_for;
	header_.current_term = current_term;
	header_.voted_for = voted_for;
	log_header_file_->async_write(boost::asio::buffer(&header_, sizeof(header_)),
				      [this](boost::system::error_code ec, std::size_t bytes_transferred) mutable {
					BOOST_LOG_TRIVIAL(info) << "Server(" << this->config_manager_.configuration().my_cluster_id() <<
					  ") raft::asio::tcp_server completed sync log header sync";
					this->protocol_->on_log_header_sync();
				      });
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
        return state_machine_;
      }
    };
  }
}

#endif
