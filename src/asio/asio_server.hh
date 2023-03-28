#ifndef __RAFT_ASIO_SERVER_HH__
#define __RAFT_ASIO_SERVER_HH__

#include "boost/asio.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/log/trivial.hpp"

#include "asio/asio_block_device.hh"
#include "asio/asio_messages.hh"
#include "asio/basic_file_object.hh"
#include "asio/disk_io_service.hh"
#include "leveldb_log.hh"
#include "../native/messages.hh"
#include "protocol.hh"

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
    template<typename _Messages>
    class asio_tcp_communicator
    {
    public:
      typedef size_t endpoint;    

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
      void send(endpoint ep, const std::string & addr, const _T & msg)
      {
	  uint8_t * mem = new uint8_t [128*1024];
	  auto buf = serialization::serialize(boost::asio::buffer(mem, 128*1024), msg);
	  send_buffer(ep, addr, buf);
      }

      void vote_request(endpoint ep, const std::string & address,
			uint64_t recipient_id,
			uint64_t term_number,
			uint64_t candidate_id,
			uint64_t last_log_index,
			uint64_t last_log_term)
      {
	typename _Messages::request_vote_type msg;
	msg.recipient_id=recipient_id;
	msg.term_number=term_number;
	msg.candidate_id=candidate_id;
	msg.last_log_index=last_log_index;
	msg.last_log_term=last_log_term;
	send(ep, address, msg);	
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
	typename _Messages::append_entry_type msg;
	msg.set_recipient_id(recipient_id);
	msg.set_term_number(term_number);
	msg.set_leader_id(leader_id);
	msg.set_previous_log_index(previous_log_index);
	msg.set_previous_log_term(previous_log_term);
	msg.set_leader_commit_index(leader_commit_index);
	for(uint64_t i=0; i<num_entries; ++i) {
	  msg.add_entry(entries(i));
	}
	send(ep, address, msg);
      }

      void append_entry_response(endpoint ep, const std::string& address,
				 uint64_t recipient_id,
				 uint64_t term_number,
				 uint64_t request_term_number,
				 uint64_t begin_index,
				 uint64_t last_index,
				 bool success)
      {
	typename _Messages::append_entry_response_type msg;
	msg.recipient_id = recipient_id;
	msg.term_number = term_number;
	msg.request_term_number = request_term_number;
	msg.begin_index = begin_index;
	msg.last_index = last_index;
	msg.success = success;
	send(ep, address, msg);
      }

      void vote_response(endpoint ep, const std::string& address,
			 uint64_t peer_id,
			 uint64_t term_number,
			 uint64_t request_term_number,
			 bool granted)
      {
	typename _Messages::vote_response_type msg;
	msg.peer_id = peer_id;
	msg.term_number = term_number;
	msg.request_term_number = request_term_number;
	msg.granted = granted;
	send(ep, address, msg);
      }

      void append_checkpoint_chunk(endpoint ep, const std::string& address,
				   uint64_t recipient_id,
				   uint64_t term_number,
				   uint64_t leader_id,
				   const raft::native::checkpoint_header & last_checkpoint_header,
				   uint64_t checkpoint_begin,
				   uint64_t checkpoint_end,
				   bool checkpoint_done,
				   raft::slice data)
      {
	typename _Messages::append_checkpoint_chunk_type msg;
	msg.recipient_id=recipient_id;
	msg.term_number=term_number;
	msg.leader_id=leader_id;
	msg.last_checkpoint_header=last_checkpoint_header;
	msg.checkpoint_begin=checkpoint_begin;
	msg.checkpoint_end=checkpoint_end;
	msg.checkpoint_done=checkpoint_done;
	msg.data.assign(raft::slice::buffer_cast<const uint8_t *>(data),
			raft::slice::buffer_cast<const uint8_t *>(data) + raft::slice::buffer_size(data));
	send(ep, address, msg);
      }		       
  
      void append_checkpoint_chunk_response(endpoint ep, const std::string& address,
					    uint64_t recipient_id,
					    uint64_t term_number,
					    uint64_t request_term_number,
					    uint64_t bytes_stored)
      {
	typename _Messages::append_checkpoint_chunk_response_type msg;    
	msg.recipient_id = recipient_id;
	msg.term_number = term_number;
	msg.request_term_number = request_term_number;
	msg.bytes_stored = bytes_stored;
	send(ep, address, msg);
      }
      
      // Don't use with boost::asio::const_buffers_1 since that doesn't have
      // the right move semantics
      template<typename ConstBufferSequence>
      class owned_buffer_sequence
      {
      private:
	ConstBufferSequence seq_;
	owned_buffer_sequence(const owned_buffer_sequence& ) = delete;
	owned_buffer_sequence & operator=(const owned_buffer_sequence &) = delete;
      public:
	owned_buffer_sequence(owned_buffer_sequence&& seq)
	  :
	  seq_(std::move(seq))
	{
	}

	~owned_buffer_sequence()
	{
	  for(auto b : seq_) {
	    delete [] boost::asio::buffer_cast<const uint8_t *>(b);
	  }
	}
	
	owned_buffer_sequence & operator=(owned_buffer_sequence && seq)
	{
	  seq_ = std::move(seq);
	}

	operator const ConstBufferSequence & ()
	{
	  return seq_;
	}
      };
      
      template<typename OwnedConstBufferSequence>
      class write_handler
      {
      private:
	asio_tcp_communicator * comm_;
	OwnedConstBufferSequence buf_;
      public:
	write_handler(asio_tcp_communicator * comm,
		      OwnedConstBufferSequence && buf)
	  :
	  comm_(comm),
	  buf_(std::move(buf))
	{
	}

	write_handler(write_handler && rhs)
	  :
	  comm_(rhs.comm_),
	  buf_(std::move(rhs.buf_))
	{
	}

	write_handler(const write_handler & rhs) = delete;
	const write_handler & operator=(const write_handler & rhs) = delete;

	~write_handler() = default;
	
	void operator()(boost::system::error_code ec, std::size_t bytes_transferred)
	{
	}
      };
      
      template<typename OwnedConstBufferSequence>
      class connect_and_write_handler
      {
      private:
	asio_tcp_communicator * comm_;
	OwnedConstBufferSequence buf_;
	const std::string & addr_copy_;
      public:
	connect_and_write_handler(asio_tcp_communicator * comm,
				  OwnedConstBufferSequence && buf,
				  const std::string & addr_copy)
	  :
	  comm_(comm),
	  buf_(std::move(buf)),
	  addr_copy_(addr_copy)
	{
	}

	connect_and_write_handler(connect_and_write_handler && rhs)
	  :
	  comm_(rhs.comm_),
	  buf_(std::move(rhs.buf_)),
	  addr_copy_(rhs.addr_copy_)
	{
	}

	connect_and_write_handler(const connect_and_write_handler & rhs) = delete;
	const connect_and_write_handler & operator=(const connect_and_write_handler & rhs) = delete;

	~connect_and_write_handler() = default;
	
	void operator ()(boost::system::error_code ec)
	{
	  if (ec) {
	    BOOST_LOG_TRIVIAL(warning) << "asio_tcp_communicator " << this->endpoint_ << " failed to connect to " << addr_copy_ <<
	      ": " << ec;
	    return;
	  }
	  BOOST_LOG_TRIVIAL(info) << "asio_tcp_communicator " << this->endpoint_ << " connected to " << addr_copy_;
	  BOOST_LOG_TRIVIAL(debug) << "asio_tcp_communicator " << this->endpoint_ << " sending " << boost::asio::buffer_size(buf_) <<
	    " bytes for operation " << boost::asio::buffer_cast<const rpc_header *>(buf_)->operation <<
	    " to " << this->sockets_[addr_copy_]->peer_endpoint;
	  boost::asio::async_write(this->sockets_[addr_copy_]->peer_socket,
				   buf_,
				   write_handler<OwnedConstBufferSequence>(this, buf_));
	}
      };
      
      template<typename OwnedConstBufferSequence>
      void send_owned_buffer(endpoint ep, const std::string & addr, OwnedConstBufferSequence && buf)
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
	  ins_it.first->second->peer_socket.async_connect(sockets_[addr]->peer_endpoint, connect_and_write_handler<OwnedConstBufferSequence>(this, buf, addr_copy));	  
	} else {
	  BOOST_LOG_TRIVIAL(debug) << "asio_tcp_communicator " << endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
	    " bytes for operation " << boost::asio::buffer_cast<const rpc_header *>(buf)->operation <<
	    " to " << sit->second->peer_endpoint;
	  boost::asio::async_write(sit->second->peer_socket,
				   buf,
				   write_handler<OwnedConstBufferSequence>(this, buf));
	}
	BOOST_LOG_TRIVIAL(trace) << "Exiting asio_tcp_communicator::send on " << endpoint_;
      }      

      template<typename ConstBufferSequence>
      void send_buffer(endpoint ep, const std::string & addr, ConstBufferSequence buf)
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
							  [this, buf, addr_copy](boost::system::error_code ec) mutable {
							    if (ec) {
							      BOOST_LOG_TRIVIAL(warning) << "asio_tcp_communicator " << endpoint_ << " failed to connect to " << addr_copy <<
								": " << ec;
							      for(auto b : buf) {
								delete [] boost::asio::buffer_cast<const uint8_t *>(b);
							      }
							      return;
							    }
							    BOOST_LOG_TRIVIAL(info) << "asio_tcp_communicator " << endpoint_ << " connected to " << addr_copy;
							    BOOST_LOG_TRIVIAL(debug) << "asio_tcp_communicator " << endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
							      " bytes for operation " << boost::asio::buffer_cast<const rpc_header *>(buf)->operation <<
							      " to " << sockets_[addr_copy]->peer_endpoint;
							    boost::asio::async_write(this->sockets_[addr_copy]->peer_socket,
										     buf,
										     [buf](boost::system::error_code ec, std::size_t bytes_transferred) {
										       for(auto b : buf) {
											 delete [] boost::asio::buffer_cast<const uint8_t *>(b);
										       }
										     });
						    });
	  
	} else {
	  BOOST_LOG_TRIVIAL(debug) << "asio_tcp_communicator " << endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
	    " bytes for operation " << boost::asio::buffer_cast<const rpc_header *>(buf)->operation <<
	    " to " << sit->second->peer_endpoint;
	  boost::asio::async_write(sit->second->peer_socket,
				   buf,
				   [buf](boost::system::error_code ec, std::size_t bytes_transferred) {
				     for(auto b : buf) {
				       delete [] boost::asio::buffer_cast<const uint8_t *>(b);
				     }
				   });
	}
	BOOST_LOG_TRIVIAL(trace) << "Exiting asio_tcp_communicator::send on " << endpoint_;
      }      
    };

    struct asio_tcp_communicator_metafunction
    {
      template <typename _Messages>
      struct apply
      {
	typedef asio_tcp_communicator<_Messages> type;
      };
    };

    struct native_client_metafunction
    {
      template <typename _Messages>
      struct apply
      {
	typedef raft::native::client<_Messages> type;
      };
    };

    typedef raft::protocol<asio_tcp_communicator_metafunction, native_client_metafunction, raft::native::messages> raft_protocol_type;
    
    class raft_receiver
    {
    private:
      boost::asio::ip::tcp::socket socket_;
      boost::asio::ip::tcp::endpoint endpoint_;
      std::array<uint8_t, 128*1024> buffer_;
      raft_protocol_type & protocol_;

      
      void dispatch(const rpc_header & header, boost::asio::const_buffer buf)
      {
	switch(header.operation) {
	case 0:
	  {
	    raft::native::request_vote req;
	    serialization::deserialize(buf, req);
	    protocol_.on_request_vote(std::move(req));
	    break;
	  }
	case 1:
	  {
	    raft::native::vote_response resp;
	    serialization::deserialize(buf, resp);
	    protocol_.on_vote_response(std::move(resp));
	    break;
	  }
	case 2:
	  {
	    raft::native::messages::append_entry_type req;
	    serialization::deserialize(buf, req);
	    protocol_.on_append_entry(std::move(req));
	    break;
	  }
	case 3:
	  {
	    raft::native::messages::append_entry_response_type req;
	    serialization::deserialize(buf, req);
	    protocol_.on_append_response(std::move(req));
	    break;
	  }
	default:
	  BOOST_LOG_TRIVIAL(warning) << "raft_receiver at " << endpoint_ << " received unsupported operation " << header.operation;
	  break;
	}
      }

      void handle_body_read(boost::system::error_code ec, std::size_t bytes_transferred)
      {
	if (ec) {
	  // TODO: handle error
	  return;
	}
	BOOST_LOG_TRIVIAL(trace) << "raft_receiver at " << endpoint_ << " handle_header_read";
	dispatch(*reinterpret_cast<const rpc_header *>(&buffer_[0]),
		 boost::asio::const_buffer(&buffer_[sizeof(rpc_header)], bytes_transferred));
	boost::asio::async_read(socket_,
				boost::asio::buffer(&buffer_[0], sizeof(rpc_header)),
				std::bind(&raft_receiver::handle_header_read, this, std::placeholders::_1, std::placeholders::_2));
      }

      void handle_header_read(boost::system::error_code ec, std::size_t bytes_transferred)
      {
	if (ec) {
	  // TODO: handle error
	  return;
	}
	BOOST_LOG_TRIVIAL(trace) << "raft_receiver at " << endpoint_ << " handle_header_read";
	BOOST_ASSERT(bytes_transferred == sizeof(rpc_header));
	rpc_header * header = reinterpret_cast<rpc_header *>(&buffer_[0]);
	BOOST_ASSERT(header->magic == rpc_header::MAGIC);
	BOOST_ASSERT(header->payload_length > 0);
	boost::asio::async_read(this->socket_,
				boost::asio::buffer(&buffer_[sizeof(rpc_header)], header->payload_length),
				std::bind(&raft_receiver::handle_body_read, this, std::placeholders::_1, std::placeholders::_2));
      }
      
    public:
      raft_receiver(boost::asio::ip::tcp::socket && s, boost::asio::ip::tcp::endpoint && e, raft_protocol_type & protocol)
	:
	socket_(std::move(s)),
	endpoint_(std::move(e)),
	protocol_(protocol)
      {
	boost::asio::async_read(socket_,
				boost::asio::buffer(&buffer_[0], sizeof(rpc_header)),
				std::bind(&raft_receiver::handle_header_read, this, std::placeholders::_1, std::placeholders::_2));
      }
    };

    class tcp_server : public raft::log_header_write
    {
    private:
      boost::asio::io_service & io_service_;
      boost::asio::deadline_timer timer_;

      // Network Infrastructure
      boost::asio::ip::tcp::acceptor listener_;
      boost::asio::ip::tcp::socket listen_socket_;
      boost::asio::ip::tcp::endpoint listen_endpoint_;
      std::vector<std::shared_ptr<raft_receiver>> receivers_;

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

      raft_protocol_type::communicator_type comm_;
      raft_protocol_type::client_type c_;
      raft_protocol_type::log_type l_;
      raft_protocol_type::checkpoint_data_store_type store_;
      raft_protocol_type::configuration_manager_type config_manager_;
      std::shared_ptr<raft_protocol_type> protocol_;

      void handle_timer(boost::system::error_code ec)
      {
	if (!!protocol_) {
	  protocol_->on_timer();
	  timer_.expires_from_now(boost::posix_time::milliseconds(1));
	  timer_.async_wait(std::bind(&tcp_server::handle_timer, this, std::placeholders::_1));

	  boost::array<uint8_t, 128*1024> tmp_buf;
	  if (!l_.empty()) {
	    // TODO: a checkpoint could cause a problem here because we could truncate a log entry that
	    // didn't get persisted?  Is this logic sufficient?
	    if (last_log_index_written_ < l_.start_index()) {
	      last_log_index_written_ = l_.start_index();
	    }
	    for(;last_log_index_written_ < l_.last_index(); ++last_log_index_written_) {
	      auto serialize_buf = serialization::serialize(boost::asio::buffer(tmp_buf), l_.entry(last_log_index_written_));
	      log_writer_->append_record(boost::asio::buffer_cast<const uint8_t *>(serialize_buf),
					 boost::asio::buffer_size(serialize_buf));
	    }

	    // Sync to disk and let the protocol know when it's done
	    if (!log_sync_in_progress_ && last_log_index_synced_ < last_log_index_written_) {
	      log_sync_in_progress_ = true;
	      uint64_t idx = last_log_index_written_;
	      log_file_->flush_and_sync([this,idx](boost::system::error_code) {
		  this->protocol_->on_log_sync(idx);
		  this->last_log_index_synced_ = idx;
		  this->log_sync_in_progress_ = false;
		});
	    }
	  }	  
	}
      }
      
      void handle_accept(boost::system::error_code ec)
      {
	if (ec) {
	  BOOST_LOG_TRIVIAL(warning) << "raft::asio::tcp_server failed accepting connection: " << ec;
	  return;
	}
	
	BOOST_LOG_TRIVIAL(info) << "raft::asio::tcp_server got connection on " << listen_endpoint_;
	receivers_.push_back(std::make_shared<raft_receiver>(std::move(listen_socket_), std::move(listen_endpoint_), *protocol_.get()));
	BOOST_LOG_TRIVIAL(info) << "raft::asio::tcp_server listening on " << listener_.local_endpoint();
	listener_.async_accept(listen_socket_, listen_endpoint_,
			       std::bind(&tcp_server::handle_accept, this, std::placeholders::_1));
      }
    public:
      tcp_server(boost::asio::io_service & ios,
		 uint64_t server_id,
		 const boost::asio::ip::tcp::endpoint & e,
		 const raft::native::simple_configuration_description & config,
		 const std::string & log_directory)
	:
	io_service_(ios),
	timer_(io_service_),
	listener_(io_service_, e),
	listen_socket_(io_service_),
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
	  BOOST_LOG_TRIVIAL(error) << "Failed to open log header file " << log_header_file << ": " << ::strerror(errno);
	}
	log_header_file_.reset(new basic_file_object<disk_io_service>(io_service_, log_header_fd_));
	std::string log_file(log_directory);
	log_file += std::string("/log.bin");
	log_fd_ = ::open(log_file.c_str(), O_CREAT | O_WRONLY | O_APPEND);
	if (-1 == log_fd_) {
	  BOOST_LOG_TRIVIAL(error) << "Failed to open log header file " << log_file << ": " << ::strerror(errno);
	}
	
	log_file_.reset(new raft::asio::writable_file(io_service_, log_fd_));
	log_writer_.reset(new raft::leveldb::log_writer<raft::asio::writable_file>(*log_file_.get()));

	// Set sync callback in the log (TODO: Fix this is dumb way of doing things)
	l_.set_log_header_writer(this);

	// TODO: Make this work with flatbuffers
	auto entry = new raft_protocol_type::log_entry_type();
	entry->type = raft_protocol_type::log_entry_type::CONFIGURATION;
	entry->term = 0;
	entry->configuration.from = config;
	l_.append(std::pair<const raft_protocol_type::log_entry_type *, std::function<void()>>(entry, [entry]() { delete entry; }));
	l_.update_header(0, raft_protocol_type::INVALID_PEER_ID);
	protocol_.reset(new raft_protocol_type(comm_, l_, store_, config_manager_));

	// Setup periodic timer callbacks from ASIO
	timer_.expires_from_now(boost::posix_time::milliseconds(1));
	timer_.async_wait(std::bind(&tcp_server::handle_timer, this, std::placeholders::_1));

	// Setup listener for interprocess communication
	if (config_manager_.configuration().includes_self()) {
	  BOOST_LOG_TRIVIAL(info) << "Server(" << config_manager_.configuration().my_cluster_id() << ") raft::asio::tcp_server listening on " << listener_.local_endpoint();
	  // TODO: Handle v6, selecting a specific interface on multihomed servers
	  listener_.async_accept(listen_socket_, listen_endpoint_,
				  std::bind(&tcp_server::handle_accept, this, std::placeholders::_1));
	}
      }

      ~tcp_server()
      {
	// Use error code to prevent exception
	boost::system::error_code ec;
	listener_.close(ec);

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
    };
  }
}

#endif
