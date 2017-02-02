#ifndef __RAFT_ASIO_SERVER_HH__
#define __RAFT_ASIO_SERVER_HH__

#include "boost/endian/arithmetic.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/log/trivial.hpp"
#include "basic_file_object.hh"
#include "disk_io_service.hh"
#include "leveldb_log.hh"
#include "protocol.hh"
#include "server.hh"

namespace raft {
  namespace asio {

    struct log_header
    {
      boost::endian::little_uint64_t current_term;
      boost::endian::little_uint64_t voted_for;	  
    };
    
    struct rpc_header
    {
      static const uint16_t MAGIC=0x09ab;
      boost::endian::little_uint16_t magic;
      boost::endian::little_uint32_t payload_length;
      boost::endian::little_uint16_t service;
      boost::endian::little_uint16_t operation;
    };

    class little_request_vote
    {
    public:
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t candidate_id;
      boost::endian::little_uint64_t last_log_index;
      boost::endian::little_uint64_t last_log_term;
    };
    
    class little_vote_response
    {
    public:
      boost::endian::little_uint64_t peer_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t request_term_number;
      uint8_t granted;
    };

    class little_append_entry
    {
    public:
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t leader_id;
      boost::endian::little_uint64_t previous_log_index;
      boost::endian::little_uint64_t previous_log_term;
      boost::endian::little_uint64_t leader_commit_index;
    };

    class little_append_response
    {
    public:
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t request_term_number;
      boost::endian::little_uint64_t begin_index;
      boost::endian::little_uint64_t last_index;
      uint8_t success;
    };
    
    class little_log_entry
    {
    public:
      boost::endian::little_uint64_t term;
      uint8_t type;
    };

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

    struct serialization
    {
      static std::size_t serialize_helper(boost::asio::mutable_buffer b, const std::string& str)
      {
	boost::endian::little_uint32_t * data_size = boost::asio::buffer_cast<boost::endian::little_uint32_t *>(b);
	*data_size = str.size();
	return sizeof(boost::endian::little_uint32_t) + boost::asio::buffer_copy(b+sizeof(boost::endian::little_uint32_t), boost::asio::buffer(str));
      }

      static std::size_t serialize_helper(boost::asio::mutable_buffer b, const raft::server_description& desc)
      {
	boost::endian::little_uint64_t * id = boost::asio::buffer_cast<boost::endian::little_uint64_t *>(b);
	*id = desc.id;
	std::size_t sz = sizeof(boost::endian::little_uint64_t);
	sz += serialization::serialize_helper(b+sz, desc.address);
	return sz;
      }

      static std::size_t serialize_helper(boost::asio::mutable_buffer b, const raft::log_entry<raft::configuration_description>& entry)
      {
	little_log_entry * log_buf = boost::asio::buffer_cast<little_log_entry *>(b);
	log_buf->term = entry.term;
	log_buf->type = entry.type;
	std::size_t sz = sizeof(little_log_entry);
	sz += serialization::serialize_helper(b+sz, entry.data);
	sz += serialization::serialize_helper(b+sz, entry.configuration.from.servers);
	sz += serialization::serialize_helper(b+sz, entry.configuration.to.servers);
	return sz;
      }

      template<typename _T>
      static std::size_t serialize_helper(boost::asio::mutable_buffer b, const std::vector<_T>& vec)
      {
	boost::endian::little_uint32_t * data_size = boost::asio::buffer_cast<boost::endian::little_uint32_t *>(b);
	*data_size = vec.size();
	std::size_t sz = sizeof(boost::endian::little_uint32_t);
	for (auto & t : vec) {
	  sz += serialization::serialize_helper(b+sz, t);
	}
	return sz;
      }

      static std::size_t deserialize_helper(boost::asio::const_buffer b, raft::server_description& desc)
      {
	std::size_t sz = 0;
	desc.id = *boost::asio::buffer_cast<const boost::endian::little_uint64_t *>(b);
	sz += sizeof(boost::endian::little_uint64_t *);
	sz += serialization::deserialize_helper(b + sz, desc.address);
	return sz;
      }

      static std::size_t deserialize_helper(boost::asio::const_buffer b, std::string& str)
      {
	static const std::size_t header_sz = sizeof(boost::endian::little_uint32_t);
	uint32_t data_size = *boost::asio::buffer_cast<const boost::endian::little_uint32_t *>(b);
	str.assign(boost::asio::buffer_cast<const char *>(b+header_sz), data_size);
	return header_sz + data_size;
      }

      template<typename _T>
      static std::size_t deserialize_helper(boost::asio::const_buffer b, std::vector<_T>& vec)
      {
	static const std::size_t header_sz = sizeof(boost::endian::little_uint32_t);
	uint32_t data_size = *boost::asio::buffer_cast<const boost::endian::little_uint32_t *>(b);
	vec.reserve(data_size);
	std::size_t sz = header_sz;
	for (uint32_t i=0; i!=data_size; ++i) {
	  _T t;
	  sz += serialization::deserialize_helper(b+sz, t);
	  vec.push_back(std::move(t));
	}
	return sz;
      }

      static std::size_t deserialize_helper(boost::asio::const_buffer b, raft::log_entry<raft::configuration_description>& entry)
      {
	const little_log_entry * log_buf = boost::asio::buffer_cast<const little_log_entry *>(b);
	entry.term = log_buf->term;
	switch(log_buf->type) {
	case 0:
	  entry.type = raft::log_entry<raft::configuration_description>::COMMAND;
	  break;
	case 1:
	  entry.type = raft::log_entry<raft::configuration_description>::CONFIGURATION;
	  break;
	case 2:
	  entry.type = raft::log_entry<raft::configuration_description>::NOOP;
	  break;
	default:
	  // TODO: error handling
	  break;
	}
	std::size_t sz = sizeof(little_log_entry);
	sz += serialization::deserialize_helper(b+sz, entry.data);
	sz += serialization::deserialize_helper(b+sz, entry.configuration.from.servers);
	sz += serialization::deserialize_helper(b+sz, entry.configuration.to.servers);
	return sz;
      }

      template<typename _T>
      static boost::asio::const_buffers_1 serialize(boost::asio::mutable_buffer b, const _T & msg)
      {
	return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), 0);
      }

      static void deserialize(boost::asio::const_buffer b, raft::messages::request_vote_type & msg)
      {
	BOOST_ASSERT(boost::asio::buffer_size(b) == sizeof(little_request_vote));
	const little_request_vote * buf = boost::asio::buffer_cast<const little_request_vote *>(b);
	msg.recipient_id = buf->recipient_id;
	msg.term_number = buf->term_number;
	msg.candidate_id = buf->candidate_id;
	msg.last_log_index = buf->last_log_index;
	msg.last_log_term = buf->last_log_term;
      }

      static void deserialize(boost::asio::const_buffer b, raft::messages::vote_response_type & msg)
      {
	BOOST_ASSERT(boost::asio::buffer_size(b) == sizeof(little_vote_response));
	const little_vote_response * buf = boost::asio::buffer_cast<const little_vote_response *>(b);
	msg.peer_id = buf->peer_id;
	msg.term_number = buf->term_number;
	msg.request_term_number = buf->request_term_number;
	msg.granted = (buf->granted != 0);
      }

      static std::size_t deserialize(boost::asio::const_buffer b, raft::messages::append_entry_type & msg)
      {
	const little_append_entry * buf = boost::asio::buffer_cast<const little_append_entry *>(b);
	msg.recipient_id = buf->recipient_id;
	msg.term_number = buf->term_number;
	msg.leader_id = buf->leader_id;
	msg.previous_log_index = buf->previous_log_index ;
	msg.previous_log_term = buf->previous_log_term;
	msg.leader_commit_index = buf->leader_commit_index;
	std::size_t sz = sizeof(little_append_entry);
	sz += serialization::deserialize_helper(b+sz, msg.entry);
	return sz;
      }

      static std::size_t deserialize(boost::asio::const_buffer b, raft::messages::append_entry_response_type & msg)
      {
	const little_append_response * buf = boost::asio::buffer_cast<const little_append_response *>(b);
	msg.recipient_id = buf->recipient_id;
	msg.term_number = buf->term_number;
	msg.request_term_number = buf->request_term_number;
	msg.begin_index = buf->begin_index;
	msg.last_index = buf->last_index;
	msg.success = buf->success ? 1 : 0;
	return sizeof(little_append_response);
      };    
    };
    
    template<>
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::messages::request_vote_type & msg)
    {
      static const std::size_t sz = sizeof(little_request_vote) + sizeof(rpc_header);
      BOOST_ASSERT(boost::asio::buffer_size(b) >= sz);
      rpc_header * header = boost::asio::buffer_cast<rpc_header *>(b);
      header->magic = rpc_header::MAGIC;
      header->payload_length = sizeof(little_request_vote);
      header->service = 0;
      header->operation = 0;
      little_request_vote * buf = boost::asio::buffer_cast<little_request_vote *>(b+sizeof(rpc_header));
      buf->recipient_id = msg.recipient_id;
      buf->term_number = msg.term_number;
      buf->candidate_id = msg.candidate_id;
      buf->last_log_index = msg.last_log_index;
      buf->last_log_term = msg.last_log_term;
      return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), sz);
    }

    template<>
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::messages::vote_response_type & msg)
    {
      static const std::size_t sz = sizeof(little_vote_response) + sizeof(rpc_header);
      BOOST_ASSERT(boost::asio::buffer_size(b) >= sz);
      rpc_header * header = boost::asio::buffer_cast<rpc_header *>(b);
      header->magic = rpc_header::MAGIC;
      header->payload_length = sizeof(little_vote_response);
      header->service = 0;
      header->operation = 1;
      little_vote_response * buf = boost::asio::buffer_cast<little_vote_response *>(b+sizeof(rpc_header));
      buf->peer_id = msg.peer_id;
      buf->term_number = msg.term_number;
      buf->request_term_number = msg.request_term_number;
      buf->granted = msg.granted ? 1 : 0;
      return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), sz);
    }

    template<>
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::messages::append_entry_type & msg)
    {
      // TODO: Make sure the msg fits in our buffer; either throw or support by allocating a new buffer
      std::size_t sz = 0;
      rpc_header * header = boost::asio::buffer_cast<rpc_header *>(b);
      header->magic = rpc_header::MAGIC;
      // Needs to be updated once we've written everything
      header->payload_length = 0;
      header->service = 0;
      header->operation = 2;
      sz += sizeof(rpc_header);
      little_append_entry * buf = boost::asio::buffer_cast<little_append_entry *>(b+sz);
      buf->recipient_id = msg.recipient_id;
      buf->term_number = msg.term_number;
      buf->leader_id = msg.leader_id;
      buf->previous_log_index = msg.previous_log_index;
      buf->previous_log_term = msg.previous_log_term;
      buf->leader_commit_index = msg.leader_commit_index;
      sz += sizeof(little_append_entry);
      sz += serialization::serialize_helper(b+sz, msg.entry);
      header->payload_length = sz-sizeof(rpc_header);
      return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), sz);
    }

    template<>
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::messages::append_entry_response_type & msg)
    {
      static const std::size_t sz = sizeof(little_append_response) + sizeof(rpc_header);
      BOOST_ASSERT(boost::asio::buffer_size(b) >= sz);
      rpc_header * header = boost::asio::buffer_cast<rpc_header *>(b);
      header->magic = rpc_header::MAGIC;
      header->payload_length = sizeof(little_append_response);
      header->service = 0;
      header->operation = 3;
      little_append_response * buf = boost::asio::buffer_cast<little_append_response *>(b+sizeof(rpc_header));
      buf->recipient_id = msg.recipient_id;
      buf->term_number = msg.term_number;
      buf->request_term_number = msg.request_term_number;
      buf->begin_index = msg.begin_index;
      buf->last_index = msg.last_index;
      buf->success = (msg.success != 0);
      return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), sz);
    };
    
    template<>
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::log_entry<raft::configuration_description>& entry)
    {
      std::size_t sz = serialize_helper(b, entry);
      return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), sz);
    }
    
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
	BOOST_LOG_TRIVIAL(trace) << "Entering asio_tcp_communicator::send on " << endpoint_;
	// TODO: Make sure at most one send outstanding on each socket?
	auto sit = sockets_.find(addr);
	if (sit == sockets_.end()) {
	  // Only handling v4 addresses
	  auto pos = addr.find_last_of(':');
	  auto v4address = boost::asio::ip::address_v4::from_string(addr.substr(0, pos));
	  auto port = boost::lexical_cast<unsigned short>(addr.substr(pos+1));
	  auto ins_it = sockets_.emplace(addr, std::shared_ptr<communicator_peer>(new communicator_peer(io_service_)));
	  uint8_t * mem = new uint8_t [128*1024];
	  auto buf = serialization::serialize(boost::asio::buffer(mem, 128*1024), msg);
	  const std::string & addr_copy(ins_it.first->first);
	  ins_it.first->second->peer_endpoint.address(v4address);
	  ins_it.first->second->peer_endpoint.port(port);	  
	  ins_it.first->second->peer_socket.async_connect(sockets_[addr]->peer_endpoint,
							  [this, buf, addr_copy](boost::system::error_code ec) mutable {
							    if (ec) {
							      BOOST_LOG_TRIVIAL(warning) << "asio_tcp_communicator " << endpoint_ << " failed to connect to " << addr_copy <<
								": " << ec;
							      delete [] boost::asio::buffer_cast<const uint8_t *>(buf);
							      return;
							    }
							    BOOST_LOG_TRIVIAL(info) << "asio_tcp_communicator " << endpoint_ << " connected to " << addr_copy;
							    BOOST_LOG_TRIVIAL(debug) << "asio_tcp_communicator " << endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
							      " bytes for operation " << boost::asio::buffer_cast<const rpc_header *>(buf)->operation <<
							      " to " << sockets_[addr_copy]->peer_endpoint;
							    boost::asio::async_write(this->sockets_[addr_copy]->peer_socket,
										     buf,
										     [buf](boost::system::error_code ec, std::size_t bytes_transferred) {
										       delete [] boost::asio::buffer_cast<const uint8_t *>(buf);
										     });
						    });
	  
	} else {
	  uint8_t * mem = new uint8_t [128*1024];
	  auto buf = serialization::serialize(boost::asio::buffer(mem, 128*1024), msg);	  
	  BOOST_LOG_TRIVIAL(debug) << "asio_tcp_communicator " << endpoint_ << " sending " << boost::asio::buffer_size(buf) <<
	    " bytes for operation " << boost::asio::buffer_cast<const rpc_header *>(buf)->operation <<
	    " to " << sit->second->peer_endpoint;
	  boost::asio::async_write(sit->second->peer_socket,
				   buf,
				   [buf](boost::system::error_code ec, std::size_t bytes_transferred) {
				     delete [] boost::asio::buffer_cast<const uint8_t *>(buf);
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

    typedef raft::server<asio_tcp_communicator_metafunction, raft::messages> raft_protocol_type;
    
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
	    raft::request_vote req;
	    serialization::deserialize(buf, req);
	    protocol_.on_request_vote(req);
	    break;
	  }
	case 1:
	  {
	    raft::vote_response resp;
	    serialization::deserialize(buf, resp);
	    protocol_.on_vote_response(resp);
	    break;
	  }
	case 2:
	  {
	    raft::messages::append_entry_type req;
	    serialization::deserialize(buf, req);
	    protocol_.on_append_entry(req);
	    break;
	  }
	case 3:
	  {
	    raft::messages::append_entry_response_type req;
	    serialization::deserialize(buf, req);
	    protocol_.on_append_response(req);
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
		 const raft_protocol_type::simple_configuration_description_type & config,
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
	
	std::vector<raft_protocol_type::log_entry_type> entries;
	entries.push_back(raft_protocol_type::log_entry_type());
	entries.back().type = raft_protocol_type::log_entry_type::CONFIGURATION;
	entries.back().term = 0;
	entries.back().configuration.from = config;
	l_.append(entries);
	l_.update_header(entries.back().term, raft_protocol_type::INVALID_PEER_ID);
	protocol_.reset(new raft_protocol_type(comm_, l_, store_, config_manager_));

	// Setup periodic timer callbacks from ASIO
	timer_.expires_from_now(boost::posix_time::milliseconds(1));
	timer_.async_wait(std::bind(&tcp_server::handle_timer, this, std::placeholders::_1));

	// Setup listener for interprocess communication
	if (config_manager_.configuration().includes_self()) {
	  BOOST_LOG_TRIVIAL(info) << "raft::asio::tcp_server listening on " << listener_.local_endpoint();
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
	BOOST_LOG_TRIVIAL(info) << "raft::asio::tcp_server request to sync log header current_term=" << current_term <<
	  " voted_for=" << voted_for;
	header_.current_term = current_term;
	header_.voted_for = voted_for;
	log_header_file_->async_write(boost::asio::buffer(&header_, sizeof(header_)),
				      [this](boost::system::error_code ec, std::size_t bytes_transferred) mutable {
					this->protocol_->on_log_header_sync();
				      });
      }
    };
  }
}

#endif
