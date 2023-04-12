#ifndef __RAFT_NATIVE_SERIALIZTION__
#define __RAFT_NATIVE_SERIALIZTION__

#include "boost/endian/arithmetic.hpp"
#include "messages.hh"

namespace raft {
  namespace native { 
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
    
    class little_client_request
    {
    public:
    };
    
    class little_client_response
    {
    public:
      boost::endian::little_uint8_t result;
      boost::endian::little_uint64_t index;
      boost::endian::little_uint64_t leader_id;
    };

    class little_set_configuration_request
    {
    public:
      boost::endian::little_uint64_t old_id;
    };

    class little_set_configuration_response
    {
    public:
      boost::endian::little_uint8_t result;
    };

    class little_append_checkpoint_chunk
    {
    public:
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t leader_id;
      boost::endian::little_uint64_t last_log_entry_index;
      boost::endian::little_uint64_t last_log_entry_term;
      boost::endian::little_uint64_t configuration_index;
      boost::endian::little_uint64_t checkpoint_begin;
      boost::endian::little_uint64_t checkpoint_end;
      boost::endian::little_uint8_t checkpoint_done;
    };

    class little_append_checkpoint_chunk_response
    {
    public:
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t request_term_number;
      boost::endian::little_uint64_t bytes_stored;
    };

    class little_checkpoint_header
    {
    public:
    };

    class little_log_entry
    {
    public:
      boost::endian::little_uint64_t term;
      uint8_t type;
    };

    struct serialization
    {
      static std::size_t serialize_helper(raft::mutable_slice && b, const std::string& str)
      {
	boost::endian::little_uint32_t * data_size = reinterpret_cast<boost::endian::little_uint32_t *>(b.data());
	*data_size = str.size();
	return sizeof(boost::endian::little_uint32_t) + raft::buffer_copy(b+sizeof(boost::endian::little_uint32_t), raft::slice::create(str));
      }

      static std::size_t serialize_helper(raft::mutable_slice && b, const raft::native::server_description& desc)
      {
	boost::endian::little_uint64_t * id = reinterpret_cast<boost::endian::little_uint64_t *>(b.data());
	*id = desc.id;
	std::size_t sz = sizeof(boost::endian::little_uint64_t);
	sz += serialization::serialize_helper(b+sz, desc.address);
	return sz;
      }

      template<typename _T>
      static std::size_t serialize_helper(raft::mutable_slice && b, const std::vector<_T>& vec)
      {
	boost::endian::little_uint32_t * data_size = reinterpret_cast<boost::endian::little_uint32_t *>(b.data());
	*data_size = vec.size();
	std::size_t sz = sizeof(boost::endian::little_uint32_t);
	for (auto & t : vec) {
	  sz += serialization::serialize_helper(b+sz, t);
	}
	return sz;
      }

      static std::size_t serialize_helper(raft::mutable_slice && b, const std::vector<uint8_t>& vec)
      {
	boost::endian::little_uint32_t * data_size = reinterpret_cast<boost::endian::little_uint32_t *>(b.data());
	*data_size = vec.size();
	return sizeof(boost::endian::little_uint32_t) + raft::buffer_copy(b+sizeof(boost::endian::little_uint32_t), raft::slice(&vec[0], vec.size()));
      }

      static std::size_t serialize_helper(raft::mutable_slice && b, const raft::native::simple_configuration_description& desc)
      {
	return serialization::serialize_helper(b+0, desc.servers);
      }

      static std::size_t serialize_helper(raft::mutable_slice && b, const raft::native::configuration_description& desc)
      {
	std::size_t sz = 0;
	sz += serialization::serialize_helper(b+sz, desc.from);
	sz += serialization::serialize_helper(b+sz, desc.to);
	return sz;
      }
      
      static std::size_t serialize_helper(raft::mutable_slice && b, const raft::native::log_entry<raft::native::configuration_description>& entry)
      {
	little_log_entry * log_buf = reinterpret_cast<little_log_entry *>(b.data());
	log_buf->term = entry.term;
	log_buf->type = entry.type;
	std::size_t sz = sizeof(little_log_entry);
	sz += serialization::serialize_helper(b+sz, entry.data);
	sz += serialization::serialize_helper(b+sz, entry.configuration);
	return sz;
      }

      static client_result convert_client_result(uint8_t val)
      {
	switch(val) {
	case 0:
	  return SUCCESS;
	case 1:
	  return FAIL;
	case 2:
	  return RETRY;
	case 3:
	  return NOT_LEADER;
	default:
	  throw std::runtime_error("Invalid client result");
	}
      }

      static std::size_t deserialize_helper(raft::slice && b, raft::native::server_description& desc)
      {
	std::size_t sz = 0;
	desc.id = *reinterpret_cast<const boost::endian::little_uint64_t *>(b.data());
	sz += sizeof(boost::endian::little_uint64_t *);
	sz += serialization::deserialize_helper(b + sz, desc.address);
	return sz;
      }

      static std::size_t deserialize_helper(raft::slice && b, std::string& str)
      {
	static const std::size_t header_sz = sizeof(boost::endian::little_uint32_t);
	uint32_t data_size = *reinterpret_cast<const boost::endian::little_uint32_t *>(b.data());
	str.assign(reinterpret_cast<const char *>((b+header_sz).data()), data_size);
	return header_sz + data_size;
      }

      template<typename _T>
      static std::size_t deserialize_helper(raft::slice && b, std::vector<_T>& vec)
      {
	static const std::size_t header_sz = sizeof(boost::endian::little_uint32_t);
	uint32_t data_size = *reinterpret_cast<const boost::endian::little_uint32_t *>(b.data());
	vec.reserve(data_size);
	std::size_t sz = header_sz;
	for (uint32_t i=0; i!=data_size; ++i) {
	  _T t;
	  sz += serialization::deserialize_helper(b+sz, t);
	  vec.push_back(std::move(t));
	}
	return sz;
      }

      static std::size_t deserialize_helper(raft::slice && b, std::vector<uint8_t>& vec)
      {
	static const std::size_t header_sz = sizeof(boost::endian::little_uint32_t);
	uint32_t data_size = *reinterpret_cast<const boost::endian::little_uint32_t *>(b.data());
	auto ptr = reinterpret_cast<const uint8_t *>((b+header_sz).data());
	vec.assign(ptr, ptr+data_size);
	return header_sz + data_size;
      }

      static std::size_t deserialize_helper(raft::slice && b, raft::native::simple_configuration_description& desc)
      {
	return serialization::deserialize_helper(b+0, desc.servers);
      }

      static std::size_t deserialize_helper(raft::slice && b, raft::native::configuration_description& desc)
      {
	std::size_t sz = 0;
	sz += serialization::deserialize_helper(b+sz, desc.from);
	sz += serialization::deserialize_helper(b+sz, desc.to);
	return sz;
      }

      static std::size_t deserialize_helper(raft::slice && b, raft::native::log_entry<raft::native::configuration_description>& entry)
      {
	const little_log_entry * log_buf = reinterpret_cast<const little_log_entry *>(b.data());
	entry.term = log_buf->term;
	switch(log_buf->type) {
	case 0:
	  entry.type = raft::native::log_entry<raft::native::configuration_description>::COMMAND;
	  break;
	case 1:
	  entry.type = raft::native::log_entry<raft::native::configuration_description>::CONFIGURATION;
	  break;
	case 2:
	  entry.type = raft::native::log_entry<raft::native::configuration_description>::NOOP;
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

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(const raft::native::log_entry<raft::native::configuration_description>& entry);
	
      static raft::native::messages::request_vote_type deserialize_request_vote(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::request_vote_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_request_vote));
	const little_request_vote * buf = reinterpret_cast<const little_request_vote *>(b.first.data());
	msg.set_recipient_id(buf->recipient_id);
	msg.set_term_number(buf->term_number);
	msg.set_candidate_id(buf->candidate_id);
	msg.set_last_log_index(buf->last_log_index);
	msg.set_last_log_term(buf->last_log_term);
	return msg;
      }

      static raft::native::messages::vote_response_type deserialize_vote_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::vote_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_vote_response));
	const little_vote_response * buf = reinterpret_cast<const little_vote_response *>(b.first.data());
	msg.peer_id = buf->peer_id;
	msg.term_number = buf->term_number;
	msg.request_term_number = buf->request_term_number;
	msg.granted = (buf->granted != 0);
	return msg;
      }

      static raft::native::messages::append_entry_type deserialize_append_entry(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::append_entry_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_append_entry));
	const little_append_entry * buf = reinterpret_cast<const little_append_entry *>(b.first.data());
	msg.recipient_id = buf->recipient_id;
	msg.term_number = buf->term_number;
	msg.leader_id = buf->leader_id;
	msg.previous_log_index = buf->previous_log_index ;
	msg.previous_log_term = buf->previous_log_term;
	msg.leader_commit_index = buf->leader_commit_index;
	std::size_t sz = sizeof(little_append_entry);
	sz += serialization::deserialize_helper(b.first+sz, msg.entry);
	return msg;
      }

      static raft::native::messages::append_entry_response_type deserialize_append_entry_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::append_entry_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_append_response));
	const little_append_response * buf = reinterpret_cast<const little_append_response *>(b.first.data());
	msg.recipient_id = buf->recipient_id;
	msg.term_number = buf->term_number;
	msg.request_term_number = buf->request_term_number;
	msg.begin_index = buf->begin_index;
	msg.last_index = buf->last_index;
	msg.success = buf->success ? 1 : 0;
	return msg;
      }
    
      static raft::native::messages::client_request_type deserialize_client_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::client_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_client_request));
	deserialize_helper(b.first+0, msg.command);
	return msg;
      }
    
      static raft::native::messages::client_response_type deserialize_client_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::client_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_client_response));
	auto buf = reinterpret_cast<const little_client_response *>(b.first.data());
	msg.result = convert_client_result(buf->result);
	msg.index = buf->index;
	msg.leader_id = buf->leader_id;
	return msg;
      }
    
      static raft::native::messages::set_configuration_request_type deserialize_set_configuration_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::set_configuration_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_set_configuration_request));
	auto buf = reinterpret_cast<const little_set_configuration_request *>(b.first.data());
	msg.old_id = buf->old_id;
	std::size_t sz = sizeof(little_set_configuration_request);
	sz += deserialize_helper(b.first+sz, msg.new_configuration);
	return msg;
      }
    
      static raft::native::messages::set_configuration_response_type deserialize_set_configuration_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::set_configuration_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_set_configuration_response));
	auto buf = reinterpret_cast<const little_set_configuration_response *>(b.first.data());
	msg.result = convert_client_result(buf->result);
	std::size_t sz = sizeof(little_set_configuration_response);
	sz += deserialize_helper(b.first+sz, msg.bad_servers);
	return msg;
      }
    
      static raft::native::messages::append_checkpoint_chunk_type deserialize_append_checkpoint_chunk(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::append_checkpoint_chunk_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_append_checkpoint_chunk));
	auto buf = reinterpret_cast<const little_append_checkpoint_chunk *>(b.first.data());
	msg.recipient_id = buf->recipient_id;
	msg.term_number= buf->term_number;
	msg.leader_id = buf->leader_id;
	msg.last_checkpoint_header.last_log_entry_index = buf->last_log_entry_index;
	msg.last_checkpoint_header.last_log_entry_term = buf->last_log_entry_term;
	msg.last_checkpoint_header.configuration.index = buf->configuration_index;
	msg.checkpoint_begin = buf->checkpoint_begin;
	msg.checkpoint_end = buf->checkpoint_end;
	msg.checkpoint_done = buf->checkpoint_done != 0;
	std::size_t sz = sizeof(little_append_checkpoint_chunk);
	sz += deserialize_helper(b.first+sz, msg.last_checkpoint_header.configuration.description);
	sz += deserialize_helper(b.first+sz, msg.data);
	return msg;
      }
    
      static raft::native::messages::append_checkpoint_chunk_response_type deserialize_append_checkpoint_chunk_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::append_checkpoint_chunk_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_append_checkpoint_chunk_response));
	auto buf = reinterpret_cast<const little_append_checkpoint_chunk_response *>(b.first.data());
	msg.recipient_id = buf->recipient_id;
	msg.term_number= buf->term_number;
	msg.request_term_number = buf->request_term_number;
	msg.bytes_stored = buf->bytes_stored;
	return msg;
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::request_vote_type && msg)
      {
	auto buf = new little_request_vote();
	buf->recipient_id = raft::native::messages::request_vote_traits_type::recipient_id(msg);
	buf->term_number = raft::native::messages::request_vote_traits_type::term_number(msg);
	buf->candidate_id = raft::native::messages::request_vote_traits_type::candidate_id(msg);
	buf->last_log_index = raft::native::messages::request_vote_traits_type::last_log_index(msg);
	buf->last_log_term = raft::native::messages::request_vote_traits_type::last_log_term(msg);
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_request_vote)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::vote_response_type && msg)
      {
	auto buf = new little_vote_response();
	buf->peer_id = msg.peer_id;
	buf->term_number = msg.term_number;
	buf->request_term_number = msg.request_term_number;
	buf->granted = msg.granted ? 1 : 0;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_vote_response)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::append_entry_type && msg)
      {
	// TODO: Make sure the msg fits in our buffer; either throw or support by allocating a new buffer
	raft::mutable_slice b(new uint8_t [1024], 1024);
	auto * buf = reinterpret_cast<little_append_entry *>(b.data());
	buf->recipient_id = msg.recipient_id;
	buf->term_number = msg.term_number;
	buf->leader_id = msg.leader_id;
	buf->previous_log_index = msg.previous_log_index;
	buf->previous_log_term = msg.previous_log_term;
	buf->leader_commit_index = msg.leader_commit_index;
	auto sz = sizeof(little_append_entry);
	sz += serialize_helper(b+sz, msg.entry);      
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::append_entry_response_type && msg)
      {
	auto buf = new little_append_response();
	buf->recipient_id = msg.recipient_id;
	buf->term_number = msg.term_number;
	buf->request_term_number = msg.request_term_number;
	buf->begin_index = msg.begin_index;
	buf->last_index = msg.last_index;
	buf->success = (msg.success != 0);
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_append_response)),
								  [buf]() { delete buf; });
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::client_request_type && msg)
      {
	// TODO: Make sure the msg fits in our buffer; either throw or support by allocating a new buffer
	raft::mutable_slice b(new uint8_t [1024], 1024);
	auto sz = sizeof(little_client_request);
	sz += serialize_helper(b+sz, msg.command);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::client_response_type && msg)
      {
	auto buf = new little_client_response();
	buf->result = msg.result;
	buf->index = msg.index;
	buf->leader_id = msg.leader_id;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_client_response)),
								  [buf]() { delete buf; });
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::set_configuration_request_type && msg)
      {
	// TODO: Make sure the msg fits in our buffer; either throw or support by allocating a new buffer
	raft::mutable_slice b(new uint8_t [1024], 1024);
	auto * buf = reinterpret_cast<little_set_configuration_request *>(b.data());
	buf->old_id = msg.old_id;
	auto sz = sizeof(little_set_configuration_request);
	sz += serialize_helper(b+sz, msg.new_configuration);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::set_configuration_response_type && msg)
      {
	// TODO: Make sure the msg fits in our buffer; either throw or support by allocating a new buffer
	raft::mutable_slice b(new uint8_t [1024], 1024);
	auto * buf = reinterpret_cast<little_set_configuration_response *>(b.data());
	buf->result = msg.result;
	auto sz = sizeof(little_set_configuration_response);
	sz += serialize_helper(b+sz, msg.bad_servers);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::append_checkpoint_chunk_type && msg)
      {
	// TODO: Make sure the msg fits in our buffer; either throw or support by allocating a new buffer
	raft::mutable_slice b(new uint8_t [1024], 1024);
	auto * buf = reinterpret_cast<little_append_checkpoint_chunk *>(b.data());
	buf->recipient_id = msg.recipient_id;
	buf->term_number= msg.term_number;
	buf->leader_id = msg.leader_id;
	buf->last_log_entry_index = msg.last_checkpoint_header.last_log_entry_index;
	buf->last_log_entry_term = msg.last_checkpoint_header.last_log_entry_term;
	buf->configuration_index = msg.last_checkpoint_header.configuration.index;
	buf->checkpoint_begin = msg.checkpoint_begin;
	buf->checkpoint_end = msg.checkpoint_end;
	buf->checkpoint_done = msg.checkpoint_done ? 1 : 0;
	std::size_t sz = sizeof(little_append_checkpoint_chunk);
	sz += serialize_helper(b+sz, msg.last_checkpoint_header.configuration.description);
	sz += serialize_helper(b+sz, msg.data);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::append_checkpoint_chunk_response_type && msg)
      {
	auto buf = new little_append_checkpoint_chunk_response();
	buf->recipient_id = msg.recipient_id;
	buf->term_number = msg.term_number;
	buf->request_term_number = msg.request_term_number;
	buf->bytes_stored = msg.bytes_stored;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_append_checkpoint_chunk_response)),
								  [buf]() { delete buf; });
      }
    };
    
    std::pair<raft::slice, raft::util::call_on_delete> serialization::serialize(const raft::native::log_entry<raft::native::configuration_description>& entry)
    {
      auto ptr = new uint8_t [1024];
      std::size_t sz = serialize_helper(raft::mutable_slice(ptr, 1024), entry);
      return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
    }
  }
}

#endif