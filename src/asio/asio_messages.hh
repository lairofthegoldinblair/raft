#ifndef __MESSAGES_H__
#define __MESSAGES_H__

#include "boost/asio/buffer.hpp"
#include "boost/endian/arithmetic.hpp"
#include "../native/messages.hh"

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

    struct serialization
    {
      static std::size_t serialize_helper(boost::asio::mutable_buffer b, const std::string& str)
      {
	boost::endian::little_uint32_t * data_size = boost::asio::buffer_cast<boost::endian::little_uint32_t *>(b);
	*data_size = str.size();
	return sizeof(boost::endian::little_uint32_t) + boost::asio::buffer_copy(b+sizeof(boost::endian::little_uint32_t), boost::asio::buffer(str));
      }

      static std::size_t serialize_helper(boost::asio::mutable_buffer b, const raft::native::server_description& desc)
      {
	boost::endian::little_uint64_t * id = boost::asio::buffer_cast<boost::endian::little_uint64_t *>(b);
	*id = desc.id;
	std::size_t sz = sizeof(boost::endian::little_uint64_t);
	sz += serialization::serialize_helper(b+sz, desc.address);
	return sz;
      }

      static std::size_t serialize_helper(boost::asio::mutable_buffer b, const raft::native::log_entry<raft::native::configuration_description>& entry)
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

      static std::size_t deserialize_helper(boost::asio::const_buffer b, raft::native::server_description& desc)
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

      static std::size_t deserialize_helper(boost::asio::const_buffer b, raft::native::log_entry<raft::native::configuration_description>& entry)
      {
	const little_log_entry * log_buf = boost::asio::buffer_cast<const little_log_entry *>(b);
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

      template<typename _T>
      static boost::asio::const_buffers_1 serialize(boost::asio::mutable_buffer b, const _T & msg)
      {
	return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), 0);
      }

      static void deserialize(boost::asio::const_buffer b, raft::native::messages::request_vote_type & msg)
      {
	BOOST_ASSERT(boost::asio::buffer_size(b) == sizeof(little_request_vote));
	const little_request_vote * buf = boost::asio::buffer_cast<const little_request_vote *>(b);
	msg.set_recipient_id(buf->recipient_id);
	msg.set_term_number(buf->term_number);
	msg.set_candidate_id(buf->candidate_id);
	msg.set_last_log_index(buf->last_log_index);
	msg.set_last_log_term(buf->last_log_term);
      }

      static void deserialize(boost::asio::const_buffer b, raft::native::messages::vote_response_type & msg)
      {
	BOOST_ASSERT(boost::asio::buffer_size(b) == sizeof(little_vote_response));
	const little_vote_response * buf = boost::asio::buffer_cast<const little_vote_response *>(b);
	msg.peer_id = buf->peer_id;
	msg.term_number = buf->term_number;
	msg.request_term_number = buf->request_term_number;
	msg.granted = (buf->granted != 0);
      }

      static std::size_t deserialize(boost::asio::const_buffer b, raft::native::messages::append_entry_type & msg)
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

      static std::size_t deserialize(boost::asio::const_buffer b, raft::native::messages::append_entry_response_type & msg)
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
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::native::messages::request_vote_type & msg)
    {
      static const std::size_t sz = sizeof(little_request_vote) + sizeof(rpc_header);
      BOOST_ASSERT(boost::asio::buffer_size(b) >= sz);
      rpc_header * header = boost::asio::buffer_cast<rpc_header *>(b);
      header->magic = rpc_header::MAGIC;
      header->payload_length = sizeof(little_request_vote);
      header->service = 0;
      header->operation = 0;
      little_request_vote * buf = boost::asio::buffer_cast<little_request_vote *>(b+sizeof(rpc_header));
      buf->recipient_id = raft::native::messages::request_vote_traits_type::recipient_id(msg);
      buf->term_number = raft::native::messages::request_vote_traits_type::term_number(msg);
      buf->candidate_id = raft::native::messages::request_vote_traits_type::candidate_id(msg);
      buf->last_log_index = raft::native::messages::request_vote_traits_type::last_log_index(msg);
      buf->last_log_term = raft::native::messages::request_vote_traits_type::last_log_term(msg);
      return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), sz);
    }

    template<>
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::native::messages::vote_response_type & msg)
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
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::native::messages::append_entry_type & msg)
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
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::native::messages::append_entry_response_type & msg)
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
    boost::asio::const_buffers_1 serialization::serialize(boost::asio::mutable_buffer b, const raft::native::log_entry<raft::native::configuration_description>& entry)
    {
      std::size_t sz = serialize_helper(b, entry);
      return boost::asio::buffer(boost::asio::buffer_cast<const void *>(b), sz);
    }
  }
}

#endif
