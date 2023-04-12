#ifndef __ASIO_MESSAGES_H__
#define __ASIO_MESSAGES_H__

#include <type_traits>
#include "boost/asio/buffer.hpp"
#include "boost/endian/arithmetic.hpp"
#include "boost/tti/has_static_member_function.hpp"
#include "slice.hh"
#include "util/call_on_delete.hh"

namespace raft {
  namespace asio {

    struct rpc_header
    {
      static const uint16_t MAGIC=0x09ab;
      static const uint16_t POISON=0xbaad;
      boost::endian::little_uint16_t magic;
      boost::endian::little_uint32_t payload_length;
      boost::endian::little_uint16_t service;
      boost::endian::little_uint16_t operation;
    };

    struct log_header
    {
      boost::endian::little_uint64_t current_term;
      boost::endian::little_uint64_t voted_for;	  
    };

    template<typename _Messages, typename _Serialization>
    struct serialization
    {
      typedef typename _Messages::request_vote_traits_type::arg_type request_vote_arg_type;
      typedef typename _Messages::vote_response_traits_type::arg_type vote_response_arg_type;
      typedef typename _Messages::append_entry_traits_type::arg_type append_entry_arg_type;
      typedef typename _Messages::append_entry_response_traits_type::arg_type append_entry_response_arg_type;
      typedef typename _Messages::client_request_traits_type::arg_type client_request_arg_type;
      typedef typename _Messages::client_response_traits_type::arg_type client_response_arg_type;
      typedef typename _Messages::set_configuration_request_traits_type::arg_type set_configuration_request_arg_type;
      typedef typename _Messages::set_configuration_response_traits_type::arg_type set_configuration_response_arg_type;
      typedef typename _Messages::append_checkpoint_chunk_traits_type::arg_type append_checkpoint_chunk_arg_type;
      typedef typename _Messages::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;
      typedef typename _Messages::log_entry_type log_entry_type;
      typedef _Serialization serialization_type;

      BOOST_TTI_HAS_STATIC_MEMBER_FUNCTION(classify);
	
      static std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> make_return(const void * ptr,
													 std::size_t sz,
													 std::pair<raft::slice, raft::util::call_on_delete> && inner)
      {
	raft::util::call_on_delete deleter([i = std::move(inner.second), ptr]() { delete [] reinterpret_cast<const uint8_t *>(ptr); });
	return std::make_pair(std::array<boost::asio::const_buffer, 2>({ boost::asio::buffer(ptr, sz), boost::asio::buffer(inner.first.data(), inner.first.size()) }),
			      std::move(deleter));
      }
    
      static std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> serialize_request_vote(boost::asio::mutable_buffer b, request_vote_arg_type && msg)
      {
        typedef typename _Messages::request_vote_traits_type rv;
        BOOST_LOG_TRIVIAL(trace) << "request_vote(recipient_id=" << rv::recipient_id(msg) << ", term_number=" << rv::term_number(msg) <<
          ", candidate_id=" << rv::candidate_id(msg) << ", last_log_index=" << rv::last_log_index(msg) << ", last_log_term=" << rv::last_log_term(msg) << ")";
	auto inner = serialization_type::serialize(std::move(msg));
        BOOST_LOG_TRIVIAL(trace) << "request_vote serialzed: " << inner.first;
	static const std::size_t sz = sizeof(rpc_header);
	BOOST_ASSERT(boost::asio::buffer_size(b) >= sz);
	rpc_header * header = reinterpret_cast<rpc_header *>(b.data());
	header->magic = rpc_header::MAGIC;
	header->payload_length = inner.first.size();
	header->service = 0;
	header->operation = 0;
	return make_return(b.data(), sz, std::move(inner));
      }

      static std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> serialize_vote_response(boost::asio::mutable_buffer b, vote_response_arg_type && msg)
      {
        typedef typename _Messages::vote_response_traits_type vr;
        BOOST_LOG_TRIVIAL(trace) << "vote_response(peer_id=" << vr::peer_id(msg) << ", term_number=" << vr::term_number(msg) <<
          ", request_term_number=" << vr::request_term_number(msg) << ", granted=" << vr::granted(msg) << ")";
	auto inner = serialization_type::serialize(std::move(msg));
        BOOST_LOG_TRIVIAL(trace) << "vote_response serialzed: " << inner.first;
	static const std::size_t sz = sizeof(rpc_header);
	BOOST_ASSERT(boost::asio::buffer_size(b) >= sz);
	rpc_header * header = reinterpret_cast<rpc_header *>(b.data());
	header->magic = rpc_header::MAGIC;
	header->payload_length = inner.first.size();
	header->service = 0;
	header->operation = 1;
	return make_return(b.data(), sz, std::move(inner));
      }

      static std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> serialize_append_entry(boost::asio::mutable_buffer b, append_entry_arg_type && msg)
      {
	// TODO: Make sure the msg fits in our buffer; either throw or support by allocating a new buffer
	auto inner = serialization_type::serialize(std::move(msg));
	std::size_t sz = sizeof(rpc_header);
	rpc_header * header = reinterpret_cast<rpc_header *>(b.data());
	header->magic = rpc_header::MAGIC;
	header->payload_length = inner.first.size();
	header->service = 0;
	header->operation = 2;
	return make_return(b.data(), sz, std::move(inner));
      }

      static std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete> serialize_append_entry_response(boost::asio::mutable_buffer b, append_entry_response_arg_type && msg)
      {
	auto inner = serialization_type::serialize(std::move(msg));
	static const std::size_t sz = sizeof(rpc_header);
	BOOST_ASSERT(boost::asio::buffer_size(b) >= sz);
	rpc_header * header = reinterpret_cast<rpc_header *>(b.data());
	header->magic = rpc_header::MAGIC;
	header->payload_length = inner.first.size();
	header->service = 0;
	header->operation = 3;
	return make_return(b.data(), sz, std::move(inner));
      };

      template<typename _M = _Messages, typename _S = _Serialization>
      static std::enable_if_t<!has_static_member_function_classify<_S, int32_t(typename _M::request_vote_traits_type::arg_type &&)>::value, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>
      serialize(boost::asio::mutable_buffer b,
	        typename _M::request_vote_traits_type::arg_type && msg)
      {
	return serialize_request_vote(b, std::move(msg));
      }
      
      template<typename _M = _Messages, typename _S = _Serialization>
      static std::enable_if_t<!has_static_member_function_classify<_S, int32_t(typename _M::request_vote_traits_type::arg_type &&)>::value, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>
      serialize(boost::asio::mutable_buffer b,
	        typename _M::vote_response_traits_type::arg_type && msg)
      {
	return serialize_vote_response(b, std::move(msg));
      }
      
      template<typename _M = _Messages, typename _S = _Serialization>
      static std::enable_if_t<!has_static_member_function_classify<_S, int32_t(typename _M::request_vote_traits_type::arg_type &&)>::value, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>
      serialize(boost::asio::mutable_buffer b,
	        typename _M::append_entry_traits_type::arg_type && msg)
      {
	return serialize_append_entry(b, std::move(msg));
      }
      
      template<typename _M = _Messages, typename _S = _Serialization>
      static std::enable_if_t<!has_static_member_function_classify<_S, int32_t(typename _M::request_vote_traits_type::arg_type &&)>::value, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>
      serialize(boost::asio::mutable_buffer b,
	        typename _M::append_entry_response_traits_type::arg_type && msg)
      {
	return serialize_append_entry_response(b, std::move(msg));
      }
      
      template<typename T, typename _M = _Messages, typename _S = _Serialization>
      static std::enable_if_t<!has_static_member_function_classify<_S, int32_t(typename _M::request_vote_traits_type::arg_type &&)>::value, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>
      serialize(boost::asio::mutable_buffer b,
	        T && msg)
      {	
	return std::make_pair(std::array<boost::asio::const_buffer, 2>(), raft::util::call_on_delete());
      }

      template<typename _M = _Messages, typename _S = _Serialization>
      static std::enable_if_t<has_static_member_function_classify<_S, int32_t(typename _M::request_vote_traits_type::arg_type &&)>::value, std::pair<std::array<boost::asio::const_buffer, 2>, raft::util::call_on_delete>>
      serialize(boost::asio::mutable_buffer b,
      		typename _M::append_entry_traits_type::arg_type && msg)
      {
      	switch(serialization_type::classify(std::move(msg))) {
      	case 0:
      	  return serialize_request_vote(b, std::move(msg));
      	case 1:
      	  return serialize_vote_response(b, std::move(msg));
      	case 2:
      	  return serialize_append_entry(b, std::move(msg));
      	case 3:
      	  return serialize_append_entry_response(b, std::move(msg));
      	default:
      	  throw std::runtime_error("Not yet implemented");
      	}
      }

      static std::pair<boost::asio::const_buffer, raft::util::call_on_delete> serialize(boost::asio::mutable_buffer b, const log_entry_type& entry)
      {
	auto inner = serialization_type::serialize(entry);
	return std::make_pair(boost::asio::buffer(inner.first.data(), inner.first.size()), std::move(inner.second));
      }
	
      static request_vote_arg_type deserialize_request_vote(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_request_vote(std::make_pair(std::move(s), std::move(deleter)));
      }

      static vote_response_arg_type deserialize_vote_response(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_vote_response(std::make_pair(std::move(s), std::move(deleter)));
      }

      static append_entry_arg_type deserialize_append_entry(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_append_entry(std::make_pair(std::move(s), std::move(deleter)));
      }

      static append_entry_response_arg_type deserialize_append_entry_response(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_append_entry_response(std::make_pair(std::move(s), std::move(deleter)));
      };
      
      static client_request_arg_type deserialize_client_request(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_client_request(std::make_pair(std::move(s), std::move(deleter)));

      }

      static client_response_arg_type deserialize_client_response(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_client_response(std::make_pair(std::move(s), std::move(deleter)));
      }
      
      static set_configuration_request_arg_type deserialize_set_configuration_request(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_set_configuration_request(std::make_pair(std::move(s), std::move(deleter)));
      }

      static set_configuration_response_arg_type deserialize_set_configuration_response(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_set_configuration_response(std::make_pair(std::move(s), std::move(deleter)));
      }
      
      static append_checkpoint_chunk_arg_type deserialize_append_checkpoint_chunk(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_append_checkpoint_chunk(std::make_pair(std::move(s), std::move(deleter)));
      }

      static append_checkpoint_chunk_response_arg_type deserialize_append_checkpoint_chunk_response(boost::asio::const_buffer & b, raft::util::call_on_delete && deleter)
      {
	raft::slice s(reinterpret_cast<const uint8_t *>(b.data()), b.size());
	return serialization_type::deserialize_append_checkpoint_chunk_response(std::make_pair(std::move(s), std::move(deleter)));
      };    
    };

  }
}

#endif
