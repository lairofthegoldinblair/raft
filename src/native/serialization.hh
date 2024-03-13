#ifndef __RAFT_NATIVE_SERIALIZTION__
#define __RAFT_NATIVE_SERIALIZTION__

#include "boost/endian/arithmetic.hpp"
#include "messages.hh"

namespace raft {
  namespace native { 
    class little_vote_request
    {
    public:
      boost::endian::little_uint64_t request_id;
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t candidate_id;
      boost::endian::little_uint64_t log_index_end;
      boost::endian::little_uint64_t last_log_term;
    };
    
    class little_vote_response
    {
    public:
      boost::endian::little_uint64_t peer_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t request_term_number;
      boost::endian::little_uint64_t request_id;
      uint8_t granted;
    };

    class little_append_entry_request
    {
    public:
      boost::endian::little_uint64_t request_id;
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t leader_id;
      boost::endian::little_uint64_t log_index_begin;
      boost::endian::little_uint64_t previous_log_term;
      boost::endian::little_uint64_t leader_commit_index_end;
    };

    class little_append_entry_response
    {
    public:
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t request_term_number;
      boost::endian::little_uint64_t request_id;
      boost::endian::little_uint64_t index_begin;
      boost::endian::little_uint64_t index_end;
      uint8_t success;
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

    class little_get_configuration_request
    {
    public:
      uint8_t dummy;
    };

    class little_get_configuration_response
    {
    public:
      boost::endian::little_uint8_t result;
      boost::endian::little_uint64_t id;
    };

    class little_append_checkpoint_chunk_request
    {
    public:
      boost::endian::little_uint64_t request_id;
      boost::endian::little_uint64_t recipient_id;
      boost::endian::little_uint64_t term_number;
      boost::endian::little_uint64_t leader_id;
      boost::endian::little_uint64_t log_entry_index_end;
      boost::endian::little_uint64_t last_log_entry_term;
      boost::endian::little_uint64_t last_log_entry_cluster_time;
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
      boost::endian::little_uint64_t request_id;
      boost::endian::little_uint64_t bytes_stored;
    };

    class little_open_session_request
    {
    public:
      uint8_t dummy;
    };

    class little_open_session_response
    {
    public:
      boost::endian::little_uint64_t session_id;
    };    

    class little_close_session_request
    {
    public:
      boost::endian::little_uint64_t session_id;
    };

    class little_close_session_response
    {
    public:
      uint8_t dummy;
    };

    class little_linearizable_command_request
    {
    public:
      boost::endian::little_uint64_t session_id;
      boost::endian::little_uint64_t first_unacknowledged_sequence_number;
      boost::endian::little_uint64_t sequence_number;
    };

    class little_log_entry_command
    {
    public:
      uint8_t type;
    };

    class little_checkpoint_header
    {
    public:
      boost::endian::little_uint64_t log_entry_index_end;
      boost::endian::little_uint64_t last_log_entry_term;
      boost::endian::little_uint64_t last_log_entry_cluster_time;
      boost::endian::little_uint64_t configuration_index;
    };

    class little_log_entry
    {
    public:
      boost::endian::little_uint64_t term;
      boost::endian::little_uint64_t cluster_time;
      uint8_t type;
    };

    class log_entry_command_view_deserialization
    {
    private:
      raft::native::log_entry_command cmd_;
    public:
      log_entry_command_view_deserialization(messages::log_entry_traits_type::const_arg_type le);
      log_entry_command_view_deserialization(log_entry_command_view_deserialization &&) = default;
      log_entry_command_view_deserialization(const log_entry_command_view_deserialization &) = delete;
      log_entry_command_view_deserialization & operator=(log_entry_command_view_deserialization &&) = default;
      log_entry_command_view_deserialization & operator=(const log_entry_command_view_deserialization &) = delete;
      log_entry_command_traits::const_arg_type view() const
      {
        return &cmd_;
      }
    };

    struct serialization
    {
      typedef log_entry_command_view_deserialization log_entry_command_view_deserialization_type;

      static std::size_t serialize_helper(const std::string& str)
      {
	return sizeof(boost::endian::little_uint32_t) + str.size();
      }

      static std::size_t serialize_helper(const raft::native::server_description& desc)
      {
	return sizeof(boost::endian::little_uint64_t) + serialize_helper(desc.address);
      }

      template<typename _T>
      static std::size_t serialize_helper(const std::vector<_T>& vec)
      {
	std::size_t sz = sizeof(boost::endian::little_uint32_t);
	for (auto & t : vec) {
	  sz += serialization::serialize_helper(t);
	}
	return sz;
      }

      static std::size_t serialize_helper(const std::vector<uint8_t>& vec)
      {
	return sizeof(boost::endian::little_uint32_t) + vec.size();
      }

      static std::size_t serialize_helper(const raft::native::simple_configuration_description& desc)
      {
	return serialization::serialize_helper(desc.servers);
      }

      static std::size_t serialize_helper(const raft::native::configuration_description& desc)
      {
	return serialize_helper(desc.from) + serialize_helper(desc.to);
      }
      
      static std::size_t serialize_helper(const raft::native::log_entry<raft::native::configuration_description>& entry)
      {
	return sizeof(little_log_entry) + serialize_helper(entry.data) + serialize_helper(entry.configuration);
      }

      static std::size_t serialize_helper(const raft::native::open_session_request & msg)
      {
        return sizeof(little_open_session_request);
      }

      static std::size_t serialize_helper(const raft::native::close_session_request & msg)
      {
        return sizeof(little_close_session_request);
      }
      
      static std::size_t serialize_helper(const raft::native::linearizable_command_request & msg)
      {
	return sizeof(little_linearizable_command_request) + serialize_helper(msg.command);
      }

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
	log_buf->cluster_time = entry.cluster_time;
	std::size_t sz = sizeof(little_log_entry);
	sz += serialization::serialize_helper(b+sz, entry.data);
	sz += serialization::serialize_helper(b+sz, entry.configuration);
	return sz;
      }

      static std::size_t serialize_helper(raft::mutable_slice && b, const raft::native::open_session_request & msg)
      {
	little_open_session_request * buf = reinterpret_cast<little_open_session_request *>(b.data());
        buf->dummy = 0xef;
        return sizeof(little_open_session_request);
      }

      static std::size_t serialize_helper(raft::mutable_slice && b, const raft::native::close_session_request & msg)
      {
	auto * buf = reinterpret_cast<little_close_session_request *>(b.data());
	buf->session_id = msg.session_id;
        return sizeof(little_close_session_request);
      }
      
      static std::size_t serialize_helper(raft::mutable_slice && b, const raft::native::linearizable_command_request & msg)
      {
	auto * buf = reinterpret_cast<little_linearizable_command_request *>(b.data());
	buf->session_id = msg.session_id;
	buf->first_unacknowledged_sequence_number = msg.first_unacknowledged_sequence_number;
	buf->sequence_number = msg.sequence_number;
	std::size_t sz = sizeof(little_linearizable_command_request);
	sz += serialize_helper(b+sz, msg.command);
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
	case 4:
	  return SESSION_EXPIRED;
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
	entry.cluster_time = log_buf->cluster_time;
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

      static std::size_t deserialize_helper(raft::slice && b, raft::native::messages::open_session_request_type & msg)
      {
	BOOST_ASSERT(b.size() >= sizeof(little_open_session_request));
	auto buf = reinterpret_cast<const little_open_session_request *>(b.data());
        BOOST_ASSERT(buf->dummy == 0xef);
        return sizeof(little_open_session_request);
      }
    
      static std::size_t deserialize_helper(raft::slice && b, raft::native::messages::close_session_request_type & msg)
      {
	BOOST_ASSERT(b.size() >= sizeof(little_close_session_request));
	auto buf = reinterpret_cast<const little_close_session_request *>(b.data());
	msg.session_id = buf->session_id;
        return sizeof(little_close_session_request);
      }
    
      static std::size_t deserialize_helper(raft::slice && b, raft::native::messages::linearizable_command_request_type & msg)
      {
	BOOST_ASSERT(b.size() >= sizeof(little_linearizable_command_request));
	auto buf = reinterpret_cast<const little_linearizable_command_request *>(b.data());
	msg.session_id = buf->session_id;
	msg.first_unacknowledged_sequence_number = buf->first_unacknowledged_sequence_number;
	msg.sequence_number = buf->sequence_number;
	std::size_t sz = sizeof(little_linearizable_command_request);
	sz += deserialize_helper(b+sz, msg.command);
	return sz;
      }
    
      static std::size_t deserialize_helper(raft::slice && b, raft::native::messages::log_entry_command_type & cmd)
      {
	BOOST_ASSERT(b.size() >= sizeof(little_log_entry_command));
	const little_log_entry_command * log_buf = reinterpret_cast<const little_log_entry_command *>(b.data());
	std::size_t sz = sizeof(little_log_entry_command);
	switch(log_buf->type) {
	case 0:
	  cmd.type = raft::native::log_entry_command::OPEN_SESSION;
          sz += serialization::deserialize_helper(b+sz, cmd.open_session);
	  break;
	case 1:
	  cmd.type = raft::native::log_entry_command::CLOSE_SESSION;
          sz += serialization::deserialize_helper(b+sz, cmd.close_session);
	  break;
	case 2:
	  cmd.type = raft::native::log_entry_command::LINEARIZABLE_COMMAND;
          sz += serialization::deserialize_helper(b+sz, cmd.command);
	  break;
	default:
	  // TODO: error handling
	  break;
	}
	return sz;
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(const raft::native::log_entry<raft::native::configuration_description>& entry);
	
      static raft::native::messages::vote_request_type deserialize_vote_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::vote_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_vote_request));
	const little_vote_request * buf = reinterpret_cast<const little_vote_request *>(b.first.data());
	msg.set_request_id(buf->request_id);
	msg.set_recipient_id(buf->recipient_id);
	msg.set_term_number(buf->term_number);
	msg.set_candidate_id(buf->candidate_id);
	msg.set_log_index_end(buf->log_index_end);
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
	msg.request_id = buf->request_id;
	msg.granted = (buf->granted != 0);
	return msg;
      }

      static raft::native::messages::append_entry_request_type deserialize_append_entry_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::append_entry_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_append_entry_request));
	const little_append_entry_request * buf = reinterpret_cast<const little_append_entry_request *>(b.first.data());
	msg.request_id = buf->request_id;
	msg.recipient_id = buf->recipient_id;
	msg.term_number = buf->term_number;
	msg.leader_id = buf->leader_id;
	msg.log_index_begin = buf->log_index_begin ;
	msg.previous_log_term = buf->previous_log_term;
	msg.leader_commit_index_end = buf->leader_commit_index_end;
	std::size_t sz = sizeof(little_append_entry_request);
	sz += serialization::deserialize_helper(b.first+sz, msg.entry);
	return msg;
      }

      static raft::native::messages::append_entry_response_type deserialize_append_entry_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::append_entry_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_append_entry_response));
	const little_append_entry_response * buf = reinterpret_cast<const little_append_entry_response *>(b.first.data());
	msg.recipient_id = buf->recipient_id;
	msg.term_number = buf->term_number;
	msg.request_term_number = buf->request_term_number;
	msg.request_id = buf->request_id;
	msg.index_begin = buf->index_begin;
	msg.index_end = buf->index_end;
	msg.success = buf->success ? 1 : 0;
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
	std::size_t sz = sizeof(little_client_response);
	sz += deserialize_helper(b.first+sz, msg.response);
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
    
      static raft::native::messages::get_configuration_request_type deserialize_get_configuration_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::get_configuration_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_get_configuration_request));
	auto buf = reinterpret_cast<const little_get_configuration_request *>(b.first.data());
        BOOST_ASSERT(buf->dummy == 0xef);
	return msg;
      }
    
      static raft::native::messages::get_configuration_response_type deserialize_get_configuration_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::get_configuration_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_get_configuration_response));
	auto buf = reinterpret_cast<const little_get_configuration_response *>(b.first.data());
	msg.result = convert_client_result(buf->result);
	msg.id = buf->id;
	std::size_t sz = sizeof(little_get_configuration_response);
	sz += deserialize_helper(b.first+sz, msg.configuration);
	return msg;
      }
    
      static raft::native::messages::append_checkpoint_chunk_request_type deserialize_append_checkpoint_chunk_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::append_checkpoint_chunk_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_append_checkpoint_chunk_request));
	auto buf = reinterpret_cast<const little_append_checkpoint_chunk_request *>(b.first.data());
	msg.request_id = buf->request_id;
	msg.recipient_id = buf->recipient_id;
	msg.term_number= buf->term_number;
	msg.leader_id = buf->leader_id;
	msg.last_checkpoint_header.log_entry_index_end = buf->log_entry_index_end;
	msg.last_checkpoint_header.last_log_entry_term = buf->last_log_entry_term;
	msg.last_checkpoint_header.last_log_entry_cluster_time = buf->last_log_entry_cluster_time;
	msg.last_checkpoint_header.configuration.index = buf->configuration_index;
	msg.checkpoint_begin = buf->checkpoint_begin;
	msg.checkpoint_end = buf->checkpoint_end;
	msg.checkpoint_done = buf->checkpoint_done != 0;
	std::size_t sz = sizeof(little_append_checkpoint_chunk_request);
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
	msg.request_id = buf->request_id;
	msg.bytes_stored = buf->bytes_stored;
	return msg;
      }
    
      static raft::native::messages::open_session_request_type deserialize_open_session_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::open_session_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_open_session_request));
	auto buf = reinterpret_cast<const little_open_session_request *>(b.first.data());
        BOOST_ASSERT(buf->dummy == 0xef);
	return msg;
      }
    
      static raft::native::messages::open_session_response_type deserialize_open_session_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::open_session_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_open_session_response));
	auto buf = reinterpret_cast<const little_open_session_response *>(b.first.data());
	msg.session_id = buf->session_id;
	return msg;
      }
    
      static raft::native::messages::close_session_request_type deserialize_close_session_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::close_session_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_close_session_request));
	auto buf = reinterpret_cast<const little_close_session_request *>(b.first.data());
	msg.session_id = buf->session_id;
	return msg;
      }
    
      static raft::native::messages::close_session_response_type deserialize_close_session_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::close_session_response_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_close_session_response));
	auto buf = reinterpret_cast<const little_close_session_response *>(b.first.data());
        BOOST_ASSERT(buf->dummy == 0xfa);
	return msg;
      }
    
      static raft::native::messages::linearizable_command_request_type deserialize_linearizable_command_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	raft::native::messages::linearizable_command_request_type msg;
	BOOST_ASSERT(b.first.size() >= sizeof(little_linearizable_command_request));
	auto buf = reinterpret_cast<const little_linearizable_command_request *>(b.first.data());
	msg.session_id = buf->session_id;
	msg.first_unacknowledged_sequence_number = buf->first_unacknowledged_sequence_number;
	msg.sequence_number = buf->sequence_number;
	std::size_t sz = sizeof(little_linearizable_command_request);
	sz += deserialize_helper(b.first+sz, msg.command);
	return msg;
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::vote_request_type && msg)
      {
	auto buf = new little_vote_request();
	buf->request_id = raft::native::messages::vote_request_traits_type::request_id(msg);
	buf->recipient_id = raft::native::messages::vote_request_traits_type::recipient_id(msg);
	buf->term_number = raft::native::messages::vote_request_traits_type::term_number(msg);
	buf->candidate_id = raft::native::messages::vote_request_traits_type::candidate_id(msg);
	buf->log_index_end = raft::native::messages::vote_request_traits_type::log_index_end(msg);
	buf->last_log_term = raft::native::messages::vote_request_traits_type::last_log_term(msg);
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_vote_request)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::vote_response_type && msg)
      {
	auto buf = new little_vote_response();
	buf->peer_id = msg.peer_id;
	buf->term_number = msg.term_number;
	buf->request_term_number = msg.request_term_number;
	buf->request_id = msg.request_id;
	buf->granted = msg.granted ? 1 : 0;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_vote_response)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::append_entry_request_type && msg)
      {
        auto to_alloc = sizeof(little_append_entry_request) + serialize_helper(msg.entry);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_append_entry_request *>(b.data());
	buf->request_id = msg.request_id;
	buf->recipient_id = msg.recipient_id;
	buf->term_number = msg.term_number;
	buf->leader_id = msg.leader_id;
	buf->log_index_begin = msg.log_index_begin;
	buf->previous_log_term = msg.previous_log_term;
	buf->leader_commit_index_end = msg.leader_commit_index_end;
	auto sz = sizeof(little_append_entry_request);
	sz += serialize_helper(b+sz, msg.entry);      
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::append_entry_response_type && msg)
      {
	auto buf = new little_append_entry_response();
	buf->recipient_id = msg.recipient_id;
	buf->term_number = msg.term_number;
	buf->request_term_number = msg.request_term_number;
	buf->request_id  = msg.request_id;
	buf->index_begin = msg.index_begin;
	buf->index_end = msg.index_end;
	buf->success = (msg.success != 0);
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_append_entry_response)),
								  [buf]() { delete buf; });
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::client_response_type && msg)
      {
        auto to_alloc = sizeof(little_client_response) + serialize_helper(msg.response);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_client_response *>(b.data());
	buf->result = msg.result;
	buf->index = msg.index;
	buf->leader_id = msg.leader_id;
	auto sz = sizeof(little_client_response);
	sz += serialize_helper(b+sz, msg.response);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::set_configuration_request_type && msg)
      {
        auto to_alloc = sizeof(little_set_configuration_request) + serialize_helper(msg.new_configuration);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_set_configuration_request *>(b.data());
	buf->old_id = msg.old_id;
	auto sz = sizeof(little_set_configuration_request);
	sz += serialize_helper(b+sz, msg.new_configuration);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::set_configuration_response_type && msg)
      {
        auto to_alloc = sizeof(little_set_configuration_response) + serialize_helper(msg.bad_servers);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_set_configuration_response *>(b.data());
	buf->result = msg.result;
	auto sz = sizeof(little_set_configuration_response);
	sz += serialize_helper(b+sz, msg.bad_servers);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::get_configuration_request_type && msg)
      {
        auto to_alloc = sizeof(little_get_configuration_request);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_get_configuration_request *>(b.data());
        buf->dummy = 0xef;
	auto sz = sizeof(little_get_configuration_request);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::get_configuration_response_type && msg)
      {
        auto to_alloc = sizeof(little_get_configuration_response) + serialize_helper(msg.configuration);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_get_configuration_response *>(b.data());
	buf->result = msg.result;
	buf->id = msg.id;
	auto sz = sizeof(little_get_configuration_response);
	sz += serialize_helper(b+sz, msg.configuration);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }
    
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::append_checkpoint_chunk_request_type && msg)
      {
        auto to_alloc = sizeof(little_append_checkpoint_chunk_request) + serialize_helper(msg.last_checkpoint_header.configuration.description) + serialize_helper(msg.data);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_append_checkpoint_chunk_request *>(b.data());
	buf->request_id = msg.request_id;
	buf->recipient_id = msg.recipient_id;
	buf->term_number= msg.term_number;
	buf->leader_id = msg.leader_id;
	buf->log_entry_index_end = msg.last_checkpoint_header.log_entry_index_end;
	buf->last_log_entry_term = msg.last_checkpoint_header.last_log_entry_term;
	buf->last_log_entry_cluster_time = msg.last_checkpoint_header.last_log_entry_cluster_time;
	buf->configuration_index = msg.last_checkpoint_header.configuration.index;
	buf->checkpoint_begin = msg.checkpoint_begin;
	buf->checkpoint_end = msg.checkpoint_end;
	buf->checkpoint_done = msg.checkpoint_done ? 1 : 0;
	std::size_t sz = sizeof(little_append_checkpoint_chunk_request);
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
	buf->request_id = msg.request_id;
	buf->bytes_stored = msg.bytes_stored;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_append_checkpoint_chunk_response)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::open_session_request_type && msg)
      {
	auto buf = new little_open_session_request();
        buf->dummy = 0xef;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_open_session_request)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::open_session_response_type && msg)
      {
	auto buf = new little_open_session_response();
	buf->session_id = msg.session_id;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_open_session_response)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::close_session_request_type && msg)
      {
	auto buf = new little_close_session_request();
	buf->session_id = msg.session_id;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_close_session_request)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::close_session_response_type && msg)
      {
	auto buf = new little_close_session_response();
        buf->dummy = 0xfa;
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(reinterpret_cast<const uint8_t *>(buf), sizeof(little_close_session_response)),
								  [buf]() { delete buf; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(raft::native::messages::linearizable_command_request_type && msg)
      {
        auto to_alloc = serialize_helper(msg);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
        auto sz = serialize_helper(b+0, msg);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize_log_entry_command(raft::native::messages::open_session_request_type && msg)
      {
        auto to_alloc = sizeof(little_log_entry_command) + serialize_helper(msg);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_log_entry_command *>(b.data());
        buf->type = raft::native::log_entry_command::OPEN_SESSION;
        std::size_t sz = sizeof(little_log_entry_command);
        sz += serialize_helper(b+sz, msg);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize_log_entry_command(raft::native::messages::close_session_request_type && msg)
      {
        auto to_alloc = sizeof(little_log_entry_command) + serialize_helper(msg);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_log_entry_command *>(b.data());
        buf->type = raft::native::log_entry_command::CLOSE_SESSION;
        std::size_t sz = sizeof(little_log_entry_command);
        sz += serialize_helper(b+sz, msg);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize_log_entry_command(raft::native::messages::linearizable_command_request_type && msg)
      {
        auto to_alloc = sizeof(little_log_entry_command) + serialize_helper(msg);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_log_entry_command *>(b.data());
        buf->type = raft::native::log_entry_command::LINEARIZABLE_COMMAND;
        std::size_t sz = sizeof(little_log_entry_command);
        sz += serialize_helper(b+sz, msg);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize_checkpoint_header(const raft::native::checkpoint_header & header)
      {
        auto to_alloc = sizeof(little_checkpoint_header) + serialize_helper(header.configuration.description);
	raft::mutable_slice b(new uint8_t [to_alloc], to_alloc);
	auto * buf = reinterpret_cast<little_checkpoint_header*>(b.data());
	buf->log_entry_index_end = header.log_entry_index_end;
	buf->last_log_entry_term = header.last_log_entry_term;
	buf->last_log_entry_cluster_time = header.last_log_entry_cluster_time;
	buf->configuration_index = header.configuration.index;
	std::size_t sz = sizeof(little_checkpoint_header);
	sz += serialize_helper(b+sz, header.configuration.description);
	auto ptr = reinterpret_cast<const uint8_t *>(b.data());
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }

      static raft::native::checkpoint_header deserialize_checkpoint_header(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
        raft::native::checkpoint_header header;
	BOOST_ASSERT(b.first.size() >= sizeof(little_checkpoint_header));
	auto buf = reinterpret_cast<const little_checkpoint_header *>(b.first.data());
	header.log_entry_index_end = buf->log_entry_index_end;
	header.last_log_entry_term = buf->last_log_entry_term;
	header.last_log_entry_cluster_time = buf->last_log_entry_cluster_time;
	header.configuration.index = buf->configuration_index;
	std::size_t sz = sizeof(little_checkpoint_header);
	sz += deserialize_helper(b.first+sz, header.configuration.description);
        return header;
      }
    };
    
    std::pair<raft::slice, raft::util::call_on_delete> serialization::serialize(const raft::native::log_entry<raft::native::configuration_description>& entry)
    {
      auto to_alloc = serialize_helper(entry);
      auto ptr = new uint8_t [to_alloc];
      std::size_t sz = serialize_helper(raft::mutable_slice(ptr, to_alloc), entry);
      return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
    }

    inline log_entry_command_view_deserialization::log_entry_command_view_deserialization(messages::log_entry_traits_type::const_arg_type le)
    {
      BOOST_ASSERT(messages::log_entry_traits_type::is_command(le));
      auto s = messages::log_entry_traits_type::data(le);
      serialization::deserialize_helper(std::move(s), cmd_);
    }
  }
}

#endif
