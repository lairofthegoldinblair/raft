#ifndef __RAFT_FLATBUFFERS_SERIALIZATION__
#define __RAFT_FLATBUFFERS_SERIALIZATION__

#include "raft_flatbuffer_messages.hh"

namespace raft {
  namespace fbs {
    struct serialization
    {
      static int32_t classify(std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> && entry)
      {
	return entry.first->message_type() - 1;
      }
      static std::pair<raft::slice, raft::util::call_on_delete> serialize(std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> && entry)
      {
	auto buf = ::flatbuffers::GetBufferStartFromRootPointer(entry.first);
	buf -= sizeof(::flatbuffers::uoffset_t);
	return std::make_pair(raft::slice(buf, ::flatbuffers::GetPrefixedSize(buf)+sizeof(::flatbuffers::uoffset_t)), std::move(entry.second));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	auto obj = ::raft::fbs::GetSizePrefixedraft_message(b.first.data());
	BOOST_ASSERT(b.first.size() >= ::flatbuffers::GetPrefixedSize(reinterpret_cast<const uint8_t *>(obj))+sizeof(::flatbuffers::uoffset_t));
	return std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete>(obj, [s = std::move(b)]() {});
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_request_vote(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_vote_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_append_entry(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_append_entry_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      };    
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_client_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_client_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_set_configuration_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_set_configuration_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_append_checkpoint_chunk(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_append_checkpoint_chunk_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_open_session_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_open_session_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_close_session_request(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_close_session_response(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }
      static std::pair<const raft::fbs::raft_message * , raft::util::call_on_delete> deserialize_linearizable_command(std::pair<raft::slice, raft::util::call_on_delete> && b)
      {
	return deserialize(std::move(b));
      }

      static std::pair<raft::slice, raft::util::call_on_delete> serialize(const uint8_t & entry)
      {
	// TODO: Really want to eliminate the copy here, but that requires pinning the buffer into memory
	std::size_t sz = ::flatbuffers::GetPrefixedSize(&entry)+sizeof(::flatbuffers::uoffset_t);
	auto ptr = new uint8_t [sz];
	::memcpy(ptr, &entry, sz);
	return std::pair<raft::slice, raft::util::call_on_delete>(raft::slice(ptr, sz), [ptr]() { delete [] ptr; });
      }
    };
  }
}

#endif
