#ifndef __RAFTCHECKPOINT_HH__
#define __RAFTCHECKPOINT_HH__

#include <chrono>
#include <set>
#include <string>
#include <vector>

#include <boost/assert.hpp>

#include "util/call_on_delete.hh"

namespace raft {

  class checkpoint_block
  {
  public:
    const uint8_t * block_data_;
    std::size_t block_length_;

    checkpoint_block()
      :
      block_data_(nullptr),
      block_length_(0)
    {
    }

    checkpoint_block(const uint8_t * block_data, std::size_t block_length)
      :
      block_data_(block_data),
      block_length_(block_length)
    {
    }

    const void * data() const
    {
      return block_data_;
    }

    size_t size() const
    {
      return block_length_;
    }

    bool is_null() const {
      return block_data_ == nullptr;
    }
  };

  // TODO: What abstractions are needed for representation of checkpoints.
  // For example, for a real system this is likely to be on disk (at least somewhere "reliable")
  // but is it a dedicated file, is it just a bunch of blocks scattered throughout a file or something else entirely?
  // Right now I'm representing a checkpoint as a list of blocks with an implementation as an
  // array of data (could be a linked list of stuff as well).
  // TODO: This block stuff is half baked because it isn't consistent with the ack'ing protocol that is expressed
  // in terms of byte offsets; it works but it's goofy.
  template<typename _Messages>
  class checkpoint_data
  {
  public:
    typedef typename _Messages::checkpoint_header_type header_type;
  private:
    // We have to use a generic deleter for the header because the header
    // may or may not be embedded in a larger chunk of memory.
    // 1) If we initialize the checkpoint then the header is created directly
    // 2) If the checkpoint is sent to us then it is embedded in an append_checkpoint_chunk message
    const header_type * header_;
    raft::util::call_on_delete header_deleter_;
    std::vector<uint8_t> data_;
    std::size_t block_size_;
  public:
    checkpoint_data(const header_type * header, raft::util::call_on_delete && deleter, std::size_t block_size)
      :
      header_(header),
      header_deleter_(std::move(deleter)),
      block_size_(block_size)
    {
    }

    std::size_t block_size() const
    {
      return block_size_;
    }

    const header_type & header() const
    {
      return *header_;
    }

    checkpoint_block block_at_offset(uint64_t offset) const {
      if (offset >= data_.size()) {
	return checkpoint_block();	
      }
      
      std::size_t next_block_start = offset;
      std::size_t next_block_end = (std::min)(next_block_start+block_size_, data_.size());
      std::size_t next_block_size = next_block_end - next_block_start;
      return checkpoint_block(&data_[next_block_start], next_block_size);
    }
    
    checkpoint_block next_block(const checkpoint_block & current_block) {
      if (current_block.is_null()) {
	return checkpoint_block(&data_[0], (std::min)(block_size_, data_.size()));
      } else if (!is_final(current_block)) {
	std::size_t next_block_start = (current_block.block_data_ - &data_[0]) + current_block.block_length_;
	std::size_t next_block_end = (std::min)(next_block_start+block_size_, data_.size());
	std::size_t next_block_size = next_block_end - next_block_start;
	return checkpoint_block(&data_[next_block_start], next_block_size);
      } else {
	return checkpoint_block();
      }
    }

    uint64_t block_begin(const checkpoint_block & current_block) const {
      return current_block.block_data_ - &data_[0];
    }

    uint64_t block_end(const checkpoint_block & current_block) const {
      return current_block.block_length_ + block_begin(current_block);
    }

    bool is_final(const checkpoint_block & current_block) {
      return !current_block.is_null() &&
	(current_block.block_data_ + current_block.block_length_) == &data_[data_.size()];
    }

    void write(const uint8_t * data, std::size_t len)
    {
      for(std::size_t i=0; i<len; ++i) {
	data_.push_back(data[i]);
      }
    }
  };


  // Checkpoints live here
  template<typename _Messages>
  class checkpoint_data_store
  {
  public:
    typedef checkpoint_data<_Messages> checkpoint_data_type;
    typedef std::shared_ptr<checkpoint_data_type> checkpoint_data_ptr;
    typedef typename _Messages::checkpoint_header_type header_type;
    typedef typename _Messages::checkpoint_header_traits_type header_traits_type;
    typedef checkpoint_block block_type;
    typedef typename _Messages::configuration_checkpoint_type configuration_type;
  private:
    checkpoint_data_ptr last_checkpoint_;
    std::size_t block_size_ = 1024*1024;
  public:
    std::size_t block_size() const
    {
      return block_size_;
    }
    void block_size(std::size_t val)
    {
      block_size_ = val;
    }
    checkpoint_data_ptr create(const header_type * header, raft::util::call_on_delete && deleter) const
    {
      return checkpoint_data_ptr(new checkpoint_data_type(header, std::move(deleter), block_size_));
    }
    checkpoint_data_ptr create(std::pair<const header_type *, raft::util::call_on_delete> && header) const
    {
      return checkpoint_data_ptr(new checkpoint_data_type(header.first, std::move(header.second), block_size_));
    }
    void commit(checkpoint_data_ptr f)
    {
      last_checkpoint_ = f;
    }
    void discard(checkpoint_data_ptr f)
    {
      // TODO: Perform any necessary cleanup (e.g. on disk data)
    }
    checkpoint_data_ptr last_checkpoint() {
      return last_checkpoint_;
    }
  };
}

#endif
