#ifndef __RAFTPEER_HH__
#define __RAFTPEER_HH__

#include <chrono>
#include <vector>

#include "boost/logic/tribool.hpp"

#include "messages.hh"

namespace raft {
  // Raft only really cares about the fact that a checkpoint maintains the log index, term and configuration
  // at which the checkpoint is taken.  What client data and how that client data is communicated
  // to peers and disk is more or less opaque.  I want to structure things so that these latter
  // aspects are decoupled.
  template<typename checkpoint_data_store_type>
  class peer_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
    typedef typename checkpoint_data_store_type::block_type block_type;
    typedef typename checkpoint_data_store_type::configuration_type configuration_type;
    typedef append_checkpoint_chunk<checkpoint_data_store_type> append_checkpoint_chunk_type;
    
    // One past last byte to written in checkpoint file.
    uint64_t checkpoint_next_byte_;
    // The last log entry that the checkpoint covers.  Need to resume
    // sending log entries to peer from this point.
    uint64_t checkpoint_last_log_entry_index_;
    // The term of the last log entry that the checkpoint covers.  
    uint64_t checkpoint_last_log_entry_term_;
    // Configuration in effect at the time the checkpoint occured
    configuration_type checkpoint_last_configuration_;
    // Checkpoint data we are sending to this peer
    checkpoint_data_ptr data_;
    // Last block sent.  We do not assume that the checkpoint bytes are contiguous in memory
    // so cannot use checkpoint_next_byte_ to know where the next chunk is in data_.
    block_type last_block_sent_;
    // Has the last block been acked?  TODO: Generalize to a window/credit system?
    bool awaiting_ack_;

    peer_checkpoint(uint64_t checkpoint_last_log_entry_index, uint64_t checkpoint_last_log_entry_term,
		    const configuration_type & config, checkpoint_data_ptr data)
      :
      checkpoint_next_byte_(0),
      checkpoint_last_log_entry_index_(checkpoint_last_log_entry_index),
      checkpoint_last_log_entry_term_(checkpoint_last_log_entry_term),
      checkpoint_last_configuration_(config),
      data_(data),
      awaiting_ack_(false)
    {
    }

    bool prepare_checkpoint_chunk(append_checkpoint_chunk_type & msg)
    {
      if (awaiting_ack_) {
	return false;
      }

      if (data_->is_final(last_block_sent_)) {
	return false;
      }

      last_block_sent_ = data_->block_at_offset(checkpoint_next_byte_);

      msg.last_checkpoint_index = checkpoint_last_log_entry_index_;
      msg.last_checkpoint_term = checkpoint_last_log_entry_term_;
      msg.last_checkpoint_configuration = checkpoint_last_configuration_;
      msg.checkpoint_begin = data_->block_begin(last_block_sent_);
      msg.checkpoint_end = data_->block_end(last_block_sent_);
      msg.checkpoint_done = data_->is_final(last_block_sent_);
      msg.data.assign(last_block_sent_.block_data_,
		      last_block_sent_.block_data_+last_block_sent_.block_length_);
      awaiting_ack_ = true;
      return true;
    }
  };

  // A peer encapsulates what a server knows about other servers in the cluster
  template<typename checkpoint_data_store_type, typename configuration_change_type>
  class peer
  {
  public:
    typedef typename checkpoint_data_store_type::configuration_type::address_type address_type;

  public:
    // peer id = same as index in peer array
    uint64_t peer_id;
    // TODO: Templatize; do we actually want multiple addresses?  Does protocol really
    // need to know whether this is one or more addresses?
    address_type address;
    // Leader specific state about peers
    // Index of next log entry to send
    uint64_t next_index_;
    // One past the index of last log entry known to be replicated to peer
    uint64_t match_index_;
    // Vote we got from this peer.  boost::logic::indeterminate means we haven't heard back yet.
    boost::logic::tribool vote_;
    // Used only when LEADER; when does this peer need another heartbeat?
    std::chrono::time_point<std::chrono::steady_clock> requires_heartbeat_;
    // Is the value of next_index_ a guess or has it been confirmed by communication with the peer
    bool is_next_index_reliable_;
    // TODO: Implement the exit protocol
    bool exiting_;
    // Checkpoint state for sending a checkpoint from a server to this peer
    std::shared_ptr<peer_checkpoint<checkpoint_data_store_type> > checkpoint_;
    // State for tracking whether a peer that is newly added to a configuration tracking
    // log appends
    std::shared_ptr<configuration_change_type> configuration_change_;

    void exit()
    {
      exiting_ = true;
    }
  };

  // Peer side of a checkpoint transfer
  template<typename checkpoint_data_store_type>
  class in_progress_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::header_type header_type;
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
  public:
    uint64_t end_;
    checkpoint_data_ptr file_;
    
    uint64_t end() const {
      return end_;
    }

    void write(const std::vector<uint8_t> & data)
    {
      // TODO: Support async here
      file_->write(&data[0], data.size());
      end_ += data.size();
    }

    in_progress_checkpoint(checkpoint_data_store_type & store,
			   const header_type & header)
      :
      end_(0),
      file_(store.create(header))
    {
    }
  };
}

#endif
