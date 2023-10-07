#ifndef __RAFTPEER_HH__
#define __RAFTPEER_HH__

#include <chrono>
#include <vector>

#include "boost/logic/tribool.hpp"

#include "slice.hh"

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
    typedef typename checkpoint_data_store_type::header_type header_type;
    
    // One past last byte to written in checkpoint file.
    uint64_t checkpoint_next_byte_;
    // Header of the checkpoint
    const header_type & checkpoint_last_header_;
    // Checkpoint data we are sending to this peer
    checkpoint_data_ptr data_;
    // Last block sent.  We do not assume that the checkpoint bytes are contiguous in memory
    // so cannot use checkpoint_next_byte_ to know where the next chunk is in data_.
    block_type last_block_sent_;
    // Has the last block been acked?  TODO: Generalize to a window/credit system?
    bool awaiting_ack_;

    peer_checkpoint(const header_type & header, checkpoint_data_ptr data)
      :
      checkpoint_next_byte_(0),
      checkpoint_last_header_(header),
      data_(data),
      awaiting_ack_(false)
    {
    }
  };

  // A peer encapsulates what a server knows about other servers in the cluster
  template<typename checkpoint_data_store_type, typename configuration_change_type>
  class peer
  {
  public:
    // peer id = same as index in peer array
    uint64_t peer_id;
    // TODO: Templatize; do we actually want multiple addresses?  Does protocol really
    // need to know whether this is one or more addresses?
    std::string address;
    // Leader specific state about peers
    // Index of next log entry to send
    uint64_t next_index_;
    // One past the index of last log entry known to be replicated to peer
    uint64_t match_index_;
    // The last request_id this peer has acknowledged
    uint64_t last_request_id_;
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

    void acknowledge_request_id(uint64_t request_id)
    {
      // TODO: Should we allow request id to go backwards?
      last_request_id_ = request_id;
    }

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

    void write(slice && data)
    {
      // TODO: Support async here
      file_->write(slice::buffer_cast<const uint8_t *>(data), slice::buffer_size(data));
      end_ += slice::buffer_size(data);
    }

    in_progress_checkpoint(checkpoint_data_store_type & store,
			   const header_type * header,
			   raft::util::call_on_delete && deleter)
      :
      end_(0),
      file_(store.create(header, std::move(deleter)))
    {
    }
  };
}

#endif
