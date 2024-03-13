#ifndef __RAFT_CIRCULAR_BUFFER_HH__
#define __RAFT_CIRCULAR_BUFFER_HH__

#include <array>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <boost/circular_buffer.hpp>
#include <boost/endian/arithmetic.hpp>

#include "slice.hh"
#include "util/call_on_delete.hh"

// This is the state machine.                                                                                                                                                                                         
template<typename _Messages, typename _Serialization, typename _Checkpoint>
struct logger
{
  typedef _Messages messages_type;
  typedef _Serialization serialization_type;
  typedef typename messages_type::linearizable_command_request_traits_type linearizable_command_request_traits_type;
  typedef typename messages_type::log_entry_command_traits_type log_entry_command_traits_type;
  typedef typename serialization_type::log_entry_command_view_deserialization_type log_entry_command_view_deserialization_type;
  typedef typename _Checkpoint::template apply<messages_type>::type checkpoint_data_store_type;
  typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
  struct continuation
  {
    log_entry_command_view_deserialization_type cmd;
    std::function<void(bool, std::pair<raft::slice, raft::util::call_on_delete> &&)> callback;

    template<typename _Callback>
    continuation(log_entry_command_view_deserialization_type && _cmd, _Callback && cb)
      :
      cmd(std::move(_cmd)),
      callback(std::move(cb))
    {
    }
  };

  logger()
    :
    commands(1000000),
    current_buffer_(0),
    checkpoint_buffer_states_({ buffer_state::FREE, buffer_state::FREE })
  {
  }
  
  boost::circular_buffer<std::string> commands;
  std::unique_ptr<continuation> cont;
  enum class checkpoint_state { START, WRITE_COMMAND_SIZE, WRITE_COMMAND, PENULTIMATE_WRITE, FINAL_WRITE };
  checkpoint_state checkpoint_state_;
  boost::circular_buffer<std::string>::const_iterator commands_it_;
  std::vector<uint8_t> checkpoint_buffer_;
  enum class buffer_state { FREE, POPULATING, IN_TRANSIT };
  std::size_t current_buffer_;
  std::array<buffer_state, 2> checkpoint_buffer_states_;
  std::array<std::vector<uint8_t>, 2> checkpoint_buffers_;
  enum checkpoint_restore_state { START, READ_COMMANDS_SIZE, READ_COMMAND_SIZE, READ_COMMAND };
  checkpoint_restore_state state_;
  raft::slice checkpoint_slice_;
  std::size_t checkpoint_commands_size_;
  std::size_t checkpoint_command_size_;
  bool async = false;
  void complete()
  {
    if (!cont) {
      return;
    }
    ;
    auto c = linearizable_command_request_traits_type::command(log_entry_command_traits_type::linearizable_command(cont->cmd.view()));
    commands.push_back(std::string(reinterpret_cast<const char *>(c.data()), c.size()));
    // Must reset cont before the callback because the callback may add a new async command                                                                                                                           
    // from the log.   If we reset it after that then we'll lose a async call.                                                                                                                                        
    auto tmp = std::move(cont);
    cont.reset();
    tmp->callback(true, std::make_pair(raft::slice(), raft::util::call_on_delete()));
  }
  template<typename _Callback>
  void on_command(log_entry_command_view_deserialization_type && cmd, _Callback && cb)
  {
    cont = std::make_unique<continuation>(std::move(cmd), std::move(cb));
    if (!async) {
      complete();
    }
  }
  void start_checkpoint()
  {
    checkpoint_state_ = checkpoint_state::START;
  }

  void complete_checkpoint(std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    checkpoint(clock_now, checkpoint_data_ptr(), [](std::chrono::time_point<std::chrono::steady_clock> , bool ){});
  }

  std::vector<uint8_t> & current_buffer()
  {
    return checkpoint_buffers_[current_buffer_];
  }
  
  buffer_state current_buffer_state() const
  {
    return checkpoint_buffer_states_[current_buffer_];
  }
  
  buffer_state & current_buffer_state() 
  {
    return checkpoint_buffer_states_[current_buffer_];
  }
  
  std::vector<uint8_t> & next_buffer()
  {
    return checkpoint_buffers_[(current_buffer_+1)%2];
  }

  buffer_state next_buffer_state() const
  {
    return checkpoint_buffer_states_[(current_buffer_+1)%2];
  }
  
  buffer_state & next_buffer_state()
  {
    return checkpoint_buffer_states_[(current_buffer_+1)%2];
  }
  
  template<typename _Callback>
  void write_current_buffer_and_switch(std::chrono::time_point<std::chrono::steady_clock> clock_now,
                                       checkpoint_data_ptr ckpt,
                                       _Callback && cb)
  {
    BOOST_ASSERT(next_buffer_state() == buffer_state::FREE);
    ckpt->write(clock_now, reinterpret_cast<const uint8_t *>(&current_buffer()[0]), current_buffer().size(), 
                [cb = std::move(cb)](std::chrono::time_point<std::chrono::steady_clock> clock_now) { cb(clock_now, false); });
    current_buffer_state() = buffer_state::IN_TRANSIT;          
    current_buffer_ = (current_buffer_+1)%2;
    current_buffer_state() = buffer_state::POPULATING;
  }
  
  template<typename _Callback>
  bool checkpoint(std::chrono::time_point<std::chrono::steady_clock> clock_now,
                  checkpoint_data_ptr ckpt,
                  _Callback && cb)
  {
    switch(checkpoint_state_) {
    case checkpoint_state::START:
      BOOST_ASSERT(checkpoint_buffer_states_[0] == buffer_state::FREE);
      BOOST_ASSERT(checkpoint_buffer_states_[1] == buffer_state::FREE);
      checkpoint_buffers_[0].reserve(1024*1024);
      checkpoint_buffers_[1].reserve(1024*1024);
      current_buffer_state() = buffer_state::POPULATING;
      current_buffer().resize(sizeof(boost::endian::little_uint32_t));
      *reinterpret_cast<boost::endian::little_uint32_t *>(&current_buffer()[0]) = commands.size();
      for(commands_it_ = commands.begin(); commands_it_ != commands.end(); ++commands_it_) {
        if (current_buffer().size() + sizeof(boost::endian::little_uint32_t) > current_buffer().capacity()) {
          if (next_buffer_state() == buffer_state::IN_TRANSIT) {
            checkpoint_state_ = checkpoint_state::WRITE_COMMAND_SIZE;
            return false;
          case checkpoint_state::WRITE_COMMAND_SIZE:
            next_buffer().resize(0);
            next_buffer_state() = buffer_state::FREE;
          } else {
            BOOST_ASSERT(next_buffer_state() == buffer_state::FREE);
          }
          write_current_buffer_and_switch(clock_now, ckpt, std::move(cb));
        }
        {
          auto pos = current_buffer().size();
          current_buffer().resize(sizeof(boost::endian::little_uint32_t)+pos);
          *reinterpret_cast<boost::endian::little_uint32_t *>(&current_buffer()[pos]) = commands_it_->size();
        }
        if (current_buffer().size() + commands_it_->size() > current_buffer().capacity()) {
          if (next_buffer_state() == buffer_state::IN_TRANSIT) {          
            checkpoint_state_ = checkpoint_state::WRITE_COMMAND;
            return false;
          case checkpoint_state::WRITE_COMMAND:
            next_buffer().resize(0);
            next_buffer_state() = buffer_state::FREE;
          } else {
            BOOST_ASSERT(next_buffer_state() == buffer_state::FREE);
          }
          write_current_buffer_and_switch(clock_now, ckpt, std::move(cb));
        }
        current_buffer().insert(current_buffer().end(),
                                  reinterpret_cast<const uint8_t *>(commands_it_->c_str()),
                                  reinterpret_cast<const uint8_t *>(commands_it_->c_str())+commands_it_->size());
      }
      BOOST_ASSERT(current_buffer_state() != buffer_state::IN_TRANSIT);
      if (next_buffer_state() == buffer_state::IN_TRANSIT) {          
        checkpoint_state_ = checkpoint_state::PENULTIMATE_WRITE;
        return false;
      case checkpoint_state::PENULTIMATE_WRITE:
        next_buffer().resize(0);
        next_buffer_state() = buffer_state::FREE;
      } else {
        BOOST_ASSERT(next_buffer_state() == buffer_state::FREE);
      }
      // This write could be empty but is simplifies the code that determines whether the callback indicates
      // is the final write.
      checkpoint_state_ = checkpoint_state::FINAL_WRITE;
      current_buffer_state() = buffer_state::IN_TRANSIT;
      ckpt->write(clock_now, reinterpret_cast<const uint8_t *>(&current_buffer()[0]), current_buffer().size(),
                  [cb = std::move(cb)](std::chrono::time_point<std::chrono::steady_clock> clock_now) { cb(clock_now, true); });
      return true;
    case checkpoint_state::FINAL_WRITE:
      current_buffer_state() = buffer_state::FREE;
    }
    return true;
  }
  void restore_checkpoint_block(raft::slice && s, bool is_final)
  {
    if (nullptr == s.data()) {
      state_ = START;
    }
    checkpoint_slice_ = std::move(s);

    switch(state_) {
    case START:
      commands.resize(0);
      if (1024 > checkpoint_buffer_.capacity()) {
        checkpoint_buffer_.reserve(1024);
      }
      checkpoint_buffer_.resize(0);
      while (true) {
        if (0 == checkpoint_slice_.size()) {
          state_ = READ_COMMANDS_SIZE;
          return;
        case READ_COMMANDS_SIZE:
          ;
        }
        BOOST_ASSERT(0 < checkpoint_slice_.size());
        if (0 == checkpoint_buffer_.size() && s.size() >= sizeof(boost::endian::little_uint32_t)) {
          checkpoint_commands_size_ = *reinterpret_cast<const boost::endian::little_uint32_t *>(checkpoint_slice_.data());
          checkpoint_slice_ += sizeof(boost::endian::little_uint32_t);
          break;
        } else {
          std::size_t to_insert = sizeof(boost::endian::little_uint32_t) > checkpoint_buffer_.size() ? sizeof(boost::endian::little_uint32_t) - checkpoint_buffer_.size() : 0;
          to_insert = to_insert > checkpoint_slice_.size() ? checkpoint_slice_.size() : to_insert;
          auto begin = reinterpret_cast<const uint8_t *>(checkpoint_slice_.data());
          auto end = begin + to_insert;
          checkpoint_buffer_.insert(checkpoint_buffer_.end(), begin, end);
          checkpoint_slice_ += to_insert;
          if (checkpoint_buffer_.size() >= sizeof(boost::endian::little_uint32_t)) {
            checkpoint_commands_size_ = *reinterpret_cast<const boost::endian::little_uint32_t *>(&checkpoint_buffer_[0]);
            checkpoint_buffer_.resize(0);
            break;
          }
        }
      }
      while(commands.size() < checkpoint_commands_size_) {
        while (true) {
          if (0 == checkpoint_slice_.size()) {
            state_ = READ_COMMAND_SIZE;
            return;
          case READ_COMMAND_SIZE:
            ;
          }
          BOOST_ASSERT(0 < checkpoint_slice_.size());
          if (0 == checkpoint_buffer_.size() && s.size() >= sizeof(boost::endian::little_uint32_t)) {
            checkpoint_command_size_ = *reinterpret_cast<const boost::endian::little_uint32_t *>(checkpoint_slice_.data());
            checkpoint_slice_ += sizeof(boost::endian::little_uint32_t);
            break;
          } else {
            std::size_t to_insert = sizeof(boost::endian::little_uint32_t) > checkpoint_buffer_.size() ? sizeof(boost::endian::little_uint32_t) - checkpoint_buffer_.size() : 0;
            to_insert = to_insert > checkpoint_slice_.size() ? checkpoint_slice_.size() : to_insert;
            auto begin = reinterpret_cast<const uint8_t *>(checkpoint_slice_.data());
            auto end = begin + to_insert;
            checkpoint_buffer_.insert(checkpoint_buffer_.end(), begin, end);
            checkpoint_slice_ += to_insert;
            if (checkpoint_buffer_.size() >= sizeof(boost::endian::little_uint32_t)) {
              checkpoint_command_size_ = *reinterpret_cast<const boost::endian::little_uint32_t *>(&checkpoint_buffer_[0]);
              checkpoint_buffer_.resize(0);
              break;
            }
          }
        }
        commands.push_back(std::string());
        while(commands.back().size() < checkpoint_command_size_) {
          if (0 == checkpoint_slice_.size()) {
            state_ = READ_COMMAND;
            return;
          case READ_COMMAND:
            ;
          }
          std::size_t to_append =  checkpoint_command_size_ - commands.back().size();
          to_append = to_append > checkpoint_slice_.size() ? checkpoint_slice_.size() : to_append;
          commands.back().append(reinterpret_cast<const char *>(checkpoint_slice_.data()), to_append);
          checkpoint_slice_ += to_append;
        }
        if (commands.size() == checkpoint_commands_size_ && !is_final) {
          throw std::runtime_error("INVALID CHECKPOINT");
        }
      }
    }
  }
};

#endif
