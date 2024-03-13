#ifndef __RAFT_ASIO_CHECKPOINT_HH__
#define __RAFT_ASIO_CHECKPOINT_HH__

#include <chrono>
#include <memory>
#include <string>

#include <boost/asio.hpp>
#include <boost/assert.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "asio/file_base.hh"
#include "util/call_on_delete.hh"
#include "util/json.hh"

namespace raft {
  namespace asio {
    class checkpoint_block
    {
    public:
      const uint8_t * block_data_;
      std::size_t size_;
      uint64_t offset_;

      checkpoint_block()
        :
        block_data_(nullptr),
        size_(0),
        offset_(0)
      {
      }

      ~checkpoint_block()
      {
        delete [] block_data_;
      }
      
      checkpoint_block(const checkpoint_block & ) = delete;
      
      checkpoint_block(checkpoint_block && rhs)
        :
        block_data_(rhs.block_data_),
        size_(rhs.size_),
        offset_(rhs.offset_)
      {
        rhs.block_data_ = nullptr;
        rhs.size_ = 0;
      }

      checkpoint_block(const uint8_t * block_data, std::size_t sz, uint64_t offset)
        :
        block_data_(std::move(block_data)),
        size_(sz),
        offset_(offset)
      {
      }

      checkpoint_block & operator=(const checkpoint_block & ) = delete;
      
      checkpoint_block & operator=(checkpoint_block && rhs)
      {
        if (this != &rhs) {
          delete [] block_data_;
          block_data_ = rhs.block_data_;
          size_ = rhs.size_;
          offset_ = rhs.offset_;
          rhs.block_data_ = nullptr;
          rhs.size_ = 0;
        }
        return *this;
      }
      
      const void * data() const
      {
        return block_data_;
      }

      size_t size() const
      {
        return size_;
      }

      uint64_t offset() const
      {
        return offset_;
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
    template<typename _Messages, typename _Policy>
    class checkpoint_data
    {
    public:
      typedef typename _Policy::service_type service_type;
      typedef typename _Policy::endpoint_type endpoint_type;
      typedef typename _Messages::checkpoint_header_type header_type;
    private:
      // We have to use a generic deleter for the header because the header
      // may or may not be embedded in a larger chunk of memory.
      // 1) If we initialize the checkpoint then the header is created directly
      // 2) If the checkpoint is sent to us then it is embedded in an append_checkpoint_chunk message
      const header_type * header_;
      raft::util::call_on_delete header_deleter_;
      std::size_t block_size_;
      service_type & service_;
      endpoint_type file_;
      boost::filesystem::path directory_;
    public:
      checkpoint_data(const header_type * header,
                      raft::util::call_on_delete && deleter,
                      std::size_t block_size,
                      service_type & service,
                      endpoint_type && f,
                      boost::filesystem::path && nm)
        :
        header_(header),
        header_deleter_(std::move(deleter)),
        block_size_(block_size),
        service_(service),
        file_(std::move(f)),
        directory_(std::move(nm))
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

      boost::filesystem::path filename() const
      {
        return filename(directory());
      }

      boost::filesystem::path header_filename() const
      {
        return directory() / "checkpoint_header.bin";
      }

      const boost::filesystem::path & directory() const
      {
        return directory_;
      }

      void directory(const boost::filesystem::path & val)
      {
        directory_ = val;
      }

      uint64_t size() const
      {
        return boost::filesystem::file_size(filename());
      }
      
      template<typename _Callback>
      void read_block_at_current_file_pointer(_Callback && cb) {
        auto sz = block_size_;
        uint8_t * ptr = new uint8_t [sz];
        boost::asio::mutable_buffer buf(ptr, sz);
        BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data::read_block_at_current_file_pointer] Reading block of size " << sz << " from checkpoint file "
                                 << filename() << " at offset " << this->service_.seek(file_, 0, file_base::seek_cur);
        service_.async_read(file_, buf, [this, offset = this->service_.seek(file_, 0, file_base::seek_cur), ptr, cb=std::move(cb)](boost::system::error_code ec, std::size_t bytes_transferred) mutable {
                                 // if (ec) {
                                 //   BOOST_LOG_TRIVIAL(warning) << "[raft::asio::checkpoint_data::read_block_at_current_file_pointer] read failed: " << ec;
                                 //   cb(std::chrono::steady_clock::now(), checkpoint_block());
                                 //   return;
                                 // }
                                 BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data::read_block_at_current_file_pointer] Read " << bytes_transferred << " bytes from checkpoint file "
                                                          << filename() << " at offset " << offset;
                                 cb(std::chrono::steady_clock::now(), checkpoint_block(ptr, bytes_transferred, offset));
                               });
      }

      template<typename _Callback>
      void block_at_offset(std::chrono::time_point<std::chrono::steady_clock> clock_now, uint64_t offset, _Callback && cb) {
        if (offset >= size()) {
          cb(clock_now, checkpoint_block());
          return;	
        }
        auto sz = block_size_;
        uint8_t * ptr = new uint8_t [sz];
        boost::asio::mutable_buffer buf(ptr, sz);
        BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data::block_at_offset] Reading block of size " << sz << " from checkpoint file "
                                 << filename() << " at offset " << offset;
        service_.async_read_at(file_, buf, offset, [this, offset, ptr, cb=std::move(cb)](boost::system::error_code ec, std::size_t bytes_transferred) mutable {
                                            // if (ec) {
                                            //   BOOST_LOG_TRIVIAL(warning) << "[raft::asio::checkpoint_data::block_at_offset] read failed: " << ec;
                                            //   cb(std::chrono::steady_clock::now(), checkpoint_block());
                                            //   return;
                                            // }
                                            BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data::block_at_offset] Read " << bytes_transferred << " bytes from checkpoint file "
                                                                     << filename() << " at offset " << offset;
                                            cb(std::chrono::steady_clock::now(), checkpoint_block(ptr, bytes_transferred, offset));
                                          });
      }
    
      template<typename _Callback>
      void next_block(std::chrono::time_point<std::chrono::steady_clock> clock_now, const checkpoint_block & current_block, _Callback && cb) {
        if (current_block.is_null()) {
          service_.seek(file_, 0, file_base::seek_set);
        }
        if (current_block.is_null() || !is_final(current_block)) {
          read_block_at_current_file_pointer(std::move(cb));
          return;
        } else {
          cb(clock_now, checkpoint_block());
          return;
        }
      }

      uint64_t block_begin(const checkpoint_block & current_block) const {
        return current_block.offset();
      }

      uint64_t block_end(const checkpoint_block & current_block) const {
        return current_block.offset() + current_block.size();
      }

      bool is_final(const checkpoint_block & current_block) {
        return !current_block.is_null() && block_end(current_block) == size();
      }

      template<typename _Callback>
      void write(std::chrono::time_point<std::chrono::steady_clock> clock_now,
                 const uint8_t * data, std::size_t len,
                 _Callback && cb)
      {
        BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data::write] Writing block of size " << len << " to checkpoint file "
                                 << filename();
        service_.async_write(file_, boost::asio::buffer(data, len), [this, cb=std::move(cb)](boost::system::error_code ec, std::size_t bytes_transferred) mutable {
                                                             // if (ec) {
                                                             //   BOOST_LOG_TRIVIAL(warning) << "[raft::asio::checkpoint_data::write] write failed: " << ec;
                                                             //   cb(std::chrono::steady_clock::now());
                                                             //   return;
                                                             // }
                                                             BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data::write] Wrote " << bytes_transferred << " bytes to checkpoint file "
                                                                                      << filename();
                                                             cb(std::chrono::steady_clock::now());
                                                           });
      }

      template<typename _Callback>
      void sync(std::chrono::time_point<std::chrono::steady_clock> clock_now,
                _Callback && cb)
      {
        BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data::sync] Syncing checkpoint file " << filename();
        service_.async_sync(file_, [this, cb=std::move(cb)](boost::system::error_code ec) mutable {
                            // if (ec) {
                            //   BOOST_LOG_TRIVIAL(warning) << "[raft::asio::checkpoint_data::write] write failed: " << ec;
                            //   cb(std::chrono::steady_clock::now());
                            //   return;
                            // }
                            BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data::sync] Synced checkpoint file "
                                                     << filename();
                            cb(std::chrono::steady_clock::now());
                          });
      }

      static boost::filesystem::path filename(const boost::filesystem::path & directory)
      {
        return directory / "checkpoint.bin";
      }
    };

    // Checkpoints live here
    template<typename _Messages, typename _Serialization, typename _Policy>
    class checkpoint_data_store
    {
    public:
      typedef typename _Policy::service_type service_type;
      typedef typename _Policy::endpoint_type endpoint_type;
      typedef _Serialization serialization_type;
      typedef checkpoint_data<_Messages, _Policy> checkpoint_data_type;
      typedef std::shared_ptr<checkpoint_data_type> checkpoint_data_ptr;
      typedef typename _Messages::checkpoint_header_type header_type;
      typedef typename _Messages::checkpoint_header_traits_type header_traits_type;
      typedef checkpoint_block block_type;
      typedef typename _Messages::configuration_checkpoint_type configuration_type;
      typedef raft::util::json<_Messages> json_type;
    private:
      service_type & service_;
      boost::filesystem::path checkpoint_directory_;
      checkpoint_data_ptr last_checkpoint_;
      std::size_t block_size_ = 1024*1024;
      endpoint_type header_file_;

      static std::string base16_encode(const uint8_t * in,
                                       std::size_t bufSz)
      {
        const char * lut = "0123456789ABCDEFG";
        std::string tmpStr(2*bufSz, ' ');
        char * tmpIt = &tmpStr[0];
        for(std::size_t i = 0; i<bufSz; i++) {
          *tmpIt++ = lut[(in[i] & 0xF0) >> 4];
          *tmpIt++ = lut[in[i] & 0x0F];    
        }  
        return tmpStr;
      }
      
      boost::filesystem::path temp_directory_name() const
      {
        boost::uuids::uuid tmp = boost::uuids::random_generator()();
        return checkpoint_directory_ / base16_encode((const uint8_t *) &tmp, tmp.size());
      }

      endpoint_type checkpoint_file(const boost::filesystem::path & f) const
      {
	auto checkpoint_fd = ::open(f.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if (-1 == checkpoint_fd) {
          return service_.create();
	}
        return service_.create(checkpoint_fd);
      }

      endpoint_type checkpoint_data_file(const boost::filesystem::path & checkpoint_directory) const
      {
        auto ret = ::mkdir(checkpoint_directory.c_str(), S_IRUSR | S_IWUSR | S_IXUSR);
        if (-1 == ret) {
          return service_.create();
        }
        return checkpoint_file(checkpoint_data_type::filename(checkpoint_directory));
      }

    public:
      checkpoint_data_store(service_type & ios,
                            const std::string & checkpoint_directory)
        :
        service_(ios),
        checkpoint_directory_(checkpoint_directory)
      {
      }
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
        auto nm = temp_directory_name();
        auto tmp = checkpoint_data_file(nm);
        if (!!tmp) {
          checkpoint_data_ptr ckpt(new checkpoint_data_type(header, std::move(deleter), block_size_, service_, std::move(tmp), std::move(nm)));
          BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data_store::create] Creating checkpoint with header "
                                   << json_type::checkpoint_header(*header) 
                                   << " and checkpoint data file " << ckpt->filename();
          return ckpt;
        } else {
          return checkpoint_data_ptr();
        }
      }
      checkpoint_data_ptr create(std::pair<const header_type *, raft::util::call_on_delete> && header) const
      {
        auto nm = temp_directory_name();
        auto tmp = checkpoint_data_file(nm);
        if (!!tmp) {
          checkpoint_data_ptr ckpt(new checkpoint_data_type(header.first, std::move(header.second), block_size_, service_, std::move(tmp), std::move(nm)));
          BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data_store::create] Creating checkpoint with header "
                                   << json_type::checkpoint_header(*header.first) 
                                   << " and checkpoint data file " << ckpt->filename();
          return ckpt;
        } else {
          return checkpoint_data_ptr();
        }
      }

      template<typename _Callback>
      void internal_rename_checkpoint(std::chrono::time_point<std::chrono::steady_clock> clock_now,
                                      checkpoint_data_ptr f,
                                      _Callback && cb)
      {
        if (boost::filesystem::exists(checkpoint_directory_ / "current")) {          
          if (boost::filesystem::exists(checkpoint_directory_ / "previous")) {
            boost::filesystem::remove_all(checkpoint_directory_ / "previous");
          }
          boost::filesystem::rename(checkpoint_directory_ / "current", checkpoint_directory_ / "previous");
        }
        boost::filesystem::rename(f->directory(), checkpoint_directory_ / "current");
        f->directory(checkpoint_directory_ / "current");
        last_checkpoint_ = f;
        cb(clock_now);
      }
      
      template<typename _Callback>
      void commit(std::chrono::time_point<std::chrono::steady_clock> clock_now,
                  checkpoint_data_ptr f,
                  _Callback && cb)
      {
        auto header = serialization_type::serialize_checkpoint_header(f->header());
        header_file_ = checkpoint_file(f->header_filename());
        BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data_store::commit] Writing checkpoint header "
                                 << json_type::checkpoint_header(f->header()) << " of size " << header.first.size()
                                 << " to checkpoint header file " << f->header_filename();
        service_.async_write(header_file_, boost::asio::buffer(header.first.data(), header.first.size()),
                             [this, f, cb=std::move(cb), deleter = std::move(header.second)](boost::system::error_code ec, std::size_t bytes_transferred) mutable {
                               BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data_store::commit] Wrote " << bytes_transferred << " bytes to checkpoint header file.  Syncing.";
                               this->service_.async_sync(this->header_file_, [this, f, cb=std::move(cb)](boost::system::error_code ec) mutable {
                                   BOOST_LOG_TRIVIAL(trace) << "[raft::asio::checkpoint_data_store::commit] Synced checkpoint header file to disk";
                                   this->internal_rename_checkpoint(std::chrono::steady_clock::now(),
                                                                    f,
                                                                    std::move(cb));
                                 });
                             });
      }
      void discard(checkpoint_data_ptr f)
      {
        if(!!f) {
          boost::filesystem::remove_all(f->directory());
        }
      }
      checkpoint_data_ptr last_checkpoint() {
        return last_checkpoint_;
      }
    };
  }
}

#endif
