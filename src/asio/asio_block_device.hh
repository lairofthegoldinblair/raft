#ifndef ASIO_BLOCK_DEVICE_HH
#define ASIO_BLOCK_DEVICE_HH

#include <algorithm>
#include "asio/basic_file_object.hh"
#include "asio/disk_io_service.hh"

// TODO: A different model is one that doesn't use buffers at all rather accepts iovecs (ConstBufferSequences)
// and always writes immediately to ASIO (then flush is a noop).  I should implement that too and see which performs
// better (will depend on the smallest size of writes I suppose).  Such a model will require more finesse around memory management.
// TODO: Use a strand to keep file IOs from getting out of order or should this be part of
// the service for the file?
namespace raft {
  namespace asio {
    class writable_file
    {
    private:
      static const int BLOCK_SIZE = 32768;

      basic_file_object<disk_io_service> file_;
      uint8_t * block_begin_;
      uint8_t * block_ptr_;
      uint8_t * block_end_;
      
    public:
      writable_file(boost::asio::io_service & ios, int fd)
	:
	file_(ios, fd),
	block_begin_(nullptr),
	block_ptr_(nullptr),
	block_end_(nullptr)
      {
      }

      ~writable_file()
      {
	flush();
      }

      int write(const uint8_t * data, std::size_t len)
      {
	while(len > 0) {
	  // TODO: Limit amount of memory for outstanding IOs
	  if (nullptr == block_begin_) {
	    block_begin_ = block_ptr_ = new uint8_t[BLOCK_SIZE];
	    block_end_ = block_begin_ + BLOCK_SIZE;
	  }
	  std::size_t to_copy = (std::min)(len, (std::size_t)(block_end_ - block_ptr_));
	  ::memcpy(block_ptr_, data, to_copy);
	  data += to_copy;
	  block_ptr_ += to_copy;
	  len -= to_copy;

	  if (block_ptr_ == block_end_) {
	    // Can this fail?  Right now we are assuming this is atomic(not a composed operation) 
	    // and that we can have multiple async_writes outstanding.  In fact this is an atomic operation
	    // but it is worth pointing out the assumption
	    auto buf = boost::asio::buffer(block_begin_, std::distance(block_begin_, block_ptr_));
	    file_.async_write(buf,
			      [this,buf] (boost::system::error_code ec, std::size_t bytes_transferred) {
				if (ec) {
				  // TODO: Error handling
				  return;
				}
				delete [] boost::asio::buffer_cast<const uint8_t *>(buf);
			      });
	    block_begin_ = block_ptr_ = block_end_ = nullptr;
	  }
	}
	return 0;
      }

      void flush()
      {
	  if (block_ptr_ != block_begin_) {
	    auto buf = boost::asio::buffer(block_begin_, std::distance(block_begin_, block_ptr_));
	    file_.async_write(buf,
			      [this,buf] (boost::system::error_code ec, std::size_t bytes_transferred) {
				if (ec) {
				  // TODO: Error handling
				  return;
				}
				delete [] boost::asio::buffer_cast<const uint8_t *>(buf);
			      });
	    block_begin_ = block_ptr_ = block_end_ = nullptr;
	  }
      }

      template<typename FileOpHandler>
      void sync(FileOpHandler && handler)
      {
	file_.async_sync(handler);
      }

      template<typename FileOpHandler>
      void flush_and_sync(FileOpHandler && handler)
      {
	  if (block_ptr_ != block_begin_) {
	    auto buf = boost::asio::buffer(block_begin_, std::distance(block_begin_, block_ptr_));
	    file_.async_write(buf,
			      [this,buf,handler] (boost::system::error_code ec, std::size_t bytes_transferred) {
				if (ec) {
				  // TODO: Error handling?
				  handler(ec);
				  return;
				}
				delete [] boost::asio::buffer_cast<const uint8_t *>(buf);
				this->file_.async_sync(handler);
			      });
	    block_begin_ = block_ptr_ = block_end_ = nullptr;
	  } else {
	    this->file_.async_sync(handler);
	  }
      }
    };
  }
}

#endif
