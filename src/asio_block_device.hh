#ifndef ASIO_BLOCK_DEVICE_HH
#define ASIO_BLOCK_DEVICE_HH

#include <algorithm>
#include "basic_file_object.hh"
#include "disk_io_service.hh"

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
	    // Can this fail?
	    file_.async_write(boost::asio::buffer(block_begin_, std::distance(block_begin_, block_ptr_)),
			      [=] (boost::system::error_code ec, std::size_t bytes_transferred) {
				delete [] block_begin_;
			      });
	    block_begin_ = block_ptr_ = block_end_ = nullptr;
	  }
	}
	return 0;
      }

      void flush()
      {
	  if (block_ptr_ != block_begin_) {
	    file_.async_write(boost::asio::buffer(block_begin_, std::distance(block_begin_, block_ptr_)),
			      [=] (boost::system::error_code ec, std::size_t bytes_transferred) {
				delete [] block_begin_;
			      });
	    block_begin_ = block_ptr_ = block_end_ = nullptr;
	  }
      }
    };
  }
}

#endif
