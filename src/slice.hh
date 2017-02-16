#ifndef __SLICE_HH__
#define __SLICE_HH__

namespace raft {

  class slice
  {
  private:
    const uint8_t * buffer;
    std::size_t sz;
  public:
    // Non-copyable only moveable
    slice(const slice & s) = delete;
    const slice & operator=(const slice & s) = delete;

    slice(const uint8_t * b, std::size_t s)
      :
      buffer(b),
      sz(s)
    {
    }

    slice(slice && s)
      :
      buffer(s.buffer),
      sz(s.sz)
    {
      s.buffer = nullptr;
      s.sz = 0;
    }

    slice & operator=(slice && s)
    {
      buffer = s.buffer;
      sz = s.sz;
      s.buffer = nullptr;
      s.sz = 0;
      return *this;
    }

    slice share() const
    {
      return slice(buffer, sz);
    }

    slice share(std::size_t pos, std::size_t len) const
    {
      slice s = share();
      s.buffer += pos;
      s.sz = len;
      return s;
    }

    void clear()
    {
      buffer = nullptr;
      sz = 0;
    }

    void trim_prefix(std::size_t len)
    {
      buffer += len;
      sz -= len;
    }

    static slice create(const std::string & str)
    {
      return slice(reinterpret_cast<const uint8_t *>(&str[0]), str.size());
    }

    template<typename _T>
    static _T buffer_cast(const slice & s)
    {
      return reinterpret_cast<_T>(s.buffer);
    }

    static std::size_t buffer_size(const slice & s)
    {
      return s.sz;
    }

    // Get the total size of a sequence of slices
    template<typename ConstSliceSequence>
    static std::size_t total_size(const ConstSliceSequence & slices)
    {
      std::size_t total = 0;
      auto it = slices.begin();
      auto end = slices.end();
      for(; it != end; ++it) {
	total += slice::buffer_size(*it);
      }
      return total;
    }
      
      
    // Return slices corresponding to the byte range [left,left+len)
    template<typename ConstSliceSequence, typename SliceOutputIterator>
    static void share(const ConstSliceSequence & slices, std::size_t left, std::size_t len,
		      SliceOutputIterator out)
    {
      auto it = slices.begin();
      auto end = slices.end();

      // Find the slice containing left and add it
      std::size_t total_bytes = 0;
      for(; it != end; ++it) {
	std::size_t sz = slice::buffer_size(*it);
	if (total_bytes + sz > left) {
	  std::size_t to_share = (std::min)(len, std::size_t(total_bytes + sz - left));
	  *out++ = it->share(left-total_bytes, to_share);
	  len -= to_share;
	  total_bytes += sz;
	  ++it;
	  break;
	} else {
	  total_bytes += sz;
	}
      }

      // Keep adding until we get all of the bytes or hit the end of the sequence
      for(; len > 0 && it != end; ++it) {
	std::size_t sz = slice::buffer_size(*it);
	if (sz > len) {
	  *out++ = slice(slice::buffer_cast<const uint8_t *>(*it), len);
	  len = 0;
	} else {
	  len -= sz;
	  *out++ = it->share();
	}
      }	
    }
  };
}

#endif
