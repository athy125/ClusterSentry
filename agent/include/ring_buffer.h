#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#include <vector>
#include <atomic>

template<typename T, size_t Capacity>
class RingBuffer {
    std::vector<T> buffer;
    std::atomic<size_t> head{0}, tail{0};

public:
    RingBuffer() { buffer.resize(Capacity); }
    bool push(const T& item) {
        size_t next = (head + 1) % Capacity;
        if (next == tail) return false;
        buffer[head] = item;
        head = next;
        return true;
    }
    T& back() { return buffer[(head - 1) % Capacity]; }
    size_t size() const { return (head >= tail) ? head - tail : Capacity - tail + head; }
    T& operator[](size_t idx) { return buffer[(tail + idx) % Capacity]; }
};

#endif // RING_BUFFER_H