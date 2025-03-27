#include <zmq.hpp>
#include <redis/redis++.h>
#include <sys/epoll.h>
#include <unistd.h>

class Ingestion {
    zmq::context_t context{1};
    zmq::socket_t socket{context, ZMQ_REP};
    sw::redis::Redis redis{"tcp://localhost:6379"};

public:
    Ingestion() { socket.bind("tcp://*:5556"); }

    void run() {
        int epoll_fd = epoll_create1(0);
        epoll_event event{.events = EPOLLIN, .data.fd = socket.get_fd()};
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket.get_fd(), &event);

        while (true) {
            epoll_event events[10];
            int nfds = epoll_wait(epoll_fd, events, 10, -1);
            for (int i = 0; i < nfds; i++) {
                zmq::message_t request;
                socket.recv(request);
                std::string msg(static_cast<char*>(request.data()), request.size());
                redis.publish("node_events", msg);
                socket.send(zmq::message_t("ACK", 3), zmq::send_flags::none);
            }
        }
    }
};

int main() {
    Ingestion ingestion;
    ingestion.run();
    return 0;
}