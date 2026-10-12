import threading
import time
import queue

class Process(threading.Thread):
    def __init__(self, pid, n, message_queues):
        super().__init__()
        self.pid = pid
        self.n = n
        self.message_queues = message_queues
        self.timestamp = 0
        self.replies = 0
        self.requesting = False

    def send_request(self):
        self.timestamp += 1
        msg = ("REQUEST", self.pid, self.timestamp)
        self.requesting = True
        for i in range(self.n):
            if i != self.pid:
                self.message_queues[i].put(msg)
        print(f"Process {self.pid} sent REQUEST at {self.timestamp}")

    def send_reply(self, to_pid):
        msg = ("REPLY", self.pid, self.timestamp)
        self.message_queues[to_pid].put(msg)
        print(f"Process {self.pid} sent REPLY to {to_pid}")

    def run(self):
        # Request critical section
        time.sleep(self.pid)  # stagger requests
        self.send_request()

        while True:
            try:
                msg = self.message_queues[self.pid].get(timeout=1)
                if msg[0] == "REQUEST":
                    # Simple rule: reply immediately if not in CS
                    if not self.requesting:
                        self.send_reply(msg[1])
                    else:
                        # defer reply until exit
                        threading.Timer(2, self.send_reply, args=(msg[1],)).start()
                elif msg[0] == "REPLY":
                    self.replies += 1
                    if self.replies == self.n - 1:
                        print(f"Process {self.pid} ENTERS critical section")
                        time.sleep(2)
                        print(f"Process {self.pid} EXITS critical section")
                        self.requesting = False
                        break
            except queue.Empty:
                continue

def main():
    print("Starting Ricart–Agrawala Simulator...")
    n = 3  # number of processes
    message_queues = [queue.Queue() for _ in range(n)]
    processes = [Process(i, n, message_queues) for i in range(n)]

    for p in processes:
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    main()
