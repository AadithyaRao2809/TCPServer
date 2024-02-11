/*
 * tcpserver.c - A multithreaded TCP echo server
 * usage: tcpserver <port>
 *
 * Testing :
 * nc localhost <port> < input.txt
 */

#include <stdio.h>
#include <bits/stdc++.h>
#include <unistd.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <pthread.h>
#include <sys/socket.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>

#define DEBUG 0
#define NUM_THREADS 128
using namespace std;

struct thread_args;
void error(const char* msg);
int connect_client(thread_args* args);


class Semaphore {
  pthread_mutex_t kv_store_mutex;
  int read_count;
  pthread_mutex_t read_count_mutex;
public:
  Semaphore() {
    pthread_mutex_init(&kv_store_mutex, NULL);
    pthread_mutex_init(&read_count_mutex, NULL);
    read_count = 0;
  }
  void read_lock() {
    pthread_mutex_lock(&read_count_mutex);
    read_count++;
    if (read_count == 1)
      pthread_mutex_lock(&kv_store_mutex);
    pthread_mutex_unlock(&read_count_mutex);
  }

  void read_unlock() {
    pthread_mutex_lock(&read_count_mutex);
    read_count--;
    if (read_count == 0)
      pthread_mutex_unlock(&kv_store_mutex);
    pthread_mutex_unlock(&read_count_mutex);
  }

  void write_lock() {
    pthread_mutex_lock(&kv_store_mutex);
  }

  void write_unlock() {
    pthread_mutex_unlock(&kv_store_mutex);
  }

};

struct thread_args {
  int client_fd;
  unordered_map<string, string> *kv_store;
  Semaphore *sem;

  thread_args(int cl, unordered_map<string, string> *kv, Semaphore *s)
  {
    client_fd = cl;
    kv_store = kv;
    sem = s;
  }


};

class ThreadPool {
  queue<thread_args> tasks;
  vector<pthread_t> threads;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  const int THREADS;

  public:
  ThreadPool(int num_threads) : THREADS(num_threads) {
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
    for (int i = 0; i < THREADS; i++) {
      pthread_t tid;
      pthread_create(&tid, NULL, worker, this);
      threads.push_back(tid);
    }
  }

  void add_task(thread_args task) {
    pthread_mutex_lock(&mutex);
    tasks.push(task);
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
  }

  static void* worker(void* arg) {
    ThreadPool* pool = (ThreadPool*)arg;
    while (true) {
      pthread_mutex_lock(&pool->mutex);
      while (pool->tasks.empty()) {
        pthread_cond_wait(&pool->cond, &pool->mutex);
      }
      thread_args task = pool->tasks.front();
      pool->tasks.pop();
      pthread_mutex_unlock(&pool->mutex);
      connect_client(&task);
    }
  }

  ~ThreadPool() {
    for (int i = 0; i < THREADS; i++) {
      pthread_join(threads[i], NULL);
    }
  }



};

void error(const char* msg) {
  perror(msg);
  exit(1);
}

int connect_client(thread_args* args) {

  char buffer[256];
  bzero(buffer, 256);
  bool end;
  int count = 0;
  int n;
  n = read(args->client_fd, buffer, 255);
  if (n < 0)
    error("ERROR reading from socket");
#if DEBUG == 1
  // printf("Msg: %s\n\n----------------------\n",buffer);
#endif
  // checking for broken query
  string str_buf = string(buffer);
  if(str_buf.find("END") == -1)
  {
    write(args->client_fd, "NULL\n", 5);
#if DEBUG == 1
    cout << "NULL" << endl;
#endif
    close(args->client_fd);
    return 1;
  }

  stringstream msg;
  msg << string(buffer);
  do {


    string command;
    msg >> command;
#if DEBUG == 1
    cout << command << endl;
#endif

    string key,val;



    if(command == "READ") {
      msg >> key;

      args->sem->read_lock();

      if(args->kv_store->find(key) != args->kv_store->end())
      {
#if DEBUG == 1
        cout << key << ":" << (*(args->kv_store))[key] << endl;
#endif
        write(args->client_fd, ((*(args->kv_store))[key]+"\n").c_str(), (*(args->kv_store))[key].size()+1);
      }
      else
      {
#if DEBUG == 1
        cout << key << " NOT FOUND" << endl;
#endif
        write(args->client_fd, "NULL\n", 5);
      }
      args->sem->read_unlock();
    }
    else if(command == "WRITE") {
      msg >> key;
      msg >> val;
      val = val.substr(1, val.size()-1);

      args->sem->write_lock();

      (*(args->kv_store))[key] = val;
#if DEBUG == 1
      cout << key << " -> " << val << endl;
#endif
      write(args->client_fd, "FIN\n", 4);

      args->sem->write_unlock();

    }
    else if(command == "DELETE")
    {
      msg >> key;

      args->sem->write_lock();

      if(args->kv_store->find(key) != args->kv_store->end())
      {
        args->kv_store->erase(key);
        write(args->client_fd, "FIN\n", 4);
        cout << key << " deleted" << endl;
      }
      else
#if DEBUG == 1
        cout << "NOT FOUND" << endl;
#endif
      write(args->client_fd, "NULL\n", 5);

      args->sem->write_unlock();

    }
    else if (command == "COUNT")
    {

      args->sem->read_lock();

      string count = to_string(args->kv_store->size())+"\n";
      write(args->client_fd,count.c_str(), sizeof(count));
#if DEBUG == 1
      cout << args->kv_store->size() << endl;
#endif
      args->sem->read_unlock();
    }
    else if(command == "END")
    {
      // end = true;   //break
      break;
    }
    else
    {
#if DEBUG == 1
      cout << "INVAILD COMMAND" << endl;
#endif
    }

  } while(true);


  close(args->client_fd);
  return 0;


}




int main(int argc, char ** argv) {
  int portno; /* port to listen on */

  unordered_map<string, string> kv_store;
  Semaphore sem;
  ThreadPool pool(NUM_THREADS);

  /*
   * check command line arguments
   */
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  portno = atoi(argv[1]);

  struct sockaddr_in serv_addr, remote_host;
  bzero((char *) &serv_addr, sizeof(serv_addr));
  socklen_t addr_len = sizeof(remote_host);
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(portno);
  serv_addr.sin_addr.s_addr = INADDR_ANY;

  // create a socket
  int option = 1;
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
    error("error opening socket");
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
  //bind socket fd to port
  int bind_err = bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));
  if (bind_err < 0)
    error("error on binding");

  vector<pthread_t> client_threads;
  do{
    //listen for incoming tcp connections
    int list_err = listen(sockfd, 100);
    if (list_err < 0)
      error("error on listen");

#if DEBUG == 1
    printf("\tlistening on %s:%d\n", inet_ntoa(serv_addr.sin_addr), ntohs(serv_addr.sin_port));
#endif


    int client_fd = accept(sockfd, (struct sockaddr *) &remote_host, &addr_len);
    if (client_fd< 0)
      error("ERROR on accept");

    pthread_t client_tid;

    thread_args args = thread_args(client_fd , &kv_store, &sem);
    pool.add_task(args);




  }
  while(true);

  close(sockfd);


}
