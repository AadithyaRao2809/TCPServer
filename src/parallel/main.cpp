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

#define DEBUG 1
using namespace std;



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


void error(const char* msg)
{
  perror(msg);
  exit(1);
}


void* connect_client(void* client_fd_void) {
  thread_args *args = (thread_args*)client_fd_void;
  int client_fd = args->client_fd;
  unordered_map<string, string> kv_store = *(args->kv_store);
  Semaphore sem = *(args->sem);

  char buffer[256];
  bzero(buffer, 256);
  bool end;
  stringstream msg;
  int count = 0;
  int n;
  n = read(client_fd, buffer, 255);
  if (n < 0)
    error("ERROR reading from socket");
#if DEBUG == 1
  printf("Msg: %s\n\n----------------------\n",buffer);
#endif
  // checking for broken query
  string str_buf = string(buffer);
  if(str_buf.find("END") == -1)
  {
    write(client_fd, "NULL\n", 5);
#if DEBUG == 1
    cout << "NULL" << endl;
#endif
    close(client_fd);
    return 0;
  }

  do {


    msg << string(buffer);
    string command;
    msg >> command;
#if DEBUG == 1
    cout << command << endl;
#endif

    string key,val;



    if(command == "READ") {
      msg >> key;

      sem.read_lock();

      if(kv_store.find(key) != kv_store.end())
      {
#if DEBUG == 1
        cout << key << ":" << kv_store[key] << endl;
#endif
        write(client_fd, (kv_store[key]+"\n").c_str(), kv_store[key].size()+1);
      }
      else
      {
#if DEBUG == 1
        cout << key << " NOT FOUND" << endl;
#endif
        write(client_fd, "NULL\n", 5);
      }
      sem.read_unlock();
    }
    else if(command == "WRITE") {
      msg >> key;
      msg >> val;
      val = val.substr(1, val.size()-1);

      sem.write_lock();

      kv_store[key] = val;
#if DEBUG == 1
      cout << key << " -> " << val << endl;
#endif
      write(client_fd, "FIN\n", 4);

      sem.write_unlock();

    }
    else if(command == "DELETE")
    {
      msg >> key;

      sem.write_lock();

      if(kv_store.find(key) != kv_store.end())
      {
        kv_store.erase(key);
        cout << key << " deleted" << endl;
      }
      else
#if DEBUG == 1
        cout << "NOT FOUND" << endl;
#endif
      write(client_fd, "NULL\n", 5);

      sem.write_unlock();

    }
    else if (command == "COUNT")
    {

      sem.read_lock();

      string count = to_string(kv_store.size())+"\n";
      write(client_fd,count.c_str(), sizeof(count));
#if DEBUG == 1
      cout << kv_store.size() << endl;
#endif
      sem.read_unlock();
    }
    else if(command == "END")
    {
      end = true;   //break
      break;
    }
    else
    {
#if DEBUG == 1
      cout << "INVAILD COMMAND" << endl;
#endif
    }

  } while(true);


  close(client_fd);
  pthread_exit(NULL);


}



int main(int argc, char ** argv) {
  int portno; /* port to listen on */

  unordered_map<string, string> kv_store;
  Semaphore sem;

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
#if DEBUG == 1
    printf("\tlistening on %s:%d\n", inet_ntoa(serv_addr.sin_addr), ntohs(serv_addr.sin_port));
#endif
    int list_err = listen(sockfd, 100);
    if (list_err < 0)
      error("error on listen");


    int client_fd = accept(sockfd, (struct sockaddr *) &remote_host, &addr_len);
    if (client_fd< 0)
      error("ERROR on accept");

    pthread_t client_tid;

    thread_args *args = new thread_args(client_fd , &kv_store, &sem);

    int pthread_err = pthread_create(&client_tid, NULL, connect_client, (void*)args);
      if (pthread_err)
      error("ERROR on pthread_create");
    client_threads.push_back(client_tid);
#if DEBUG == 1
    printf("Connected to %s:%d\n", inet_ntoa(remote_host.sin_addr), ntohs(remote_host.sin_port));
    for(auto tid : client_threads)
      printf("\t%d\n", tid);
#endif


  }
  while(true);

  for(auto tid : client_threads)
    pthread_join(tid, NULL);
  close(sockfd);


}
