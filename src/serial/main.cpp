/*
 * tcpserver.c - A multithreaded TCP echo server
 * usage: tcpserver
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

using namespace std;
#define DEBUG 0

int sockfd, clientsock_fd;
// creater a signal handler to safely close connection and close sockets
void signalHandler(int signum) {
  printf("\nclosing connection\n");
  close(clientsock_fd);
  close(sockfd);
  printf("received SIGINT\n");
  exit(signum);
}
// error function

void error(const char *msg)
{
  perror(msg);
  exit(1);
}

int main(int argc, char ** argv) {
  signal(SIGINT, signalHandler);
  int portno; /* port to listen on */

  /*
   * check command line arguments
   */
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  portno = atoi(argv[1]);

  int  n;

  struct sockaddr_in serv_addr, remote_host;
  bzero((char *) &serv_addr, sizeof(serv_addr));
  socklen_t addr_len = sizeof(remote_host);
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(portno);
  serv_addr.sin_addr.s_addr = INADDR_ANY;

  // create a socket
  int option = 1;
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
    error("error opening socket");
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
  //bind socket fd to port
  int bind_err = bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));
  if (bind_err < 0)
    error("error on binding");

  //listen for incoming tcp connections
  // do{
  printf("listening on %s:%d\n", inet_ntoa(serv_addr.sin_addr), ntohs(serv_addr.sin_port));
  int list_err = listen(sockfd, 5);
  if (list_err < 0)
    error("error on listen");

  clientsock_fd = accept(sockfd, (struct sockaddr *) &remote_host, &addr_len);
  if (clientsock_fd< 0)
    error("ERROR on accept");


  printf("Message received from %s:%d\n", inet_ntoa(remote_host.sin_addr), ntohs(remote_host.sin_port));

  //read data from socket
  char buffer[256];
  bzero(buffer, 256);
  unordered_map<string, string> kv_store;
  bool end;
  stringstream msg;
  int count = 0;
  n = read(clientsock_fd, buffer, 255);
  if (n < 0)
    error("ERROR reading from socket");
#if DEBUG
  printf("Msg: %s\n\n----------------------\n",buffer);
#endif
  // checking for broken query
  string str_buf = string(buffer);
  if(str_buf.find("END") == -1)
  {
    write(clientsock_fd, "NULL\n", 5);
#if DEBUG == 1
    cout << "NULL" << endl;
#endif
    close(clientsock_fd);
    close(sockfd);
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
        if(kv_store.find(key) != kv_store.end())
        {
#if DEBUG == 1
          cout << key << ":" << kv_store[key] << endl;
#endif
          write(clientsock_fd, (kv_store[key]+"\n").c_str(), kv_store[key].size()+1);
        }
        else
        {
#if DEBUG == 1
          cout << key << " NOT FOUND" << endl;
#endif
          write(clientsock_fd, "NULL\n", 5);
        }
      }
      else if(command == "WRITE") {
        msg >> key;
        msg >> val;
        val = val.substr(1, val.size()-1);
        kv_store[key] = val;
#if DEBUG == 1
        cout << key << " -> " << val << endl;
#endif
        write(clientsock_fd, "FIN\n", 4);
      }
      else if(command == "DELETE")
      {
        msg >> key;
        if(kv_store.find(key) != kv_store.end())
        {
          kv_store.erase(key);
          cout << key << " deleted" << endl;
        }
        else
#if DEBUG == 1
          cout << "NOT FOUND" << endl;
#endif
        write(clientsock_fd, "NULL\n", 5);
      }
      else if (command == "COUNT")
      {
        string count = to_string(kv_store.size())+"\n";
        write(clientsock_fd,count.c_str(), sizeof(count));
#if DEBUG == 1
        cout << kv_store.size() << endl;
#endif
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


  close(clientsock_fd);
// }while(true);
  close(sockfd);
  return 0;




}
