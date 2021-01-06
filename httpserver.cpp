
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <err.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>


const int MAX_SIZE = 16000;
const int MAP_SIZE = 128;

int out = 0;
int nthreads;
int offset = 0;
int mapOffset = 65;
int logging = 0;
int log_file, mapping_file = -1;
int *sockBuffer;
sem_t empty, full, mutex, offsetMutex, mapMutex;

void writeHeader(int sock, int contentLength, char respCode[]) {
    char http[] = "HTTP/1.1";
    char ok[] = "OK";
    char created[] = "Created";
    char badreq[] = "Bad Request";
    char forbid[] = "Forbidden";
    char notfound[] = "Not Found";
    char ise[] = "Internal Server Error";
    char clText[] = "Content-Length: ";
    char cLength[50];
    sprintf(cLength, "%d", contentLength);

    char header[MAX_SIZE];
    int size = 0;
    strcpy(header, http);
    size += strlen(http);
    strcat(header, " ");
    size += 1;
    strcat(header, respCode);
    size += strlen(respCode);
    strcat(header, " ");
    size += 1;

    if (strcmp(respCode, "200") == 0) {
        strcat(header, ok);
        size += strlen(ok);
    } else if (strcmp(respCode, "201") == 0) {
        strcat(header, created);
        size += strlen(created);
    } else if (strcmp(respCode, "400") == 0) {
        strcat(header, badreq);
        size += strlen(badreq);
    } else if (strcmp(respCode, "403") == 0) {
        strcat(header, forbid);
        size += strlen(forbid);
    } else if (strcmp(respCode, "404") == 0) {
        strcat(header, notfound);
        size += strlen(notfound);
    } else if (strcmp(respCode, "500") == 0) {
        strcat(header, ise);
        size += strlen(ise);
    }

    strcat(header, "\r\n");
    size += 2;
    strcat(header, clText);
    size += strlen(clText);
    strcat(header, cLength);
    size += strlen(cLength);
    strcat(header, "\r\n\r\n");
    size += 4;

    write(sock, header, size);
}

char* checkResource(char* head, int requiredLength) {
    char* err = (char*)malloc(sizeof(char)*5);
    strcpy(err, "error");
    char* firstSpace;
    if ((firstSpace = strstr(head, " ")) != NULL) {
        char* secondSpace;
        if ((secondSpace = strstr(&head[firstSpace-head+1], " ")) != NULL) {
            int resourceIdx = firstSpace - head + 1;
            if (head[resourceIdx] == '/')
                resourceIdx += 1;
            int endResIdx = secondSpace - head;
            int resLength = endResIdx - resourceIdx;
            char* resource = (char*)malloc(sizeof(char) * (resLength+1));
            strncpy(resource, &head[resourceIdx], resLength);
            resource[resLength] = '\0';
            if (requiredLength == -1) {
                requiredLength = strlen(resource);
            }
            if (strlen(resource) == (size_t)requiredLength) {
                for (int i=0; i<requiredLength; i++) {
                    char c = resource[i];
                    int numFlag = 0;
                    int alpFlag = 0;
                    int othFlag = 0;
                    if (c >= 48 && c <= 57) {
                        numFlag = 1;
                    } else if ((c >= 65 && c <= 90) || (c >= 97 && c <= 122)) {
                        alpFlag = 1;
                    } else if (c == 45 || c == 95) {
                        othFlag = 1;
                    }
                    if (numFlag + alpFlag + othFlag != 1) {
                        return err;
                    }
                }
                return resource;
            }
            free(&resource);
        }
    }
    return err;
}

void writeFailToLog(char *request, char *resource, char respCode[]) {
    int failLineLength = 34 + strlen(resource) + strlen(request);
    char *failLine = (char*)malloc(failLineLength*sizeof(char));
    sprintf(failLine, "FAIL: %s %s HTTP/1.1 --- response %s\n", request, resource, respCode);
    sem_wait(&offsetMutex);
    int threadOffset = offset;
    offset += failLineLength;
    sem_post(&offsetMutex);
    pwrite(log_file, failLine, failLineLength, threadOffset);
    free(&failLine);
}

void writeToLog(int charNum, char* buffer, int threadOffset, int bufferOffset, int readLength) {
    char line[69] = {0};
    char buf[21];
    char lineNumber[9];

    sprintf(lineNumber, "%d", charNum);
    int lenNumber = strlen(lineNumber);
    char zero[] = "0";
    for (int j=0; j<8-lenNumber; j++) {
        strcat(line, zero);
    }
    strcat(line, lineNumber);

    strncpy(buf, buffer+bufferOffset, readLength);
    buf[readLength] = '\0';
    for (int j=0; j<readLength; j++) {
        char hex[4];
        sprintf(hex, " %02x", buf[j]);
        strcat(line, hex);
    }
    char nl[] = "\n";

    strcat(line, nl);
    pwrite(log_file, line, strlen(line), threadOffset);
}

int checkValidAliasName(char* name) {
    for (int i=0; i<(int)strlen(name); i++) {
        char c = name[i];
        int numFlag = 0;
        int alpFlag = 0;
        int othFlag = 0;
        if (c >= 48 && c <= 57) {
            numFlag = 1;
        } else if ((c >= 65 && c <= 90) || (c >= 97 && c <= 122)) {
            alpFlag = 1;
        } else if (c == 45 || c == 95) {
            othFlag = 1;
        }
        if (numFlag + alpFlag + othFlag != 1) {
            return 0;
        }
    }
    return 1;
}

int findName(char* name) {
    char* file = (char*)malloc(sizeof(char) * (strlen(name)+3));
    strcpy(file, "./");
    strcat(file, name);
    if (access(file, F_OK) != -1) {
        free(&file);
        return 1;
    }
    for (int i=0; i<8000; i++) {
        char line[129];
        int readLength = (i*MAP_SIZE) + 65;
        pread(mapping_file, line, MAP_SIZE, readLength);
        char* existing_alias = strtok(line, ":");
        if (existing_alias != NULL && strcmp(name, existing_alias) == 0) {
            return 1;
        }
    }
    return 0;
}

void setName(char* new_name, char* existing_name) {
    sem_wait(&mapMutex);
    int currOffset = mapOffset;
    mapOffset += MAP_SIZE;
    sem_post(&mapMutex);

    char* aliasSet = (char*)malloc(sizeof(char) * MAP_SIZE);
    strcat(aliasSet, new_name);
    strcat(aliasSet, ":");
    strcat(aliasSet, existing_name);
    strcat(aliasSet, "\n");

    pwrite(mapping_file, aliasSet, MAP_SIZE, currOffset);
    free(&aliasSet);
}

char* getCorrectResource(char* name) {
    char* file = (char*)malloc(sizeof(char) * (strlen(name)+3));
    strcpy(file, "./");
    strcat(file, name);
    if (access(file, F_OK) != -1) {
        return file;
    }
    for (int i=0; i<8000; i++) {
        char line[129];
        int readLength = (i*MAP_SIZE) + 65;
        pread(mapping_file, line, MAP_SIZE, readLength);
        line[MAP_SIZE] = '\0';
        char* actual = strstr(line, ":");
        actual += 1;
        actual[strlen(actual)-1] = '\0';
        char* existing_alias = strtok(line, ":");
        if (existing_alias != NULL && strcmp(name, existing_alias) == 0) {
            return getCorrectResource(actual);
        }
    }
    return NULL;
}

void getRequest(char* head, int sock, char respCode[], int resLength) {
    char* res = checkResource(head, resLength);

    if (strcmp(res, "error") != 0) {
        if (!findName(res)) {
            strcpy(respCode, "404");
            writeHeader(sock, 0, respCode);
            char request[] = "GET";
            if (logging)
              writeFailToLog(request, res, respCode);
            return;
        }

        char* resource = getCorrectResource(res);
        if (resource == NULL) {
            strcpy(respCode, "500");
            writeHeader(sock, 0, respCode);
            char request[] = "GET";
            if (logging)
              writeFailToLog(request, res, respCode);
            return;
        }

        int fd = open(resource, O_RDWR);
        if (fd == -1) {
            if (errno == EACCES) {
                strcpy(respCode, "403");
            } else if (errno == ENOENT) {
                strcpy(respCode, "404");
            } else {
                strcpy(respCode, "500");
            }
            writeHeader(sock, 0, respCode);
            char request[] = "GET";
            if (logging)
              writeFailToLog(request, res, respCode);
            return;
        } else {
            int threadOffset;

            int totalSize = 0;
            int size;
            char buffer[MAX_SIZE];
            while ((size = read(fd, buffer, MAX_SIZE)) > 0) {
                totalSize += size;
            }
            close(fd);

            if (logging) {
                char clString[MAX_SIZE];
                sprintf(clString, "%d", totalSize);
                int clLength = strlen(clString);

                int reserve = 39;
                reserve += clLength;
                int numLinesOf20 = totalSize/20;
                int numCharPerLineOf20 = 69;
                reserve += numLinesOf20 * numCharPerLineOf20;
                if (totalSize % 20 > 0) {
                    int restOfContent = totalSize % 20;
                    int numCharLastLine = 8 + 3 * restOfContent + 1;
                    reserve += numCharLastLine;
                }
                reserve += 9;

                sem_wait(&offsetMutex);
                threadOffset = offset;
                offset += reserve;
                sem_post(&offsetMutex);

                int firstLineCount = 40 + clLength;
                char* firstLine = (char*)malloc(firstLineCount*sizeof(char));

                sprintf(firstLine, "GET %s length %d\n", res, totalSize);
                pwrite(log_file, firstLine, firstLineCount, threadOffset);
                threadOffset += firstLineCount;
                free(&firstLine);
            }

            writeHeader(sock, totalSize, respCode);

            fd = open(resource, O_RDWR);
            while ((size = read(fd, buffer, MAX_SIZE)) > 0) {
                write(sock, buffer, size);
                if (logging) {
                    int c = 0;
                    while(size > 20*(c+1)) {
                        int charNum = 20*c;
                        writeToLog(charNum, buffer, threadOffset, 20*c, 20);
                        threadOffset += 69;
                        c++;
                    }
                    int cRest = size - 20*c;
                    int charNum = 20*c;
                    writeToLog(charNum, buffer, threadOffset, 20*c, cRest);
                    threadOffset += (cRest*3 + 9);
                }
            }
            close(fd);
        }
        free(&resource);
    } else {
      if (resLength > -1) {
          getRequest(head, sock, respCode, -1);
      }
      strcpy(respCode, "400");
      writeHeader(sock, 0, respCode);
      char request[] = "GET";
      if (logging)
        writeFailToLog(request, res, respCode);
    }
}

int getContentLength(char *head, char *cl) {
    int clPos = cl - head;
    clPos += 16;
    int clLength = strlen(head) - 4 - clPos;
    char* length = (char*)malloc(sizeof(char) * (clLength+1));
    strncpy(length, &head[clPos], clLength);
    length[clLength] = '\0';

    int contentLength = atoi(length);
    free(&length);
    return contentLength;
}

void putRequest(char* head, int sock, char respCode[], int resLength) {
    char* res = checkResource(head, resLength);

    if (strcmp(res, "error") != 0) {

        char* resource = getCorrectResource(res);
        if (resource == NULL && resLength == -1) {
            strcpy(respCode, "400");
            writeHeader(sock, 0, respCode);
            char request[] = "GET";
            if (logging)
              writeFailToLog(request, res, respCode);
            return;
        }
        resource = (char*)malloc(sizeof(char) * (strlen(res)+3));
        strcpy(resource, "./");
        strcat(resource, res);

        int fd = open(resource, O_RDWR);
        if (fd == -1 and errno == EACCES) {
            strcpy(respCode, "403");
            writeHeader(sock, 0, respCode);
            return;
        }
        fd = open(resource, O_CREAT | O_EXCL | O_RDWR, 0666);
        if (fd == -1) {
            if (errno == EEXIST) {
                remove(resource);
                fd = open(resource, O_CREAT | O_RDWR, 0666);
                if (fd == -1) {
                    strcpy(respCode, "500");
                    writeHeader(sock, 0, respCode);
                    char request[] = "PUT";
                    if (logging)
                      writeFailToLog(request, res, respCode);
                    return;
                }
            } else {
                strcpy(respCode, "500");
                writeHeader(sock, 0, respCode);
                char request[] = "PUT";
                if (logging)
                  writeFailToLog(request, res, respCode);
                return;
            }
        } else {
            strcpy(respCode, "201");
        }

        char* cl;
        char buffer[MAX_SIZE];
        if ((cl = strstr(head, "Content-Length")) != NULL) {
            int threadOffset;

            int contentLength = getContentLength(head, cl);

            if (logging) {
                int reserve = 39;
                int clPos = cl - head;
                clPos += 16;
                int clLength = strlen(head) - 4 - clPos;
                reserve += clLength;
                int numLinesOf20 = contentLength/20;
                int numCharPerLineOf20 = 69;
                reserve += numLinesOf20 * numCharPerLineOf20;
                if (contentLength % 20 > 0) {
                    int restOfContent = contentLength % 20;
                    int numCharLastLine = 8 + 3 * restOfContent + 1;
                    reserve += numCharLastLine;
                }
                reserve += 9;

                sem_wait(&offsetMutex);
                threadOffset = offset;
                offset += reserve;
                sem_post(&offsetMutex);

                int firstLineCount = 39 + clLength;
                char* firstLine = (char*)malloc(firstLineCount*sizeof(char));

                sprintf(firstLine, "PUT %s length %d\n", res, contentLength);
                pwrite(log_file, firstLine, firstLineCount, threadOffset);
                threadOffset += firstLineCount;
                free(&firstLine);
            }

            if (contentLength > MAX_SIZE) {
                int i = 1;
                while (contentLength > MAX_SIZE*i) {
                    read(sock, buffer, MAX_SIZE);
                    write(fd, buffer, MAX_SIZE);
                    if (logging) {
                        int c = 0;
                        while(MAX_SIZE > 20*c) {
                            int charNum = MAX_SIZE*(i-1) + 20*c;
                            writeToLog(charNum, buffer, threadOffset, 20*c, 20);
                            threadOffset += 69;
                            c++;
                        }
                    }
                    i++;
                }
                int rest = contentLength - MAX_SIZE*(i-1);
                read(sock, buffer, rest);
                write(fd, buffer, rest);
                if (logging) {
                    int c = 0;
                    while(rest > 20*(c+1)) {
                        int charNum = MAX_SIZE*(i-1) + 20*c;
                        writeToLog(charNum, buffer, threadOffset, 20*c, 20);
                        threadOffset += 69;
                        c++;
                    }
                    int cRest = rest - 20*c;
                    int charNum = contentLength - cRest;
                    writeToLog(charNum, buffer, threadOffset, 20*c, cRest);
                    threadOffset += (cRest*3 + 9);
                }
            } else {
                int size = read(sock, buffer, contentLength);
                write(fd, buffer, size);
                if (logging) {
                    int c = 0;
                    while(size > 20*(c+1)) {
                        int charNum = 20*c;
                        writeToLog(charNum, buffer, threadOffset, 20*c, 20);
                        threadOffset += 69;
                        c++;
                    }
                    int cRest = size - 20*c;
                    int charNum = 20*c;
                    writeToLog(charNum, buffer, threadOffset, 20*c, cRest);
                    threadOffset += (cRest*3 + 9);
                }
            }
            char lastLine[] = "========\n";
            pwrite(log_file, lastLine, 9, threadOffset);
        } else {
          strcpy(respCode, "400");
          char request[] = "PUT";
          if (logging)
            writeFailToLog(request, res, respCode);
        }
        close(fd);
        free(&resource);
    } else {
        if (resLength > -1) {
            getRequest(head, sock, respCode, -1);
        }
        strcpy(respCode, "400");
        char request[] = "PUT";
        if (logging)
          writeFailToLog(request, res, respCode);
    }
    writeHeader(sock, 0, respCode);
}

void patchRequest(char* head, int sock, char respCode[]) {
    char* res = checkResource(head, -1);
    int contentLength = 0;
    char* cl;
    if ((cl = strstr(head, "Content-Length")) != NULL) {
        contentLength = getContentLength(head, cl);
    }

    char buf[2];
    char *body = (char*)malloc(sizeof(char) * (contentLength+1));
    while (read(sock, buf, 1) > 0) {
        strcat(body, buf);
        if (strlen(body) > 2) {
            if (strcmp(&body[strlen(body)-2], "\r\n") == 0) {
                break;
            }
        }
        if (strlen(body) == (size_t)contentLength) {
            break;
        }
        buf[1] = '\0';
    }

    if (strlen(body) > MAP_SIZE+7) {
        strcpy(respCode, "400");
        if (logging) {
            char request[] = "PATCH";
            writeFailToLog(request, res, respCode);
        }
        writeHeader(sock, 0, respCode);
        free(&body);
        return;
    }

    char alias[6];
    strncpy(alias, body, 5);
    alias[5] = '\0';
    if (strcmp(alias, "ALIAS") != 0) {
        strcpy(respCode, "400");
        if (logging) {
            char request[] = "PATCH";
            writeFailToLog(request, res, respCode);
        }
        writeHeader(sock, 0, respCode);
        free(&body);
        return;
    }

    char* names = strstr(body, " ");
    names += 1;
    char* new_name = strstr(names, " ");
    new_name += 1;
    new_name[strlen(new_name)-1] = '\0';
    char* existing_name = strtok(names, " ");

    if (!checkValidAliasName(existing_name)) {
        strcpy(respCode, "400");
        if (logging) {
            char request[] = "PATCH";
            writeFailToLog(request, res, respCode);
        }
        writeHeader(sock, 0, respCode);
        free(&body);
        return;
    }
    if (!findName(existing_name)) {
        strcpy(respCode, "404");
        if (logging) {
            char request[] = "PATCH";
            writeFailToLog(request, res, respCode);
        }
        writeHeader(sock, 0, respCode);
        free(&body);
        return;
    }

    if (findName(new_name)) {
        strcpy(respCode, "400");
        if (logging) {
            char request[] = "PATCH";
            writeFailToLog(request, res, respCode);
        }
        writeHeader(sock, 0, respCode);
        free(&body);
        return;
    }

    setName(new_name, existing_name);

    strcpy(respCode, "200");
    if (logging) {
        char request[] = "PATCH";
        writeFailToLog(request, res, respCode);
    }
    writeHeader(sock, 0, respCode);
    free(&body);
}

void readSocket(int new_socket) {
    char respCode[] = "200";

    char buf[2];
    char header[MAX_SIZE] = {0};
    while (read(new_socket, buf, 1) > 0) {
        strcat(header, buf);
        if (strlen(header) > 2) {
            if (strcmp(&header[strlen(header)-4], "\r\n\r\n") == 0) {
                break;
            }
        }
        buf[1] = '\0';
    }

    char request[4];
    strncpy(request, header, 3);
    request[3] = '\0';
    if (strcmp(request, "PUT") == 0) {
        putRequest(header, new_socket, respCode, 27);
    } else if (strcmp(request, "GET") == 0) {
        getRequest(header, new_socket, respCode, 27);
    } else {
        char patch[6];
        strncpy(patch, header, 5);
        patch[5] = '\0';
        if (strcmp(patch, "PATCH") == 0) {
            patchRequest(header, new_socket, respCode);
        } else {
            strcpy(respCode, "400");
            writeHeader(new_socket, 0, respCode);
        }
    }

    close(new_socket);
}

void *consume(void* arg) {
    int sock = *((int*)arg);
    while(1) {
        sem_wait(&full);
        sem_wait(&mutex);

        sock = sockBuffer[out];
        out = (out + 1) % nthreads;

        sem_post(&mutex);
        sem_post(&empty);

        readSocket(sock);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        perror("Usage: httpserver <hostname> [port] [-N <nthreads>] [-l <logfile>] -a <mappingfile");
        return 1;
    }

    char* hostname = (char*)malloc(sizeof(char) * strlen(argv[1]));
    strcpy(hostname, argv[1]);
    char* port = (char*)malloc(sizeof(char) * MAX_SIZE);
    nthreads = 4;

    if (argc >= 3) {
        strcpy(port, argv[2]);
    } else {
        strcpy(port, "8888");
    }

    int opt;

    while ((opt=getopt(argc, argv, "N:l:a:")) != -1) {
        switch (opt) {
            case 'N':
            nthreads = atoi(optarg);
            break;
            case 'l':
            log_file = open(optarg, O_CREAT | O_RDWR, 0666);
            logging = 1;
            break;
            case 'a':
            mapping_file = open(optarg, O_CREAT | O_RDWR, 0666);
            break;
            default:
            break;
        }
    }

    if (mapping_file == -1) {
        perror("Usage: httpserver <hostname> [port] [-N <nthreads>] [-l <logfile>] -a <mappingfile");
        return 1;
    }

    // "Magic Number": BR6W7PPO9Y6U5IJFSUYLRURZISHE7CICMUBAV573OHELBBQZYRNBRTEJYG21MLL6
    char magicNumber[66] = "BR6W7PPO9Y6U5IJFSUYLRURZISHE7CICMUBAV573OHELBBQZYRNBRTEJYG21MLL6\n";
    char key[66];
    int keySize = pread(mapping_file, key, 65, 0);
    key[65] = '\0';
    if (keySize == 0) {
        pwrite(mapping_file, magicNumber, 65, 0);
    } else if (keySize != 65) {
        perror("MappingFileError: Mapping file is not a valid mapping file");
        return 1;
    } else {
        if (strcmp(key, magicNumber) != 0) {
            perror("MappingFileError: Mapping file is not a valid mapping file");
            return 1;
        }
    }

    char mapBuffer[MAP_SIZE+1];
    int mapSize;
    while ((mapSize = pread(mapping_file, mapBuffer, MAP_SIZE, mapOffset)) > 0) {
        char emptyMapping[MAP_SIZE] = {0};
        mapBuffer[MAP_SIZE] = '\0';
        if (strcmp(mapBuffer, emptyMapping) == 0) {
            break;
        }
        mapOffset += MAP_SIZE;
    }

    int in = 0;
    int main_socket, new_socket;
    struct addrinfo *addrs, hints = {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostname, port, &hints, &addrs) != 0) {
        perror("getaddrinfo");
        return 1;
    }
    if ((main_socket = socket(addrs->ai_family, addrs->ai_socktype, addrs->ai_protocol)) == 0) {
        perror("socket failed");
        return 1;
    }
    int enable = 1;
    if (setsockopt(main_socket, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable))) {
        perror("setsockopt");
        return 1;
    }
    if (bind(main_socket, addrs->ai_addr, addrs->ai_addrlen) < 0) {
        perror("bind failed");
        return 1;
    }
    if (listen(main_socket, 16) < 0) {
        perror("listen");
        return 1;
    }

    pthread_t *consumers = (pthread_t*)malloc(nthreads*sizeof(pthread_t));
    sockBuffer = (int*)malloc(nthreads*sizeof(int));
    sem_init(&empty, 0, nthreads);
    sem_init(&full, 0, 0);
    sem_init(&mutex, 0, 1);
    sem_init(&offsetMutex, 0, 1);
    sem_init(&mapMutex, 0, 1);

    for (int i=0; i<nthreads; i++) {
        sockBuffer[i] = 0;
        pthread_create(&consumers[i], NULL, consume, (void*)&i);
    }

    while(1) {
        if ((new_socket = accept(main_socket, NULL, NULL)) < 0) {
            perror("accept");
            return 1;
        }

        sem_wait(&empty);
        sem_wait(&mutex);

        sockBuffer[in] = new_socket;
        in = (in + 1) % nthreads;

        sem_post(&mutex);
        sem_post(&full);
    }

    free(&hostname);
    free(&port);
    free(&sockBuffer);
    free(&consumers);
}
