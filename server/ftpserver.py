''' ############################### primary file for ftp logic of server #######################################'''

''' importing necessary packages'''
import socket                           # for importing socket functions
import threading                        # for running threads
import os                               # for accessing files, folders on client & server
import sys                              # sys for getting system messages
import pdb                              # for python debugging
import math
from fsplit.filesplit import FileSplit
import json

''' important control variables for data and control connection '''
data_thread_message = ""                # variable for storing data messages
localhost = "127.0.0.1"                 # local ip of ftp server
encoding = "utf-8"                      # encoding standard for info. exchange
buffer_size = 1024                      # buffer size of info. exchange
command_port = 7711                     # port of connection of server
data_port = 6548                        # port of data connection
chuck_dictionary = {}
list_of_split_files = []
next_segment = 1
''' server side data connection handler '''
class ftp_data_handler(threading.Thread):

    # class constructor
    def __init__(self, socket, cmd, data, filename = ""):
        self.data_socket = socket
        self.cmd = cmd
        self. data = data
        self.filename = filename
        threading.Thread.__init__(self)

    # run method for threading.Thread
    def run(self):
        print('printing self.cmd', self.cmd)
        if self.cmd == "dir":
            self.dir()
        elif self.cmd == "get":
            self.get()
        elif self.cmd == "upload":
            self.upload(self.filename)
        else:
            print("command not registered with the server. please try again....\r\n")

    # method for handling dir command
    def dir(self):
        global data_thread_message
        global localhost
        global data_port
        global encoding
        try:
            self.data_socket.connect(("127.0.0.1",6548))
            self.data_socket.sendall(bytearray(self.data,encoding))
            self.data_socket.close()
            data_thread_message = "closing data connection..."
        except:
            data_thread_message = "oops! couldn't open data connection..."

    # method for handling get command
    def get(self):
        global data_thread_message
        global localhost
        global data_port
        try:
            self.data_socket.connect((localhost, data_port))
            self.data_socket.sendall(bytearray(self.data))
            self.data_socket.close()
            data_thread_message = "data sent! closing data connection..."
        except:
            data_thread_message = "oops! couldn't open data connection..."

    # method for handling upload command
    def upload(self, filename):
        global data_thread_message
        global localhost
        global data_port
        try:
            self.data_socket.connect((localhost, data_port))
        except:
            data_thread_message = "oops! couldn't open data connection..."

        try:
            try:
                data = self.data_socket.recv(buffer_size)
                if data :
                    file = open(filename, "wb")
                    print(filename)
                    while True:
                        if not data:
                            break
                        print(data)
                        file.write(data)
                        data = self.data_socket.recv(buffer_size)
                    file.close()
                    data_thread_message = "file uploaded! closing data connection..."
            except:
                data_thread_message = "oops! data thread unable to recieve data..."
        except:
            data_thread_message = "oops! error opening uploaded file..."

''' server side control connection handler '''
class ftp_command_handler(threading.Thread):

    # class constructor
    def __init__(self, socket):
        self.socket = socket
        self.curr_dir = os.path.abspath("./files/")
        self.split_dir = os.path.abspath("./files/splitfiles")
        self.data_socket = None
        threading.Thread.__init__(self)
        self.finished_running = False
        self.is_authenticated = False
    # run method for implementing threading.Thread
    def run(self):
        while True:
            command = str(self.socket.recv(1024), "utf-8")
            if command:
                if not command.endswith("\r\n"):
                    self.send_back_resp("command not correct, try again...1")
                    continue
            split = command.rstrip("\r\n").lower().split(" ")
            print("checkpoint - calling command",split)
            try:
                getattr(self,split[0])(split)
            except:
                self.send_back_resp("command not correct, try again...2")
            if self.finished_running:
                return

    # method for handling authentication
    def authenticate(self, commands):
        username = commands[1]
        password = commands[2]
        if username == "user" and password == "pass":
            self.is_authenticated = True
            self.send_next_segment()
        else:
            self.is_authenticated = False
            self.send_back_resp("wrong username or password")

    def send_next_segment(self):
        global next_segment

        dict_to_send = {
        "my_segment": str(next_segment),
        "chunk_list": chuck_dictionary
        }
        print(dict_to_send)
        self.send_back_resp(json.dumps(dict_to_send))
        next_segment+=1

    # method for sending back response to client
    def send_back_resp(self, msg, encoding=encoding):
        print("sending back command response..."+msg)
        self.socket.sendall(bytearray(msg+"\r\n",encoding))

    # method for closing control connection
    def close(self, commands):
        self.send_back_resp("closing control connection")
        self.socket.close()
        self.finished_running = True

    # method for sending messages for buggy commands
    def send_err_response(self):
        self.send_back_resp("syntax error in command arugments")

    # control method for implementing dir command
    def dir(self, commands):
        if not self.is_authenticated:
            self.send_back_resp("client not authenticated, use authenticate <username> <password> to get authenticated..")
            return

        if len(commands) > 2:
            return

        dir = commands[1] if len(commands) == 2 else self.curr_dir
        # socket for streaming data using IPV4 addresses
        data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            files = os.listdir(self.curr_dir)
            files.sort()
            fs = ""
            for file in files:
                fs += file + "\n"
            # data thread
            dt = ftp_data_handler(socket=data_socket, cmd ="dir", data = fs)
            self.send_back_resp("\nabout to open data connection for listing files in directory ...")
            dt.start()
            dt.join()
            self.send_back_resp(data_thread_message)
        except:
            self.send_back_resp("\ninternal error in developing data connection with server...")

    # control method for implementing get command
    def get(self, commands):
        if not self.is_authenticated:
            self.send_back_resp("client not authenticated, use authenticate <username> <password> to get authenticated..")
            return

        global data_thread_message
        if len(commands) is not 2:
            self.send_err_response()
            print("get command: no filename mentioned!")
            return
        name = commands[1]
        print("trying to get file with name :"+name)
        name = os.path.join(self.split_dir, name)
        print("path resolved as : "+name+"...")

        if not os.path.exists(name):
            self.send_back_resp("file not found on server. please try again...")
            return
        print("file found! getting it...")

        if os.access(name, os.R_OK):
            ds = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            data_from_file = ""
            try:
                file = open(name,"rb")
                data = file.read()
                dt = ftp_data_handler(socket=ds, cmd="get",data=data)
                self.send_back_resp("opening data connection...")
                print("started thread for data connection...")
                dt.start()
                dt.join()
                if data_thread_message == "":
                    self.send_back_resp("closing data connection...")
                else:
                    self.send_back_resp(data_thread_message)
            except:
                self.send_back_resp("file not found...")
        else:
            self.send_back_resp("server does not have access to the file")
        return

    # control method for implementing upload command
    def upload(self, commands):
        if not self.is_authenticated:
            self.send_back_resp("client not authenticated, use authenticate <username> <password> to get authenticated..")
            return

        if len(commands) is not 2:
            self.send_err_response()
            print("upload error: filename missing in command")
            return
        name = commands[1]
        name = os.path.join(self.curr_dir, name)
        if os.access(name, os.R_OK) or not os.path.isfile(name):
            ds = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                dt = ftp_data_handler(socket=ds, cmd="upload", data="",filename=name)
                self.send_back_resp("opening data connection for file upload...")
                dt.start()
                dt.join()
                if data_thread_message == "":
                    self.send_back_resp("closing data connection ...")
                else:
                    self.send_back_resp(data_thread_message)
            except:
                self.send_back_resp("unable to create new file or find an existing file with same name...")
        else:
            self.send_back_resp("file unavailable")
            return

''' class for running ftp server and handling client connections '''
class ftp_server:
    def join(self,fromdir, tofile):
        output = open(tofile, 'wb')
        parts  = os.listdir(fromdir)
        parts.sort()
        for filename in parts:
            filepath = os.path.join(fromdir, filename)
            fileobj  = open(filepath, 'rb')
            while 1:
                filebytes = fileobj.read()
                if not filebytes: break
                output.write(filebytes)
            fileobj.close(  )
        output.close(  )

    def split(self,fromfile, todir, chunksize):
        if not os.path.exists(todir):                  # caller handles errors
            os.mkdir(todir)                            # make dir, read/write parts
        else:
            for fname in os.listdir(todir):            # delete any existing files
                os.remove(os.path.join(todir, fname))
        partnum = 0
        input = open(fromfile, 'rb')                   # use binary mode on Windows
        while 1:                                       # eof=empty string from read
            chunk = input.read(chunksize)              # get next part <= chunksize
            if not chunk: break
            partnum  = partnum+1
            filename = os.path.join(todir, ('part%04d.split' % partnum))
            fileobj  = open(filename, 'wb')
            fileobj.write(chunk)
            fileobj.close()                            # or simply open(  ).write(  )
        input.close(  )
        assert partnum <= 9999                         # join sort fails if 5 digits
        return partnum

    def __init__(self):
        # self.port = 7711
        curr_dir = os.path.abspath("./files/")
        split_dir = os.path.abspath("./files/splitfiles")
        fs = self.split(fromfile=str(curr_dir)+"/test.pdf", todir = split_dir, chunksize=100000)
        for file in os.listdir(split_dir):
            list_of_split_files.append(file)
        no_of_files = len(list_of_split_files)
        list_of_split_files.sort()
        for i in range(0,no_of_files):
            key = (i%5)+1
            chuck_dictionary[key] = chuck_dictionary.get(key,[])+[ list_of_split_files[i]]
        self.server_socket = socket.socket()
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(("localhost", command_port))
        self.server_socket.listen(1)

        while True:
            try:
                print("\nwaiting for connection on " + self.server_socket.getsockname()[0] + ":" + str(self.server_socket.getsockname()[1]))
                conn, addr = self.server_socket.accept()
                print("accepted command connection: " + addr[0] + ":" + str(addr[1]))
                fct = ftp_command_handler(conn)
                fct.start()
            except:
                print("Unexpected error: ", sys.exc_info()[0])
                print("Unexpected error: ", sys.exc_info()[1])
                print("Unexpected error: ", sys.exc_info()[2])
                self.server_socket.close()
                exit()

''' entry into the ftp server script '''
if __name__ == '__main__':
    server = ftp_server()
