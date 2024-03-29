''' ############################### primary file for ftp logic of client #######################################'''

''' importing necessary packages '''
import socket                           # for importing socket functions
import threading                        # for running threads
import os                               # for accessing files, folders on client & server
import sys                              # sys for getting system messages                              
import pdb                              # for python debugging
from lib2to3.fixer_util import String   # for python2 to python3 conversion on the go
import cmd                              # command line utilities

''' important control variables for data and control connection '''
port_for_response = 6548
localhost = "127.0.0.1"
buffer_size = 1024
encoding = "utf-8"

''' class for data connection handling with the server '''
class client_data_thread(threading.Thread):
    
    # importing global variables
    global port_for_response
    global localhost

    ''' class constructor '''
    def __init__(self, cmd, file):
        self.data_port = port_for_response
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((localhost, self.data_port))
        self.sock.listen(1)
        self.sock.settimeout(1)
        self.cmd = cmd
        self.filename = file
        self.current_dir = os.path.abspath("./files/")
        threading.Thread.__init__(self)

    # class implementing main threading function 'run' of threading.Thread
    def run(self):
        ''' listing out commands for data connect handling'''
        try:
            self.data_connection, addr = self.sock.accept()
        except:
            return
        if self.cmd == "dir":
            self.dir()
        elif self.cmd == "get":
            self.get()
        elif self.cmd == "upload":
            self.upload()
        else:
            print('unknown data thread command encountered...')

    # command printing out directory listing results
    def dir(self):
        global encoding
        global buffer_size
        print("\nReceiving file list\n")
        total_payload = ""
        data = str(self.data_connection.recv(buffer_size), encoding)
        while data:
            total_payload += data
            data = str(self.data_connection.recv(buffer_size), encoding)
        print("\nFTP File List\n--------------\n" + total_payload + "\n")
        self.data_connection.close()

    # get command of data connection to get file bytes from server
    def get(self):
        try:
            complete_path = os.path.join(self.current_dir, self.filename)
            print ("Full Dir: " + complete_path)
            f = open(complete_path, "wb+")
            print ("Retrieving file: " + self.filename)
            try:
                data = self.data_connection.recv(buffer_size)
                while data:
                    f.write(data)
                    data = self.data_connection.recv(buffer_size)
            except:
                print("Problem receiving data")
            f.close()
            print("File received.")
        except:
            if not f.closed:
                f.close()
            print("Cannot open file on client side...")
        self.data_connection.close()
    
    # upload command for sending file bytes from client directory to server directory
    def upload(self):
        complete_path = os.path.join(self.current_dir, self.filename)
        print("Full Dir " + complete_path)
        try:
            f = open(complete_path, "rb")
            print("Storing to file: " + self.filename)
        except:
            print("File not found")
            self.data_connection.close()
            return

        while True:
            self.data = f.read(8)
            if not self.data: break
            self.data_connection.sendall(self.data)
        f.close()

''' class for continously receiving responses from server '''
class client_response(threading.Thread):
    # class constructor
    def __init__(self, conn):
        self.conn = conn
        threading.Thread.__init__(self)

    # method for threading
    def run(self):
        while True:
            self.empty()
    
    # read server response until empty
    def empty(self):
        global buffer_size
        global encoding
        try:
            response = str(self.conn.recv(buffer_size), encoding)
            print(response)
        except:
            return

''' main class for ftp client '''
class client_for_ftp:
    def __init__(self):
        self.control_socket = socket.socket()
        self.control_socket.settimeout(2)
        self.current_directory = os.path.abspath("./downloads/")
        if not os.path.exists(self.current_directory):
            os.makedirs(self.current_directory)
        
        # keep taking in commands from terminal
        while True:
            input_arr = input("\n punch in your command, please :").lower().split(" ")
            if input_arr[0] == "authenticate":
                self.authenticate(input_arr)
            elif input_arr[0] == "ftpclient":
                self.ftpclient(input_arr)
            elif input_arr[0] == "dir":
                self.dir(input_arr)
            elif input_arr[0] == "get":
                self.get(input_arr)
            elif input_arr[0] == "upload":
                self.upload(input_arr)
            elif input_arr[0] == "close":
                self.close(input_arr)
            else:
                print("unknown command : '"+input_arr[0]+"'")

    # command for connecting to the ftp server
    def ftpclient(self, input_arr):
        # check the parameters passed
        if len(input_arr) != 3:
            print("parameters passed to ftpclient command were not correct. use format : <IP> <port>")
            print("trying connecting using the default connection strings ...")
            input_arr = ["ftpclient", "127.0.0.1", 7711]

        # convert port to integer value
        try:
            ctrlPort = int(input_arr[2])
        except ValueError:
            print("Invalid port number")
            return

        # setting up tcp connection for tcp (control connection)
        try:
            self.control_socket.connect((input_arr[1], ctrlPort))
            self.client_response = client_response(self.control_socket)
            self.client_response.setDaemon(True)
            self.client_response.start()
        except ConnectionRefusedError:
            print("Connection refused - check port number")
            return
        except OSError:
            print("Connect request was made on an already connected socket or the server is not listening on that port.")
            return

        print("Connection established on port {}.".format(ctrlPort))
    
    # command for client authentication
    def authenticate(self, input_arr):
        if len(input_arr) < 3:
            print("invalid command, correct format : authenticate <username> <password>")
            return
        try:
            self.send_to_server("authenticate "+input_arr[1]+" "+input_arr[2])
        except:
            print("seems you've not connected to the ftp server, please try again...")
            return
        self.data_port_connect(cmd = "authenticate", file="")
    
    # command for listing server directory
    def dir(self, input_arr):
        if len(input_arr) > 1:
            print("invalid command, dir requires no additional arguments...")
            return
        try:
            self.send_to_server("dir")
        except:
            print("seems you've not connected to the ftp server, please try again...")
            return
        self.data_port_connect(cmd="dir")

    # command for file download
    def get(self, input_arr):
        if len(input_arr) != 2:
            print("invalid command, get <filename> is the correct format...")
            return
        name = input_arr[1]
        try:
            self.send_to_server("get "+name)
        except:
            print("seems you've not connected to the ftp server, please try again...")
            return
        self.data_port_connect(cmd="get", file=name)
    
    # command for file upload
    def upload(self, input_arr):
        if len(input_arr) != 2:
            print("invalid command, upload <filename> is the correct format...")
            return
        name = input_arr[1]
        try:
            self.send_to_server("upload "+name)
        except:
            print("seems you've not connected to the ftp server, please try again...")
            return
        self.data_port_connect(cmd="upload",file=name)

    # command for closing the client connection
    def close(self, input_arr):
        if len(input_arr) != 1:
            print("close command requires no argument ...")
            return
        else:
            try:
                self.send_to_server("close")
            except:
                exit()
        self.client_response.empty()
        exit()
        return

    # send message to server
    def send_to_server(self, msg="", encoding="utf-8"):    
        self.control_socket.sendall(bytearray(msg+"\r\n", encoding))

    # command for communicating with server's data port
    def data_port_connect(self, cmd ="", file=""):
        try:
            cdt = client_data_thread(cmd=cmd, file=file)
            cdt.start()
            cdt.join()
        except:
            print("error occured in client data thread: ", sys.exc_info()[0])
            print("error occured in client data thread: ", sys.exc_info()[1])
            print("error occured in client data thread: ", sys.exc_info()[2])
            exit()

# main entry point for ftp server
if __name__ == '__main__':
    client = client_for_ftp()
