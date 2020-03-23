"""
:Authors: Fariha Zainab
:Assignment: Bully Algorithm

:Running the code: You would have to pass the port number on command line
"""
import pickle
import socket
import select
import queue as Queue
import sys
import time
from datetime import datetime, timedelta
import random

backlog = 100
buffer_size = 4096

class Bully(object):
    """
    Class to perform the bully algorithm as required by the lab2 assignment
    """
    ElectionMessageTag = 'ELECTION'
    CoordinatorMessageTag = 'COORDINATOR'
    ProbeMessageTag = 'PROBE'

    def __init__(self, gcd_host, gcd_port, id1, id2):
        """
        Construct a lab2 object to talk to Group Coordinator Daemon
        :param gcd_host: host name of gcd
        :param gcd_port: port number og gcd
        """
        self.process_id = (id1, id2)
        self.selectTimeout = 2   #seconds
        self.gcdAddress = (gcd_host, gcd_port)
        self.listenAddress = ()
        self.members = {}
        self.timeout = 1.5 #seconds
        self.electionMessage = ()
        self.coordinatorMessage = ()
        self.readSockets = []
        self.writeSockets = []
        self.isElectionInProgress = False
        self.leader = None
        self.state = "QUIESCENT"
        self.messageToSend = {}
        self.messageToReceive = {}
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clientAddresses = {}
        self.electionMsgSendTime = {}
        self.activeProcesses = {}
        self.expectedElectionResponse = True
        self.probeInProgress = False
        self.max_time_for_response = datetime.now()
        self.probeSocket = None
        self.probe_msg_send_time = datetime.now()
        self.probeTime = random.uniform(0.5, 3) #seconds
        self.lastProbeTime = datetime.now()
        self.failureTime = random.uniform(10, 30) #seconds
        self.failureTrackingTime = datetime.now() + timedelta(seconds = self.failureTime)    

    def CreateSockets(self):
        NewSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        NewSocket.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        NewSocket.setblocking(0)
        NewSocket.settimeout(self.timeout)
        return NewSocket

    def StartListening(self):
        """
        Create a server socket for the lab2 process on which it would listen the client request
        :param port: port passed to the server
        :param backlog: indicates how many incoming connections should be queued up.
        """
        self.server.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
        self.server.setblocking(0)
        self.server.bind(('localhost', 0))
        self.listenAddress = self.server.getsockname()
        self.server.listen(backlog)
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Listening at {}".format(self.listenAddress))
        self.readSockets.append(self.server)
    
    def ProcessDetails(self):
        """
        Print out all the details about the Process
        """
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Details of the Process")
        print("\n====================================================\n")
        date_of_birthday = datetime.now() + timedelta(days=days_to_birthday)
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Birthday of the process {}".format(date_of_birthday.strftime('%d %B %Y')))
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Process Id: ({})".format(self.process_id))
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Process's Listening address: ({})".format(self.listenAddress))
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("State of the process: {}".format(self.state))
        print("\n====================================================\n")

    def join_group(self):
        """
        Send the Join message i.e a pair (procedd_ID, listenAddress) to the GCD to join the group.
        Also verbosely print out what it is doing
        """
        self.ProcessDetails()
        S = self.CreateSockets()
        try:
            S.setblocking(1)
            S.connect(self.gcdAddress)
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Connected to GCD server at {}".format(self.gcdAddress))
            join_message = ('JOIN' , (self.process_id, self.listenAddress))
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Sending JOIN message with process_id {} and listenAddress {} to the GCD Server".format(self.process_id, self.listenAddress))
            self.send_message(S, join_message)
            self.members = self.recv_message(S, buffer_size)
            if(self.members == None):
                sys.exit(1)
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Received the response from GCD Server")
            print("\n====================================================\n")
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("STARTING THE BULLY ALGORITHM")
            print("\n====================================================\n")
            self.SelectElectionMembers(False)
        except socket.gaierror as e:
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Address Error: {}".format(e))
            sys.exit(1)
        except socket.error as e:
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Socket error: {}".format(e))
            sys.exit(1)
        except socket.timeout as e:
            print("\n Connection Timeout occured")

    def RemoveFromReadWriteList(self, s):
        """
        Remove the sockets from the readSockets and writeSockets list
        """
        if s in self.readSockets:
            self.readSockets.remove(s)
        if s in self.writeSockets:
            self.writeSockets.remove(s)

    def StopProbeIfInProgress(self):
        """
        Closing the probe sockets
        Resetting the probeInProgress flag
        """ 
        if self.probeSocket is not None:
            self.RemoveFromReadWriteList(self.probeSocket)
            self.ShutDownSocket(self.probeSocket)
            self.probeSocket = None
            self.probeInProgress = False

    def SelectElectionMembers(self, skipLeader):
        """
        Shuts down any probe in progress
        Select the group members to send the election message to
        """
        self.StopProbeIfInProgress()
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Starting an Election")
        self.state = "ELECTION_IN_PROGRESS"
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("The Current state of the process {}".format(self.state))
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Checking for the members with higher process ID than mine")
        electionMembers = {}

        for id, addr in self.members.items():
            if skipLeader and self.leader is not None and self.leader[0] == id:
                continue
            if self.is_greater(id) == True:
                electionMembers[id] = addr

        if not electionMembers:                        #Check if there is no bigger process
            if len(self.members) == 1:
                print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                print("No other process present to send the COORDINATOR Message")
                self.leader = self.process_id
                print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                print("The leader is ({})".format(self.leader))
                self.state = "QUIESCENT"
                print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                print("The Current state of the process {}".format(self.state))
                self.SendReceiveResponse()
            else:
                print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                print("I have the biggest process_Id")            
                self.Coordinator(self.members)
                self.SendReceiveResponse()
        else:
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Process with bigger IDs {}".format(electionMembers))
            self.Election(electionMembers)
            self.SendReceiveResponse()
                
    def Election(self, electionMembers):
        """
        Sends the election message to all the processes whose Id is greater than lab2 process_id
        :param electionMembers: Dictionary of the process with id greater than lab2
        """
        for values, addr in electionMembers.items():
            if values != self.process_id:
                try:
                    peer = self.CreateSockets()
                    print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                    print("Trying to Connect to Process: {}".format(values))
                    peer.connect(addr)
                    self.clientAddresses[peer] = addr
                    self.electionMessage = (Bully.ElectionMessageTag, self.members)
                    self.send_message(peer, self.electionMessage)
                    self.electionMsgSendTime[peer] = datetime.now()
                    self.max_time_for_response = datetime.now()
                    self.messageToReceive[peer] = "OK"
                    print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                    print("Sending ELECTION message to the process ({})".format(values))
                    self.readSockets.append(peer)
                except socket.error:
                    pass

    def HandleServerSocket(self):
        """
        Handle the Connection requests received on the Server Socket
        """
        try:
            conn, addr = self.server.accept()
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("New connection request from {}".format(addr))
            self.clientAddresses[conn] = addr
            conn.setblocking(0)
            self.readSockets.append(conn)
        except socket.gaierror:
            pass
        except socket.error:
            pass
        except socket.timeout:
            pass

    def UpdateMembers(self, clientRequest):
        """
        Update the list of processes
        :param clientRequest: The list of members sent along with the Election and Coordinator message
        """
        self.members = clientRequest[1]

    def SetLeader(self, clientRequest):
        """
        Check the Active member list received from Coordinator and Extract the highest process ID as the leader
        :param clientRequest: The list of members sent along with the Election and Coordinator message
        """
        maxId = (0, 0)
        for id in clientRequest.keys():
            if id[0] > maxId[0]:
                maxId = id
            elif id[0]==maxId[0]:
                if id[1] > maxId[1]:
                    maxId = id
        return maxId

    def HandleElectionMessageSockets(self, s, clientRequest):
        """
        Handles the election message received from other process
        Save the response to be send on this socket in messageToSend dictionary
        :param s: Socket on which Election message is received
        """
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Received ELECTION message from {}".format(self.clientAddresses[s]))
        self.UpdateMembers(clientRequest) 
        if s not in self.writeSockets:
            self.readSockets.remove(s)
            self.writeSockets.append(s)
            self.messageToSend[s] = "OK"
        else:
            print("\n Unexpected Socket in write list")

    def HandleCoordinatorMessageSockets(self, s, clientRequest):
        """
        Handles the coordinator message received from the leader
        Sets the new leader
        sets the state 
        :param s: Socket on which Coordinator message is received
        """
        self.leader = self.SetLeader(clientRequest[1])
        self.UpdateMembers(clientRequest)
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Received COORDINATOR message from {}".format(self.leader))
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("The current leader is {}".format(self.leader))
        self.state = "QUIESCENT"
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("The state of process is now {}".format(self.state))
        self.readSockets.remove(s)
        self.clientAddresses.pop(s, None)
        s.shutdown(socket.SHUT_RDWR)
        s.close()

    def HandleProbeMessageSockets(self, s):
        """
        Handles the probe message received from other process
        Save the response to be send on this socket in messageToSend dictionary
        :param s: Socket on which probe message is received
        """
        if s not in self.writeSockets:
            self.readSockets.remove(s)
            self.writeSockets.append(s)
            self.messageToSend[s] = "ALIVE"

    def HandleAliveMessageSockets(self, s):
        """
        Handles the alive message received from other leader
        :param s: Socket on which alive message is received
        """
        self.probeInProgress = False
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Received Alive message from {}".format(self.leader))
        self.readSockets.remove(s)
        self.clientAddresses.pop(s, None)
        self.lastProbeTime = datetime.now()
        s.shutdown(socket.SHUT_RDWR)
        s.close()

    def HandleOkMessageSockets(self, s, clientRequest):
        """
        Handles the ok message received from other processes
        :param s: Socket on which ok message is received
        """
        clientRequest = clientRequest.lower()
        if(clientRequest == "ok"):
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Received OK message from {}".format(self.clientAddresses[s]))
            self.readSockets.remove(s)
            self.clientAddresses.pop(s, None)
            s.shutdown(socket.SHUT_RDWR)
            s.close()

    def HandleFailedReceive(self, s):
        """
        Handling exception if receive message fails from any socket
        """
        if s in self.messageToReceive:
            if self.messageToReceive[s] == "ALIVE":
                self.SelectElectionMembers(True)
            else:
                self.RemoveFromReadWriteList(s)

    def ReadClientSocket(self, s):
        """
        Handles all the read sockets 
        """
        try:
            clientRequest = self.recv_message(s, buffer_size)
            if(clientRequest is None):
                self.HandleFailedReceive(s)
                return
            if "ELECTION" in clientRequest:
                self.HandleElectionMessageSockets(s, clientRequest)
            elif "COORDINATOR" in clientRequest:
                self.HandleCoordinatorMessageSockets(s, clientRequest)    
            elif "PROBE" in clientRequest:
                self.HandleProbeMessageSockets(s)    
            elif (clientRequest.lower() == "alive"):
                self.HandleAliveMessageSockets(s)    
            else:
                self.HandleOkMessageSockets(s, clientRequest)
                
        except socket.gaierror:
            pass

        except socket.error:
            pass

        except socket.timeout:
            pass

    def HandleReadRequests(self, s):
        """
        Handle the message to be read from the sockets that are ready
        :param s: Socket that is in the to_read list of the select, i.e. it is ready to be read
        """
        if s is self.server:
            self.HandleServerSocket()
        else:
            if s in self.electionMsgSendTime.keys():
                receive_time = datetime.now()
                self.expectedElectionResponse = False
                if receive_time <= self.electionMsgSendTime[s] + timedelta(seconds=self.timeout):
                    self.ReadClientSocket(s)
                    self.electionMsgSendTime.pop(s, None)
                else:
                    print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                    print("Received OK message after the timeout")
                    self.ReadClientSocket(s)
            elif s == self.probeSocket:
                self.ReadClientSocket(s)
            else:
                self.ReadClientSocket(s)

    def HandleWriteRequests(self, s):
        """
        Handle the message/response to be written/send back on the sockets that are ready
        :param s: Socket that is in the to_write list of the select, i.e. it is ready to be written on
        """
        if s is self.server:
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("\nUnexpexted write socket from the server")
        else:
            data = self.messageToSend[s]
            data = data.lower()
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("\nSending {} message to {}.".format(data, self.clientAddresses[s]))
            try:
                self.send_message(s, data)
                self.RemoveFromReadWriteList(s)
                s.shutdown(socket.SHUT_RDWR)
                s.close()
                if self.state == "QUIESCENT" and data == "ok":
                    print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                    print("State of the process is {}".format(self.state))
                    self.SelectElectionMembers(False)
                if data == "alive":
                    print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                    print("I am the Leader. Sending Alive Message")
            except:
                print("Error occured while sending {} message".format(self.messageToSend[s]))

    def HandleErrorSockets(self, s):
        """
        Handle the sockets that are in the errored socket list of select
        """
        self.readSockets.remove(s)
        if s in self.writeSockets:
            self.writeSockets.remove(s)
        s.shutdown(socket.SHUT_RDWR)
        s.close()

    def HandleProbe(self):
        """
        Manages when to send Probe message to the leader
        Check if the leader response in the required time
        If not will start the election
        """
        if(self.state == "QUIESCENT" and self.leader != self.process_id):
            Probe_current_time = datetime.now()
            if (self.probeInProgress == False):
                if (Probe_current_time >= self.lastProbeTime + timedelta(seconds=self.probeTime)):
                    self.probeTime = random.uniform(0.5, 3) #seconds
                    self.Probe()
            else:
                if(Probe_current_time > self.probe_msg_send_time + timedelta(seconds=self.timeout)):
                    self.probeInProgress = False
                    print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                    print("Alive Message not received from leader")
                    self.SelectElectionMembers(True)

    def HandleFailure(self):
        """
        Manages when the process will feigh failure
        """
        current_time_for_failure = datetime.now()
        if (current_time_for_failure >= self.failureTrackingTime):
            self.ShutDown()

    def HandleTimeOutForElection(self):
        """
        Manages if the processes replied within time for the election messages send to them, if not sends coordinator messages
        """
        #Check if we have received any Ok, if not and time have passed send Coordinator message
        if(self.state == "ELECTION_IN_PROGRESS" and self.expectedElectionResponse == True):  
            current_time = datetime.now()
            if(current_time > self.max_time_for_response + timedelta(seconds=self.timeout)):
                self.Coordinator(self.members)

    def SendReceiveResponse(self):
        """
        Send/Receive request/response to/from the server and clients.
        The response/request can be the Election message, coordinator message, Probe message, Alive message or OK message
        """ 
        #print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        #print("Entered in select method")
        cntr = 0
        while True:
            cntr += 1
            to_read, to_write, error = select.select(self.readSockets, self.writeSockets , self.readSockets, self.selectTimeout)
            print("\nSelect returned: Iteration={}, Leader={}, MyProcessId={}, ReadReady={}, WriteReady={}, InError={}.".format(cntr, self.leader, self.process_id, len(to_read), len(to_write), len(error)))
            
            for s in to_read:
                self.HandleReadRequests(s)
            for s in to_write:
                self.HandleWriteRequests(s)
            for s in error:
                self.HandleErrorSockets(s)
            self.HandleTimeOutForElection()
            self.HandleProbe()
            self.HandleFailure()
            
    def Coordinator(self, members):
        """
        Sends the coordinator message to all the members present
        :param members: the dictionary of processes present received from the gcd
        """
        self.activeProcesses[self.process_id] = self.listenAddress
        for id, addr in self.members.items():
            if self.is_smaller(id) == True:
                self.activeProcesses[id] = addr
        for id, addr in self.members.items():
            if id != self.process_id:
                try:
                    coordinateSocket = self.CreateSockets()
                    coordinateSocket.connect(addr)
                    coordinatorMessage = (Bully.CoordinatorMessageTag, self.activeProcesses)
                    self.send_message(coordinateSocket, coordinatorMessage)
                    print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
                    print("Sending Coordinator message to process {}".format(id))
                except socket.gaierror:
                    pass
                except socket.error:
                    pass
                except socket.timeout:
                    pass
        
        self.leader = self.process_id
        self.state = "QUIESCENT"
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("Leader is: {}".format(self.leader))
        print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
        print("The Current state of the process {}".format(self.state))

    def Probe(self):
        """
        Sends the probe message to the leader
        If the process fails while connecting to the leader to send the probe msg,
        starts the election 
        """
        try:
            self.probeSocket = self.CreateSockets()
            address = self.members.get(self.leader)
            self.probeSocket.connect(address)
            data = (Bully.ProbeMessageTag, None)
            self.send_message(self.probeSocket, data)
            self.probe_msg_send_time = datetime.now()
            self.messageToReceive[self.probeSocket] = "ALIVE"
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Sending PROBE message to the leader")
            self.probeInProgress = True
            self.readSockets.append(self.probeSocket)
        except:
            print((datetime.now().time()).strftime(("%H:%M:%S:%f")), end="-> ")
            print("Probe connect failed")
            self.SelectElectionMembers(True)

    def send_message(self,sock,send_data):
        """
        Pickles and sends the given message to the given socket.
        :param sock: socket to message/recv
        :param send_data: message data (anything pickle-able)
        """
        try:
            data = pickle.dumps(send_data)
            sock.sendall(data)
            sock.shutdown(socket.SHUT_WR)
        except socket.gaierror:
            pass

        except socket.error:
            pass

        except socket.timeout:
            pass
        return None

    def recv_message(self, sock, buffer_size):
        """
        receive and Pickle receive data.
        :param buffer_size: number of bytes in receive buffer
        :return: message response (unpickled--pickled data must fit in buffer_size bytes)
        """
        try:
            full_msg = b''

            # Workaroud for windows bug where socket becomes ready in select function but
            # the data is not available until sender side has shutdown the write channel.
            # Setting the socket to blocking temporarily so that recv function does not fail
            # immediately. After reading setting the socket back to non-blocking.
            sock.setblocking(1)
            while True:
                recvd_data = sock.recv(buffer_size)
                if not recvd_data:
                    break
                else:
                    full_msg += recvd_data
            
            sock.setblocking(0) # workaround for widows bug. See description at above.
            if(len(full_msg) == 0):
                return None
            return pickle.loads(full_msg)
        except socket.gaierror:
            pass

        except socket.error:
            pass

        except socket.timeout:
            pass
        return None

    def ShutDown(self):
        """
        Handles all the socket when the process feigh failure
        and raises an exception
        """
        for s in self.readSockets:
            self.ShutDownSocket(s)
        for s in self.writeSockets:
            self.ShutDownSocket(s)
        raise RuntimeError("Process is feighing failure")

    def ShutDownSocket(self, s):
        """
        Shuts down and close all the sockets that are open
        :param s: Sockets that are open for read/write
        """  
        try:
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except socket.error:
            pass

    def is_greater(self,values):
        """
        Helper function, find the members with greater process_id than lab2
        :param values: it is the tuple of process_id of the group members
        """
        if (values[0]>self.process_id[0]):
            return True
        elif(values[0]==self.process_id[0]):
            if(values[1]>self.process_id[1]):
                return True
            else:
                return False
        else:
            return False
    
    def is_smaller(self, values):
        """
        Helper function, find the members with smaller process_id than lab2
        :param values: it is the tuple of process_id of the group members
        """
        if (self.process_id[0]>values[0]):
            return True
        elif(self.process_id[0]==values[0]):
            if(self.process_id[1]>values[1]):
                return True
            else:
                return False
        else:
            return False

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python lab1.py GCDHOST GCDPORT")
        exit(1)   
    host = 'localhost'
    port = int(sys.argv[1]) 
    days_to_birthday = 110
    SU_id = 4062477
    while True:
        try:
            bully = Bully(host, port, days_to_birthday, SU_id)
            bully.StartListening()
            bully.join_group()
        except RuntimeError as e:
            print("\n------FEIGNING FAILURE---------\n")
            sleepTime = random.uniform(5, 10) #seconds
            time.sleep(sleepTime)
