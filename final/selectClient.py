import socket
import struct
import binascii
import time
import threading
import random

window_size = 8  # Size of sending window
M = 16  # Max sequence number
window = {}  # Saving packets in window, waiting for ack and retransmit
send_lock = threading.Lock()  # acquire the lock and then send
r_lock = threading.Lock()
r_next = 0  # indicate the next seq to send
curr_seq = 0


# class for sender part in client socket
class Sender(threading.Thread):
    def __init__(self, server_host_name, server_port_num, filename, socket_type):
        threading.Thread.__init__(self)
        self.host = server_host_name
        self.port = server_port_num
        self.file = filename
        self.win_size = window_size
        self.sock = socket_type
        self.start()  # start the sender thread

    def checksumCRC(self, data):
        # return the crc value of the data for transmitting
        return binascii.crc_hqx(bytes(data, 'UTF-8'), 0)

    def make_packet(self, data, seq_num):
        """
        Merge seq_num, crc. data into a packet for transmitting
        :param data: the bytes read from file
        :param seq_num: The sequence number of transporting
        :return: The merged packet, in bytes
        """
        seq = struct.pack('=I', seq_num)
        crc = struct.pack('=H', self.checksumCRC(data))
        packet = seq + crc + bytes(data, 'UTF-8')
        return packet

    def run(self):
        """
        Overwrite the run method of Thread
        :return: None
        """
        self.send()

    def send(self):
        """
        Main function to send data, consisting of common sending and retransmitting due to timeout
        :return: None
        """
        global window
        global send_lock
        global curr_seq
        global r_lock

        f = open(self.file, 'r')
        first_send = True
        file_end = False

        while True:
            r_lock.acquire()
            temp_rnext = r_next
            r_lock.release()
            print(curr_seq, temp_rnext)
            if len(window) == 0 and file_end:
                # Sending should be ended if there is no packet in window and the file has been read to the end
                print("All the packets are sent and acknowledged, ready to close socket.")
                break
            self.retransmit()  # First check the timeout status of all the packets in window
            # curr_seq = r_next  # After check the timeout status, send the packet with sequence number r_next
            # window begins with r_next
            if not file_end:  # If file has something to read, begin sending to fill the sending window
                  # obtain the sending lock to avoid interfering
                if first_send:
                    for i in range(0, window_size):

                        send_lock.acquire()

                        seq_num = (curr_seq + i) % M
                        if seq_num not in window.keys():
                            data = f.read(1024)
                            if not data:
                                file_end = True
                                send_lock.release()
                                break
                            else:
                                packet = self.make_packet(data, seq_num)
                                window[seq_num] = (packet, time.time(), 0)
                                if random.random() >= 0.1:
                                    self.sock.send(packet)
                                print("Packet with sequence number", str(seq_num), "is sent, waiting for ack.")
                        send_lock.release()
                        time.sleep(0.4)
                    first_send = False
                else:
                    for i in range(0, (temp_rnext - curr_seq) % M):
                        send_lock.acquire()
                        seq_num = (curr_seq + i + window_size) % M
                        if seq_num not in window.keys():
                            data = f.read(1024)
                            if not data:
                                file_end = True
                                send_lock.release()
                                break
                            else:
                                packet = self.make_packet(data, seq_num)
                                window[seq_num] = (packet, time.time(), 0)
                                if random.random() >= 0.1:
                                    self.sock.send(packet)
                                print("Packet with sequence number", str(seq_num), "is sent, waiting for ack.")
                        send_lock.release()
                        time.sleep(0.4)

            curr_seq = temp_rnext

        time.sleep(0.5)
        end_message = '101010ending010101'  # After all the data are sent, send the first end packet to tell the server
        send_lock.acquire()
        end_packet = self.make_packet(end_message, 100)
        window[100] = (end_packet, time.time(), 0)
        self.sock.send(end_packet)
        print("First ending packet is sent, waiting for ack to send final end packet.")
        send_lock.release()
        while len(window) > 0:  # check for ack for first end packet
            self.retransmit()
        f.close()

    def retransmit(self):
        """
        retransmit timeout packets
        :return: None
        """
        global window
        global send_lock

        send_lock.acquire()
        for seq in window.keys():
            if window[seq][2] == 0 and time.time() - window[seq][1] > 3.6:
                print("Timeout expired for packet seq number: " + str(seq) + ", retransmitting.")
                window[seq] = (window[seq][0], time.time(), 0)
                if random.random() >= 0.1:
                    self.sock.send(window[seq][0])
        send_lock.release()


# The receiver part of client, to check ack packets
class ACKChecker(threading.Thread):
    def __init__(self, server_host_name, server_port_name, filename, socket_type):
        threading.Thread.__init__(self)
        self.host = server_host_name
        self.port = server_port_name
        self.file = filename
        self.win_size = window_size
        self.sock = socket_type
        self.start()

    def parse(self, packet):
        """
        Parse the ack packet into seq, ack type, and next_seq
        :param packet: received packet
        :return: acknowledged seq number, ack or nak, next_seq need to be transmitted
        """
        seq = struct.unpack('=I', packet[0:4])
        ack = struct.unpack('=H', packet[4:6])
        next_seq = struct.unpack('=I', packet[6:])
        return seq[0], ack[0], next_seq[0]

    def run(self):
        """
        Overwrite the run method in Thread
        :return: None
        """
        global window
        global send_lock
        global r_next
        global r_lock

        while True:
            ack_packet = self.sock.recv(2048)
            print(len(ack_packet))
            seq, ack, next_seq = self.parse(ack_packet)
            print(seq, ack, next_seq)
            print(window)
            r_lock.acquire()
            curr = r_next
            r_next = next_seq
            r_lock.release()
            # send_lock.acquire()
            # # del all the packet from the curr_seq till next seq, since server has received
            # for i in range(0, (r_next - curr) % M):
            #     seq_num = (curr + i) % M
            #     if seq_num not in window.keys():
            #         continue
            #     origin_time = window[seq_num][1]
            #     window[seq_num] = (window[seq_num][0], origin_time, 1)
            #     del window[seq_num]
            # send_lock.release()

            if ack == 0:  # If NAK received, retransmit packet with seq next_seq immediately
                send_lock.acquire()
                print("NAK packet received for seq number checked", str(seq), "retransmit packet with sequence number",
                      str(next_seq))
                window[next_seq] = (window[next_seq][0], time.time(), 0)
                origin_time = window[seq][1]
                window[seq] = (window[seq][0], origin_time, 1)
                print("delete seq", str(seq), "\n")
                del window[seq]
                if random.random() >= 0.1:
                    self.sock.send(window[next_seq][0])
                send_lock.release()
            else:
                if seq in window.keys():  # If ACK received, delete the corresponding packet in sending window
                    send_lock.acquire()
                    print("ACK packet received with sequence number", str(seq))
                    print("Next seq to send is", str(next_seq))
                    origin_time = window[seq][1]
                    window[seq] = (window[seq][0], origin_time, 1)
                    print("delete seq", str(seq), "\n")
                    del window[seq]
                    # # del all the packet from the curr_seq till next seq, since server has received
                    for i in range(0, (r_next - curr) % M):
                        seq_num = (curr + i) % M
                        if seq_num not in window.keys():
                            continue
                        else:
                            origin_time = window[seq_num][1]
                            window[seq_num] = (window[seq_num][0], origin_time, 1)
                            del window[seq_num]
                    send_lock.release()
                    if seq == 100:  # If ACK for first end packet, send final end packet and close the socket.
                        send_lock.acquire()
                        print("ACK packet for first end packet is received, sending final end packet and close socket.")
                        message = '00000endok.11111'
                        seq_final = struct.pack('=I', 999)
                        crc_final = struct.pack('=H', binascii.crc_hqx(bytes(message, 'UTF-8'), 0))
                        packet_final = seq_final + crc_final + bytes(message, 'UTF-8')
                        self.sock.send(packet_final)
                        send_lock.release()
                        self.sock.close()
                        return


if __name__ == '__main__':
    server_host = "192.168.159.128"  # server address
    server_port = 12345  # port to transmit
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_host, server_port))

    file = '/home/version control/shakespeare.txt'
    print("Start sending data.")
    sender = Sender(server_host, server_port, file, client_socket)
    ack_checker = ACKChecker(server_host, server_port, file, client_socket)
    sender.join()
    ack_checker.join()

    print("All data is sent, finish socket.")
    if client_socket:
        client_socket.close()
