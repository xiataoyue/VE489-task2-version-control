import socket
import struct
import binascii
import random


r_next = 0  # next seq needed
window_size = 8  # receiving window size
M = 16  # max seq num
buffer = {}  # buffer to store packets with wrong seq
result = []  # result array to store the data in correct order


def parse(packet):
    """
    Parse the packet received from client
    :param packet: packet made up of seq, crc and data
    :return: seq, crc, data
    """
    seq = struct.unpack('=I', packet[0:4])
    crc = struct.unpack('=H', packet[4:6])
    data = packet[6:].decode('UTF-8', errors='ignore')
    return seq[0], crc[0], data


def verify_crc(crc, data):
    """
    Verify the correctness of packet
    :param crc: crc parsed from pocket
    :param packet: packet received
    :return: True or False
    """
    calc = binascii.crc_hqx(bytes(data, 'UTF-8'), 0)
    if calc == crc:
        return True
    else:
        return False


def make_ack_packet(seq, ack_nak):
    """
    Make ACK packet to send to client
    :param seq: acknowledged seq
    :param ack_nak: ack or nak
    :return: the merged packet
    """
    seq_num = struct.pack('=I', seq)
    ack_nak = struct.pack('=H', ack_nak)
    next_seq = struct.pack('=I', r_next)
    packet = seq_num + ack_nak + next_seq
    return packet


def run():
    """
    Main function of server to receive and send
    :return: None
    """
    global r_next
    global buffer
    global result

    client_host = '192.168.31.42'
    port = 12345
    filename = '/home/xiataoyue/Desktop/task2.txt'
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local_host = socket.gethostname()
    server_socket.bind(("192.168.159.128", port))
    server_socket.listen(1)
    client_socket, client_addr = server_socket.accept()
    print("Client address: ", client_addr)
    nak = False
    end_flag = False

    while True:
        print(r_next)
        try:
            client_socket.settimeout(10)
            client_packet = client_socket.recv(2048)
            client_socket.settimeout(None)
            print("hihihi")
            seq, crc, data = parse(client_packet)
            if seq == 999 and data == '00000endok.11111':
                print("Final end packet received, server is going to close.")
                break
            if seq == 100 and data == '101010ending010101':
                end_flag = True
                end_ack = make_ack_packet(seq, 1)
                client_socket.send(end_ack)
                print("First ending packet received, going to send ack and waiting for final end packet.")
            print("Packet of sequence nuumber", str(seq), "is received.")

            if end_flag:
                continue
            print("Window starts from", str(r_next), "---", str((r_next + window_size - 1) % M))
            checksum = verify_crc(crc, data)
            in_window = False
            if r_next + window_size < M:
                if r_next <= seq <= r_next + window_size:
                    in_window = True
            else:
                if seq >= r_next or seq < (r_next + window_size) % M:
                    in_window = True
            if in_window and not end_flag:
                if seq != r_next:
                    if checksum:
                        if seq not in buffer:
                            buffer[seq] = data
                            print("Packet of sequence number", str(seq), "saved in buffer.")
                        else:
                            print("Packet", str(seq),  "in receiving window, but already saved, drop the current one.")
                        if not nak:
                            print("NAK packet for seq", str(r_next), "is sent, also acked seq", str(seq))
                            ack_packet = make_ack_packet(seq, 0)
                            nak = True
                            if random.random() >= 0.1:
                                client_socket.send(ack_packet)
                        else:
                            ack_packet = make_ack_packet(seq, 1)
                            if random.random() >= 0.1:
                                client_socket.send(ack_packet)
                else:
                    if checksum:
                        result.append(data)
                        next_seq = (r_next + 1) % M
                        for i in range(1, window_size):
                            seq_num = (r_next + i) % M
                            if seq_num in buffer.keys():
                                result.append(buffer[seq_num])
                                del buffer[seq_num]
                                next_seq = (next_seq + 1) % M
                            else:
                                break
                        nak = False
                        r_next = next_seq
                        ack_packet = make_ack_packet(seq, 1)
                        print("ACK packet for seq", str(seq), "is sent.")
                        print(ack_packet)
                        if random.random() >= 0.1:
                            client_socket.send(ack_packet)
            else:
                ack_packet = make_ack_packet(seq, 1)
                if random.random() >= 0.1:
                    client_socket.send(ack_packet)
                print("Sequence number is not in receiving window, packet dropped. ack sent for seq", str(seq))
            print("Next intended receiving sequence number:", str(r_next))
            print("#############################################################\n")

        except socket.timeout:
            break

        except ConnectionResetError:
            print("Client closed connection.")
            break

    print("All packets are received, server will be closed.")
    with open(filename, 'w') as f:
        for data in result:
            f.write(data)

    server_socket.close()


if __name__ == '__main__':
    run()
