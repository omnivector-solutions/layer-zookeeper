import re
import socket


def netcat(host, port, content):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, int(port)))
    s.sendall(content.encode())
    s.shutdown(socket.SHUT_WR)
    while True:
        data = s.recv(4096)
        if not data:
            break
        dat = data
    s.close()
    return dat


def get_zookeeper_version(host, port):
    netcat_out = netcat(host, port, "srvr").decode()
    regex = re.compile(r'Zookeeper version: (\S+)')
    re_match = re.findall(regex, netcat_out)
    if re_match:
        return re_match[0][:15]
    else:
        return "SOMETHING IS WRONG PLEASE DEBUG"


def get_zookeeper_mode(host, port):
    netcat_out = netcat(host, port, "srvr").decode()
    regex = re.compile(r'Mode: (\S+)')
    re_match = re.findall(regex, netcat_out)
    if re_match:
        return re_match[0]
    else:
        return "initializing"
