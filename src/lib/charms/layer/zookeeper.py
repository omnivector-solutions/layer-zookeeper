import re
from time import sleep
import socket

from charmhelpers.core.hookenv import log
from charmhelpers.core.host import service_running

from charms.layer import status


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


def poll_zk_ready(host, port):
    if service_running('zookeeper'):
        count = 0
        while count <= 100:
            if get_zookeeper_mode(host, port) == "initializing":
                sleep(1)
                count += 1
            else:
                return True
    return False


def zk_status_and_log(status_level, msg):
    if status_level == "active":
        status.active(msg)
        log(msg)
    elif status_level == "blocked":
        status.blocked(msg)
        log(msg)
    elif status_level == "waiting":
        status.waiting(msg)
        log(msg)
    elif status_level == "maint" or status_level == "maintenance":
        status.maint(msg)
        log(msg)
    return
