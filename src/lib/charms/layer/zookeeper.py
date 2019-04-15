from jinja2 import Environment, FileSystemLoader

import os

from pathlib import Path

import re
import socket
from time import sleep

from charmhelpers.core.hookenv import log, charm_dir
from charmhelpers.core.host import service_running, chownr

from charms.layer import status


ZK_CLIENT_PORT = 2181
ZK_FOLLOWER_PORT = 2888
ZK_LEADER_ELECTION_PORT = 3888

ZK_DATA_DIR = Path('/srv/zookeeper_data')
ZK_ID_FILE = ZK_DATA_DIR / 'myid'

ZK_DATALOG_DIR = Path('/srv/zookeeper_datalog')

ZK_DYNAMIC_CONFIG_DIR = Path('/srv/zookeeper_config')
ZK_DYNAMIC_CONFIG_FILE = ZK_DYNAMIC_CONFIG_DIR / 'zookeeper.cfg.dynamic'


ZK_LOG_DIR = Path('/var/log/zookeeper')
ZK_TRACELOG_DIR = ZK_LOG_DIR / 'trace'

ZK_HOME_DIR = Path('/opt/zookeeper')
ZK_BIN_DIR = ZK_HOME_DIR / 'bin'
ZK_SERVER_SH = ZK_BIN_DIR / 'zkServer.sh'

ZK_CONFIG_DIR = ZK_HOME_DIR / 'conf'
ZK_ENV_FILE = ZK_CONFIG_DIR / 'zookeeper-env.sh'
ZK_CONFIG_FILE = ZK_CONFIG_DIR / 'zoo.cfg'
LOG4J_CONFIG_FILE = ZK_CONFIG_DIR / 'log4j.properties'


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
                zk_status_and_log(
                    'maint',
                    f"Polling Zookeeper, still initializing: {count}."
                )
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


def update_zookeeper_dynamic_config(ctxt):
    """Update the zookeeper dynamic config file.
    """
    path = os.path.join(charm_dir(), 'templates')
    env = Environment(loader=FileSystemLoader(path))
    zk_dynamic_config = env.get_template('zookeeper.cfg.dynamic').render(ctxt)
    ZK_DYNAMIC_CONFIG_FILE.write_text(zk_dynamic_config)
    chownr(str(ZK_DYNAMIC_CONFIG_FILE), 'zookeeper', 'zookeeper')
