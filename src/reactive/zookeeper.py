import os
import hashlib
from subprocess import check_call
from pathlib import Path
from time import sleep

from charms.reactive import (
    clear_flag,
    endpoint_from_flag,
    hook,
    when,
    when_not,
    set_flag,
    is_flag_set,
)

from charmhelpers.core import unitdata
from charmhelpers.core.host import (
    adduser,
    chownr,
    service_start,
    service_stop,
    service_running,
)
from charmhelpers.core.hookenv import (
    application_version_set,
    config,
    log,
    network_get,
    open_port,
    open_ports,
    resource_get,
)
from charmhelpers.core.templating import render
from charms.layer import status


CONFIG = config()
KV = unitdata.kv()

ZK_CLIENT_PORT = 2181

ZK_DATA_DIR = Path('/srv/zookeeper')
ZK_ID_FILE = ZK_DATA_DIR / 'myid'

ZK_LOG_DIR = Path('/var/log/zookeeper')
ZK_TRACELOG_DIR = ZK_LOG_DIR / 'trace'

ZK_HOME_DIR = Path('/usr/lib/zookeeper')
ZK_CONFIG_DIR = ZK_HOME_DIR / 'conf'
ZK_ENV_FILE = ZK_CONFIG_DIR / 'zookeeper-env.sh'
ZK_HOSTS_FILE = ZK_CONFIG_DIR / 'zookeeper_hosts.cfg'
ZK_CONFIG_FILE = ZK_CONFIG_DIR / 'zoo.cfg'
LOG4J_CONFIG_FILE = ZK_CONFIG_DIR / 'log4j.properties'


@when('apt.installed.openjdk-8-jre-headless')
@when_not('zk.apt.deps.available')
def zookeeper_apt_deps_available():
    """Zookeeper deps available.
    """
    set_flag('zk.apt.deps.available')


@when('zk.apt.deps.available')
@when_not('zk.resource.available')
def provision_zookeeper():
    """Proivision zookeeper resource.
    """
    log("PROVISIONING ZOOKEEPER RESOURCE")
    status.maint("PROVISIONING ZOOKEEPER RESOURCE")

    zk_resource_provisioned = provision_zookeeper_resource()

    if not zk_resource_provisioned:
        status.blocked("TROUBLE PROVISIONING ZOOKEEPER RESOURCE, PLEASE DEBUG")
        log("TROUBLE PROVISIONING ZOOKEEPER RESOURCE, PLEASE DEBUG")
        return

    log("ZOOKEEPER RESOURCE {} ready".format(get_zookeeper_version()))
    status.maint("ZOOKEEPER RESOURCE {} ready".format(get_zookeeper_version()))

    set_flag('zk.resource.available')


@when_not('zk.user.available')
def create_zookeeper_user():
    """Create zookeeper user.
    """
    adduser('zookeeper', system_user=True)
    set_flag('zk.user.available')


@when('zk.user.available')
@when_not('zk.dirs.available')
def create_zookeeper_dirs():
    """Create zookeeper dirs.
    """
    for directory in [ZK_LOG_DIR, ZK_TRACELOG_DIR, ZK_DATA_DIR]:
        if not directory.exists():
            directory.mkdir(parents=True)
            chownr(str(directory), 'zookeeper', 'zookeeper', chowntopdir=True)
    set_flag('zk.dirs.available')


@when_not('zk.bind.address.available')
def bind_address_zk_nodesavailable():
    """Get the correct ip address for zookeeper to bind.
    """
    ip = network_get('zk')['ingress-addresses'][0]
    zk_id = os.environ['JUJU_UNIT_NAME'].split("/")[1]
    KV.set('bind_address', ip)
    KV.set('zk_nodes', [{'host': ip, 'zk_id': zk_id}])
    KV.set('zk_id', zk_id)
    set_flag('zk.bind.address.available')


@when('zk.bind.address.available',
      'zk.resource.available',
      'zk.dirs.available')
@when_not('zk.init.config.available')
def zookeeper_init_config_available():
    setup_zookeeper_init_config()
    set_flag('zk.hosts.config.available')
    set_flag('zk.init.config.available')


@when('zk.init.started',
      'zk.bind.address.available')
@when_not('zk.hosts.config.available')
def rerender_zookeeper_config():
    """When a new unit joins the zookeeper
    cluster we need to rerender the config with the new members.
    """
    ctxt = {'zk_nodes': KV.get('zk_nodes')}

    if ZK_HOSTS_FILE.exists():
        ZK_HOSTS_FILE.unlink()
    render(
        source='zookeeper_hosts.cfg',
        target=str(ZK_HOSTS_FILE),
        context=ctxt,
        owner='zookeeper',
        group='zookeeper'
    )
    set_flag('zk.hosts.config.available')


@when('zk.bind.address.available',
      'zk.init.config.available')
@when_not('zk.systemd.available')
def render_zookeeper_systemd():
    """Install zk systemd service.
    """

    # Provision and enable the systemd service
    render(
        source='zookeeper.service',
        target='/etc/systemd/system/zookeeper.service',
        context=[]
    )
    check_call(['systemctl', 'enable', 'zookeeper'])
    set_flag('zk.systemd.available')


@when('zk.init.config.available',
      'zk.systemd.available',
      'zk.bind.address.available')
@when_not('zk.init.complete')
def set_zookeeper_init():
    set_flag('zk.init.complete')


@when('zk.init.complete')
@when_not('zk.version.available')
def get_set_zookeeper_version():
    application_version_set(get_zookeeper_version())
    set_flag('zk.version.available')


@when('zk.init.complete')
@when_not('zk.init.started')
def start_zookeeper_systemd():
    start_zookeeper()
    set_flag('zk.init.started')


@hook('upgrade-charm')
def reprovision_all_the_things():
    """Stop the appropriate services, reprovision all the things,
    start the services back up.
    """
    if is_flag_set('zk.init.installed'):
        if service_running('zookeeper'):
            service_stop('zookeeper')

    # Reprovision/reinstall
    zk_resource_provisioned = provision_zookeeper_resource()

    if not zk_resource_provisioned:
        status.blocked("TROUBLE PROVISIONING ZOOKEEPER RESOURCE, PLEASE DEBUG")
        log("TROUBLE PROVISIONING ZOOKEEPER RESOURCE, PLEASE DEBUG")
        return

    setup_zookeeper_init_config()

    # Start the appropriate services back up
    if not service_running('zookeeper'):
        service_start('zookeeper')


@when('endpoint.zk-peers.available')
def update_unitdata_kv_with_cuurent_peers():
    """
    This handler is ran whenever a peer is joined.
    (all node types use this handler to coordinate peers)
    """

    peers = endpoint_from_flag('endpoint.zk-peers.available').all_joined_units
    zk_nodes = []

    if len(peers) > 0 and \
       len([peer._data['private-address']
            for peer in peers if peer._data is not None]) > 0:

        for peer in peers:
            zk_nodes.append(
                 {'host': peer._data['private-address'],
                  'zk_id': peer._unit_name.split("/")[1]})

        zk_nodes.append({'host': KV.get('bind_address'),
                         'zk_id': KV.get('zk_id')})
        KV.set('zk_nodes', zk_nodes)
        clear_flag('endpoint.zk-peers.available')
        clear_flag('zk.hosts.config.available')


@when('endpoint.zookeeper.available',
      'zk.bind.address.available')
def provide_client_relation_data():
    """
    Set client relation data.
    """
    endpoint_from_flag('endpoint.zookeeper.available').configure(
        KV.get('bind_address'), ZK_CLIENT_PORT)
    clear_flag('endpoint.zookeeper.available')

#
# Utility functions
#


def start_zookeeper():

    """
    Start Zookeeper
    """
    if service_start('zookeeper'):
        open_port(ZK_CLIENT_PORT)
        open_ports(2888, 3888)
        log('Zookeeper Running')
        status.active('Zookeeper Running')
    else:
        log('PLEASE DEBUG: Zookeeper not starting.')


def setup_zookeeper_init_config():
    """Unpack the tarball, render the config, chown the dirs.
    """

    # Render the configs
    if ZK_ENV_FILE.exists():
        ZK_ENV_FILE.unlink()
    render(
        source='zookeeper-env.sh',
        target=str(ZK_ENV_FILE),
        context={},
        perms=755,
        owner='zookeeper',
        group='zookeeper',
    )

    if ZK_CONFIG_FILE.exists():
        ZK_CONFIG_FILE.unlink()
    render(
        source='zoo.cfg',
        target=str(ZK_CONFIG_FILE),
        context={'bind_address': KV.get('bind_address')},
        owner='zookeeper',
        group='zookeeper',
    )

    if LOG4J_CONFIG_FILE.exists():
        LOG4J_CONFIG_FILE.unlink()
    render(
        source='log4j.properties',
        target=str(LOG4J_CONFIG_FILE),
        context={},
        owner='zookeeper',
        group='zookeeper',
    )

    if ZK_HOSTS_FILE.exists():
        ZK_HOSTS_FILE.unlink()
    render(
        source='zookeeper_hosts.cfg',
        target=str(ZK_HOSTS_FILE),
        context={'zk_nodes': KV.get('zk_nodes')},
        owner='zookeeper',
        group='zookeeper',
    )

    if ZK_ID_FILE.exists():
        ZK_ID_FILE.unlink()
    render(
        source='myid',
        target=str(ZK_ID_FILE),
        context={'zk_id': KV.get('zk_id')},
        owner='zookeeper',
        group='zookeeper',
    )


def get_zookeeper_version():
    # this is a placeholder, fix this to make it get the real zk version
    return "3.4.14"


def provision_zookeeper_resource():
    """Unpack the zookeeper resource.
    """

    zk_tarball = resource_get('zookeeper-tarball')

    if not zk_tarball:
        status.blocked("Could not find resource 'zookeeper-tarball'")
        return

    if ZK_HOME_DIR.exists():
        check_call(['rm', '-rf', str(ZK_HOME_DIR)])
    check_call(['mkdir', '-p', str(ZK_HOME_DIR)])
    check_call(
        ['tar', '-xzf', zk_tarball, '--strip=1', '-C', str(ZK_HOME_DIR)])

    while not Path('/usr/lib/zookeeper/bin/zkServer.sh').exists():
        sleep(1)

    chownr(str(ZK_HOME_DIR), 'zookeeper', 'zookeeper', chowntopdir=True)
    return True


def hash_ip_id(host):
    """Take an ip addresses and return a dict of the ip and its hash..
    """

    m = hashlib.sha256()
    m.update(host.encode())
    return {'id': m.hexdigest()[:6], 'host': host}
