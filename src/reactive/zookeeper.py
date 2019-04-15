import os
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
    service_restart,
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

from charms.layer.zookeeper import (
    get_zookeeper_version,
    get_zookeeper_mode,
)


CONFIG = config()
KV = unitdata.kv()

ZK_CLIENT_PORT = 2181

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


@when('apt.installed.openjdk-8-jre-headless')
@when_not('zk.apt.deps.available')
def zookeeper_apt_deps_available():
    """Zookeeper deps available.
    """

    zk_status_and_log('active', "Apt deps available.")
    set_flag('zk.apt.deps.available')


@when_not('zk.user.available')
def create_zookeeper_user():
    """Create zookeeper user.
    """

    zk_status_and_log('maint', "Creating 'zookeeper' user and home dir.")

    adduser('zookeeper', system_user=True,
            home_dir=str(ZK_HOME_DIR))

    zk_status_and_log('active', "'zookeeper' user and home dir created.")
    set_flag('zk.user.available')


@when('zk.user.available')
@when_not('zk.dirs.available')
def create_zookeeper_dirs():
    """Ensure directories exist for zookeeper operation..
    """

    zk_status_and_log('maint', "Creating Zookeeper dirs.")

    for directory in [ZK_LOG_DIR, ZK_TRACELOG_DIR, ZK_DATA_DIR,
                      ZK_DATALOG_DIR, ZK_DYNAMIC_CONFIG_DIR]:
        if not directory.exists():
            directory.mkdir(parents=True)
        chownr(str(directory), 'zookeeper', 'zookeeper', chowntopdir=True)

    zk_status_and_log('active', "Zookeeper dirs created.")
    set_flag('zk.dirs.available')


@hook('datadir-storage-attached')
def set_datadir_available_flag():
    set_flag('zk.datadir.storage.available')


@hook('datalogdir-storage-attached')
def set_datalogdir_available_flag():
    set_flag('zk.datalogdir.storage.available')


@when('zk.user.available',
      'zk.datalogdir.storage.available',
      'zk.datadir.storage.available')
@when_not('zk.storage.available')
def prepare_zk_storage_dirs():
    """
    Create (if not exists) and set perms on zk storage dirs.
    Make sure we do a second pass chowning the dirs here to
    ensure ownership of the dirs following the mount.
    """

    zk_status_and_log(
        'maint',
        "Creating/chowning the zookeeper datadir and datalogdir."
    )

    for directory in [ZK_DATA_DIR, ZK_DATALOG_DIR]:
        if not directory.exists():
            directory.mkdir(parents=True)

        chownr(path=str(directory), owner='zookeeper',
               group='zookeeper', follow_links=True,
               chowntopdir=True)

    zk_status_and_log('active', "Creating/chowning mounts successful.")
    set_flag('zk.storage.available')


@when('zk.apt.deps.available',
      'zk.user.available')
@when_not('zk.resource.available')
def provision_zookeeper():
    """Proivision zookeeper resource.
    """

    zk_status_and_log('maint', "Provisioning zookeeper resource.")

    zk_resource_provisioned = provision_zookeeper_resource()

    if not zk_resource_provisioned:
        zk_status_and_log(
            'blocked',
            "TROUBLE PROVISIONING ZOOKEEPER RESOURCE, PLEASE DEBUG"
        )
        return

    chownr(str(ZK_HOME_DIR), 'zookeeper', 'zookeeper', chowntopdir=True)

    zk_status_and_log('active', "Zookeeper resource available.")
    set_flag('zk.resource.available')


@when_not('zk.bind.address.available')
def bind_address_zk_nodesavailable():
    """Get the correct ip address for zookeeper to bind.
    """

    zk_status_and_log('maint', "Acquiring bind address.")

    ip = network_get('zk')['ingress-addresses'][0]
    zk_id = int(os.environ['JUJU_UNIT_NAME'].split("/")[1]) + 1
    KV.set('bind_address', ip)
    KV.set('zk_nodes', [{'host': ip, 'zk_id': zk_id}])
    KV.set('zk_id', zk_id)

    zk_status_and_log('active', "Bind address acquired.")
    set_flag('zk.bind.address.available')


@when('zk.bind.address.available',
      'zk.resource.available',
      'zk.dirs.available',
      'zk.storage.available',
      'zk.apt.deps.available')
@when_not('zk.init.config.available')
def create_zookeeper_init_config():
    """Create inital zookeeper configs.
    """

    zk_status_and_log('maint', "Writing inital config.")

    setup_zookeeper_init_config()

    zk_status_and_log('active', "Initial config rendered.")
    set_flag('zk.dynamic.config.available')
    set_flag('zk.init.config.available')


@when('zk.bind.address.available',
      'zk.init.config.available')
@when_not('zk.systemd.available')
def render_zookeeper_systemd():
    """Install zk systemd service.
    """

    zk_status_and_log('maint', "Enabling 'zookeeper' systemd service.")

    # Provision and enable the systemd service
    ctxt = {
        'zk_cfg': str(ZK_CONFIG_FILE),
        'zk_home': str(ZK_HOME_DIR),
        'zk_server_sh': str(ZK_SERVER_SH),
    }
    render(
        source='zookeeper.service',
        target='/etc/systemd/system/zookeeper.service',
        context=ctxt
    )
    check_call(['systemctl', 'enable', 'zookeeper'])

    zk_status_and_log('active', "'zookeeper' systemd service enabled.")
    set_flag('zk.systemd.available')


@when('zk.systemd.available')
@when_not('zk.init.complete')
def set_zookeeper_init_complete():
    """Set the 'zk.init.complete' flag and log about it.
    """

    zk_status_and_log('active', "Zookeeper initialization complete.")
    set_flag('zk.init.complete')


@when('zk.init.complete')
@when_not('zk.dynamic.config.available')
def render_zookeeper_dynamic_config():
    """When a new unit joins the zookeeper
    cluster we need to rerender the config with the new members.

    Note: This handler is only run when the 'zk.dynamic.config.available'
    becomes cleared or unset.
    """

    zk_status_and_log('maint', "Writing zk peers to dynamic config file.")

    ctxt = {'zk_nodes': KV.get('zk_nodes')}

    if ZK_DYNAMIC_CONFIG_FILE.exists():
        ZK_DYNAMIC_CONFIG_FILE.unlink()
    render(
        source='zookeeper.cfg.dynamic',
        target=str(ZK_DYNAMIC_CONFIG_FILE),
        context=ctxt,
        owner='zookeeper',
        group='zookeeper'
    )

    if not is_flag_set('zk.init.start.available') and not \
            is_flag_set('leadership.is_leader'):

        init_start_zookeeper()

        # Need to find a better way to do this other then sleep
        sleep(5)

        set_flag('zk.init.started')
        set_flag('zk.init.start.available')

    elif is_flag_set('zk.init.start.available') and not \
            is_flag_set('leadership.is_leader'):

        service_restart('zookeeper')

        # Need to find a better way to do this other then sleep
        sleep(5)

    elif is_flag_set('leadership.is_leader'):

        service_restart('zookeeper')

        # Need to find a better way to do this other then sleep
        sleep(5)

    zk_status_and_log('active', "Zookeeper dynamic config rendered.")
    zk_running_status()
    set_flag('zk.dynamic.config.available')


@when('zk.init.complete',
      'leadership.is_leader')
@when_not('zk.init.started')
def start_initial_zookeeper_systemd_for_leader():
    """Start the zookeeper service for the first time on the master node.
    """

    zk_status_and_log('maint', "Starting Zookeeper.")
    init_start_zookeeper()
    set_flag('zk.init.started')


@when('zk.init.started',
      'leadership.is_leader')
@when_not('zk.version.available')
def zookeeper_version():
    """Set the zookeeper version.
    """

    set_zookeeper_version()
    set_flag('zk.version.available')


@when('zk.init.started')
def get_set_zookeeper_status():
    """Set Zookeeper status once init complete.
    """

    zk_running_status()


@hook('upgrade-charm')
def reprovision_all_the_things():
    """Stop the appropriate services, reprovision all the things,
    start the services back up.
    """

    zk_status_and_log('maint', "Upgrading Zookeeper.")

    if is_flag_set('zk.init.installed'):
        if service_running('zookeeper'):
            service_stop('zookeeper')

    # Reprovision/reinstall
    zk_resource_provisioned = provision_zookeeper_resource()

    if not zk_resource_provisioned:
        zk_status_and_log(
            'blocked',
            "TROUBLE PROVISIONING ZOOKEEPER RESOURCE, PLEASE DEBUG"
        )
        return

    chownr(str(ZK_HOME_DIR), 'zookeeper', 'zookeeper', chowntopdir=True)

    setup_zookeeper_init_config()

    # Start the appropriate services back up
    if not service_running('zookeeper'):
        service_start('zookeeper')

    # Need to find a better way to poll for zookeeper
    if is_flag_set('leadership.is_leader'):
        sleep(5)
        set_zookeeper_version()

    zk_status_and_log('active', "Zookeeper upgrade complete.")
    clear_flag('zk.version.available')


@when('endpoint.zk-peers.available')
def update_unitdata_kv_with_curent_peers():
    """
    This handler is ran whenever a peer is joined.
    (all node types use this handler to coordinate peers)
    """

    zk_status_and_log('maint', "Acquiring Zookeeper peers.")

    peers = endpoint_from_flag('endpoint.zk-peers.available').all_joined_units
    zk_nodes = []

    if len(peers) > 0 and \
       len([peer._data['private-address']
            for peer in peers if peer._data is not None]) > 0:
        KV.set('num_peers', len(peers))

        for peer in peers:
            zk_nodes.append(
                 {'host': peer._data['private-address'],
                  'zk_id': int(peer._unit_name.split("/")[1]) + 1})

        zk_nodes.append({'host': KV.get('bind_address'),
                         'zk_id': KV.get('zk_id')})

        KV.set('zk_nodes', zk_nodes)
        clear_flag('endpoint.zk-peers.available')
        clear_flag('zk.dynamic.config.available')
        zk_status_and_log('active', "Zookeeper peers acquired.")


@when('endpoint.zookeeper.available',
      'zk.bind.address.available')
def provide_client_relation_data():
    """
    Set client relation data.
    """

    zk_status_and_log('maint', "Sending client data over 'zk' endpoint.")

    endpoint_from_flag('endpoint.zookeeper.available').configure(
        KV.get('bind_address'), ZK_CLIENT_PORT)

    zk_status_and_log('active', "Zookeeper client data sent.")
    clear_flag('endpoint.zookeeper.available')

#
# Utility functions
#


def init_start_zookeeper():
    """
    Start Zookeeper
    """

    if service_start('zookeeper'):
        open_port(ZK_CLIENT_PORT)
        open_ports(2888, 3888)
        zk_status_and_log('active', "Zookeeper started.")
        return
    else:
        zk_status_and_log('blocked', "PLEASE DEBUG: Zookeeper not starting.")
        return


def setup_zookeeper_init_config():
    """Render the initial config.
    """

    # Provision /opt/zookeeper/conf/zookeeper-env.sh
    if ZK_ENV_FILE.exists():
        ZK_ENV_FILE.unlink()
    render(
        source='zookeeper-env.sh',
        target=str(ZK_ENV_FILE),
        context={},
        perms=0o755,
        owner='zookeeper',
        group='zookeeper',
    )

    # Provision /opt/zookeeper/conf/zoo.cfg
    if ZK_CONFIG_FILE.exists():
        ZK_CONFIG_FILE.unlink()
    ctxt = {
        'zk_bind_address': KV.get('bind_address'),
        'zk_data_dir': str(ZK_DATA_DIR),
        'zk_datalog_dir': str(ZK_DATALOG_DIR),
        'zk_dynamic_config': str(ZK_DYNAMIC_CONFIG_FILE),
        'zk_four_letter_words': CONFIG.get('four-letter-words'),
        'zk_standalone_enabled': CONFIG.get('standalone-enabled'),
    }
    render(
        source='zoo.cfg',
        target=str(ZK_CONFIG_FILE),
        context=ctxt,
        owner='zookeeper',
        group='zookeeper',
    )

    # Provision /opt/zookeeper/conf/log4j.properties
    if LOG4J_CONFIG_FILE.exists():
        LOG4J_CONFIG_FILE.unlink()
    render(
        source='log4j.properties',
        target=str(LOG4J_CONFIG_FILE),
        context={},
        owner='zookeeper',
        group='zookeeper',
    )

    # Provision /opt/zookeeper/conf/zookeeper.cfg.dynamic
    if not ZK_DYNAMIC_CONFIG_FILE.exists():
        render(
            source='zookeeper.cfg.dynamic',
            target=str(ZK_DYNAMIC_CONFIG_FILE),
            context={'zk_nodes': []},
            owner='zookeeper',
            group='zookeeper',
        )

    # Provision /srv/zookeeper_datadir/myid
    if ZK_ID_FILE.exists():
        ZK_ID_FILE.unlink()
    render(
        source='myid',
        target=str(ZK_ID_FILE),
        context={'zk_id': KV.get('zk_id')},
        owner='zookeeper',
        group='zookeeper',
    )
    return


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

    while not ZK_SERVER_SH.exists():
        sleep(1)

    return True


def zk_running_status():
    """Set zookeeper running status.
    """

    if service_running('zookeeper'):
        zk_mode = get_zookeeper_mode(KV.get('bind_address'), ZK_CLIENT_PORT)
        num_zk_nodes = len(KV.get('zk_nodes', []))
        zk_status_and_log('active', f'ZK {zk_mode} - {num_zk_nodes} nodes')
        return
    else:
        zk_status_and_log('blocked', 'Zookeeper not starting, please debug')
        return


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


def set_zookeeper_version():

    zk_status_and_log('maint', "Setting Zookeeper version.")

    zk_version = application_version_set(
        get_zookeeper_version(
            KV.get('bind_address'),
            ZK_CLIENT_PORT
        )
    )

    zk_status_and_log('active', f"Zookeeper {zk_version} installed.")
