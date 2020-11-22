from time import sleep

from Crypto.PublicKey import RSA
from uctp.peer import Aliases, Peer, Trusted
from yaml import safe_load

from src.helpers.analytics import Analytics
from src.helpers.commands import Commands
from src.models.cache import HashStorage
from src.models.keywords import Keywords
from src.models.provider import Provider
from src.store import conf, main
from src.utils.log import Logger
from src.utils.protocol import Code
from src.utils.storage import KernelStorage
from .remote import RemoteThreadHandler
from .resolver import Resolver
from .scripter import ScriptManager
from .thread_manager import ThreadManager


class KernelPanic(Exception):
    pass


class Kernel:
    log: Logger
    state: int

    analytics: Analytics
    provider: Provider
    remote: Peer
    resolver: Resolver
    script_manager: ScriptManager
    thread_manager: ThreadManager

    def __init__(
            self,
            remote: Peer = None,
            resolver: Resolver = None,
            script_manager: ScriptManager = None,
            thread_manager: ThreadManager = None
    ):
        self.log = Logger('K')

        if isinstance(script_manager, ScriptManager):
            self.script_manager = script_manager
        else:
            self.script_manager = ScriptManager()

        if isinstance(resolver, Resolver):
            self.resolver = resolver
        else:
            self.resolver = Resolver(self.script_manager)

        if isinstance(thread_manager, ThreadManager):
            self.thread_manager = thread_manager
        else:
            self.thread_manager = ThreadManager(self.resolver, self.script_manager)

        self.provider = Provider()
        self.analytics = Analytics(self.provider, self.script_manager, self.thread_manager)

        if isinstance(remote, Peer):
            self.remote = remote
        else:
            self.remote = Peer(
                'monitor',
                RSA.import_key(KernelStorage().file('monitor.pem').read()),
                '0.0.0.0',
                trusted=Trusted(*safe_load(KernelStorage().file('authority.yaml'))['trusted']),
                aliases=Aliases(safe_load(KernelStorage().file('authority.yaml'))['aliases']),
                auth_timeout=4,
                buffer=8192,
                error_handler=RemoteThreadHandler(self.script_manager.event_handler)
            )
        Commands(self.analytics, self, self.provider,
                 self.script_manager, self.thread_manager).bind(self.remote)  # Bind UCTP commands

    def run(self):
        self.state = 1

        # Staring
        main.loads(method=0)

        if KernelStorage().check('config.toml'):
            conf.load(KernelStorage().file('config.toml'))  # Load ./config.yaml

        Keywords.load()
        HashStorage.load()  # Load success hashes from cache

        if main.production:  # Notify about production mode
            self.log.info(Code(20101))

        self.script_manager.index.config_load()  # Load scripts.yaml
        self.script_manager.event_handler.start()  # Start event loop
        self.script_manager.index.reindex()  # Index scripts
        self.script_manager.load_all()  # Load scripts

        self.remote.start()  # Run UCTP server

        self.script_manager.event_handler.monitor_starting()

        self.analytics.dump(0)  # Create startup report

        self.thread_manager.start()  # Start pipeline
        self.script_manager.event_handler.monitor_started()
        # Starting end

        try:  # Waiting loop
            while 0 < self.state < 2:
                try:
                    if self.thread_manager.is_alive():
                        sleep(1)
                    else:
                        self.log.fatal(KernelPanic(Code(50101)))
                except KeyboardInterrupt:
                    self.log.info(Code(20102))
                    self.state = 2
                except KernelPanic:
                    self.state = 2
        finally:  # Stopping
            self.log.info(Code(20103))

            self.script_manager.event_handler.monitor_stopping()

            self.remote.stop()  # Stop UCTP server

            self.thread_manager.join(self.thread_manager.close())  # Stop pipeline and wait

            self.provider.proxy_dump()  # Save proxies to ./proxy.json
            self.analytics.dump(2)  # Create stop report

            self.script_manager.event_handler.monitor_stopped()

            self.script_manager.event_handler.stop()  # Stop event loop
            self.script_manager.unload_all()  # Unload scripts
            self.script_manager.del_()  # Delete all data about scripts (index, parsers, etc.)

            self.log.info(Code(20104))
            HashStorage.unload()  # Dump success hashes
            self.log.info(Code(20105))

            Keywords.dump()
            conf.dump(KernelStorage().file('config.toml', 'w+'))

            self.log.info(Code(20106))
