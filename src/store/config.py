from src.utils.store import Section, Store


class Main(Section):
    mount: str = './storage'  # Storage folder location
    production: bool = False  # If True monitor will try to avoid fatal errors as possible


class Cache(Section):
    item: int = 1209600  # How long save hashes of success & failed targets
    target: int = 604800


class Analytics(Section):
    datetime: bool = True  # True - all output time will presented as , False - all output time as timestamps
    datetime_format: str = '%Y-%m-%d %H:%M:%S.%f'
    beautify: bool = False


class ThreadManager(Section):
    _name = 'thread_manager'
    tick: float = 1.0
    lock_ticks: int = 16  # How much ticks lock can be acquired, then it will released


class Pipe(Section):
    tick: float = 0.5  # Delta time for queue manage (in seconds)
    wait: float = 10.0  # Timeout to join() when turning off monitor (in seconds)


class Worker(Section):
    count: int = 5  # Max workers count in normal condition
    tick: float = 1.0  # Delta time for worker run loop
    wait: float = 5.0


class CatalogWorker(Section):
    _name = 'catalog_worker'
    count: int = 5
    tick: float = 1.
    wait: int = 7


class Queue(Section):
    catalog_queue_size: int = 256  # Size for catalog_queue (will be waiting if full)
    catalog_queue_put_wait: float = 8.0
    target_queue_size: int = 512  # Size for target_queue (will be waiting if full)
    target_queue_put_wait: float = 8.0


class Log(Section):
    content: int = 1  # (0 - Only code, 1 - Code & Message, 2 - Code & Title & Message
    level: int = 4  # (5 - Test, 4 - Debug, 3 - Info, 2 - Warn, 1 - Error, 0 - Fatal)
    mode: int = 1  # (0 - off, 1 - Console only, 2 - File only, 3 - Console & File)


class Priority(Section):
    CSmart: int = 10
    CScheduled: int = 50
    CInterval: int = 100
    TSmart: list = [10, 0]
    TScheduled: list = [50, 0]
    TInterval: list = [100, 100]  # First value is base priority, second value is range (0 for static priority)
    RTSmart: list = [10, 0]
    RTScheduled: list = [50, 0]
    RTInterval: list = [100, 100]  # First value is base priority, second value is range (0 for static priority)
    catalog_default: int = 100
    target_default: int = 1001


class Provider(Section):
    max_bad: int = 25
    test_url: str = 'http://ip-api.com/json?fields=2154502'
    proxy_timeout: float = 3.0


class SubProvider(Section):
    _name = 'sub_provider'
    max_redirects: int = 5
    max_retries: int = 3
    connect_timeout: float = 1.0
    read_timeout: float = 2.0
    compression: bool = False
    comp_type: str = 'gzip, deflate, br'
    verify: bool = False


class EventHandler(Section):
    _name = 'event_handler'
    tick: float = 0.1
    wait: float = 3.0


class Config(Store):
    main: Main
    cache: Cache
    analytics: Analytics
    thread_manager: ThreadManager
    pipe: Pipe
    worker: Worker
    catalog_worker: CatalogWorker
    queue: Queue
    log: Log
    priority: Priority
    provider: Provider
    sub_provider: SubProvider
    event_handler: EventHandler
