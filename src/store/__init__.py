from .config import Config

conf = Config()

main = conf.main
cache = conf.cache
analytics = conf.analytics
thread_manager = conf.thread_manager
pipe = conf.pipe
worker = conf.worker
catalog_worker = conf.catalog_worker
queue = conf.queue
log = conf.log
priority = conf.priority
provider = conf.provider
sub_provider = conf.sub_provider
event_handler = conf.event_handler
