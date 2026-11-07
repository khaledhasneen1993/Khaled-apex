"""Keep HTTP library records out of logs: URLs can contain signatures/tokens.

Application diagnostics remain in the allowlisted operational journal. This is
configured at client construction, including standalone read-only CLI tools.
"""
import logging


def secure_http_logging():
    prefixes = ('httpx', 'httpcore')
    names = set(prefixes)
    names.update(name for name in logging.Logger.manager.loggerDict
                 if any(name.startswith(prefix + '.') for prefix in prefixes))
    for name in names:
        logger = logging.getLogger(name)
        # Existing children are covered too; future children with the default
        # NOTSET level inherit this threshold, even with a DEBUG root logger.
        logger.setLevel(logging.CRITICAL + 1)
        logger.disabled = True
