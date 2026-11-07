"""Exception metadata only: never messages, repr, arguments, locals or URLs."""
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def diagnostic_fields(error):
    status, code = getattr(error, 'status', 0), getattr(error, 'code', 0)
    fields = {'error_type': type(error).__name__,
              'http_status': status if type(status) is int else 0,
              'exchange_error_code': code if type(code) is int else 0}
    transport = getattr(error, 'transport_error_type', None)
    if transport in ('ReadTimeout', 'ConnectTimeout', 'WriteTimeout', 'PoolTimeout',
                     'ConnectError', 'ReadError', 'WriteError', 'CloseError',
                     'LocalProtocolError', 'RemoteProtocolError', 'ProxyError', 'UnsupportedProtocol'):
        fields['transport_error_type'] = transport
    tb = error.__traceback__
    while tb is not None:
        # Walk frame metadata only. Do not extract source text or frame locals.
        try:
            path = Path(tb.tb_frame.f_code.co_filename).resolve().relative_to(_ROOT)
        except (ValueError, OSError, RuntimeError):
            pass
        else:
            if '.venv' not in path.parts:
                fields.update(source=str(path), line=tb.tb_lineno,
                              function=tb.tb_frame.f_code.co_name)
        tb = tb.tb_next
    return fields
