#!/usr/bin/env python3
"""
OB Python UDF Sandbox Executor - runs inside sandbox process.
User code cannot see this file.
"""
import sys
import os
import json
import struct
import traceback
import io
import types
import datetime as _dt_mod
from decimal import Decimal as _Decimal

# ── ODPS compatibility shim ──────────────────────────────────────────
# Create fake odps.udf module so that user code like
#   from odps.udf import annotate
#   @annotate("string,string->bigint")
# works without a real ODPS SDK installed.
def _create_odps_shim():
    odps = types.ModuleType('odps')
    udf  = types.ModuleType('odps.udf')

    def annotate(type_signature):
        def decorator(cls):
            param_str, return_str = type_signature.split('->')
            param_types = [t.strip() for t in param_str.split(',')]
            return_type = return_str.strip()
            for attr_name, attr_value in cls.__dict__.items():
                if callable(attr_value) and attr_name == 'evaluate':
                    attr_value.__annotations__ = {}
                    for i, param_type in enumerate(param_types):
                        attr_value.__annotations__['param_{0}'.format(i)] = param_type
                    attr_value.__annotations__['return'] = return_type
                    break
            return cls
        return decorator

    udf.annotate = annotate
    odps.udf = udf
    sys.modules['odps'] = odps
    sys.modules['odps.udf'] = udf

_create_odps_shim()

# ── Message type constants ───────────────────────────────────────────
MAGIC        = 0x50594F42
MSG_LOAD     = 1
MSG_EXECUTE  = 2
MSG_RESULT_OK  = 3
MSG_RESULT_ERR = 4
MSG_SHUTDOWN   = 7

# ── UDF registry (module-level, but hidden from user code below) ─────
_loaded_udfs = {}
_module_namespaces = {}

# ── Redirect user print to /dev/null to avoid polluting pipe or logs ─
# Move pipe fd handles to high-numbered fds so user code cannot easily guess them.
_PIPE_IN_FD  = os.dup(sys.stdin.fileno())    # dup to a high fd
_PIPE_OUT_FD = os.dup(sys.stdout.fileno())   # dup to a high fd
os.close(sys.stdin.fileno())                  # close original fd 0
# Redirect stdout to /dev/null (not stderr) to prevent log injection
sys.stdout = open(os.devnull, 'w')
_raw_stdin  = io.open(_PIPE_IN_FD,  'rb', closefd=False)
_raw_stdout = io.open(_PIPE_OUT_FD, 'wb', closefd=False)

# ── Lazy pyarrow import ──────────────────────────────────────────────
pa = None
pa_ipc = None

def _ensure_pyarrow():
    global pa, pa_ipc
    if pa is not None:
        return
    # Force single-threaded mode for BLAS/LAPACK libraries to prevent
    # pthread_create failures under RLIMIT_NPROC in the sandbox.
    os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
    os.environ.setdefault('MKL_NUM_THREADS', '1')
    os.environ.setdefault('OMP_NUM_THREADS', '1')
    import pyarrow as _pa
    import pyarrow.ipc as _pa_ipc
    pa = _pa
    pa_ipc = _pa_ipc

# ── Frame I/O ───────────────────────────────────────────────────────
def _read_exact(n):
    buf = b''
    while len(buf) < n:
        chunk = _raw_stdin.read(n - len(buf))
        if not chunk:
            raise EOFError("pipe closed")
        buf += chunk
    return buf

MAX_BODY_LEN = 32 * 1024 * 1024  # must match C++ OB_PY_MAX_FRAME_BODY_SIZE

def read_frame():
    hdr = _read_exact(12)
    magic, msg_type, body_len = struct.unpack('>III', hdr)
    if magic != MAGIC:
        raise ValueError("bad magic: 0x{0:08X}".format(magic))
    if body_len > MAX_BODY_LEN:
        raise ValueError("body_len {0} exceeds max {1}".format(body_len, MAX_BODY_LEN))
    body = _read_exact(body_len) if body_len else b''
    return msg_type, body

def write_frame(msg_type, body):
    hdr = struct.pack('>III', MAGIC, msg_type, len(body))
    _raw_stdout.write(hdr + body)
    _raw_stdout.flush()

def write_json_frame(msg_type, obj):
    write_frame(msg_type, json.dumps(obj, ensure_ascii=False).encode('utf-8'))

# ── LOAD handler ─────────────────────────────────────────────────────
def handle_load(body):
    msg       = json.loads(body)
    udf_id    = int(msg['udf_id'])
    source    = msg['source_code']
    entry     = msg['entry_name']

    # Parse entry_name — aligned with ObPyUtils::load_routine_py logic:
    #   "ClassName"            -> module = inner_auto_py_udf_{udf_id}, cls = ClassName
    #   "ModuleName.ClassName" -> module = ModuleName, cls = ClassName
    #   "a.b.c"                -> error (not supported)
    parts = entry.split('.')
    if len(parts) == 1:
        module_name = 'inner_auto_py_udf_{0}'.format(udf_id)
        cls_name = parts[0]
    elif len(parts) == 2:
        module_name = parts[0]
        cls_name = parts[1]
    else:
        raise ValueError(
            "python udf symbol '{0}' invalid: must be 'ClassName' or 'ModuleName.ClassName'".format(entry))

    # Reuse namespace for same module_name so that multiple UDFs from the
    # same script share the module (exec only once).
    if module_name in _module_namespaces:
        ns = _module_namespaces[module_name]
    else:
        ns = {'__name__': module_name}
        exec(source, ns)
        _module_namespaces[module_name] = ns

    if cls_name not in ns:
        raise NameError(
            "class '{0}' not found in module '{1}'".format(cls_name, module_name))

    _loaded_udfs[udf_id] = ns[cls_name]

    # Send acknowledgment so C++ side knows LOAD succeeded
    write_json_frame(MSG_RESULT_OK, {'udf_id': udf_id, 'status': 'loaded'})

def _arrow_val_to_py(scalar):
    """Convert an Arrow scalar to a Python value.
    For binary columns (varchar/datetime passed as binary),
    decode as UTF-8 str; if decode fails, keep as bytes.
    Do NOT auto-convert to datetime/date — UDF authors expect the declared
    SQL types (varchar → str), and auto-conversion breaks methods like
    str.replace() that collide with datetime.date.replace()."""
    val = scalar.as_py()
    if isinstance(val, bytes):
        try:
            return val.decode('utf-8')
        except UnicodeDecodeError:
            return val
    return val

# Map Arrow type name (from C++ ToString()) to pyarrow type.
# Used to construct return arrays with the correct type.
_ARROW_TYPE_HINT_MAP = None
def _arrow_type_from_hint(hint):
    global _ARROW_TYPE_HINT_MAP
    if _ARROW_TYPE_HINT_MAP is None:
        _ARROW_TYPE_HINT_MAP = {
            'int64': pa.int64(),
            'uint64': pa.uint64(),
            'float': pa.float32(),
            'double': pa.float64(),
            'binary': pa.binary(),
            'utf8': pa.utf8(),
            'timestamp[us]': pa.timestamp('us'),
        }
    return _ARROW_TYPE_HINT_MAP.get(hint)

# ── EXECUTE handler ──────────────────────────────────────────────────
def handle_execute(body):
    _ensure_pyarrow()

    reader = pa_ipc.open_stream(pa.BufferReader(body))
    batch  = reader.read_next_batch()

    meta   = batch.schema.metadata or {}
    udf_id = int(meta.get(b'ob_udf_id', b'0'))
    mode   = meta.get(b'ob_mode', b'scalar').decode()
    ret_type_hint = meta.get(b'ob_ret_type', b'').decode()

    if udf_id not in _loaded_udfs:
        raise KeyError("udf_id {0} not loaded, send LOAD first".format(udf_id))

    cls = _loaded_udfs[udf_id]

    if mode == 'scalar':
        results = []
        for i in range(batch.num_rows):
            row_args = [_arrow_val_to_py(batch.column(j)[i]) for j in range(batch.num_columns)]
            val = cls().evaluate(*row_args)
            # Convert date/datetime to str so Arrow passes them as strings, not Date32/Timestamp
            if isinstance(val, _dt_mod.datetime):
                val = val.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif isinstance(val, _dt_mod.date):
                val = val.isoformat()
            elif isinstance(val, _Decimal):
                # Convert Decimal based on target return type:
                # - int64/uint64: truncate to int (like SQL CAST(decimal AS INT))
                # - float/double: convert to float
                # - utf8 (i.e. RETURNS DECIMAL): keep as string
                if ret_type_hint in ('int64', 'uint64'):
                    val = int(val)
                elif ret_type_hint in ('float', 'double'):
                    val = float(val)
                else:
                    val = format(val, 'f')
            elif isinstance(val, bool):
                val = int(val)
            results.append(val)
        # Use return type hint from C++ metadata to construct Arrow array
        # with the correct type (e.g. uint64 vs int64), falling back to auto-infer.
        pa_ret_type = _arrow_type_from_hint(ret_type_hint) if ret_type_hint else None
        try:
            result_arr = pa.array(results, type=pa_ret_type)
        except (OverflowError, pa.lib.ArrowInvalid, pa.lib.ArrowTypeError) as e:
            raise OverflowError(
                "UDF return value out of range: {0}".format(e))
    elif mode == 'arrow':
        col_args   = [batch.column(j) for j in range(batch.num_columns)]
        result_arr = cls().evaluate(*col_args)
        if not isinstance(result_arr, pa.Array):
            result_arr = pa.array(result_arr)
    else:
        raise ValueError("unknown mode: {0}".format(mode))

    # Serialize result -> Arrow IPC
    result_schema = pa.schema([pa.field('result', result_arr.type)])
    result_batch  = pa.record_batch([result_arr], schema=result_schema)
    sink   = pa.BufferOutputStream()
    writer = pa_ipc.new_stream(sink, result_schema)
    writer.write_batch(result_batch)
    writer.close()
    write_frame(MSG_RESULT_OK, sink.getvalue().to_pybytes())

_EXECUTOR_FILENAME = os.path.basename(__file__) if '__file__' in dir() else 'ob_python_executor.py'

def _format_user_traceback():
    """Format traceback, filtering out frames from ob_python_executor.py
    and stripping absolute paths. Only keep user UDF frames + the exception line."""
    typ, val, tb = sys.exc_info()
    if tb is None:
        return traceback.format_exception_only(typ, val)
    # Collect only frames that are NOT from this executor script
    user_frames = []
    cur = tb
    while cur is not None:
        fn = os.path.basename(cur.tb_frame.f_code.co_filename)
        if fn != _EXECUTOR_FILENAME:
            user_frames.append(cur)
        cur = cur.tb_next
    lines = ['Traceback (most recent call last):\n']
    for frame in user_frames:
        lines.extend(traceback.format_tb(frame, limit=1))
    lines.extend(traceback.format_exception_only(typ, val))
    return ''.join(lines)

def _build_error_summary():
    """Build a one-line error summary from current exception context.
    e.g. 'AssertionError at ChineseCalendar.evaluate (<string>:156)'"""
    typ, val, tb = sys.exc_info()
    exc_name = typ.__name__ if typ else 'Exception'
    # Find the deepest user frame (not from this executor)
    cur = tb
    user_frame = None
    while cur is not None:
        fn = os.path.basename(cur.tb_frame.f_code.co_filename)
        if fn != _EXECUTOR_FILENAME:
            user_frame = cur
        cur = cur.tb_next
    if user_frame is not None:
        f = user_frame.tb_frame
        cls_name = ''
        if 'self' in f.f_locals:
            cls_name = type(f.f_locals['self']).__name__ + '.'
        func_name = f.f_code.co_name
        filename = os.path.basename(f.f_code.co_filename)
        lineno = user_frame.tb_lineno
        return '{0} at {1}{2} ({3}:{4})'.format(exc_name, cls_name, func_name, filename, lineno)
    return exc_name

# ── Hide internal state from user code ────────────────────────────────
# User code runs via exec() in an isolated namespace, but can still reach
# this module through sys.modules['__main__']. Replace __main__ in
# sys.modules with a dummy so that user code cannot access pipe handles
# or the UDF registry.
def _hide_internals():
    """Replace __main__ in sys.modules so user code cannot access internals."""
    import types
    dummy = types.ModuleType('__main__')
    dummy.__doc__ = None
    sys.modules['__main__'] = dummy

# ── Main loop ────────────────────────────────────────────────────────
def main():
    _hide_internals()
    while True:
        try:
            msg_type, body = read_frame()
            if msg_type == MSG_LOAD:
                handle_load(body)
            elif msg_type == MSG_EXECUTE:
                handle_execute(body)
            elif msg_type == MSG_SHUTDOWN:
                break
        except EOFError:
            break
        except Exception as e:
            try:
                tb = _format_user_traceback()
            except Exception:
                tb = traceback.format_exc()
            err_msg = str(e)
            if not err_msg:
                # For exceptions without a message (e.g. bare `assert`),
                # build a one-line summary: "AssertionError at evaluate (<string>:156)"
                err_msg = _build_error_summary()
            write_json_frame(MSG_RESULT_ERR, {
                'error': err_msg,
                'traceback': tb
            })

if __name__ == '__main__':
    main()
