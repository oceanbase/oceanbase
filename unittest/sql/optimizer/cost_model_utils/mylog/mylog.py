import logging
import sys

class Singleton(object):
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args, **kw)
        return cls._instance


class MyLogger(Singleton):
    log = logging.getLogger(__name__)
    ##set to stdout
    fmt = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    formatter = logging.Formatter(fmt)
    out_hdlr = logging.StreamHandler(sys.stdout)
    #handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)
    out_hdlr.setFormatter(formatter)
    out_hdlr.setLevel(logging.INFO)
    log.addHandler(out_hdlr)
    log.setLevel(logging.INFO)

    @staticmethod
    def get_logger():
        return MyLogger.log

    @staticmethod
    def info(str, *args, **kargs):
        MyLogger.log.info(str, *args, **kargs)

    @staticmethod
    def warn(str, *args, **kargs):
        MyLogger.log.warn(str, *args, **kargs)

    @staticmethod
    def error(str, *args, **kargs):
        MyLogger.log.error(str, *args, **kargs)

if __name__ == '__main__':
    MyLogger.get_logger().info("test")
    MyLogger.get_logger().warn("test warn %s", 'test')
    MyLogger.error("test error")
