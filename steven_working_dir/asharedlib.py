from ctypes import *
from ctypes.util import find_library
import os

import platform
print(platform.architecture())
print(windll.kernel32)
# print(cdll.lstore)
# libc = cdll.lstore
print(find_library('c'))