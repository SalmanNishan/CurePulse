from CurePulse import CurePulse
import sys

if __name__ == "__main__":

    arg = sys.argv
    curepulse = CurePulse(arg[1])

    curepulse.start()
    curepulse.end()