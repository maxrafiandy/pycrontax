#!/usr/bin/python
import sys
import os
from src.autodebet import autodebet
from src.koordinatwp import koordinatwp, updateKoordinatWp
from src.offline import offline
from src.penarikan import penarikan
from src.pgomset import pgomset
from src.setting import log_error

if __name__ == '__main__':
    args = sys.argv
    for arg in args:
        # skip argumen pertama (command)
        if arg == sys.argv[0]: continue
        # jalankan mirror/update
        if arg == "autodebet":
            print "Updating tbl_mirror_autodebet . . ."
            autodebet()
        elif arg == "koordinatwp":
            print "Updating tbl_koordinatwp . . ."
            koordinatwp()
        elif arg == "updatewp":
            print "Updating tbl_koordinatwp . . ."
            updateKoordinatWp()
        elif arg == "offline":
            print "Updating tbl_mirror_history_offline . . ."
            offline()
        elif arg == "penarikan":
            print "Updating tbl_mirror_penarikan . . ."
            penarikan()
        elif arg == "pgomset":
            print "Updating tbl_mirror_pgomset . . ."
            pgomset()
        else:
            log_error("{arg} is not a valid argument!".format(arg = arg))
            cmd = "usage: {app} [autodebet|koordinatwp|offline|penarikan|pgomset]"
            print cmd.format(app = sys.argv[0])
    print "Done!"
