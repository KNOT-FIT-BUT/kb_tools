#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vim: set tabstop=4 softtabstop=4 expandtab shiftwidth=4

import hashlib

def generateId(prefix, counter):
    result = prefix + ":" + hashlib.sha224(str(counter).encode('utf-8')).hexdigest()[:10]
    return result

# konec souboru KbGenerateId.py
