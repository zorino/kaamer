#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author:  	maxime déraspe
# email:	maximilien1er@gmail.com
# date:    	2020-01-14
# version: 	0.01

import sys
import gzip
import re

# Get only taxon entries from GBK input file
def taxon(taxon, gbk_file):

    if gbk_file[-2:] == "gz":
        filein = gzip.open(gbk_file, "rt")
    else:
        filein = open(gbk_file)

    entry = ""
    keep = False
    skip = False

    taxonReg1 = ("%s." % taxon)
    taxonReg2 = ("%s;" % taxon)
    inside_taxon = False

    for l in filein:
        if l[0:2] == "//":
            if (keep == True) and (skip == False):
                entry += l
                print(entry)
            entry = ""
            keep = False
            skip = False
        else:
            entry += l

            if l[0:12] != "            ":
                inside_taxon = False

            if inside_taxon:
                if (taxonReg1 in l):
                    keep = True
                elif (taxonReg2 in l):
                    keep = True

            if l[0:10] == "  ORGANISM":
                inside_taxon = True

    return

# Get only taxon entries from GBK input file
def taxon_file(taxon_file, gbk_file):

    taxonsObj = {}
    with open(taxon_file) as taxons:
        for l in taxons:
            taxonsObj[l.strip()] = 1

    if gbk_file[-2:] == "gz":
        filein = gzip.open(gbk_file, "rt")
    else:
        filein = open(gbk_file)

    entry = ""
    keep = False
    skip = False

    taxonReg1 = ("%s." % taxon)
    taxonReg2 = ("%s;" % taxon)
    inside_taxon = False
    currentTaxon = ""

    for l in filein:
        if l[0:2] == "//":
            if (keep == True) and (skip == False):
                entry += l
                if currentTaxon != "":
                    with open("%s.dat"%currentTaxon, "a") as text_file:
                        text_file.write(entry)
            currentTaxon = ""
            entry = ""
            keep = False
            skip = False
        else:
            entry += l

            if l[0:12] != "            ":
                inside_taxon = False

            if inside_taxon:
                for t in l[5:].split(";"):
                    t = t.strip()
                    t = t.strip('.')
                    if t in taxonsObj:
                        keep = True
                        currentTaxon = t

            if l[0:10] == "  ORGANISM":
                inside_taxon = True

    return

def fasta(gbk_file):

    if gbk_file[-2:] == "gz":
        filein = gzip.open(gbk_file, "rt")
    else:
        filein = open(gbk_file)

    entry = ""
    keep = 0
    reg = re.compile("\s+")
    inside_seq = False
    skip = False

    for l in filein:
        if l[0:2] == "//":
            if entry != "" and (skip != True):
                print(entry)
            entry = ""
            inside_seq = False
            skip = False
        elif l[0:7] == "VERSION":
            entry += ">%s\n" % (reg.split(l))[1]
        elif l[0:6] == "ORIGIN":
            inside_seq = True
        elif inside_seq:
            seq_split = reg.split(l.strip())[1:]
            entry += "".join(seq_split)

    return

def ids(ids_file, gbk_file):

    ids = {}
    with open(ids_file) as f:
        for l in f:
            _id = l.strip()
            if _id != "":
                ids[_id] = 1

    if gbk_file[-2:] == "gz":
        filein = gzip.open(gbk_file, "rt")
    else:
        filein = open(gbk_file)

    entry = ""
    keep = False
    reg = re.compile("\s+")

    for l in filein:
        if l[0:2] == "//":
            if (keep == True):
                entry += l
                print(entry)
            entry = ""
            keep = False
        else:
            entry += l

            if l[0:9] == "ACCESSION":
                _id = (reg.split(l))[1].strip()
                if _id in ids:
                    keep = True

            elif l[0:7] == "VERSION":
                for _id in reg.split(l):
                    _id = _id.replace(";","")
                    if _id in ids:
                        keep = True


# Main #
if __name__ == "__main__":

    usage = """

gbk-filter.py [options] INPUT_GBK

  taxon          <taxon> (down to genus.. ex. Escherichia)

  taxon_file     <taxon_list.txt> (down to genus) - will split file by genus

  fasta          extract fasta sequence

  ids            <ids_list.txt> will only keep those proteins ids

Note: All 'Flags: Fragment;' will be skipped

    """

    if len(sys.argv) < 2:
        print(usage)
        exit(1)

    if sys.argv[1] == "taxon":
        if len(sys.argv) < 4:
            print(usage)
            exit(1)

        taxon(sys.argv[2], sys.argv[-1])

    if sys.argv[1] == "taxon_file":
        if len(sys.argv) < 4:
            print(usage)
            exit(1)

        taxon_file(sys.argv[2], sys.argv[-1])

    elif sys.argv[1] == "fasta":
        if len(sys.argv) < 3:
            print(usage)
            exit(1)
        fasta(sys.argv[2])

    elif sys.argv[1] == "ids":
        if len(sys.argv) < 4:
            print(usage)
            exit(1)
        ids(sys.argv[2], sys.argv[-1])

    else:
        print(usage)
        exit(1)

