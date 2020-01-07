#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author:  	maxime déraspe
# email:	maximilien1er@gmail.com
# date:    	2019-10-16
# version: 	0.01

import sys
import argparse
import re

def read_sequence(f):

    seq = ""
    with open(f) as fread:
        for l in fread:
            if l[0:1] != ">":
                seq += l.strip()

    return seq


def print_gff(features, args):
    print("##gff-version 3")

    for ft in features:
        strand = "+"
        qstart = ft['QStart']
        qend = ft['QEnd']

        if int(ft['QStart']) > int(ft['QEnd']):
            strand = "-"
            qstart = ft['QEnd']
            qend = ft['QStart']

        featureCol = "ID=" + ft['SubjectId']

        if (args.ftProduct in ft) and (ft[args.ftProduct] != ""):
            featureCol += ";"
            featureCol += "product="
            featureCol += ft[args.ftProduct]

        if args.ftGene in ft and (ft[args.ftGene] != ""):
            featureCol += ";"
            featureCol += "gene="
            featureCol += ft[args.ftGene]

        print("%s\t.\tCDS\t%s\t%s\t.\t%s\t.\t%s" % (ft['QueryId'], qstart, qend, strand, featureCol))

def print_gbk(features):

    print("##gff-version 3")

    for ft in features:
        strand = "+"
        qstart = ft['QStart']
        qend = ft['QEnd']

        if int(ft['QStart']) > int(ft['QEnd']):
            strand = "-"
            qstart = ft['QEnd']
            qend = ft['QStart']

        print("%s\t.\tCDS\t%s\t%s\t.\t%s\t.\tID=%s" % (ft['QueryId'], qstart, qend, strand, ft['SubjectId'] ))


def build_feature(_fts, features):

    protein_names = []
    gene_names = []
    bestPid = 0

    for ft in _fts:
        _protein_name = re.sub(r' {.+}', '', ft['ProteinName'])
        protein_names.append(_protein_name)
        if 'GeneName' in ft:
            gene_names.append(ft['GeneName'])
        if ft['pId'] > bestPid:
            bestPid = ft['pId']

    if (len(set(protein_names)) < len(protein_names)):
        cs_protein_name = max(set(protein_names), key = protein_names.count)
        cs_gene_name = max(set(gene_names), key = gene_names.count)
    else:
        # take best hit if no consensus
        cs_protein_name = _fts[0]['ProteinName']
        cs_gene_name = _fts[0]['GeneName']

    fstFt = _fts[0]
    fstFt['ProteinName'] = cs_protein_name
    fstFt['GeneName'] = cs_gene_name
    fstFt['Pid'] = bestPid

    features.append(fstFt)


def resolve_features(features):

    fts_to_keep = []

    for ft in features:
        if len(fts_to_keep) < 1:
            fts_to_keep.append(ft)
            continue

        if int(ft['QEnd']) < int(ft['QStart']):
            qStart = int(ft['QEnd'])
            qEnd = int(ft['QStart'])
        else:
            qStart = int(ft['QStart'])
            qEnd = int(ft['QEnd'])

        if int(fts_to_keep[-1]['QEnd']) < int(fts_to_keep[-1]['QStart']):
            lStart = int(fts_to_keep[-1]['QEnd'])
            lEnd = int(fts_to_keep[-1]['QStart'])
        else:
            lStart = int(fts_to_keep[-1]['QStart'])
            lEnd = int(fts_to_keep[-1]['QEnd'])

        qPid = float(ft['Bitscore'])
        lPid = float(fts_to_keep[-1]['Bitscore'])

        if qStart < lEnd and qEnd < lEnd:
            if qPid > lPid:
                fts_to_keep[-1] = ft

        if qStart < lEnd and qEnd > lEnd:
            if (lEnd - qStart) < 60:
                fts_to_keep.append(ft)

        if qStart > lEnd:
            fts_to_keep.append(ft)

    return fts_to_keep


def extract_features(args):

    if args.kaamer_res != None:
        reader = open(args.kaamer_res)
    elif args.stream:
        reader = sys.stdin
        kaamerResOut = open("Kaamer-Res.tsv", "w")

    header = reader.readline().split("\t")
    header[-1] = header[-1].strip()
    if args.stream:
        print(header)
        kaamerResOut.write("\t".join(header))
        kaamerResOut.write("\n")

    currentLoc = ""
    currentFts = []
    features = []

    if '%Identity' in header:
        pIdHeader = '%Identity'
    else:
        pIdHeader = '%KMatchIdentity'

    for l in reader:

        if args.stream:
            kaamerResOut.write(l)
        lA = l.split("\t")
        lA[-1] = lA[-1].strip()
        ft = dict(zip(header, lA))

        loc = ft['QStart'] + ".." + ft['QEnd']
        abs_loc = min(int(ft['QStart']), int(ft['QEnd']))
        abs_end = max(int(ft['QStart']), int(ft['QEnd']))
        length = (abs_end-abs_loc)/3

        if length < args.minLen:
            continue

        ft['abs_loc'] = abs_loc

        ft['pId'] = float(ft[pIdHeader])
        if ft['pId'] < args.minId:
            continue

        if currentLoc == "":
            currentLoc = loc

        if currentLoc != loc:
            if len(currentFts) > 0:
                build_feature(currentFts, features)
            currentFts = []
            currentLoc = loc

        currentFts.append(ft)

    features = sorted(features, key=lambda k: k['abs_loc'])
    print(features)
    features = resolve_features(features)

    print_gff(features, args)

# Main #
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create annotation file from kaamer tsv results')
    parser.add_argument('--seq', type=str, help='sequence input (fasta file)')

    parser.add_argument('--minId', type=float, help='minimum identity or kmatchidentity to call the annotation on protein', default=90.0)
    parser.add_argument('--minLen', type=float, help='minimum alignment length of CDS to call the annotation on protein', default=60)

    parser.add_argument('--kaamer_res', type=str, help='kaamer results TSV file')

    parser.add_argument('--ftProduct', type=str, help='the column name in result file to report CDS product annotation', default="ProteinName")
    parser.add_argument('--ftGene', type=str, help='the column name in result file to report CDS gene annotation', default="GeneName")

    parser.add_argument('--stream', action='store_true', help='stream kaamer results output (read from pipe)')

    args = parser.parse_args()

    genome_seq = ""

    if args.seq != None:
        genome_seq = read_sequence(args.seq)

    # No kaamer results input
    if args.kaamer_res == None and args.stream != True:
        print("\nNo kaamer results !!\n")
        parser.print_help()
    else:
        extract_features(args)

