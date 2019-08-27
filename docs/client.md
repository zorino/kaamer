## kaamer CLI

The kaamer CLI is a client to query (-search) a kaamer database.


```shell

> kaamer

 kaamer

  // Search

  -search           search for a query

    (input)

      -h            server host (default http://localhost:8321)

      -t            (prot, nt, fastq) query type

      -i            input file (fasta or fastq)

      -m            max number of results (default 10)

      -o            output file (default stdout)

      -fmt          (tsv, json) output format (default tsv)

    (flag)

      -ann          add hit annotations in tsv fmt (always true in json fmt)

      -pos          add query positions that hit


```


## Options

* -h Host

    Host is where the database server is running, default is http://localhost:8321 \
    See the [kAAmer database section](/database.md) for how to build and run a kaamer database server\
    Prefix http(s):// is required for the host

* -t Query type

    One of the 3 supported input type :
    * prot : for a protein sequence in the fasta format
    * nt : for nucleotide sequence (contigs, genes) in the fasta format
    * fastq : short reads sequence in the fastq format

* -i Input File

    Input file path, can be relative or complete

* -m Max Results

    Maximum number of results to return (default 10) 

* -o Outpout

    Output file, default is stdout
    
* -fmt Output format

    Output format currently supported are tsv or json

* -ann Hit Annotations

    Add hit annotations output (default false)

* -pos Positions Match

    Add the positions that has a match with the hit (default false) 


## Result example - TSV

```shell
kaamer -search -h http://localhost:8321 -t prot -i query.fasta -m 1 -o results.tsv -fmt tsv -ann -pos
```

|QueryName|QueryKSize|QStart|QEnd|KMatch|Hit.Id          |Hit.ProteinName                                                        |Hit.Organism    |Hit.EC                               |Hit.GO                                                |Hit.HAMAP|Hit.KEGG   |Hit.Biocyc|Hit.Taxonomy                                                                                                                                                                                      |QueryHit.Positions|
|---------|----------|------|----|------|----------------|-----------------------------------------------------------------------|----------------|-------------------------------------|------------------------------------------------------|---------|-----------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
|query    |264       |1     |270 |264   |BLAN1_KLEPN     |Metallo-beta-lactamase type 2 {ECO:0000305}                            |Klebsiella pneumoniae|3.5.2.6 {ECO:0000269&#124;PubMed:19770275}|GO:0042597;GO:0008800;GO:0008270;GO:0017001;GO:0046677|         |ag:CAZ39946|          |Bacteria; Proteobacteria; Gammaproteobacteria; Enterobacterales; Bacteria; Proteobacteria; Gammaproteobacteria; Enterobacterales;Enterobacteriaceae; Klebsiella. Enterobacteriaceae; Klebsiella.  |1-264             |


## Result example - JSON

```shell
kaamer -search -h http://localhost:8321 -t prot -i query.fasta -m 1 -o results.tsv -fmt json -ann -pos | jq
```
```javascript
[
  {
    "Query": {
      "Sequence": "MELPNIMHPVAKLSTALAAALMLSGCMPGEIRPTIGQQMETGDQRFGDLVFRQLAPNVWQHTSYLDMPGFGAVASNGLIVRDGGRVLVVDTAWTDDQTAQILNWIKQEINLPVALAVVTHAHQDKMGGMDALHAAGIATYANALSNQLAPQEGMVAAQHSLTFAANGWVEPATAPNFGPLKVFYPGPGHTSDNITVGIDGTDIAFGGCLIKDSKAKSLGNLGDADTEHYAASARAFGAAFPKASMIVMSHSAPDSRAAITHTARMADKLR",
      "Name": "query",
      "SizeInKmer": 264,
      "Type": "Protein Query",
      "Location": {
        "StartPosition": 1,
        "EndPosition": 270,
        "PlusStrand": true,
        "StartsAlternative": []
      },
      "Contig": ""
    },
    "SearchResults": {
      "Counter": {},
      "Hits": [
        {
          "Key": 25479,
          "Kmatch": 264
        }
      ],
      "PositionHits": {
        "25479": [true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true]
      }
    },
    "HitEntries": {
      "25479": {
        "Entry": "BLAN1_KLEPN",
        "ProteinName": "Metallo-beta-lactamase type 2 {ECO:0000305}",
        "Organism": "Klebsiella pneumoniae",
        "Taxonomy": "Bacteria; Proteobacteria; Gammaproteobacteria; Enterobacterales; Bacteria; Proteobacteria; Gammaproteobacteria; Enterobacterales;Enterobacteriaceae; Klebsiella. Enterobacteriaceae; Klebsiella.",
        "EC": "3.5.2.6 {ECO:0000269|PubMed:19770275}",
        "Sequence": "MELPNIMHPVAKLSTALAAALMLSGCMPGEIRPTIGQQMETGDQRFGDLVFRQLAPNVWQHTSYLDMPGFGAVASNGLIVRDGGRVLVVDTAWTDDQTAQILNWIKQEINLPVALAVVTHAHQDKMGGMDALHAAGIATYANALSNQLAPQEGMVAAQHSLTFAANGWVEPATAPNFGPLKVFYPGPGHTSDNITVGIDGTDIAFGGCLIKDSKAKSLGNLGDADTEHYAASARAFGAAFPKASMIVMSHSAPDSRAAITHTARMADKLR",
        "Length": 270,
        "GO": [
          "GO:0042597",
          "GO:0008800",
          "GO:0008270",
          "GO:0017001",
          "GO:0046677"
        ],
        "KEGG": [
          "ag:CAZ39946"
        ]
      },
    }
  }
]
```
