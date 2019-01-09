package makedb

import (
	"bufio"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/zorino/metaprot/internal"
	"github.com/zorino/metaprot/cmd/download_db"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// uniprotkb-bacteria (https://github.com/zorino/microbe-dbs)
type Protein struct {
	Entry            string
	EntryName        string
	Status           string
	ProteinNames     string
	GeneNames        string
	Organism         string
	TaxonomicLineage string
	GeneOntology     string  // g_ struct
	FunctionCC       string  // f_ struct
	Pathway          string
	EC_Number        string
	Mass             string
	Length           string
	Sequence         string
}

type DBStructs struct {
	k_batch         *db_struct.K_
	g_batch         *db_struct.G_
}

func NewMakedb(dbPath string, inputPath string, kmerSize int) {

	// Glob all uniprot tsv files to be processed
	files, _ := filepath.Glob(inputPath + "/*.tsv")

	if len(files) == 0 {
		download_db.Download(inputPath)
		os.Exit(0)
	}

	os.Mkdir(dbPath, 0700)

	dbStructs := new(DBStructs)
	dbStructs.k_batch = db_struct.K_New(dbPath)
	dbStructs.g_batch = db_struct.G_New(dbPath)

	for _, file := range files {
		run(file, kmerSize, dbStructs)
	}

	// Last DB flushes
	dbStructs.k_batch.Close()
	dbStructs.g_batch.Close()

}

func run(fileName string, kmerSize int, dbStructs *DBStructs) int {

	file, _ := os.Open(fileName)

	jobs := make(chan string)
	results := make(chan int)
	wg := new(sync.WaitGroup)

	// thread pool
	var nbThreads = 12
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBuffer(jobs, results, wg, kmerSize, dbStructs)
	}

	// Go over a file line by line and queue up a ton of work
	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			jobs <- scanner.Text()
		}
		close(jobs)
	}()

	// Collect all the results...
	// First, make sure we close the result channel when everything was processed
	go func() {
		wg.Wait()
		close(results)
	}()

	// Now, add up the results from the results channel until closed
	counts := 0
	for v := range results {
		counts += v
	}

	return counts

}

func readBuffer(jobs <-chan string, results chan<- int, wg *sync.WaitGroup, kmerSize int, dbStructs *DBStructs) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInput(j, kmerSize, dbStructs)
	}
	results <- 1

}

func processProteinInput(line string, kmerSize int, dbStructs *DBStructs) {

	s := strings.Split(line, "\t")
	if len(s) < 11 {
		return
	}

	c := Protein{}
	c.Entry = s[0]
	c.EntryName = s[1]
	c.Status = s[2]
	c.ProteinNames = s[3]
	c.GeneNames = s[4]
	c.Organism = s[5]
	c.TaxonomicLineage = s[6]
	c.GeneOntology = s[7]
	c.FunctionCC = s[8]
	c.Pathway = s[9]
	c.EC_Number = s[10]
	c.Mass = s[11]
	c.Length = s[12]
	c.Sequence = s[13]

	// skip peptide shorter than kmerSize
	if len(c.Sequence) < kmerSize {
		return
	}

	// build badger transactions
	// i_ := "i_" + c.Entry
	// i_val := ""
	// u_ := "u_" + c.Status

	// sliding windows of kmerSize on Sequence
	for i := 0; i < len(c.Sequence)-kmerSize+1; i++ {

		key := c.Sequence[i:i+kmerSize]

		var currentValue []byte
		dbStructs.k_batch.DB.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err == nil {
				item.Value(func(val []byte) error {
					// Accessing val here is valid.
					// fmt.Printf("The answer is: %s\n", val)
					currentValue = append([]byte{}, val...)
					return nil
				})
			}
			return nil
		})

		var g_val = dbStructs.g_batch.CreateValues(c.GeneOntology, string(currentValue))

		if g_val != string(currentValue) {
			dbStructs.k_batch.Mu.Lock()
			dbStructs.k_batch.Add(key, g_val)
			dbStructs.k_batch.Mu.Unlock()
		}

	}

	fmt.Printf("%#v done\n", c.Entry)

}

// func PrintDB (db *badger.DB) {
// 	db.View(func(txn *badger.Txn) error {
// 		opts := badger.DefaultIteratorOptions
// 		opts.PrefetchSize = 10
// 		it := txn.NewIterator(opts)
// 		defer it.Close()
// 		for it.Rewind(); it.Valid(); it.Next() {
// 			item := it.Item()
// 			k := item.Key()
// 			err := item.Value(func(v []byte) error {
// 				fmt.Printf("key=%s, value=%s\n", k, v)
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})
// }


// P23883
// PUUC_ECOLI
// reviewed
// NADP/NAD-dependent aldehyde dehydrogenase PuuC (ALDH) (EC 1.2.1.5) (3-hydroxypropionaldehyde dehydrogenase) (Gamma-glutamyl-gamma-aminobutyraldehyde dehydrogenase) (Gamma-Glu-gamma-aminobutyraldehyde dehydrogenase)
// puuC aldH b1300 JW1293
// Escherichia coli (strain K12)
// cellular organisms, Bacteria, Proteobacteria, Gammaproteobacteria, Enterobacterales, Enterobacteriaceae, Escherichia, Escherichia coli, Escherichia coli (strain K12)
// aldehyde dehydrogenase [NAD(P)+] activity [GO:0004030]; putrescine catabolic process [GO:0009447]
// FUNCTION: Catalyzes the oxidation of 3-hydroxypropionaldehyde (3-HPA) to 3-hydroxypropionic acid (3-HP) (PubMed:18668238). It acts preferentially with NAD but can also use NADP (PubMed:18668238). 3-HPA appears to be the most suitable substrate for PuuC followed by isovaleraldehyde, propionaldehyde, butyraldehyde, and valeraldehyde (PubMed:18668238). It might play a role in propionate and/or acetic acid metabolisms (PubMed:18668238). Also involved in the breakdown of putrescine through the oxidation of gamma-Glu-gamma-aminobutyraldehyde to gamma-Glu-gamma-aminobutyrate (gamma-Glu-GABA) (PubMed:15590624). {ECO:0000269|PubMed:15590624, ECO:0000269|PubMed:18668238, ECO:0000305|PubMed:1840553}.
// PATHWAY: Amine and polyamine degradation; putrescine degradation; 4-aminobutanoate from putrescine: step 3/4. {ECO:0000305|PubMed:15590624}.
// 1.2.1.5
// 53,419
// 495
// MNFHHLAYWQDKALSLAIENRLFINGEYTAAAENETFETVDPVTQAPLAKIARGKSVDIDRAMSAARGVFERGDWSLSSPAKRKAVLNKLADLMEAHAEELALLETLDTGKPIRHSLRDDIPGAARAIRWYAEAIDKVYGEVATTSSHELAMIVREPVGVIAAIVPWNFPLLLTCWKLGPALAAGNSVILKPSEKSPLSAIRLAGLAKEAGLPDGVLNVVTGFGHEAGQALSRHNDIDAIAFTGSTRTGKQLLKDAGDSNMKRVWLEAGGKSANIVFADCPDLQQAASATAAGIFYNQGQVCIAGTRLLLEESIADEFLALLKQQAQNWQPGHPLDPATTMGTLIDCAHADSVHSFIREGESKGQLLLDGRNAGLAAAIGPTIFVDVDPNASLSREEIFGPVLVVTRFTSEEQALQLANDSQYGLGAAVWTRDLSRAHRMSRRLKAGSVFVNNYNDGDMTVPFGGYKQSGNGRDKSLHALEKFTELKTIWISLEA

// # Prefix scan on kmers : k_
// # namespases :
//                k_   ->   peptide sequence kmers => [] i_  array de prot ids
//                i_   ->   protein id            [] array des keys pour les autres champs (combinaison pour un même champ ...)
//                n_   ->   proteine / gene name
//                o_   ->   organism_name
//                t_   ->   taxonomy
//                g_   ->   gene ontology
//                f_   ->   function (uniprot)
//                p_   ->   pathway
//                e_   ->   ec_number
//                m_   ->   mass
//                l_   ->   lenght
//                u_   ->   status
//
//  double-letter are combination of values (flyweight design pattern)
//     example : nn_XXXXXXXXXX = combination of names
//
