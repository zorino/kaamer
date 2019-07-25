package search

import (
	"sort"
	"strings"
)

var (
	frameStartPosition = map[int]int{0: 0, 1: 1, 2: 2, 3: 0, 4: 1, 5: 2}
	minLenCDS          = 21
)

type AminoAcid struct {
	AA    string
	Start bool
	Stop  bool
}

type Location struct {
	StartPosition     int
	EndPosition       int
	PlusStrand        bool
	StartsAlternative []int
}

type ORF struct {
	Sequence string
	Location Location
}

func Reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func ReverseComplement(dna string) string {

	r := strings.NewReplacer("a", "t", "t", "a", "g", "c", "c", "g")

	dnaComplement := r.Replace(Reverse(strings.ToLower(dna)))

	return dnaComplement

}

func GetORFs(dna string) []ORF {

	orfs := []ORF{}
	dna = strings.ToLower(dna)
	frames := []string{
		GetFrame(1, dna),
		GetFrame(2, dna),
		GetFrame(3, dna),
		GetFrame(-1, dna),
		GetFrame(-2, dna),
		GetFrame(-3, dna),
	}

	for framePos, frameSeq := range frames {

		// fmt.Printf("Sequence NT (%d): %s\n", len(frameSeq), frameSeq)

		startPos := frameStartPosition[framePos]
		plusStrand := framePos <= 2
		absPos := framePos
		if !plusStrand {
			absPos = len(dna) - startPos - 1
		}
		currentPos := 0

		loc := Location{
			StartPosition:     absPos + 1,
			EndPosition:       0,
			PlusStrand:        plusStrand,
			StartsAlternative: []int{},
		}
		orf := ORF{
			Sequence: "",
			Location: loc,
		}
		insideORF := true
		cds := ""

		var currentAA AminoAcid
		currentAAPos := 0

		for i := 0; i < len(frameSeq)-(len(frameSeq)%3); i += 3 {
			// fmt.Println(gcodeBacteria[f[i:i+3]])
			currentPos = i
			currentAA = gcodeBacteria[frameSeq[i:i+3]]

			if currentAA.Start {
				if insideORF == false {
					insideORF = true
					currentAAPos = 0
					orf.Location.StartPosition = framePos + i + 1
					if !plusStrand {
						orf.Location.StartPosition = len(dna) - (framePos + i) + 3
					}
					orf.Location.StartsAlternative = append(orf.Location.StartsAlternative, currentAAPos)
				} else {
					// new possible start
					orf.Location.StartsAlternative = append(orf.Location.StartsAlternative, currentAAPos)
				}
			}

			if insideORF {
				cds += currentAA.AA
			}

			if currentAA.Stop {
				if insideORF && len(cds) >= minLenCDS {
					endPos := i + 3 + framePos
					if !plusStrand {
						endPos = orf.Location.StartPosition - (len(cds) * 3) + 1
					}
					orf.Location.EndPosition = endPos
					orf.Sequence = cds
					// fmt.Printf("Sequence AA (%d): %s\n", len(cds), cds)
					orfs = append(orfs, orf)
				}
				loc = Location{
					StartPosition:     0,
					EndPosition:       0,
					PlusStrand:        plusStrand,
					StartsAlternative: []int{},
				}
				orf = ORF{
					Sequence: "",
					Location: loc,
				}
				cds = ""
				insideORF = false
			}

			currentAAPos += 1
		}

		if insideORF && len(cds) >= minLenCDS {
			endPos := currentPos + 3 + framePos
			if !plusStrand {
				endPos = orf.Location.StartPosition - (len(cds) * 3) + 1
			}
			orf.Location.EndPosition = endPos
			orf.Sequence = cds
			orfs = append(orfs, orf)
		}

	}

	return orfs

}

func GetFrame(frameNumber int, dna string) string {

	if frameNumber < 0 {
		dna = ReverseComplement(dna)
		frameNumber = -frameNumber
	}

	startPos := frameNumber - 1
	lenFrame := len(dna) - startPos
	endPos := len(dna) - (lenFrame % 3)

	return dna[startPos:endPos]

}

func ResolveORFs(queryResults []QueryResult) []QueryResult {

	goodResults := new([]QueryResult)
	var startPositions []int
	var endPositions []int

	// sort query with most hits first
	sort.Slice(queryResults[:], func(i, j int) bool {
		if len(queryResults[j].SearchResults.Hits) == 0 {
			return true
		} else if len(queryResults[i].SearchResults.Hits) == 0 {
			return false
		}
		return queryResults[i].SearchResults.Hits[0].Kmatch > queryResults[j].SearchResults.Hits[0].Kmatch
	})

	for _, r := range queryResults {
		SetBestStartCodon(&r)
		PruneORFs(r, &startPositions, &endPositions, goodResults)
	}

	return *goodResults

}

func SetBestStartCodon(queryResult *QueryResult) {

	var bestHits []Hit
	bestHitScore := int64(0)

	for _, h := range queryResult.SearchResults.Hits {
		if h.Kmatch >= bestHitScore {
			bestHitScore = h.Kmatch
			bestHits = append(bestHits, h)
		}
	}

	if len(queryResult.Query.Location.StartsAlternative) < 1 {
		return
	}

	bestStart := queryResult.Query.Location.StartsAlternative[0]
	bestStartScore := 0
	firstStart := queryResult.Query.Location.StartsAlternative[0]

	// Set start codon to the first start preceding first best hit position
	firstBestHitPos := 999999999

	if bestStartScore == 0 {
		// haven't found best hit at start codon or next position..
		// choose start codon preceding first best hit position
		exit := false
		for _, bestHit := range bestHits {
			for i, isMatch := range queryResult.SearchResults.PositionHits[bestHit.Key] {
				if isMatch {
					if i < firstBestHitPos {
						firstBestHitPos = i
					}
					exit = true
				}
				if exit {
					break
				}
			}
		}

		exit = false
		for _, s := range queryResult.Query.Location.StartsAlternative {
			if s <= firstBestHitPos {
				bestStart = s
			} else {
				exit = true
			}
			if exit {
				break
			}
		}
	}

	if bestStart != firstStart {
		// fmt.Printf("Correcting start codong for Query at %d bestStart(%d)\n", queryResult.Query.Location.StartPosition, bestStart)
		if queryResult.Query.Location.PlusStrand {
			queryResult.Query.Location.StartPosition = queryResult.Query.Location.StartPosition + 3*(bestStart)
		} else {
			queryResult.Query.Location.StartPosition = queryResult.Query.Location.StartPosition - 3*(bestStart)
		}
		queryResult.Query.Sequence = queryResult.Query.Sequence[bestStart:]
		for _k, _positions := range queryResult.SearchResults.PositionHits {
			queryResult.SearchResults.PositionHits[_k] = _positions[bestStart:]
		}
		queryResult.Query.SizeInKmer = len(queryResult.Query.Sequence) - KMER_SIZE + 1
		if queryResult.Query.Sequence[len(queryResult.Query.Sequence)-1:] == "*" {
			queryResult.Query.SizeInKmer = queryResult.Query.SizeInKmer - 1
		}

	}

	queryResult.Query.Location.StartsAlternative = []int{}

}

func PruneORFs(queryResult QueryResult, startPositions *[]int, endPositions *[]int, goodResults *[]QueryResult) {

	if len(queryResult.SearchResults.Hits) == 0 {
		return
	}

	// Set start and end position in same order for +/- strand
	queryStart := 0
	queryEnd := 0

	if queryResult.Query.Location.PlusStrand {
		queryStart = queryResult.Query.Location.StartPosition
		queryEnd = queryResult.Query.Location.EndPosition
	} else {
		queryStart = queryResult.Query.Location.EndPosition
		queryEnd = queryResult.Query.Location.StartPosition
	}

	if len(*startPositions) == 0 {
		// fmt.Println("Adding because startposition empty")
		*startPositions = append(*startPositions, queryStart)
		*endPositions = append(*endPositions, queryEnd)
		*goodResults = append(*goodResults, queryResult)
		return
	}

	ePos := 0
	overlappedPositions := false

	for i, sPos := range *startPositions {

		ePos = (*endPositions)[i]

		// If overlap is > 60 bps discard
		// see https://bmcgenomics.biomedcentral.com/articles/10.1186/1471-2164-9-335

		// Case left overlap
		// -----|-------|--------
		// ---|------*-----|-----
		if queryStart < sPos &&
			queryEnd > sPos {

			overlappedPositions = true
			overlap := queryEnd - sPos
			if overlap < 60 {
				// fmt.Println("Adding left overlap")
				*startPositions = append(*startPositions, queryStart)
				*endPositions = append(*endPositions, queryEnd)
				*goodResults = append(*goodResults, queryResult)
			}

		}

		// Case right overlap
		// -----|-------|--------
		// -------|--------|-----
		if queryStart > sPos &&
			queryStart < ePos &&
			queryEnd > ePos {

			overlappedPositions = true
			overlap := ePos - queryStart
			if overlap < 60 {
				// fmt.Println("Adding right overlap")
				*startPositions = append(*startPositions, queryStart)
				*endPositions = append(*endPositions, queryEnd)
				*goodResults = append(*goodResults, queryResult)
			} else {
				overlappedPositions = true
			}

		}

		// Case inside overlap
		// -----|-------|--------
		// ------|----|----------
		if queryStart > sPos &&
			queryStart < ePos &&
			queryEnd > sPos &&
			queryEnd < ePos {
			overlappedPositions = true
		}

		// Case outside overlap
		// -----|-------|--------
		// ---|------------|-----
		if queryStart < sPos &&
			queryStart < ePos &&
			queryEnd > sPos &&
			queryEnd > ePos {
			overlappedPositions = true
		}

	}

	if !overlappedPositions {
		// fmt.Println("Adding because no overlap")
		*startPositions = append(*startPositions, queryStart)
		*endPositions = append(*endPositions, queryEnd)
		*goodResults = append(*goodResults, queryResult)
	}

}
