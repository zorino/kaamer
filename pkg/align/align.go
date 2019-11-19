package align

import (
	"fmt"
	"math"
	"regexp"

	"github.com/biogo/biogo/align"
	"github.com/biogo/biogo/align/matrix"
	"github.com/biogo/biogo/alphabet"
	"github.com/biogo/biogo/seq/linear"
	"github.com/zorino/kaamer/pkg/kvstore"
)

import ()

type AlignmentResult struct {
	Identity     float32
	Length       int
	Mismatches   int
	GapOpenings  int
	Raw          int
	BitScore     float64
	EValue       float64
	AlnString    string
	QueryStart   int
	QueryEnd     int
	SubjectStart int
	SubjectEnd   int
}

type Scorer interface {
	Score() int
}

var (
	re = regexp.MustCompile(`[uU]`)
)

// Make a new anonymous linear.Seq.
func NewAnonLinearSeq(s string) *linear.Seq {
	return &linear.Seq{Seq: alphabet.BytesToLetters([]byte(s))}
}

func Align(querySeq string, refSeq string, dbStats kvstore.KStats, subMatrix string, gapOpen int, gapPenalty int) (AlignmentResult, error) {

	matrixScores, err := GetMatrixScores("blosum62", 11, 1)

	if err != nil {
		return AlignmentResult{}, err
	}

	querySeq = re.ReplaceAllString(querySeq, "*")
	refSeq = re.ReplaceAllString(refSeq, "*")

	nwsa := NewAnonLinearSeq(querySeq)
	nwsa.Alpha = alphabet.Protein
	nwsb := NewAnonLinearSeq(refSeq)
	nwsb.Alpha = alphabet.Protein

	sw := align.SWAffine{
		Matrix:  matrix.BLOSUM62,
		GapOpen: -11,
	}

	aln, err := sw.Align(nwsa, nwsb)

	alnOutput := align.Format(nwsa, nwsb, aln, '-')

	// Compute percent identity and nb. of mismatches
	identity := float32(0)
	nbPos := float32(0)
	mismatches := 0
	aString := fmt.Sprint(alnOutput[0])
	bString := fmt.Sprint(alnOutput[1])
	alnString := fmt.Sprintf("%s\n%s\n", aString, bString)
	for i, a := range aString {
		if rune(bString[i]) == a {
			identity += 1
		} else if rune(bString[i]) != '-' && a != '-' {
			mismatches += 1
		}
		nbPos += 1
	}
	identity = (identity / nbPos) * 100

	// Compute raw score and gap openings
	rawScore := 0
	lenght := len(aString)
	gapOpenings := 0
	gapLen := 0
	queryStart := 0
	queryEnd := 0
	subjectStart := 0
	subjectEnd := 0
	alnLen := len(aln)

	for i, a := range aln {
		if i == 0 {
			queryStart = a.Features()[0].Start()
			subjectStart = a.Features()[1].Start()
		}
		if i == (alnLen - 1) {
			queryEnd = a.Features()[0].End()
			subjectEnd = a.Features()[1].End()
		}

		rawScore += a.(Scorer).Score()
		if a.(Scorer).Score() == -matrixScores.GapOpen {
			gapOpenings += 1
			gapLen = MaxInt(a.Features()[0].Len(), a.Features()[1].Len())
			rawScore = rawScore - ((gapLen - 1) * matrixScores.GapExtend)
		}
	}

	// bit score
	// S′=(lambda∗S−ln(K))/ln(2)
	bitscore := ((matrixScores.Lambda * float64(rawScore)) - math.Log(matrixScores.K)) / math.Log(2)

	// e-value
	// E = n*m / 2^S′
	// m*n = database_size * query_size
	evalue := float64(len(querySeq)) * float64(dbStats.NumberOfAA) / math.Pow(2, bitscore)

	alnScore := AlignmentResult{
		Identity:     identity,
		Length:       lenght,
		Mismatches:   mismatches,
		GapOpenings:  gapOpenings,
		Raw:          rawScore,
		BitScore:     bitscore,
		EValue:       evalue,
		AlnString:    alnString,
		QueryStart:   queryStart + 1,
		QueryEnd:     queryEnd,
		SubjectStart: subjectStart + 1,
		SubjectEnd:   subjectEnd,
	}

	return alnScore, nil

}

func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}
