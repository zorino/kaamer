package align

import (
	"errors"
	"fmt"
	"strings"

	"github.com/biogo/biogo/align/matrix"
)

type MatrixScores struct {
	SubMatrix [][]int
	GapOpen   int
	GapExtend int
	K         float64
	Lambda    float64
}

// Reference : https://github.com/bbuchfink/diamond/blob/master/src/basic/score_matrix.cpp

var (
	AllMatrixScores = map[string]MatrixScores{
		"blosum45_13_3": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 13, GapExtend: 3, Lambda: 0.207, K: 0.049},
		"blosum45_12_3": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 12, GapExtend: 3, Lambda: 0.199, K: 0.039},
		"blosum45_11_3": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 11, GapExtend: 3, Lambda: 0.190, K: 0.031},
		"blosum45_10_3": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 10, GapExtend: 3, Lambda: 0.179, K: 0.023},
		"blosum45_16_2": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 16, GapExtend: 2, Lambda: 0.210, K: 0.051},
		"blosum45_15_2": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 15, GapExtend: 2, Lambda: 0.203, K: 0.041},
		"blosum45_14_2": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 14, GapExtend: 2, Lambda: 0.195, K: 0.032},
		"blosum45_13_2": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 13, GapExtend: 2, Lambda: 0.185, K: 0.024},
		"blosum45_12_2": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 12, GapExtend: 2, Lambda: 0.171, K: 0.016},
		"blosum45_19_1": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 19, GapExtend: 1, Lambda: 0.205, K: 0.040},
		"blosum45_18_1": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 18, GapExtend: 1, Lambda: 0.198, K: 0.032},
		"blosum45_17_1": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 17, GapExtend: 1, Lambda: 0.189, K: 0.024},
		"blosum45_16_1": MatrixScores{SubMatrix: matrix.BLOSUM45, GapOpen: 16, GapExtend: 1, Lambda: 0.176, K: 0.016},
		"blosum50_13_3": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 13, GapExtend: 3, Lambda: 0.212, K: 0.063},
		"blosum50_12_3": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 12, GapExtend: 3, Lambda: 0.206, K: 0.055},
		"blosum50_11_3": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 11, GapExtend: 3, Lambda: 0.197, K: 0.042},
		"blosum50_10_3": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 10, GapExtend: 3, Lambda: 0.186, K: 0.031},
		"blosum50_9_3":  MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 9, GapExtend: 3, Lambda: 0.172, K: 0.022},
		"blosum50_16_2": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 16, GapExtend: 2, Lambda: 0.215, K: 0.066},
		"blosum50_15_2": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 15, GapExtend: 2, Lambda: 0.210, K: 0.058},
		"blosum50_14_2": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 14, GapExtend: 2, Lambda: 0.202, K: 0.045},
		"blosum50_13_2": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 13, GapExtend: 2, Lambda: 0.193, K: 0.035},
		"blosum50_12_2": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 12, GapExtend: 2, Lambda: 0.181, K: 0.025},
		"blosum50_19_1": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 19, GapExtend: 1, Lambda: 0.212, K: 0.057},
		"blosum50_18_1": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 18, GapExtend: 1, Lambda: 0.207, K: 0.050},
		"blosum50_17_1": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 17, GapExtend: 1, Lambda: 0.198, K: 0.037},
		"blosum50_16_1": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 16, GapExtend: 1, Lambda: 0.186, K: 0.025},
		"blosum50_15_1": MatrixScores{SubMatrix: matrix.BLOSUM50, GapOpen: 15, GapExtend: 1, Lambda: 0.171, K: 0.015},
		"blosum62_11_2": MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 11, GapExtend: 2, Lambda: 0.297, K: 0.082},
		"blosum62_10_2": MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 10, GapExtend: 2, Lambda: 0.291, K: 0.075},
		"blosum62_9_2":  MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 9, GapExtend: 2, Lambda: 0.279, K: 0.058},
		"blosum62_8_2":  MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 8, GapExtend: 2, Lambda: 0.264, K: 0.045},
		"blosum62_7_2":  MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 7, GapExtend: 2, Lambda: 0.239, K: 0.027},
		"blosum62_6_2":  MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 6, GapExtend: 2, Lambda: 0.201, K: 0.012},
		"blosum62_13_1": MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 13, GapExtend: 1, Lambda: 0.292, K: 0.071},
		"blosum62_12_1": MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 12, GapExtend: 1, Lambda: 0.283, K: 0.059},
		"blosum62_11_1": MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 11, GapExtend: 1, Lambda: 0.267, K: 0.041},
		"blosum62_10_1": MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 10, GapExtend: 1, Lambda: 0.243, K: 0.024},
		"blosum62_9_1":  MatrixScores{SubMatrix: matrix.BLOSUM62, GapOpen: 9, GapExtend: 1, Lambda: 0.206, K: 0.010},
		"blosum80_25_2": MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 25, GapExtend: 2, Lambda: 0.342, K: 0.17},
		"blosum80_13_2": MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 13, GapExtend: 2, Lambda: 0.336, K: 0.15},
		"blosum80_9_2":  MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 9, GapExtend: 2, Lambda: 0.319, K: 0.11},
		"blosum80_8_2":  MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 8, GapExtend: 2, Lambda: 0.308, K: 0.090},
		"blosum80_7_2":  MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 7, GapExtend: 2, Lambda: 0.293, K: 0.070},
		"blosum80_6_2":  MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 6, GapExtend: 2, Lambda: 0.268, K: 0.045},
		"blosum80_11_1": MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 11, GapExtend: 1, Lambda: 0.314, K: 0.095},
		"blosum80_10_1": MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 10, GapExtend: 1, Lambda: 0.299, K: 0.071},
		"blosum80_9_1":  MatrixScores{SubMatrix: matrix.BLOSUM80, GapOpen: 9, GapExtend: 1, Lambda: 0.279, K: 0.048},
		"blosum90_9_2":  MatrixScores{SubMatrix: matrix.BLOSUM90, GapOpen: 9, GapExtend: 2, Lambda: 0.310, K: 0.12},
		"blosum90_8_2":  MatrixScores{SubMatrix: matrix.BLOSUM90, GapOpen: 8, GapExtend: 2, Lambda: 0.300, K: 0.099},
		"blosum90_7_2":  MatrixScores{SubMatrix: matrix.BLOSUM90, GapOpen: 7, GapExtend: 2, Lambda: 0.283, K: 0.072},
		"blosum90_6_2":  MatrixScores{SubMatrix: matrix.BLOSUM90, GapOpen: 6, GapExtend: 2, Lambda: 0.259, K: 0.048},
		"blosum90_11_1": MatrixScores{SubMatrix: matrix.BLOSUM90, GapOpen: 11, GapExtend: 1, Lambda: 0.302, K: 0.093},
		"blosum90_10_1": MatrixScores{SubMatrix: matrix.BLOSUM90, GapOpen: 10, GapExtend: 1, Lambda: 0.290, K: 0.075},
		"blosum90_9_1":  MatrixScores{SubMatrix: matrix.BLOSUM90, GapOpen: 9, GapExtend: 1, Lambda: 0.265, K: 0.044},
		"pam250_15_3":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 15, GapExtend: 3, Lambda: 0.205, K: 0.049},
		"pam250_14_3":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 14, GapExtend: 3, Lambda: 0.200, K: 0.043},
		"pam250_13_3":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 13, GapExtend: 3, Lambda: 0.194, K: 0.036},
		"pam250_12_3":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 12, GapExtend: 3, Lambda: 0.186, K: 0.029},
		"pam250_11_3":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 11, GapExtend: 3, Lambda: 0.174, K: 0.020},
		"pam250_17_2":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 17, GapExtend: 2, Lambda: 0.204, K: 0.047},
		"pam250_16_2":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 16, GapExtend: 2, Lambda: 0.198, K: 0.038},
		"pam250_15_2":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 15, GapExtend: 2, Lambda: 0.191, K: 0.031},
		"pam250_14_2":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 14, GapExtend: 2, Lambda: 0.182, K: 0.024},
		"pam250_13_2":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 13, GapExtend: 2, Lambda: 0.171, K: 0.017},
		"pam250_21_1":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 21, GapExtend: 1, Lambda: 0.205, K: 0.045},
		"pam250_20_1":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 20, GapExtend: 1, Lambda: 0.199, K: 0.037},
		"pam250_19_1":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 19, GapExtend: 1, Lambda: 0.192, K: 0.029},
		"pam250_18_1":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 18, GapExtend: 1, Lambda: 0.183, K: 0.021},
		"pam250_17_1":   MatrixScores{SubMatrix: matrix.PAM250, GapOpen: 17, GapExtend: 1, Lambda: 0.171, K: 0.014},
		"pam30_7_2":     MatrixScores{SubMatrix: matrix.PAM30, GapOpen: 7, GapExtend: 2, Lambda: 0.305, K: 0.15},
		"pam30_6_2":     MatrixScores{SubMatrix: matrix.PAM30, GapOpen: 6, GapExtend: 2, Lambda: 0.287, K: 0.11},
		"pam30_5_2":     MatrixScores{SubMatrix: matrix.PAM30, GapOpen: 5, GapExtend: 2, Lambda: 0.264, K: 0.079},
		"pam30_10_1":    MatrixScores{SubMatrix: matrix.PAM30, GapOpen: 10, GapExtend: 1, Lambda: 0.309, K: 0.15},
		"pam30_9_1":     MatrixScores{SubMatrix: matrix.PAM30, GapOpen: 9, GapExtend: 1, Lambda: 0.294, K: 0.11},
		"pam30_8_1":     MatrixScores{SubMatrix: matrix.PAM30, GapOpen: 8, GapExtend: 1, Lambda: 0.270, K: 0.072},
		"pam70_8_2":     MatrixScores{SubMatrix: matrix.PAM70, GapOpen: 8, GapExtend: 2, Lambda: 0.301, K: 0.12},
		"pam70_7_2":     MatrixScores{SubMatrix: matrix.PAM70, GapOpen: 7, GapExtend: 2, Lambda: 0.286, K: 0.093},
		"pam70_6_2":     MatrixScores{SubMatrix: matrix.PAM70, GapOpen: 6, GapExtend: 2, Lambda: 0.264, K: 0.064},
		"pam70_11_1":    MatrixScores{SubMatrix: matrix.PAM70, GapOpen: 11, GapExtend: 1, Lambda: 0.305, K: 0.12},
		"pam70_10_1":    MatrixScores{SubMatrix: matrix.PAM70, GapOpen: 10, GapExtend: 1, Lambda: 0.291, K: 0.091},
		"pam70_9_1":     MatrixScores{SubMatrix: matrix.PAM70, GapOpen: 9, GapExtend: 1, Lambda: 0.270, K: 0.060},
	}
)

func GetMatrixScores(subMatrix string, gapOpen int, gapExtend int) (MatrixScores, error) {

	key := fmt.Sprintf("%s_%d_%d", strings.ToLower(subMatrix), gapOpen, gapExtend)

	if m, ok := AllMatrixScores[key]; ok {
		return m, nil
	}

	return MatrixScores{}, errors.New("No matrix found")

}
