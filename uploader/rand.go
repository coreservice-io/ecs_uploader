package uploader

import (
	"math/rand"
	"time"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func init() {
	rand.Seed(time.Now().UnixNano())
}

func genRandStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func (upl *Uploader) GenRandIdStr() string {
	return genRandStr(42)
}
