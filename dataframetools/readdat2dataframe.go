package dataframetools

import (
	"github.com/go-gota/gota/dataframe"
	"golang.org/x/text/encoding/charmap"
	"os"
)

type SetCepCconfig struct {
	FilterFile        string   `json:"filterFile"`
	SetColImportant   []int    `json:"setColImportant"`
	SetColNames       []string `json:"setColNames"`
	SetDelimeter      rune     `json:"setDelimeter"`
	HasHeader         bool     `json:"hasHeader"`
	HasWithLazyQuotes bool     `json:"hasWithLazyQuotes"`
	UnicodeModel      string   `json:"unicodeModel"`
}

func ReadData(listOfFilename []string, config SetCepCconfig) (dataframe.DataFrame, error) {

	var df, dfConcat dataframe.DataFrame

	for _, filename := range listOfFilename {
		content, err := os.Open(filename)
		if err != nil {
			return dataframe.DataFrame{}, err
		}

		ioContent := charmap.ISO8859_1.NewDecoder().Reader(content)
		df = dataframe.ReadCSV(ioContent, dataframe.WithDelimiter(config.SetDelimeter),
			dataframe.WithLazyQuotes(config.HasWithLazyQuotes),
			dataframe.HasHeader(config.HasHeader))
		dfConcat = dfConcat.Concat(df)

	}
	dfConcat = dfConcat.Select(config.SetColImportant)
	dfConcat.SetNames(config.SetColNames...)

	return dfConcat, nil
}
