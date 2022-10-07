package main

import (
	"CEP/files"
	"CEP/tools"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"golang.org/x/text/encoding/charmap"
	"log"
	"os"
)

type CepConfig struct {
	Logradouro  string
	Bairro      string
	FaixaBairro string
	Cpcd        string
}

func main() {
	var orig, dest string

	orig = "./data/atualiza_base_cep_out_2022.zip"
	dest = "./data/unzipfiles/"

	cepData := CepConfig{
		Logradouro:  "LOG_LOGRADOURO",
		Bairro:      "LOG_BAIRRO",
		FaixaBairro: "LOG_BAIRRO",
		Cpcd:        "LOG_CPC",
	}

	files.Unzip(orig, dest)
	listFile, err := files.ScanDir(dest)
	if err != nil {
		print(err)
	}

	listf := tools.Filter(listFile, cepData.Logradouro)

	//var dataFrameList []dataframe.DataFrame
	var df dataframe.DataFrame

	df, err = ReadData(listf)
	if err != nil {
		log.Fatalf("data can't be reading")
	}

	fmt.Println(df) //df.Select([]int{1, 2, 3, 7))
	fmt.Println(df.Describe())

}

func ReadData(listOfFilename []string) (dataframe.DataFrame, error) {

	//var temp string
	//var content []byte
	//var err error
	//
	//for _, filename := range listOfFilename {
	//	content, err = os.ReadFile(filename)
	//	if err != nil {
	//		return dataframe.DataFrame{}, err
	//	}
	//
	//	temp += string(content)
	//}
	//ioContent := strings.NewReader(string(content))
	//
	//return dataframe.ReadCSV(ioContent, dataframe.WithDelimiter('@'),
	//	dataframe.WithLazyQuotes(true),
	//	dataframe.HasHeader(false)), nil

	//var temp string
	//var content []byte

	var df, dfConcat dataframe.DataFrame

	for _, filename := range listOfFilename {
		content, err := os.Open(filename)
		if err != nil {
			return dataframe.DataFrame{}, err
		}

		//ioContent := strings.NewReader(string(content))
		ioContent := charmap.ISO8859_1.NewDecoder().Reader(content)
		df = dataframe.ReadCSV(ioContent, dataframe.WithDelimiter('@'),
			dataframe.WithLazyQuotes(true),
			dataframe.HasHeader(false))
		dfConcat = dfConcat.Concat(df)

	}

	return dfConcat, nil
}
