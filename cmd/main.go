package main

import (
	"CEP/files"
	"CEP/tools"
	"github.com/go-gota/gota/dataframe"
	"log"
	"os"
	"strings"
)

func main() {
	var orig, dest string

	orig = "./data/atualiza_base_cep_out_2022.zip"
	dest = "./data/unzipfiles/"

	files.Unzip(orig, dest)
	listFile, err := files.ScanDir(dest)
	if err != nil {
		print(err)
	}

	listf := tools.Filter(listFile, ".TXT")

	var dataFrameList []dataframe.DataFrame
	var df dataframe.DataFrame

	for _, file := range listf {
		df, err = ReadData(file)
		if err != nil {
			log.Fatalf("data can't be reading")
		}
		dataFrameList = append(dataFrameList, df)
	}

}

func ReadData(filename string) (dataframe.DataFrame, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return dataframe.DataFrame{}, err
	}

	ioContent := strings.NewReader(string(content))

	return dataframe.ReadCSV(ioContent, dataframe.WithDelimiter('@')), nil

}
