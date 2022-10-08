package main

import (
	"CEP/dataframetools"
	"CEP/files"
	"CEP/tools"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"log"
)

type CepConfig struct {
	Logradouro  string
	Bairro      string
	FaixaBairro string
	Cpcd        string
}

//type SetCepCconfig struct {
//	FilterFile        string   `json:"filterFile"`
//	SetColImportant   []int    `json:"setColImportant"`
//	SetColNames       []string `json:"setColNames"`
//	SetDelimeter      rune     `json:"setDelimeter"`
//	HasHeader         bool     `json:"hasHeader"`
//	HasWithLazyQuotes bool     `json:"hasWithLazyQuotes"`
//	UnicodeModel      string   `json:"unicodeModel"`
//}

func main() {
	var orig, dest string
	var dataframe_list []dataframe.DataFrame

	orig = "./data/atualiza_base_cep_out_2022.zip"
	dest = "./data/unzipfiles/"
	fmt.Println(orig)

	//files.Unzip(orig, dest)
	listFile, err := files.ScanDir(dest)

	if err != nil {
		print(err)
	}

	setCepCconfig := dataframetools.SetCepCconfig{FilterFile: "atualiza_cep/LOG", SetColImportant: []int{1, 2, 3, 7},
		SetColNames: []string{"uf", "loc_nu", "bai_nu", "cep"}, SetDelimeter: '@', HasHeader: false, HasWithLazyQuotes: true,
		UnicodeModel: "ISO88591"}
	setCepCconfig1 := dataframetools.SetCepCconfig{FilterFile: "LOG_BAIRRO", SetColImportant: []int{0, 1, 2, 3},
		SetColNames: []string{"bai_nu", "uf", "loc_nu", "bairro"}, SetDelimeter: '@', HasHeader: false, HasWithLazyQuotes: true,
		UnicodeModel: "ISO88591"}
	setCepCconfig2 := dataframetools.SetCepCconfig{FilterFile: "LOG_CPC", SetColImportant: []int{1, 2, 3, 5},
		SetColNames: []string{"uf", "loc_nu", "cpc_nu", "cep"}, SetDelimeter: '@', HasHeader: false, HasWithLazyQuotes: true,
		UnicodeModel: "ISO88591"}
	setCepCconfig3 := dataframetools.SetCepCconfig{FilterFile: "LOG_FAIXA_BAIRRO", SetColImportant: []int{0, 1, 2},
		SetColNames: []string{"bai_nu", "cep_inicial", "cep_final"}, SetDelimeter: '@', HasHeader: false, HasWithLazyQuotes: true,
		UnicodeModel: "ISO88591"}
	setCepCconfig4 := dataframetools.SetCepCconfig{FilterFile: "LOG_LOCALIDADE.TXT", SetColImportant: []int{0, 1, 2, 3},
		SetColNames: []string{"loc_nu", "uf", "municipio", "cep"}, SetDelimeter: '@', HasHeader: false, HasWithLazyQuotes: true,
		UnicodeModel: "ISO88591"}

	arrCepConfig := []dataframetools.SetCepCconfig{setCepCconfig, setCepCconfig1, setCepCconfig2, setCepCconfig3, setCepCconfig4}

	for _, cep := range arrCepConfig {
		listf := tools.Filter(listFile, cep.FilterFile)
		//fmt.Println(listf)

		var df dataframe.DataFrame

		df, err = dataframetools.ReadData(listf, cep)
		if err != nil {
			log.Fatalf("data can't be reading")
		}
		//fmt.Println(df)

		dataframe_list = append(dataframe_list, df)

	}

	dataframe_list[0] = dataframe_list[0].Concat(dataframe_list[2].Select([]string{"uf", "loc_nu", "cep"}))

	f := dataframe.F{Colname: "uf", Comparator: series.Eq, Comparando: "AC"}
	dataframe_list[0] = dataframe_list[0].Filter(f)
	dataframe_list[1] = dataframe_list[1].LeftJoin(dataframe_list[4].Select([]string{"loc_nu", "municipio"}), "loc_nu")
	dataframe_list[0] = dataframe_list[0].LeftJoin(dataframe_list[1].Select([]string{"bai_nu", "municipio", "bairro"}), "bai_nu")
	//dataframe_list[0] = dataframe_list[0].LeftJoin(dataframe_list[1].Select([]string{"bai_nu", "bairro"}), "bai_nu")

	fmt.Println(dataframe_list[0])
	fmt.Println(dataframe_list[1])
	fmt.Println(dataframe_list[4])
}
