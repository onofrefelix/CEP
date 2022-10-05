package main

import (
	"CEP/files"
	"CEP/tools"
	"fmt"
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

	fmt.Println(tools.Filter(listFile, ".TXT")[0:])

}
