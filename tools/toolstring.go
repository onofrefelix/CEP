package tools

import "strings"

func Filter(arr []string, filter string) []string {
	var result []string
	for i := range arr {
		if strings.Contains(arr[i], filter) {
			result = append(result, arr[i])
		}
	}
	return result
}
