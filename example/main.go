package main

import (
	"fmt"
	"io/ioutil"
	"sqlproxy/sqlparser"
	"strings"
)

func main() {
	filename := "E:\\gxcx\\database-gc\\sqlproxy\\example\\tendim_bi.sql"
	content, err := readSqlFile(filename)
	if err != nil {
		panic(err)
	}
	parserContent(content)
}

func readSqlFile(filePath string) ([]byte, error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func parserContent(content []byte) {
	sqls := strings.Split(string(content), ";")
	converter := sqlparser.GetSQLConverter(sqlparser.MYSQL_TO_ORACLE, nil, nil, nil)

	for _, sql := range sqls {
		sql = strings.TrimSpace(sql)
		if len(sql) <= 0 {
			continue
		}
		switch sql[0] {
		case 47:
			continue
		case 45:
			for _, v := range strings.Split(sql, "\n") {
				if len(v) <= 0 {
					continue
				}
				if v[0] == 45 {
					continue
				}
				if v[0] == 47 {
					continue
				}
				_, res, _, err := converter.Convert(sql)
				if err != nil {
					fmt.Println(sql)
					panic(err)
				}
				fmt.Println(res[0])
			}
		default:
			_, res, _, err := converter.Convert(sql)
			if err != nil {
				panic(err)
			}
			fmt.Println(res[0])
		}

	}
}
