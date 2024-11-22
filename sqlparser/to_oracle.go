package sqlparser

import (
	"fmt"
	"log"
	"sqlproxy/core/golog"
	"strconv"
	"strings"
	"unicode"
)

type OracleConverter struct {
	replaceChars      map[string]string
	tableUniqueIndexs map[string]map[string][]string
	tableColumns      map[string][]string
	incrementColumns  map[string]map[string]int
}

func NewOracleConverter(tableUniqueIndexs map[string]map[string][]string, tableColumns map[string][]string, incrementColumns map[string]map[string]int) *OracleConverter {
	return &OracleConverter{
		replaceChars: map[string]string{
			"`":                   "\"",
			"0000-00-00 00:00:00": "0001-01-01 00:00:00",
			`\'`:                  `''`,
		},
		tableUniqueIndexs: tableUniqueIndexs,
		incrementColumns:  incrementColumns,
		tableColumns:      tableColumns,
	}
}

// Convert sql from mysql to oracle
// 1. parse sql to ast
// 2. check if need to convert
// 3. convert mysql ast to oracle ast
// 4. rebuild oracle sql from ast
func (c *OracleConverter) Convert(sql string, args ...interface{}) ([]string, []string, []interface{}, error) {
	sqlType := Preview(sql)
	if !supportConvert(sqlType) {
		return nil, []string{sql}, args, nil
	}
	sql, fks := SplitFk(sql)
	stmt, err := Parse(sql)
	if err != nil {
		log.Printf("ignoring error parsing sql '%s': %v", sql, err)
		return nil, nil, args, err
	}
	for i, fk := range fks {
		fks[i] = fmt.Sprintf("alter table `%s` add %s;", stmt.(*DDL).NewName.Name, fk)
	}

	// 转换statement时可能带来参数数量的变化，例如：replace转merge， insert去掉increment column等，
	// 因此，参数args也需要作相应的配套处理
	oracleStmt, args := c.convertStmt(stmt, args...)

	if oracleStmt == nil {
		return fks, []string{c.replaceCommonIdents(sql)}, args, nil
	}
	buf := NewTrackedBuffer(nil).WriteNode(oracleStmt)
	convertSQL := c.replaceCommonIdents(buf.String())
	golog.Info("OracleConverter", "Convert", "ConvertSQL", 0, convertSQL)
	return fks, []string{convertSQL}, args, nil
}

func (c *OracleConverter) convertStmt(stmt Statement, args ...interface{}) (Statement, []interface{}) {
	var newStmt Statement
	switch stmt.(type) {
	case *Insert:
		newStmt = c.convertInsert(stmt.(*Insert))
	case *Update:
		newStmt = c.convertUpdateIncrement(stmt.(*Update))
	case *Select:
		newStmt = c.convertSelect(stmt.(*Select))
	case *DDL:
		newStmt = c.convertDDL(stmt.(*DDL))
	case *DBDDL:
		newStmt = c.convertDBDDL(stmt.(*DBDDL))
	default:
		newStmt = stmt
	}

	if c.needConvertArgs(newStmt, args...) {
		return c.convertStmtArgs(newStmt, args...)
	}
	return newStmt, args
}

func (c *OracleConverter) convertSelect(stmt *Select) Statement {
	for _, expr := range stmt.From {
		if t, ok := expr.(*AliasedTableExpr); ok {
			if t.Hints != nil && t.Hints.Type == "force " {
				t.Hints = nil
			}
		}
	}
	return stmt
}

func (c *OracleConverter) needConvertArgs(stmt Statement, args ...interface{}) bool {
	if len(args) == 0 {
		return false
	}
	switch stmt.(type) {
	case *Merge, *Insert:
		return true
	default:
		return false
	}
}

func (c *OracleConverter) convertStmtArgs(stmt Statement, args ...interface{}) (Statement, []interface{}) {
	newArgs := []interface{}{}
	id := 1
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case *SQLVal:
			n := node.(*SQLVal)
			v := string(n.Val)
			i, _ := strconv.Atoi(strings.ReplaceAll(v, ":v", ""))
			n.Val = []byte(fmt.Sprintf(":v%d", id))
			newArgs = append(newArgs, args[i-1])
			id++
			return true, nil
		default:
			return true, nil
		}
	}
	_ = Walk(visit, stmt)
	return stmt, newArgs
}

func (c *OracleConverter) convertInsert(stmt *Insert) Statement {
	// try to find auto increment columns and remove them
	stmt = c.convertInsertIncrement(stmt)
	// write a method to walk ast tree, recognize all kinds of expr, and rebuild oracle ast
	if stmt.Action == InsertStr && stmt.OnDup == nil {
		return stmt
	}
	// The case where columns is empty is not currently supported for conversion.
	if len(stmt.Columns) == 0 {
		return stmt
	}

	// find unique columns for table
	condcols := c.getUniqueConditionColumns(stmt)
	if len(condcols) == 0 {
		stmt.OnDup = nil
		return stmt
	}
	tableExpr := c.buildMergeTableExpr(stmt, condcols)
	matchedExpr := buildMatchedExpr(stmt, condcols)

	if tableExpr == nil || len(matchedExpr) == 0 {
		return stmt
	}

	// sets the qualifier for columns
	// setQualifierForCols(tableExpr)
	setQualifierForCols(matchedExpr)
	log.Printf("condcols: %v, tableExpr: %v, matchedExpr: %v", condcols, tableExpr, matchedExpr)

	return &Merge{
		Comments: stmt.Comments,
		Table:    tableExpr,
		Matched:  matchedExpr,
		Unmatched: &UnmatchedExpr{
			Columns: stmt.Columns,
			// Rows:    Rows,
			Values: buildValuesExpr(stmt),
		},
	}
}

func buildValuesExpr(stmt *Insert) ValuesExpr {
	values := make([]*ColName, 0, len(stmt.Columns))
	for _, column := range stmt.Columns {
		values = append(values, &ColName{
			Name: column,
			Qualifier: TableName{
				Name: TableIdent{v: "s"},
			},
		})
	}
	return ValuesExpr(values)
}

func (c *OracleConverter) convertInsertIncrement(stmt *Insert) *Insert {
	if _, ok := stmt.Rows.(Values); !ok {
		return stmt
	}

	incrementColumns := c.incrementColumns[stmt.Table.Name.String()]
	if incrementColumns == nil || len(incrementColumns) == 0 {
		return stmt
	}

	// 没有指定columns的补充columns
	if len(stmt.Columns) == 0 {
		stmt.Columns = []ColIdent{}
		for _, column := range c.tableColumns[stmt.Table.Name.String()] {
			stmt.Columns = append(stmt.Columns, NewColIdent(column))
		}
	}

	// remove auto increment columns
	ns := []int{}
	newColumns := []ColIdent{}
	for i, column := range stmt.Columns {
		if _, ok := incrementColumns[column.String()]; ok {
			ns = append(ns, i)
		} else {
			newColumns = append(newColumns, column)
		}
		stmt.Columns = newColumns
	}

	var rows Values
	for _, row := range stmt.Rows.(Values) {
		newRow := ValTuple{}
		for i, v := range row {
			if len(ns) == 0 {
				newRow = append(newRow, v)
			} else {
				for n := range ns {
					if i != n {
						newRow = append(newRow, v)
					}
				}
			}
		}
		rows = append(rows, newRow)
	}
	stmt.Rows = rows
	return stmt
}

func (c *OracleConverter) convertUpdateIncrement(stmt *Update) *Update {
	if len(stmt.TableExprs) == 0 {
		return stmt
	}
	incrementColumns := c.incrementColumns[getTableName(stmt)]
	if incrementColumns == nil || len(incrementColumns) == 0 {
		return stmt
	}

	// remove auto increment columns
	newExprs := make([]*UpdateExpr, 0, len(stmt.Exprs))
	for _, expr := range stmt.Exprs {
		if _, ok := incrementColumns[expr.Name.Name.String()]; ok {
			continue
		}
		newExprs = append(newExprs, expr)
	}
	stmt.Exprs = newExprs
	return stmt
}

func (c *OracleConverter) buildMergeTableExpr(stmt *Insert, condcols [][]string) *MergeTableExpr {
	onCondition := buildJoinConditions(stmt, condcols)
	if onCondition == nil {
		return nil
	}

	return &MergeTableExpr{
		LeftExpr: &AliasedTableExpr{
			Expr: stmt.Table,
			As:   NewTableIdent("t"),
		},
		RightExpr: buildRightTableExpr(stmt),
		Condition: JoinCondition{
			On: onCondition,
		},
	}
}

func (c *OracleConverter) getUniqueConditionColumns(stmt *Insert) [][]string {
	condcols := [][]string{}
	// Case1: If user has configured unique index condcols for the table, use it as condition condcols
	tableIndexs := c.tableUniqueIndexs[stmt.Table.Name.String()]
	for _, iii := range tableIndexs {
		i := 0
		for _, column := range stmt.Columns {
			for _, v := range iii {
				if v == column.String() {
					i++
				}
			}
		}
		if i == len(iii) {
			condcols = append(condcols, iii)
		}
	}

	return condcols
}

func buildMatchedExpr(stmt *Insert, condcols [][]string) MatchedExpr {
	if stmt.OnDup != nil {
		return MatchedExpr(stmt.OnDup)
	}
	vals := getInsertValues(stmt)
	exprs := make([]*UpdateExpr, 0, len(vals))

	allCondcol := []string{}
	for _, condcol := range condcols {
		allCondcol = append(allCondcol, condcol...)
	}
	for i, column := range stmt.Columns {

		if !StringIn(column.String(), allCondcol...) {
			exprs = append(exprs, &UpdateExpr{
				Name: &ColName{Name: column},
				Expr: vals[i],
			})
		}
	}
	return exprs
}

func getInsertValues(stmt *Insert) []*SQLVal {
	vals := make([]*SQLVal, 0, len(stmt.Columns))
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case *SQLVal:
			vals = append(vals, node.(*SQLVal))
			return true, nil
		default:
			return true, nil
		}
	}
	Walk(visit, stmt.Rows)
	return vals
}

func getTableName(stmt *Update) string {
	var tableName string
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case TableName:
			table := node.(TableName)
			tableName = table.Name.String()
			return false, nil
		default:
			return true, nil
		}
	}
	Walk(visit, stmt.TableExprs)
	return tableName
}

// sets the qualifier for columns in the SQLNode.
//
// It takes a SQLNode as a parameter and sets the qualifier of any ColName
// nodes to TableName "t".
func setQualifierForCols(node SQLNode) SQLNode {
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case *ColName:
			node.(*ColName).Qualifier = TableName{
				Name: NewTableIdent("t"),
			}
			//log.Printf("set qualifier for column: %v", node.(*ColName).Name)
			return true, nil
		default:
			return true, nil
		}
	}
	Walk(visit, node)
	return node
}

func buildRightTableExpr(stmt *Insert) *VirtualTableExpr {
	return &VirtualTableExpr{
		TableName: NewTableIdent("s"),
		Columns:   stmt.Columns,
		Rows:      buildSelectValues(stmt),
	}
}

func buildSelectValues(stmt *Insert) SelectValues {
	values := make([]SelectTuple, 0, 10)
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case Exprs:
			values = append(values, SelectTuple(node.(Exprs)))
			return true, nil
		default:
			return true, nil
		}
	}
	Walk(visit, stmt.Rows)
	return SelectValues(values)
}

func buildJoinConditions(stmt *Insert, condcols [][]string) Expr {
	exprs := make([][]*ComparisonExpr, 0, len(condcols))
	for _, condcol := range condcols {
		expr := make([]*ComparisonExpr, 0, len(condcol))
		for _, column := range stmt.Columns {
			if StringIn(column.String(), condcol...) {
				expr = append(expr, &ComparisonExpr{
					Operator: EqualStr,
					Left: &ColName{
						Name: column,
						Qualifier: TableName{
							Name: NewTableIdent("t"),
						},
					},
					Right: &ColName{
						Name: column,
						Qualifier: TableName{
							Name: NewTableIdent("s"),
						},
					},
				})
			}
		}
		exprs = append(exprs, expr)
	}

	if len(exprs) == 0 {
		return nil
	}
	var conditions Expr
	for i, expr := range exprs {
		var condition Expr
		for j, comparisonExpr := range expr {
			if j == 0 {
				condition = comparisonExpr
			} else {
				condition = &AndExpr{condition, comparisonExpr}
			}
		}
		if i == 0 {
			conditions = condition
		} else {
			conditions = &OrExpr{conditions, condition}
		}
	}

	return conditions
}

func (c *OracleConverter) replaceCommonIdents(sql string) string {
	for old, new := range c.replaceChars {
		sql = strings.Replace(sql, old, new, -1)
	}
	return sql
}

func (c *OracleConverter) convertDDL(stmt *DDL) Statement {
	switch stmt.Action {
	case CreateStr:
		newDmDdl := &DmDDL{}
		newDmDdl.FromCreateDDL(stmt)
		return newDmDdl
	case AlterStr:
		return nil
	}
	return nil
}

func (c *OracleConverter) covertUse(stmt *Use) Statement {
	return &DmUse{DBName: stmt.DBName}
}
func (c *OracleConverter) convertDBDDL(stmt *DBDDL) Statement {
	switch stmt.Action {
	case CreateStr:
		newDMDBDDL := &DMDBDDL{
			Action:   stmt.Action,
			DBName:   stmt.DBName,
			IfExists: stmt.IfExists,
			Charset:  stmt.Charset,
			Collate:  stmt.Collate,
		}
		return newDMDBDDL
	case DropStr:
		newDMDBDDL := &DMDBDDL{
			Action:   stmt.Action,
			DBName:   stmt.DBName,
			IfExists: stmt.IfExists,
			Charset:  stmt.Charset,
			Collate:  stmt.Collate,
		}
		return newDMDBDDL
	}
	return nil
}

func supportConvert(sqlType int) bool {
	switch sqlType {
	case StmtDDL, StmtInsert, StmtReplace, StmtDelete, StmtSelect, StmtUpdate, StmtUse:
		return true
	default:
		return false
	}
}

func SplitFk(sql string) (string, []string) {
	trimmed := StripLeadingComments(sql)

	firstWord := trimmed
	if end := strings.IndexFunc(trimmed, unicode.IsSpace); end != -1 {
		firstWord = trimmed[:end]
	}
	firstWord = strings.TrimLeftFunc(firstWord, func(r rune) bool { return !unicode.IsLetter(r) })
	// Comparison is done in order of priority.
	loweredFirstWord := strings.ToLower(firstWord)
	if loweredFirstWord != "create" {
		return sql, nil
	}
	lists := strings.Split(sql, "\n")
	var fks []string
	for i, v := range lists {
		if strings.Contains(v, "CONSTRAINT") || strings.Contains(v, "FOREIGN KEY") || strings.Contains(v, "constraint") || strings.Contains(v, "foreign key") {
			lists = append(lists[:i], lists[i+1:]...)
			fks = append(fks, v)
		}
	}

	lists[len(lists)-2] = strings.TrimRight(lists[len(lists)-2], ",")
	sql = strings.Join(lists, "\n")

	return sql, fks
}
