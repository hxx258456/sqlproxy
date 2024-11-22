package sqlparser

import (
	"fmt"
	"regexp"
	"sqlproxy/sqlparser/dependency/querypb"
	"sqlproxy/sqlparser/dependency/sqltypes"
	"strings"
)

type Merge struct {
	Comments  Comments
	Table     *MergeTableExpr
	Matched   MatchedExpr
	Unmatched *UnmatchedExpr
}

func (node *Merge) iStatement() {}

// Format formats the node.
func (node *Merge) Format(buf *TrackedBuffer) {
	buf.Myprintf("merge %vinto %v %v %v",
		node.Comments,
		node.Table, node.Matched, node.Unmatched)
}

func (node *Merge) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Comments,
		node.Table,
		node.Matched,
		node.Unmatched,
	)
}

type MatchedExpr UpdateExprs

// Format formats the node.
func (node MatchedExpr) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("when matched then update set %v", UpdateExprs(node))
}

func (node MatchedExpr) walkSubtree(visit Visit) error {
	return Walk(visit, UpdateExprs(node))
}

type UnmatchedExpr struct {
	Columns Columns
	Values  ValuesExpr
}

// Format formats the node.
func (node *UnmatchedExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("when not matched then insert %v %v",
		node.Columns, node.Values)
}

func (node *UnmatchedExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Columns,
		node.Values,
	)
}

type ValuesExpr []*ColName

// Format formats the node.
func (node ValuesExpr) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	prefix := "values ("
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteString(")")
}

func (node ValuesExpr) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// FindColumn finds a column in the column list, returning
// the index if it exists or -1 otherwise
func (node ValuesExpr) FindColumn(col ColIdent) int {
	for i, colName := range node {
		if colName.Name.Equal(col) {
			return i
		}
	}
	return -1
}

// MergeTableExpr represents a TableExpr that's a JOIN operation.
type MergeTableExpr struct {
	LeftExpr  TableExpr     // AliasedTableExpr
	RightExpr TableExpr     // VirtualTableExpr
	Condition JoinCondition //
}

func (node *MergeTableExpr) iTableExpr() {}

// Format formats the node.
func (node *MergeTableExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v using %v%v", node.LeftExpr, node.RightExpr, node.Condition)
}

func (node *MergeTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.LeftExpr,
		node.RightExpr,
		node.Condition,
	)
}

// using语句拼接的虚拟表定义
type VirtualTableExpr struct {
	Rows      SelectValues // 虚拟表数据，来自于Insert.Rows
	TableName TableIdent   // 表名
	Columns   Columns      // 虚拟表数据声明的列
}

func (node *VirtualTableExpr) iTableExpr() {}

// Format formats the node.
func (node *VirtualTableExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("(%v) %v %v", node.Rows, node.TableName, node.Columns)
}

func (node *VirtualTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Rows,
		node.TableName,
		node.Columns,
	)
}

type SelectValues []SelectTuple

func (node SelectValues) Format(buf *TrackedBuffer) {
	prefix := ""
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = " union all "
	}
}

func (node SelectValues) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

type SelectTuple Exprs

// Format formats the node.
func (node SelectTuple) Format(buf *TrackedBuffer) {
	buf.Myprintf("select %v", Exprs(node))
}

func (node SelectTuple) walkSubtree(visit Visit) error {
	return Walk(visit, Exprs(node))
}

// DmDDL represents a CREATE, ALTER, DROP, RENAME or TRUNCATE statement.
// Table is set for AlterStr, DropStr, RenameStr, TruncateStr
// NewName is set for AlterStr, CreateStr, RenameStr.
// VindexSpec is set for CreateVindexStr, DropVindexStr, AddColVindexStr, DropColVindexStr
// VindexCols is set for AddColVindexStr
type DmDDL struct {
	Action        string
	Table         TableName
	NewName       TableName
	IfExists      bool
	TableSpec     *DmTableSpec
	PartitionSpec *PartitionSpec
	VindexSpec    *VindexSpec
	VindexCols    []ColIdent
}

func (node *DmDDL) iStatement() {}

func (node *DmDDL) FromCreateDDL(ddl *DDL) {

	re := regexp.MustCompile(`auto_increment=\d+`)
	match := re.FindString(ddl.TableSpec.Options)
	if len(match) >= 15 {
		match = match[15:]
	}
	node.Action = ddl.Action
	node.Table = ddl.Table
	node.NewName = ddl.NewName
	node.IfExists = ddl.IfExists
	node.PartitionSpec = ddl.PartitionSpec
	node.VindexSpec = ddl.VindexSpec
	node.VindexCols = ddl.VindexCols
	node.TableSpec = &DmTableSpec{}
	for _, col := range ddl.TableSpec.Columns {
		node.TableSpec.Columns = append(node.TableSpec.Columns, &DmColumnDefinition{
			Name: col.Name,
			Type: DmColumnType{
				AutoIncrement: match,
				Type:          col.Type.Type,
				NotNull:       col.Type.NotNull,
				Default:       col.Type.Default,
				Autoincrement: col.Type.Autoincrement,
				Charset:       col.Type.Charset,
				OnUpdate:      col.Type.OnUpdate,
				Length:        col.Type.Length,
				Unsigned:      col.Type.Unsigned,
				Zerofill:      col.Type.Zerofill,
				Scale:         col.Type.Scale,
				Collate:       col.Type.Collate,
				EnumValues:    col.Type.EnumValues,
				KeyOpt:        col.Type.KeyOpt,
				Comment:       col.Type.Comment,
			},
		})
	}
	for _, i := range ddl.TableSpec.Indexes {
		node.TableSpec.AddIndex(i)
	}
}

// Format formats the node.
func (node *DmDDL) Format(buf *TrackedBuffer) {
	switch node.Action {
	case CreateStr:

		if node.TableSpec == nil {
			buf.Myprintf("%s table %v", node.Action, node.NewName)
		} else {
			buf.Myprintf("%s table %v %v", node.Action, node.NewName, node.TableSpec)
		}
	case DropStr:
		exists := ""
		if node.IfExists {
			exists = " if exists"
		}
		buf.Myprintf("%s table%s %v", node.Action, exists, node.Table)
	case RenameStr:
		buf.Myprintf("%s table %v to %v", node.Action, node.Table, node.NewName)
	case AlterStr:
		if node.PartitionSpec != nil {
			buf.Myprintf("%s table %v %v", node.Action, node.Table, node.PartitionSpec)
		} else {
			buf.Myprintf("%s table %v", node.Action, node.Table)
		}
	case CreateVindexStr:
		buf.Myprintf("%s %v %v", node.Action, node.VindexSpec.Name, node.VindexSpec)
	case AddColVindexStr:
		buf.Myprintf("alter table %v %s %v (", node.Table, node.Action, node.VindexSpec.Name)
		for i, col := range node.VindexCols {
			if i != 0 {
				buf.Myprintf(", %v", col)
			} else {
				buf.Myprintf("%v", col)
			}
		}
		buf.Myprintf(")")
		if node.VindexSpec.Type.String() != "" {
			buf.Myprintf(" %v", node.VindexSpec)
		}
	case DropColVindexStr:
		buf.Myprintf("alter table %v %s %v", node.Table, node.Action, node.VindexSpec.Name)
	default:
		buf.Myprintf("%s table %v", node.Action, node.Table)
	}
}

func (node *DmDDL) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Table,
		node.NewName,
	)
}

// DmTableSpec describes the structure of a table from a CREATE TABLE statement
type DmTableSpec struct {
	Columns []*DmColumnDefinition
	Indexes []*IndexDefinition
	Options string
}

// Format formats the node.
func (dts *DmTableSpec) Format(buf *TrackedBuffer) {
	buf.Myprintf("(\n")
	for i, col := range dts.Columns {
		if i == 0 {
			buf.Myprintf("\t%v", col)
		} else {
			buf.Myprintf(",\n\t%v", col)
		}
	}
	for _, idx := range dts.Indexes {
		buf.Myprintf(",\n\t%v", idx)
	}

	buf.Myprintf("\n)%s", strings.Replace(dts.Options, ", ", ",\n  ", -1))
}

// AddColumn appends the given column to the list in the spec
func (dts *DmTableSpec) AddColumn(cd *DmColumnDefinition) {
	dts.Columns = append(dts.Columns, cd)
}

// AddIndex appends the given index to the list in the spec
func (dts *DmTableSpec) AddIndex(id *IndexDefinition) {
	dts.Indexes = append(dts.Indexes, id)
}

func (dts *DmTableSpec) walkSubtree(visit Visit) error {
	if dts == nil {
		return nil
	}

	for _, n := range dts.Columns {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	for _, n := range dts.Indexes {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
}

// DmColumnDefinition describes a column in a CREATE TABLE statement
type DmColumnDefinition struct {
	Name ColIdent
	Type DmColumnType
}

// Format formats the node.
func (dmCol *DmColumnDefinition) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v %v", dmCol.Name, &dmCol.Type)
}

func (dmCol *DmColumnDefinition) walkSubtree(visit Visit) error {
	if dmCol == nil {
		return nil
	}
	return Walk(
		visit,
		dmCol.Name,
		&dmCol.Type,
	)
}

// DmColumnType represents a sql type in a CREATE TABLE statement
// All optional fields are nil if not specified
type DmColumnType struct {
	// The base type string
	Type string

	// Generic field options.
	NotNull       BoolVal
	Autoincrement BoolVal
	Default       *SQLVal
	OnUpdate      *SQLVal
	Comment       *SQLVal

	// Numeric field options
	Length   *SQLVal
	Unsigned BoolVal
	Zerofill BoolVal
	Scale    *SQLVal

	// Text field options
	Charset string
	Collate string

	// Enum values
	EnumValues []string

	// Key specification
	KeyOpt        ColumnKeyOption
	AutoIncrement string
}

// Format returns a canonical string representation of the type and all relevant options
func (dct *DmColumnType) Format(buf *TrackedBuffer) {
	// 时间格式转换
	switch dct.Type {
	case "datetime":
		buf.Myprintf("%s", "timestamp")
	case "char":
		if dct.Length != nil {
			buf.Myprintf("char(%v)", dct.Length)
		}
	case "varchar":
		if dct.Length != nil {
			buf.Myprintf("varchar(%v CHAR)", dct.Length)
		}
	case "longtext":
		buf.Myprintf("%s", "text")
	case "mediumtext":
		buf.Myprintf("%s", "text")
	default:
		buf.Myprintf("%s", dct.Type)
	}

	//if dct.EnumValues != nil {
	//	buf.Myprintf("(%s)", strings.Join(dct.EnumValues, ", "))
	//}

	opts := make([]string, 0, 16)
	//if dct.Unsigned {
	//	opts = append(opts, keywordStrings[UNSIGNED])
	//}
	// TODO:达梦数据库char会自动填充，想要避免需要使用varchar2
	//if dct.Zerofill {
	//	opts = append(opts, keywordStrings[ZEROFILL])
	//}
	//if dct.Charset != "" {
	//	opts = append(opts, keywordStrings[CHARACTER], keywordStrings[SET], dct.Charset)
	//}
	//if dct.Collate != "" {
	//	opts = append(opts, keywordStrings[COLLATE], dct.Collate)
	//}
	if dct.KeyOpt == colKeyPrimary {
		//opts = append(opts, keywordStrings[PRIMARY], keywordStrings[KEY])
		opts = append(opts, "PRIMARY KEY")
	}

	if dct.NotNull {
		opts = append(opts, keywordStrings[NOT], keywordStrings[NULL])
	}

	if dct.Autoincrement {
		if dct.Type == "bigint" || dct.Type == "integer" || dct.Type == "int" {
			if dct.AutoIncrement != "" {

				opts = append(opts, fmt.Sprintf("IDENTITY(%s,1)", dct.AutoIncrement))
			} else {
				opts = append(opts, "IDENTITY(1,1)")
			}
		}
	}

	if dct.Default != nil {

		switch dct.Type {
		case "timestamp":
			if string(dct.Default.Val) == "current_timestamp" {
				opts = append(opts, keywordStrings[DEFAULT], "CURRENT_TIMESTAMP")
			} else {
				opts = append(opts, keywordStrings[DEFAULT], "NULL")
			}
			//opts = append(opts, keywordStrings[DEFAULT], "CURRENT_TIMESTAMP")
		case "datetime":
			if string(dct.Default.Val) == "current_timestamp" {
				opts = append(opts, keywordStrings[DEFAULT], "CURRENT_TIMESTAMP")
			} else {
				opts = append(opts, keywordStrings[DEFAULT], "NULL")
			}
		default:
			val := string(dct.Default.Val)
			if dct.Type == "char" || dct.Type == "varchar" {
				val = String(dct.Default)
			}
			opts = append(opts, keywordStrings[DEFAULT], val)
		}

	}
	//if dct.OnUpdate != nil {
	//	opts = append(opts, keywordStrings[ON], keywordStrings[UPDATE], String(dct.OnUpdate))
	//}

	if dct.Comment != nil {
		opts = append(opts, keywordStrings[COMMENT_KEYWORD], String(dct.Comment))
	}
	//if dct.KeyOpt == colKeyUnique {
	//	opts = append(opts, keywordStrings[UNIQUE])
	//}
	//if dct.KeyOpt == colKeyUniqueKey {
	//	opts = append(opts, keywordStrings[UNIQUE], keywordStrings[KEY])
	//}
	//if dct.KeyOpt == colKeySpatialKey {
	//	opts = append(opts, keywordStrings[SPATIAL], keywordStrings[KEY])
	//}
	if dct.KeyOpt == colKey {
		opts = append(opts, keywordStrings[KEY])
	}

	if len(opts) != 0 {
		buf.Myprintf(" %s", strings.Join(opts, " "))
	}
}

// DescribeType returns the abbreviated type information as required for
// describe table
func (dct *DmColumnType) DescribeType() string {
	buf := NewTrackedBuffer(nil)
	buf.Myprintf("%s", dct.Type)
	if dct.Length != nil && dct.Scale != nil {
		buf.Myprintf("(%v,%v)", dct.Length, dct.Scale)
	} else if dct.Length != nil {
		buf.Myprintf("(%v)", dct.Length)
	}

	opts := make([]string, 0, 16)
	if dct.Unsigned {
		opts = append(opts, keywordStrings[UNSIGNED])
	}
	if dct.Zerofill {
		opts = append(opts, keywordStrings[ZEROFILL])
	}
	if len(opts) != 0 {
		buf.Myprintf(" %s", strings.Join(opts, " "))
	}
	return buf.String()
}

// SQLType returns the sqltypes type code for the given column
func (dct *DmColumnType) SQLType() querypb.Type {
	switch dct.Type {
	case keywordStrings[TINYINT]:
		if dct.Unsigned {
			return sqltypes.Uint8
		}
		return sqltypes.Int8
	case keywordStrings[SMALLINT]:
		if dct.Unsigned {
			return sqltypes.Uint16
		}
		return sqltypes.Int16
	case keywordStrings[MEDIUMINT]:
		if dct.Unsigned {
			return sqltypes.Uint24
		}
		return sqltypes.Int24
	case keywordStrings[INT]:
		fallthrough
	case keywordStrings[INTEGER]:
		if dct.Unsigned {
			return sqltypes.Uint32
		}
		return sqltypes.Int32
	case keywordStrings[BIGINT]:
		if dct.Unsigned {
			return sqltypes.Uint64
		}
		return sqltypes.Int64
	case keywordStrings[TEXT]:
		return sqltypes.Text
	case keywordStrings[TINYTEXT]:
		return sqltypes.Text
	case keywordStrings[MEDIUMTEXT]:
		return sqltypes.Text
	case keywordStrings[LONGTEXT]:
		return sqltypes.Text
	case keywordStrings[BLOB]:
		return sqltypes.Blob
	case keywordStrings[TINYBLOB]:
		return sqltypes.Blob
	case keywordStrings[MEDIUMBLOB]:
		return sqltypes.Blob
	case keywordStrings[LONGBLOB]:
		return sqltypes.Blob
	case keywordStrings[CHAR]:
		return sqltypes.Char
	case keywordStrings[VARCHAR]:
		return sqltypes.VarChar
	case keywordStrings[BINARY]:
		return sqltypes.Binary
	case keywordStrings[VARBINARY]:
		return sqltypes.VarBinary
	case keywordStrings[DATE]:
		return sqltypes.Date
	case keywordStrings[TIME]:
		return sqltypes.Time
	case keywordStrings[DATETIME]:
		return sqltypes.Datetime
	case keywordStrings[TIMESTAMP]:
		return sqltypes.Timestamp
	case keywordStrings[YEAR]:
		return sqltypes.Year
	case keywordStrings[FLOAT_TYPE]:
		return sqltypes.Float32
	case keywordStrings[DOUBLE]:
		return sqltypes.Float64
	case keywordStrings[DECIMAL]:
		return sqltypes.Decimal
	case keywordStrings[BIT]:
		return sqltypes.Bit
	case keywordStrings[ENUM]:
		return sqltypes.Enum
	case keywordStrings[SET]:
		return sqltypes.Set
	case keywordStrings[JSON]:
		return sqltypes.TypeJSON
	case keywordStrings[GEOMETRY]:
		return sqltypes.Geometry
	case keywordStrings[POINT]:
		return sqltypes.Geometry
	case keywordStrings[LINESTRING]:
		return sqltypes.Geometry
	case keywordStrings[POLYGON]:
		return sqltypes.Geometry
	case keywordStrings[GEOMETRYCOLLECTION]:
		return sqltypes.Geometry
	case keywordStrings[MULTIPOINT]:
		return sqltypes.Geometry
	case keywordStrings[MULTILINESTRING]:
		return sqltypes.Geometry
	case keywordStrings[MULTIPOLYGON]:
		return sqltypes.Geometry
	}
	panic("unimplemented type " + dct.Type)
}

func (dct *DmColumnType) walkSubtree(visit Visit) error {
	return nil
}

// DMDBDDL represents a CREATE, DROP database statement.
type DMDBDDL struct {
	Action   string
	DBName   string
	IfExists bool
	Collate  string
	Charset  string
}

func (node *DMDBDDL) iStatement() {}

// Format formats the node.
func (node *DMDBDDL) Format(buf *TrackedBuffer) {
	switch node.Action {
	case CreateStr:
		buf.WriteString(fmt.Sprintf("%s schema %s", node.Action, node.DBName))
	case DropStr:
		exists := ""
		if node.IfExists {
			exists = " if exists"
		}
		buf.WriteString(fmt.Sprintf("%s schema%s %v", node.Action, exists, node.DBName))
	}
}

// walkSubtree walks the nodes of the subtree.
func (node *DMDBDDL) walkSubtree(visit Visit) error {
	return nil
}

// DmUse represents a use statement.
type DmUse struct {
	DBName TableIdent
}

func (node *DmUse) iStatement() {}

// Format formats the node.
func (node *DmUse) Format(buf *TrackedBuffer) {
	if node.DBName.v != "" {
		buf.Myprintf("set schema %v", node.DBName)
	}
}

func (node *DmUse) walkSubtree(visit Visit) error {
	return Walk(visit, node.DBName)
}
