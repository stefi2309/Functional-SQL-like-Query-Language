type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {
  // functie pt a transforma un rand in string
  private def rowToString(row: Row): String = 
    header.map(key => row.getOrElse(key, "")).mkString(",")

  override def toString: String =
    // functie auxiliara pentru toString
    def op(row: Row, acc: String): String =
      val rowString = rowToString(row)
      if (acc.isEmpty) rowString
      else rowString + "\n" + acc
    val dataString = data.foldRight("")(op)
    val headerString = header.foldRight("")((key,acc) => if (acc.isEmpty) key else key + "," + acc)
    headerString + "\n" + dataString

  // inserez randul doar daca nu exista deja
  def insert(row: Row): Table =
    if(!data.contains(row)) new Table(name, data :+ row)
    else this

  def delete(row: Row): Table =
    new Table(name, data.filterNot(_ == row))

  def sort(column: String): Table =
    new Table(name, data.sortBy(_.getOrElse(column,"")))

  // daca se respecta conditia adaugam updates la rand altfel il lasam asa
  def update(f: FilterCond, updates: Map[String, String]): Table =
    val updatedData = data.map ( row => if (f.eval(row).getOrElse(false)) row ++ updates else row )
    new Table(name, updatedData)

  // construim datele filtrate in functie de conditie daca este respectata sau nu
  def filter(f: FilterCond): Table =
    val filteredData = data.foldRight(List[Row]()) ( (row, acc) =>
      if (f.eval(row).getOrElse(false)) row +: acc else acc )
    new Table(name, filteredData)

  //selectam anumite coloane din tabel
  def select(columns: List[String]): Table =
    new Table(name, data.map(row =>
      row.filter((key, _) => columns.contains(key))))

  def header: List[String] = data.headOption.map(_.keys.toList).getOrElse(Nil)
  def data: Tabular = tableData
  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table =
    // impartim sirul in linii separate
    val lines = s.split("\n").toList
    // extragem antetul
    val header = lines.headOption.map(_.split(",").toList).getOrElse(Nil)
    // construim randurile folosind antetul si valorile din fiecare linie
    val rows: Tabular = lines.tail.map ( line =>
      val values = line.split(",").toList
      header.zip(values).toMap
    )
    new Table(name, rows)
}

extension (table: Table) {
  // functie pt a accesa un anumit rand din tabel
  def apply(index: Int): Row =
    if (index >= 0 && index < table.data.size) table.data(index)
    else throw new IndexOutOfBoundsException("invalid index: " + index)
}


