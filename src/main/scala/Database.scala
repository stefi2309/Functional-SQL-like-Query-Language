case class Database(tables: List[Table]) {
  override def toString: String = tables.map(_.name).mkString(",")

  // daca tabelul care trebuie creat exista deja il returnam
  // altfel il cream si adaugam la lista de tabele
  def create(tableName: String): Database =
    if (tables.exists(_.name == tableName)) this
    else Database(tables :+ Table(tableName, List.empty))

  def drop(tableName: String): Database = Database(tables.filterNot(_.name == tableName))
  
  // selectam tabelele pe baza listei de nume de tabele
  def selectTables(tableNames: List[String]): Option[Database] =
    val selectedTables = tables.filter(table => tableNames.contains(table.name))
    // verificam daca nr de tabele selectate este egal cu nr de tabele date
    if (selectedTables.length == tableNames.length) Some(Database(selectedTables))
    else None

  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] =
    // cautam tabelele pe baza numelor
    val tab1 = tables.find(_.name == table1)
    val tab2 = tables.find(_.name == table2)
    (tab1, tab2) match
      // verificam daca ambele tabele exista
      case (Some(t1), Some(t2)) =>
        if (t1.data.isEmpty) return Some(t2)
        if (t2.data.isEmpty) return Some(t1)
        // determinam toate antetele coloanelor si eliminam duplicata daca c1 si c2 sunt egale
        val allHeaders =
          if (c1 == c2) (t1.header ++ t2.header).distinct
          else (t1.header ++ t2.header).filterNot(_ == c2)
        // construim un map pt a accesa randurile din t2 pe baza valorilor din c2
        val mapTable2 = t2.data.map(row => row.getOrElse(c2, "") -> row).toMap
        // construim randurile rezultate din operatia join
        val joinRows = t1.data.flatMap ( row1 =>
          val key1 = row1.getOrElse(c1, "")
          mapTable2.get(key1).map ( row2 =>
            val combinedRow = allHeaders.map ( column =>
              val value1 = row1.getOrElse(column, "")
              val value2 = row2.getOrElse(column, "")
              column -> (
                if (value1.isEmpty) value2
                else if (value2.isEmpty || value1 == value2) value1
                else value1 + ";" + value2 )
            ).toMap
            // adaugam randul combinat la lista de randuri rezultate
            // flag 0 (din ambele tabele)
            // index -1 (din t1 si se potrivește cu t2)
            List((combinedRow, 0, -1))
          ).getOrElse {
            // daca nu exista in t2 se adauga randul din t1 cu valorile lipsa pt coloanele din t2
            val rowWithEmptyValuesForT2 = t2.header.filterNot
              (h => t1.header.contains(h) || h == c2).map(column => column -> "").toMap
            // adaugam randul la lista de randuri rezultate
            // flag 1 (din t1)
            // index -1 (din t1)
            List((row1 ++ rowWithEmptyValuesForT2, 1, -1))
          }
          )
        // construim randurile ramase din t2 care nu sunt in t1
        val remainingRows = t2.data.zipWithIndex.collect {
          case (row2, index) if !t1.data.exists(_.getOrElse(c1, "") == row2.getOrElse(c2, "")) =>
            // construim un rand nou cu valori din t2 si valori lipsa pt coloanele din t1
            val rowWithEmptyValuesForT1 = t1.header.map(column => column -> "").toMap
            val updatedRow2 = rowWithEmptyValuesForT1 ++ row2 - c2 + (c1 -> row2.getOrElse(c2, ""))
            // adaugam randul la lista de randuri rezultate
            // flag 2 (din t2)
            // indexul original pt a pastra ordinea
            (updatedRow2, 2, index)
        }
        // sortam randurile dupa flag si apoi dupa index pt a mentine ordinea originala
        val allRows = (joinRows ++ remainingRows).sortBy (
          (_, flag, index) => (flag, index)).map(_._1)
        Some(Table(t1.name, allRows))
      case _ => None

  // Implement indexing here
  def apply(index: Int): Table =
    if (index >= 0 && index < tables.size) tables(index)
    else throw new IndexOutOfBoundsException("invalid index: " + index)
}

