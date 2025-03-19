object Queries {

  def killJackSparrow(t: Table): Option[Table] =
    queryT(PP_SQL_Table_Filter((Some(t), "FILTER", row =>
      Option(row.get("name").exists(_ != "Jack")))))

  def insertLinesThenSort(db: Database): Option[Table] =
    queryDB(PP_SQL_DB_Create_Drop((Some(db), "CREATE", "Inserted Fellas")))
      .flatMap(_.tables.find(_.name == "Inserted Fellas"))
      .flatMap(table => queryT(PP_SQL_Table_Insert((Some(table), "INSERT", List(
        Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
        Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
        Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
        Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")
      )))))
      .flatMap(table => queryT(PP_SQL_Table_Sort((Some(table), "SORT", "age"))))

  def youngAdultHobbiesJ(db: Database): Option[Table] =
    queryDB(PP_SQL_DB_Join((Some(db), "JOIN", "People", "name", "Hobbies", "name")))
      .flatMap(_.tables.headOption)
      .flatMap(table => queryT(PP_SQL_Table_Filter((Some(table), "FILTER", row =>
        Option(row.get("age").exists(age => age.nonEmpty && age.toInt < 25) &&
          row.get("name").exists(_.startsWith("J")) &&
          row.contains("hobby")))))
        .flatMap(table => queryT(PP_SQL_Table_Select((Some(table), "EXTRACT", List("name", "hobby"))))))

}
