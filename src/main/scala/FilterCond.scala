import scala.language.implicitConversions

trait FilterCond {def eval(r: Row): Option[Boolean]}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = r.get(colName).map(predicate)
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] =
    // evaluam fiecare conditie si pastram rezultatele intr-o lista
    val results = conditions.flatMap(_.eval(r))
    if (results.isEmpty) None
    else Some(results.reduce(op)) // reducem lista prin op
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = f.eval(r).map(!_)
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_ && _, List(f1, f2))
def Or(f1: FilterCond, f2: FilterCond): FilterCond = Compound(_ || _, List(f1, f2))
def Equal(f1: FilterCond, f2: FilterCond): FilterCond =
  (r: Row) => f1.eval(r).flatMap(res1 => f2.eval(r).map(res2 => res1 == res2))

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = Option(fs.map(_.eval(r)).contains(Some(true)))
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = Option(fs.map(_.eval(r)).forall(_.contains(true)))
}

implicit def tuple2Field(t: (String, String => Boolean)): Field = Field(t._1, t._2)

extension (f: FilterCond) {
  def ===(other: FilterCond) = Equal(f, other)
  def &&(other: FilterCond) = And(f, other)
  def ||(other: FilterCond) = Or(f, other)
  def !! = Not(f)
}