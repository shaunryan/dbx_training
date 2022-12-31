// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Acknowledgments
// MAGIC 
// MAGIC This presentation is adapted from Dan Garrette’s Scala Basics web page.
// MAGIC 
// MAGIC * Dan is presently a CS postdoc at Univ. of Washington, and created this material to assist students in his courses at UT Austin
// MAGIC * The original page is hosted at <a href="http://www.dhgarrette.com/nlpclass/scala/basics.html" target="_blank">http&#58;//www.dhgarrette.com/nlpclass/scala/basics.html</a>
// MAGIC * Additional material thanks to Brian Clapper’s Scala Bootcamp

// COMMAND ----------

// MAGIC %md
// MAGIC ## Java Language
// MAGIC * Embody object-oriented principles
// MAGIC * Closer to C++ than to Smalltalk
// MAGIC * Easier to use, harder to mess up than C++
// MAGIC   * Automatic memory management
// MAGIC * From a programming language point of view it was a “MVP” (minimal viable product)
// MAGIC * However, that made it easy to learn
// MAGIC   * Though missing many sophisticated language features
// MAGIC * Java Community Process (open but slow)

// COMMAND ----------

// MAGIC %md
// MAGIC ## JVM (Java Virtual Machine)
// MAGIC ### Goals?
// MAGIC * Like Smalltalk, run in a known environment
// MAGIC * Partially isolated from underlying OS/Hardware
// MAGIC   * “Write once run anywhere”
// MAGIC * Hopefully perform better than Smalltalk
// MAGIC * Automatic memory management via garbage collection
// MAGIC 
// MAGIC ### Success?
// MAGIC * Mostly! Great performance, portability!

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scala: a JVM language
// MAGIC 
// MAGIC Scala was designed from the beginning as a JVM language. Although it has many features which work differently than Java, and many other features which Java lacks entirely,
// MAGIC * it compiles to JVM bytecode, 
// MAGIC * deploys as .class or .jar files, 
// MAGIC * runs on any standard JVM,
// MAGIC * and interoperates with any existing Java classes.
// MAGIC 
// MAGIC <pre>import java.util.{Date, Locale}
// MAGIC import java.text.DateFormat
// MAGIC import java.text.DateFormat._
// MAGIC object FrenchDate {
// MAGIC   def main(args: Array[String]) = {
// MAGIC     val now = new Date
// MAGIC     val df = getDateInstance(LONG, Locale.FRANCE)
// MAGIC     println(df format now)
// MAGIC   }
// MAGIC }</pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scala, scalac
// MAGIC 
// MAGIC To compile a program in Scala:  
// MAGIC * put your source code in a file (Scala does not have the package/folder restrictions that Java does)
// MAGIC * run the scalac compiler executable that is part of the Scala distro
// MAGIC <pre>scalac HelloWorld.scala</pre>
// MAGIC 
// MAGIC To run your program, use the scala command:
// MAGIC <pre>scala –classpath . HelloWorld</pre>
// MAGIC 
// MAGIC What about that interop with Java? You can package your Scala projects
// MAGIC so that they include all dependencies, and can run using the traditional
// MAGIC **java** command as well.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### The Scala REPL
// MAGIC 
// MAGIC Scala projects are typically built with Maven, or a tool called sbt, which allows configuration of build tasks themselves in Scala.
// MAGIC 
// MAGIC However, we don’t need to build or even compile Scala code to try it out!
// MAGIC 
// MAGIC Scala includes a REPL (“read-eval-print-loop”) where we can experiment and test out code. Just run the scala binary:
// MAGIC <pre style="font-weight:bold">Welcome to Scala version 2.10.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_05).
// MAGIC Type in expressions to have them evaluated.
// MAGIC Type :help for more information.
// MAGIC 
// MAGIC scala> val s = "Hello World!"
// MAGIC s: String = Hello World!
// MAGIC 
// MAGIC scala> println(s.toLowerCase)
// MAGIC hello world!
// MAGIC </pre>
// MAGIC 
// MAGIC *Follow along with this presentation by trying out code in the Scala REPL!*

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## SBT
// MAGIC SBT is the Scala Build Tool (<a href="http://www.scala-sbt.org" target="_blank">http&#58;//www.scala-sbt.org</a>).
// MAGIC 
// MAGIC The "S" used to stand for "Simple", but it's not all that simple.  
// MAGIC (It's gotten a lot simpler, though…)
// MAGIC 
// MAGIC The easiest way to create an SBT project is with the Lightbend Activator:  
// MAGIC <a href="https://www.lightbend.com/activator/download" target="_blank">https&#58;//www.lightbend.com/activator/download</a>
// MAGIC 
// MAGIC If you're using a Mac with Homebrew, use:  
// MAGIC <pre style="font-weight:bold">brew install typesafe-activator</pre

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scala and IDEs
// MAGIC 
// MAGIC If you're doing development in an IDE, there are two main choices:
// MAGIC * The Scala IDE (<a href="http://scala-ide.org" target="_blank">http&#58;//scala-ide.org</a>), an Eclipse plugin
// MAGIC * IntelliJ IDEA, with the Scala plugin (version 14 or better)
// MAGIC 
// MAGIC There's also a Netbeans Scala plugin, but Netbeans is far
// MAGIC less popular, so you'll have more trouble getting help.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Databricks
// MAGIC * Databricks is a tool and environment for
// MAGIC   * Easily launching, managing, and stopping Spark clusters
// MAGIC   * Collaboratively creating Spark code projects and queries
// MAGIC   * Scheduling repeated jobs
// MAGIC   * Publishing visualizations, dashboards, and web services
// MAGIC   
// MAGIC * Today, we will focus on using Databricks as an easy, interactive, cloud-based way to write Scala code.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Programming Fundamentals
// MAGIC To do procedural programming, we need
// MAGIC * Variables – a place to store data
// MAGIC   * "Represent" memory, but in modern architectures they
// MAGIC are fairly far from physical memory
// MAGIC   * And techniques that bring them closer to physical
// MAGIC memory often do so in surprising (though clever) ways
// MAGIC * Syntax (the rules about parens, braces, etc.)
// MAGIC * Flow control constructs
// MAGIC   * Looping (repeating sections of code)
// MAGIC   * Branching (conditionally jumping to other code sections)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Grab a Cheat Sheet!
// MAGIC * Class handout – nice to have in print! – is by Alvin
// MAGIC Alexander (author of O’Reilly Scala Cookbook)
// MAGIC   * <a href="http://alvinalexander.com/downloads/scala/Scala-Cheat-Sheet-devdaily.pdf" target="_blank">http&#58;//alvinalexander.com/downloads/scala/Scala-Cheat-Sheet-devdaily.pdf</a>
// MAGIC *  I’m good with all that, now I want style recommendations
// MAGIC  * <a href="http://docs.scala-lang.org/cheatsheets" target="blank">http&#58;//docs.scala-lang.org/cheatsheets</a>
// MAGIC * Ok, just get me some examples and the hard stuff
// MAGIC   * <a href="https://github.com/lampepfl/progfun-wiki/blob/gh-pages/CheatSheet.md" target="_blank">https&#58;//github.com/lampepfl/progfun-wiki/blob/gh-pages/CheatSheet.md</a>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Variables (and values)
// MAGIC There are two keywords for declaring “variables”: val and var.
// MAGIC * Identifiers declared with val cannot be reassigned;
// MAGIC * this is like a final variable in Java
// MAGIC * Identifiers declared with var may be reassigned.

// COMMAND ----------

val a = 1
var b = 2

b = 3 // fine
a = 4 // error: reassignment to val

// COMMAND ----------

// MAGIC %md
// MAGIC You should generally use val. If you find yourself wanting to use a
// MAGIC var, there may be a better way to structure your code.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Style and syntax basics
// MAGIC * Variables and methods start with lowercase letters
// MAGIC * Constants start with capitals
// MAGIC * Class names start with a capital letter
// MAGIC * Everything else uses camelCase
// MAGIC * Blocks are delimited by `{` and `}` (and are expressions)
// MAGIC * Multiple expressions on one line are separated with a `;`
// MAGIC * Line ends don’t require `;` and whitespace is not semantically significant

// COMMAND ----------

// MAGIC %md
// MAGIC ## Types
// MAGIC * Scala has a rich and complex type system
// MAGIC   * Here, we’re interested in the most common/useful types, not
// MAGIC an exhaustive analysis
// MAGIC * Value types: `Double`, `Float`, `Int`, `Long`, `Short`, `Byte`, `Char`, `Boolean`, `Unit`
// MAGIC * `AnyRef` (`java.lang.Object`)
// MAGIC   * `Seq`, `List`, other Scala class/trait types
// MAGIC   * Java object types (classes, interfaces)
// MAGIC * `AnyVal`, `AnyRef` derive from `Any` ("unified type system")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Specifying Types
// MAGIC 
// MAGIC Scala has powerful type inference capabilities:
// MAGIC * In many cases, types do not need to be specified
// MAGIC * However, types may be specified at any time

// COMMAND ----------

val a = 4
val b: Int = 4
var c: Int = _

// COMMAND ----------

// MAGIC %md
// MAGIC This can make complex code more readable, or protect against errors. 
// MAGIC 
// MAGIC Types can be specified on any subexpression, not just on variable assignments.

// COMMAND ----------

val d = (a: Double) + 5

// COMMAND ----------

// MAGIC %md
// MAGIC *All types are determined statically during compilation.*
// MAGIC 
// MAGIC Common types include `Int`, `Long`, `Double`, `Boolean`, `String`, `Char`, `Unit` ("void")

// COMMAND ----------

// MAGIC %md
// MAGIC ## First data structure: Tuple
// MAGIC Fixed length and element types
// MAGIC * Immutable
// MAGIC * Access elements via `._1`, `._2`, etc.

// COMMAND ----------

val customer = ("Bob", "bob@foomail.com", 201)
println(customer._2)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Control
// MAGIC Scala has many familiar control structures.
// MAGIC ### `if-else`

// COMMAND ----------

val x = 4
  if(x > 2)
    println("greater than 2")
  else if(x < 4)
    println("less than to 2")
  else
    println("equal to 2")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Lab 1: Customer
// MAGIC Create 3 customer “records” using Tuples
// MAGIC * Each customer record will contain the customer’s
// MAGIC   * First name
// MAGIC   * Last name
// MAGIC   * Account value
// MAGIC * Calculate the mean of the 3 account values
// MAGIC   * Assign the result to the variable `acctMean`
// MAGIC * Use the following data:
// MAGIC   * Smith, John, 150, assign to the variable `customerA`
// MAGIC   * Jackson, Anna, 250, assign to the variable `customerB`
// MAGIC   * Hernandez, Tim, 350, assign to the variable `customerC`

// COMMAND ----------

// ANSWER
val customerA = ("Smith", "John", 150)
val customerB = ("Jackson", "Anna", 250)
val customerC = ("Hernandez", "Tim", 350)

val acctMean = (customerA._3+customerB._3+customerC._3)/3

// COMMAND ----------

// MAGIC %md
// MAGIC Run the following cell to verify your solution.

// COMMAND ----------

// TEST - Run this cell to test your solution.

// utility method to help testing
def expect(name:String, actual:Any, expected:Any)={ 
  assert (actual == expected, s"""$name: Expected "$expected" but found "$actual" instead.""")
} 

expect("customerA's last name", customerA._1, "Smith")
expect("customerA's first name", customerA._2, "John")
expect("customerA's account value", customerA._3, 150)

expect("customerB's last name", customerB._1, "Jackson")
expect("customerB's first name", customerB._2, "Anna")
expect("customerB's account value", customerB._3, 250)

expect("customerC's last name", customerC._1, "Hernandez")
expect("customerC's first name", customerC._2, "Tim")
expect("customerC's account value", customerC._3, 350)

expect("mean", acctMean, 250)

println("All tests passed!\n"+("-"*80))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Loops
// MAGIC 
// MAGIC One looping construct is the **while** loop:

// COMMAND ----------

var x = 0

while (x < 3) {
  println ("x is " + x)
  x = x + 1
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Lab 2: FizzBuzz
// MAGIC 
// MAGIC Made famous by Joel Spolsky, the "Fizz-Buzz test" is an
// MAGIC interview question designed to help filter out the 99.5% of
// MAGIC programming job candidates who can't seem to program
// MAGIC their way out of a wet paper bag.
// MAGIC 
// MAGIC The text of the programming assignment is as follows:
// MAGIC 
// MAGIC > Write a program that prints the numbers from 1 to 100.  
// MAGIC But for multiples of three print “Fizz” instead of the number and for the multiples of five print “Buzz”.  
// MAGIC For numbers which are multiples of both three and five print “FizzBuzz”.
// MAGIC 
// MAGIC Congratulations, you can now code FizzBuzz in Scala!

// COMMAND ----------

// MAGIC %md
// MAGIC ## for-each loop
// MAGIC Basic loop similar to Python for-in or Java for-each
// MAGIC 
// MAGIC First example:

// COMMAND ----------

for (x <- 1 to 5) // 1 to 5 is a "Range" object
  println("Value: "+x)

// COMMAND ----------

val r = 1 to 5

// COMMAND ----------

Second example:

// COMMAND ----------

val xs = Vector(1,2,3,4,5)

for(x <- xs)
  println("Number: "+x)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Everything is an expression
// MAGIC 
// MAGIC In Scala, many things are expressions that are not in other languages.
// MAGIC 
// MAGIC Blocks are expressions that are evaluated and resolve to the value of the final expression in the block:

// COMMAND ----------

val x = {
  val intermediate1 = 2 + 3
  val intermediate2 = 4 + 5
  // No "return" statement required.
  // The following result will be "returned" from the block:
  intermediate1 * intermediate2
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Functions
// MAGIC * Functions are defined using def
// MAGIC * Parameter types must be specified
// MAGIC * Return types are optional
// MAGIC   * can be inferred at compile-time (unless the function is recursive)
// MAGIC * Function body should be separated from signature by =
// MAGIC * Braces are not needed if the function body is just a single
// MAGIC expression
// MAGIC * Parens are not needed in the function signature if there are
// MAGIC no params
// MAGIC   * Empty parens means they are optional on the call
// MAGIC   * Function defined without parens, means they are not allowed, so the call
// MAGIC looks like a variable access
// MAGIC * The return keyword is not needed (and is rarely used)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Function Declaration Examples

// COMMAND ----------

def mult(i: Int, j: Int): Int = i * j // return type specified
def add(i: Int, j: Int) = i + j       // no braces needed
def mystring() = "something"          // parentheses option in caller
def mystring2 = "something else"      // no parentheses allowed in call
def doubleSum(i: Int, j: Int) = {     // braces for multiple statements
  val sum = i + j
  sum * 2 // "return value"
}
def optSum(i:Int, j:Int = 100) = i + j // j defaults to 100 if omitted

// COMMAND ----------

// MAGIC %md
// MAGIC ## Function Usage Examples

// COMMAND ----------

val d = mult(2,3)
val e = add(2,3)
val f = mystring()
val g = mystring
val h = mystring2
val i = doubleSum(2,3)
val j = optSum(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Function objects and lambdas
// MAGIC 
// MAGIC Scala also supports function objects, which have types, and can be assigned:

// COMMAND ----------

val add = (a:Int, b:Int) => a+b

val k = add(4, 5)

// COMMAND ----------

// MAGIC %md
// MAGIC ...and lambdas, or function expressions, which can be used inline without an assignment:

// COMMAND ----------

val l = List(3,4,5).map(x => x*x)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Lab 3a: Functions
// MAGIC Starting with your “customer” code from Lab 1
// MAGIC * Write a function that takes a customer (tuple) and returns the customer’s last name, a `String`.
// MAGIC * Name the function `custTupleToLastName`

// COMMAND ----------

// ANSWER

def custTupleToLastName(customer:(String,String,Int)): String = {
  return customer._1
}

// COMMAND ----------

// TEST - Run this cell to test your solution.

val answerA = custTupleToLastName(customerA)
val answerB = custTupleToLastName(customerB)
val answerC = custTupleToLastName(customerC)

expect("customerA's last name", answerA, "Smith")
expect("customerB's last name", answerB, "Jackson")
expect("customerC's last name", answerC, "Hernandez")

println("All tests passed!\n"+("-"*80))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Lab 3b: Functions
// MAGIC * Write a function that takes 2 customer tuples and returns a tuple containing each of their last names as well as the mean of the account values
// MAGIC * Name the function `extractStats`
// MAGIC * The return value will be of the form `(String, String, Double)`

// COMMAND ----------

// ANSWER

def extractStats(customerA:(String,String,Int), customerB:(String,String,Int)): (String,String,Double) = {
  return (customerA._1, customerB._1, (customerA._3+customerB._3)/2.0)
}

// COMMAND ----------

// TEST - Run this cell to test your solution.

val statsA = extractStats(customerA, customerB)
val statsB = extractStats(customerB, customerC)
val statsC = extractStats(customerC, customerA)

expect("customerA's last name", statsA._1, "Smith")
expect("customerB's last name", statsA._2, "Jackson")
expect("mean", statsA._3, 200)

expect("customerA's last name", statsB._1, "Jackson")
expect("customerB's last name", statsB._2, "Hernandez")
expect("mean", statsB._3, 300)

expect("customerA's last name", statsC._1, "Hernandez")
expect("customerB's last name", statsC._2, "Smith")
expect("mean", statsC._3, 250)

println("All tests passed!\n"+("-"*80))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Lab 3c: Functions
// MAGIC 
// MAGIC Create a function to calculate the mean of two numbers:
// MAGIC   * Name the function `calcMean`
// MAGIC   * The function should take two `Int`s
// MAGIC   * The function should return the mean of the two values as a `Double`
// MAGIC   
// MAGIC Create a function to calculate the difference of two numbers:
// MAGIC   * Name the function `calcDiff`
// MAGIC   * The function should take two `Int`s
// MAGIC   * The function should return the difference of the two values as a `Double`

// COMMAND ----------

// ANSWER

def calcMean(a:Int, b:Int): Double = {
  (a+b)/2.0
}

def calcDiff(a:Int, b:Int): Double = {
  return a-b
}

// COMMAND ----------

// TEST - Run this cell to test your solution.

val meanValue = calcMean(3,6)
expect("Mean", meanValue, 4.5)

val diffValue = calcDiff(7,5)
expect("Difference", diffValue, 2)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Lab 3d: Functions
// MAGIC 
// MAGIC Create a function to compute variable stats on two customer tuples:
// MAGIC * Name the fuction `evalCustomers`
// MAGIC * The function will return a `Double`
// MAGIC * The first parameter should be a customer tuple such as `customerA:(String,String,Int)`
// MAGIC * The second parameter should be a customer tuple such as `customerB:(String,String,Int)`
// MAGIC * The third parameter should be a function reference with the signature `(Int,Int):Double`, the same as our two previous functions.
// MAGIC   * Hint: The third parameter will be of the form `func: (Int,Int) => Double`
// MAGIC * The method body should call the passed in function with both customer's account values.
// MAGIC 
// MAGIC Test it by calling `evalCustomers(customerA, customerB, calcDiff)` or `evalCustomers(customerB, customerA, calcMean)` or other variations.

// COMMAND ----------

// ANSWER

def evalCustomers(customerA:(String,String,Int), customerB:(String,String,Int), func: (Int,Int) => Double): Double = {
  func(customerA._3, customerB._3)
}

// COMMAND ----------

// TEST - Run this cell to test your solution.

val custMean = evalCustomers(customerA, customerB, calcMean)
val custDiff = evalCustomers(customerB, customerC, calcDiff)

expect("mean", custMean, 200.0)
expect("diff", custDiff, -100.0)

println("All tests passed!\n"+("-"*80))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scopes and Closures
// MAGIC Scala supports nested scopes (e.g., functions defined inside of other functions) as well as closures.
// MAGIC 
// MAGIC This means that any time we create a function which references free variables defined in an outer scope, that function holds a reference to those outer scope variables, and those variables maintain their definition-site meanings.
// MAGIC 
// MAGIC So when we store a function or pass a function as a parameter, we’re also storing/passing any variables from the outer scope(s) that we might be using in our function.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Object-Oriented Programming
// MAGIC * Combine state and behavior into an "object" which only exposes behavior ("encapsulation")
// MAGIC   * Send messages ("method calls") to objects, get output
// MAGIC * Create new, extended objects from existing ones ("inheritance")
// MAGIC * Sending a message defined for a base type will automatically trigger relevant extended behavior in a derived type ("polymorphism")

// COMMAND ----------

// MAGIC %md
// MAGIC ## OO Example, Pros/Cons

// COMMAND ----------

trait Shape {
  def area:Double
}

class Square(side:Double) extends Shape {
  def area:Double = { side * side }
}

class Circle(rad:Double) extends Shape {
  def area:Double = { Math.PI * rad * rad }
}

def printArea(shape:Shape) = {
  println(s"The area of a ${shape.getClass().getSimpleName().toLowerCase()} is " + shape.area)
}

val square = new Square(2)
val circle = new Circle(3)

printArea(square)
printArea(circle)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC #Classes
// MAGIC * Classes can be declared using the class keyword.
// MAGIC * Methods are declared with the def keyword.
// MAGIC * Methods and fields are public by default, but can be specified as protected or private.
// MAGIC * Constructor arguments are, by default, private, but can be proceeded by val to be made public.

// COMMAND ----------

class SomeClass(i: Int, val j: Int) {
  val iPlus5 = i + 5
  private[this] val jPlus5 = j + 5 //instance privacy
  
  def addTo(k: Int) = new SomeClass(i + k, j + k)
  def sum = i + j
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Lab 4: Customer class
// MAGIC * Create a customer class called `Customer`
// MAGIC * It should have three attributes (just like our customer tuples)
// MAGIC   * `firstName`
// MAGIC   * `lastName`
// MAGIC   * `value`
// MAGIC * Add a method that combines the first & last name in the form of "Last, First"
// MAGIC   * Name the method `fullName`
// MAGIC   * The method should not have parenthesis
// MAGIC * Add a `toString` method that returns `fullName`

// COMMAND ----------

// ANSWER

class Customer(val firstName:String, val lastName:String, val value:Int) {
  def fullName = lastName+", "+firstName
  override def toString() = fullName
}

// COMMAND ----------

// TEST - Run this cell to test your solution.

val customer = new Customer("Thomas", "Jefferson", 195)

expect("name", customer.firstName, "Thomas")
expect("name", customer.lastName, "Jefferson")
expect("name", customer.value, 195)
expect("fullName", customer.fullName, "Jefferson, Thomas")
expect("fullName", customer.toString(), "Jefferson, Thomas")

println("All tests passed!\n"+("-"*80))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Inheritance and Traits
// MAGIC 
// MAGIC Inheritance: Classes are extended using the extends keyword

// COMMAND ----------

class OtherClass(i: Int, k: Int) extends SomeClass(i, 4)

// COMMAND ----------

// MAGIC %md
// MAGIC Traits are like interfaces, but they are allowed to have members declared ("mix-in" members).

// COMMAND ----------

trait C { 
  def doCThing = "C thing" 
}
trait D { 
  def doDThing = "D thing" 
}
class E extends C with D { 
}

val e = new E()

println("EC: " + e.doCThing)
println("ED: " + e.doDThing)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Generics

// COMMAND ----------

class Gift[T](item:T) {
  def lookInside() = item
}

val giftA = new Gift[Shape](circle)
println("The area of gift A is " + giftA.lookInside().area + " whatevers")

val giftB = new Gift[Shape](square)
println("The area of gift B is " + giftB.lookInside().area + " whatevers")

println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC **Advanced note:** Scala can preserve type info subject to erasure in Java, via ClassTag / TypeTag

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scala's Built-in Collections
// MAGIC The most common collections are `Vector`, `List` (similar to Vector), `Map`, and `Set`.
// MAGIC 
// MAGIC `Vector[T]` is a sequence of items of type `T`. Elements can be accessed by 0-based index, using parens () as the subscript operator:

// COMMAND ----------

val vector = Vector(1,2,3)
val first = vector(0)
val last = vector(2)

// COMMAND ----------

// MAGIC %md
// MAGIC A `Map[K,V]` is an associative array or dictionary type mapping elements of type `K` to elements of type `V`. Values can be accessed through their keys.

// COMMAND ----------

val map = Map("one" -> 1, "two" -> 2, "three" -> 3)
val uno = map("one")
val dos = map("two")
val tres = map("three")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Type Inference and Empty Collections
// MAGIC * What if Scala doesn’t have enough information to infer a type for a container?

// COMMAND ----------

val list = List()

// COMMAND ----------

// MAGIC %md
// MAGIC We get a fairly useless list! We can fix this two ways...

// COMMAND ----------

// Option #1
val listA:List[String] = List()

// Option #2
val listB = List.empty[String]

// COMMAND ----------

// MAGIC %md
// MAGIC ## Collections API
// MAGIC * Key OO methods:
// MAGIC   * `apply(n)` retrieve nth (0-based) element, can omit apply
// MAGIC   * `:+ x` new collection with x appended
// MAGIC   * `x +:` coll new collection with x prepended
// MAGIC   * `contains` test whether collection contains passed element
// MAGIC   * `indexOf` retrieve index of passed element (or -1)
// MAGIC   * `length` get size of collection
// MAGIC   * `slice(from,to)` subsequence up to (not incl) "to"
// MAGIC   * `sorted` sorted copy of collection
// MAGIC 
// MAGIC * Most of the API is suited to functional programming; we’ll come back to that later!

// COMMAND ----------

// MAGIC %md
// MAGIC ## ScalaDoc Tips
// MAGIC 
// MAGIC Because of implicit classes, “magic” apply, and other patterns, you may see a function
// MAGIC call but have a hard time locating that function in the docs! Here are some tips:
// MAGIC * Note the O and C symbols next to each class name. They permit you to navigate to
// MAGIC the documentation for a class (C) or its companion object (O). Implicit functions will
// MAGIC often be defined in the companion object.
// MAGIC * Remember enriched classes.
// MAGIC * Look into RichInt, RichDouble, etc., if you want to know how to work with numeric
// MAGIC types. Similarly, for strings, look at StringOps.
// MAGIC * The mathematical functions are in the package scala.math, not in a class.
// MAGIC * Sometimes, you’ll see functions with funny names. For example, in BigInt, there’s a
// MAGIC unary_- method. This is how to you define the prefix negation operator -x.
// MAGIC * Methods can take functions as parameters. For instance, the count method in
// MAGIC StringOps requires a function that returns true or false for a Char, specifying which
// MAGIC characters should be counted: `def count(p: (Char) => Boolean): Int`

// COMMAND ----------

// MAGIC %md
// MAGIC ## Imports
// MAGIC Classes, objects, and static methods can all be imported.
// MAGIC 
// MAGIC Underscore can be used as a wildcard to import everything from a particular context.

// COMMAND ----------

import scala.collection.immutable.BitSet
import scala.math.log
import scala.math._

// COMMAND ----------

// MAGIC %md
// MAGIC ## Immutability
// MAGIC 
// MAGIC Default collections are immutable: if you use a “write” operation on them, they return a new collection.

// COMMAND ----------

val vectorA = Vector(1,2)
val vectorB = vectorA :+ 3

val sameVector = vectorA eq vectorB

// COMMAND ----------

// MAGIC %md
// MAGIC However, mutable collections are also available:

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer

val bufferA = ArrayBuffer(1,2)
val bufferB = bufferA += 3

val sameBuffer = bufferA eq bufferB

// COMMAND ----------

// MAGIC %md
// MAGIC ## Functional Basics
// MAGIC * Leverage higher-order functions, referential transparency, immutable data structures, recursion, scope chain for state, etc.
// MAGIC * Ok, what does that mean in practice?

// COMMAND ----------

// MAGIC %md
// MAGIC ## Procedural iteration

// COMMAND ----------

// mutable state
var i = 0           

val data = List("foo", "bar", "baz")

// leak implementation and manually test state
while (i<data.size) {
  println(data(i)) // redundant ref
  i = i + 1        // manage state
}
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Challenges
// MAGIC * Manual state management and conditional logic
// MAGIC   * that is not directly relevant to the task
// MAGIC   * can easily lead to errors (e.g., fencepost)
// MAGIC * Mutable state makes thread safety much harder
// MAGIC * Exposing unnecessary internals of data structures
// MAGIC   * Violates encapsulation
// MAGIC   * Makes extension (e.g., parallelization) harder
// MAGIC * Reduces clarity by disguising transformations, burying semantics

// COMMAND ----------

// MAGIC %md
// MAGIC ## Functional Iteration

// COMMAND ----------

List("foo","bar","baz").foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Benefits
// MAGIC * Omits all state management and control logic
// MAGIC * Leaves data structure internals hidden
// MAGIC * Implementation, location, etc. independent
// MAGIC * Surfaces key semantics (“list – foreach – println”)
// MAGIC 
// MAGIC Suppose we have a collection of numbers
// MAGIC   * We want to pick out the numbers under 20
// MAGIC   * Calculate their squares
// MAGIC   * Sum the even ones
// MAGIC * Functional style:

// COMMAND ----------

val data = List(3,4,8,12,15,19)

val answerA = data
  .filter(n => n<10)      // keep only values 10
  .map(n => n*n)          // multiply each number by itself
  .filter(n => n%2 == 0)  // keep only even numbers
  .reduce((a,b) => a+b)   // sum the remaining values

// COMMAND ----------

// MAGIC %md
// MAGIC Same operation using Scala shortcuts:

// COMMAND ----------

val answerB = data.filter(_ < 10).map(n => n*n).filter(_%2 == 0).reduce(_+_)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Collections, Option, and functional iteration
// MAGIC Instead of `null` (Java), `None` (Python), etc., Scala uses a parametric type called `Option` to handle the situation where
// MAGIC * a strongly typed object handle is needed
// MAGIC * but a value may or may not be present
// MAGIC 
// MAGIC An option...
// MAGIC * May represent a value: Some (value)
// MAGIC * Or no value: None

// COMMAND ----------

val x = Some("foo"); val y = None
println("x is " + x.getOrElse("no val in x!"))
println("y is " + y.getOrElse("no val in y!"))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC It is often useful to treat Option as a container:

// COMMAND ----------

val flatList = List(Some(3), None, None, Some(20)).flatten

// COMMAND ----------

// MAGIC %md
// MAGIC ## Iterators
// MAGIC An Iterator[T] is a lazy sequence
// MAGIC * It only evaluates its elements once they are accessed
// MAGIC * Iterators can only be traversed one time
// MAGIC 
// MAGIC Accidentally traversing the same iterator more than once is a common
// MAGIC source of bugs. If you want to be able to access the elements more than
// MAGIC once, you can always call .toVector to load the entire thing into memory.

// COMMAND ----------

val m = Iterator(1,2,3)
val n = m.map(x => x + 1)
val o = n.sum
val p = n.mkString(" ")

val q = Iterator(1,2,3)
val r = q.map(x => x + 1)
val s = r.toVector
val t = s.mkString(" ")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Case Classes
// MAGIC Case classes are syntactic sugar for classes with a few methods pre-specified for convenience. They are designed to support structural pattern-matching.
// MAGIC 
// MAGIC These include toString, equals, hashCode, and static methods apply (so that the new keyword is not needed for construction) and unapply (for pattern matching).
// MAGIC 
// MAGIC Case class constructor args are public by default. Case classes are not allowed to be extended. Otherwise, they are just like normal classes.

// COMMAND ----------

case class NumberThingy(i: Int, j: Int) {
  def sum = i + j
}

val thingy = NumberThingy(4, 5)
val total = thingy.sum

val sameThingy = thingy == NumberThingy(4,5) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Pattern Matching

// COMMAND ----------

// MAGIC %md
// MAGIC Allows for succinct code and can be used in a variety of situations.
// MAGIC * Many built-in types have pattern-matching behavior defined
// MAGIC * A main use of pattern matching is in match expressions
// MAGIC * Scala also supports conditional, wildcard, and recursive matching

// COMMAND ----------

val aAndB = Vector(5,6)
val oneTwoThree = Vector(1,2,3)

// replace oneTwoThree with aAndB
val sum = oneTwoThree match {
  case Vector(x,y) => x + y
  case Vector(x,y,z) => x + y + z
}

// COMMAND ----------

// MAGIC %md
// MAGIC We can match values, types, structure, or combinations of those:

// COMMAND ----------

// Try the different values for "testValue"
val testValue:Any = (3,4)
// val testValue:Any = (30, 4)
// val testValue:Any = (30, "four")
// val testValue:Any = (3,4,5)

val out = testValue match {
  case (3, _) => "I got a 3"
  case (_, s:String) => "ends with " + s
  case (_,_) => "Tuple2!"
  case _ => "default"
}

println(out)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Pattern matching can be combined with recursion to simplify many algorithms.

// COMMAND ----------

// MAGIC %md
// MAGIC ## More complex for-each
// MAGIC 
// MAGIC The for-each loop can be used in more complex ways, allowing
// MAGIC succinct syntax for looping over multiple collections and filtering:

// COMMAND ----------

for(x <- Vector(1,2,3,4,5); // outer loop over a vector
    if x % 2 == 1;          // keep odd values xs
    y <- Set(1,2,3);        // inner loop over a list
    if x + y == 6           // keep values that sum to 6
) {
  println(s"x=$x, y=$y")    // print with interpolation
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## For-compressions
// MAGIC Using `yield` allows the for-each expression to evaluate to a value, and not just produce side effects:

// COMMAND ----------

val yieldData = for(
  x <- Vector(1,2,3,4,5);
  if x % 2 == 1;
  y <- Set(1,2,3);
  if x + y == 6
) yield x*y

// COMMAND ----------

// MAGIC %md
// MAGIC ## Implicit Classes
// MAGIC Scala allows you to “add” behavior to existing classes in a principled way using implicit classes.
// MAGIC 
// MAGIC An implicit class takes exactly one constructor argument that is the type to be extended and defines behavior that should be allowed for that type.

// COMMAND ----------

implicit class EnhancedVector(xs: Vector[Int]) {
  def sumOfSquares = xs.map(x => x * x).sum
}

val sos = Vector(1,2,3).sumOfSquares

// COMMAND ----------

// MAGIC %md
// MAGIC ## When are "." and "()" Optional?
// MAGIC Scala makes no distinction between methods and "operators." 
// MAGIC 
// MAGIC You can actually drop the `.` and `()` from any 1-argument method:

// COMMAND ----------

case class Summer(i: Int) {
  def addTo(s: Summer) = Summer(i + s.i)
}

val testA = Summer(5)
val testB = Summer(7)

val sumOfAB = testA addTo testB

// COMMAND ----------

// MAGIC %md
// MAGIC This can sometimes make things more readable:

// COMMAND ----------

1 to 5

// COMMAND ----------

// MAGIC %md
// MAGIC ## One last bit of magic
// MAGIC The `apply` method of a class or object is used to overload the
// MAGIC parentheses syntax, allowing you to specify the behavior of what
// MAGIC looks like function application.

// COMMAND ----------

case class Wierd(i: Int){
  def apply(j: Int) = i + j
}

val somethingWeird = new Wierd(9)
val weirdResult = somethingWeird(7)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>