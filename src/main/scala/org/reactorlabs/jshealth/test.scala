package org.reactorlabs.jshealth

import sangria.ast.Document
import sangria.parser.QueryParser
import sangria.renderer.QueryRenderer

object test {


  import scala.util.Success

  val query =
    """
    query FetchLukeAndLeiaAliased(
          $someVar: Int = 1.23
          $anotherVar: Int = 123) @include(if: true) {
      luke: human(id: "1000")@include(if: true){
        friends(sort: NAME)
      }

      leia: human(id: "10103\n \u00F6 ö") {
        name
      }

      ... on User {
        birth{day}
      }

      ...Foo
    }

    fragment Foo on User @foo(bar: 1) {
      baz
    }
  """

  // Parse GraphQl query
  val Success(document: Document) = QueryParser.parse(query)

  // Pretty rendering of GraphQl query as a `String`
  println(QueryRenderer.render(document))

  // Compact rendering of GraphQl query as a `String`
  println(QueryRenderer.render(document, QueryRenderer.Compact))
}
