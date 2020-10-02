### Using the Low-Level Library

To use MQL, include the [mql-jvm](https://search.maven.org/search?q=a:mql-jvm) library in your dependencies.

Refer to the example below for a minimalist getting started guide. Imagine we have a [source] of
`Map<String, Object>` representing your data (recall that Mantis events can be easily parsed as
such). Executing a query against that [Observable] is only a matter of putting that `Observable`
into a `Map<String, Observable>` to represent the context and calling `evalMql` against said
context. The result will be an `Observable` that represents the results of the query:

```java hl_lines="5 6 7 8 9 10 11 12 13 14 15 16"
package my.package;
import java.util.HashMap;

class MqlExample {
    public static void main(String[] args) {
        // Create a test observable source of x, y coordinates.
        Observable<HashMap<String, Object>> source = Observable.interval(100, TimeUnit.MILLISECONDS).map(x -> {
            HashMap<String, Object> d = new HashMap<>();
            d.put("x", x);
            d.put("y", x);
        });

        HashMap<String, Observable<HashMap<String, Object>> context = new HashMap<>();
        context.put("observations", source);
        // You don't have to block, and shouldn't. It is just to keep the example running.
        io.mantisrx.mql.Core.evalMql("select y from observations where x > 50 OR y == 10", context).toBlocking().forEach(System.out::println);
}
```
