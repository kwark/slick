Deadlock patch
==============
This is a fork of slick 3.1.1 containing a backport of https://github.com/slick/slick/pull/1461
Binaries and source are published to bintray.

You can include the library as follows in your SBT build:

Make sure to add a resolver for the bintray repo:

```scala
resolvers ++= Seq(Resolver.bintrayRepo("kwark", "maven"))
```

and add also add/modify your dependencies to the patched version:

```scala
"com.typesafe.slick" %% "slick" % "3.1.1.2"
"com.typesafe.slick" %% "slick-hikaricp" % "3.1.1.2"
```
*3.1.1.2* is the patched version of the *3.1.1* release. The patched version needs to be higher than the original, otherwise it might get evicted in favor of the original slick library.

The original license still applies: Licensing conditions (BSD-style) can be found in LICENSE.txt.

