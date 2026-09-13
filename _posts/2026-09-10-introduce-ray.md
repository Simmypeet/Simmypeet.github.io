---
layout: post
title: "My Hobby Programming Language"
date: 2026-03-08 10:00:00 +0000
categories: update
---

This past summer, I've got a chance to join an internship at Flix programming 
language team. It was a very exciting and wonderful experience for me to study
about the type and effect system of Flix, and at the same time, I also felt 
inspired to create a new programming language that explores the ideas that I've 
learned from the internship.

Previously, I  have also built an another programming language called **Pernix** 
for quite several years, but I couldn't resist the urge to "play around with new 
ideas and start a new project from scratch" :P

So, I'd like to introduce my programming language called **Ray**. I'd like to 
see whether if we can have a programming language that:

- Is a system-level programming language that is compiled to native code like 
  C/C++/Rust
- Has an effect system that tracks the effects of functions 
- Has an effect handler implementation with predictable performance and low overhead
- Has a trait/typeclass system in an explicit dictionary-passing style (like OCaml's module system).
- Has memory-safety analysis like Rust's ownership rules and borrow checker

In this post, I'll summarize my experiences in building Ray compiler and discuss
interesting ideas that I implemented in Ray.

# The Effect System and Effect Handlers

The user can declare effects in Ray using `eff` keyword like this:

```ray
eff Calculator:
  def add(x: int32, y: int32) -> int32
```

From the surface, it looks like a trait or interface declaration, which 
essentially is in some sense. It declares a set of operations that **can be 
performed**. Then we can make use of the effect in a function like this:

```ray
def sumTwice(x: int32, y: int32) -> int32 \ {Calculator}:
  let a = Calculator.add(x, y)
  let b = Calculator.add(x, y)
  return a + b
```

The function `sumTwice` signature almost looks like a normal function signature,
except that it has an additional effect annotation `\ {Calculator}`. This 
signifies that the function `sumTwice` **may perform** the effect `Calculator`.
That annotation is important because it allows the type-checker to track the effects of functions and ensure that all of the effects that a function may
perform are visible to the caller.

Here, we'll show an another more complex example of using effects in Ray. Let's
say we declares an additional effect called `Logger` like this:

```ray
eff Calculator:
  def add(x: int32, y: int32) -> int32

# Say hello to the logger effect!
eff Logger:
  def log(x: int32)
```

Here, we'll define a function `printSumTwice` that uses the earlier defined `sumTwice` function and prints the result through the `Logger` effect:

```ray
def printSumTwice(x: int32, y: int32) \ {Calculator, Logger}:
  let result = sumTwice(x, y)
  Logger.log(result)
```

Notice that the function `printSumTwice` now includes both `Calculator` and 
`Logger` in its effect annotation. This makes sense because calling `sumTwice`
performs the `Calculator` effect as its `\ {Calculator}` signature indicates 
and calling `Logger.log` performs the `Logger` effect. Together, the function 
`printSumTwice` may perform both effects, and the type-checker ensures that this 
is properly tracked.

"What if I forget to include an effect in the annotation?" Need not worry! The
type-checker will catch that mistake and report an error. For example, if we
forget to include `Calculator` in the function signature of `printSumTwice`, the type-checker will raise an error like this:

```
[error]: function body effects do not match its signature: expected `{Logger}`, but found `{Calculator, Logger | {any}}`
   ╭▸ test.ray:15:5
   │
15 │ def printSumTwice(x: int32, y: int32) \ {Logger}:
   │     ━━━━━━━━━━━━━ the function body has effects outside its signature
16 │     let result = sumTwice(x, y)
   │                  ────────────── effect `{Calculator}` introduced here
17 │     Logger.log(result)
   ╰╴    ────────────────── effect `{Logger}` introduced here

[error]: Compilation aborted due to 1 error(s)
```

So far, we have shown how the language tracks and verifies the effects of 
functions. But how do we **provide** an actual implementation for the effects?

Here, user can provide an **effect handler** for the effect like this:

```ray
def sumTwiceWithHandler(x: int32, y: int32) -> int32:
  run:
    return sumTwice(x, y)
  
  with Calculator:
    def add(x, y):
      return x + y
```

The function `sumTwiceWithHandler` provides an implementation for the 
`Calculator` effect. The `run ... with ...` block is a special construct that
allows user to provide an implementation for the effect. The `run` block 
contains the code that may perform the effect, and the `with` block contains
the implementation of the effect. The above example defines an implementation 
for the `Calculator.add` operation that simply adds two integers together.

However, you might notice that the function `sumTwiceWithHandler`'s signature no
longer specifies the `Calculator` effect.  This is not a mistake! The effect 
handler implementation **handles** the `Calculator` effect, it subtracts the `Calculator` effect from the `sumTwice(x, y)` call expression, leaving the 
expression having no effects. 

Essentially, the effect system and effect handlers work together to provide a
guarantee that **all effects are eventually handled**!