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

## The Effect System and Effect Handlers

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
In some sense, we could view this as **supplying implementations of the effect operations** to the function `sumTwice`.

If we use the effect `Calculator` in the `sumTwice` function and don't 
annotate the `\ {Calculator}` effect.

```ray
def sumTwice(x: int32, y: int32) -> int32:
    let a = Calculator.add(x, y)
    let b = Calculator.add(x, y)
    return a + b
```

We'd get a compile-time error like this (look at this beautiful error message!):

```
[error]: function body effects do not match its signature: expected `{}`, but found `{Calculator | {any}}`
  ╭▸ test.ray:5:5
  │
5 │ def sumTwice(x: int32, y: int32) -> int32:
  │     ━━━━━━━━ the function body has effects outside its signature
6 │     let a = Calculator.add(x, y)
  ╰╴            ──────────────────── effect `{Calculator}` introduced here

[error]: Compilation aborted due to 1 error(s)
```

The above error message means that the function signature of `sumTwice` says
that it has no effects, which is represented by having `{}` in the effect 
annotation. But in fact, the function body of `sumTwice` does use the 
`Calculator` effect, which contradicts to what the function signature says. 

So, the **effect system** of Ray is able to track the effects of each expression 
in the function body, and check whether they match the function signature. 

Here, we'll show an another more complex example of using effects in Ray. Let's
say we declares an additional effect called `Logger` like this:

```ray
eff Calculator:
  def add(x: int32, y: int32) -> int32

# Say hello to the logger effect!
eff Logger:
  def log(x: int32)
```
