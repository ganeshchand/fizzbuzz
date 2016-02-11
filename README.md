# Fizz Buzz game using Apache Spark

There are many variations of [Fizz Buzz](https://en.wikipedia.org/wiki/Fizz_buzz) game. I have implemented the 
following variation:

Any number divisible by three is replaced by the word fizz and any divisible by five by the word buzz.
Numbers divisible by both become fizz buzz. For example:

1, 2, Fizz, 4, Buzz, Fizz, 7, 8, Fizz, Buzz, 11, Fizz, 13, 14, Fizz Buzz, 16, 17, Fizz, 19, Buzz

-----------

## Part 1: Batch

FizzBuzz parallelizes a collection of numbers from 1 to 15 and displays the result.
See FizzBuzz


## Part 2: Streaming

The goal is to have a Spark streaming job which accepts numbers (digits separated by newline characters) 
from a TCP socket (localhost:2222 is fine), applies FizzBuzz on them , do a count for each returned value from the batch 
(# of Fizz, # of Buzz, ...), and print this data on the console *of the driver*.

See FizzBuzzStream

 
Examples:

1. A mini-batch containing 4x1, 4x3, 4x5 and 4x15 will print

    1 - 4

    Buzz - 4

    Fizz - 4

    Fizz Buzz - 4


2. A mini-batch containing once all numbers from 1 to 20 will print:

    1 - 1

    11 - 1
    
    13 - 1
    
    14 - 1
    
    16 - 1
    
    17 - 1
    
    19 - 1
    
    2 - 1
    
    4 - 1
    
    7 - 1
    
    8 - 1
    
    Buzz - 3
    
    Fizz - 5

    Fizz Buzz - 1

## Part 2: Stateful Streaming

Same as Part 2 but the count is cumulative. See FizzBuzzStreamStateful


## To Do
*   Code comments
*   Unit tests
*   Exception Handling
*   Commandline arguments. Remove hard-coded values

