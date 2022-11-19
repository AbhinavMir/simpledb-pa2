## Design Decisions

### HeapPage

The code defines a HeapPage class to store data for one page of HeapFiles and implements the Page interface that is used by BufferPool.

Each HeapPage object is associated with a HeapPageId object that specifies the page's location.

The HeapPage class has two constructors. The first is used to create an empty page. The second is used to create a page from a set of bytes of data read from disk. The format of a HeapPage is a set of header bytes indicating the slots of the page that are in use, some number of tuple slots.

The HeapPage class has several methods for accessing and manipulating the data on the page. The getPageData method generates a byte array representing th contents of the page. The insertTuple and deleteTuple methods can be used to add and remove tuples from the page. The iterator method returns an Iterator over all the tuples on the page.

### HeapFile

The code above is responsible for creating a new HeapFile object. This object is used to store data in a heap file. The constructor takes in a File object and a TupleDesc object. The File object is used to store the data in the heap file. The TupleDesc object is used to describe the schema of the data that will be stored in the heap file.

### Insert And Delete

`Insert.java` inserts tuples read from the child operator into the tableId specified in the constructor.
`Delete.java` deletes tuples read from the child operator from the tableId specified in the constructor.

## Time spent on assignment and the difficult parts

We spent about 3 days grokking the overall architecture and the requirements and reverse-engineering the tests. From there, it took us another 2 days to actually do some code writing, and another day to fix bugs and tie things up.


The idea behind the extra credit was relatively simple the necessary steps to maintain the core structure of the B+ tree
were maintained even as pages were merged, split, deleted what have you.  The pictures of what each function was supposed
to do were very helpful and I initially tested the functions by simulating those pictures and trying to get the same results.


I had to tweak the BufferPages insert and deletion function for tuples because they were initially built for Heap files and not
B+tree entries.  Getting the right IDs for merging and splitting took some time until I figured out how to properly use
both Iterator and Reverse Iterators to get the right IDs.

No real changes where made to the API.

Collaborators
Finn Jensen and Abhinav Srivastava

Exercise 1 - Abhinav
Exercise 2 - Abhinav and Finn
Exercise 3 - Finn
Exercise 4 (Bonus) - Abhinav and Finn