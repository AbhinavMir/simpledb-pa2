package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int tableid;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile f = new RandomAccessFile(this.f, "r");
            int offset = BufferPool.getPageSize() * pid.pageNumber();
            byte[] data = new byte[BufferPool.getPageSize()];
            if (offset + BufferPool.getPageSize() > f.length()) {
                System.err.println("Page offset exceeds max size, error!");
                System.exit(1);
            }
            f.seek(offset);
            f.readFully(data);
            f.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (FileNotFoundException e) {
            System.err.println("FileNotFoundException: " + e.getMessage());
            throw new IllegalArgumentException();
        } catch (IOException e) {
            System.err.println("Caught IOException: " + e.getMessage());
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {

        RandomAccessFile f = new RandomAccessFile(this.f, "rw");
        int offset = BufferPool.getPageSize() * page.getId().pageNumber();
        f.seek(offset);
        f.write(page.getPageData(), 0, BufferPool.getPageSize());
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // XXX: this seems to be rounding it down. isn't that wrong?
        // XXX: (marcua) no - we only ever write full pages
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // create a new tuple and insert it
        // return the modified page
        // if the table is empty, create a new page
        // if the table is not empty, find the first page that has empty slot
        // if no page has empty slot, create a new page
        // if the table is not empty, find the first page that has empty slot
        // if no page has empty slot, create a new page
        ArrayList<Page> modifiedPages = new ArrayList<Page>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                modifiedPages.add(page);
                return modifiedPages;
            }
        }

        // if no page has empty slot, create a new page
        HeapPageId pid = new HeapPageId(getId(), numPages());
        HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        modifiedPages.add(page);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        HeapPage heap_pg = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        ArrayList<Page> pages = new ArrayList<Page>();
        heap_pg.deleteTuple(t);
        heap_pg.markDirty(true, tid);
        pages.add(heap_pg);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile
 */
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    int curpgno = 0;

    TransactionId tid;
    HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open() throws DbException, TransactionAbortedException {
        curpgno = -1;
    }

    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext()) it = null;

        while (it == null && curpgno < hf.numPages() - 1) {
            curpgno++;
            HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
            HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid, curpid, Permissions.READ_ONLY);
            it = curp.iterator();
            if (!it.hasNext()) it = null;
        }

        if (it == null) return null;
        return it.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    public void close() {
        super.close();
        it = null;
        curpgno = Integer.MAX_VALUE;
    }
}
