
package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;
 
public class ISAMIndex implements IndexFile {
  
  public ISAMIndex(ManagedFile file, IndexKeySpec keySpec)
    throws IOException, SqlException
  {
    throw new SqlException("Unimplemented");
  }
  
  public static ISAMIndex create(FileManager fm,
                                 File path,
                                 Iterator<Datum[]> dataSource,
                                 IndexKeySpec key)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }
  
  public IndexIterator scan() 
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanTo(Datum[] toKey)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanFrom(Datum[] fromKey)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScan(Datum[] start, Datum[] end)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public Datum[] get(Datum[] key)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }
    
}