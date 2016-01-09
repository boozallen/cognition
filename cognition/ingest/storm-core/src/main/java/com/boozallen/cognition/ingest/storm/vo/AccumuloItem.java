/*
 * Licensed to Booz Allen Hamilton under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Booz Allen Hamilton licenses this file to you
 * under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.boozallen.cognition.ingest.storm.vo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.accumulo.core.iterators.user.WholeRowIterator.decodeRow;

/**
 * Generic base class for items persisted to Accumulo
 */
public abstract class AccumuloItem implements Serializable {

  public AccumuloItem() {
  }

  /**
   * Create and return the <code>Mutation<code> object which will store this item in accumulo
   *
   * @return
   * @throws InvalidItemException
   */
  public Mutation toMutation() throws InvalidItemException {
    Mutation m = createMutation();
    m.put("\0implementation", "class", getClass().getCanonicalName());
    return m;
  }

  /**
   * Create and return the <code>Mutation<code> object which will store this item in accumulo
   *
   * @param vis Accumulo entry visibility
   * @return
   * @throws InvalidItemException
   */
  public Mutation toMutation(ColumnVisibility vis) throws InvalidItemException {
    Mutation m = createMutation(vis);
    m.put("\0implementation", "class", getClass().getCanonicalName());
    return m;
  }

  /**
   * Abstract method for subclasses to create and add all subclass specific properties to a storage mutation.
   *
   * @return
   * @throws InvalidItemException
   */
  protected abstract Mutation createMutation() throws InvalidItemException;

  /**
   * Abstract method for subclasses to create and add all subclass specific properties to a storage mutation.
   *
   * @param vis Accumulo entry visibility
   * @return
   * @throws InvalidItemException
   */
  protected abstract Mutation createMutation(ColumnVisibility vis) throws InvalidItemException;

  /**
   * Given the <code>row</code> SortedMap, subclasses should populate their properties using entries from the map.
   *
   * @param row
   */
  protected abstract void populate(SortedMap<Key, Value> row);

  /**
   * Public method to populate the contents of this item from a given SortedMap
   *
   * @param row
   */
  public void populateFromRow(SortedMap<Key, Value> row) {
    Key first = row.firstKey();
    if ("\0implementation".equals(first.getColumnFamily().toString())
        && "class".equals(first.getColumnQualifier().toString())) {
      row.remove(row.firstKey());
    }
    populate(row);
  }

  /**
   * Populate the contents of this item using a row encoded by <code>org.apache.accumulo.core.iterators.user.WholeRowIterator</code>
   * into a single Key-Value pair
   *
   * @param encodedRow
   * @throws IOException
   */
  public void populateFromRow(Entry<Key, Value> encodedRow) throws IOException {
    populateFromRow(buildRowMap(encodedRow));
  }

  /**
   * Populate the contents of this item using the supplied <code>PeekingIterator</code>, only consuming the next row
   * from the iterator.
   *
   * @param row
   */
  public void populateFromRow(PeekingIterator<Entry<Key, Value>> row) {
    populateFromRow(buildRowMap(row));
  }

  /**
   * Decodes a Key-Value pair encoded by <code>org.apache.accumulo.core.iterators.user.WholeRowIterator</code> into a
   * <code>SortedMap</code> containing the contents of the encoded row.
   *
   * @param encodedRow
   * @return
   * @throws IOException
   */
  public static SortedMap<Key, Value> buildRowMap(Entry<Key, Value> encodedRow) throws IOException {
    return decodeRow(encodedRow.getKey(), encodedRow.getValue());
  }

  /**
   * Pulls the Key-Value pairs representing the next row from the supplied <code>PeekingIterator</code> and returns
   * them as a <code>SortedMap</code>
   *
   * @param row
   * @return
   */
  public static SortedMap<Key, Value> buildRowMap(PeekingIterator<Entry<Key, Value>> row) {
    TreeMap<Key, Value> aggregatedRow = new TreeMap<>();
    Text rowid = null;
    while (row.hasNext() && (rowid == null || rowid.equals(row.peek().getKey().getRow()))) {
      Entry<Key, Value> entry = row.next();
      if (rowid == null) {
        rowid = entry.getKey().getRow();
      }
      aggregatedRow.put(entry.getKey(), entry.getValue());
    }
    return aggregatedRow;
  }

  /**
   * Using the row encoded by <code>org.apache.accumulo.core.iterators.user.WholeRowIterator</code> construct an
   * AccumuloItem stored in that row, if one exists.
   *
   * @param encodedRow
   * @return
   * @throws IOException
   */
  public static AccumuloItem buildFromEncodedRow(Entry<Key, Value> encodedRow) throws IOException {
    SortedMap<Key, Value> m = buildRowMap(encodedRow);
    Key first = m.firstKey();
    Value val = m.remove(first);
    AccumuloItem i = constructItemFromFirstPair(first, val);
    if (i != null) {
      i.populateFromRow(m);
    }
    return i;
  }

  /**
   * Using the provided PeekingIterator, construct an AccumuloItem from the Key-Value pairs it returns.  If skipBadRow
   * is true, this method will skip rows not containing AccumuloItems until there are now more Key-Value pairs or an
   * Accumulo Item is found, otherwise the method will return null;
   *
   * @param iter
   * @param skipBadRow
   * @return
   */
  public static AccumuloItem buildFromIterator(PeekingIterator<Entry<Key, Value>> iter, boolean skipBadRow) {
    while (iter.hasNext()) {
      Entry<Key, Value> pair = iter.peek();
      AccumuloItem i = constructItemFromFirstPair(pair.getKey(), pair.getValue());
      if (i != null) {
        iter.next();
        i.populateFromRow(iter);
        return i;
      } else if (skipBadRow) {
        buildRowMap(iter);
      } else {
        break;
      }
    }
    return null;
  }

  private static AccumuloItem constructItemFromFirstPair(Key first, Value value) {
    AccumuloItem i = null;
    try {
      if ("\0implementation".equals(first.getColumnFamily().toString())
          && "class".equals(first.getColumnQualifier().toString())) {
        String classname = value.toString();
        Class<?> cls = Class.forName(classname);
        if (AccumuloItem.class.isAssignableFrom(cls)) {
          i = cls.asSubclass(AccumuloItem.class).newInstance();
        }
      } else {
        //log incorrect value
      }
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {

    }
    return i;
  }

  /**
   * Builds an AccumuloItem from the give PeekingIterator, skipping rows until none remain or an AccumuloItem is
   * found.
   *
   * @param iter
   * @return
   */
  public static AccumuloItem buildFromIterator(PeekingIterator<Entry<Key, Value>> iter) {
    return buildFromIterator(iter, true);
  }

  /**
   * Adds the contents of <code>items</code> to the row represented by <code>out</code> as Column Qualifiers an Values
   * in the Column Family <code>columnFamily</code>
   *
   * @param out
   * @param columnFamily
   * @param items
   */
  public static void writeMap(Mutation out, String columnFamily, Map<String, String> items) {
    if (items == null) {
      return;
    }

    for (Entry<String, String> entry : items.entrySet()) {
      if (!(entry.getKey() == null || entry.getValue() == null)) {
        out.put(columnFamily, entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Adds the contents of <code>items</code> to the row represented by <code>out</code> as Column Qualifiers an Values
   * in the Column Family <code>columnFamily</code>
   *
   * @param out
   * @param columnFamily
   * @param items
   * @param vis          Accumulo entry visibility
   */
  public static void writeMap(Mutation out, String columnFamily, Map<String, String> items, ColumnVisibility vis) {
    if (items == null) {
      return;
    }

    for (Entry<String, String> entry : items.entrySet()) {
      if (!(entry.getKey() == null || entry.getValue() == null)) {
        out.put(columnFamily, entry.getKey(), vis, entry.getValue());
      }
    }
  }

  /**
   * Construct a map based upon all ColumnQualifiers and Values in the Column Family <code>columnFamily</code>,
   * optionally removing any used Key-Value pairs from <code>row</code>, the source map.
   *
   * @param row
   * @param columnFamily
   * @param clearUsed    when true, clear used entries from <code>row</code>
   * @return
   */
  public static Map<String, String> readMap(SortedMap<Key, Value> row, String columnFamily, boolean clearUsed) {
    Map<String, String> retVal = new LinkedHashMap<>();
    String rowid = row.firstKey().getRow().toString();

    SortedMap<Key, Value> familyMap = row.subMap(
        new Key(rowid, columnFamily),
        new Key(rowid, columnFamily + "\0"));
    for (Entry<Key, Value> entry : familyMap.entrySet()) {
      retVal.put(entry.getKey().getColumnQualifier().toString(), entry.getValue().toString());
    }
    if (clearUsed) {
      familyMap.clear();
    }
    return retVal;
  }
}
