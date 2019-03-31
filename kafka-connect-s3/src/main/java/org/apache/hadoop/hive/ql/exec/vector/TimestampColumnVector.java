package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.io.Writable;

public class TimestampColumnVector  extends ColumnVector {
  public TimestampColumnVector(int len) {
    super(len);
    throw new UnsupportedOperationException("Just a mock, waiting until hive-exec library get its update");
  }

  @Override
  public Writable getWritableObject(int i) {
    throw new UnsupportedOperationException("Just a mock, waiting until hive-exec library get its update");
  }

  @Override
  public void flatten(boolean b, int[] ints, int i) {
    throw new UnsupportedOperationException("Just a mock, waiting until hive-exec library get its update");
  }

  @Override
  public void setElement(int i, int i1, ColumnVector columnVector) {
    throw new UnsupportedOperationException("Just a mock, waiting until hive-exec library get its update");
  }
}