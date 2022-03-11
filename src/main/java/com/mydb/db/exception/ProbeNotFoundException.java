package com.mydb.db.exception;

public class ProbeNotFoundException extends RuntimeException {
  public ProbeNotFoundException(String errorMessage) {
    super(errorMessage);
  }
}

